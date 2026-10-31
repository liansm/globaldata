"""
Index Spot Data Fetcher (A股 + 港股)

A股: akshare.stock_zh_index_spot_sina()
  Columns: 代码, 名称, 最新价, 涨跌额, 涨跌幅, 成交量, 成交额, 振幅, 最高, 最低, 今开, 昨收
  成交额 unit: 元，直接存储；matched by 代码 (e.g. "sh000001")

港股: akshare.stock_hk_index_spot_sina()
  Columns: 指数(code), 名称, 最新价, 涨跌额, 涨跌幅, 昨收, 今开, 最高, 最低
  无成交额列；matched by 指数 code (e.g. "HSI", "HSCEI", "HSTECH")

Writes to index_spot (one row per index, upserted on every run).
``snapshot_kind`` 区分 'intraday'（盘中实时）与 'close'（收盘快照，带成交额），
见 classify_snapshot() —— 收盘后不再跳过写库，而是落一行 close 快照。
Run any time during or after market hours for fresh data.
"""

import os
import sys
from datetime import date, datetime, time as dtime

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

# A股: index_key → sina code (e.g. "sh000001")
AH_SPOT_CONFIGS = [
    {"key": "idx_sh",    "symbol": "sh000001", "name": "上证指数"},
    {"key": "idx_sz",    "symbol": "sz399001", "name": "深证成指"},
    {"key": "idx_hs300", "symbol": "sh000300", "name": "沪深300"},
    {"key": "idx_cyb",   "symbol": "sz399006", "name": "创业板指"},
    {"key": "idx_kc50",  "symbol": "sh000688", "name": "科创50"},
    # 北证50 (bj899050) 不在 stock_zh_index_spot_sina 覆盖范围内，跳过
]

# 港股: index_key → 指数 code (matched by 指数 column in stock_hk_index_spot_sina)
HK_SPOT_CONFIGS = [
    {"key": "idx_hsi",    "hk_code": "HSI",    "name": "恒生指数"},
    {"key": "idx_hscei",  "hk_code": "HSCEI",  "name": "恒生国企"},
    {"key": "idx_hstech", "hk_code": "HSTECH", "name": "恒生科技"},
]

# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS index_spot (
    index_key  VARCHAR(60) PRIMARY KEY REFERENCES market_indices(key) ON DELETE CASCADE,
    price      NUMERIC(16,4),
    change_pct NUMERIC(8,4),
    turnover   NUMERIC(24,4),
    prev_close NUMERIC(16,4),
    spot_date  DATE,
    snapshot_kind VARCHAR(8) DEFAULT 'intraday',
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);
-- 老表补列（幂等）
ALTER TABLE index_spot ADD COLUMN IF NOT EXISTS snapshot_kind VARCHAR(8) DEFAULT 'intraday';
"""


def db_init(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def db_upsert_spot(conn, rows):
    """rows: list of (index_key, price, change_pct, turnover, prev_close, spot_date, kind)"""
    if not rows:
        return
    sql = """
    INSERT INTO index_spot (index_key, price, change_pct, turnover, prev_close, spot_date, snapshot_kind)
    VALUES %s
    ON CONFLICT (index_key) DO UPDATE SET
        price      = EXCLUDED.price,
        change_pct = EXCLUDED.change_pct,
        turnover   = EXCLUDED.turnover,
        prev_close = EXCLUDED.prev_close,
        spot_date  = EXCLUDED.spot_date,
        snapshot_kind = EXCLUDED.snapshot_kind,
        updated_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)


def _safe_float(val):
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


# 各市场开盘/收盘时刻（Asia/Shanghai，收盘留 5~10 分钟余量）。
# 只用于区分「盘中快照」和「收盘快照」，不用于判断是否交易日。
_MARKET_OPEN  = {"A股": dtime(9, 15), "港股": dtime(9, 15)}
_MARKET_CLOSE = {"A股": dtime(15, 5), "港股": dtime(16, 10)}


def classify_snapshot(cur, key, price, prev_close, turnover, today, market):
    """判断这条快照是「盘中实时」还是「收盘快照」，并给出它真正属于哪个交易日。

    返回 ``(kind, spot_date, reason)``；``kind`` 为 None 表示不落库（保留旧行）。

    为什么区分两种（2026-10-31 定案，B 方案）
    -----------------------------------------
    旧的 ``_is_today_snapshot()`` 只有「是不是当日盘中」一个判据，判否就**不写库**：

    * 好处：盘前不会把昨日收盘错记成今天（这个判定本身是对的）。
    * 坏处一：**收盘后也永远不刷新** → 那一行停在最后一次盘中快照，
      接口拿它当现价 → 周六卡片显示「10-30 09:44 实时 3615.40」，而真收盘是 3599.02。
    * 坏处二：``index_prices.turnover`` 是 NULL（新浪 stock_zh_index_daily 不给成交额），
      只能用 spot 的成交额；而收盘后 spot 又不写库，周末就退化成"成交量 4.98 亿手"。

    所以改成：**收盘后照写**，标 ``kind='close'`` 并带成交额。前端 badge 据 kind 显示
    「收盘」而非「实时」。既不再假实时，又保住了收盘成交额。

    判定顺序
    --------
    1. 最新价 ≤ 0 → 不落库（盘前 深证成指/创业板指 会返回 0）
    2. 与某历史收盘吻合（且昨收也对得上）→ ``close`` + 那天
       盘前抓到的是上一交易日收盘、周末抓到的是周五收盘，都走这条，
       所以 ``spot_date`` 不会变成「周六」「国庆」这种非交易日。
       **今天若已结算入库，也会命中这条**（今日收盘 == 日线今日收盘），正是需要的。
    3. 工作日 + 在交易时段内 → ``intraday`` + today
    4. 工作日 + 已过收盘 → ``close`` + today（今天已收市，价格即今日收盘）
    5. 其余（盘前 / 非交易日但没对上任一历史收盘）→ 不落库。
       宁可不写也不猜：保留旧行比写一行错日期的数据更安全。
    """
    cur.execute(
        """SELECT price_date, close FROM index_prices
           WHERE index_key = %s ORDER BY price_date DESC LIMIT 3""",
        (key,),
    )
    hist = cur.fetchall()

    if price is None or price <= 0:
        return None, None, "源侧最新价为 0"

    # ① 与某历史收盘吻合 → 这是「某一天的收盘静态快照」
    for i, (d, close) in enumerate(hist):
        if close is None or close <= 0:
            continue
        # 相对 1e-6 + 绝对 0.01 容差：指数点位允许四舍五入误差
        if abs(float(close) - price) > max(0.01, abs(price) * 1e-6):
            continue
        if prev_close is not None and i + 1 < len(hist) and hist[i + 1][1] is not None:
            if abs(float(hist[i + 1][1]) - prev_close) > max(0.01, abs(prev_close) * 1e-6):
                continue  # 昨收对不上，不是那天的快照（只是价格巧合）
        return "close", d, f"等于 {d} 收盘价"

    # ② 对不上任何历史收盘 → 只可能是「今天正在交易（或刚收市）」的快照
    now = datetime.now()
    if now.weekday() >= 5:              # 周六 / 周日
        return None, None, "非交易日且与历史收盘对不上"

    t = now.time()
    open_at  = _MARKET_OPEN.get(market, dtime(9, 15))
    close_at = _MARKET_CLOSE.get(market, dtime(15, 5))

    if t < open_at:
        if turnover is not None and turnover <= 0:
            return None, None, "盘前，成交额为 0（最新价是竞价参考价）"
        return None, None, "盘前且与历史收盘对不上"

    if t <= close_at:
        return "intraday", today, ""

    return "close", today, "收盘后抓取，价格即当日收盘"


# ---------------------------------------------------------------------------
# Fetch spot snapshots
# ---------------------------------------------------------------------------
def fetch_a_spot() -> dict | None:
    """Call stock_zh_index_spot_sina → dict keyed by sina code (e.g. 'sh000001')."""
    try:
        df = ak.stock_zh_index_spot_sina()
    except Exception as exc:
        print(f"[ERROR] stock_zh_index_spot_sina: {exc}")
        return None
    if df is None or df.empty:
        print("[ERROR] stock_zh_index_spot_sina: empty response")
        return None
    result = {}
    for _, row in df.iterrows():
        code = str(row.get("代码", "")).strip()
        if code:
            result[code] = row
    return result


def fetch_hk_spot() -> dict | None:
    """Call stock_hk_index_spot_sina → dict keyed by 指数 code (e.g. 'HSI')."""
    try:
        df = ak.stock_hk_index_spot_sina()
    except Exception as exc:
        print(f"[ERROR] stock_hk_index_spot_sina: {exc}")
        return None
    if df is None or df.empty:
        print("[ERROR] stock_hk_index_spot_sina: empty response")
        return None
    # First column is the index code (HSI, HSCEI, HSTECH, ...)
    code_col = df.columns[0]
    result = {}
    for _, row in df.iterrows():
        code = str(row.iloc[0]).strip()
        if code:
            result[code] = row
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        db_init(conn)
        print("  [OK] Connected. Schema ready.\n")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] Cannot connect: {exc}")
        return 1

    today = date.today().isoformat()
    rows: list = []
    failed: list = []

    # ── A股 ─────────────────────────────────────────────────────────────────
    print("Fetching stock_zh_index_spot_sina (A股)...")
    a_map = fetch_a_spot()
    if a_map is None:
        conn.close()
        return 1
    print(f"  [OK] {len(a_map)} entries in A-share snapshot\n")

    cur = conn.cursor()
    for cfg in AH_SPOT_CONFIGS:
        key    = cfg["key"]
        symbol = cfg["symbol"]
        name   = cfg["name"]
        row = a_map.get(symbol)
        if row is None:
            print(f"  [{name}] NOT FOUND (symbol={symbol})")
            failed.append(key)
            continue
        price      = _safe_float(row.get("最新价"))
        change_pct = _safe_float(row.get("涨跌幅"))
        turnover   = _safe_float(row.get("成交额"))   # 元
        prev_close = _safe_float(row.get("昨收"))
        kind, spot_date, why = classify_snapshot(
            cur, key, price, prev_close, turnover, today, "A股")
        if kind is None:
            print(f"  {name:8s}  price={price}  跳过：{why}")
            continue
        print(f"  {name:8s}  price={price}  chg={change_pct}%  turnover={turnover}  "
              f"昨收={prev_close}  [{kind} @ {spot_date}]")
        rows.append((key, price, change_pct, turnover, prev_close, spot_date, kind))

    # ── 港股 ─────────────────────────────────────────────────────────────────
    print("\nFetching stock_hk_index_spot_sina (港股)...")
    hk_map = fetch_hk_spot()
    if hk_map is None:
        print("  [WARN] 港股实时数据获取失败，跳过")
    else:
        print(f"  [OK] {len(hk_map)} entries in HK snapshot\n")
        for cfg in HK_SPOT_CONFIGS:
            key     = cfg["key"]
            hk_code = cfg["hk_code"]
            name    = cfg["name"]
            row = hk_map.get(hk_code)
            if row is None:
                print(f"  [{name}] NOT FOUND (hk_code={hk_code})")
                print(f"    Available codes: {list(hk_map.keys())[:10]}")
                failed.append(key)
                continue
            price      = _safe_float(row.get("最新价"))
            change_pct = _safe_float(row.get("涨跌幅"))
            # stock_hk_index_spot_sina 无成交额列
            turnover   = None
            prev_close = _safe_float(row.get("昨收"))
            kind, spot_date, why = classify_snapshot(
                cur, key, price, prev_close, turnover, today, "港股")
            if kind is None:
                print(f"  {name:8s}  price={price}  跳过：{why}")
                continue
            print(f"  {name:8s}  price={price}  chg={change_pct}%  昨收={prev_close}  "
                  f"[{kind} @ {spot_date}]")
            rows.append((key, price, change_pct, turnover, prev_close, spot_date, kind))

    cur.close()

    # ── 写库 ─────────────────────────────────────────────────────────────────
    try:
        db_upsert_spot(conn, rows)
        conn.commit()
        print(f"\n[OK] Upserted {len(rows)} spot records.")
    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] DB error: {exc}")
        import traceback; traceback.print_exc()
        conn.close()
        return 1
    finally:
        conn.close()

    if failed:
        print(f"[WARN] {len(failed)} symbols not found: {', '.join(failed)}")
        return 1

    print(f"\n[SUMMARY] {len(rows)} index spots updated for {today}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
