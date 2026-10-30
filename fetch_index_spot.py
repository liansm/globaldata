"""
Index Spot Data Fetcher (A股 + 港股)

A股: akshare.stock_zh_index_spot_sina()
  Columns: 代码, 名称, 最新价, 涨跌额, 涨跌幅, 成交量, 成交额, 振幅, 最高, 最低, 今开, 昨收
  成交额 unit: 元，直接存储；matched by 代码 (e.g. "sh000001")

港股: akshare.stock_hk_index_spot_sina()
  Columns: 指数(code), 名称, 最新价, 涨跌额, 涨跌幅, 昨收, 今开, 最高, 最低
  无成交额列；matched by 指数 code (e.g. "HSI", "HSCEI", "HSTECH")

Writes to index_spot (one row per index, upserted on every run).
Run any time during or after market hours for fresh data.
"""

import os
import sys
from datetime import date

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
    updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);
"""


def db_init(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def db_upsert_spot(conn, rows):
    """rows: list of (index_key, price, change_pct, turnover, prev_close, spot_date)"""
    if not rows:
        return
    sql = """
    INSERT INTO index_spot (index_key, price, change_pct, turnover, prev_close, spot_date)
    VALUES %s
    ON CONFLICT (index_key) DO UPDATE SET
        price      = EXCLUDED.price,
        change_pct = EXCLUDED.change_pct,
        turnover   = EXCLUDED.turnover,
        prev_close = EXCLUDED.prev_close,
        spot_date  = EXCLUDED.spot_date,
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


def _is_today_snapshot(cur, key, price, prev_close, turnover, today):
    """判断这条 spot 快照是不是「当日真实盘中数据」；返回 (bool, 原因)。

    sina 的 spot 接口不带日期字段。盘前（或隔天早上）抓到的「最新价 / 涨跌幅」
    其实是**上一交易日收盘**的静态快照，直接记成 today 会让卡片显示
    「2026-10-30 · 实时」而数字是 10-29 的 —— 比"数据不新"更难发现。

    判定顺序：
      1. 最新价 ≤ 0             → 源侧无行情（盘前 深证成指/创业板指 会返回 0）
      2. 最新价 与某历史收盘吻合 → 是那天的收盘快照，日线里已有同一数字
      3. A股 成交额 = 0         → 当日没成交，最新价是集合竞价参考价
                                   （实测上证 3651.4075 vs 昨收 3651.4340）
    以上三种都**不写库**，让页面回落到日线，避免出现「今天 · 实时」的假数据。
    """
    cur.execute(
        """SELECT price_date, close FROM index_prices
           WHERE index_key = %s ORDER BY price_date DESC LIMIT 3""",
        (key,),
    )
    hist = cur.fetchall()
    if price is None or price <= 0:
        return False, "源侧最新价为 0"
    for i, (d, close) in enumerate(hist):
        if d.isoformat() == today or close is None or close <= 0:
            continue
        # 1e-6 相对容差 + 0.01 绝对容差：指数点位允许四舍五入误差
        if abs(float(close) - price) > max(0.01, abs(price) * 1e-6):
            continue
        if prev_close is not None and i + 1 < len(hist) and hist[i + 1][1] is not None:
            if abs(float(hist[i + 1][1]) - prev_close) > max(0.01, abs(prev_close) * 1e-6):
                continue  # 昨收对不上，不是那天的快照
        return False, f"等于 {d} 收盘价（盘前/收盘快照，日线已有）"
    if turnover is not None and turnover <= 0:
        return False, "成交额为 0（未开盘，最新价是竞价参考价）"
    return True, ""


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
        ok, why = _is_today_snapshot(cur, key, price, prev_close, turnover, today)
        if not ok:
            print(f"  {name:8s}  price={price}  跳过：{why}")
            continue
        print(f"  {name:8s}  price={price}  chg={change_pct}%  turnover={turnover}  昨收={prev_close}")
        rows.append((key, price, change_pct, turnover, prev_close, today))

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
            ok, why = _is_today_snapshot(cur, key, price, prev_close, turnover, today)
            if not ok:
                print(f"  {name:8s}  price={price}  跳过：{why}")
                continue
            print(f"  {name:8s}  price={price}  chg={change_pct}%  昨收={prev_close}")
            rows.append((key, price, change_pct, turnover, prev_close, today))

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
