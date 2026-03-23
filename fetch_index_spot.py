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
            print(f"  {name:8s}  price={price}  chg={change_pct}%  昨收={prev_close}")
            rows.append((key, price, change_pct, turnover, prev_close, today))

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
