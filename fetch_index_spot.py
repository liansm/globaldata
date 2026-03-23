"""
A-Share Index Spot Data Fetcher

Uses akshare.stock_zh_index_spot_sina — real-time snapshot for all A-share indices.
  ak.stock_zh_index_spot_sina()
  Returns columns: 代码, 名称, 最新价, 涨跌额, 涨跌幅, 成交量, 成交额, 振幅, 最高, 最低, 今开, 昨收
  成交额 unit: 元，与 index_prices.turnover 单位一致，直接存储

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

# Maps index_key → sina symbol code (e.g. "sh000001")
SPOT_CONFIGS = [
    {"key": "idx_sh",    "symbol": "sh000001", "name": "上证指数"},
    {"key": "idx_sz",    "symbol": "sz399001", "name": "深证成指"},
    {"key": "idx_hs300", "symbol": "sh000300", "name": "沪深300"},
    {"key": "idx_cyb",   "symbol": "sz399006", "name": "创业板指"},
    {"key": "idx_kc50",  "symbol": "sh000688", "name": "科创50"},
    # 北证50 (bj899050) 不在 stock_zh_index_spot_sina 覆盖范围内，跳过
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
# Fetch spot snapshot
# ---------------------------------------------------------------------------
def fetch_spot() -> dict | None:
    """Call stock_zh_index_spot_sina and return dict keyed by sina code."""
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
        if not code:
            continue
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

    print("Fetching stock_zh_index_spot_sina...")
    spot_map = fetch_spot()
    if spot_map is None:
        conn.close()
        return 1
    print(f"  [OK] {len(spot_map)} indices in snapshot\n")

    today = date.today().isoformat()
    rows = []
    failed = []

    for cfg in SPOT_CONFIGS:
        key    = cfg["key"]
        symbol = cfg["symbol"]
        name   = cfg["name"]

        row = spot_map.get(symbol)
        if row is None:
            print(f"  [{name}] NOT FOUND in snapshot (symbol={symbol})")
            failed.append(key)
            continue

        price      = _safe_float(row.get("最新价"))
        change_pct = _safe_float(row.get("涨跌幅"))   # already in %, e.g. 1.23
        turnover   = _safe_float(row.get("成交额"))    # 元
        prev_close = _safe_float(row.get("昨收"))

        print(f"  {name:8s}  price={price}  chg={change_pct}%  turnover={turnover}  昨收={prev_close}")
        rows.append((key, price, change_pct, turnover, prev_close, today))

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

    print(f"\n[SUMMARY] All {len(rows)} A-share index spots updated for {today}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
