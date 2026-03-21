"""
A-Share Index Intraday (1-Minute) Data Fetcher

Uses akshare.stock_zh_a_minute (Sina Finance) — NOT EastMoney.
  ak.stock_zh_a_minute(symbol, period='1', adjust='')
  Returns columns: day, open, high, low, close, volume, amount

Supported indices:
  sh000001 上证指数   sz399001 深证成指   sz399006 创业板指
  sh000688 科创50    bj899050 北证50

Run after market close (15:30 CST) to capture latest trading day.
"""

import os
import sys

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

# Full exchange-prefixed symbols — required by stock_zh_a_minute (Sina format)
ASTOCK_MINUTE_CONFIGS = [
    {"key": "idx_sh",   "name": "上证指数", "symbol": "sh000001"},
    {"key": "idx_sz",   "name": "深证成指", "symbol": "sz399001"},
    {"key": "idx_cyb",  "name": "创业板指", "symbol": "sz399006"},
    {"key": "idx_kc50", "name": "科创50",   "symbol": "sh000688"},
    {"key": "idx_bz50", "name": "北证50",   "symbol": "bj899050"},
]

# ---------------------------------------------------------------------------
# Database schema
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS index_minutes (
    id        BIGSERIAL    PRIMARY KEY,
    index_key VARCHAR(60)  NOT NULL REFERENCES market_indices(key) ON DELETE CASCADE,
    dt        TIMESTAMP    NOT NULL,
    open      NUMERIC(16,4),
    high      NUMERIC(16,4),
    low       NUMERIC(16,4),
    close     NUMERIC(16,4),
    volume    NUMERIC(24,4),
    turnover  NUMERIC(24,4),
    UNIQUE (index_key, dt)
);

CREATE INDEX IF NOT EXISTS idx_index_minutes_key_dt
    ON index_minutes (index_key, dt DESC);
"""


def db_init(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def db_upsert_minutes(conn, index_key, entries):
    if not entries:
        return 0
    rows = [
        (index_key, e["dt"], e.get("open"), e.get("high"),
         e.get("low"), e.get("close"), e.get("volume"), e.get("turnover"))
        for e in entries
    ]
    sql = """
    INSERT INTO index_minutes (index_key, dt, open, high, low, close, volume, turnover)
    VALUES %s
    ON CONFLICT (index_key, dt) DO UPDATE SET
        open     = EXCLUDED.open,
        high     = EXCLUDED.high,
        low      = EXCLUDED.low,
        close    = EXCLUDED.close,
        volume   = EXCLUDED.volume,
        turnover = EXCLUDED.turnover
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    return len(rows)


def _safe_float(val):
    try:
        f = float(val)
        return None if pd.isna(f) else round(f, 4)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Fetch 1-minute bars via Sina (stock_zh_a_minute)
# ---------------------------------------------------------------------------
def fetch_minutes(cfg: dict) -> list | None:
    symbol = cfg["symbol"]
    label  = cfg["name"]

    try:
        df = ak.stock_zh_a_minute(symbol=symbol, period="1", adjust="")
    except Exception as exc:
        print(f"[ERROR] {label}: {exc}")
        return None

    if df is None or df.empty:
        print(f"[ERROR] {label}: empty response")
        return None

    # Columns: day, open, high, low, close, volume, amount
    entries = []
    for _, row in df.iterrows():
        dt_val = row.get("day")
        if dt_val is None:
            continue
        dt_str = (dt_val.strftime("%Y-%m-%d %H:%M:%S")
                  if hasattr(dt_val, "strftime") else str(dt_val)[:19])

        entries.append({
            "dt":       dt_str,
            "open":     _safe_float(row.get("open")),
            "high":     _safe_float(row.get("high")),
            "low":      _safe_float(row.get("low")),
            "close":    _safe_float(row.get("close")),
            "volume":   _safe_float(row.get("volume")),
            "turnover": _safe_float(row.get("amount")),
        })

    return entries if entries else None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    failed = []

    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        db_init(conn)
        print("  [OK] Connected. Schema ready.\n")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] Cannot connect: {exc}")
        return 1

    saved = 0
    try:
        for cfg in ASTOCK_MINUTE_CONFIGS:
            print(f"  {cfg['name']} ({cfg['symbol']})...", end=" ", flush=True)
            entries = fetch_minutes(cfg)
            if entries is None:
                failed.append(cfg["key"])
                print()
                continue
            n = db_upsert_minutes(conn, cfg["key"], entries)
            latest = max(entries, key=lambda e: e["dt"])
            print(f"[OK] {n} bars  latest={latest['dt'][:16]}  close={latest.get('close')}")
            saved += 1

        conn.commit()
        print(f"\nCommitted {saved} index dataset(s).")

    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] {exc}")
        import traceback; traceback.print_exc()
        return 1
    finally:
        conn.close()

    if failed:
        print(f"\n[SUMMARY] {len(failed)} failed: {', '.join(failed)}")
        return 1

    print(f"\n[SUMMARY] All {saved} A-share indices fetched successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
