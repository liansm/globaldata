"""
A-Share Index Intraday (1-Minute) Data Fetcher

Fetches 1-minute bar data for A-share indices via akshare:
  ak.index_zh_a_hist_min_em(symbol, period='1', start_date, end_date)

Supported indices:
  000001 上证指数   399001 深证成指   399006 创业板指
  000688 科创50    899050 北证50

Persists results to PostgreSQL table `index_minutes`.
Run after market close (e.g. 15:30 CST) to capture the latest trading day.
"""

import os
import sys
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

# A-share indices only — strip exchange prefix, keep 6-digit code
ASTOCK_MINUTE_CONFIGS = [
    {"key": "idx_sh",   "name": "上证指数", "symbol": "000001"},
    {"key": "idx_sz",   "name": "深证成指", "symbol": "399001"},
    {"key": "idx_cyb",  "name": "创业板指", "symbol": "399006"},
    {"key": "idx_kc50", "name": "科创50",   "symbol": "000688"},
    {"key": "idx_bz50", "name": "北证50",   "symbol": "899050"},
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
# Fetch 1-minute bars for one A-share index
# ---------------------------------------------------------------------------
def fetch_minutes(cfg: dict, start: datetime, end: datetime) -> list | None:
    symbol = cfg["symbol"]
    label  = cfg["name"]

    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = end.strftime("%Y-%m-%d %H:%M:%S")

    try:
        df = ak.index_zh_a_hist_min_em(
            symbol=symbol,
            period="1",
            start_date=start_str,
            end_date=end_str,
        )
    except Exception as exc:
        print(f"  [ERROR] {label}: {exc}")
        return None

    if df is None or df.empty:
        print(f"  [ERROR] {label}: empty response")
        return None

    # Column mapping — akshare may return Chinese or English column names
    time_col = next((c for c in ["时间", "datetime", "date"] if c in df.columns), None)
    if time_col is None:
        print(f"  [ERROR] {label}: no time column. Got: {list(df.columns)}")
        return None

    col = lambda *names: next((c for c in names if c in df.columns), None)
    open_col     = col("开盘", "open")
    high_col     = col("最高", "high")
    low_col      = col("最低", "low")
    close_col    = col("收盘", "close")
    volume_col   = col("成交量", "volume", "vol")
    turnover_col = col("成交额", "amount", "turnover")

    entries = []
    for _, row in df.iterrows():
        dt_val = row.get(time_col)
        if dt_val is None:
            continue
        # Normalise to "YYYY-MM-DD HH:MM:SS" string
        if hasattr(dt_val, "strftime"):
            dt_str = dt_val.strftime("%Y-%m-%d %H:%M:%S")
        else:
            raw = str(dt_val).strip()
            dt_str = raw if len(raw) >= 19 else raw + ":00"

        entries.append({
            "dt":       dt_str,
            "open":     _safe_float(row.get(open_col))     if open_col     else None,
            "high":     _safe_float(row.get(high_col))     if high_col     else None,
            "low":      _safe_float(row.get(low_col))      if low_col      else None,
            "close":    _safe_float(row.get(close_col))    if close_col    else None,
            "volume":   _safe_float(row.get(volume_col))   if volume_col   else None,
            "turnover": _safe_float(row.get(turnover_col)) if turnover_col else None,
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

    # Fetch the last 7 calendar days to cover weekends / public holidays
    end   = datetime.now()
    start = end - timedelta(days=7)
    print(f"Fetching 1-minute bars from {start.date()} to {end.date()}...\n")

    saved = 0
    try:
        for cfg in ASTOCK_MINUTE_CONFIGS:
            print(f"  {cfg['name']} ({cfg['symbol']})...", end=" ", flush=True)
            entries = fetch_minutes(cfg, start, end)
            if entries is None:
                failed.append(cfg["key"])
                print()
                continue
            n = db_upsert_minutes(conn, cfg["key"], entries)
            # Find latest bar for summary
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
