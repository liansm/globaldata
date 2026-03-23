#!/usr/bin/env python3
"""
Commodity Intraday Minutes Fetcher
====================================
Fetches 1-minute OHLCV bars for domestic futures main contracts via
akshare.futures_zh_minute_sina(symbol) and upserts into commodity_minutes.

Strategy
--------
1. For each tracked domestic commodity, call futures_zh_spot to find the
   main contract (highest-volume row) and read its contract code.
2. Call futures_zh_minute_sina(symbol=<contract_code>) for intraday bars.
3. Upsert only today's bars (to avoid overwriting historical data with stale
   after-hours ticks from rolled contracts).

Run this script any time during or after market hours (09:00–15:30 CST).
"""

import os
import sys
from datetime import date, datetime

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

# commodity_key → Chinese product name for futures_zh_spot
DOMESTIC_MAP = {
    "copper":            "铜",
    "aluminum":          "铝",
    "lithium_carbonate": "碳酸锂",
    "methanol":          "甲醇",
    "urea":              "尿素",
    "meg":               "乙二醇",
    "styrene":           "苯乙烯",
    "polypropylene":     "聚丙烯",
    "natural_rubber":    "橡胶",
}


def _safe_float(val) -> float | None:
    try:
        f = float(str(val).replace(",", "").strip())
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Step 1: determine main contract code for each product
# ---------------------------------------------------------------------------

def get_main_contract_code(product_name: str) -> str | None:
    """
    Call futures_zh_spot to find the main (highest-volume) contract code.
    Returns a lowercase contract code like 'cu2505', or None on failure.
    """
    try:
        df = ak.futures_zh_spot(symbol=product_name, market="CF", adjust="0")
    except Exception as exc:
        print(f"    [WARN] futures_zh_spot({product_name}): {exc}")
        return None

    if df is None or df.empty:
        return None

    # Find contract code column (first column, or one containing "代码"/"合约")
    code_col = next(
        (c for c in df.columns if "代码" in c or "合约" in c),
        df.columns[0]
    )
    # Find volume column to pick main contract
    vol_col = next((c for c in df.columns if "成交量" in c), None)

    if vol_col:
        df = df.copy()
        df[vol_col] = pd.to_numeric(df[vol_col], errors="coerce")
        main_row = df.loc[df[vol_col].idxmax()]
    else:
        main_row = df.iloc[0]

    code = str(main_row[code_col]).strip().lower()
    return code if code and code != "nan" else None


# ---------------------------------------------------------------------------
# Step 2: fetch minute bars for a contract
# ---------------------------------------------------------------------------

def fetch_minutes(contract_code: str) -> pd.DataFrame | None:
    """
    Fetch 1-minute OHLCV bars via futures_zh_minute_sina.
    Returns a cleaned DataFrame with columns [dt, open, high, low, close, volume, turnover]
    or None on failure.
    """
    try:
        df = ak.futures_zh_minute_sina(symbol=contract_code)
    except Exception as exc:
        print(f"    [ERROR] futures_zh_minute_sina({contract_code}): {exc}")
        return None

    if df is None or df.empty:
        print(f"    [WARN] {contract_code}: empty minute data")
        return None

    # Detect datetime column (usually '时间' or '日期')
    dt_col = next(
        (c for c in df.columns if "时间" in c or "日期" in c or "date" in c.lower()),
        df.columns[0]
    )
    col_map = {
        "open":  next((c for c in df.columns if "开" in c), None),
        "high":  next((c for c in df.columns if "高" in c), None),
        "low":   next((c for c in df.columns if "低" in c), None),
        "close": next((c for c in df.columns if "收" in c or "价" in c), None),
        "volume": next((c for c in df.columns if "量" in c or "volume" in c.lower()), None),
        "turnover": next((c for c in df.columns if "额" in c), None),
    }

    rows = []
    today_str = date.today().isoformat()

    for _, row in df.iterrows():
        raw_dt = row[dt_col]
        # Parse datetime — might be string or datetime object
        try:
            if isinstance(raw_dt, (pd.Timestamp, datetime)):
                dt = raw_dt
            else:
                raw_str = str(raw_dt).strip()
                # "2025-05-10 09:31:00" or "09:31:00" (time only)
                if len(raw_str) <= 8:
                    dt = datetime.strptime(f"{today_str} {raw_str}", "%Y-%m-%d %H:%M:%S")
                else:
                    dt = pd.to_datetime(raw_str)
        except Exception:
            continue

        # Only keep today's bars
        if dt.strftime("%Y-%m-%d") != today_str:
            continue

        rows.append({
            "dt":       dt,
            "open":     _safe_float(row[col_map["open"]])   if col_map["open"]     else None,
            "high":     _safe_float(row[col_map["high"]])   if col_map["high"]     else None,
            "low":      _safe_float(row[col_map["low"]])    if col_map["low"]      else None,
            "close":    _safe_float(row[col_map["close"]])  if col_map["close"]    else None,
            "volume":   _safe_float(row[col_map["volume"]]) if col_map["volume"]   else None,
            "turnover": _safe_float(row[col_map["turnover"]]) if col_map["turnover"] else None,
        })

    if not rows:
        return None
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Database schema + upsert
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS commodity_minutes (
    id            BIGSERIAL PRIMARY KEY,
    commodity_key VARCHAR(60) NOT NULL REFERENCES commodities(key) ON DELETE CASCADE,
    dt            TIMESTAMP NOT NULL,
    open          NUMERIC(20,6),
    high          NUMERIC(20,6),
    low           NUMERIC(20,6),
    close         NUMERIC(20,6),
    volume        NUMERIC(24,4),
    turnover      NUMERIC(24,4),
    UNIQUE (commodity_key, dt)
);
CREATE INDEX IF NOT EXISTS idx_commodity_minutes_key_dt
    ON commodity_minutes (commodity_key, dt DESC);
"""

UPSERT_SQL = """
INSERT INTO commodity_minutes
    (commodity_key, dt, open, high, low, close, volume, turnover)
VALUES %s
ON CONFLICT (commodity_key, dt) DO UPDATE SET
    open     = EXCLUDED.open,
    high     = EXCLUDED.high,
    low      = EXCLUDED.low,
    close    = EXCLUDED.close,
    volume   = EXCLUDED.volume,
    turnover = EXCLUDED.turnover
"""


def db_upsert_minutes(conn, commodity_key: str, bars: pd.DataFrame):
    rows = [
        (commodity_key, r["dt"], r["open"], r["high"],
         r["low"], r["close"], r["volume"], r["turnover"])
        for _, r in bars.iterrows()
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        print("  [OK] Connected. Schema ready.\n")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] Cannot connect: {exc}")
        return 1

    total_bars = 0
    failures = []

    for key, product_name in DOMESTIC_MAP.items():
        print(f"── {key} ({product_name}) ─────────────────────────────────")

        # 1. Get main contract code
        contract = get_main_contract_code(product_name)
        if not contract:
            print(f"    [SKIP] Could not determine main contract for {product_name}")
            failures.append(key)
            continue
        print(f"    main contract: {contract}")

        # 2. Fetch minute bars
        bars = fetch_minutes(contract)
        if bars is None or bars.empty:
            print(f"    [SKIP] No minute bars for {contract}")
            failures.append(key)
            continue
        print(f"    fetched {len(bars)} bars for today")

        # 3. Upsert
        try:
            n = db_upsert_minutes(conn, key, bars)
            print(f"    [DB] upserted {n} rows")
            total_bars += n
        except Exception as exc:
            conn.rollback()
            import traceback
            print(f"    [ERROR] DB upsert failed: {exc}")
            traceback.print_exc()
            failures.append(key)

    conn.close()

    print(f"\n[SUMMARY] {total_bars} minute bars upserted across "
          f"{len(DOMESTIC_MAP) - len(failures)} commodities.")
    if failures:
        print(f"[WARN] Failed: {', '.join(failures)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
