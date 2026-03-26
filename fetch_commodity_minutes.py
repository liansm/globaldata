#!/usr/bin/env python3
"""
Commodity Intraday Minutes Fetcher
====================================
Fetches 1-minute OHLCV bars for domestic futures main contracts via
akshare.futures_zh_minute_sina(symbol) and upserts into commodity_minutes.

Strategy
--------
1. For each tracked domestic commodity, call futures_zh_realtime to find the
   main contract (highest open-interest row) and read its contract code.
   Some product names cause KeyError in futures_zh_realtime on Windows due to
   encoding mismatches in the internal symbol_mark dict — for those we look up
   the exact symbol string from futures_symbol_mark() using the ASCII mark value.
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

# commodity_key → (chinese_name, sina_mark_value_or_None)
# mark value is used for products whose Chinese name causes KeyError in
# futures_zh_realtime (encoding mismatch on Windows).  None = direct name works.
DOMESTIC_MAP = {
    "copper":            ("铜",    "tong_qh"),
    "aluminum":          ("铝",    "lv_qh"),
    "lithium_carbonate": ("碳酸锂", "lc_qh"),
    "polysilicon":       ("多晶硅", "ps_qh"),
    "methanol":          ("甲醇",   "mh_qh"),
    "urea":              ("尿素",   None),
    "meg":               ("乙二醇", None),
    "styrene":           ("苯乙烯", None),
    "polypropylene":     ("聚丙烯", "jbx_qh"),
    "natural_rubber":    ("橡胶",   None),
    "zinc":              ("锌",    "xin_qh"),
    "lead":              ("铅",    "qian_qh"),
    "tin":               ("锡",    "xi_qh"),
    "nickel":            ("镍",    "nie_qh"),
    "corn":              ("玉米",  "yumi_qh"),
    "soybean_oil":       ("豆油",  "douya_qh"),
    "eggs":              ("鸡蛋",  "jidan_qh"),
    "live_hog":          ("生猪",  "shengzhu_qh"),
    "cotton":            ("棉花",  "mianhua_qh"),
    "sugar":             ("白糖",  "tang_qh"),
}

# ---------------------------------------------------------------------------
# Symbol mark table (loaded once)
# ---------------------------------------------------------------------------

_mark_df: pd.DataFrame | None = None

def _get_mark_df() -> pd.DataFrame:
    global _mark_df
    if _mark_df is None:
        try:
            _mark_df = ak.futures_symbol_mark()
        except Exception as exc:
            print(f"  [WARN] futures_symbol_mark() failed: {exc}")
            _mark_df = pd.DataFrame(columns=["symbol", "mark"])
    return _mark_df


def _symbol_from_mark(mark: str) -> str | None:
    """Return the exact symbol string for a given ASCII mark value (encoding-safe)."""
    df = _get_mark_df()
    if df.empty:
        return None
    rows = df[df["mark"] == mark]
    return str(rows["symbol"].iloc[0]) if not rows.empty else None


def _safe_float(val) -> float | None:
    try:
        f = float(str(val).replace(",", "").strip())
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Step 1: determine main contract code for each product
# ---------------------------------------------------------------------------

def get_main_contract_code(product_name: str, mark: str | None) -> str | None:
    """
    Use futures_zh_realtime to list all active contracts for a product,
    pick the one with the highest open interest (position), and return its
    lowercase contract code (e.g. 'cu2506').

    For products whose Chinese name causes a KeyError in futures_zh_realtime
    (encoding mismatch on Windows), supply the ASCII mark value so we can
    look up the exact symbol string from futures_symbol_mark().
    """
    # Resolve the symbol string to pass to futures_zh_realtime
    if mark:
        symbol = _symbol_from_mark(mark)
        if not symbol:
            print(f"    [WARN] mark '{mark}' not found in futures_symbol_mark(); "
                  f"falling back to '{product_name}'")
            symbol = product_name
    else:
        symbol = product_name

    try:
        df = ak.futures_zh_realtime(symbol=symbol)
    except Exception as exc:
        print(f"    [WARN] futures_zh_realtime({symbol!r}): {exc}")
        return None

    if df is None or df.empty:
        print(f"    [WARN] futures_zh_realtime({symbol!r}): empty")
        return None

    # Sort by open interest (position) descending → main contract is first row
    if "position" in df.columns:
        df = df.copy()
        df["position"] = pd.to_numeric(df["position"], errors="coerce")
        df = df.sort_values("position", ascending=False)

    code = str(df.iloc[0]["symbol"]).strip().lower()
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

    # Detect datetime column — futures_zh_minute_sina uses 'datetime' (English)
    dt_col = next(
        (c for c in df.columns if c == "datetime" or "时间" in c or "日期" in c
         or "date" in c.lower()),
        df.columns[0]
    )
    # Support both English (futures_zh_minute_sina) and Chinese column names
    col_map = {
        "open":     next((c for c in df.columns
                          if c == "open"  or "开" in c), None),
        "high":     next((c for c in df.columns
                          if c == "high"  or "高" in c), None),
        "low":      next((c for c in df.columns
                          if c == "low"   or "低" in c), None),
        "close":    next((c for c in df.columns
                          if c == "close" or "收" in c or "价" in c), None),
        "volume":   next((c for c in df.columns
                          if c == "volume" or "量" in c), None),
        "turnover": next((c for c in df.columns
                          if c in ("amount", "turnover") or "额" in c), None),
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

        # Filter out invalid/non-trading times (e.g. midnight 00:00:00 artifacts)
        # Domestic futures trade 09:00-15:30 and 21:00-02:30 only
        hhmm = dt.hour * 100 + dt.minute
        if not (900 <= hhmm <= 1530 or hhmm >= 2100 or hhmm <= 230):
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

    for key, (product_name, mark) in DOMESTIC_MAP.items():
        print(f"── {key} ({product_name}) ─────────────────────────────────")

        # 1. Get main contract code via futures_zh_realtime
        contract = get_main_contract_code(product_name, mark)
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
