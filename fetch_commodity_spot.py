#!/usr/bin/env python3
"""
Commodity Real-Time Spot Fetcher
=================================
Fetches real-time price snapshots for tracked commodities and upserts into
the commodity_spot table (one row per commodity, overwritten each run).

Sources
-------
International futures  : futures_foreign_commodity_realtime(symbol)
Domestic futures       : futures_zh_spot(symbol, market='CF') with
                         futures_zh_realtime(symbol) as fallback
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

# ---------------------------------------------------------------------------
# Commodity → API symbol mappings
# ---------------------------------------------------------------------------

# commodity_key → futures_foreign_commodity_realtime symbol name
INTL_MAP = {
    "intl_gold":    "纽约黄金",
    "intl_silver":  "纽约白银",
    "intl_copper":  "纽约铜",
    "intl_alum":    "伦敦铝",
    "intl_oil_wti": "纽约原油",
    "intl_gas":     "纽约天然气",
}

# commodity_key → (Chinese product name for futures_zh_spot, exchange acronym)
DOMESTIC_MAP = {
    "copper":            ("铜",   "SHFE"),
    "aluminum":          ("铝",   "SHFE"),
    "lithium_carbonate": ("碳酸锂", "GFEX"),
    "methanol":          ("甲醇", "CZCE"),
    "urea":              ("尿素", "CZCE"),
    "meg":               ("乙二醇", "DCE"),
    "styrene":           ("苯乙烯", "DCE"),
    "polypropylene":     ("聚丙烯", "DCE"),
    "natural_rubber":    ("橡胶", "SHFE"),
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(str(val).replace(",", "").replace("%", "").strip())
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _find_col(row, *candidates):
    """Return first value from row whose column name is in candidates."""
    for c in candidates:
        v = row.get(c)
        if v is not None and str(v).strip() not in ("", "nan", "None"):
            return v
    return None


# ---------------------------------------------------------------------------
# International futures
# ---------------------------------------------------------------------------

def fetch_intl() -> dict:
    results = {}
    today = date.today().isoformat()

    for key, sina_name in INTL_MAP.items():
        try:
            df = ak.futures_foreign_commodity_realtime(symbol=sina_name)
        except Exception as exc:
            print(f"  [ERROR] {key} ({sina_name}): {exc}")
            continue

        if df is None or df.empty:
            print(f"  [WARN] {key} ({sina_name}): empty response")
            continue

        # Use first row (there's usually only one per symbol)
        row = df.iloc[0]

        price      = _safe_float(_find_col(row, "最新价", "现价", "price"))
        change_pct = _safe_float(_find_col(row, "涨跌幅", "涨跌幅%"))
        change_amt = _safe_float(_find_col(row, "涨跌额", "涨跌"))
        prev_close = _safe_float(_find_col(row, "昨收", "昨结算", "前结算"))
        volume     = _safe_float(_find_col(row, "成交量", "成交手数"))
        turnover   = _safe_float(_find_col(row, "成交额", "成交额(万元)"))
        # turnover might be in 万元
        if turnover is not None and "万元" in str(df.columns.tolist()):
            turnover *= 10_000

        if price is None:
            print(f"  [WARN] {key} ({sina_name}): no price in row {dict(row)}")
            continue

        results[key] = dict(price=price, change_pct=change_pct,
                            change_amt=change_amt, prev_close=prev_close,
                            volume=volume, turnover=turnover, spot_date=today)
        print(f"  [OK] {key:20s}  price={price}  chg={change_pct}%  昨收={prev_close}")

    return results


# ---------------------------------------------------------------------------
# Domestic futures
# ---------------------------------------------------------------------------

def _parse_domestic_df(df: pd.DataFrame, key: str, name: str) -> dict | None:
    """Extract spot snapshot from a futures_zh_spot / futures_zh_realtime DataFrame."""
    if df is None or df.empty:
        return None

    # Pick main contract: highest 成交量 row
    vol_col = next((c for c in df.columns if "成交量" in c), None)
    if vol_col:
        df = df.copy()
        df[vol_col] = pd.to_numeric(df[vol_col], errors="coerce")
        row = df.loc[df[vol_col].idxmax()]
    else:
        row = df.iloc[0]

    price      = _safe_float(_find_col(row, "最新价", "现价"))
    change_pct = _safe_float(_find_col(row, "涨跌幅"))
    change_amt = _safe_float(_find_col(row, "涨跌额", "涨跌"))
    prev_close = _safe_float(_find_col(row, "昨结算", "昨收", "前结算"))
    volume     = _safe_float(_find_col(row, "成交量(手)", "成交量", "成交手数"))

    turnover_raw = _safe_float(_find_col(row, "成交额(万元)", "成交额"))
    # Determine unit: if column contains "万元" it's in 万元
    turnover_col = next((c for c in df.columns
                         if "成交额" in c and "万" in c), None)
    turnover = turnover_raw * 10_000 if (turnover_raw is not None and turnover_col) else turnover_raw

    if price is None:
        print(f"  [WARN] {key} ({name}): no price. Cols: {list(df.columns)}")
        return None

    today = date.today().isoformat()
    return dict(price=price, change_pct=change_pct, change_amt=change_amt,
                prev_close=prev_close, volume=volume, turnover=turnover,
                spot_date=today)


def fetch_domestic() -> dict:
    results = {}

    for key, (name, exchange) in DOMESTIC_MAP.items():
        data = None

        # ── Try futures_zh_spot first ──────────────────────────────────────
        try:
            df = ak.futures_zh_spot(symbol=name, market="CF", adjust="0")
            data = _parse_domestic_df(df, key, name)
        except Exception as exc:
            print(f"  [INFO] {key} futures_zh_spot failed ({exc}), trying futures_zh_realtime...")

        # ── Fallback: futures_zh_realtime ──────────────────────────────────
        if data is None:
            try:
                df = ak.futures_zh_realtime(symbol=name)
                data = _parse_domestic_df(df, key, name)
            except Exception as exc2:
                print(f"  [ERROR] {key} futures_zh_realtime also failed: {exc2}")
                continue

        if data is None:
            continue

        results[key] = data
        print(f"  [OK] {key:20s}  price={data['price']}  chg={data['change_pct']}%  "
              f"昨结算={data['prev_close']}")

    return results


# ---------------------------------------------------------------------------
# Database upsert
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS commodity_spot (
    commodity_key VARCHAR(60) PRIMARY KEY REFERENCES commodities(key) ON DELETE CASCADE,
    price         NUMERIC(20,6),
    change_pct    NUMERIC(8,4),
    change_amt    NUMERIC(20,6),
    prev_close    NUMERIC(20,6),
    volume        NUMERIC(24,4),
    turnover      NUMERIC(24,4),
    spot_date     DATE,
    updated_at    TIMESTAMPTZ DEFAULT NOW() NOT NULL
);
"""

UPSERT_SQL = """
INSERT INTO commodity_spot
    (commodity_key, price, change_pct, change_amt, prev_close, volume, turnover, spot_date, updated_at)
VALUES %s
ON CONFLICT (commodity_key) DO UPDATE SET
    price      = EXCLUDED.price,
    change_pct = EXCLUDED.change_pct,
    change_amt = EXCLUDED.change_amt,
    prev_close = EXCLUDED.prev_close,
    volume     = EXCLUDED.volume,
    turnover   = EXCLUDED.turnover,
    spot_date  = EXCLUDED.spot_date,
    updated_at = NOW()
"""


def db_upsert(conn, spots: dict):
    rows = [
        (k, d["price"], d["change_pct"], d["change_amt"], d["prev_close"],
         d["volume"], d["turnover"], d["spot_date"])
        for k, d in spots.items()
    ]
    if not rows:
        print("[WARN] No rows to upsert.")
        return
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        execute_values(cur, UPSERT_SQL, rows,
                       template="(%s,%s,%s,%s,%s,%s,%s,%s,NOW())")
    conn.commit()
    print(f"\n[DB] Upserted {len(rows)} commodity spot rows.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("  [OK] Connected.\n")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] Cannot connect: {exc}")
        return 1

    spots: dict = {}

    print("── International futures (futures_foreign_commodity_realtime) ──────────")
    spots.update(fetch_intl())

    print("\n── Domestic futures (futures_zh_spot / futures_zh_realtime) ────────────")
    spots.update(fetch_domestic())

    print(f"\nTotal real-time data fetched: {len(spots)} commodities")

    try:
        db_upsert(conn, spots)
    except Exception as exc:
        conn.rollback()
        import traceback
        print(f"\n[FATAL] DB error: {exc}")
        traceback.print_exc()
        conn.close()
        return 1
    finally:
        conn.close()

    print(f"\n[SUMMARY] {len(spots)} commodity spots updated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
