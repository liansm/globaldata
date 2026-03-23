#!/usr/bin/env python3
"""
Commodity Real-Time Spot Fetcher
=================================
Fetches real-time price snapshots for tracked domestic futures via
akshare.futures_zh_realtime() and upserts into commodity_spot.

Key design
----------
* futures_zh_realtime() looks up symbols via an internal dict built from
  futures_symbol_mark(). On Windows the Chinese strings in that dict may
  differ in encoding from literals typed in source code, causing KeyError.
  → Solution: call futures_symbol_mark() at startup, build mark→symbol map
    keyed by the ASCII mark value (e.g. "tong_qh"), then look up the exact
    symbol string from the DataFrame so the encoding always matches.

* futures_zh_realtime() returns English column names:
    trade         → latest price (最新价)
    presettlement → previous settlement (昨结算)
    changepercent → change % (涨跌幅), signed, e.g. 1.23 for +1.23%
    volume        → trading volume
    position      → open interest (pick max for main contract)

* futures_foreign_commodity_realtime() is broken in the current akshare
  version (Length mismatch). International commodities are skipped here;
  they already have daily close data from fetch_commodities.py.
"""

import os
import sys
from datetime import date
from typing import Optional

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

# ---------------------------------------------------------------------------
# commodity_key → akshare futures_symbol_mark "mark" value (ASCII, stable)
# ---------------------------------------------------------------------------
DOMESTIC_MARK_MAP = {
    "copper":            "tong_qh",   # 铜  SHFE
    "aluminum":          "lv_qh",     # 铝  SHFE
    "lithium_carbonate": "lc_qh",     # 碳酸锂  GFEX
    "methanol":          "mh_qh",     # 甲醇  CZCE
    "urea":              "cj_qh",     # 尿素  CZCE
    "meg":               "dy_qh",     # 乙二醇  DCE
    "styrene":           "bst_qh",    # 苯乙烯  CZCE / DCE
    "polypropylene":     "jbx_qh",    # 聚丙烯 (PP)  DCE
    "natural_rubber":    "lq_qh",     # 橡胶  SHFE
}


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(str(val).replace(",", "").strip())
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Build mark → exact symbol string (encoding-safe)
# ---------------------------------------------------------------------------

def build_mark_to_symbol() -> dict:
    """
    Call futures_symbol_mark() once and return {mark: symbol} dict.
    The symbol values come directly from akshare's DataFrame so their
    encoding always matches what futures_zh_realtime() expects.
    """
    try:
        df = ak.futures_symbol_mark()
        return dict(zip(df["mark"], df["symbol"]))
    except Exception as exc:
        print(f"  [ERROR] futures_symbol_mark(): {exc}")
        return {}


# ---------------------------------------------------------------------------
# Fetch domestic futures real-time data
# ---------------------------------------------------------------------------

def fetch_domestic(mark_to_symbol: dict) -> dict:
    results = {}
    today = date.today().isoformat()

    for key, mark in DOMESTIC_MARK_MAP.items():
        symbol = mark_to_symbol.get(mark)
        if symbol is None:
            print(f"  [WARN] {key}: mark '{mark}' not found in futures_symbol_mark()")
            continue

        try:
            df = ak.futures_zh_realtime(symbol=symbol)
        except Exception as exc:
            print(f"  [ERROR] {key} (symbol={symbol!r}): {exc}")
            continue

        if df is None or df.empty:
            print(f"  [WARN] {key}: empty response")
            continue

        # Pick main contract by highest open interest (position)
        if "position" in df.columns:
            df = df.copy()
            df["position"] = pd.to_numeric(df["position"], errors="coerce")
            row = df.loc[df["position"].idxmax()]
        else:
            row = df.iloc[0]

        # English column names returned by futures_zh_realtime
        price      = _safe_float(row.get("trade"))
        prev_close = _safe_float(row.get("presettlement") or row.get("prevsettlement")
                                 or row.get("preclose"))
        volume     = _safe_float(row.get("volume"))

        # changepercent is a decimal ratio (e.g. 0.0248 = +2.48%), multiply by 100
        _cp = _safe_float(row.get("changepercent"))
        change_pct = round(_cp * 100, 4) if _cp is not None else None

        # Compute change_pct from price / prev_close if not available
        if change_pct is None and price is not None and prev_close is not None and prev_close != 0:
            change_pct = round((price - prev_close) / prev_close * 100, 4)

        # change_amt
        change_amt = None
        if price is not None and prev_close is not None:
            change_amt = round(price - prev_close, 6)

        if price is None:
            print(f"  [WARN] {key} (symbol={symbol!r}): no price. cols={list(df.columns)}")
            continue

        results[key] = dict(
            price=price, change_pct=change_pct, change_amt=change_amt,
            prev_close=prev_close, volume=volume, turnover=None,
            spot_date=today,
        )
        print(f"  [OK] {key:20s}  symbol={symbol!r:10s}  "
              f"price={price}  chg={change_pct}%  昨结算={prev_close}")

    return results


# ---------------------------------------------------------------------------
# Fetch SGE spot data (domestic gold & silver)
# ---------------------------------------------------------------------------

# commodity_key → SGE symbol
SGE_MAP = {
    "gold":   "Au99.99",
    "silver": "Ag99.99",
}


def fetch_prev_close_from_db(conn, commodity_key: str) -> Optional[float]:
    """Query the most recent daily close price from the prices table."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT price FROM prices WHERE commodity_key = %s "
            "ORDER BY price_date DESC LIMIT 1",
            (commodity_key,)
        )
        row = cur.fetchone()
    if row and row[0] is not None:
        return float(row[0])
    return None


def fetch_sge(conn) -> dict:
    """
    Fetch real-time spot prices from Shanghai Gold Exchange via
    spot_quotations_sge().  Returns a series of minute bars for the
    current session; we take the last row as the latest price.
    prev_close is read from the local prices table (most recent daily close).
    """
    results = {}
    today = date.today().isoformat()

    for key, sge_symbol in SGE_MAP.items():
        try:
            df = ak.spot_quotations_sge(symbol=sge_symbol)
        except Exception as exc:
            print(f"  [ERROR] {key} spot_quotations_sge({sge_symbol}): {exc}")
            continue

        if df is None or df.empty:
            print(f"  [WARN] {key} ({sge_symbol}): empty response (outside trading hours?)")
            continue

        # Columns (positional, names are garbled on Windows):
        #   col[0] = 品种, col[1] = 时间, col[2] = 最新价, col[3] = 更新时间
        last = df.iloc[-1]
        price = _safe_float(last.iloc[2])
        if price is None:
            print(f"  [WARN] {key} ({sge_symbol}): no price in last row: {last.to_dict()}")
            continue

        prev_close = fetch_prev_close_from_db(conn, key)
        change_pct = None
        change_amt = None
        if prev_close and prev_close != 0:
            change_amt = round(price - prev_close, 6)
            change_pct = round(change_amt / prev_close * 100, 4)

        results[key] = dict(
            price=price, change_pct=change_pct, change_amt=change_amt,
            prev_close=prev_close, volume=None, turnover=None,
            spot_date=today,
        )
        print(f"  [OK] {key:8s}  symbol={sge_symbol}  "
              f"price={price}  昨收={prev_close}  chg={change_pct}%")

    return results


# ---------------------------------------------------------------------------
# Database schema + upsert
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

    print("Building futures symbol mark map...")
    mark_to_symbol = build_mark_to_symbol()
    print(f"  [OK] {len(mark_to_symbol)} symbols loaded.\n")

    print("── Domestic futures (futures_zh_realtime) ──────────────────────────────")
    spots = fetch_domestic(mark_to_symbol)

    print("\n── SGE spot (spot_quotations_sge) — 国内黄金/白银 ──────────────────────")
    spots.update(fetch_sge(conn))

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
