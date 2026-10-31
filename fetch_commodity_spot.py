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

* Each row carries ``snapshot_kind``: 'intraday'（盘中/夜盘实时）or
  'close'（收市静态价，日期取最近交易日）。**周末不再把周五夜盘价标成
  「今天实时」** —— 见 classify_snapshot()。
"""

import os
import sys
from datetime import date, datetime
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
# ⚠ 2026-10-31 查实：这张表原来**大面积串种**，因为 mark 是手写猜的，而官方
#   `futures_symbol_mark()` 里同名不存在的 mark 恰好会撞上别家品种，且错得自洽
#   —— prev_close 同错，涨跌幅看着毫无异常，只有拿它和日线序列比量级才看得出来：
#       mh_qh 官方=棉花（甲醇显示 15,275 元/吨 = 棉花价，真甲醇 3,480）
#       dy_qh=豆油（乙二醇 9,073）  bst_qh=白糖（苯乙烯 5,245）
#       lq_qh=沥青（橡胶 5,304）    cj_qh=纯碱（尿素 972）
#       xin/xj/nie_qh、yumi/douya/jidan/shengzhu/mianhua/tang_qh 在官方表里根本不存在
#   → 现在每个 mark 都按 `ak.futures_symbol_mark()` 的实际输出核对过；
#     另外 fetch_domestic() 里加了「与日线量级对照」的闸门，串种会当场被拦下。
DOMESTIC_MARK_MAP = {
    "copper":            "tong_qh",   # 沪铜  SHFE
    "aluminum":          "lv_qh",     # 沪铝  SHFE
    "lithium_carbonate": "lc_qh",     # 碳酸锂 GFEX
    "polysilicon":       "ps_qh",     # 多晶硅 GFEX
    "methanol":          "zc_qh",     # 郑醇（甲醇）CZCE —— 官方表里叫「郑醇」
    "urea":              "ns_qh",     # 尿素  CZCE
    "meg":               "yec_qh",    # 乙二醇 DCE
    "styrene":           "byx_qh",    # 苯乙烯 DCE
    "polypropylene":     "jbx_qh",    # PP  DCE
    "natural_rubber":    "xj_qh",     # 橡胶  SHFE
    "zinc":              "xing_qh",   # 沪锌  SHFE
    "lead":              "qian_qh",   # 沪铅  SHFE
    "tin":               "xi_qh",     # 沪锡  SHFE
    "nickel":            "ni_qh",     # 沪镍  SHFE
    "corn":              "hym_qh",    # 玉米  DCE
    "soybean_oil":       "dy_qh",     # 豆油  DCE
    "eggs":              "jd_qh",     # 鸡蛋  DCE
    "live_hog":          "lh_qh",     # 生猪  DCE
    "cotton":            "mh_qh",     # 棉花  CZCE
    "sugar":             "bst_qh",    # 白糖  CZCE
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
# Snapshot kind: intraday vs close
# ---------------------------------------------------------------------------
# 国内期货交易时段（Asia/Shanghai）：
#   日盘 09:00–15:05（留收盘余量）；夜盘 21:00–次日 02:30
# 夜盘属于**下一个**交易日，所以凌晨抓到的快照其日期是「前一交易日」。
DAY_OPEN, DAY_CLOSE = 9 * 60, 15 * 60 + 5        # 540, 905
NIGHT_OPEN          = 21 * 60                     # 1260
NIGHT_CLOSE         = 2 * 60 + 30                 # 150


def _last_trading_date(conn, commodity_key: str, today: date):
    """该品种最近一个已收盘的交易日（prices 表最新日期，不晚于 today）。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(price_date) FROM prices "
            "WHERE commodity_key = %s AND price_date <= %s",
            (commodity_key, today),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] else today


def classify_snapshot(conn, commodity_key: str, now: datetime | None = None):
    """判断这条商品快照是「盘中/夜盘实时」还是「收市静态价」；返回 (kind, spot_date, why)。

    ⚠ 为什么需要这个（2026-10-31 定案，B 方案）
    原来这里直接 `spot_date = date.today()`，从不判断新旧。2026-10-31（周六）
    实测 commodity_spot 8/14 行是 `spot_date=2026-10-31`、`updated_at=11:34`，
    价格却是**周五夜盘收盘**（铜 107250 / 金 833）→ 卡片显示「今天 11:34 实时」，
    而期货根本没开市。股市那边早已有 `_is_today_snapshot()` 防护，商品这边漏了。

    现在按交易时段判定，并且不让周末的 `spot_date` 落到周六：

      * 日盘 / 夜盘里 → ``intraday``，日期 = 今天（凌晨夜盘用前一交易日）
      * 工作日已收盘 → ``close``，日期 = 今天
      * 盘前 / 周末 / 节假日 → ``close``，日期 = 最近交易日（prices 表最新日期）

    这样周末卡片显示「2026-10-30 · 收盘」，价格是真实的最近收盘价，而不是
    「2026-10-31 11:34 · 实时」。接口层（/api/commodities）再用 kind 决定
    是拿它当现价还是回落日线。

    不判节假日：节假日会走「非交易日」分支，日期取 prices 最新（= 节前最后交易日），
    结果依然正确 —— 这正是靠 prices 定日期而不是靠日历的好处。
    """
    now = now or datetime.now()
    today = now.date()
    weekday = now.weekday()          # Mon=0 … Sun=6
    m = now.hour * 60 + now.minute

    # 日盘：周一~周五
    if weekday <= 4 and DAY_OPEN <= m <= DAY_CLOSE:
        return "intraday", today, ""
    # 夜盘前半夜：周一~周五 21:00 起
    if weekday <= 4 and m >= NIGHT_OPEN:
        return "intraday", today, ""
    # 夜盘后半夜：周二~周六 00:00–02:30（属前一交易日）
    if 1 <= weekday <= 5 and m <= NIGHT_CLOSE:
        return "intraday", _last_trading_date(conn, commodity_key, today), "夜盘（属前一交易日）"

    # 以下都是静态价（没有实时行情）
    if weekday <= 4 and m > DAY_CLOSE:
        return "close", today, "已收盘"
    if weekday <= 4 and m < DAY_OPEN:
        return "close", _last_trading_date(conn, commodity_key, today), "盘前"
    return "close", _last_trading_date(conn, commodity_key, today), "非交易日"


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

def fetch_domestic(conn, mark_to_symbol: dict) -> dict:
    results = {}

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

        # ── 量级闸门：和该品种**自己的日线**比一下 ──────────────────────────
        # 串种的 mark 会拿到别家品种的价、且 prev_close 一起错，「涨跌幅」看不出毛病
        # （甲醇 15,275 / +0.13% 看着完全正常，实际是棉花）。期货主力与自家日线收盘
        # 相差通常 <5%，这里给 ±25% 的余量；超了就说明映射或单位不对，**不入库**。
        daily = fetch_prev_close_from_db(conn, key)
        if daily and daily > 0:
            ratio = price / daily
            if not (0.75 <= ratio <= 1.25):
                print(f"  [REJECT] {key:20s} symbol={symbol!r:10s} price={price} "
                      f"vs 日线 {daily} → 比值 {ratio:.2f}，疑似映射/单位错误，跳过")
                continue

        kind, spot_date, why = classify_snapshot(conn, key)
        results[key] = dict(
            price=price, change_pct=change_pct, change_amt=change_amt,
            prev_close=prev_close, volume=volume, turnover=None,
            spot_date=spot_date, snapshot_kind=kind,
        )
        ratio_txt = f"  (price/daily={price / daily:.2f})" if daily else ""
        print(f"  [OK] {key:20s}  symbol={symbol!r:10s}  "
              f"price={price}  chg={change_pct}%  昨结算={prev_close}  "
              f"[{kind} @ {spot_date}]{' ' + why if why else ''}{ratio_txt}")

    return results


# ---------------------------------------------------------------------------
# Fetch SGE spot data (domestic gold & silver)
# ---------------------------------------------------------------------------

# commodity_key → (SGE symbol, price_divisor)
# Au99.99 is quoted in yuan/gram  → divisor 1    (matches prices table unit)
# Ag99.99 is quoted in yuan/kg    → divisor 1000 (convert to yuan/gram)
SGE_MAP = {
    "gold":   ("Au99.99", 1),
    "silver": ("Ag99.99", 1000),
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

    for key, (sge_symbol, divisor) in SGE_MAP.items():
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
        raw_price = _safe_float(last.iloc[2])
        if raw_price is None:
            print(f"  [WARN] {key} ({sge_symbol}): no price in last row: {last.to_dict()}")
            continue
        price = round(raw_price / divisor, 6)  # normalise to prices table unit

        prev_close = fetch_prev_close_from_db(conn, key)
        change_pct = None
        change_amt = None
        if prev_close and prev_close != 0:
            change_amt = round(price - prev_close, 6)
            change_pct = round(change_amt / prev_close * 100, 4)

        kind, spot_date, why = classify_snapshot(conn, key)
        results[key] = dict(
            price=price, change_pct=change_pct, change_amt=change_amt,
            prev_close=prev_close, volume=None, turnover=None,
            spot_date=spot_date, snapshot_kind=kind,
        )
        print(f"  [OK] {key:8s}  symbol={sge_symbol}  "
              f"price={price}  昨收={prev_close}  chg={change_pct}%  "
              f"[{kind} @ {spot_date}]{' ' + why if why else ''}")

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
    snapshot_kind VARCHAR(8) DEFAULT 'intraday',
    updated_at    TIMESTAMPTZ DEFAULT NOW() NOT NULL
);
-- 老表补列（幂等）
ALTER TABLE commodity_spot ADD COLUMN IF NOT EXISTS snapshot_kind VARCHAR(8) DEFAULT 'intraday';
"""

UPSERT_SQL = """
INSERT INTO commodity_spot
    (commodity_key, price, change_pct, change_amt, prev_close, volume, turnover,
     spot_date, snapshot_kind, updated_at)
VALUES %s
ON CONFLICT (commodity_key) DO UPDATE SET
    price      = EXCLUDED.price,
    change_pct = EXCLUDED.change_pct,
    change_amt = EXCLUDED.change_amt,
    prev_close = EXCLUDED.prev_close,
    volume     = EXCLUDED.volume,
    turnover   = EXCLUDED.turnover,
    spot_date  = EXCLUDED.spot_date,
    snapshot_kind = EXCLUDED.snapshot_kind,
    updated_at = NOW()
"""


def db_upsert(conn, spots: dict):
    rows = [
        (k, d["price"], d["change_pct"], d["change_amt"], d["prev_close"],
         d["volume"], d["turnover"], d["spot_date"], d["snapshot_kind"])
        for k, d in spots.items()
    ]
    if not rows:
        print("[WARN] No rows to upsert.")
        return
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        execute_values(cur, UPSERT_SQL, rows,
                       template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())")
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
    spots = fetch_domestic(conn, mark_to_symbol)

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
