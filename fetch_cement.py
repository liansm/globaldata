#!/usr/bin/env python3
"""
Cement Price Fetcher (全国水泥价格 / CEMPI)
===========================================
Fetches China's national cement price data from 中国水泥网 (index.ccement.com)
and writes two series:

 1. cement_cempi — 全国水泥价格指数 CEMPI (点, 2009年=100)
    → market_indices (market='建材') + index_prices (只写 close)

 2. cement_po425 — P.O42.5 散装水泥全国均价 (元/吨)
    → commodities + prices

Data source
-----------
    POST https://index.ccement.com/index/priceindex/getPriceIndex   {"timeType": 5}
    POST https://index.ccement.com/index/priceindex/po425zsline     {"timeType": 5}

Both are plain form POSTs — no login, no API key, no JS rendering.
Response shape: {"Code":200,"Msg":"查找成功","Data":{...}}

timeType is the look-back window, NOT the granularity:
    1 = 近1月   2 = 近3月   3 = 近1年   4 = 近3年   5 = 全历史
(6 is not a real value — the server falls back to the 3 behaviour.)
We always ask for 5 (full history) and let the DB dedupe.

History depth (as of 2026-08)
-----------------------------
    getPriceIndex  → 3591 daily points, 2011-09-09 → today   (CEMPI 指数)
    po425zsline    → 3200 daily points, 2013-08-01 → today   (元/吨)

getPriceIndex 另含 mjc (水泥煤价差) 与 coal_price (动力煤指数)，
暂未入库（动力煤指数口径偏归一化、与 CEMPI 同源，已在 2026-08 经评估后移除）。
如需判断水泥企业毛利可后续单独评估 mjc。

Why not akshare / futures
-------------------------
* akshare has NO cement interface. Its `get_qhkc_index()` exposes
  symbol_dict['水泥指数'] = '1003', but that is 奇货可查 data and the backing
  endpoint qhkch.com/ajax/index_show.php now returns 404 — dead.
* There is no cement futures contract on SHFE / DCE / CZCE / GFEX, so the
  project's usual futures_zh_spot route does not apply. Cement is spot-only.
* 数字水泥网 jg.dcement.com/price/index/cn.aspx stopped updating — its last
  row is 2021-02-22 (153.85).

Incremental updates
-------------------
Queries MAX(price_date) per series first and only upserts strictly newer
rows, so a daily refresh writes a handful of rows instead of all ~6.8k.

Compliance note
---------------
中国水泥网 states that content is copyright 水泥网 (www.ccement.com);
reproduction requires attribution and commercial use requires a licence
(0571-85871519). Fine for an internal database — check before publishing.

Usage
-----
    python fetch_cement.py            # incremental fetch + upsert
    python fetch_cement.py --full     # ignore existing data, re-write all
    python fetch_cement.py --dry-run  # fetch + print summary, no DB write
"""

import os
import sys
from datetime import date

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

BASE_URL = "https://index.ccement.com"
URL_INDEX = BASE_URL + "/index/priceindex/getPriceIndex"   # CEMPI 指数
URL_PO425 = BASE_URL + "/index/priceindex/po425zsline"     # P.O42.5 均价

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
    "Referer": BASE_URL + "/",
    "X-Requested-With": "XMLHttpRequest",
}
TIMEOUT   = 60
TIME_TYPE = 5          # 全历史

# ---------------------------------------------------------------------------
# Series configuration
# ---------------------------------------------------------------------------
# CEMPI 指数 → market_indices / index_prices
INDEX_KEY   = "cement_cempi"
INDEX_NAME  = "水泥价格指数 CEMPI"
INDEX_SYM   = "CEMPI"
MARKET      = "建材"
INDEX_UNIT  = "点"

# P.O42.5 均价 → commodities / prices
CMDTY_KEY   = "cement_po425"
CMDTY_NAME  = "水泥 P.O42.5"
CMDTY_SYM   = "PO42.5"
CMDTY_UNIT  = "元/吨"
CMDTY_TYPE  = "散装全国市场参考基准价"
GRADE_TYPE  = "P.O42.5散装"

SOURCE_API  = "index.ccement.com"

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
UPSERT_INDEX_SQL = """
INSERT INTO market_indices (key, symbol, name, market, unit, updated_at)
VALUES (%s, %s, %s, %s, %s, NOW())
ON CONFLICT (key) DO UPDATE SET
    symbol     = EXCLUDED.symbol,
    name       = EXCLUDED.name,
    market     = EXCLUDED.market,
    unit       = EXCLUDED.unit,
    updated_at = NOW()
"""

# CEMPI 是单值指数（无开高低/成交量），只写 close
UPSERT_INDEX_PRICES_SQL = """
INSERT INTO index_prices (index_key, price_date, close)
VALUES %s
ON CONFLICT (index_key, price_date) DO UPDATE SET
    close = EXCLUDED.close
"""

UPSERT_COMMODITY_SQL = """
INSERT INTO commodities
    (key, symbol, commodity, unit, source_api, price_type, grade_type, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (key) DO UPDATE SET
    symbol     = EXCLUDED.symbol,
    commodity  = EXCLUDED.commodity,
    unit       = EXCLUDED.unit,
    source_api = EXCLUDED.source_api,
    price_type = EXCLUDED.price_type,
    grade_type = EXCLUDED.grade_type,
    updated_at = NOW()
"""

UPSERT_PRICES_SQL = """
INSERT INTO prices (commodity_key, price_date, price)
VALUES %s
ON CONFLICT (commodity_key, price_date) DO UPDATE SET
    price = EXCLUDED.price
"""

LOG_FETCH_SQL = """
INSERT INTO fetch_log (commodity_key, latest_date, latest_price, change_day)
VALUES (%s, %s, %s, %s)
"""


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------
def _post(url: str, payload: dict) -> dict:
    """POST a form and return the decoded JSON envelope."""
    resp = requests.post(url, data=payload, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_series() -> tuple:
    """
    Fetch both series.

    Returns
    -------
    (cempi_points, po425_points) where each is a list of
    (date_str, value) sorted ascending by date.

    Raises RuntimeError when the API reports a non-200 Code.
    """
    j = _post(URL_INDEX, {"timeType": TIME_TYPE})
    if j.get("Code") != 200:
        raise RuntimeError(f"getPriceIndex 返回 Code={j.get('Code')} Msg={j.get('Msg')}")
    d = j["Data"]
    cempi = list(zip(d["cement"]["dynamicIndexDate"], d["cement"]["dynamicIndexAll"]))

    j = _post(URL_PO425, {"timeType": TIME_TYPE})
    if j.get("Code") != 200:
        raise RuntimeError(f"po425zsline 返回 Code={j.get('Code')} Msg={j.get('Msg')}")
    p = j["Data"]
    po425 = list(zip(p["dynamicIndexDate"], p["dynamicIndex"]))

    return cempi, po425


def _to_float(val):
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        return None


def normalise(points: list) -> list:
    """Drop unparsable values, round floats, sort by date, dedupe by date."""
    out, seen = [], set()
    for d, v in points:
        fv = _to_float(v)
        if not d or fv is None or d in seen:
            continue
        seen.add(d)
        out.append((d, fv))
    out.sort(key=lambda x: x[0])
    return out


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def latest_date(cur, sql: str, key: str):
    cur.execute(sql, (key,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    full    = "--full"    in sys.argv

    print("── 抓取中国水泥网 全国水泥价格 ────────────────────────────────────────")
    try:
        cempi, po425 = fetch_series()
    except Exception as exc:
        print(f"  [FATAL] 请求失败: {exc}")
        return 1

    cempi = normalise(cempi)
    po425 = normalise(po425)

    if not cempi or not po425:
        print("  [FATAL] 未解析到任何数据")
        return 1

    print(f"  {INDEX_NAME:28s} {len(cempi):5d} 行  {cempi[0][0]} → {cempi[-1][0]}  末值 {cempi[-1][1]} {INDEX_UNIT}")
    print(f"  {CMDTY_NAME:28s} {len(po425):5d} 行  {po425[0][0]} → {po425[-1][0]}  末值 {po425[-1][1]} {CMDTY_UNIT}")

    if dry_run:
        print(f"\n[DRY-RUN] 跳过写库。CEMPI {len(cempi)} 行 / P.O42.5 {len(po425)} 行。")
        return 0

    print("\n连接数据库...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接: {exc}")
        return 1

    try:
        with conn.cursor() as cur:
            # ── 1. CEMPI 指数 → market_indices + index_prices ───────────────
            cur.execute(UPSERT_INDEX_SQL,
                        (INDEX_KEY, INDEX_SYM, INDEX_NAME, MARKET, INDEX_UNIT))
            print(f"  [OK] market_indices 就绪：{INDEX_KEY} (market='{MARKET}')")

            since = None if full else latest_date(
                cur, "SELECT MAX(price_date) FROM index_prices WHERE index_key = %s",
                INDEX_KEY)
            rows = [(INDEX_KEY, d, v) for d, v in cempi if since is None or d > str(since)]
            if rows:
                execute_values(cur, UPSERT_INDEX_PRICES_SQL, rows)
            print(f"  [OK] index_prices  写入 {len(rows)} 行"
                  + (f"（增量，已有至 {since}）" if since else "（全量）"))

            # ── 2. P.O42.5 均价 → commodities + prices ─────────────────────
            cur.execute(UPSERT_COMMODITY_SQL,
                        (CMDTY_KEY, CMDTY_SYM, CMDTY_NAME, CMDTY_UNIT,
                         SOURCE_API, CMDTY_TYPE, GRADE_TYPE))
            print(f"  [OK] commodities   就绪：{CMDTY_KEY} ({CMDTY_UNIT})")

            since = None if full else latest_date(
                cur, "SELECT MAX(price_date) FROM prices WHERE commodity_key = %s",
                CMDTY_KEY)
            rows = [(CMDTY_KEY, d, v) for d, v in po425 if since is None or d > str(since)]
            if rows:
                execute_values(cur, UPSERT_PRICES_SQL, rows)
            print(f"  [OK] prices        写入 {len(rows)} 行"
                  + (f"（增量，已有至 {since}）" if since else "（全量）"))

            change = None
            if len(po425) >= 2:
                change = round(po425[-1][1] - po425[-2][1], 4)
            cur.execute(LOG_FETCH_SQL, (CMDTY_KEY, po425[-1][0], po425[-1][1], change))

        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()

    print(f"\n[OK] 水泥数据更新完成：CEMPI {cempi[-1][1]} {INDEX_UNIT}、"
          f"P.O42.5 {po425[-1][1]} {CMDTY_UNIT}（{cempi[-1][0]}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
