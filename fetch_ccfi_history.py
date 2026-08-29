#!/usr/bin/env python3
"""
CCFI Historical Backfill (GreenPacific / SSE)
=============================================
Backfills the full CCFI weekly history (composite + 12 route indices) into
market_indices / index_prices (market='航运').

Why GreenPacific
----------------
The Shanghai Shipping Exchange (SSE) only publishes the LATEST two weekly
periods for free; the full multi-year history is behind a paid subscription
(CNY 15,000/yr). microbell (中华航运网 mirror) does carry 2002+ history but
gates each individual series behind a login-walled viewer, so it cannot be
scraped without an account.

GreenPacific.org republishes the SSE CCFI composite + all 12 routes as a
Highcharts/wpDataTables widget whose full series data (dates + values for
every line) is embedded as JSON in the page HTML. That gives us a clean,
license-free weekly history:

    https://greenpacific.org/ccfi-china-containerized-freight-index/

Coverage: 2023-04-21 .. <latest Friday> (weekly), 13 lines.

Note on coverage
----------------
This is NOT the full 2002+ history (that stays paywalled). It is a solid,
weekly, multi-year backfill that makes the commodities-page CCFI chart useful.
Run weekly (e.g. via refresh_all.py) to keep it growing.

Usage
-----
    python fetch_ccfi_history.py            # fetch + upsert
    python fetch_ccfi_history.py --dry-run  # fetch + print, no DB write
"""

import os
import re
import sys
import json
from datetime import datetime

import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
SRC_URL = "https://greenpacific.org/ccfi-china-containerized-freight-index/"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
}
TIMEOUT = 30
MARKET = "航运"
UNIT = "点"

# GreenPacific series label -> project ROUTE_CONFIGS key
# (mirrors the keys in fetch_ccfi.py)
SERIES_MAP = {
    "COMPOSITE INDEX":          ("ccfi_total",       "CCFI 综合指数",   "CCFI"),
    "JAPAN":                     ("ccfi_japan",       "CCFI 日本航线",   "CCFI-S01"),
    "EUROPE":                    ("ccfi_europe",      "CCFI 欧洲航线",   "CCFI-S02"),
    "W/C AMERICA":               ("ccfi_wc_america",  "CCFI 美西航线",   "CCFI-S03"),
    "E/C AMERICA":               ("ccfi_ec_america",  "CCFI 美东航线",   "CCFI-S04"),
    "KOREA":                     ("ccfi_korea",       "CCFI 韩国航线",   "CCFI-S06"),
    "SOUTHEAST":                 ("ccfi_se_asia",     "CCFI 东南亚航线", "CCFI-S07"),
    "MEDITERRANEAN":             ("ccfi_med",         "CCFI 地中海航线", "CCFI-S08"),
    "AUSTRALIA/NEW ZEALAND":     ("ccfi_anz",         "CCFI 澳新航线",   "CCFI-S09"),
    "SOUTH AFRICA":              ("ccfi_south_africa","CCFI 南非航线",   "CCFI-S12"),
    "SOUTH AMERICA":             ("ccfi_south_america","CCFI 南美航线",  "CCFI-S13"),
    "WEST EAST AFRICA":          ("ccfi_we_africa",   "CCFI 东西非航线", "CCFI-S11"),
    "PERSIAN GULF/RED SEA":      ("ccfi_persian_gulf", "CCFI 波红航线",   "CCFI-S14"),
}

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

UPSERT_PRICES_SQL = """
INSERT INTO index_prices (index_key, price_date, close)
VALUES %s
ON CONFLICT (index_key, price_date) DO UPDATE SET
    close = EXCLUDED.close
"""


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
def _extract_render_data(html: str) -> dict:
    """
    Pull the wpDataCharts[501].render_data JSON object out of the page.

    The assignment looks like:
        wpDataCharts[501] = { render_data: {"wdtNumberFormat":"2","options":{...}} };
    The OUTER object uses an unquoted JS key (render_data:), so it is NOT valid
    JSON. Only the VALUE of render_data is real JSON (double-quoted keys). So we
    locate 'render_data' and parse the {...} object assigned to it.
    """
    marker = "wpDataCharts[501]"
    i = html.find(marker)
    if i < 0:
        raise ValueError("未找到 wpDataCharts[501]（页面结构可能已变更）")
    rd = html.find("render_data", i)
    if rd < 0:
        raise ValueError("未找到 render_data 字段")
    eq = html.find("{", rd)            # opening brace of the render_data JSON value
    if eq < 0:
        raise ValueError("未找到 render_data 起始 {")
    depth = 0
    end = -1
    for j in range(eq, len(html)):
        c = html[j]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = j
                break
    if end < 0:
        raise ValueError("render_data JSON 未闭合")
    obj = json.loads(html[eq:end + 1])   # this IS the render_data value object
    return obj["options"]


def _parse_dates(categories):
    """DD/MM/YYYY -> YYYY-MM-DD."""
    out = []
    for c in categories:
        c = c.strip()
        try:
            out.append(datetime.strptime(c, "%d/%m/%Y").strftime("%Y-%m-%d"))
        except ValueError:
            out.append(None)
    return out


def parse_ccfi_history(html: str) -> dict:
    """
    Returns { key: {"name":..,"symbol":..,"dates":[...],"values":[...]} }
    Skips series we can't map and dates we can't parse.
    """
    opts = _extract_render_data(html)
    series = opts.get("series", [])
    categories = opts.get("xAxis", {}).get("categories", [])
    dates = _parse_dates(categories)

    result = {}
    for s in series:
        label = (s.get("name") or s.get("label") or "").strip().upper()
        if label not in SERIES_MAP:
            print(f"  [WARN] 未识别的航线，跳过: {s.get('name')}")
            continue
        key, name, symbol = SERIES_MAP[label]
        data = s.get("data", [])
        pairs = []
        for d, v in zip(dates, data):
            if d is None:
                continue
            try:
                val = round(float(v), 4)
            except (TypeError, ValueError):
                continue
            pairs.append((d, val))
        if pairs:
            result[key] = {"name": name, "symbol": symbol, "pairs": pairs}
    if not result:
        raise ValueError("未解析到任何可识别的 CCFI 系列")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    dry_run = "--dry-run" in sys.argv
    print("── 抓取 GreenPacific CCFI 历史 ───────────────────────────────────")
    try:
        resp = requests.get(SRC_URL, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        resp.encoding = "utf-8"
    except Exception as exc:
        print(f"  [FATAL] 请求失败: {exc}")
        return 1

    try:
        series = parse_ccfi_history(resp.text)
    except ValueError as exc:
        print(f"  [FATAL] 解析失败: {exc}")
        return 1

    total = 0
    for key, info in series.items():
        d0 = info["pairs"][0][0]
        d1 = info["pairs"][-1][0]
        total += len(info["pairs"])
        print(f"  {info['name']:16s} ({key})  {len(info['pairs']):>4d} 点  "
              f"{d0} → {d1}  末值 {info['pairs'][-1][1]}")
    print(f"\n共 {len(series)} 条航线，{total} 个数据点。")

    if dry_run:
        print("\n[DRY-RUN] 跳过写库。")
        return 0

    print("连接数据库...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接: {exc}")
        return 1

    try:
        with conn.cursor() as cur:
            seen = set()
            for key, info in series.items():
                if key in seen:
                    continue
                seen.add(key)
                cur.execute(UPSERT_INDEX_SQL,
                           (key, info["symbol"], info["name"], MARKET, UNIT))
            print(f"  [OK] market_indices 就绪（{len(seen)} 条航线, market='{MARKET}'）")

            entries = []
            for key, info in series.items():
                for d, v in info["pairs"]:
                    entries.append((key, d, v))
            from psycopg2.extras import execute_values
            execute_values(cur, UPSERT_PRICES_SQL, entries)
            print(f"  [OK] index_prices 写入 {len(entries)} 行")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()

    print("\n[OK] CCFI 历史回补完成。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
