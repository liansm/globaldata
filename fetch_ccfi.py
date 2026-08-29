#!/usr/bin/env python3
"""
CCFI Freight Index Fetcher
==========================
Fetches 中国出口集装箱运价指数 (CCFI, China Containerized Freight Index)
from the Shanghai Shipping Exchange (上海航运交易所) and upserts it into
the existing market_indices / index_prices tables with market='航运'.

Data source
-----------
    GET https://www.sse.net.cn/index/singleIndex?indexType=ccfi

The page is **server-side rendered** — a plain requests.get() is enough, no
JS rendering and no login required. Each page returns TWO periods at once:

    航线 | 上期 <date> | 本期 <date> | 与上期比涨跌 (%)

so every run yields two data points per series (e.g. 2026-08-21 + 2026-08-28).

Frequency & history
-------------------
* CCFI is published every **Friday** (基期 1998-01-01 = 1000).
* Only the LATEST period is free. The multi-period JSON endpoint
  (GET /index/mutipleIndex) returns {"success": false, "message":
  "对不起你没有登陆!"} and POSTing a historical `date` to the single-period
  page returns an empty table — historical backfill is paywalled
  (subscription: CNY 15,000/year).
* Therefore this script **accumulates history by running weekly**. There is
  no shortcut: run it every Friday and the series grows one point per week.

Compliance note
---------------
上海航运交易所 states that SCFI/CCFI data is for perusal only; without
written permission it must not be reproduced, redistributed, or used for
commercial purposes. Fine for internal research — check before publishing.

Usage
-----
    python fetch_ccfi.py            # fetch + upsert
    python fetch_ccfi.py --dry-run  # fetch + print, no DB write
"""

import os
import re
import sys
from datetime import date

import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

SSE_URL = "https://www.sse.net.cn/index/singleIndex?indexType=ccfi"
SSE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9",
}
TIMEOUT = 25

# ---------------------------------------------------------------------------
# 航线配置
# 匹配依据：表格里每条航线都带英文服务名 (JAPAN SERVICE 等)，比中文更稳定。
# 综合指数是唯一没有英文名的行，用"综合"关键字识别。
# 注：香港航线(S05) 在航交所多期查询里存在，但官网当前表格已不再发布，
#     因此这里不配置；脚本只会写入页面上真实出现的行。
# ---------------------------------------------------------------------------
ROUTE_CONFIGS = [
    {"key": "ccfi_total",         "en": None,                            "name": "CCFI 综合指数", "symbol": "CCFI"},
    {"key": "ccfi_japan",         "en": "JAPAN SERVICE",                 "name": "CCFI 日本航线", "symbol": "CCFI-S01"},
    {"key": "ccfi_europe",        "en": "EUROPE SERVICE",                "name": "CCFI 欧洲航线", "symbol": "CCFI-S02"},
    {"key": "ccfi_wc_america",    "en": "W/C AMERICA SERVICE",           "name": "CCFI 美西航线", "symbol": "CCFI-S03"},
    {"key": "ccfi_ec_america",    "en": "E/C AMERICA SERVICE",           "name": "CCFI 美东航线", "symbol": "CCFI-S04"},
    {"key": "ccfi_korea",         "en": "KOREA SERVICE",                 "name": "CCFI 韩国航线", "symbol": "CCFI-S06"},
    {"key": "ccfi_se_asia",       "en": "SOUTHEAST ASIA SERVICE",        "name": "CCFI 东南亚航线", "symbol": "CCFI-S07"},
    {"key": "ccfi_med",           "en": "MEDITERRANEAN SERVICE",         "name": "CCFI 地中海航线", "symbol": "CCFI-S08"},
    {"key": "ccfi_anz",           "en": "AUSTRALIA/NEW ZEALAND SERVICE", "name": "CCFI 澳新航线", "symbol": "CCFI-S09"},
    {"key": "ccfi_south_africa",  "en": "SOUTH AFRICA SERVICE",          "name": "CCFI 南非航线", "symbol": "CCFI-S12"},
    {"key": "ccfi_south_america", "en": "SOUTH AMERICA SERVICE",         "name": "CCFI 南美航线", "symbol": "CCFI-S13"},
    {"key": "ccfi_we_africa",     "en": "WEST EAST AFRICA SERVICE",      "name": "CCFI 东西非航线", "symbol": "CCFI-S11"},
    {"key": "ccfi_persian_gulf",  "en": "PERSIAN GULF/RED SEA SERVICE",  "name": "CCFI 波红航线", "symbol": "CCFI-S14"},
]

# en 名称 → 配置（综合指数用 "综合" 关键字单独处理）
_EN_TO_CFG = {c["en"].upper(): c for c in ROUTE_CONFIGS if c["en"]}

MARKET = "航运"
UNIT   = "点"

# ---------------------------------------------------------------------------
# Database
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

# CCFI 是周频单值指数，没有开高低/成交量，只写 close
UPSERT_PRICES_SQL = """
INSERT INTO index_prices (index_key, price_date, close)
VALUES %s
ON CONFLICT (index_key, price_date) DO UPDATE SET
    close = EXCLUDED.close
"""


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def _clean(cell: str) -> str:
    """Strip tags and collapse whitespace: '<p>日本航线</p><p>(JAPAN SERVICE)</p>' → '日本航线 (JAPAN SERVICE)'"""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", cell)).strip()


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        s = str(val).replace(",", "").strip()
        if not s:
            return None
        return round(float(s), 4)
    except (TypeError, ValueError):
        return None


def fetch_page() -> str:
    """Fetch the SSE single-period CCFI page (server-side rendered HTML)."""
    resp = requests.get(SSE_URL, headers=SSE_HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def parse_ccfi(html: str) -> dict:
    """
    Parse the CCFI table.

    Returns
    -------
    {
      "prev_date": "2026-08-21",
      "curr_date": "2026-08-28",
      "rows": [{"key": ..., "name": ..., "symbol": ..., "prev": float, "curr": float, "chg_pct": float}, ...]
    }

    Raises ValueError if the table cannot be located or contains no data.
    """
    table = None
    for m in re.finditer(r"<table[^>]*>.*?</table>", html, re.S):
        if "本期" in m.group(0):
            table = m.group(0)
            break
    if table is None:
        raise ValueError("未找到 CCFI 数据表格（页面结构可能已变更）")

    hdr = re.search(r"<td>上期<br>([\d-]+)</td>\s*<td>本期<br>([\d-]+)</td>", table, re.S)
    if not hdr:
        raise ValueError("未解析到上期/本期日期")
    prev_date, curr_date = hdr.group(1), hdr.group(2)

    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
        cells = [_clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if len(cells) != 4:
            continue
        label, prev, curr, chg = cells

        # 表头行的 prev/curr 是 "上期 2026-08-21" 这类文本，转不成 float，直接跳过
        prev_v, curr_v = _safe_float(prev), _safe_float(curr)
        if prev_v is None and curr_v is None:
            continue

        # 匹配航线配置
        if "综合" in label:
            cfg = ROUTE_CONFIGS[0]
        else:
            m = re.search(r"\(([^)]+)\)", label)
            cfg = _EN_TO_CFG.get(m.group(1).strip().upper()) if m else None
        if cfg is None:
            print(f"  [WARN] 未识别的航线，跳过: {label}")
            continue

        rows.append({
            "key":     cfg["key"],
            "name":    cfg["name"],
            "symbol":  cfg["symbol"],
            "prev":    prev_v,
            "curr":    curr_v,
            "chg_pct": _safe_float(chg),
        })

    if not rows:
        raise ValueError("表格中未解析到任何航线数据")
    return {"prev_date": prev_date, "curr_date": curr_date, "rows": rows}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    dry_run = "--dry-run" in sys.argv

    print("── 抓取上海航运交易所 CCFI ──────────────────────────────────────────")
    try:
        html = fetch_page()
    except Exception as exc:
        print(f"  [FATAL] 请求失败: {exc}")
        return 1

    try:
        data = parse_ccfi(html)
    except ValueError as exc:
        print(f"  [FATAL] 解析失败: {exc}")
        return 1

    prev_date, curr_date, rows = data["prev_date"], data["curr_date"], data["rows"]
    print(f"  上期 {prev_date}   本期 {curr_date}")
    print(f"  解析到 {len(rows)} 条航线\n")

    for r in rows:
        chg = f"{r['chg_pct']:+.1f}%" if r["chg_pct"] is not None else "—"
        print(f"  {r['name']:18s}  上期 {str(r['prev']):>9s}  →  本期 {str(r['curr']):>9s}   {chg}")

    # 展开成 (index_key, price_date, close) 三条：上期 + 本期
    entries: list = []
    for r in rows:
        if r["prev"] is not None:
            entries.append((r["key"], prev_date, r["prev"]))
        if r["curr"] is not None:
            entries.append((r["key"], curr_date, r["curr"]))

    if dry_run:
        print(f"\n[DRY-RUN] 跳过写库。将写入 {len(entries)} 行（{len(rows)} 条航线 × 2 期）。")
        return 0

    print("\n连接数据库...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接: {exc}")
        return 1

    try:
        with conn.cursor() as cur:
            seen: set = set()
            for r in rows:
                if r["key"] in seen:
                    continue
                seen.add(r["key"])
                cur.execute(UPSERT_INDEX_SQL, (r["key"], r["symbol"], r["name"], MARKET, UNIT))
            print(f"  [OK] market_indices 就绪（{len(seen)} 条航线, market='{MARKET}'）")

            from psycopg2.extras import execute_values
            execute_values(cur, UPSERT_PRICES_SQL, entries)
            print(f"  [OK] index_prices 写入 {len(entries)} 行 "
                  f"({prev_date} + {curr_date})")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()

    print(f"\n[OK] CCFI 更新完成：本期 {curr_date}，共 {len(rows)} 条航线。")
    print("    提示：CCFI 每周五发布，历史只能靠每周运行累积，无法回补。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
