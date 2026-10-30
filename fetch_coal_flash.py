#!/usr/bin/env python3
"""
动力煤港口价 —— 太原煤炭交易中心「市场快讯」转载源（救 sxcoal 付费墙）
=====================================================================
背景（2026-10-29 实测）
-----------------------
原先的 **sxcoal.com 汾渭CCI** 源已 **彻底付费墙化**：
    https://www.sxcoal.com/hub/thermalcoal 的 __NEXT_DATA__.initialTabMap
    里，CCI 各品种（CCI5500/CCI5000/CCI进口…）的 `result` 字段全部返回 "*"
    （打码），chartConfig.data 是空数组 → 页面只剩「日期 + *」。
    库里 9 个 `coal_*` 序列因此停在 2026-09-30，10-01 起一行未入库。
    → sxcoal 这条线是**死路**，除非买订阅。

本脚本改用 **中国太原煤炭交易中心 (ctctc.cn)「市场快讯」**：该站每天免费转载
CCI 港口指数与 CCTD 环渤海参考价原文，数值未打码，服务端渲染（plain requests）。

Data source
-----------
    GET https://www.ctctc.cn/main/news/flashnewlist.jspx
        ?nodeid=323&kw=<关键词>&tag=&pageNumber=N     # 10 条/页

    ⚠ 带 `kw=` 是站内搜索，直接过滤出目标条目 —— 1 条/交易日，回补极快；
      不带 kw 是全站快讯（每天几十条），同样深度要多翻 5 倍页数。

    单条结构：
        <li>
          <div class="list-data">2026-10-29&nbsp;17:18</div>
          <div class="list-desc"><p>CCI港口指数继续小幅回调：10月29日，
            CCI港口指数日环比降5-6元/吨。其中，5500大卡1021元/吨、
            5000大卡918元/吨，均降5元/吨；4500大卡814元/吨，降6元/吨。</p></div>
        </li>

抓什么（四个 kw，各翻自己的页）
-------------------------------
1) kw=CCI  → 正文含「CCI港口」：汾渭 CCI 港口指数 5500/5000/4500
       → 写回**原有** key：coal_port_5500 / coal_port_5000 / coal_port_4500
       （与 sxcoal 时期同一指标；2026-08~10 与库内值重叠 33 个 (品种,日期)，
         逐条零差异 → 可无缝续接）

2) kw=CCTD → 正文含「CCTD环渤海」：CCTD 环渤海动力煤参考价 5000/4500
       → 覆盖 cctd_bohai_5000 / cctd_bohai_4500
       ⚠ 这两个 key 原先来自 CCTD 首页那块（10-23 记 703/632），而同日官方参考价
         是 934/836 —— 差 230+，明显不是同一口径（首页那块量级更像指数点数）。
         本脚本用官方参考价覆盖，把错误口径顶掉。
       5500 **不在本脚本写**：由 fetch_commodities.py 的 CCTD 历史 API 序列
         （2018 起、日更）负责，避免两个写入方（两者数值已核对一致）。

3) kw=广州港 → 进口动力煤**港口场地含税价**（人民币，非汾渭指数）
       澳大利亚 5500 大卡 → coal_gz_au_5500
       印尼     4500 大卡 → coal_gz_id_4500
       ⚠ 口径 ≠ 汾渭「CCI进口5500/4700」指数：这是**广州港单一港口、按来源国**
         的现货含税价，量级可比但序列不等价 → **新建 key，绝不续到 coal_import_* 上**。
       ⚠ 非每个交易日都发（只在调价时发），中间会断档。

4) kw=外购 → 神华巴图塔「大集团外购车板价」5500/5000/4500
       → coal_shenhua_5500 / coal_shenhua_5000 / coal_shenhua_4500
       ⚠ 口径 ≠ 汾渭产地 CCI：这是**单一大集团（神华）在巴图塔的采购挂牌价**，
         且只在调价时发（约 20 次/年）→ 新建 key，不续到 coal_ordos_* 上。

覆盖不到的（明确记录，不猜）
---------------------------
原 sxcoal 源这 9 条**专有指数**序列，ctctc 无同口径数据，维持停更（最后值 2026-09-30）：
    coal_import_5500 / 4700 / 3800            进口动力煤 CCI 指数（元/吨）
    coal_import_5500_fob / 4700_fob / 3800_fob  同上 FOB（美元/吨）
    coal_ordos_5500 / coal_datong_5500 / coal_yulin_5800  产地 CCI（元/吨）
本脚本用上面第 3、4 组**近似口径**新建 key 顶上，不是替换、不覆盖原序列。

Usage
-----
    python fetch_coal_flash.py                 # 增量：翻到库内最新日期为止
    python fetch_coal_flash.py --pages 10      # 强制往回翻 10 页（回补 2 个月够用）
    python fetch_coal_flash.py --only cci      # 只抓某一路（cci/cctd/gz/shenhua）
    python fetch_coal_flash.py --dry-run       # 只解析打印，不写库
    python fetch_coal_flash.py --status        # 只看库内现状

Compliance note
---------------
数值原始出处是汾渭能源 CCI / 中国煤炭市场网 CCTD，ctctc 为转载方。本站未见
robots 限制，仅作内部研究库使用；对外发布前需确认原机构的转载授权。
"""

import argparse
import os
import re
import sys
import time
from datetime import date

import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

LIST_URL = ("https://www.ctctc.cn/main/news/flashnewlist.jspx"
            "?nodeid=323&kw={kw}&tag=&pageNumber={page}")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9",
}

_RE_KCAL = re.compile(r"(\d{4})\s*大卡\s*(\d+(?:\.\d+)?)\s*元/吨")
# 广州港进口煤：正文形如「澳大利亚5500大卡动力煤广州港场地含税价1065元/吨」
#   必须锚住「场地含税价」，否则会误抓后面「日环比降10元/吨」这类涨跌值
_RE_GZ_INPORT = re.compile(
    r"(\d{4})\s*大卡动力煤\s*广州港\s*场地含税价\s*(\d+(?:\.\d+)?)\s*元/吨")
_RE_MDD = re.compile(r"(\d{1,2})月(\d{1,2})日")
_RE_LISTDATE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# 每个源的：搜索词 / 正文特征词 / 大卡 → commodity_key
#   match: 正文必须**全部**包含的子串（用来区分同一 kw 下的不同品种）
#   price_re: 抽价正则，group(1)=大卡 group(2)=价格；缺省用 _RE_KCAL
SOURCES = [
    {
        "name": "cci",
        "label": "汾渭 CCI 港口指数",
        "kw": "CCI",
        "marker": "CCI港口",
        "kcal_map": {
            5500: "coal_port_5500",
            5000: "coal_port_5000",
            4500: "coal_port_4500",
        },
    },
    {
        "name": "cctd",
        "label": "CCTD 环渤海动力煤参考价",
        "kw": "CCTD",
        "marker": "CCTD环渤海",
        # 5500 由 fetch_commodities.py 的 CCTD 历史 API 序列负责，这里不写
        "kcal_map": {
            5000: "cctd_bohai_5000",
            4500: "cctd_bohai_4500",
        },
    },
    {
        "name": "gz_au",
        "label": "进口煤 广州港澳洲场地含税价（近似口径，替代 CCI进口5500）",
        "kw": "广州港",
        "marker": "广州港",
        "match": ["广州港", "澳大利亚"],
        "price_re": _RE_GZ_INPORT,
        "kcal_map": {5500: "coal_gz_au_5500"},
    },
    {
        "name": "gz_id",
        "label": "进口煤 广州港印尼场地含税价（近似口径，替代 CCI进口4700/3800）",
        "kw": "广州港",
        "marker": "广州港",
        "match": ["广州港", "印尼", "场地含税价"],
        "price_re": _RE_GZ_INPORT,
        "kcal_map": {4500: "coal_gz_id_4500"},
    },
    {
        "name": "shenhua",
        "label": "神华巴图塔外购车板价（近似口径，替代 CCI产地）",
        "kw": "外购",
        "marker": "外购车板价",
        "kcal_map": {
            5500: "coal_shenhua_5500",
            5000: "coal_shenhua_5000",
            4500: "coal_shenhua_4500",
        },
    },
]

# 新 key 的元数据（首次运行时自动登记进 commodities 表）
NEW_COMMODITIES = {
    "coal_gz_au_5500": ("GZ-AU-5500", "进口煤 广州港澳洲5500",
                        "元/吨", 5500, "进口场地价"),
    "coal_gz_id_4500": ("GZ-ID-4500", "进口煤 广州港印尼4500",
                        "元/吨", 4500, "进口场地价"),
    "coal_shenhua_5500": ("SH-BTT-5500", "产地煤 神华车板价5500",
                          "元/吨", 5500, "产地车板价"),
    "coal_shenhua_5000": ("SH-BTT-5000", "产地煤 神华车板价5000",
                          "元/吨", 5000, "产地车板价"),
    "coal_shenhua_4500": ("SH-BTT-4500", "产地煤 神华车板价4500",
                          "元/吨", 4500, "产地车板价"),
}
NEW_SOURCE_API = "ctctc.cn 市场快讯（转载 CCTD/汾渭）"

SLEEP = 0.8          # 页间间隔，别把对方站点打穿
MAX_PAGES = 80       # 单次最多翻多少页（10 条/页 ≈ 40 个交易日的日评）


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
def db_connect():
    return psycopg2.connect(DATABASE_URL)


def all_keys() -> list[str]:
    return [k for s in SOURCES for k in s["kcal_map"].values()]


def db_latest_dates(conn) -> dict[str, date]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT commodity_key, MAX(price_date)
            FROM prices
            WHERE commodity_key = ANY(%s)
            GROUP BY commodity_key
        """, (all_keys(),))
        return {k: d for k, d in cur.fetchall()}


def db_upsert(conn, rows: list[tuple[str, date, float]]) -> int:
    """rows: (commodity_key, price_date, price) —— 幂等 upsert。"""
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO prices (commodity_key, price_date, price)
            VALUES %s
            ON CONFLICT (commodity_key, price_date)
            DO UPDATE SET price = EXCLUDED.price
        """, rows, page_size=500)
    conn.commit()
    return len(rows)


def ensure_commodities(conn) -> int:
    """把 NEW_COMMODITIES 里缺的 key 登记进 commodities（幂等，不覆盖已有行）。"""
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO commodities ("key", symbol, commodity, unit, source_api,
                                     price_type, kcal, grade_type)
            VALUES (%s, %s, %s, %s, %s, NULL, %s, %s)
            ON CONFLICT ("key") DO NOTHING
        """, [(k, sym, name, unit, NEW_SOURCE_API, kcal, grade)
              for k, (sym, name, unit, kcal, grade) in NEW_COMMODITIES.items()])
        n = cur.rowcount
    conn.commit()
    return n


def print_status(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c."key", c.commodity, MAX(p.price_date) AS latest, COUNT(p.id) AS rows
            FROM commodities c
            LEFT JOIN prices p ON p.commodity_key = c."key"
            WHERE c."key" LIKE 'coal\\_%%' OR c."key" LIKE 'cctd\\_%%'
            GROUP BY 1, 2
            ORDER BY 1
        """)
        print(f"\n{'key':<22}{'最新日期':<14}{'行数':>7}  名称")
        for k, name, latest, n in cur.fetchall():
            print(f"{k:<22}{str(latest):<14}{n:>7}  {name}")
        print()


# ---------------------------------------------------------------------------
# Fetch / parse
# ---------------------------------------------------------------------------
def fetch_page(page: int, kw: str) -> list[dict]:
    """返回 [{list_date, text}]；失败返回 []。"""
    url = LIST_URL.format(kw=kw, page=page)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  [ERROR] page {page}: {exc}")
        return []

    soup = BeautifulSoup(resp.content.decode("utf-8", "ignore"), "lxml")
    out = []
    for li in soup.select(".list-sckx li"):
        d = li.select_one(".list-data")
        c = li.select_one(".list-desc")
        if not d or not c:
            continue
        out.append({
            "list_date": d.get_text(" ", strip=True),
            "text": c.get_text(" ", strip=True),
        })
    return out


def parse_items(items: list[dict], src: dict) -> list[dict]:
    """从快讯里抽价格。返回 [{key, date, price, text}]。"""
    found = []
    rx = src.get("price_re", _RE_KCAL)
    for it in items:
        text = it["text"]
        if src["marker"] not in text:
            continue
        # match 里任何一个子串缺失都跳过（区分同一 kw 下的不同品种）
        if any(m not in text for m in src.get("match", ())):
            continue

        m_list = _RE_LISTDATE.search(it["list_date"])
        if not m_list:
            continue
        pub_year, pub_month = int(m_list.group(1)), int(m_list.group(2))

        m_mdd = _RE_MDD.search(text)
        if not m_mdd:
            print(f"  [WARN] {src['name']} 条目无日期，跳过: {text[:60]}")
            continue
        mon, day = int(m_mdd.group(1)), int(m_mdd.group(2))

        # 跨年：快讯 1 月引用 12 月的价 → 年份减 1
        year = pub_year - 1 if mon > pub_month else pub_year
        try:
            d = date(year, mon, day)
        except ValueError:
            print(f"  [WARN] 日期非法 {year}-{mon}-{day}，跳过")
            continue

        for kcal_str, price_str in rx.findall(text):
            key = src["kcal_map"].get(int(kcal_str))
            if not key:
                continue
            found.append({"key": key, "date": d, "price": float(price_str), "text": text})
    return found


def collect(src: dict, target_pages: int, stop_before: date | None) -> list[dict]:
    """翻页收集。某页所有条目都早于 stop_before 即收工。"""
    out: list[dict] = []
    print(f"\n[{src['label']}] kw={src['kw']}  最多 {target_pages} 页")
    for page in range(1, target_pages + 1):
        items = fetch_page(page, src["kw"])
        if not items:
            print(f"  [WARN] page {page} 无条目，停止")
            break

        parsed = parse_items(items, src)
        out.extend(parsed)
        dates = sorted({p["date"] for p in parsed})
        print(f"  page {page:<3d} 条目 {len(items):>3d}  解析 {len(parsed):>3d}"
              f"  日期 {dates[0] if dates else '—'} ~ {dates[-1] if dates else '—'}")

        if stop_before:
            page_dates = []
            for it in items:
                m = _RE_LISTDATE.search(it["list_date"])
                if m:
                    page_dates.append(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
            if page_dates and max(page_dates) < stop_before:
                print(f"  [STOP] 本页最新 {max(page_dates)} 已早于停点 {stop_before}")
                break

        time.sleep(SLEEP)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="动力煤港口价（ctctc 市场快讯转载）")
    ap.add_argument("--dry-run", action="store_true", help="只解析打印，不写库")
    ap.add_argument("--pages", type=int, default=0,
                    help=f"强制往回翻的页数（默认增量，翻到库内最新日期为止，上限 {MAX_PAGES}）")
    ap.add_argument("--only", choices=[s["name"] for s in SOURCES], help="只抓某一个源")
    ap.add_argument("--status", action="store_true", help="只看库内现状")
    args = ap.parse_args()

    try:
        conn = db_connect()
    except psycopg2.OperationalError as exc:
        print(f"[FATAL] 无法连接数据库: {exc}")
        return 1

    try:
        if args.status:
            print_status(conn)
            return 0

        latest = db_latest_dates(conn)
        newest = max(latest.values()) if latest else None
        # 增量停点：库里最新日期往前留 3 天余量（防补录/改值）
        stop_before = (date.fromordinal(newest.toordinal() - 3) if newest else None)
        print(f"库内最新: {newest or '无'}   "
              f"增量停点: {('< ' + str(stop_before)) if stop_before else '无（全回补）'}")

        target_pages = args.pages if args.pages > 0 else (MAX_PAGES if not newest else 5)
        target_pages = min(target_pages, MAX_PAGES)

        srcs = [s for s in SOURCES if not args.only or s["name"] == args.only]
        found: list[dict] = []
        for s in srcs:
            # 显式 --pages 视为回补，不设停点
            found += collect(s, target_pages, stop_before if args.pages == 0 else None)

        if not found:
            print("\n[SUMMARY] 未解析到任何价格")
            return 1

        # 同一天可能有多条（转载/更正），后者覆盖前者
        dedup: dict[tuple[str, date], float] = {}
        for f in found:
            dedup[(f["key"], f["date"])] = f["price"]
        rows = [(k, d, p) for (k, d), p in sorted(dedup.items(), key=lambda x: x[0][1])]

        print(f"\n解析到 {len(rows)} 个 (品种, 日期) 组合：")
        by_key: dict[str, list] = {}
        for k, d, p in rows:
            by_key.setdefault(k, []).append((d, p))
        for k, lst in sorted(by_key.items()):
            print(f"  {k:<20} {len(lst):>3d} 天  {lst[0][0]}@{lst[0][1]} ~ {lst[-1][0]}@{lst[-1][1]}")

        if args.dry_run:
            print("\n[dry-run] 不写库")
            return 0

        added = ensure_commodities(conn)
        if added:
            print(f"\n[OK] 新登记 {added} 个 commodity 元数据")
        n = db_upsert(conn, rows)
        print(f"\n[OK] upsert {n} 行")
        print_status(conn)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
