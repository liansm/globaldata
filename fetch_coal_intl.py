#!/usr/bin/env python3
"""
国际动力煤 FOB 价格 —— CCTD「国际煤炭」栏目周报（免费，补 sxcoal 的 FOB 缺口）
================================================================================
背景（2026-10-29）
------------------
原 `sxcoal.com 汾渭CCI` 的 3 条 FOB 序列（coal_import_*_fob，美元/吨）随付费墙
一起停更在 2026-09-30。ctctc 快讯只有人民币到岸/场地价，**没有美元离岸价**。

本脚本改用 **CCTD 中国煤炭市场网「国际煤炭」栏目**（catid=464）：该栏目每周二
免费发《CCTD：国际主要港口动力煤价格…》，正文给出三个国际港口动力煤价格指数
（美元/吨），数值未打码、无登录墙。

Data source
-----------
    GET https://www.cctd.com.cn/list-464-{page}.html          # 20 条/页，475 条（约 24 页）
    文章 https://www.cctd.com.cn/show-464-{id}-1.html        # GB18030 编码！

    正文样例（2026-10-28 发布）：
        据中国煤炭市场网监测：截止10月23日，澳大利亚纽卡斯尔港动力煤价格指数为
        153.42美元/吨，较截止10月16日的150.22美元/吨上涨3.2美元/吨，涨幅为2.13%。
        南非理查兹港动力煤价格指数为117美元/吨…欧洲三港动力煤价格指数为95.88美元/吨。

    ⚠ 日期取 **正文里「截止X月X日」的数据日期**，不是文章发布日 ——
      周报周二发、数据截止上周五，用发布日会把序列整体推后 3~5 天。
    ⚠ 每句里都有「较截止…的旧值」第二、三个数字，正则必须锚「价格指数为」+
      只取第一个匹配（search 而非 findall），否则会抓到对比基期的旧值。

已知源侧坑（2026-10-29 实测，都写在这里免得下次重踩）
------------------------------------------------------
1. **单位笔误**：CCTD 有若干期把「美元/吨」写成「元/吨」，数值仍是美元
   （2024-09-24 / 2024-10-08 / 2024-10-15 三期，值 136~149）。只认「美元」会
   直接丢数据 → 本单位照收，再用 `FOB_MIN/FOB_MAX` 量纲闸门挡真正的人民币报价。
2. **欧洲三港疑似停更**：`coal_ara_fob` 自 2025-11-28 起一直报 95.88（每周都写
   「持平」），已连续 40+ 期不变。数据是原文真实数值，但**别再当有效变量用**。
3. **2017-2018 部分老文章已下线**：列表页仍有标题和链接，点进去是站内
   「提示信息」页（无正文）→ 这段时间序列是断的，不是解析失败。

抓什么
------
    coal_newc_fob      澳洲纽卡斯尔港动力煤价格指数   6000 大卡  美元/吨
    coal_richards_fob  南非理查兹港动力煤价格指数     6000 大卡  美元/吨
    coal_ara_fob       欧洲三港 (ARA) 动力煤价格指数  6000 大卡  美元/吨

口径说明（重要，别当等价替换）
------------------------------
这三条是**真正的国际离岸/到岸指数（美元/吨，6000 大卡）**，而原来的
`coal_import_5500_fob / 4700_fob / 3800_fob` 是汾渭的**中国进口到岸 FOB 折算价
（5500/4700/3800 大卡）**。热值与编制方法都不同 → **新建 key，不覆盖原序列**。
用途：作为国际煤价方向性锚点（周频），不能直接与 5500 大卡人民币价做价差。

覆盖不到的
----------
汾渭 `coal_import_5500/4700/3800`（人民币到岸）与三条 FOB —— 专有指数，无免费替代。
原序列维持停更在 2026-09-30，不伪造、不外推。

Usage
-----
    python fetch_coal_intl.py                 # 增量：抓到库内最新日期前 21 天为止
    python fetch_coal_intl.py --pages 24      # 全量回补（475 条约 9 年）
    python fetch_coal_intl.py --dry-run       # 只解析打印，不写库
    python fetch_coal_intl.py --status        # 只看库内现状

Compliance note
---------------
数据原始出处为中国煤炭市场网（CCTD）。该栏目免费公开、无 robots 限制，
仅作内部研究库使用；对外发布前需确认 CCTD 转载授权。
"""

import argparse
import os
import re
import sys
import time
from datetime import date, timedelta

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

LIST_URL = "https://www.cctd.com.cn/list-464-{page}.html"
ARTICLE_URL = "https://www.cctd.com.cn/show-464-{aid}-1.html"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9",
}

# key → (正则, 名称)
#   ⚠ 单位分组单独捕获：CCTD 有若干期把「美元/吨」误写成「元/吨」
#     （2024-09-24、2024-10-08、2024-10-15 实测），数值仍是美元。
#     只认「美元」会把这几期直接丢掉 → 必须收，再用量纲闸门挡真正的人民币价。
SERIES = {
    "coal_newc_fob": (
        re.compile(r"纽卡斯尔港动力煤价格指数为\s*(\d+(?:\.\d+)?)\s*(美元|元)\s*/\s*吨"),
        "澳洲纽卡斯尔港动力煤价格指数",
    ),
    "coal_richards_fob": (
        re.compile(r"理查兹港动力煤价格指数为\s*(\d+(?:\.\d+)?)\s*(美元|元)\s*/\s*吨"),
        "南非理查兹港动力煤价格指数",
    ),
    "coal_ara_fob": (
        re.compile(r"欧洲三港动力煤价格指数为\s*(\d+(?:\.\d+)?)\s*(美元|元)\s*/\s*吨"),
        "欧洲三港动力煤价格指数",
    ),
}

# 量纲闸门：国际动力煤 FOB 历史区间约 40~460 美元/吨（2022 峰值 NEWC 452）。
# 超出这个范围说明抓到的是人民币价或别的口径 → 丢弃告警，宁缺勿错。
FOB_MIN, FOB_MAX = 20.0, 600.0

NEW_COMMODITIES = {
    "coal_newc_fob": ("NEWC-6000", "国际煤价 纽卡斯尔6000", "美元/吨", 6000),
    "coal_richards_fob": ("RBCT-6000", "国际煤价 理查兹港6000", "美元/吨", 6000),
    "coal_ara_fob": ("ARA-6000", "国际煤价 欧洲三港6000",
                     "美元/吨", 6000),
}
NEW_SOURCE_API = "cctd.com.cn 国际煤炭周报（CCTD 监测）"

_RE_LISTITEM = re.compile(
    r'href="(https://www\.cctd\.com\.cn/show-464-(\d+)-1\.html)"[^>]*>([^<]{4,80})</a>'
    r'.*?<td width="20%" align="center">(\d{4}-\d{2}-\d{2})</td>', re.S)
_RE_CUTOFF = re.compile(r"截止\s*(\d{1,2})月(\d{1,2})日")

SLEEP = 0.6
MAX_PAGES = 30


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
def db_connect():
    return psycopg2.connect(DATABASE_URL)


def db_latest_date(conn) -> date | None:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(price_date) FROM prices WHERE commodity_key = ANY(%s)",
                    (list(SERIES),))
        return cur.fetchone()[0]


def ensure_commodities(conn) -> int:
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO commodities ("key", symbol, commodity, unit, source_api,
                                     price_type, kcal, grade_type)
            VALUES (%s, %s, %s, %s, %s, NULL, %s, %s)
            ON CONFLICT ("key") DO NOTHING
        """, [(k, sym, name, unit, NEW_SOURCE_API, kcal, "国际FOB")
              for k, (sym, name, unit, kcal) in NEW_COMMODITIES.items()])
        n = cur.rowcount
    conn.commit()
    return n


def db_upsert(conn, rows: list[tuple[str, date, float]]) -> int:
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


def print_status(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c."key", c.commodity, MAX(p.price_date), COUNT(p.id)
            FROM commodities c LEFT JOIN prices p ON p.commodity_key = c."key"
            WHERE c."key" IN %s
            GROUP BY 1, 2 ORDER BY 1
        """, (tuple(SERIES),))
        print(f"\n{'key':<20}{'最新日期':<14}{'行数':>7}  名称")
        for k, name, latest, n in cur.fetchall():
            print(f"{k:<20}{str(latest):<14}{n:>7}  {name}")
        print()


# ---------------------------------------------------------------------------
# Fetch / parse
# ---------------------------------------------------------------------------
def fetch_text(url: str) -> str | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  [ERROR] {url} : {exc}")
        return None
    # 站点是 GB18030；先试 utf-8，出现乱码替换符再回退
    txt = resp.content.decode("utf-8", "ignore")
    if txt.count("\ufffd") > 5 or "国际煤炭" not in txt:
        txt = resp.content.decode("gb18030", "ignore")
    return txt


def list_articles(page: int) -> list[dict]:
    html = fetch_text(LIST_URL.format(page=page))
    if not html:
        return []
    out = []
    for url, aid, title, pub in _RE_LISTITEM.findall(html):
        out.append({"url": url, "aid": aid, "title": title.strip(), "pub": pub})
    # 去重（同一页可能重复出现头条）
    seen, uniq = set(), []
    for a in out:
        if a["aid"] in seen:
            continue
        seen.add(a["aid"])
        uniq.append(a)
    return uniq


def parse_article(html: str, pub: str) -> tuple[date | None, dict[str, float], dict[str, str]]:
    """返回 (数据日期, {key: price}, {key: 原文单位标注})。数据日期取正文「截止X月X日」。"""
    body = re.sub(r"<script.*?</script>", " ", html, flags=re.S)
    body = re.sub(r"<style.*?</style>", " ", body, flags=re.S)
    body = re.sub(r"<[^>]+>", " ", body)
    body = body.replace("&nbsp;", " ")
    body = re.sub(r"\s+", " ", body)

    vals: dict[str, float] = {}
    units: dict[str, str] = {}
    for key, (rx, _) in SERIES.items():
        m = rx.search(body)          # 只取第一个（后面是「较截止…的旧值」）
        if not m:
            continue
        val = float(m.group(1))
        if not (FOB_MIN <= val <= FOB_MAX):
            print(f"  [WARN] {key} 值 {val} 超出 FOB 量纲区间 "
                  f"[{FOB_MIN},{FOB_MAX}]，疑似人民币价，丢弃（{pub}）")
            continue
        vals[key] = val
        units[key] = m.group(2)

    data_date = None
    m = _RE_CUTOFF.search(body)
    if m:
        pub_y, pub_m = int(pub[:4]), int(pub[5:7])
        mon, day = int(m.group(1)), int(m.group(2))
        year = pub_y - 1 if mon > pub_m else pub_y
        try:
            data_date = date(year, mon, day)
        except ValueError:
            data_date = None
    return data_date, vals, units


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="国际动力煤 FOB（CCTD 国际煤炭周报）")
    ap.add_argument("--dry-run", action="store_true", help="只解析打印，不写库")
    ap.add_argument("--pages", type=int, default=0,
                    help=f"强制往回翻的页数（默认增量；上限 {MAX_PAGES}，全量约 24 页）")
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

        newest = db_latest_date(conn)
        # 周报：库内最新日期前 21 天作停点，留足补录余量
        stop_before = (newest - timedelta(days=21)) if newest else None
        print(f"库内最新: {newest or '无'}   增量停点: "
              f"{('< ' + str(stop_before)) if stop_before else '无（全回补）'}")

        target_pages = args.pages if args.pages > 0 else (MAX_PAGES if not newest else 6)
        target_pages = min(target_pages, MAX_PAGES)

        rows: dict[tuple[str, date], float] = {}
        hit_articles = 0
        unit_typos = 0
        for page in range(1, target_pages + 1):
            arts = list_articles(page)
            if not arts:
                print(f"  [WARN] page {page} 无条目，停止")
                break

            page_hits = 0
            for a in arts:
                html = fetch_text(a["url"])
                if html is None:
                    continue
                d, vals, units = parse_article(html, a["pub"])
                if not vals:
                    continue
                if not d:
                    print(f"  [WARN] 无「截止X月X日」数据日期，跳过: {a['title'][:30]}")
                    continue
                for k, v in vals.items():
                    rows[(k, d)] = v
                unit_typos += sum(1 for u in units.values() if u == "元")
                page_hits += 1
                hit_articles += 1
                time.sleep(SLEEP)

            print(f"  page {page:<3d} 条目 {len(arts):>3d}  解析 {page_hits:>3d}")

            # 整页都早于停点即收工（显式 --pages 视为回补，不设停点）
            pubs = [a["pub"] for a in arts]
            if args.pages == 0 and stop_before and pubs and max(pubs) < str(stop_before):
                print(f"  [STOP] 本页最新 {max(pubs)} 早于停点 {stop_before}")
                break
            time.sleep(SLEEP)

        ordered = sorted(rows.items(), key=lambda x: x[0][1])
        print(f"\n解析到 {len(ordered)} 个 (品种, 日期) 组合，来自 {hit_articles} 篇文章：")
        by_key: dict[str, list] = {}
        for (k, d), v in ordered:
            by_key.setdefault(k, []).append((d, v))
        for k, lst in sorted(by_key.items()):
            print(f"  {k:<20}{len(lst):>3d} 期  {lst[0][0]}@{lst[0][1]} ~ {lst[-1][0]}@{lst[-1][1]}")
        if unit_typos:
            print(f"  ⚠ 有 {unit_typos} 个数值原文标注「元/吨」（CCTD 笔误，实为美元），已按美元收录")

        if not ordered:
            print("\n[SUMMARY] 未解析到任何价格")
            return 1
        if args.dry_run:
            print("\n[dry-run] 不写库")
            return 0

        added = ensure_commodities(conn)
        if added:
            print(f"\n[OK] 新登记 {added} 个 commodity 元数据")
        n = db_upsert(conn, [(k, d, v) for (k, d), v in ordered])
        print(f"\n[OK] upsert {n} 行")
        print_status(conn)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
