#!/usr/bin/env python3
"""
Milk (生鲜乳收购价) Fetcher
============================
农业农村部畜牧兽医局「畜产品和饲料集贸市场价格情况」周报里的
**主产省份生鲜乳收购价**（元/公斤），upsert 进 commodities / prices 表。

为什么是这个源
--------------
国内没有牛奶期货 —— 上期所/大商所/郑商所/广期所都不上市牛奶品种，
畜牧类只有鸡蛋(JD)、生猪(LH)。所以「牛奶价格」在国内的官方基准
就是这份周报里的生鲜乳收购价：对全国 500 个县集贸市场和采集点监测，
取 10 个主产省（河北、山西、内蒙古、辽宁、黑龙江、山东、河南、陕西、
宁夏、新疆）的算术平均。这是乳业最上游的原奶定价锚。

    CME 有乳品期货（DA/DK/CSC/CB/DY/NFS），但：
      - akshare `futures_foreign_hist` 对上述代码全部 ValueError，
      - 新浪外盘 `hf_DA/hf_DC/hf_DY/hf_CSC/hf_CB` 返回空串（同时 hf_GC 正常），
    两条免费路都是死的，要抓只能走 Barchart / investing 那类反爬站。→ 未接。

数据入口（关键，非直觉）
------------------------
周报正文页本身没有列表页 —— `https://www.moa.gov.cn/ztzl/nybrl/rlxx/`
的 index.htm 只是一段 meta refresh，跳到 zdscxx 平台的日历页；而
`/ztzl/nybrl/rlxx/202610/` 这类月目录直接 **403**（无目录索引）。

真正的索引藏在「数据日历」页的 JS 里：

    GET https://www.moa.gov.cn/sj/index.htm      (~470 KB)
        var message = new Array(
            "20261027 10月第3周畜产品和饲料集贸市场价格情况|2235|../ztzl/nybrl/rlxx/202610/t20261029_6489018.htm",
            ... )

2700+ 条条目，其中标题含「畜产品和饲料集贸市场价格情况」的即为周报。
这是**唯一**能批量拿到历史 URL 的入口，别再试图猜月目录或爬列表页。

深度与死路
----------
* 该数组只保留**最近 2500 条**，最早到 2017-07；去重后周报 455 期
  （2017-08-01 ~ 至今），与「每周一期」对得上。
* 更早（2014-12 ~ 2015-08）散落在老栏目分页 `rlxx/index_1..4.htm`，
  2016 ~ 2017-07 目前无公开索引 → **未覆盖，缺口要记着**。
* 没有分省数据：周报只给 10 省合计均价，各省单值未公开。
* 「主产省份生鲜乳」在报告表里**同时**出现在数据行和脚注行（脚注是
  「生鲜乳主产省统计范围：…」），必须用 `主产省` 打头精确匹配，
  否则会抓到脚注。
* **9 期源站空页**（见 `KNOWN_EMPTY`）：返回 200 但 body 0 字节，
  换 UA/Referer/http/错时段全复现 → 源侧文件已无内容，archive.org 本机不可达。
* **3 期抓不到表格**（20180920 / 20200415 / 20230628）：前者 HTML 里
  `<span>` 未闭合把表格截断，后两者**整张表是图片**，页面没有 `<table>`。
  已加**正文正则兜底**（「…生鲜乳平均价格3.45元/公斤，…」），三期都已补上。
* **覆盖缺口（2026-10-31 实测，别误判成抓漏）**
    - 2018 年 43 期（应 52）：9 期是上面那批源站空页。
    - 2019 / 2020 / 2021 分别 47 / 50 / 49 期：主站 message 数组**本身漏期**
      （例：2025 年 4 月第 1/2 周主站索引里没有，畜牧兽医局 jcyj 里有 → 后者补上了）。
      jcyj 只能翻到 `index_24`（2022-01），`nahs.org.cn` 到 2021-11，
      `scs.moa.gov.cn` 到 2025-04 且标题格式不同 → 2019~2021 那几个洞暂无免费源可补。
    - **2022 年起每年 52~53 期，完整。** 全序列 462 期 / 2017-07-26 ~ 2026-10-22。
* **间歇性空回**：同一个 URL 可能这次 0 字节、下次 35 KB。`_get()` 因此按
  **body 长度 < 2 KB 判失败并重试**，不能只看 HTTP 状态码。

口径
----
* 价格 = 10 个主产省份算术平均收购价，单位 **元/公斤**，非期货、非批发价。
* 频率：周（每周一期）。报告的「周」以**采集日**界定，正文会写
  「（采集日为10月22日）」，采集日固定在周四。
* price_date 取**采集日**（不是发布日）—— 发布日比采集日晚 5~7 天且不固定，
  直接用会让曲线间隔不齐。解析不到采集日时退回发布日并打 WARN。
* 2022 年前后行名从「主产省生鲜乳」改成「主产省份生鲜乳」，两套都要认。

Usage
-----
    python fetch_milk_price.py                 # 增量（只补最近若干期）
    python fetch_milk_price.py --full          # 全量回补（首轮约 12 分钟，可续跑）
    python fetch_milk_price.py --dry-run       # 抓取并打印，不写库、不写进度
    python fetch_milk_price.py --full --dry-run
    python fetch_milk_price.py --full --retry-empty   # 连 9 期已知空页一起重试
"""

import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from urllib.parse import urljoin

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INDEX_URL = "https://www.moa.gov.cn/sj/index.htm"

# 第二索引：畜牧兽医局自己的「监测预警」栏目，**可翻页**，比主站的 message 数组完整。
#   https://xmsyj.moa.gov.cn/jcyj/index_0.htm ... index_24.htm（index_25 起 404）
# 实测覆盖 2022-01 ~ 至今；主站 message 数组覆盖 2017-07 起但**会漏期**
# （2025 年 4 月第 1/2 周主站索引里就没有，jcyj 里有）→ 两个源取并集。
JCYJ_BASE = "https://xmsyj.moa.gov.cn/jcyj/"
JCYJ_MAX_PAGES = 60          # 实测 index_24 是最后一页，留余量
INCR_JCYJ_PAGES = 6          # 增量只翻前 6 页 ≈ 10 个月，够覆盖 20 天窗口

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9",
}
TIMEOUT = 30
SLEEP_BETWEEN = 0.25          # 政府站别打太狠
RETRIES = 3

KEY = "milk_raw"
COMMODITY = "生鲜乳 主产省份收购价"
SYMBOL = "MOA-XMRU"
UNIT = "元/公斤"
SOURCE_API = "moa.gov.cn 畜产品和饲料集贸市场价格情况(周报)"
PRICE_TYPE = "10主产省平均收购价"

TITLE_PAT = "畜产品和饲料集贸市场价格情况"

# 增量模式：只抓「发布日 >= 库中最新采集日 - 该天数」的期次。
# 发布日比采集日晚 5~7 天，留 20 天余量足够覆盖，且不会重复抓 455 页。
INCR_LOOKBACK_DAYS = 20

PROGRESS_FILE = os.path.join(".workbuddy", "milk_progress.json")

# 源站空页白名单（2026-10-31 实测定论）
# --------------------------------------
# 这 9 个 URL 在 moa.gov.cn 上返回 `HTTP 200 + Content-Length: 0`，body 完全为空。
# 换 UA / 加 Referer / 走 http / 隔时段重试（两轮共 6 次）全部复现，
# 而同期其它期次正常（≥17 KB）→ **源侧这些文件没有内容**，不是频控、不是反爬。
# archive.org 在本机代理下不可达（ProxyError / CDX 502），无法用快照补。
# 因此默认直接跳过，免得每次 --full 白等 9×3 次重试的退避；
# 要再试一次（比如怀疑源站已修复）：python fetch_milk_price.py --full --retry-empty
KNOWN_EMPTY = {
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201802/t20180207_6136582.htm",  # 2018 1月末周
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201802/t20180228_6137631.htm",  # 2018 2月第3周
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201803/t20180307_6138025.htm",  # 2018 2月第4周
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201804/t20180425_6140957.htm",  # 2018 4月第3周
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201805/t20180516_6142314.htm",  # 2018 5月第2周
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201805/t20180530_6148889.htm",  # 2018 5月第4周
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201806/t20180621_6152784.htm",  # 2018 6月第2周
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201812/t20181226_6165610.htm",  # 2018 12月第3周
    "https://www.moa.gov.cn/ztzl/nybrl/rlxx/201904/t20190411_6178645.htm",  # 2019 4月第1周
}

UPSERT_COMMODITY_SQL = """
INSERT INTO commodities (key, symbol, commodity, unit, source_api, price_type, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (key) DO UPDATE SET
    symbol = EXCLUDED.symbol,
    commodity = EXCLUDED.commodity,
    unit = EXCLUDED.unit,
    source_api = EXCLUDED.source_api,
    price_type = EXCLUDED.price_type,
    updated_at = NOW()
"""

UPSERT_PRICES_SQL = """
INSERT INTO prices (commodity_key, price_date, price)
VALUES %s
ON CONFLICT (commodity_key, price_date) DO UPDATE SET
    price = EXCLUDED.price
"""


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
def _get(url: str, min_len: int = 2000) -> str:
    """GET + 重试。

    ⚠ 必须检查 **body 长度**，不能只看 HTTP 状态码：源站对部分期次会返回
      `HTTP 200 + Content-Length: 0`（空 body），requests 看来是一次成功请求，
      实际拿到 0 字节 → 解析成「没有生鲜乳行」，很容易被误判成页面结构变了。
      实测同一 URL 重试能拿到 35865 B 的正常页面（2026-10-31，20180920 那期），
      所以这是**间歇性空回**，重试就能补上。正常周报页 ≥ 17 KB，取 2 KB 作阈值。
    """
    last = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=False)
            r.raise_for_status()
            if len(r.content) < min_len:
                raise RuntimeError(f"响应过短 {len(r.content)} B（疑似源站空回）")
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as exc:                      # noqa: BLE001
            last = exc
            if attempt < RETRIES - 1:
                time.sleep(2.0 * (attempt + 1))
    raise RuntimeError(f"{url} 失败 {RETRIES} 次: {last}")


# ---------------------------------------------------------------------------
# Index parsing
# ---------------------------------------------------------------------------
def parse_index(html: str) -> list:
    """从「数据日历」页的 message 数组里抽周报条目 → [(pub_date, title, url)]"""
    items = re.findall(r'"(\d{8})\s+([^"|]+)\|(\d+)\|([^"]+\.htm)"', html)
    out, seen = [], set()
    for d8, title, _size, rel in items:
        if TITLE_PAT not in title:
            continue
        url = urljoin(INDEX_URL, rel)
        if url in seen:
            continue
        seen.add(url)
        try:
            pub = datetime.strptime(d8, "%Y%m%d").date()
        except ValueError:
            continue
        out.append((pub, title.strip(), url))
    out.sort()
    return out


def parse_jcyj_index(html: str, page_url: str) -> list:
    """从畜牧兽医局「监测预警」列表页抽周报条目。

    列表页形如：10月第3周畜产品和饲料集贸市场价格情况2026-10-27
    （<a> 里带着日期文本，非 <span> 分离）→ 用正则从锚文本尾部取日期。
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        if TITLE_PAT not in text:
            continue
        m = re.search(r"(20\d{2}-\d{2}-\d{2})\s*$", text)
        if not m:
            continue
        try:
            pub = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            continue
        title = re.sub(r"\s*20\d{2}-\d{2}-\d{2}\s*$", "", text).strip()
        out.append((pub, title, urljoin(page_url, a["href"])))
    return out


def collect_reports(max_jcyj_pages: int = JCYJ_MAX_PAGES) -> list:
    """两个索引取并集 → [(pub_date, title, url)]，按发布日排序。

    `max_jcyj_pages` 用来在增量模式里省时间：jcyj 每页 20 条（其中 10 条是周报）
    ≈ 2 个月，日常增量只需要最近几页；`--full` 才翻到底（25 页，约 40 秒）。
    """
    merged: dict = {}

    # ① 主站 message 数组（覆盖最深，但会漏期）
    try:
        for pub, title, url in parse_index(_get(INDEX_URL)):
            merged[url] = (pub, title, url)
    except Exception as exc:                          # noqa: BLE001
        print(f"  [WARN] 主站索引页失败: {exc}")

    # ② 畜牧兽医局 jcyj 列表页（近 5 年更完整）
    pages_ok = 0
    for i in range(max_jcyj_pages):
        page_url = JCYJ_BASE if i == 0 else f"{JCYJ_BASE}index_{i}.htm"
        try:
            html = _get(page_url)
        except Exception:                             # noqa: BLE001
            continue                                  # 尾页 404 走 _get 重试后抛错，直接跳过
        pages_ok += 1
        rows = parse_jcyj_index(html, page_url)
        for pub, title, url in rows:
            merged.setdefault(url, (pub, title, url))
        # 连续两页都没有周报 → 到底了（404 页不会进这里，但留个兜底）
        if not rows and i > 0:
            break
    print(f"  jcyj 列表页翻到 {pages_ok} 页")

    out = sorted(merged.values(), key=lambda x: (x[0], x[2]))
    return out


# ---------------------------------------------------------------------------
# Page parsing
# ---------------------------------------------------------------------------
_WS = re.compile(r"\s+")


def _norm(s: str) -> str:
    return _WS.sub("", s or "")


def parse_report(html: str, pub_date: date):
    """解析一期周报，返回 dict 或 None。

    返回: {date, price, prev_price, yoy, mom, week_label}
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # ---- 采集日（正文「（采集日为10月22日）」）----
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = _norm(soup.get_text(" ", strip=True))

    price = prev = yoy = mom = None
    week_label = None

    for tbl in soup.find_all("table"):
        for tr in tbl.find_all("tr"):
            cells = [_norm(c.get_text(strip=True)) for c in tr.find_all(["td", "th"])]
            if not cells:
                continue
            name = cells[0]
            # 表头首行即「2026年10月第3周（总第43周）畜产品、饲料集市价格」
            if week_label is None and "年" in name and "周" in name and len(cells) == 1:
                week_label = name
            # 数据行：主产省生鲜乳 / 主产省份生鲜乳（脚注行以「生鲜乳」打头，被排除）
            if name.startswith("主产省") and "生鲜乳" in name and len(cells) >= 4:
                def _f(x):
                    try:
                        return float(x)
                    except (TypeError, ValueError):
                        return None
                price = _f(cells[1])
                prev = _f(cells[3])           # 列序: 本周|上年同期|前一周|同比%|环比%
                if len(cells) >= 6:
                    yoy, mom = _f(cells[4]), _f(cells[5])
                break
        if price is not None:
            break

    if price is None:
        # ── 正文兜底 ────────────────────────────────────────────────────────
        # 少数期次的表格抓不到，三种原因都实测过（2026-10-31）：
        #   ① 源站 HTML 里 <span> 未闭合 → BeautifulSoup 把表格截成 4 行（20180920）
        #   ② 整张表格是**图片**，页面没有 <table>（20200415 / 20230628）
        # 但正文那句一定在：
        #   「内蒙古、河北等10个奶牛主产省（区）生鲜乳平均价格3.45元/公斤，…」
        #   「内蒙古、河北等10个主产省份生鲜乳平均价格3.81元/公斤，…」
        # 只截单句（不跨「。」）避免串到前后别的品种价格。
        m_body = re.search(
            r"生鲜乳(?:平均)?价格([\d.]+)元/公斤", text
        ) or re.search(
            r"生鲜乳[^。]{0,24}?平均价格([\d.]+)元/公斤", text
        )
        if m_body:
            try:
                price = float(m_body.group(1))
                prev = yoy = mom = None
                source = "body"
            except ValueError:
                return None
        else:
            return None
    else:
        source = "table"

    # ---- 采集日 → price_date ----
    price_date = None
    m = re.search(r"采集日为(\d{1,2})月(\d{1,2})日", text)
    if m:
        mo, dy = int(m.group(1)), int(m.group(2))
        yr = pub_date.year
        if mo > pub_date.month:      # 12 月采集、次年 1 月发布
            yr -= 1
        try:
            price_date = date(yr, mo, dy)
        except ValueError:
            price_date = None

    if price_date is None:
        price_date = pub_date          # 退回发布日

    return {
        "date": price_date,
        "price": price,
        "prev": prev,
        "yoy": yoy,
        "mom": mom,
        "week_label": week_label,
        "fallback_date": not bool(m),
        "source": source,
    }


# ---------------------------------------------------------------------------
# Progress (只记成功，绝不记失败)
# ---------------------------------------------------------------------------
def load_progress() -> dict:
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:                  # noqa: BLE001
        return {}


def save_progress(done: dict) -> None:
    """成功即落盘。Windows 上 `os.replace` 偶发 WinError 5（目标文件被杀软/索引
    瞬时占用），2026-10-31 全量跑时因此丢过一条进度 → 加重试并退回直接覆盖写。
    写进度失败**不该**让一期已抓到的数据被记成失败，所以最后只 WARN 不抛。
    """
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    payload = json.dumps(done, ensure_ascii=False, indent=0, sort_keys=True)
    tmp = PROGRESS_FILE + ".tmp"
    for attempt in range(4):
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(payload)
            os.replace(tmp, PROGRESS_FILE)
            return
        except OSError:
            time.sleep(0.4 * (attempt + 1))
    try:                                   # 退回直接覆盖写
        with open(PROGRESS_FILE, "w", encoding="utf-8") as fh:
            fh.write(payload)
    except OSError as exc:                 # noqa: BLE001
        print(f"    [WARN] 进度文件写不进去（数据已采集，下次会重抓这一期）: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    dry_run = "--dry-run" in sys.argv
    full = "--full" in sys.argv

    print("── 农业农村部 生鲜乳收购价 (周报) ──────────────────────────────")
    print(f"  索引①: {INDEX_URL}")
    print(f"  索引②: {JCYJ_BASE}index_N.htm")
    reports = collect_reports(JCYJ_MAX_PAGES if full else INCR_JCYJ_PAGES)
    if not reports:
        print("  [FATAL] 两个索引都没解析到周报条目（页面结构可能变了）")
        return 1
    print(f"  合并后 {len(reports)} 条周报: {reports[0][0]} → {reports[-1][0]}")
    print(f"  最早一期: {reports[0][1]}")
    print(f"  最新一期: {reports[-1][1]}")

    progress = load_progress()

    # ---- 增量：按库中最新价日期裁掉老期次 ----
    todo = reports
    if not full:
        try:
            conn0 = psycopg2.connect(DATABASE_URL)
            with conn0.cursor() as cur:
                cur.execute("SELECT MAX(price_date) FROM prices WHERE commodity_key = %s", (KEY,))
                row = cur.fetchone()
            conn0.close()
            latest = row[0] if row else None
        except Exception as exc:                  # noqa: BLE001
            print(f"  [WARN] 查库失败，退回全量: {exc}")
            latest = None

        if latest:
            cutoff = latest - timedelta(days=INCR_LOOKBACK_DAYS)
            todo = [r for r in reports if r[0] >= cutoff]
            print(f"  库中最新采集日 {latest} → 增量只抓发布日 >= {cutoff} 的 {len(todo)} 期")
        else:
            print("  库中还没有该品种 → 走全量")
    else:
        print("  [--full] 全量回补")

    # 进度里已成功的跳过（--full 也跳，除非库里没有）
    pending = [r for r in todo if r[2] not in progress]
    skipped = len(todo) - len(pending)
    if skipped:
        print(f"  进度文件已有 {skipped} 期，跳过")

    # 已知源站空页：默认跳过，--retry-empty 才重试
    if "--retry-empty" not in sys.argv:
        empty = [r for r in pending if r[2] in KNOWN_EMPTY]
        if empty:
            pending = [r for r in pending if r[2] not in KNOWN_EMPTY]
            print(f"  跳过 {len(empty)} 期已知源站空页（源侧 Content-Length: 0，"
                  f"要重试加 --retry-empty）")

    print()
    rows, failed = [], []
    for i, (pub, title, url) in enumerate(pending, 1):
        tag = f"[{i}/{len(pending)}] {pub}"
        try:
            html = _get(url)
            rec = parse_report(html, pub)
            if rec is None:
                failed.append((pub, title, f"未解析到生鲜乳行 {url}"))
                print(f"  {tag} [FAIL] 无生鲜乳行  {title[:26]}  {url}")
                continue
            flag = " (日期退回发布日)" if rec["fallback_date"] else ""
            if rec.get("source") == "body":
                flag += " (正文兜底，表格缺失)"
            print(f"  {tag} {rec['date']}  {rec['price']:.2f} 元/公斤"
                  f"  (前一周 {rec['prev']}){flag}")
            rows.append((rec["date"], rec["price"], url))
            if not dry_run:                        # dry-run 绝不写进度（否则重跑会跳过）
                progress[url] = rec["date"].isoformat()
                save_progress(progress)            # 成功即落盘，可续跑
        except Exception as exc:               # noqa: BLE001
            failed.append((pub, title, str(exc)[:80]))
            print(f"  {tag} [ERR] {type(exc).__name__}: {str(exc)[:70]}")
        time.sleep(SLEEP_BETWEEN)

    print()
    if failed:
        print(f"  [WARN] {len(failed)} 期失败（未写入进度，下次重跑会补）:")
        for pub, title, why in failed[:10]:
            print(f"         {pub} {title[:34]} → {why}")

    if not rows:
        print("  没有新数据。")
        return 0 if not failed else 1

    rows.sort()
    # 同一采集日会有两个源的副本（主站 ztzl + 畜牧兽医局 jcyj，文章 id 不同但内容同）。
    # 主站优先：它的页面带完整 <table>，jcyj 那批多数把表格做成了图片（只能正文兜底）。
    dedup = {}
    for d, p, u in rows:
        rank = 0 if "/ztzl/nybrl/rlxx/" in u else 1
        if d not in dedup or rank < dedup[d][2]:
            dedup[d] = (p, u, rank)
    entries = [(KEY, d, v[0]) for d, v in sorted(dedup.items())]

    print(f"  解析成功 {len(rows)} 期，去重后 {len(entries)} 个采集日"
          f"（{entries[0][1]} ~ {entries[-1][1]}）")

    if dry_run:
        print("\n[DRY-RUN] 跳过写库。")
        for d in sorted(dedup)[-8:]:
            print(f"    {d}  {dedup[d][0]:.2f}")
        return 0

    print("\n连接数据库...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接: {exc}")
        return 1

    try:
        with conn.cursor() as cur:
            cur.execute(UPSERT_COMMODITY_SQL,
                        (KEY, SYMBOL, COMMODITY, UNIT, SOURCE_API, PRICE_TYPE))
            execute_values(cur, UPSERT_PRICES_SQL, entries)
        conn.commit()
        print(f"  [OK] commodities 就绪: {KEY} ({COMMODITY}, {UNIT})")
        print(f"  [OK] prices 写入 {len(entries)} 行")
    except Exception as exc:                   # noqa: BLE001
        conn.rollback()
        print(f"\n[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()

    print(f"\n[OK] 生鲜乳收购价更新完成：{entries[0][1]} ~ {entries[-1][1]}，"
          f"最新 {entries[-1][2]:.2f} 元/公斤。")
    if failed:
        print(f"     注意：仍有 {len(failed)} 期失败，重跑本脚本会自动补。")
    return 0 if not failed else 1


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings()
    sys.exit(main())
