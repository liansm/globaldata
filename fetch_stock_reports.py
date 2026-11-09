#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股季度财报 PDF 链接（只存链接，不下载正文）
==============================================
写入 ``stock_reports``：一只股票 × 一个报告期（Q1 / 中报 / Q3 / 年报）一行，
按报告期倒序即可在前端展示。**绝不下载 PDF 内容**。

数据源（**两个交易所官方公开接口，免费、无需 key**）
-----------------------------------------------------
1) 上交所（沪市主板 + 科创板）
       GET https://query.sse.com.cn/security/stock/queryCompanyBulletin.do
   页面：https://www.sse.com.cn/assortment/stock/list/info/announcement/index.shtml?productId=688775
   ⚠ 接口名是 ``queryCompanyBulletin.do``，**没有 New**；
     ``queryCompanyBulletinNew.do`` 对所有参数都返回 total=0（死路，别再试）。
   参数：productId（**留空 = 全市场**）、keyWord、beginDate/endDate、
         pageHelp.pageSize=100、pageHelp.pageNo、reportType=ALL、
         securityType=0101,120100,020100,020200,120200。
   ⚠⚠ **翻页必须同时把 ``pageHelp.beginPage`` 和 ``pageHelp.endPage`` 设成 pageNo**。
     只改 pageNo 而把 beginPage 固定成 1 时，**每一页都返回第 1 页的内容**，
     而且 ``total`` 仍然是正确值（如 4736）、不报错、不重试 —— 表现是「沪市每年
     只落 100 行左右」，被误判成「沪市公告本来就少」。2026-11-08 首轮全量就栽在这，
     靠「沪 932 行 vs 深 99,850 行」比例异常才发现。修完每年 2,700~6,800 条。
   返回 jsonp：pageHelp.data[] 里 SECURITY_CODE / TITLE / SSEDATE / URL /
         BULLETIN_HEADING（定期报告|临时公告）/ BULLETIN_TYPE。
   ``keyWord`` **真的进 WHERE**（600519: "季度报告"→24 条、"年度报告"→52 条），
   配合留空 productId 就能全市场批量；``reportType`` 各种值都无效（死路）。
   PDF：``https://static.sse.com.cn`` + URL 字段。
   ⚠ 脚本 GET/HEAD 该域名会拿到阿里云 WAF 的 JS 挑战页（acw_sc__v2，200 text/html），
     **不是 404、不是链接坏了**；浏览器带 cookie 能正常打开。所以只做 URL 拼接，
     不用脚本去「验活」，验收请用浏览器点开。

2) 深交所（深市主板 + 创业板）
       POST http://www.szse.cn/api/disc/announcement/annList   ← **必须是 http**
   页面：https://www.szse.cn/disclosure/listed/notice/index.html?stock=300298
   ⚠ 2026-11-08 实测：本机代理对 `www.szse.cn:443` 的 HTTPS 隧道会断连
     （`ProxyError: Unable to connect to proxy` / 直连 `RemoteDisconnected`），
     连根页和所有 https 接口都失败；**换成 http 立刻 200、数据正常**。
     一开始误判成「深交所临时封 IP」，其实是代理隧道问题 —— 排查要先分清
     「源站挂了」和「出口挂了」。PDF 静态域 `disc.static.szse.cn` 的 https 不受影响。
   body：seDate=[起,止]、channelCode=["listedNotice_disc"]、
         bigCategoryId=["010301","010303","010305","010307"]、pageSize=50、pageNum=N。
   ⚠ ``bigCategoryId`` 四个值正好是四类定期报告（实测 000001）：
     010301 年度报告 / 010303 半年度报告 / 010305 一季度报告 / 010307 三季度报告。
     （010302/010304/010306/010308 全为 0。）不带 stock 即全市场批量；
     ``stock`` 也支持多代码数组。``pageSize`` 传 500 会被**静默夹到 50**。
     ``searchkey`` / ``keyword`` / ``title`` 等关键词参数全部无效；``searchKey``
     还会让响应变成非 JSON（死路）。
   PDF：``https://disc.static.szse.cn`` + attachPath → 200 application/pdf。
   ⚠⚠ **不要再加 ``/download`` 路径段**：``/download/disc/...`` 会带
     ``Content-Disposition: attachment``，浏览器点了直接下载；去掉后是同一份文件、
     ``application/pdf`` 且**不带** Content-Disposition → 浏览器内置 PDF 阅读器打开。
     2026-11-08 实测：两版字节完全一致（sha256 相同，3/3），15 条跨 2017~2026 抽样
     去掉 /download 后全部 200 + %PDF 头 + 无 CD。库内统一存内联版，点开即「查看」。
     （用户确实要下载时，Chrome 阅读器工具栏自带下载按钮；也可把 /download 加回去。）

口径
----
* ``report_year`` 是**报告期年份**（标题里的「20XX年」），不是披露年份：
  「2025年年度报告」通常在 2026 年披露。quarter_num 1/2/3/4 用于倒序。
* 同一 (code, year, quarter) 只留一条**正文**：排除摘要 / 英文版 / 更正公告 /
  问询函 / 专项报告 / 持续督导 / 说明会等；若存在「（更正后）」正文则优先取更正版，
  再按披露日取最新，最后按标题最短（正文通常比带后缀的短）。
* 只写 ``stocks`` 表里的 5562 只（沪深主板/创业板/科创板），北交所与 B 股不收。
* 所有 URL 都是**链接字符串**，脚本不会请求 PDF、不会保存文件。

增量语义
--------
* 默认（``refresh_all.py`` 接的就是这个）``--recent-days 20``：只查最近 20 天
  两个交易所的新披露公告，把新财报 upsert 进去，约 1~2 分钟。
* ``--years 10``：按年份窗口回填近 10 年（首次用，后台跑，可断点续跑）。
* ``--full``：从 2001 年起全历史。
* 进度文件 ``.workbuddy/stock_reports_progress.json`` 记「已成功完成的
  (源, 年份, 关键词) 任务」，**只有该任务全部翻页成功且入库后才记**。
* 失败可见：任一分页失败打印 WARN 并且该任务不计进度，下次自动补。

用法
----
    python fetch_stock_reports.py                  # 增量（最近 20 天）
    python fetch_stock_reports.py --years 10      # 首次回填近 10 年（后台）
    python fetch_stock_reports.py --full          # 全历史
    python fetch_stock_reports.py --dry-run       # 只抓不写库，打印样本
    python fetch_stock_reports.py --codes 600519,000001   # 只写指定股票（联调）
    python fetch_stock_reports.py --no-recent --years 10 --only szse  # 只补深市
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings()
load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
BASE_DIR = Path(__file__).parent
PROGRESS = BASE_DIR / ".workbuddy" / "stock_reports_progress.json"

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
      "Accept": "*/*", "Accept-Language": "zh-CN,zh;q=0.9"}
SSE = "https://query.sse.com.cn/security/stock/queryCompanyBulletin.do"
# ⚠ 深交所**必须用 http**：本机代理对 www.szse.cn:443 的 HTTPS 隧道会断连
#   （ProxyError: Unable to connect to proxy / RemoteDisconnected），实测 http 200 正常、
#   https 全失败。PDF 静态域 disc.static.szse.cn 的 https 不受影响。
SZSE = "http://www.szse.cn/api/disc/announcement/annList"
SSE_PDF = "https://static.sse.com.cn"
# ⚠ 深市 PDF 只差一个路径段，浏览器行为却完全不同（2026-11-08 实测）：
#   https://disc.static.szse.cn/download/...  → Content-Disposition: attachment（下载）
#   https://disc.static.szse.cn/...           → 无 CD + application/pdf（内联查看）
#   两个 URL 返回的 PDF **字节完全一致**（sha256 相同），所以只存内联版；
#   老数据的订正见 SQL_NORMALIZE_SZSE_PDF。
SZSE_PDF = "https://disc.static.szse.cn"
SSE_SECURITY_TYPE = "0101,120100,020100,020200,120200"
SZSE_CATS = ["010301", "010303", "010305", "010307"]

DEFAULT_WORKERS = 10
TIMEOUT = 25
RETRIES = 2

SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS stock_reports (
    id            bigserial PRIMARY KEY,
    code          varchar(10)  NOT NULL,
    exchange      varchar(20)  NOT NULL,
    report_year   integer      NOT NULL,
    quarter       varchar(4)   NOT NULL,
    quarter_num   integer      NOT NULL,
    title         text,
    pdf_url       text         NOT NULL,
    publish_date  date,
    source        varchar(12)  NOT NULL,
    updated_at    timestamptz  NOT NULL DEFAULT NOW(),
    CONSTRAINT stock_reports_uniq UNIQUE (code, report_year, quarter)
);
CREATE INDEX IF NOT EXISTS idx_stock_reports_code_period ON stock_reports(code, report_year, quarter_num);
CREATE INDEX IF NOT EXISTS idx_stock_reports_publish     ON stock_reports(publish_date);
"""

# 一次性数据订正（2026-11-08）：老版本写入的深市链接带 /download，点开会下载而非查看。
# 幂等：订正过之后 WHERE 匹配 0 行，每次跑的开销只是一条 0 行 UPDATE。
SQL_NORMALIZE_SZSE_PDF = """
UPDATE stock_reports
   SET pdf_url = replace(pdf_url,
                         'https://disc.static.szse.cn/download/',
                         'https://disc.static.szse.cn/')
 WHERE source = 'szse'
   AND pdf_url LIKE 'https://disc.static.szse.cn/download/%'
"""

UPSERT = """
INSERT INTO stock_reports
    (code, exchange, report_year, quarter, quarter_num, title, pdf_url, publish_date, source)
VALUES %s
ON CONFLICT (code, report_year, quarter) DO UPDATE SET
    exchange     = EXCLUDED.exchange,
    quarter_num  = EXCLUDED.quarter_num,
    title        = EXCLUDED.title,
    pdf_url      = EXCLUDED.pdf_url,
    publish_date = EXCLUDED.publish_date,
    source       = EXCLUDED.source,
    updated_at   = NOW()
"""

# 排除「不是正文报告」的公告。注意故意**不排除**「（更正后）」「更正版」—— 那才是最终正文。
EXCLUDE = re.compile(
    r'摘要|英文|更正公告|问询函|专项报告|持续督导|审计报告|内部控制|说明会|'
    r'提示性公告|备考|监管|反馈回复|核查意见|独立意见|会计政策|财务报告|'
    r'披露|H股|境外|翻译|简版|编制|议事规则|制度|办法|指引')
CORRECTED = re.compile(r'更正后|更正版|修订后|修订版|更新后')
YEAR = re.compile(r'(20\d{2})年')


# ─────────────────────────────────────────────────────────────────────────────
_tls = threading.local()


def session() -> requests.Session:
    if not hasattr(_tls, "s"):
        s = requests.Session()
        s.headers.update(UA)
        _tls.s = s
    return _tls.s


def get_json(url, params=None, json_body=None, headers=None, retries=RETRIES):
    last = None
    for i in range(retries + 1):
        try:
            if json_body is not None:
                r = session().post(url, params=params, json=json_body,
                                   headers=headers, timeout=TIMEOUT, verify=False)
            else:
                r = session().get(url, params=params, headers=headers,
                                  timeout=TIMEOUT, verify=False)
            r.raise_for_status()
            return r.json()
        except Exception as exc:                 # noqa: BLE001
            last = exc
            if i < retries:
                time.sleep(0.6 * (2 ** i))
    raise RuntimeError(f"{type(last).__name__}: {last}")


def parse_day(v):
    if not v:
        return None
    s = str(v)[:10].replace("/", "-")
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def classify(title: str):
    """标题 → (报告期年份, quarter, quarter_num)；不是正文定期报告返回 None。"""
    if not title or EXCLUDE.search(title):
        return None
    m = YEAR.search(title)
    if not m:
        return None
    year = int(m[1])
    if year < 1991 or year > date.today().year + 1:
        return None
    if "第一季度报告" in title or "一季度报告" in title:
        return year, "Q1", 1
    if "半年度报告" in title or "中期报告" in title:
        return year, "H1", 2
    if "第三季度报告" in title or "三季度报告" in title:
        return year, "Q3", 3
    if "年度报告" in title and "半年度" not in title:
        return year, "FY", 4
    return None


def better(a, b):
    """同一 (code, year, quarter) 有多条时选更优的一条。"""
    if a is None:
        return b
    ka = (1 if CORRECTED.search(a["title"]) else 0,
          a["publish_date"] or date.min, -len(a["title"]), a["url"])
    kb = (1 if CORRECTED.search(b["title"]) else 0,
          b["publish_date"] or date.min, -len(b["title"]), b["url"])
    return b if kb > ka else a


# ─────────────────────────────────────────────────────────────────────────────
# 上交所：按 (年份窗口, keyWord) 翻页
# ─────────────────────────────────────────────────────────────────────────────
def sse_page(keyword: str, begin: str, end: str, page_no: int) -> dict:
    # ⚠ beginPage / endPage **必须等于 pageNo**：上交所这个接口只要 beginPage=1，
    #   无论 pageNo 传几都返回第一页（实测 pageNo=2/beginPage=1 与 pageNo=1 数据完全相同，
    #   pageNo=2/beginPage=2 才拿到第 2 页）。写错的表现是「每年只落 100 行左右」，
    #   而且不报错、total 仍是 4736，极易误判成「沪市公告本来就少」。
    params = {"jsonCallBack": "cb", "isPagination": "true", "productId": "",
              "securityType": SSE_SECURITY_TYPE, "reportType": "ALL",
              "keyWord": keyword, "beginDate": begin, "endDate": end,
              "pageHelp.pageSize": "100", "pageHelp.pageCount": "50",
              "pageHelp.pageNo": str(page_no),
              "pageHelp.beginPage": str(page_no),
              "pageHelp.cacheSize": "1", "pageHelp.endPage": str(page_no),
              "_": int(time.time() * 1000)}
    h = {**UA, "Referer": "https://www.sse.com.cn/assortment/stock/list/info/announcement/index.shtml"}
    r = session().get(SSE, params=params, headers=h, timeout=TIMEOUT, verify=False)
    r.raise_for_status()
    m = re.search(r"^\w+\((.*)\)\s*;?\s*$", r.text, re.S)
    if not m:
        raise RuntimeError("上交所响应不是 jsonp")
    return json.loads(m[1])


def sse_task(keyword: str, begin: str, end: str) -> list[dict]:
    out, page = [], 1
    while True:
        j = sse_page(keyword, begin, end, page)
        ph = j.get("pageHelp") or {}
        data = ph.get("data") or []
        for x in data:
            code = str(x.get("SECURITY_CODE") or "").strip()
            title = str(x.get("TITLE") or "").strip()
            url = str(x.get("URL") or "").strip()
            cls = classify(title)
            if not cls or not code or not url:
                continue
            year, quarter, qnum = cls
            out.append({"code": code, "year": year, "quarter": quarter, "qnum": qnum,
                        "title": title, "url": SSE_PDF + url if url.startswith("/") else url,
                        "publish_date": parse_day(x.get("SSEDATE")), "source": "sse"})
        total = int(ph.get("total") or 0)
        if page * 100 >= total or not data:
            break
        page += 1
        if page > 400:                        # 安全阀，防源侧 total 异常导致死循环
            raise RuntimeError(f"上交所 {keyword} {begin}~{end} 翻页超过 400 页")
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 深交所：按 (年份窗口) 翻页，四个分类一次查完
# ─────────────────────────────────────────────────────────────────────────────
def szse_page(begin: str, end: str, page_no: int) -> dict:
    body = {"seDate": [begin, end], "channelCode": ["listedNotice_disc"],
            "bigCategoryId": SZSE_CATS, "pageSize": 50, "pageNum": page_no}
    h = {**UA, "Referer": "http://www.szse.cn/disclosure/listed/notice/index.html",
         "Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest",
         "Origin": "http://www.szse.cn"}
    return get_json(SZSE, params={"random": str(time.time())}, json_body=body, headers=h)


def szse_task(begin: str, end: str) -> list[dict]:
    out, page = [], 1
    while True:
        j = szse_page(begin, end, page)
        data = j.get("data") or []
        for x in data:
            codes = [str(c).strip() for c in (x.get("secCode") or [])]
            title = str(x.get("title") or "").strip()
            path = str(x.get("attachPath") or "").strip()
            cls = classify(title)
            if not cls or not path:
                continue
            year, quarter, qnum = cls
            for code in codes:
                out.append({"code": code, "year": year, "quarter": quarter, "qnum": qnum,
                            "title": title,
                            # attachPath 形如 /disc/disk03/...；**不要**拼 /download
                            "url": SZSE_PDF + path if path.startswith("/") else path,
                            "publish_date": parse_day(x.get("publishTime")), "source": "szse"})
        total = int(j.get("announceCount") or 0)
        if page * 50 >= total or not data:
            break
        page += 1
        if page > 800:
            raise RuntimeError(f"深交所 {begin}~{end} 翻页超过 800 页")
    return out


# ─────────────────────────────────────────────────────────────────────────────
def year_windows(args):
    """返回 [(begin, end), ...]，近 N 年或全历史按年切分。"""
    today = date.today()
    if args.recent_days:
        return [(str(today - timedelta(days=args.recent_days)), str(today))]
    end_year = today.year
    start_year = 2001 if args.full else max(2001, end_year - args.years + 1)
    return [(f"{y}-01-01", f"{y}-12-31") for y in range(start_year, end_year + 1)]


def build_jobs(windows, only=""):
    jobs = []
    for begin, end in windows:
        if only != "szse":
            for kw in ("季度报告", "年度报告"):
                jobs.append(("sse", kw, begin, end))
        if only != "sse":
            jobs.append(("szse", "定期报告", begin, end))
    return jobs


def run_job(job):
    src, kw, begin, end = job
    try:
        rows = sse_task(kw, begin, end) if src == "sse" else szse_task(begin, end)
        return job, rows, None
    except Exception as exc:                 # noqa: BLE001
        return job, None, f"{type(exc).__name__}: {str(exc)[:90]}"


def main():
    ap = argparse.ArgumentParser(description="A 股季度财报 PDF 链接（只存链接，不下载正文）")
    ap.add_argument("--years", type=int, default=10, help="回填近 N 年，默认 10")
    ap.add_argument("--full", action="store_true", help="从 2001 年起全历史")
    ap.add_argument("--recent-days", type=int, default=20,
                    help="增量：只查最近 N 天（默认 20；refresh_all 接这个）")
    ap.add_argument("--no-recent", action="store_true", help="关闭增量模式，按 --years/--full 跑")
    ap.add_argument("--dry-run", action="store_true", help="只抓不写库")
    ap.add_argument("--codes", default="", help="只写指定代码，逗号分隔（联调）")
    ap.add_argument("--only", choices=["", "sse", "szse"], default="",
                    help="只跑某一个交易所（分批跑/单源重试用）")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = ap.parse_args()
    args.codes = {c.strip() for c in args.codes.split(",") if c.strip()}

    if args.no_recent:
        args.recent_days = 0
    t0 = time.time()
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SQL_ENSURE)
                cur.execute(SQL_NORMALIZE_SZSE_PDF)
                normalized = cur.rowcount
        with conn.cursor() as cur:
            cur.execute("SELECT code, exchange FROM stocks")
            code_map = {r[0]: r[1] for r in cur.fetchall()}
        print(f"[1/5] 库内股票 {len(code_map)} 只；DDL 就绪"
              + (f"；深市 PDF 链接订正 {normalized} 行（/download → 内联查看）"
                 if normalized else ""), flush=True)

        windows = year_windows(args)
        jobs = build_jobs(windows, args.only)
        done = set()
        if PROGRESS.exists() and not args.dry_run:
            try:
                done = set(json.loads(PROGRESS.read_text(encoding="utf-8")))
            except Exception:                # noqa: BLE001
                print("[WARN] 进度文件损坏，从头重跑")
        # 增量模式每次都要重查最近窗口，不能用旧进度跳过
        if args.recent_days:
            done = set()
        pending = [j for j in jobs if "|".join(j) not in done]
        print(f"[2/5] 窗口 {len(windows)} 个 → 任务 {len(jobs)} 个，待跑 {len(pending)} 个，"
              f"workers={args.workers}", flush=True)

        if args.dry_run:
            for j in pending[:3]:
                job, rows, err = run_job(j)
                print(f"  [dry-run] {job}: {0 if rows is None else len(rows)} 行, err={err}")
                if rows:
                    for r in rows[:3]:
                        print("     ", r)
            return 0

        collected, failed = {}, 0
        # 用 as_completed 而不是 ex.map：map 按提交顺序消费结果，只要第一个任务慢，
        # 整轮日志就一条进度都不打（实测 16 分钟无输出，无法判断是在跑还是卡死）。
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(run_job, j): j for j in pending}
            for i, fut in enumerate(as_completed(futures), 1):
                job, rows, err = fut.result()
                key = "|".join(job)
                if err or rows is None:
                    failed += 1
                    print(f"      [{i}/{len(pending)}] [WARN] {key} 失败（不计进度）: {err}", flush=True)
                    continue
                for r in rows:
                    code = r["code"]
                    if code not in code_map or (args.codes and code not in args.codes):
                        continue
                    k = (code, r["year"], r["quarter"])
                    collected[k] = better(collected.get(k), r)
                done.add(key)
                print(f"      [{i}/{len(pending)}] {key} → {len(rows)} 条，"
                      f"累计候选 {len(collected)}，失败 {failed}，"
                      f"耗时 {time.time()-t0:.1f}s", flush=True)

        vals = [(r["code"], code_map[r["code"]], r["year"], r["quarter"], r["qnum"],
                 r["title"], r["url"], r["publish_date"], r["source"])
                for r in collected.values()]
        print(f"[3/5] 去重后 {len(vals)} 行（正文报告，按 code+年+季度唯一）", flush=True)

        if vals:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, UPSERT, vals, page_size=500)
            # commit 成功后才写进度
            if not args.recent_days:
                PROGRESS.parent.mkdir(parents=True, exist_ok=True)
                PROGRESS.write_text(json.dumps(sorted(done), ensure_ascii=False), encoding="utf-8")

        with conn.cursor() as cur:
            cur.execute("""SELECT COUNT(*) AS rows, COUNT(DISTINCT code) AS stocks,
                                  MIN(report_year)::text AS earliest,
                                  MAX(report_year)::text AS latest,
                                  COUNT(*) FILTER (WHERE source='sse') AS sse,
                                  COUNT(*) FILTER (WHERE source='szse') AS szse
                           FROM stock_reports""")
            t = cur.fetchone()
        print(f"[4/5] 库内：{t[0]} 行 / {t[1]} 只 / 报告期 {t[2]}~{t[3]} / "
              f"沪 {t[4]} 深 {t[5]}")
        print(f"[5/5] 本轮失败任务 {failed} 个（未记进度，下次自动补），总耗时 {time.time()-t0:.1f}s")
        return 1 if failed else 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
