#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股每股财务指标快照（EPS / PE / 股息 / 股息率 / 每股净资产 / 市净率）
=====================================================================
写入 ``stock_fundamentals``（一只股票一行快照）。**独立于 stocks / stock_prices**。
股息率由「分红 / 最新价」自算、可验算；⚠ 但**源侧市净率 PB 与每股净资产
并不总能互相验算**（见下「口径」），别在 UI 里宣称两者恒等。

数据源（**全部免费、无需 key / 登录**）
---------------------------------------
1) 东方财富 clist 全市场行情（批量入口，60 页并发约 9 秒）
       GET https://push2.eastmoney.com/api/qt/clist/get
   ⚠ ``pz`` 被服务端**静默夹到 100**（传 10000 只返 100 条），必须翻页。
   字段映射（**用 clist 的编号，别套到 stock/get 上** —— 实测 stock/get 的
   f112 是别的含义，返回 "2" 而不是 EPS）：
       f2   最新价（元）        f9   市盈率-动态（报告期年化）
       f23  市净率              f112 每股收益（最新报告期**累计、未年化**）
       f113 每股净资产          f114 市盈率-静态（上年年报）
       f115 市盈率-TTM          f133 股息率（**口径不统一，见下，不落库**）
       f297 行情日期

2) 东方财富数据中心分红明细（批量分页，10 页约 3 秒）
       GET https://datacenter-web.eastmoney.com/api/data/v1/get
       reportName=RPT_SHAREBONUS_DET
       filter=(EX_DIVIDEND_DATE>='<今天-365天>')   ← 实测 count 57180 → 4966，过滤生效
   ``PRETAX_BONUS_RMB`` 是**每 10 股派息（元，含税）**，÷10 才是每股。
   ⚠ 结果里含**未来的除权除息日**（今天 11-08 就有 11-13 的已公告方案），
     必须再按 ``EX_DIVIDEND_DATE <= 今天`` 剔掉，否则把没到手的分红算进去。
   ⚠ 分页可能重复同一方案 → 按 (code, 除权日, 金额, 报告期) 去重再累加。

3) 报告期：akshare ``stock_yjbb_em(date=YYYYMMDD)``（东财业绩报表，另一个端点）
   一次给全市场 5776 行的「每股收益 / 每股净资产 / 净资产收益率」，且**报告期明确**。
   脚本从最近 6 个季度末里挑第一个「已大面积披露」（≥1000 行）的作为 report_date，
   同时用它做 clist f112/f113 的**独立端点交叉校验**。

口径（⚠ 这六条决定数字对不对）
--------------------------------
* 所有指标都用**同一个最新收盘价 price**（clist f2）：
      eps_ttm = price / pe_ttm（反推式），股息率 = dividend / price × 100（自算，可验算）。
  ⚠ **PB = price / bvps 不恒成立**：实测前 200 只里 5 只（688809 / 688678 / 688655 /
  688610 / 688560，多为科创板）偏差 7~28%，源侧 f23 与 f113 的报告期或股本口径
  并不总一致。保留东财源侧 PB，不自行改写，也不宣称两者可验算 —— 这是源侧口径
  差异，**不是抓取错误**。
* ``eps_ttm`` = price / pe_ttm，是**滚动 12 个月**每股收益；
  ``eps_report`` = 最新报告期**累计未年化** EPS（茅台 2026 三季报 51.57，年化后约 68.7）。
  两者绝不能混用 —— 一季报的累计 EPS 只有全年约 1/4。``report_date`` 标明报告期。
* ``pe_ttm`` / ``pe_dynamic`` / ``pe_static`` 是三种不同 PE（实测五粮液
  23.99 / 22.59 / 32.66），UI 主显示 TTM 并写清口径，另两个只作参考。
* ``dividend`` = **近 12 个月已实施**（除权除息日 ≤ 今天）现金分红合计，税前、元/股；
  ``dividend_yield`` = dividend / price × 100（%）。例：茅台近 12 个月两次派息
  28.02423 + 23.957 = 51.98123 元/股 → 51.98123 / 1453 = 3.58%。
* ⚠ **东财 clist 的 f133「股息率」口径不统一，不采用**：茅台 f133=1.93 只算了最近
  一次分红（28.02423/1453），平安银行 f133=5.16 却算了近 12 个月合计（0.609/11.81），
  五粮液 3.42 也是只算一次。同一个字段两种口径 → 只拿来交叉校验，不落库。
  同理 akshare ``stock_fhps_em`` 的「现金分红-股息率」是按分红时点价格算的，
  与当前价无关（茅台 2.31%），也不采用。
* 无分红 / 亏损 / 停牌的字段一律 **NULL**（前端显示「—」），不用 0 充数。

死路
----
* ``stock/get`` 单只端点的 f112/f113 与 clist **同名不同义**（f112 返回 "2"），
  不能用它做交叉校验或补数。
* F10 ``NewFinanceAnalysis/MainTargetAjax`` 返回「无 F10 资料」HTML 页，已废弃。

增量语义
--------
每次跑都重拉全市场快照（约 20 秒），``ON CONFLICT DO UPDATE`` 覆盖同一批 code，
**不产生新行**。不做「同一行情日就跳过」的守卫：分红方案会在同一天内新增，
跳过就会静默漏数据（宁可每次 20 秒）。失败可见：源侧任一阶段拿不到数据直接退出，
不写半张表。

合规
----
东方财富公开行情 / 数据中心接口，用于内部研究，不对外分发、不商业转售；
抓取做了并发上限，不做高频轮询。

用法
----
    python fetch_stock_fundamentals.py                # 刷新全市场（refresh_all 接这个）
    python fetch_stock_fundamentals.py --dry-run      # 只算不写库，打印 8 只样本
    python fetch_stock_fundamentals.py --verify 12    # 与业绩报表端点交叉校验
    python fetch_stock_fundamentals.py --limit 50     # 只写前 50 只（联调）
"""

import argparse
import contextlib
import io
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
BASE_DIR = Path(__file__).parent

UA = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Referer": "https://quote.eastmoney.com/",
}
UT = "bd1d9ddb04089700cf9c27f6f7426281"
CLIST = "https://push2.eastmoney.com/api/qt/clist/get"
DC = "https://datacenter-web.eastmoney.com/api/data/v1/get"
FS_A = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
CLIST_FIELDS = "f2,f3,f9,f12,f14,f23,f112,f113,f114,f115,f133,f297"

PAGE = 100
DC_PAGE = 500
WORKERS = 8
TIMEOUT = 25
SOURCE = "eastmoney_clist+datacenter_RPT_SHAREBONUS_DET+stock_yjbb_em"

SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS stock_fundamentals (
    code              varchar(10) PRIMARY KEY,
    eps_ttm           numeric(16,4),
    eps_report        numeric(16,4),
    report_date       date,
    pe_ttm            numeric(16,4),
    pe_dynamic        numeric(16,4),
    pe_static         numeric(16,4),
    bvps              numeric(16,4),
    pb                numeric(16,4),
    dividend          numeric(16,6),
    dividend_yield    numeric(10,4),
    dividend_plans    integer,
    dividend_last_ex  date,
    price             numeric(16,4),
    quote_date        date,
    source            varchar(200),
    updated_at        timestamptz NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stock_fundamentals_quote ON stock_fundamentals(quote_date);
CREATE INDEX IF NOT EXISTS idx_stock_fundamentals_pe    ON stock_fundamentals(pe_ttm);
"""

UPSERT = """
INSERT INTO stock_fundamentals (
    code, eps_ttm, eps_report, report_date, pe_ttm, pe_dynamic, pe_static,
    bvps, pb, dividend, dividend_yield, dividend_plans, dividend_last_ex,
    price, quote_date, source)
VALUES %s
ON CONFLICT (code) DO UPDATE SET
    eps_ttm          = EXCLUDED.eps_ttm,
    eps_report       = EXCLUDED.eps_report,
    report_date      = EXCLUDED.report_date,
    pe_ttm           = EXCLUDED.pe_ttm,
    pe_dynamic       = EXCLUDED.pe_dynamic,
    pe_static        = EXCLUDED.pe_static,
    bvps             = EXCLUDED.bvps,
    pb               = EXCLUDED.pb,
    dividend         = EXCLUDED.dividend,
    dividend_yield   = EXCLUDED.dividend_yield,
    dividend_plans   = EXCLUDED.dividend_plans,
    dividend_last_ex = EXCLUDED.dividend_last_ex,
    price            = EXCLUDED.price,
    quote_date       = EXCLUDED.quote_date,
    source           = EXCLUDED.source,
    updated_at       = NOW()
"""


# ─────────────────────────────────────────────────────────────────────────────
_tls = threading.local()


def session() -> requests.Session:
    if not hasattr(_tls, "s"):
        s = requests.Session()
        s.headers.update(UA)
        _tls.s = s
    return _tls.s


def num(v):
    """东财 '-' / NaN / None 一律 None。"""
    if v is None or v == "-" or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def day(v):
    """兼容 '2026-06-26 00:00:00'（datacenter）与 20261106（clist f297 整数）。"""
    if not v:
        return None
    s = str(v)[:10].replace("/", "-")
    if len(s) == 8 and s.isdigit():
        s = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def in_scope(code: str) -> bool:
    """与 fetch_stock_info.py::classify 同规则：只收沪深主板 / 创业板 / 科创板。"""
    return code.startswith(("600", "601", "603", "605", "688", "689",
                            "000", "001", "002", "003", "300", "301"))


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 1：clist 全市场估值快照
# ─────────────────────────────────────────────────────────────────────────────
def clist_page(pn: int) -> dict:
    p = {"pn": pn, "pz": PAGE, "po": 1, "np": 1, "ut": UT, "fltt": 2, "invt": 2,
         "fid": "f12", "fs": FS_A, "fields": CLIST_FIELDS, "_": int(time.time() * 1000)}
    r = session().get(CLIST, params=p, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def load_quotes() -> list[dict]:
    j = clist_page(1)
    data = j.get("data") or {}
    total = int(data.get("total") or 0)
    pages = (total + PAGE - 1) // PAGE
    rows = list(data.get("diff") or [])
    if pages > 1:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for chunk in ex.map(lambda pn: clist_page(pn).get("data", {}).get("diff") or [],
                                range(2, pages + 1)):
                rows.extend(chunk)
    # 批内去重
    seen, out = set(), []
    for r in rows:
        code = str(r.get("f12") or "").strip()
        if code and in_scope(code) and code not in seen:
            seen.add(code)
            out.append(r)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 2：近 12 个月已实施现金分红
# ─────────────────────────────────────────────────────────────────────────────
def dc_page(pn: int, flt: str) -> dict:
    p = {"reportName": "RPT_SHAREBONUS_DET", "columns": "ALL", "filter": flt,
         "pageNumber": pn, "pageSize": DC_PAGE, "sortColumns": "EX_DIVIDEND_DATE",
         "sortTypes": "-1", "source": "WEB", "client": "WEB", "_": int(time.time() * 1000)}
    r = session().get(DC, params=p, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def load_dividends(today: date) -> dict:
    since = (today - timedelta(days=365)).isoformat()
    flt = f"(EX_DIVIDEND_DATE>='{since}')"
    first = dc_page(1, flt)
    res = first.get("result") or {}
    pages = int(res.get("pages") or 1)
    raw = list(res.get("data") or [])
    if pages > 1:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            for chunk in ex.map(lambda pn: (dc_page(pn, flt).get("result") or {}).get("data") or [],
                                range(2, pages + 1)):
                raw.extend(chunk)

    out, seen = {}, set()
    today_s = today.isoformat()
    for d in raw:
        code = str(d.get("SECURITY_CODE") or "").strip()
        ex = day(d.get("EX_DIVIDEND_DATE"))
        amt = num(d.get("PRETAX_BONUS_RMB"))
        # 未来除权日 = 已公告但还没到手，不能算进「近 12 个月已实施」
        if not code or not ex or ex > today or amt is None or amt <= 0:
            continue
        key = (code, str(ex), amt, str(d.get("REPORT_DATE") or "")[:10])
        if key in seen:
            continue
        seen.add(key)
        v = out.setdefault(code, {"sum": 0.0, "plans": 0, "last_ex": None})
        v["sum"] += amt / 10.0                 # 每 10 股派 X 元 → 每股
        v["plans"] += 1
        if v["last_ex"] is None or str(ex) > v["last_ex"]:
            v["last_ex"] = str(ex)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 阶段 3：最新财报报告期（独立端点，顺带做交叉校验）
# ─────────────────────────────────────────────────────────────────────────────
def load_report(today: date):
    import akshare as ak
    cands = []
    for yy in (today.year, today.year - 1, today.year - 2):
        for mm, dd in ((12, 31), (9, 30), (6, 30), (3, 31)):
            d = date(yy, mm, dd)
            if d <= today - timedelta(days=15):   # 留出披露滞后
                cands.append(d)
    cands.sort(reverse=True)
    for d in cands[:6]:
        try:
            # akshare 内部 tqdm / print 会污染日志，两个流都临时重定向
            with contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
                df = ak.stock_yjbb_em(date=d.strftime("%Y%m%d"))
            if df is not None and len(df) >= 1000:
                return d, df
        except Exception:                        # noqa: BLE001
            continue
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# 合并 / 校验
# ─────────────────────────────────────────────────────────────────────────────
def build_rows(quotes, divs, report_date, limit=0):
    rows = []
    for r in quotes:
        code = str(r.get("f12") or "").strip()
        price = num(r.get("f2"))
        pe_ttm = num(r.get("f115"))
        pe_dyn = num(r.get("f9"))
        pe_static = num(r.get("f114"))
        bvps = num(r.get("f113"))
        pb = num(r.get("f23"))
        eps_report = num(r.get("f112"))
        quote_date = day(f"{r.get('f297')}"[:8]) if r.get("f297") else None

        # eps_ttm 由「价格 / PE(TTM)」反推，和 PE 用同一价格基准，可自行验算
        eps_ttm = price / pe_ttm if (price and pe_ttm) else None

        dv = divs.get(code)
        dividend = round(dv["sum"], 6) if dv else None
        div_yield = (dividend / price * 100) if (dividend is not None and price and price > 0) else None
        rows.append((
            code, eps_ttm, eps_report, report_date, pe_ttm, pe_dyn, pe_static,
            bvps, pb, dividend, div_yield,
            dv["plans"] if dv else None, day(dv["last_ex"]) if dv else None,
            price, quote_date, SOURCE,
        ))
        if limit and len(rows) >= limit:
            break
    return rows


SAMPLE_CODES = ["600519", "000001", "601398", "300750", "000651", "600036", "601857", "000858"]


def print_samples(quotes, divs, rows):
    by_code = {r[0]: r for r in rows}
    print("\n[样本对照] 六个指标 / 分红明细（f133 是东财股息率，只作校验不落库）")
    q = {str(r.get("f12")): r for r in quotes}
    for c in SAMPLE_CODES:
        r = by_code.get(c)
        if not r:
            continue
        (_, eps_ttm, eps_rep, rep, pe, pe_dyn, pe_st, bvps, pb,
         dividend, dy, plans, last_ex, price, qdate, _) = r
        f133 = num((q.get(c) or {}).get("f133"))
        print(f"  {c}: 价={price} | EPS(TTM)={eps_ttm and round(eps_ttm,2)} "
              f"PE(TTM)={pe} | BVPS={bvps} PB={pb} | 股息={dividend} "
              f"股息率={dy and round(dy,2)}% ({plans}次, 最近除权 {last_ex}) "
              f"| 报告期={rep} 累计EPS={eps_rep} | 东财f133={f133}")
    print("  ⚠ f133 口径不统一（茅台只算一次 1.93%，平安/中石油算近12月），故不采用。")


def verify(quotes, rows, report_df):
    """交叉校验：① 内部自洽（PE/PB 反推）② 业绩报表端点（不同端点）对照。"""
    print("\n[verify] 计算自洽（eps_ttm 反推）+ 源侧 PB/BVPS 一致性诊断")
    bad, pb_diff, samples = 0, 0, []
    for r in rows[:200]:
        (_, eps_ttm, _, _, pe, _, _, bvps, pb, _, _, _, _, price, _, _) = r
        if pe and eps_ttm and price and abs(price / pe - eps_ttm) > 0.01:
            bad += 1                       # 算式写错才是真失败
        if pb and bvps and price:
            implied = price / pb
            rel = abs(implied - bvps) / max(abs(bvps), 0.01)
            if rel > 0.02:
                pb_diff += 1               # 源侧口径差异，只诊断不算失败
                if len(samples) < 5:
                    samples.append((r[0], f"price={price} pb={pb} 反推BVPS={implied:.3f} "
                                         f"实际={bvps:.3f} 偏差={rel*100:.1f}%"))
    print(f"  eps_ttm 反推计算错误 {bad} 项；PB/BVPS 偏差>2% 的 {pb_diff}/200 只"
          f"（源侧报告期/股本口径差异，非抓取错误）")
    for s in samples:
        print("   ", s[0], s[1])

    if report_df is None or report_df.empty:
        print("  [SKIP] 业绩报表未取到，跳过端点对照")
        return bad == 0
    rp = {}
    for rec in report_df.to_dict("records"):
        code = str(rec.get("股票代码") or "").strip()
        if code:
            rp[code] = (num(rec.get("每股收益")), num(rec.get("每股净资产")))
    by_code = {r[0]: r for r in rows}
    diff = 0
    checked = 0
    for c in SAMPLE_CODES:
        if c not in rp or c not in by_code:
            continue
        r = by_code[c]
        y_eps, y_bvps = rp[c]
        print(f"  {c}: clist eps_report={r[2]} / yjbb EPS={y_eps} ; "
              f"clist bvps={r[7]} / yjbb BVPS={y_bvps}")
        checked += 1
        if y_bvps and r[7] and abs(y_bvps - r[7]) / max(abs(y_bvps), 0.01) > 0.01:
            diff += 1
    print(f"  端点对照 {checked} 只，每股净资产差异 >1% 的 {diff} 只")
    return bad == 0 and diff == 0


# ─────────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="A 股每股财务指标快照（EPS/PE/股息/股息率/BVPS/PB）")
    ap.add_argument("--dry-run", action="store_true", help="只算不写库")
    ap.add_argument("--verify", action="store_true", help="打印交叉校验结果")
    ap.add_argument("--limit", type=int, default=0, help="只写前 N 只（联调）")
    args = ap.parse_args()

    t0 = time.time()
    today = date.today()

    print("[1/4] 东财 clist 全市场估值快照…", flush=True)
    quotes = load_quotes()
    print(f"      {len(quotes)} 只，耗时 {time.time()-t0:.1f}s", flush=True)
    if len(quotes) < 3000:
        print("[FATAL] 名录明显偏少，疑似源侧异常，直接退出（不写半张表）", flush=True)
        return 1

    print("[2/4] 近 12 个月已实施现金分红（datacenter 批量分页）…", flush=True)
    divs = load_dividends(today)
    print(f"      {len(divs)} 只有近 12 个月分红记录，耗时 {time.time()-t0:.1f}s", flush=True)

    print("[3/4] 探测最新财报报告期（东财业绩报表，独立端点）…", flush=True)
    report_date, report_df = load_report(today)
    print(f"      报告期 = {report_date or '未取到'}，报表 {0 if report_df is None else len(report_df)} 行，"
          f"耗时 {time.time()-t0:.1f}s", flush=True)
    if report_date is None:
        print("[WARN] 报告期未取到 —— report_date 留空，其余指标照常写（不静默当成功）", flush=True)

    rows = build_rows(quotes, divs, report_date, args.limit)
    print_samples(quotes, divs, rows)

    if args.verify or args.dry_run:
        ok = True
        if args.verify:
            ok = verify(quotes, rows, report_df)
        if args.dry_run:
            print(f"\n[dry-run] 共 {len(rows)} 行，未写库")
            return 0
        return 0 if ok else 2

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SQL_ENSURE)
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT, rows, page_size=500)
        with conn.cursor() as cur:
            cur.execute("""SELECT COUNT(*) AS rows,
                                  COUNT(eps_ttm) AS with_eps,
                                  COUNT(pe_ttm) AS with_pe,
                                  COUNT(bvps) AS with_bvps,
                                  COUNT(dividend) AS with_div,
                                  MAX(quote_date)::text AS quote_date,
                                  MAX(report_date)::text AS report_date
                           FROM stock_fundamentals""")
            t = cur.fetchone()
        print(f"[4/4] 写入/覆盖 {len(rows)} 行。库内：{t[0]} 行 / EPS(TTM) {t[1]} / "
              f"PE(TTM) {t[2]} / BVPS {t[3]} / 有分红 {t[4]} / "
              f"行情日 {t[5]} / 报告期 {t[6]}")
        print(f"完成，总耗时 {time.time()-t0:.1f}s")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
