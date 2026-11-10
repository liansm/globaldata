#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股十大股东 + 十大流通股东（2026-11-10 落地）
================================================
写入 ``stock_shareholders``（一行 = 一只股票 × 一个报告期 × 一类股东 × 一个名次），
**按报告期批量抓取**（一个报告期一次拉全市场），幂等 UPSERT，进度按「报告期×类型」记。

数据源（东方财富免费公开接口，无需 key / 无需登录 / 不碰 PDF）
------------------------------------------------------------------
GET https://datacenter-web.eastmoney.com/api/data/v1/get   source=WEB&client=WEB
  reportName = ``RPT_F10_EH_HOLDERS``       十大股东（按**总股本**口径）
  reportName = ``RPT_F10_EH_FREEHOLDERS``   十大流通股东（按**流通股**口径）
  filter     = ``(END_DATE='2026-09-30')(SECURITY_CODE in ("600519","000001",…))``
  ⚠ ``pageSize`` 硬上限 **500**（与财报接口同一个网关）。
  ⚠ **深分页有硬上限**：只按 END_DATE 过滤、靠 pageNumber 往后翻，翻到 ~offset 5 万行
     （pageNumber 110 左右）就开始返回 ``{"success":false,"message":"服务器繁忙"}``，
     且**之后连浅页也一起被拒**（限流，不是页码越界）。2026 年一个报告期 5.6 万行，
     纯按页码翻**拿不全** —— 所以改成**按代码分批**：每批 250 个代码用
     ``SECURITY_CODE in (...)`` 过滤，一批 ~2,500 行 / 5 页，永不触到深页。
  ⚠ filter 里**日期用单引号**、**字符串用双引号**：
     ``END_DATE='2026-09-30'`` ✓ / ``END_DATE="2026-09-30"`` 不报错但返回 0 行；
     ``SECURITY_CODE="600519"`` ✓ / ``SECURITY_CODE='600519'`` 报「参数预处理错误」。
     ``>=`` / ``in`` 之外的大小比较（``>=``）会直接 ANTLR 解析失败，**只有 = 和 in**。
  ⚠ 按 ``SECUCODE=`` 单只查也行，但全量要走 5,562 次请求 × 2 个端点。

口径（决定数字对不对）
----------------------
* ``kind='top'``（十大股东，按**总股本**算比例）：
    ``total_ratio`` = HOLD_NUM_RATIO（占总股本 %），``float_ratio`` 恒为 NULL。
* ``kind='free'``（十大流通股东，按**流通股**算比例）：
    ``float_ratio`` = FREE_HOLDNUM_RATIO（占**流通股** %）
    ``total_ratio`` = HOLD_RATIO（占**总股本** %）
  ⚠ 两个比例**必须分开存**：A+H / 有未流通股的公司差得很多 ——
    中芯国际 688981 2026H1 HKSCC 占流通股 58.56% 但占总股本只有 54.82%。
    茅台这种几乎全流通的公司两者才近似相等（54.499 vs 54.499），别拿它验口径。
* ``hold_change``（增减股数）与 ``change_text``（增减文字）**分开存**：
  源侧同一个字段 HOLD_NUM_CHANGE 混着数字（"189188123" / "-42000000"）和
  中文（"不变" / "新进"），直接 numeric 化会整批丢值。
* ``END_DATE`` 是**报告期末**（03-31/06-30/09-30/12-31），不是披露日；
  披露日在 free 表叫 ``NOTICE_DATE``，top 表**没有这个字段**（NULL，不硬凑）。
* 比例单位是**百分数**（54.82 = 54.82%），不是小数。
* 源侧按 ``SECURITY_CODE`` 会混 .BJ / 拟上市主体 → **必须按 ``stocks`` 名录过滤**。

增量语义
--------
* 默认（不带 --full）＝增量：只抓**进度文件里没有**的报告期 + 最近 2 个报告期
  （最新一期仍在陆续披露、老期可能被追溯更正，重抓幂等）。
* ``--full``＝按 ``--since``（默认 2016-01-01）全量补齐。
* 一个报告期的两个端点**都成功且写库后**才记进度；失败不记，下次自动补。
* ⚠ 十大股东**不是每期都有**（部分股票只在年报/半年报披露），
  某期「行数比上一期少」是源侧如此，不是抓漏 —— 用 ``--status`` 看按期的行数分布。

用法
----
    python fetch_stock_shareholders.py --status                # 库内状态（按期行数）
    python fetch_stock_shareholders.py --full                  # 首次全量（后台跑，可续跑）
    python fetch_stock_shareholders.py                         # 增量（最近 2 期 + 缺期）
    python fetch_stock_shareholders.py --dry-run --limit-periods 1
    python fetch_stock_shareholders.py --full --codes 600519,688981
    python fetch_stock_shareholders.py --periods 2026-09-30,2026-06-30
"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
BASE_DIR = Path(__file__).parent
PROGRESS = BASE_DIR / ".workbuddy" / "stock_shareholders_progress.json"

WEB = "https://datacenter-web.eastmoney.com/api/data/v1/get"
UA = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
    "Referer": "https://data.eastmoney.com/",
}

DEFAULT_SINCE = "2016-01-01"     # 与财报三表同一起点，便于对照
PAGE_SIZE = 500                  # 东财硬上限
CHUNK_CODES = 250                # 每批代码数：250 × 10 名 ≈ 2,500 行 / 5 页，避开深分页上限
TIMEOUT = 25
WORKERS = 12
REFRESH_RECENT = 2               # 增量时始终重抓最近 N 个报告期（披露未完 / 追溯更正）
SOURCE = "eastmoney_datacenter_RPT_F10_EH_HOLDERS+RPT_F10_EH_FREEHOLDERS"

BUSY_HINTS = ("服务器繁忙", "系统繁忙", "请求过于频繁", "访问过于频繁")

KINDS = (("top", "RPT_F10_EH_HOLDERS"), ("free", "RPT_F10_EH_FREEHOLDERS"))
KIND_LABEL = {"top": "十大股东", "free": "十大流通股东"}

SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS stock_shareholders (
    code            varchar(10) NOT NULL,
    report_date     date NOT NULL,
    kind            varchar(4) NOT NULL,      -- top=十大股东(按总股本) / free=十大流通股东(按流通股)
    holder_rank     integer NOT NULL,
    holder_name     varchar(300),
    holder_code     varchar(40),
    hold_num        numeric(24,2),
    total_ratio     numeric(12,6),            -- 占总股本 %（两类都有）
    float_ratio     numeric(12,6),            -- 占流通股 %（仅 free；top 恒 NULL）
    hold_change     numeric(24,2),            -- 增减股数（源侧是数字时）
    change_text     varchar(24),              -- 增减文字（不变 / 新进 / 加仓 …）
    shares_type     varchar(24),
    holder_type     varchar(24),              -- 股东类型（仅 free：其它/投资公司/基金…）
    is_org          smallint,                 -- IS_HOLDORG 是否机构
    is_dg           smallint,                 -- IS_DJG 是否董监高（仅 free）
    is_sjkzr        smallint,                 -- IS_SJKZR 是否实际控制人（仅 free）
    market_cap      numeric(24,2),            -- 期末持仓市值
    total_shares    numeric(24,2),            -- 总股本（源侧 TOTAL_SHARES_NUM，仅 top 有）
    notice_date     date,                     -- 披露日（仅 free 有，top 为 NULL）
    report_name     varchar(24),              -- 2026三季报（仅 free 有）
    raw             jsonb,
    source          varchar(200),
    updated_at      timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, report_date, kind, holder_rank)
);
CREATE INDEX IF NOT EXISTS idx_stock_shareholders_code
    ON stock_shareholders(code, report_date DESC);
"""

_local = threading.local()


def session() -> requests.Session:
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update(UA)
        _local.s = s
    return s


def num(v):
    if v is None or v == "-" or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def day(v):
    if not v or v == "-":
        return None
    s = str(v)[:10]
    return s if len(s) == 10 and s[4] == "-" else None


def smallint(v):
    if v in (None, "", "-"):
        return None
    s = str(v).strip()
    return 1 if s in ("1", "true", "True") else 0


def dc_page(rname: str, filt: str, page: int) -> dict:
    """打一页。成功帧 {"success":true,"result":{...}}；0 行也是 success。

    ⚠ 日期用单引号、字符串用双引号包；两种写错都不报错，只是返回 0 行 / 解析失败。
    ⚠ "服务器繁忙" 是**限流**（深分页触发），退避重试；本脚本按代码分批，正常情况下不会遇到。
    """
    p = {"reportName": rname, "columns": "ALL", "filter": filt,
         "pageNumber": page, "pageSize": PAGE_SIZE,
         "sortColumns": "SECURITY_CODE", "sortTypes": "1",
         "source": "WEB", "client": "WEB"}
    last = None
    for i in range(5):
        try:
            r = session().get(WEB, params=p, timeout=TIMEOUT)
            r.raise_for_status()
            j = r.json()
            if j.get("success") is False:
                msg = str(j.get("message") or "")
                if any(k in msg for k in ("返回数据为空", "报表配置不存在", "列不存在",
                                          "排序列不存在")):
                    return {"count": 0, "pages": 0, "data": []}
                raise RuntimeError(f"源侧失败: {msg[:80]}")
            res = j.get("result") or {}
            count = res.get("count") or 0
            # ⚠ 别直接信 pages：末页越界时源侧返「服务器繁忙」，按 count 自己算更稳
            pages = min(res.get("pages") or 0,
                        (count + PAGE_SIZE - 1) // PAGE_SIZE if count else 0)
            return {"count": count, "pages": pages, "data": res.get("data") or []}
        except Exception as exc:                       # noqa: BLE001
            last = exc
            if i < 4:
                time.sleep(min(1.0 * (2 ** i), 8.0))
    raise RuntimeError(f"{type(last).__name__}: {str(last)[:100]}")


def _chunks(codes: set[str], n: int) -> list[list[str]]:
    lst = sorted(codes)
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def _chunk_filter(rd: str, codes: list[str]) -> str:
    quoted = ",".join(f'"{c}"' for c in codes)
    return f"(END_DATE='{rd}')(SECURITY_CODE in ({quoted}))"


def fetch_kind_period(rname: str, rd: str, codes: set[str],
                      pool: ThreadPoolExecutor) -> list[dict]:
    """一个报告期 × 一个端点：**按代码分批**，每批先拿首页再并发翻剩余页。

    任一页失败直接抛 → 整个报告期不记进度（绝不把半截结果标成功）。
    """
    rows: list[dict] = []
    chunks = _chunks(codes, CHUNK_CODES)
    firsts = {pool.submit(dc_page, rname, _chunk_filter(rd, ch), 1): ch for ch in chunks}
    more = {}
    for f in as_completed(firsts):
        ch = firsts[f]
        res = f.result()
        rows.extend(res["data"])
        for p in range(2, (res["pages"] or 1) + 1):
            more[pool.submit(dc_page, rname, _chunk_filter(rd, ch), p)] = p
    for f in as_completed(more):
        rows.extend(f.result()["data"])
    return rows


def build_rows(kind: str, records: list[dict], codes: set[str], rd: str,
               with_raw: bool) -> tuple[list[tuple], int]:
    out, seen, dropped = [], set(), 0
    for r in records:
        code = str(r.get("SECURITY_CODE") or "").strip()
        rank = r.get("HOLDER_RANK")
        if code not in codes or not rank:
            dropped += 1
            continue
        key = (code, int(rank))
        if key in seen:
            dropped += 1
            continue
        seen.add(key)

        raw_change = r.get("HOLD_NUM_CHANGE") or r.get("HOLD_CHANGE")
        change_num = num(raw_change)
        change_text = None if change_num is not None else (
            str(raw_change).strip()[:24] if raw_change not in (None, "") else None)

        if kind == "top":
            total_ratio, float_ratio = num(r.get("HOLD_NUM_RATIO")), None
        else:
            total_ratio, float_ratio = num(r.get("HOLD_RATIO")), num(r.get("FREE_HOLDNUM_RATIO"))

        name = str(r.get("HOLDER_NAME") or "").strip()[:300]
        out.append((
            code, rd, kind, int(rank), name, (str(r.get("HOLDER_CODE") or "")[:40] or None),
            num(r.get("HOLD_NUM")), total_ratio, float_ratio,
            change_num, change_text,
            (str(r.get("SHARES_TYPE") or "")[:24] or None),
            (str(r.get("HOLDER_TYPE") or "")[:24] or None),
            smallint(r.get("IS_HOLDORG")), smallint(r.get("IS_DJG")), smallint(r.get("IS_SJKZR")),
            num(r.get("HOLDER_MARKET_CAP")), num(r.get("TOTAL_SHARES_NUM")),
            day(r.get("NOTICE_DATE")), (str(r.get("REPORT_DATE_NAME") or "")[:24] or None),
            Json({k: v for k, v in r.items() if v not in (None, "")} if with_raw else {}),
            SOURCE,
        ))
    return out, dropped


UPSERT = """
INSERT INTO stock_shareholders (
    code, report_date, kind, holder_rank, holder_name, holder_code, hold_num,
    total_ratio, float_ratio, hold_change, change_text, shares_type, holder_type,
    is_org, is_dg, is_sjkzr, market_cap, total_shares, notice_date, report_name,
    raw, source)
VALUES %s
ON CONFLICT (code, report_date, kind, holder_rank) DO UPDATE SET
    holder_name = EXCLUDED.holder_name,
    holder_code = EXCLUDED.holder_code,
    hold_num    = EXCLUDED.hold_num,
    total_ratio = EXCLUDED.total_ratio,
    float_ratio = EXCLUDED.float_ratio,
    hold_change = EXCLUDED.hold_change,
    change_text = EXCLUDED.change_text,
    shares_type = EXCLUDED.shares_type,
    holder_type = EXCLUDED.holder_type,
    is_org      = EXCLUDED.is_org,
    is_dg       = EXCLUDED.is_dg,
    is_sjkzr    = EXCLUDED.is_sjkzr,
    market_cap  = EXCLUDED.market_cap,
    total_shares= EXCLUDED.total_shares,
    notice_date = EXCLUDED.notice_date,
    report_name = EXCLUDED.report_name,
    raw         = EXCLUDED.raw,
    source      = EXCLUDED.source,
    updated_at  = NOW()
"""


def period_list(since: str, today: date) -> list[str]:
    out = []
    for y in range(int(since[:4]), today.year + 1):
        for md in ("03-31", "06-30", "09-30", "12-31"):
            d = f"{y}-{md}"
            if d >= since and date.fromisoformat(d) <= today:
                out.append(d)
    return out


def load_progress() -> dict:
    if PROGRESS.exists():
        try:
            d = json.loads(PROGRESS.read_text(encoding="utf-8"))
            if isinstance(d, dict):
                d.setdefault("done", {})
                return d
        except Exception:                              # noqa: BLE001
            pass
    return {"done": {}}                                # {报告期: ["top","free"]}


def save_progress(p: dict) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(p, ensure_ascii=False, indent=1), encoding="utf-8")


def load_codes(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT code FROM stocks")
        return {r[0] for r in cur.fetchall()}


def status(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT report_date, kind, COUNT(*) , COUNT(DISTINCT code)
            FROM stock_shareholders GROUP BY 1, 2 ORDER BY 1 DESC, 2
        """)
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM stock_shareholders")
        total = cur.fetchone()[0]
        cur.execute("SELECT MAX(report_date) FROM stock_shareholders")
        latest = cur.fetchone()[0]
    print(f"总行数 {total:,} · 最新报告期 {latest}")
    print(f"{'报告期':<12}{'类型':<6}{'行数':>9}{'股票数':>9}")
    for rd, kind, n, c in rows:
        print(f"{str(rd):<12}{kind:<6}{n:>9,}{c:>9,}")


def main() -> None:
    ap = argparse.ArgumentParser(description="A股十大股东 / 十大流通股东")
    ap.add_argument("--full", action="store_true", help="按 --since 全量补齐（默认增量）")
    ap.add_argument("--since", default=DEFAULT_SINCE, help="起始报告期（默认 2016-01-01）")
    ap.add_argument("--periods", help="指定报告期，逗号分隔，如 2026-09-30,2026-06-30")
    ap.add_argument("--codes", help="只跑这些代码（联调用），逗号分隔")
    ap.add_argument("--limit-periods", type=int, default=0, help="只跑最近 N 个报告期")
    ap.add_argument("--with-raw", action="store_true", help="额外存整条原始报文（默认不存，省磁盘）")
    ap.add_argument("--dry-run", action="store_true", help="只拉不写库")
    ap.add_argument("--status", action="store_true", help="只看库内状态")
    args = ap.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
    if args.status:
        status(conn)
        return

    today = date.today()
    if args.periods:
        periods = [p.strip() for p in args.periods.split(",") if p.strip()]
    else:
        periods = period_list(args.since, today)
        if args.limit_periods:
            periods = periods[-args.limit_periods:]

    progress = load_progress()
    done = progress["done"]
    recent = set(periods[-REFRESH_RECENT:])
    if args.full:
        pass                                     # 全量：按 --since 的每一期都跑
    elif args.periods:
        pass                                     # 显式指定：按用户给的跑
    else:
        # 增量：缺期 + 最近 N 期（最新一期仍在陆续披露、老期会被追溯更正，重抓幂等）
        periods = [p for p in periods if p in recent or len(done.get(p, [])) < len(KINDS)]

    codes = (set(c.strip() for c in args.codes.split(",") if c.strip())
             if args.codes else load_codes(conn))
    if not periods:
        print("没有需要抓取的报告期（全部已完成）。要强制重跑用 --full 或 --periods。")
        return
    print(f"报告期 {len(periods)} 个（{periods[0]} ~ {periods[-1]}）· 名录 {len(codes)} 只"
          f"{' · DRY-RUN' if args.dry_run else ''}", flush=True)

    t0 = time.time()
    total_rows = 0
    for i, rd in enumerate(periods, 1):
        ok = True
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for kind, rname in KINDS:
                try:
                    recs = fetch_kind_period(rname, rd, codes, pool)
                except Exception as exc:                  # noqa: BLE001
                    print(f"[{i}/{len(periods)}] {rd} {kind} 抓取失败：{exc}", flush=True)
                    ok = False
                    break                                  # 断点：该期没抓全，不记完整进度
                rows, dropped = build_rows(kind, recs, codes, rd, args.with_raw)
                if not args.dry_run and rows:
                    with conn.cursor() as cur:
                        execute_values(cur, UPSERT, rows, page_size=500)
                total_rows += len(rows)
                if kind not in done.setdefault(rd, []):
                    done[rd].append(kind)
                print(f"[{i}/{len(periods)}] {rd} {KIND_LABEL[kind]}: "
                      f"源侧 {len(recs):,} 行 → 入库 {len(rows):,}"
                      f"（过滤/去重 {dropped:,}）", flush=True)
        if not args.dry_run:
            save_progress(progress)
        if not ok:
            print(f"⚠ {rd} 有端点失败，已写入的部分保留，下次自动补", flush=True)

    print(f"完成：{len(periods)} 个报告期 · {total_rows:,} 行 · "
          f"耗时 {time.time() - t0:.0f}s", flush=True)
    if not args.dry_run:
        status(conn)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
