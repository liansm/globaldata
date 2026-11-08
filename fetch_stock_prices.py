#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股历史日线（增量刷新主脚本）
==============================
写入 ``stock_prices``（不复权日线 OHLCV + 成交量 + 成交额），并维护
``stocks.latest_price_date``。基本信息在同目录 ``fetch_stock_info.py``。

这是 ``refresh_all.py`` 接入的**唯一股价刷新入口**：默认增量、可断点续跑、
二次运行 0 请求 0 新增。周 / 月 / 年线不落库，由 API 用 date_trunc 聚合日线。

数据源（**免费、无需登录 / 无需 key**）
---------------------------------------
1) 主源（逐只全序列）：akshare ``stock_zh_a_hist(symbol, period='daily',
   start_date, end_date, adjust='')`` → 东方财富 push2his K 线接口。
   **一次请求返回 start_date 之后的全部历史**（贵州茅台 1990 起 6037 行只要 2.8 秒），
   所以这是理想的 L2「单只全历史」源，不需要分页。
   字段：日期/开盘/收盘/最高/最低/成交量(手)/成交额(元)/振幅/涨跌幅/涨跌额/换手率。
   ⚠ ``adjust=''`` = **不复权**。除权日会有跳空，禁止与 qfq/hfq 序列拼接。

2) 源侧最新交易日：东财 clist 第 1 页的 ``f297``（1 个请求），用来判断
   「今天是不是已经收盘更新了」，避免每次都全市场发一遍请求。

交叉校验（探针实测，2026-11-08）
--------------------------------
东财 ``stock/get`` 快照端点（f43/f44/f45/f46/f47/f48）与 ``stock_zh_a_hist``
最近一日逐字段**完全一致**，8/8 只覆盖沪主板 / 深主板 / 创业板 / 科创板 / 新股：
开、高、低、收、成交量、成交额全部相等。``--verify`` 会重跑这项对照。

为什么不用 clist 全市场快照当日线（虽然只要 9 秒）
--------------------------------------------------
clist 给的是「当前快照」，盘中是**未完成的一根 K 线**，且只给最新一天 ——
漏跑几天就永久缺几天，无法自愈。逐只 hist 从库内最新日开抓，天然补洞、
口径与历史一致。代价是首次约数千个请求，但 12~16 线程下是可接受的一次性成本；
日常增量若已到源侧最新日则 0 请求。

增量语义 / 铁律
---------------
* ``stocks.latest_price_date`` / ``price_checked_date`` 是本脚本的**唯一进度**，
  都在事务提交后更新；``fetch_stock_info.py`` 的名录 UPSERT **不碰这两列**。
  * ``latest_price_date`` = 该股实际最新的日线日期（从 stock_prices 反查）。
  * ``price_checked_date`` = 最近一次「源侧查过、确认没有更新」的行情日。
    退市 / 长期停牌股的 latest 永远追不上源侧最新交易日，没有这列就会每天白抓
    300 多只（实测 345 只 / 30 秒）。新交易日到来时 src_latest 变大 → 自然再抓一次。
* 抓从 ``latest_price_date`` 当天开始（重抓最新日，用 ON CONFLICT 覆盖，
  可修正盘中/停牌造成的半成品行）；没抓过的按 ``--years`` 回填（默认近 5 年）。
* 失败**绝不记进度**：网络/限流异常打印 WARN 且不更新时间戳，下次自动重试；
  源侧明确返回空序列（新股未上市等）视为成功，写时间戳避免无限重抓。
* ``--full`` 从 1990-01-01 抓全历史，用 ``.workbuddy/stock_prices_full_progress.json``
  断点续跑，进度只在 commit 成功后写。
* 批内按 (code, trade_date) 去重 —— PG 的 ON CONFLICT 不允许同一条命令命中
  同一行两次（CardinalityViolation 会整批回滚）。
* 脏数据守卫：close<=0 / 日期>今天 的行显式跳过并计数，绝不静默丢弃。

用法
----
    python fetch_stock_prices.py                 # 增量（refresh_all 用这个）
    python fetch_stock_prices.py --full         # 全历史回填，后台跑、可续跑
    python fetch_stock_prices.py --years 10     # 首次回填近 10 年（默认 5）
    python fetch_stock_prices.py --limit 50     # 联调：只跑前 50 只
    python fetch_stock_prices.py --codes 600519,000001
    python fetch_stock_prices.py --verify 20    # 抽 20 只与源逐日对照 + 快照对照
    python fetch_stock_prices.py --dry-run
"""

import argparse
import json
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

import akshare as ak

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
BASE_DIR = Path(__file__).parent
PROGRESS_FILE = BASE_DIR / ".workbuddy" / "stock_prices_full_progress.json"

UA = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Referer": "https://quote.eastmoney.com/",
}
UT = "bd1d9ddb04089700cf9c27f6f7426281"
CLIST = "https://push2.eastmoney.com/api/qt/clist/get"
SGET = "https://push2.eastmoney.com/api/qt/stock/get"
FS_A = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"

DEFAULT_WORKERS = 12
DEFAULT_YEARS = 5
CHUNK = 100
TIMEOUT = 25
RETRIES = 2

# ─────────────────────────────────────────────────────────────────────────────
# DDL（幂等，与 src/db/schema.ts 对齐；与 fetch_stock_info.py 各持一份，可独立跑）
# ─────────────────────────────────────────────────────────────────────────────
SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS stocks (
    code                varchar(10) PRIMARY KEY,
    name                varchar(120) NOT NULL,
    name_search         text,
    company_name        text,
    name_en             text,
    former_name         text,
    introduction        text,
    main_business       text,
    business_scope      text,
    total_shares        numeric(24,2),
    float_shares        numeric(24,2),
    registered_capital  numeric(20,4),
    market_cap          numeric(24,2),
    float_market_cap    numeric(24,2),
    industry            varchar(120),
    industry_csrc       varchar(120),
    region              varchar(40),
    exchange            varchar(20) NOT NULL,
    board               varchar(20) NOT NULL,
    listing_date        date,
    founded_date        date,
    legal_person        varchar(100),
    registered_address  text,
    office_address      text,
    website             text,
    email               text,
    phone               text,
    latest_price        numeric(16,4),
    change_pct          numeric(10,4),
    quote_date          date,
    latest_price_date   date,
    profile_updated_at  timestamptz,
    boards_updated_at   timestamptz,
    source              varchar(200),
    updated_at          timestamptz NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_stocks_name            ON stocks(name);
CREATE INDEX IF NOT EXISTS idx_stocks_name_search     ON stocks(name_search);
CREATE INDEX IF NOT EXISTS idx_stocks_exchange_board  ON stocks(exchange, board);
CREATE INDEX IF NOT EXISTS idx_stocks_region          ON stocks(region);
CREATE INDEX IF NOT EXISTS idx_stocks_industry        ON stocks(industry);
CREATE INDEX IF NOT EXISTS idx_stocks_industry_csrc   ON stocks(industry_csrc);

CREATE TABLE IF NOT EXISTS stock_industries (
    id          bigserial PRIMARY KEY,
    code        varchar(10)  NOT NULL,
    kind        varchar(12)  NOT NULL,
    name        varchar(120) NOT NULL,
    board_code  varchar(16),
    CONSTRAINT stock_industries_uniq UNIQUE (code, kind, name)
);
CREATE INDEX IF NOT EXISTS idx_stock_industries_name ON stock_industries(name);
CREATE INDEX IF NOT EXISTS idx_stock_industries_code ON stock_industries(code);

CREATE TABLE IF NOT EXISTS stock_prices (
    id             bigserial PRIMARY KEY,
    code           varchar(10) NOT NULL,
    trade_date     date        NOT NULL,
    open           numeric(16,4),
    high           numeric(16,4),
    low            numeric(16,4),
    close          numeric(16,4),
    volume         numeric(24,2),
    turnover       numeric(24,2),
    amplitude      numeric(10,4),
    change_pct     numeric(10,4),
    change_amt     numeric(16,4),
    turnover_rate  numeric(10,4),
    CONSTRAINT stock_prices_uniq UNIQUE (code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_stock_prices_code_date ON stock_prices(code, trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date      ON stock_prices(trade_date);

ALTER TABLE stocks ADD COLUMN IF NOT EXISTS price_checked_date date;
"""

# 退市 / 停牌股每次都会返回「源侧无新数据」，也要记下已核对的行情日，
# 否则它们每天都会被重抓（实测 345 只 / 30 秒 / 写 198 行覆盖）。
MARK_CHECKED = "UPDATE stocks SET price_checked_date = %s WHERE code = ANY(%s)"

UPSERT_PRICES = """
INSERT INTO stock_prices (code, trade_date, open, high, low, close, volume, turnover,
                          amplitude, change_pct, change_amt, turnover_rate)
VALUES %s
ON CONFLICT (code, trade_date) DO UPDATE SET
    open          = EXCLUDED.open,
    high          = EXCLUDED.high,
    low           = EXCLUDED.low,
    close         = EXCLUDED.close,
    volume        = EXCLUDED.volume,
    turnover      = EXCLUDED.turnover,
    amplitude     = EXCLUDED.amplitude,
    change_pct    = EXCLUDED.change_pct,
    change_amt    = EXCLUDED.change_amt,
    turnover_rate = EXCLUDED.turnover_rate
"""

MARK_LATEST = """
UPDATE stocks s SET latest_price_date = p.d
FROM (SELECT code, MAX(trade_date) AS d FROM stock_prices
      WHERE code = ANY(%s) GROUP BY code) p
WHERE s.code = p.code
"""


# ─────────────────────────────────────────────────────────────────────────────
# 工具
# ─────────────────────────────────────────────────────────────────────────────
_tls = threading.local()


def session() -> requests.Session:
    if not hasattr(_tls, "s"):
        s = requests.Session()
        s.headers.update(UA)
        _tls.s = s
    return _tls.s


def f(v):
    """pandas 标量 → float|None（NaN / '-' / 空串一律 None）。"""
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def parse_day(v):
    """'2026-11-06' / pandas.Timestamp / 20261106 → date。"""
    if v is None:
        return None
    if isinstance(v, date):
        return v
    s = str(v).strip()[:10].replace("/", "-")
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def source_latest_date() -> date | None:
    """东财 clist 第 1 页 f297 = 源侧行情日期（周末/盘后就是最近交易日）。"""
    try:
        j = session().get(CLIST, params={
            "pn": 1, "pz": 1, "po": 1, "np": 1, "ut": UT, "fltt": 2, "invt": 2,
            "fid": "f12", "fs": FS_A, "fields": "f12,f297", "_": int(time.time() * 1000),
        }, timeout=TIMEOUT).json()
        d = (j.get("data") or {}).get("diff") or []
        first = (d[0] if isinstance(d, list) else list(d.values())[0]) if d else {}
        v = first.get("f297")
        if v:
            s = str(int(v))
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except Exception as exc:                     # noqa: BLE001
        print(f"[WARN] 取源侧最新交易日失败: {type(exc).__name__}: {exc}", flush=True)
    return None


def fetch_hist(code: str, start: date, end: date):
    """返回 (rows, rejected)；异常向上抛，由调用方决定不记进度。"""
    df = ak.stock_zh_a_hist(symbol=code, period="daily",
                            start_date=start.strftime("%Y%m%d"),
                            end_date=end.strftime("%Y%m%d"), adjust="")
    if df is None or df.empty:
        return [], 0
    rows, rejected = [], 0
    today = date.today()
    for rec in df.to_dict("records"):
        d = parse_day(rec.get("日期"))
        close = f(rec.get("收盘"))
        vol = f(rec.get("成交量"))
        # 脏数据守卫：停牌占位 / 未来日期一律不入库，计数要打印出来
        if d is None or d > today or close is None or close <= 0 or (vol is not None and vol < 0):
            rejected += 1
            continue
        rows.append((
            code, d,
            f(rec.get("开盘")), f(rec.get("最高")), f(rec.get("最低")), close,
            vol, f(rec.get("成交额")), f(rec.get("振幅")), f(rec.get("涨跌幅")),
            f(rec.get("涨跌额")), f(rec.get("换手率")),
        ))
    # 批内按主键去重（PG 同条命令命中同一行两次会整批回滚）
    dedup = {(r[0], r[1]): r for r in rows}
    return list(dedup.values()), rejected + (len(rows) - len(dedup))


# ─────────────────────────────────────────────────────────────────────────────
# 校验：库内序列 vs 源全序列 + 东财快照端点
# ─────────────────────────────────────────────────────────────────────────────
def verify(conn, codes: list[str], workers: int):
    print(f"\n[verify] 抽样 {len(codes)} 只：库内日线 vs 源全序列 + 东财快照端点", flush=True)
    with conn.cursor() as cur:
        cur.execute("SELECT code, MIN(trade_date), MAX(trade_date), COUNT(*) "
                    "FROM stock_prices WHERE code = ANY(%s) GROUP BY code", (codes,))
        meta = {r[0]: r for r in cur.fetchall()}
        cur.execute("SELECT code, trade_date, open, high, low, close, volume, turnover "
                    "FROM stock_prices WHERE code = ANY(%s)", (codes,))
        lib = {}
        for r in cur.fetchall():
            lib[(r[0], str(r[1]))] = tuple(None if x is None else float(x) for x in r[2:])

    def one(code):
        m = meta.get(code)
        if not m:
            return code, "库内无数据"
        start = m[1] if isinstance(m[1], date) else date.fromisoformat(str(m[1])[:10])
        try:
            rows, _ = fetch_hist(code, start, date.today())
        except Exception as exc:                 # noqa: BLE001
            return code, f"源侧异常 {type(exc).__name__}: {exc}"
        src = {(r[0], str(r[1])): r for r in rows}
        lib_dates = {k[1] for k in lib if k[0] == code}
        src_dates = {k[1] for k in src if k[0] == code}
        missing = src_dates - lib_dates          # 库内空洞
        extra = lib_dates - src_dates            # 库内多出来的日期
        diffs = []
        # src / lib 的键都是 (code, date) 元组，交集必须按元组取，
        # 不能拿日期字符串集合去索引（会 KeyError）
        for k in sorted(src.keys() & lib.keys()):
            s = src[k]
            got = lib[k]
            expect = tuple(None if x is None else float(x) for x in s[2:8])
            for i, (a, b) in enumerate(zip(expect, got)):
                if a is None or b is None:
                    if a != b:
                        diffs.append((k[1], i, a, b))
                elif abs(a - b) > max(0.011, abs(a) * 1e-6):
                    diffs.append((k[1], i, a, b))
        return code, {"lib": len(lib_dates), "src": len(src_dates),
                      "missing": len(missing), "extra": len(extra), "diff": len(diffs),
                      "sample": diffs[:2]}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(one, codes))
    bad = 0
    for code, r in results:
        if isinstance(r, str):
            print(f"  {code}: [SKIP] {r}")
            bad += 1
        elif r["missing"] or r["extra"] or r["diff"]:
            print(f"  {code}: [FAIL] {r}")
            bad += 1
        else:
            print(f"  {code}: OK  {r['lib']} 个交易日，日期集合与逐日 6 字段全等")
    print(f"[verify] {len(codes)-bad}/{len(codes)} 只通过\n")
    return bad == 0


def verify_snapshot(codes: list[str]):
    """不同端点交叉：东财 stock/get 最新日 OHLCV vs 库内最新行。"""
    print("[verify] stock/get 快照 vs 库内最新日（独立端点）", flush=True)
    conn = psycopg2.connect(DATABASE_URL)
    try:
        bad = 0
        with conn.cursor() as cur:
            for code in codes:
                cur.execute("SELECT trade_date, open, high, low, close, volume, turnover "
                            "FROM stock_prices WHERE code=%s ORDER BY trade_date DESC LIMIT 1", (code,))
                row = cur.fetchone()
                if not row:
                    continue
                mkt = "1" if code.startswith(("6", "9")) else "0"
                j = session().get(SGET, params={
                    "secid": f"{mkt}.{code}", "ut": UT, "fltt": 2, "invt": 2,
                    "fields": "f43,f44,f45,f46,f47,f48", "_": int(time.time() * 1000),
                }, timeout=TIMEOUT).json()
                d = j.get("data") or {}
                expect = [d.get(k) for k in ("f46", "f44", "f45", "f43", "f47", "f48")]
                got = [float(x) for x in row[1:]]
                ok = all(a is not None and abs(float(a) - b) <= max(0.011, abs(b) * 1e-6)
                         for a, b in zip(expect, got))
                print(f"  {code} ({row[0]}): {'OK' if ok else 'FAIL'} "
                      f"库内={[round(x,2) for x in got]} 快照={expect}")
                bad += 0 if ok else 1
        print(f"[verify] 快照对照失败 {bad} 只\n")
        return bad == 0
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="A 股历史日线（默认增量；--full 全历史）")
    ap.add_argument("--full", action="store_true", help="从 1990-01-01 抓全历史（可续跑）")
    ap.add_argument("--years", type=int, default=DEFAULT_YEARS, help="首次回填年数，默认 5")
    ap.add_argument("--dry-run", action="store_true", help="只抓不写库")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 只（联调）")
    ap.add_argument("--codes", default="", help="逗号分隔的指定代码")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--verify", type=int, default=0, help="抽 N 只与源逐日对照")
    args = ap.parse_args()
    args.codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    t0 = time.time()
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SQL_ENSURE)

        if args.verify:
            # 只从**已入库日线**的代码里均匀抽样：全量回填中途跑 verify 时，
            # 抽到还没抓的股票会报「库内无数据」，那是进度不是 bug。
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT code FROM stock_prices ORDER BY code")
                all_codes = [r[0] for r in cur.fetchall()]
            if not all_codes:
                print("[verify] 库内还没有日线，先跑一次回填")
                return 1
            step = max(1, len(all_codes) // args.verify)
            sample = all_codes[::step][:args.verify]
            ok1 = verify(conn, sample, args.workers)
            ok2 = verify_snapshot(sample[:8])
            return 0 if ok1 and ok2 else 2

        with conn.cursor() as cur:
            cur.execute("SELECT code, latest_price_date, price_checked_date FROM stocks ORDER BY code")
            state = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        if not state:
            print("[FATAL] stocks 表为空 —— 先跑 python fetch_stock_info.py 建名录", flush=True)
            return 1
        print(f"[1/4] 库内股票 {len(state)} 只", flush=True)

        src_latest = source_latest_date()
        today = date.today()
        print(f"      源侧最新行情日 = {src_latest or '未知'}，今天 = {today}", flush=True)

        codes = sorted(state)
        if args.codes:
            codes = [c for c in codes if c in set(args.codes)]
        if args.limit:
            codes = codes[:args.limit]

        done = set()
        if args.full and PROGRESS_FILE.exists():
            try:
                done = set(json.loads(PROGRESS_FILE.read_text(encoding="utf-8")))
            except Exception:                    # noqa: BLE001
                print("[WARN] 进度文件损坏，从头重跑")

        default_start = today - timedelta(days=365 * args.years)
        jobs, skipped = [], 0
        for c in codes:
            if args.full:
                if c in done:
                    skipped += 1
                    continue
                start = date(1990, 1, 1)
            elif args.codes:
                start = state[c][0] or default_start
            elif src_latest:
                latest, checked = state[c]
                # 已有最新日，或「已核对过这个源侧行情日且确认无新数据」→ 跳过。
                # 后者是退市/长期停牌股：它们的 latest 永远追不上 src_latest，
                # 没有 price_checked_date 就会每天白抓 300 多只。
                if (latest and latest >= src_latest) or (checked and checked >= src_latest):
                    skipped += 1                # 已是最新 → 0 请求（幂等复跑的关键）
                    continue
                start = latest or default_start
            else:
                start = state[c][0] or default_start
            jobs.append((c, start))
        print(f"[2/4] 待抓 {len(jobs)} 只，跳过 {skipped} 只（已到最新/已续跑）", flush=True)

        if args.dry_run:
            for c, s in jobs[:5]:
                try:
                    rows, rej = fetch_hist(c, s, today)
                    print(f"  [dry-run] {c} start={s} → {len(rows)} 行，过滤 {rej} 行")
                except Exception as exc:         # noqa: BLE001
                    print(f"  [dry-run] {c} 失败 {type(exc).__name__}: {exc}")
            return 0

        stats = {"rows": 0, "rejected": 0, "failed": 0, "done": 0}
        progress = set(done)

        def task(job):
            code, start = job
            last = None
            for i in range(RETRIES + 1):
                try:
                    return code, fetch_hist(code, start, today), None
                except Exception as exc:         # noqa: BLE001
                    last = exc
                    if i < RETRIES:
                        time.sleep(0.8 * (2 ** i))
            return code, None, f"{type(last).__name__}: {str(last)[:80]}"

        for beg in range(0, len(jobs), CHUNK):
            batch = jobs[beg:beg + CHUNK]
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                results = list(ex.map(task, batch))

            ok_codes, wrote, rej = [], 0, 0
            with conn:
                with conn.cursor() as cur:
                    for code, payload, error in results:
                        if error:
                            stats["failed"] += 1
                            print(f"      [WARN] {code} 抓取失败（不记进度）: {error}", flush=True)
                            continue
                        rows, rejected = payload
                        rej += rejected
                        if rows:
                            execute_values(cur, UPSERT_PRICES, rows, page_size=1000)
                            wrote += len(rows)
                        ok_codes.append(code)     # 源侧空序列也算成功，避免无限重抓
                    if ok_codes:
                        cur.execute(MARK_LATEST, (ok_codes,))
                        # 「已核对到 src_latest」与写库在**同一个事务**里提交；
                        # 失败的代码不在 ok_codes 里，下次自动重试。
                        if src_latest:
                            cur.execute(MARK_CHECKED, (src_latest, ok_codes))
            # commit 成功后才写进度文件
            if args.full and ok_codes:
                progress.update(ok_codes)
                PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
                PROGRESS_FILE.write_text(json.dumps(sorted(progress), ensure_ascii=False),
                                         encoding="utf-8")
            stats["rows"] += wrote
            stats["rejected"] += rej
            stats["done"] += len(ok_codes)
            print(f"      [{stats['done']}/{len(jobs)}] 本批 {len(batch)} 只 → 写 {wrote} 行，"
                  f"过滤 {rej} 行，累计耗时 {time.time()-t0:.1f}s", flush=True)

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS rows, COUNT(DISTINCT code) AS codes, "
                        "MIN(trade_date)::text AS earliest, MAX(trade_date)::text AS latest "
                        "FROM stock_prices")
            t = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM stocks WHERE latest_price_date IS NULL")
            no_price = cur.fetchone()[0]
        print(f"[3/4] stock_prices: {t[0]} 行 / {t[1]} 只 / {t[2]} ~ {t[3]}")
        print(f"[4/4] 本轮写 {stats['rows']} 行，过滤脏数据 {stats['rejected']} 行，"
              f"失败 {stats['failed']} 只（未记进度，下次自动补），"
              f"库内仍无日线的股票 {no_price} 只")
        print(f"完成，总耗时 {time.time()-t0:.1f}s")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
