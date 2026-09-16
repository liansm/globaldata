#!/usr/bin/env python3
"""
Public Fund NAV Fetcher (公募基金净值 —— 快照 + 全历史)
======================================================
写入两块数据：

 1. funds  快照列  — latest_nav / latest_acc_nav / latest_nav_date /
                     latest_daily_return / nav_kind（列表页用，避免每次扫 fund_nav）
 2. fund_nav      — 净值日线全历史（份额口径，A/C 类分开）

Data sources
------------
* L1 批量快照（默认，**4 个请求拿全市场当日净值**）
      https://fund.eastmoney.com/data/rankhandler.aspx
          ?op=ph&dt=kf&ft=all&pn=10000   → 20346 只（开放式非货币）
          ?op=ph&dt=hb&pn=10000          →   543 只（货币基金）
      字段（行内 CSV，非 JSON）：[0]代码 [1]名称 [2]拼音 [3]净值日期
                                 [4]单位净值 [5]累计净值 [6]日增长率%
      ⚠ 货币基金行（dt=hb）的 [4]=万份收益(元) [5]=七日年化(%)，
        且 **[6] 不是日增长率**（是区间收益），因此货币基金不写 daily_return。
* L2 全历史回填（--history，**1 只 1 请求拿全序列**）
      https://fund.eastmoney.com/pingzhongdata/<code>.js
        Data_netWorthTrend      [{x:ms, y:单位净值, equityReturn:日增长, ...}]
        Data_ACWorthTrend       [[ms, 累计净值], ...]
        Data_millionCopiesIncome    [[ms, 万份收益]]        ← 货币基金走这条
        Data_sevenDaysYearIncome    [[ms, 七日年化]]        ← 货币基金走这条
      ⚠ 时间戳是 **UTC 零点**，必须按 UTC+8 换算才是交易日（用 localtime 在
        非 +8 时区会整体错一天）。
      ⚠ 货币基金没有 Data_netWorthTrend，只看 netWorthTrend 会把 901 只货币基金
        全判成「无数据」。
* L3 补缺口（--gap）  L1 覆盖不到的基金（场内 ETF/LOF、定开、货币缺口）
      https://api.fund.eastmoney.com/f10/lsjz?fundCode=..&pageIndex=1&pageSize=20
      需 Referer: https://fundf10.eastmoney.com/jjjz_<code>.html
      ⚠ **pageSize 被服务端夹在 20**（传更大值时偶发返回空列表），
        所以它只能补最近 20 个交易日，**拉不了全历史**（6007 点要 301 个请求）。

频控（务必看懂再改）
--------------------
东财有 HTTP 514 限流（非标准码、body 空、**随机失败**）。实测：
    fundf10.eastmoney.com 串行 0s → 83% 成功、0.5s → 100%、8 线程无节流 → 27%
    fund.eastmoney.com/pingzhongdata 8 线程无节流 → 60 个请求出现 2 个 514
因此本脚本复用 fetch_funds.py 的全局 RateLimiter（跨线程统一发牌），
并且**请求失败绝不写进度文件** —— 否则缺口会被永久跳过（这个坑已经吃过两次：
2026-09-10 白丢 2700 只基金持仓、28% 规模缺口）。

覆盖边界（不是 bug）
--------------------
* L1 只覆盖开放式非货币 + 部分货币，对库内 27874 只覆盖 73%。
  场内 ETF/LOF、定开债 没有批量入口（dt=fb/zs/lof/etf 均返回 0 条）。
* 名称含「(后端)」的 190 只三源皆无净值 —— 后端份额不单独公布净值，
  与库内 27% 份额缺规模/公司是同根因。**覆盖率天花板不是 100%。**
* 清盘/终止上市的基金 pingzhongdata 仍可能有历史，按正常数据处理。

口径特性（2026-09-16 全量核验结论 —— 值本身正确，**别当 bug 去"修"**）
--------------------------------------------------------------------
* 货币基金的万份收益**含节假日合并披露**：节后首个披露日的值是假期多日收益之和。
  实测 000425 `2014-02-06 = 11.4874 ≈ 节前 1.6385 × 7`（春节）；
  000620 `2015-03-11 = 41.9967` 是成立至首个披露日的累计（该基金数组即从这天开始）。
  所以货币序列里偶见远高于日常（0.2~2 元）的值，**独立源 lsjz 返回同样的值**。
* 7 只货币份额源侧长期返回 0/0（万份收益与七日年化同时为 0）——「不披露」不是「零收益」，
  SQL_UPDATE_SNAPSHOT 已排除，`/api/funds/:code/nav` 亦在展示层过滤。
* 源数组点数 == 库内行数（000620 4000=4000、000425 4092=4092，逐只相等）→ 解析无丢行；
  与独立源 lsjz 抽样 6 只 × 60 日 = 360 个日期**全等**。

Usage
-----
    python fetch_fund_nav.py                        # 日常增量：L1 快照 + 追加当日净值
    python fetch_fund_nav.py --snapshot-only        # 只更新 funds 快照，不写 fund_nav
    python fetch_fund_nav.py --gap                  # 补 L1 覆盖不到的基金最近 20 个交易日
    python fetch_fund_nav.py --history              # 全历史回填（逐只，约 2.8 万请求）
    python fetch_fund_nav.py --history --limit 200  # 小批量试点
    python fetch_fund_nav.py --history --codes-file gaps.txt
    python fetch_fund_nav.py --history --request-interval 0.2 --workers 6
    python fetch_fund_nav.py --dry-run              # 只打印，不写库

Compliance note
---------------
天天基金数据有版权声明，自建库内部研究无碍，对外公开发布有风险（同 CCFI / 水泥网）。
"""

import argparse
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dtime, timedelta, timezone

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".fund_nav_progress.json")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RANK_URL   = "https://fund.eastmoney.com/data/rankhandler.aspx"
RANK_REF   = {"Referer": "https://fund.eastmoney.com/data/fundranking.html"}
PZ_URL     = "https://fund.eastmoney.com/pingzhongdata/{code}.js"
LSJZ_URL   = "https://api.fund.eastmoney.com/f10/lsjz"
UA         = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
TIMEOUT    = 40
SLEEP      = 0.25      # 全局最小请求间隔（秒），见 RateLimiter
RETRIES    = 4         # 单个请求最大尝试次数（抖 514 用）
RETRY_BACKOFF = 1.0    # 重试退避基数（秒）
RANK_PAGE  = 10000     # rankhandler 单次上限实测 10000
CN_TZ      = timezone(timedelta(hours=8))   # 东财时间戳口径，见文件头

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS fund_nav (
    id           BIGSERIAL PRIMARY KEY,
    fund_code    VARCHAR(12)  NOT NULL,
    nav_date     DATE         NOT NULL,
    unit_nav     NUMERIC(14,4),
    acc_nav      NUMERIC(14,4),
    daily_return NUMERIC(10,4),
    nav_kind     VARCHAR(10)  NOT NULL DEFAULT 'unit'
);
ALTER TABLE funds ADD COLUMN IF NOT EXISTS latest_nav          NUMERIC(14,4);
ALTER TABLE funds ADD COLUMN IF NOT EXISTS latest_acc_nav      NUMERIC(14,4);
ALTER TABLE funds ADD COLUMN IF NOT EXISTS latest_nav_date     DATE;
ALTER TABLE funds ADD COLUMN IF NOT EXISTS latest_daily_return NUMERIC(10,4);
ALTER TABLE funds ADD COLUMN IF NOT EXISTS nav_kind            VARCHAR(10);
ALTER TABLE funds ADD COLUMN IF NOT EXISTS nav_updated_at      TIMESTAMPTZ;
CREATE UNIQUE INDEX IF NOT EXISTS fund_nav_uniq          ON fund_nav (fund_code, nav_date);
CREATE        INDEX IF NOT EXISTS idx_fund_nav_fund_date ON fund_nav (fund_code, nav_date);
CREATE        INDEX IF NOT EXISTS idx_fund_nav_date      ON fund_nav (nav_date);
"""

# 快照更新：只在「源日期不旧于库内日期」时覆盖，避免源端偶发的陈旧数据把最新值打回去。
# 一律 COALESCE —— L1 对货币基金不给 daily_return，备源不给公司名，直接赋值会把非空抹成 NULL。
SQL_UPDATE_SNAPSHOT = """
UPDATE funds AS f SET
    latest_nav          = COALESCE(v.unit_nav,     f.latest_nav),
    latest_acc_nav      = COALESCE(v.acc_nav,      f.latest_acc_nav),
    latest_daily_return = COALESCE(v.daily_return, f.latest_daily_return),
    latest_nav_date     = v.nav_date,
    nav_kind            = v.nav_kind,
    nav_updated_at      = NOW()
FROM (VALUES %s) AS v(fund_code, nav_date, unit_nav, acc_nav, daily_return, nav_kind)
WHERE f.fund_code = v.fund_code
  AND (f.latest_nav_date IS NULL OR f.latest_nav_date <= v.nav_date)
  -- 货币基金「万份收益与七日年化同时为 0」是源侧不披露，不是零收益：
  -- 银华活钱宝 B~E / 长盛添利宝 B / 江信增利 B / 广发天天利 B 共 7 只长期返回 0/0，
  -- 独立源 api.fund.eastmoney.com/f10/lsjz 亦返回 0（2026-09-16 实测）。
  -- 写进快照列会让列表页显示「0.0000」的假净值，故直接跳过（保留库内原值）。
  AND NOT (v.nav_kind = 'money' AND v.unit_nav = 0 AND COALESCE(v.acc_nav, 0) = 0)
"""

SQL_INSERT_NAV = """
INSERT INTO fund_nav (fund_code, nav_date, unit_nav, acc_nav, daily_return, nav_kind)
VALUES %s
ON CONFLICT (fund_code, nav_date) DO UPDATE SET
    unit_nav     = COALESCE(EXCLUDED.unit_nav,     fund_nav.unit_nav),
    acc_nav      = COALESCE(EXCLUDED.acc_nav,      fund_nav.acc_nav),
    daily_return = COALESCE(EXCLUDED.daily_return, fund_nav.daily_return)
"""

NAV_TEMPLATE = "(%s, %s::date, %s::numeric, %s::numeric, %s::numeric, %s)"


# ---------------------------------------------------------------------------
# 全局限速器 —— 跨线程统一控制请求间隔（同 fetch_funds.py）
# ---------------------------------------------------------------------------
class RateLimiter:
    """确保**任意两次请求之间**至少间隔 min_interval 秒（全进程共享）。"""

    def __init__(self, min_interval: float):
        self.min_interval = max(0.0, float(min_interval))
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            sleep_for = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self.min_interval
        if sleep_for > 0:
            time.sleep(sleep_for)


REQUEST_LIMITER = RateLimiter(SLEEP)

_session_local = threading.local()


def session() -> requests.Session:
    """每线程一个 Session（requests.Session 不保证线程安全）。"""
    s = getattr(_session_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA})
        _session_local.s = s
    return s


# ---------------------------------------------------------------------------
# 异常：区分「确定没有数据」与「抓取失败」
#   只有 NoData 才允许写进度文件 —— 失败写了就永久跳过，缺口不可恢复
# ---------------------------------------------------------------------------
class NavNoData(Exception):
    """数据源明确表示该基金没有净值（404 页 / 空列表 / 无净值序列）。"""


class NavFetchError(Exception):
    """网络/限流等可重试错误，**不可**写进度。"""


# ---------------------------------------------------------------------------
# Progress —— 断点续跑（.fund_nav_progress.json）
# ---------------------------------------------------------------------------
class Progress:
    def __init__(self, path: str = PROGRESS_FILE):
        self.path = path
        self._lock = threading.Lock()
        self._scope_key = None
        self._items: list = []
        self._seen_keys: set = set()
        self._dirty = 0
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._scope_key = data.get("scope_key")
            self._items = data.get("items", [])
            for it in self._items:
                if it.get("k"):
                    self._seen_keys.add(it["k"])
        except Exception as exc:
            print(f"  [WARN] 进度文件读取失败: {exc}")

    def bind_scope(self, scope_key: str):
        with self._lock:
            if self._scope_key != scope_key:
                self._scope_key = scope_key
                self._items = []
                self._seen_keys = set()
                self._dirty = 999

    def has(self, key: str) -> bool:
        return key in self._seen_keys

    def mark(self, key: str):
        with self._lock:
            if key in self._seen_keys:
                return
            self._seen_keys.add(key)
            self._items.append({"k": key, "t": datetime.now(CN_TZ).isoformat()})
            self._dirty += 1

    def save(self, force: bool = False):
        with self._lock:
            if self._dirty < 30 and not force:
                return
            data = {"version": 1, "scope_key": self._scope_key,
                    "saved_at": datetime.now(CN_TZ).isoformat(),
                    "count": len(self._items), "items": self._items}
            tmp = self.path + ".tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                os.replace(tmp, self.path)
                self._dirty = 0
            except Exception as exc:
                print(f"    [WARN] 进度落盘失败: {exc}")

    def reset(self):
        with self._lock:
            self._items = []
            self._seen_keys = set()
            self._dirty = 999


class NullProgress(Progress):
    """刻意不落盘（用于非历史模式，避免污染断点文件）。"""

    def _load(self):
        pass

    def save(self, force: bool = False):
        pass


# ---------------------------------------------------------------------------
# 解析工具
# ---------------------------------------------------------------------------
def to_float(val):
    if val is None:
        return None
    s = str(val).strip().replace("%", "").replace(",", "")
    if not s or s in ("--", "-", "null", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def ms_to_date(ms):
    """东财时间戳 = UTC 零点，按 UTC+8 换算成交易日（见文件头说明）。"""
    return datetime.fromtimestamp(int(ms) / 1000, CN_TZ).date()


def is_money_type(fund_type: str) -> bool:
    return bool(fund_type) and "货币" in fund_type


# ---------------------------------------------------------------------------
# L1：批量最新净值
# ---------------------------------------------------------------------------
def fetch_rank_rows(dt: str, ft: str = "all", verbose: bool = True):
    """拉排行榜全部行。返回 (rows:list[str], all_records:int)。"""
    rows, all_rec = [], None
    page = 1
    while True:
        params = {"op": "ph", "dt": dt, "ft": ft, "rs": "", "gs": 0,
                  "sc": "1nzf", "st": "desc",
                  "sd": (date.today() - timedelta(days=365)).isoformat(),
                  "ed": date.today().isoformat(), "qdii": "", "tabSubtype": ",,,,,",
                  "pi": page, "pn": RANK_PAGE, "dx": 1, "v": time.time()}
        text = _get_text(RANK_URL, params=params, headers=RANK_REF, what=f"rank {dt}")
        m = re.search(r"datas:(\[.*?\])\s*,\s*allRecords", text, re.S)
        if not m:
            raise NavFetchError(f"排行榜响应异常（未匹配 datas）len={len(text)}：{text[:120]}")
        if all_rec is None:
            a = re.search(r"allRecords:(\d+)", text)
            all_rec = int(a.group(1)) if a else None
        batch = json.loads(m.group(1))
        rows.extend(batch)
        if not batch or (all_rec is not None and len(rows) >= all_rec):
            break
        page += 1
    if verbose:
        print(f"    rank dt={dt}: 拿到 {len(rows)} 行（allRecords={all_rec}，{page} 页）")
    return rows, (all_rec or len(rows))


def parse_rank_row(row: str):
    """解析一行 → (code, name, nav_date, unit, acc, chg)。

    货币基金（dt=hb）的 [6] 不是日增长率，由调用方按类型丢弃。
    """
    f = row.split(",")
    if len(f) < 7:
        return None
    code = f[0].strip()
    if not re.fullmatch(r"\d{6}", code):
        return None
    d = f[3].strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
        return None
    return code, f[1].strip(), d, to_float(f[4]), to_float(f[5]), to_float(f[6])


def run_snapshot(conn, kind_of: dict, write_nav: bool = True,
                 dry_run: bool = False, verbose: bool = True):
    """L1：批量拿全市场最新净值 → funds 快照 (+ 可选追加 fund_nav)。"""
    print("── L1 批量最新净值 ────────────────────────────────────────────────────")
    snap = {}          # code -> (date, unit, acc, chg, kind)
    for dt, kind in (("kf", "unit"), ("hb", "money")):
        try:
            rows, _ = fetch_rank_rows(dt, verbose=verbose)
        except NavFetchError as exc:
            print(f"    [FAIL] dt={dt} 排行榜拉取失败：{exc}")
            continue
        kept = 0
        for r in rows:
            p = parse_rank_row(r)
            if not p:
                continue
            code, _name, d, unit, acc, chg = p
            if unit is None or unit <= 0:
                continue
            # 货币基金 [6] 是区间收益不是日增长 → 丢弃，避免静默写错
            daily = None if kind == "money" else chg
            prev = snap.get(code)
            if prev and prev[0] >= d:
                continue
            snap[code] = (d, unit, acc, daily, kind, _name)
            kept += 1
        print(f"    dt={dt} ({kind}): 有效 {kept} 只")

    # 以库内 fund_type 校正口径（排行榜的 dt 可能把个别基金分错桶）
    for code, v in list(snap.items()):
        ft = kind_of.get(code)
        if ft and is_money_type(ft) and v[4] != "money":
            snap[code] = (v[0], v[1], v[2], None, "money", v[5])
        elif ft and not is_money_type(ft) and v[4] == "money":
            snap[code] = (v[0], v[1], v[2], None, "unit", v[5])

    unknown = sum(1 for c in snap if c not in kind_of)
    latest = max((v[0] for v in snap.values()), default=None)
    print(f"  合计 {len(snap)} 只（其中 {unknown} 只不在 funds 表中，将跳过），最新净值日 {latest}")

    if dry_run:
        for c in list(snap)[:5]:
            print(f"    [DRY] {c} {snap[c][5]} {snap[c][0]} unit={snap[c][1]} "
                  f"acc={snap[c][2]} chg={snap[c][3]} kind={snap[c][4]}")
        return {"snap": 0, "nav": 0, "latest": latest}

    rows_up, rows_ins = [], []
    for code, (d, unit, acc, daily, kind, _n) in snap.items():
        if code not in kind_of:
            continue
        rows_up.append((code, d, unit, acc, daily, kind))
        if write_nav:
            rows_ins.append((code, d, unit, acc, daily, kind))

    if not rows_up:
        print("  [WARN] 没有可写入的行（排行榜与 funds 表无交集？）")
        return {"snap": 0, "nav": 0, "latest": latest}

    cur = conn.cursor()
    execute_values(cur, SQL_UPDATE_SNAPSHOT, rows_up, template=NAV_TEMPLATE, page_size=1000)
    if rows_ins:
        execute_values(cur, SQL_INSERT_NAV, rows_ins, template=NAV_TEMPLATE, page_size=1000)
    conn.commit()
    # 注意：execute_values 分页执行，cur.rowcount 只反映最后一页，
    # 所以这里报「源行数」；UPDATE 实际命中数受 SQL 里的日期守卫影响会略少。
    print(f"  快照提交 {len(rows_up)} 行；fund_nav 提交 {len(rows_ins)} 行"
          f"（增量，净值日至 {latest}）")
    return {"snap": len(rows_up), "nav": len(rows_ins), "latest": latest}


# ---------------------------------------------------------------------------
# HTTP 通用：带 514/异常重试（失败抛 NavFetchError，绝不吞掉）
# ---------------------------------------------------------------------------
def _get_text(url, params=None, headers=None, what="") -> str:
    last = None
    for attempt in range(1, RETRIES + 1):
        REQUEST_LIMITER.wait()
        try:
            r = session().get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 200:
                return r.text
            last = f"HTTP {r.status_code}"
            if r.status_code in (514, 429, 502, 503, 504):
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            raise NavFetchError(f"{what} 非预期状态 {last}")
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
            time.sleep(RETRY_BACKOFF * attempt)
    raise NavFetchError(f"{what} 重试 {RETRIES} 次仍失败（{last}）")


# ---------------------------------------------------------------------------
# L2：单只全历史（pingzhongdata）
# ---------------------------------------------------------------------------
def fetch_pingzhong(code: str, fetch_type: str = None):
    """返回 (rows, kind)。rows 元素：(code, date, unit, acc, daily, kind)。

    抛 NavNoData = 该基金确实没有净值页（如「(后端)」份额）；
    抛 NavFetchError = 网络/限流，调用方**不得**记进度。
    """
    text = _get_text(PZ_URL.format(code=code), what=f"pingzhong {code}")

    m = re.search(r"Data_netWorthTrend\s*=\s*(\[.*?\]);", text)
    if m:
        arr = json.loads(m.group(1))
        if not arr:
            raise NavNoData("netWorthTrend 为空")
        ac_map = {}
        ma = re.search(r"Data_ACWorthTrend\s*=\s*(\[.*?\]);", text)
        if ma:
            for pair in json.loads(ma.group(1)):
                if isinstance(pair, list) and len(pair) >= 2:
                    ac_map[int(pair[0])] = to_float(pair[1])
        rows, dirty = [], 0
        for it in arr:
            y = to_float(it.get("y"))
            if y is None or y <= 0:      # 净值不可能 <= 0，脏数据占位（同新浪 close=0 的坑）
                dirty += 1
                continue
            ts = int(it["x"])
            rows.append((code, ms_to_date(ts), y, ac_map.get(ts),
                         to_float(it.get("equityReturn")), "unit"))
        if not rows:
            raise NavNoData(f"净值序列全部为脏数据（{dirty} 行）")
        return rows, "unit", dirty

    m = re.search(r"Data_millionCopiesIncome\s*=\s*(\[.*?\]);", text)
    if m:
        mi = json.loads(m.group(1))
        if not mi:
            raise NavNoData("millionCopiesIncome 为空")
        s_map = {}
        ms = re.search(r"Data_sevenDaysYearIncome\s*=\s*(\[.*?\]);", text)
        if ms:
            for pair in json.loads(ms.group(1)):
                if isinstance(pair, list) and len(pair) >= 2:
                    s_map[int(pair[0])] = to_float(pair[1])
        rows, dirty = [], 0
        for pair in mi:
            if not isinstance(pair, list) or len(pair) < 2:
                continue
            v = to_float(pair[1])
            if v is None or v < 0:
                dirty += 1
                continue
            ts = int(pair[0])
            rows.append((code, ms_to_date(ts), v, s_map.get(ts), None, "money"))
        if not rows:
            raise NavNoData(f"万份收益序列全部为脏数据（{dirty} 行）")
        return rows, "money", dirty

    if fetch_type and is_money_type(fetch_type):
        raise NavNoData("货币基金但页面无万份收益序列")
    raise NavNoData(f"无净值序列（404 页，len={len(text)}）")


# ---------------------------------------------------------------------------
# L4 备源：雪球全历史（补 pingzhongdata 没有净值页的场内品种）
# ---------------------------------------------------------------------------
XQ_HISTORY_URL = ("https://danjuanfunds.com/djapi/fund/nav/history/{code}"
                  "?page=1&size=10000&all=true")
XQ_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://danjuanfunds.com/"}


def fetch_xq_history(code: str):
    """雪球全历史。返回 (rows, dropped)。rows 元素：(code, date, unit, acc, None, 'unit')。

    为什么要这个源：**REITs 与 QDII 美元份额在 pingzhongdata 里是空页面**
    （`Data_netWorthTrend = []`，文件才 3KB），L2 会把它们判成 NavNoData 而永远补不上。
    实测（2026-09-16）库内 540 只「无任何净值记录」里有 21 只属于这一类，
    雪球能给出真实序列（508066 华泰江苏交控REIT 564 点、003244 QDII美元现钞 146 点）。

    ⚠ 两个必须处理的坑：
    1. 雪球对**低披露频率品种会按前值填充成日频**：508066 的 564 个点里 99% 都是同一个
       7.6350，508097 更极端 —— 454 个点全部取同一个值。REIT 净值实际是**半年度**披露，
       不折叠就会写进几百行假日频数据，还会把 `latest_nav_date` 顶到最近交易日。
       所以**折叠连续同值，只保留每段的第一天**（对前值填充序列，该日即披露日）。
    2. 折叠后「相邻两点」不再是原序列的相邻日 → **daily_return 一律留空**
       （同货币基金「宁缺勿错」的处理），需要时可由序列自算。
    3. 折叠比例 >50% 一律判 NavNoData（见函数末尾）—— 那种序列是纯前值填充，
       无法区分「净值真没变」与「源侧没更新」，写进快照会得到一个停在上市日的陈旧值。
    """
    text = _get_text(XQ_HISTORY_URL.format(code=code), headers=XQ_HEADERS,
                     what=f"雪球 {code}")
    try:
        items = (json.loads(text).get("data") or {}).get("items") or []
    except (ValueError, AttributeError) as exc:
        raise NavFetchError(f"雪球 {code} 响应无法解析: {exc}")
    if not items:
        raise NavNoData("雪球无净值点")

    items = sorted(items, key=lambda it: it.get("date") or "")   # 雪球按日期降序返回
    rows, dropped, prev = [], 0, None
    for it in items:
        d = (it.get("date") or "")[:10]
        v = to_float(it.get("nav"))
        if not d or v is None or v <= 0:          # 净值不可能 <= 0
            dropped += 1
            continue
        if prev is not None and v == prev:        # 前值填充段 → 折叠
            dropped += 1
            continue
        rows.append((code, d, v, to_float(it.get("value")), None, "unit"))
        prev = v
    if not rows:
        raise NavNoData(f"雪球序列全为填充/脏数据（{dropped} 行）")

    # 折叠比例过高 = 该源的这条序列本身就是前值填充，不是真实披露序列。
    # 508097 雪球给了 454 个点、折叠后只剩 1 个（全部取同一个 2.5560）——
    # 这无法区分「净值真的没变」与「源侧根本没更新」，写进快照会得到一个
    # 日期停在上市日的陈旧净值（列表页会当成最新值展示）。宁可判无数据。
    ratio = dropped / (len(rows) + dropped)
    if ratio > 0.5:
        raise NavNoData(f"雪球序列 {ratio * 100:.0f}% 是前值填充（折叠后仅剩 "
                        f"{len(rows)} 点），非真实披露序列")
    return rows, dropped


def run_history(conn, codes: list, fund_types: dict, progress: Progress,
                workers: int = 6, dry_run: bool = False, verbose: bool = True):
    """L2：逐只回填全历史。"""
    print("── L2 全历史回填（pingzhongdata，1 只 1 请求）────────────────────────")
    todo = [c for c in codes if not progress.has(f"h:{c}")]
    print(f"  待处理 {len(todo)} 只（进度已记 {len(codes) - len(todo)} 只）")
    if not todo:
        print("  没有待处理的基金（进度文件显示全部已完成；--reset-progress 可重来）")
        return {"ok": 0, "nodata": 0, "fail": 0, "rows": 0}

    stat = {"ok": 0, "nodata": 0, "fail": 0, "rows": 0, "dirty": 0}
    lock = threading.Lock()
    t0 = time.time()
    done = 0

    def work(code):
        try:
            rows, kind, dirty = fetch_pingzhong(code, fund_types.get(code))
            return code, rows, kind, dirty, None
        except NavNoData as exc:
            return code, None, None, 0, ("nodata", str(exc))
        except NavFetchError as exc:
            return code, None, None, 0, ("fail", str(exc))
        except Exception as exc:
            return code, None, None, 0, ("fail", f"{type(exc).__name__}: {exc}")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(work, c) for c in todo]
        for fut in as_completed(futs):
            code, rows, kind, dirty, err = fut.result()
            done += 1
            if err is None:
                if not dry_run:
                    try:
                        cur = conn.cursor()
                        execute_values(cur, SQL_INSERT_NAV, rows,
                                       template=NAV_TEMPLATE, page_size=1000)
                        # 快照取本序列最新一行；复用带日期守卫 + COALESCE 的同一条 SQL
                        execute_values(cur, SQL_UPDATE_SNAPSHOT, [rows[-1]],
                                       template=NAV_TEMPLATE, page_size=1000)
                        conn.commit()
                    except psycopg2.Error as exc:
                        conn.rollback()
                        with lock:
                            stat["fail"] += 1
                        print(f"    [DB-ERR] {code}: {str(exc).strip()[:120]}  ← 未记进度，可重跑")
                        continue
                # ⚠ 只有写库成功才记进度；dry-run 一律不记，否则会把试跑当成已完成
                if not dry_run:
                    progress.mark(f"h:{code}")
                with lock:
                    stat["ok"] += 1
                    stat["rows"] += len(rows)
                    stat["dirty"] += dirty
            elif err[0] == "nodata":
                if not dry_run:
                    progress.mark(f"h:{code}")  # 确定性无数据 → 允许记进度
                with lock:
                    stat["nodata"] += 1
            else:
                with lock:
                    stat["fail"] += 1
                if verbose and stat["fail"] <= 10:
                    print(f"    [FAIL] {code}: {err[1][:100]}  ← 未记进度，可重跑")

            if done % 200 == 0 or done == len(todo):
                progress.save()
                el = time.time() - t0
                rate = done / el if el else 0
                eta = (len(todo) - done) / rate / 60 if rate else 0
                print(f"    [{done}/{len(todo)}] ok={stat['ok']} 无数据={stat['nodata']} "
                      f"失败={stat['fail']} 行数={stat['rows']:,} "
                      f"| {rate:.2f} 只/s 剩余约 {eta:.0f} 分钟")

    progress.save(force=True)
    print(f"\n  完成：成功 {stat['ok']} 只 / {stat['rows']:,} 行，"
          f"无净值页 {stat['nodata']} 只，失败 {stat['fail']} 只，"
          f"跳过脏数据 {stat['dirty']} 行")
    if stat["fail"]:
        print(f"  ⚠ 有 {stat['fail']} 只失败（已排除进度，重跑本命令即可续补）")
    return stat


# ---------------------------------------------------------------------------
# L3：补 L1 覆盖不到的基金（lsjz，最近 20 个交易日）
# ---------------------------------------------------------------------------
def fetch_lsjz(code: str, kind: str, pages: int = 1):
    """东财 F10 历史净值 JSON。pageSize 被服务端夹在 20，故只补最近 N*20 天。"""
    out = []
    for pi in range(1, pages + 1):
        text = _get_text(LSJZ_URL,
                         params={"fundCode": code, "pageIndex": pi, "pageSize": 20},
                         headers={"Referer": f"https://fundf10.eastmoney.com/jjjz_{code}.html"},
                         what=f"lsjz {code}")
        try:
            j = json.loads(text)
        except ValueError:
            raise NavFetchError(f"lsjz {code} 返回非 JSON：{text[:100]}")
        lst = (j.get("Data") or {}).get("LSJZList") or []
        if not lst:
            break
        for it in lst:
            d = (it.get("FSRQ") or "").strip()
            unit = to_float(it.get("DWJZ"))
            acc = to_float(it.get("LJJZ"))
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d) or unit is None or unit <= 0:
                continue
            daily = None if kind == "money" else to_float(it.get("JZZZL"))
            out.append((code, d, unit, acc, daily, kind))
    if not out:
        raise NavNoData("lsjz 空列表")
    out.sort(key=lambda r: r[1])
    return out


def run_gap(conn, target_date, fund_types: dict, workers: int = 6,
            limit: int = None, dry_run: bool = False, verbose: bool = True):
    """L3：把 latest_nav_date 落后于 target_date 的基金补齐。"""
    print("── L3 补缺口（lsjz，最近 20 个交易日）────────────────────────────────")
    cur = conn.cursor()
    cur.execute("""
        SELECT fund_code FROM funds
        WHERE fund_name NOT LIKE '%%(后端)%%'
          AND (latest_nav_date IS NULL OR latest_nav_date < %s::date)
        ORDER BY fund_code
    """, (target_date,))
    codes = [r[0] for r in cur.fetchall()]
    if limit:
        codes = codes[:limit]
    print(f"  落后于 {target_date} 的基金 {len(codes)} 只"
          + ("（--limit 截断）" if limit else ""))
    if not codes:
        return {"ok": 0, "nodata": 0, "fail": 0, "rows": 0}

    stat = {"ok": 0, "nodata": 0, "fail": 0, "rows": 0}
    lock = threading.Lock()
    t0 = time.time()
    done = 0

    def work(code):
        kind = "money" if is_money_type(fund_types.get(code, "")) else "unit"
        try:
            return code, fetch_lsjz(code, kind), kind, None
        except NavNoData as exc:
            return code, None, kind, ("nodata", str(exc))
        except NavFetchError as exc:
            return code, None, kind, ("fail", str(exc))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(work, c) for c in codes]
        for fut in as_completed(futs):
            code, rows, kind, err = fut.result()
            done += 1
            if err is None:
                if not dry_run:
                    try:
                        cur = conn.cursor()
                        execute_values(cur, SQL_INSERT_NAV, rows,
                                       template=NAV_TEMPLATE, page_size=1000)
                        execute_values(cur, SQL_UPDATE_SNAPSHOT, [rows[-1]],
                                       template=NAV_TEMPLATE, page_size=1000)
                        conn.commit()
                    except psycopg2.Error as exc:
                        conn.rollback()
                        with lock:
                            stat["fail"] += 1
                        print(f"    [DB-ERR] {code}: {str(exc).strip()[:120]}")
                        continue
                with lock:
                    stat["ok"] += 1
                    stat["rows"] += len(rows)
            elif err[0] == "nodata":
                with lock:
                    stat["nodata"] += 1
            else:
                with lock:
                    stat["fail"] += 1
                if verbose and stat["fail"] <= 10:
                    print(f"    [FAIL] {code}: {err[1][:100]}")

            if done % 200 == 0 or done == len(codes):
                el = time.time() - t0
                rate = done / el if el else 0
                eta = (len(codes) - done) / rate / 60 if rate else 0
                print(f"    [{done}/{len(codes)}] ok={stat['ok']} 无数据={stat['nodata']} "
                      f"失败={stat['fail']} 行数={stat['rows']:,} "
                      f"| {rate:.2f} 只/s 剩余约 {eta:.0f} 分钟")

    print(f"\n  完成：成功 {stat['ok']} 只 / {stat['rows']:,} 行，"
          f"无数据 {stat['nodata']} 只，失败 {stat['fail']} 只")
    return stat


# ---------------------------------------------------------------------------
# L4：雪球备源补漏（针对 fund_nav 里一行都没有的基金）
# ---------------------------------------------------------------------------
def run_fill_xq(conn, limit=None, codes_file=None, dry_run=False):
    """把「fund_nav 里没有任何行」的基金用雪球源补上。

    刻意**不碰进度文件**：判据是 `NOT EXISTS (SELECT 1 FROM fund_nav ...)`，本身幂等，
    而进度文件的 scope 与历史模式不同，混用会把历史进度清掉（同 `--fill-scale` 的教训）。
    """
    print("── L4 雪球备源补漏（只处理 fund_nav 无任何记录的基金）──────────────")
    cur = conn.cursor()
    cur.execute("""
        SELECT f.fund_code, f.fund_name, f.fund_type
        FROM funds f
        WHERE NOT EXISTS (SELECT 1 FROM fund_nav n WHERE n.fund_code = f.fund_code)
        ORDER BY f.fund_code
    """)
    rows = cur.fetchall()
    if codes_file:
        with open(codes_file, "r", encoding="utf-8") as f:
            want = {ln.strip() for ln in f if ln.strip()}
        rows = [r for r in rows if r[0] in want]
        print(f"  --codes-file: 命中 {len(rows)}/{len(want)} 只")
    if limit:
        rows = rows[:limit]
    print(f"  候选 {len(rows)} 只（fund_nav 里没有任何记录）")
    if not rows:
        return {"ok": 0, "nodata": 0, "fail": 0, "rows": 0}

    stat = {"ok": 0, "nodata": 0, "fail": 0, "rows": 0, "folded": 0}
    for i, (code, name, ftype) in enumerate(rows, 1):
        try:
            nav_rows, folded = fetch_xq_history(code)
        except NavNoData as exc:
            stat["nodata"] += 1
            print(f"    [{i}/{len(rows)}] {code} 无数据：{exc}")
            continue
        except NavFetchError as exc:
            stat["fail"] += 1
            print(f"    [{i}/{len(rows)}] {code} [FAIL] {exc}")
            continue
        if not dry_run:
            try:
                execute_values(cur, SQL_INSERT_NAV, nav_rows,
                               template=NAV_TEMPLATE, page_size=1000)
                execute_values(cur, SQL_UPDATE_SNAPSHOT, [nav_rows[-1]],
                               template=NAV_TEMPLATE, page_size=1000)
                conn.commit()
            except psycopg2.Error as exc:
                conn.rollback()
                stat["fail"] += 1
                print(f"    [{i}/{len(rows)}] {code} [DB-ERR] {str(exc).strip()[:110]}")
                continue
        stat["ok"] += 1
        stat["rows"] += len(nav_rows)
        stat["folded"] += folded
        print(f"    [{i}/{len(rows)}] {code} {(name or '')[:20]:22s} "
              f"写入 {len(nav_rows):>4} 点（折叠掉前值填充 {folded} 行）"
              f" {nav_rows[0][1]} ~ {nav_rows[-1][1]}")

    print(f"\n  完成：成功 {stat['ok']} 只 / {stat['rows']:,} 行（折叠填充 "
          f"{stat['folded']:,} 行），无数据 {stat['nodata']} 只，失败 {stat['fail']} 只")
    return stat


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute(SQL_ENSURE)
    conn.commit()


def load_fund_types(conn) -> dict:
    cur = conn.cursor()
    cur.execute("SELECT fund_code, fund_type FROM funds")
    return {c: (t or "") for c, t in cur.fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser(description="公募基金净值抓取（快照 + 全历史）")
    ap.add_argument("--snapshot-only", action="store_true",
                    help="只更新 funds 快照，不写 fund_nav（默认两者都写）")
    ap.add_argument("--history", action="store_true",
                    help="全历史回填（逐只 pingzhongdata，1 只 1 请求）")
    ap.add_argument("--gap", action="store_true",
                    help="补 L1 覆盖不到的基金（逐只 lsjz，最近 20 个交易日）")
    ap.add_argument("--fill-xq", action="store_true",
                    help="雪球备源补漏：只处理 fund_nav 里没有任何记录的基金"
                         "（REITs / QDII 美元份额在 pingzhongdata 里是空页面）")
    ap.add_argument("--limit", type=int, default=None, help="限制基金数量（试点用）")
    ap.add_argument("--codes-file", default=None, help="只处理文件里的基金代码（每行一个）")
    ap.add_argument("--request-interval", type=float, default=SLEEP,
                    help=f"全局最小请求间隔秒数（默认 {SLEEP}）。调小会触发东财 HTTP 514")
    ap.add_argument("--workers", type=int, default=6, help="并发线程数（默认 6）")
    ap.add_argument("--pages", type=int, default=1, help="--gap 时每只取几页（每页 20 条）")
    ap.add_argument("--full", action="store_true", help="忽略进度文件，全部重来")
    ap.add_argument("--reset-progress", action="store_true", help="清空进度文件后开始")
    ap.add_argument("--dry-run", action="store_true", help="只打印，不写库")
    args = ap.parse_args()

    REQUEST_LIMITER.min_interval = max(0.0, args.request_interval)
    if args.request_interval < 0.15:
        print(f"  [WARN] --request-interval={args.request_interval}s 偏激进，"
              f"东财 HTTP 514 会明显增多（失败不记进度，可重跑续补）")

    print("── 公募基金净值：快照 + 全历史 ────────────────────────────────────────")
    print("连接数据库...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接: {exc}")
        return 1

    if not args.dry_run:
        try:
            ensure_schema(conn)
        except psycopg2.Error as exc:
            print(f"  [FATAL] 建表失败: {exc}")
            return 1
        print("  schema 就绪（fund_nav 表 + funds 净值快照列）")

    fund_types = load_fund_types(conn)
    print(f"  funds 表 {len(fund_types)} 只")

    progress = NullProgress()
    if args.history or args.gap:
        progress = Progress()
        if args.reset_progress:
            progress.reset()
        # scope 固定为 'history'：key 形如 h:<code> 自带语义，
        # 且不该因为 --limit 变化而把之前的进度清掉
        progress.bind_scope("history")

    result: dict = {}
    if args.history:
        codes = sorted(fund_types)
        if args.codes_file:
            with open(args.codes_file, "r", encoding="utf-8") as f:
                want = {ln.strip() for ln in f if ln.strip()}
            codes = [c for c in codes if c in want]
            print(f"  --codes-file: 命中 {len(codes)}/{len(want)} 只")
        if args.limit:
            codes = codes[:args.limit]
        result = run_history(conn, codes, fund_types, progress,
                             workers=args.workers, dry_run=args.dry_run)
    elif args.gap:
        cur = conn.cursor()
        cur.execute("SELECT MAX(latest_nav_date) FROM funds")
        target = cur.fetchone()[0]
        if target is None:
            print("  [FATAL] funds 里没有任何净值日期，请先跑一次快照（无参数）")
            return 1
        result = run_gap(conn, target, fund_types, workers=args.workers,
                         limit=args.limit, dry_run=args.dry_run)
    elif args.fill_xq:
        result = run_fill_xq(conn, limit=args.limit, codes_file=args.codes_file,
                             dry_run=args.dry_run)
    else:
        result = run_snapshot(conn, fund_types,
                              write_nav=not args.snapshot_only, dry_run=args.dry_run)
        if not args.snapshot_only and not args.dry_run:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*), COUNT(latest_nav), COUNT(*) FILTER (
                    WHERE latest_nav_date = (SELECT MAX(latest_nav_date) FROM funds))
                FROM funds
            """)
            tot, has, cur_day = cur.fetchone()
            print(f"  覆盖率：{has}/{tot} = {has / tot * 100:.1f}% 有净值快照，"
                  f"当日（最新净值日）{cur_day} 只")

    if not args.dry_run:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT COUNT(*) AS rows, COUNT(DISTINCT fund_code) AS funds,
                       MIN(nav_date)::text AS earliest, MAX(nav_date)::text AS latest
                FROM fund_nav
            """)
            r = cur.fetchone()
            print(f"\n  fund_nav 现状：{r[0]:,} 行 / {r[1]:,} 只基金 / {r[2]} ~ {r[3]}")
        except psycopg2.errors.UndefinedTable:
            conn.rollback()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
