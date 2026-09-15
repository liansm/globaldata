#!/usr/bin/env python3
"""
Public Fund Fetcher (公募基金名录 + 规模 + 季度持仓)
====================================================
写入三块数据：

 1. funds        — 全部公募基金名录（~2.8 万条，份额口径，A/C 类分开）
 2. funds.规模   — 单只基金最新规模/公司/经理（逐只请求，带 30 天节流）
 3. fund_holdings — 季度持仓明细（季报披露的**前十大重仓**）

Data sources
------------
* 名录:   天天基金 fund_name_em（akshare，1 次请求拿全量）
* 规模:   雪球 fund_individual_basic_info_xq（akshare，逐只）
* 持仓:   https://fundf10.eastmoney.com/FundArchivesDatas.aspx
          type=jjcc 股票 / type=zqcc 债券，year 参数一次返回该年全部季度

⚠ 东财持仓接口的两个坑（务必都看懂再改代码）
------------------------------------------------
1) 必须带 ASP.NET_SessionId cookie：裸请求 FundArchivesDatas.aspx 返回 404，
   得先用 Session GET 一次 F10 页面（ccmx_<code>.html）拿到 cookie，再带 Referer
   请求才返回 200。akshare 1.18.42 的 fund_portfolio_hold_em 因此报 JSONDecodeError。
2) **有频控，过快会返回 HTTP 514**（非标准状态码、body 为空）。
   实测同一批「确认接口有数据」的基金：
       间隔 0s（串行）   → 83% 成功
       间隔 0.5s（串行） → 100%
       8 线程无节流      → 27%
   失败是**随机**的——同一只基金时好时坏，所以「这只基金没有持仓」是错觉。
   因此本脚本用全局 RateLimiter 把请求速率锁在 1/--request-interval，
   并且**请求失败绝不写进度文件**（否则缺口会被永久跳过，增量永远补不回来）。
   2026-09-10 那次全量跑就是踩了这个坑：数千只基金的股票持仓被静默丢弃
   （含兴全合润、招商中证白酒、富国天惠、中欧新蓝筹等）。2026-09-15 按上述
   方式修复并回补，基金覆盖数 10501 → 14981（+4480），9408 个任务失败 0。

口径限制
--------
* 季报只披露前十大重仓股/券，完整持仓仅半年报/年报——行业规则，免费源都一样
* 持仓占比为「占净值比例」，股票型基金前十大约 40-60%，不代表全部仓位
* 基金规模为雪球「最新规模」（亿元），随季报更新
* 名录含已清盘/终止上市的基金，规模请求会失败，跳过即可

Incremental updates
-------------------
* 名录:   每次全量 upsert（1 次请求，无成本）
* 规模:   每只基金 30 天内更新过则跳过（--refresh-scale 强制刷新）
* 持仓:   对 scope 内每只基金，按年计算「已过披露期的季末」集合 due
          （季末 + 40 天），若 DB 缺任一 due 季度才发起请求
* 全量首跑约 2.8 万只 × 2 请求 ≈ 数小时（受全局限速约束），之后日常增量很快

Compliance note
---------------
天天基金 / 雪球数据均有版权声明，自建库内部研究无碍，
对外公开发布有风险（同 CCFI）。

Usage
-----
    python fetch_funds.py --types "混合型-偏股" --limit 100   # 小范围试点
    python fetch_funds.py                                     # 全部基金，增量
    python fetch_funds.py --years-back 5                      # 回补近 5 年持仓
    python fetch_funds.py --full                              # 忽略增量，全部重写
    python fetch_funds.py --refresh-scale                     # 强制刷新规模
    python fetch_funds.py --bonds                             # 同时抓债券持仓
    python fetch_funds.py --codes-file gaps.txt --years-back 1 --bonds
                                                              # 定向回补缺口基金
    python fetch_funds.py --request-interval 1.0              # 更保守的限速
    python fetch_funds.py --dry-run                           # 只打印，不写库
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
from io import StringIO

import pandas as pd
import requests

import akshare as ak
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
PROGRESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".funds_progress.json")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
F10_BASE      = "https://fundf10.eastmoney.com"
ARCHIVE_URL   = F10_BASE + "/FundArchivesDatas.aspx"
UA            = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                 "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
TIMEOUT       = 30
# 全局请求最小间隔（秒）。东财 F10 有频控，过快会返回 **HTTP 514**（非标准码、
# body 为空）；实测同一批「确认有数据」的基金：间隔 0s → 83% 成功、
# 0.5s → 100%、1.0s → 100%。串行如此，8 线程无节流更只有 27%。
# 因此节流必须作用在**每一次请求**上（含 shake），而不是只在重试分支里。
SLEEP         = 0.6           # = 全局最小请求间隔，见 RateLimiter
RETRIES       = 4             # 单次请求最大尝试次数（抖 514 用）
RETRY_BACKOFF = 1.0           # 重试退避基数（秒）：第 n 次失败后 sleep n*基数
SCALE_TTL_DAYS = 30           # 规模刷新节流
DISCLOSE_LAG   = 40           # 季末后 N 天视为已过披露期

SQL_UPSERT_FUND = """
INSERT INTO funds (fund_code, fund_name, fund_type, updated_at)
VALUES %s
ON CONFLICT (fund_code) DO UPDATE SET
    fund_name  = EXCLUDED.fund_name,
    fund_type  = EXCLUDED.fund_type,
    updated_at = EXCLUDED.updated_at
"""

SQL_UPDATE_SCALE = """
UPDATE funds SET
    fund_company    = %s,
    fund_manager    = %s,
    scale           = %s,
    scale_raw       = %s,
    inception_date  = %s,
    scale_updated_at = NOW(),
    updated_at      = NOW()
WHERE fund_code = %s
"""

SQL_UPSERT_HOLDING = """
INSERT INTO fund_holdings
    (fund_code, report_date, holding_type, security_code, security_name,
     ratio, shares, market_value)
VALUES %s
ON CONFLICT (fund_code, report_date, holding_type, security_code) DO UPDATE SET
    security_name = EXCLUDED.security_name,
    ratio         = EXCLUDED.ratio,
    shares        = EXCLUDED.shares,
    market_value  = EXCLUDED.market_value
"""

SQL_LOG_FETCH = """
INSERT INTO fetch_log (commodity_key, latest_date, latest_price, change_day)
VALUES ('funds', %s, NULL, NULL)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def quarter_ends(year: int) -> list:
    """某年的全部季末日期。"""
    return [date(year, 3, 31), date(year, 6, 30),
            date(year, 9, 30), date(year, 12, 31)]


def due_quarters(year: int, today: date) -> list:
    """某年已过披露期（季末 + DISCLOSE_LAG 天）的季末日期，升序。"""
    cutoff = today - timedelta(days=DISCLOSE_LAG)
    return [q for q in quarter_ends(year) if q <= cutoff]


def to_float(val):
    """'6.45%' / '20.00' / '--' / NaN → float 或 None。"""
    if val is None:
        return None
    s = str(val).strip().replace(",", "").rstrip("%")
    if s in ("", "--", "-", "nan", "None"):
        return None
    try:
        return round(float(s), 4)
    except ValueError:
        return None


def parse_scale(raw: str):
    """'39.38亿' / '2276.10万' → 统一为亿元；(None, raw) 表示解析失败。"""
    if not raw:
        return None, raw
    m = re.search(r"([\d,.]+)\s*([万亿])", str(raw))
    if m:
        try:
            v = float(m.group(1).replace(",", ""))
            if m.group(2) == "万":
                v = v / 10000.0
            return round(v, 4), str(raw).strip()
        except ValueError:
            pass
    return None, str(raw).strip()


def clean_code(val) -> str:
    """read_html 可能把债券代码读成 int/float，统一转定长字符串。"""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    if isinstance(val, float):
        val = int(val)
    return str(val).strip().zfill(6) if str(val).strip().isdigit() else str(val).strip()


# ---------------------------------------------------------------------------
# Progress — 断点续跑进度（.funds_progress.json）
# ---------------------------------------------------------------------------
class Progress:
    """线程安全的进度文件包装。

    存储: {"version":1, "scope":{args...}, "started_at":iso, "items":[{...}]}
    items 每条记录形如 {"c":"000001","k":"s|stock|2025-09-30","t":iso}
        k: "s" = 规模完成, "h:stock:2025-09-30" = 某季度某类持仓完成
    启动时按 (scope, items) 判断是否需要跳过；启动时间相同则视为同会话续跑。
    """

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
            for it in data.get("items", []):
                k = it.get("k")
                if k:
                    self._seen_keys.add(k)
            # 恢复 _items 也加载一遍（便于 --reset 不全丢）
            self._items = data.get("items", [])
        except Exception as exc:
            print(f"  [WARN] 进度文件读取失败: {exc}")

    def bind_scope(self, scope_key: str):
        """绑定当前会话的 scope_key；不同 scope 自动重置。"""
        with self._lock:
            if self._scope_key != scope_key:
                # 跨 scope，丢弃旧进度
                self._scope_key = scope_key
                self._items = []
                self._seen_keys = set()
                self._dirty = 999  # 强制落盘
            else:
                # 同 scope（续跑），保留 items 用于显示
                pass

    def has(self, key: str) -> bool:
        return key in self._seen_keys

    def mark(self, key: str):
        with self._lock:
            if key in self._seen_keys:
                return
            self._seen_keys.add(key)
            self._items.append({
                "k": key,
                "t": pd.Timestamp.now(tz="UTC").isoformat(),
            })
            self._dirty += 1

    def save(self, force: bool = False):
        with self._lock:
            if self._dirty < 30 and not force:
                return
            data = {
                "version": 1,
                "scope_key": self._scope_key,
                "saved_at": pd.Timestamp.now(tz="UTC").isoformat(),
                "count": len(self._items),
                "items": self._items,
            }
            tmp = self.path + ".tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                os.replace(tmp, self.path)
                self._dirty = 0
            except Exception as exc:
                # 进度落盘失败不影响主流程
                print(f"    [WARN] 进度落盘失败: {exc}")

    def reset(self):
        with self._lock:
            self._items = []
            self._seen_keys = set()
            self._dirty = 999


# ---------------------------------------------------------------------------
# 全局限速器 —— 跨线程统一控制请求间隔
# ---------------------------------------------------------------------------
class RateLimiter:
    """确保**任意两次请求之间**至少间隔 min_interval 秒（全进程共享）。

    与「每个线程各自 sleep」不同，这里是全局串行化发牌：无论 --workers 开到几，
    对东财的请求速率都恒定不超过 1/min_interval，从根上避开 HTTP 514 频控。
    """

    def __init__(self, min_interval: float):
        self.min_interval = max(0.0, float(min_interval))
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            sleep_for = self._next_at - now
            if sleep_for < 0:
                sleep_for = 0.0
            self._next_at = max(now, self._next_at) + self.min_interval
        if sleep_for > 0:
            time.sleep(sleep_for)


REQUEST_LIMITER = RateLimiter(SLEEP)


# ---------------------------------------------------------------------------
# Eastmoney F10 session（关键：必须带 ASP.NET_SessionId cookie）
# ---------------------------------------------------------------------------
class F10Session:
    """持有 ASP.NET_SessionId 的会话；aspx 404/514/异常时自动退避重试。"""

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA})
        self._shake()

    def _shake(self, code: str = "000001"):
        """GET 一次 F10 页面以获得/刷新 session cookie。"""
        url = f"{F10_BASE}/ccmx_{code}.html"
        REQUEST_LIMITER.wait()
        try:
            self.s.get(url, timeout=TIMEOUT)
        except requests.RequestException:
            pass

    def get_holdings(self, code: str, year: int, typ: str):
        """
        拉取某基金某年的持仓（typ='jjcc' 股票 / 'zqcc' 债券）。

        Returns
        -------
        list of (report_date_str, holding_type, security_code, security_name,
                 ratio, shares, market_value)

        Raises
        ------
        RuntimeError
            重试 RETRIES 次后仍拿不到 apidata（多半是被东财限流）。
            **调用方必须把它与「该基金确实没有此类持仓」区分开**，
            见 fetch_holding_http 的 ok 标志。
        """
        r = None
        last = "未发起请求"
        for attempt in range(1, RETRIES + 1):
            REQUEST_LIMITER.wait()
            try:
                r = self.s.get(
                    ARCHIVE_URL,
                    params={"type": typ, "code": code, "topline": "10000",
                            "year": str(year), "month": "",
                            "rt": f"{time.time()%1:.15f}"},
                    headers={"Referer": f"{F10_BASE}/ccmx_{code}.html"},
                    timeout=TIMEOUT,
                )
            except requests.RequestException as exc:
                r, last = None, f"网络异常 {type(exc).__name__}"
            else:
                last = f"HTTP {r.status_code}"
                if r.status_code == 200 and "apidata" in r.text:
                    break
            if attempt < RETRIES:
                # cookie 失效 / 被限流 → 重新握手 + 指数退避
                self._shake(code)
                time.sleep(RETRY_BACKOFF * attempt)
        else:
            hint = "（HTTP 514 = 东财限流）" if last == "HTTP 514" else ""
            raise RuntimeError(f"{code} {typ} {year}: {last}{hint}")

        m = re.search(r'content:"(.*)",arryear:', r.text, re.S)
        if not m:
            return []               # 无数据（如该基金无股票持仓）
        content = m.group(1).replace('\\"', '"')
        if "暂无数据" in content or "没有数据" in content or "<table" not in content.lower():
            return []               # 无数据（如债基无股票持仓、货基无持仓表）
        try:
            tables = pd.read_html(StringIO(content))
        except Exception:
            return []               # 空内容/非表格内容 → 视为无数据，不抛错
        if not tables:
            return []
        labels = re.findall(r"<h4 class=['\"]t['\"]>(.*?)</h4>", content, re.S)
        if len(labels) != len(tables):
            # 标签与表格未对齐时跳过该基金该年（防御性）
            raise RuntimeError(f"{code} {typ} {year}: 标签 {len(labels)} != 表格 {len(tables)}")

        rows, typ_short = [], "stock" if typ == "jjcc" else "bond"
        for label, tbl in zip(labels, tables):
            text = re.sub(r"<[^>]+>", " ", label)
            dm = re.search(r"(\d{4})年(\d)季度", text)
            dt = re.search(r"(\d{4}-\d{2}-\d{2})", text)
            if dt:
                report_date = dt.group(1)
            elif dm:
                q_end = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[int(dm.group(2))]
                report_date = f"{dm.group(1)}-{q_end}"
            else:
                continue

            tbl.columns = [str(c).replace(" ", "").replace("\u3000", "") for c in tbl.columns]
            code_col  = next((c for c in tbl.columns if c.endswith("代码")), None)
            name_col  = next((c for c in tbl.columns if c.endswith("名称")), None)
            ratio_col = next((c for c in tbl.columns if "占净值" in c), None)
            shares_col = next((c for c in tbl.columns if "持股数" in c), None)
            mv_col    = next((c for c in tbl.columns if "持仓市值" in c or "市值" in c), None)
            if not (code_col and name_col and ratio_col):
                continue

            for _, r_ in tbl.iterrows():
                sec_code = clean_code(r_.get(code_col))
                if not sec_code:
                    continue
                rows.append((
                    report_date, typ_short, sec_code,
                    str(r_.get(name_col, "")).strip() or None,
                    to_float(r_.get(ratio_col)),
                    to_float(r_.get(shares_col)) if shares_col else None,
                    to_float(r_.get(mv_col)) if mv_col else None,
                ))
        return rows


# ---------------------------------------------------------------------------
# Danjuanfunds scale（雪球的实际接口，比 akshare 包装更快，可并发）
# ---------------------------------------------------------------------------
DANJUAN_URL = "https://danjuanfunds.com/djapi/fund/{code}"

def fetch_scale_http(code: str):
    """danjuanfunds 基金详情 → (company, manager, scale_yi, scale_raw, inception).

    实际接口: https://danjuanfunds.com/djapi/fund/<6 位代码>
    字段: keeper_name / manager_name / totshare ('39.38亿') / found_date
    """
    r = requests.get(DANJUAN_URL.format(code=code),
                     headers={"User-Agent": UA},
                     timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    data = r.json().get("data") or {}
    if not data.get("fd_code"):
        raise RuntimeError("empty data")
    scale, scale_raw = parse_scale(data.get("totshare"))
    inception = None
    d0 = str(data.get("found_date") or "")
    if re.match(r"\d{4}-\d{2}-\d{2}", d0):
        inception = d0
    return (data.get("keeper_name"), data.get("manager_name"),
            scale, scale_raw, inception)


# 线程本地 F10Session（避免每次创建，复用同一 cookie）
_THREAD_LOCAL = threading.local()

def _f10_session() -> "F10Session":
    sess = getattr(_THREAD_LOCAL, "f10", None)
    if sess is None:
        sess = F10Session()
        _THREAD_LOCAL.f10 = sess
    return sess


def fetch_holding_http(code: str, year: int, typ: str):
    """线程本地 F10 拉持仓。

    Returns
    -------
    (rows, ok)
        ok=True  → 请求成功。rows 为空表示**该基金确实没有此类持仓**（如债基无股票）
        ok=False → 请求失败（限流 / 网络 / 解析异常），rows 恒为空

    ⚠ 必须区分这两种情况：把失败当成「无数据」会让数据静默丢失，
      而把失败写进进度文件更会让它**永远不再重试**。
    """
    try:
        return _f10_session().get_holdings(code, year, typ), True
    except Exception:
        return [], False


# ---------------------------------------------------------------------------
# Concurrent runners
# ---------------------------------------------------------------------------
def _write_scale(conn, code: str, info) -> bool:
    """主线程：写一条规模记录，立即 commit。失败时 rollback 清除中止状态。"""
    cur = conn.cursor()
    try:
        cur.execute(SQL_UPDATE_SCALE, (*info, code))
        conn.commit()
        return True
    except psycopg2.Error:
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def run_scale_concurrent(codes: list, conn, progress: Progress,
                          workers: int, stat: dict) -> None:
    """并发抓规模 + 主线程写库 + 进度落盘。"""
    if not codes:
        print("    规模无需刷新（DB 已齐全或已超出 TTL 命中）")
        return
    print(f"    并发抓取规模 线程={workers}  待刷 {len(codes)} 只 ...")
    t0 = time.time()
    done = 0
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch_scale_http, c): c for c in codes}
            for fut in as_completed(futures):
                code = futures[fut]
                try:
                    info = fut.result()
                    if _write_scale(conn, code, info):
                        stat["规模"] += 1
                        progress.mark(f"s:{code}")
                    else:
                        stat["规模失败"] += 1
                except (psycopg2.InterfaceError, psycopg2.OperationalError):
                    stat["规模失败"] += 1
                    # 连接断了：不重连重写（主线程只能持一个 conn），
                    # 留给下次启动时从进度判断时由 DB 兜底重跑
                except Exception as exc:
                    stat["规模失败"] += 1
                    # 失败也 mark，避免每次启动重试已知失败（如「(后端)」份额无 danjuanfunds 数据）
                    progress.mark(f"s:{code}")
                    if stat["规模失败"] <= 20:
                        print(f"    [SCALE] {code}: {type(exc).__name__} {str(exc)[:120]}")
                done += 1
                if done % 50 == 0 or done == len(codes):
                    progress.save(force=True)
                    elapsed = time.time() - t0
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (len(codes) - done) / rate / 60 if rate > 0 else 0
                    print(f"    规模 {done}/{len(codes)}  "
                          f"成功 {stat['规模']} 失败 {stat['规模失败']}  "
                          f"({rate:.1f}只/秒, 剩余约 {eta:.0f} 分钟)")
        progress.save(force=True)
    except Exception as exc:
        progress.save(force=True)
        raise


def run_holdings_concurrent(codes: list, conn, progress: Progress,
                              workers: int, years: list, bonds: bool,
                              today: date, stat: dict) -> None:
    """并发抓持仓。扁平化任务为 (code, year, typ)，主线程累积写入。"""
    if not codes:
        print("    持仓无需刷新（DB 已齐全）")
        return
    type_list = ["jjcc", "zqcc"] if bonds else ["jjcc"]
    typ_short  = {"jjcc": "stock", "zqcc": "bond"}
    # 按 (year, typ) 过滤已完成的（基于 DB）
    cur = conn.cursor()
    cur.execute("""
        SELECT fund_code, report_date, holding_type FROM fund_holdings
        WHERE fund_code = ANY(%s)
    """, (codes,))
    have_by_code = {}
    for fc, rd, ht in cur.fetchall():
        have_by_code.setdefault(fc, set()).add((str(rd), ht))

    tasks = []
    skipped_codes = 0
    for code in codes:
        have = have_by_code.get(code, set())
        # 全部 due 季度都齐了就整只跳过
        due_set = set()
        for y in years:
            for q in due_quarters(y, today):
                for typ in type_list:
                    due_set.add((str(q), typ_short[typ]))
        # 当前基金缺的 (year, typ) 任务
        need = []
        for y in years:
            for typ in type_list:
                # 抓一次该年就能拿到该年所有 due 季度，所以只要 (code, year, typ) 有
                # 任一 due 季度缺，就抓这一年
                if any((str(q), typ_short[typ]) not in have
                       for q in due_quarters(y, today)):
                    need.append((y, typ))
        # 进度文件记录本会话已完成该年该类的也跳过
        need = [(y, t) for y, t in need if not progress.has(f"hy:{code}:{y}:{t}")]
        if not need:
            skipped_codes += 1
            continue
        for y, typ in need:
            tasks.append((code, y, typ))

    stat["持仓跳过基金"] += skipped_codes
    if not tasks:
        print("    持仓 DB 已齐全，无需抓取")
        return

    print(f"    并发抓取持仓 线程={workers}  基金 {len(codes) - skipped_codes} 只"
          f"，任务 {len(tasks)} 个（跳过整只 {skipped_codes}） ...")
    t0 = time.time()
    done_tasks = 0
    rows_buf: list = []
    BUF_FLUSH = 5000

    def flush_rows():
        nonlocal rows_buf
        if not rows_buf:
            return
        # 去重（同一 (code, report_date, holding_type, security_code) 只取首个）
        seen, uniq = set(), []
        for r_ in rows_buf:
            k = (r_[0], r_[1], r_[2], r_[3])
            if k not in seen:
                seen.add(k)
                uniq.append(r_)
        # 防御性：截断超长字段（schema: security_code 12 / security_name 100）
        # QDII 海外债券的代码是 ISIN/CUSIP/英文标识，可达 20+ 字符
        uniq = [(
            r_[0], r_[1], r_[2],
            (str(r_[3])[:12] if r_[3] else None),
            (str(r_[4])[:100] if r_[4] else None),
            r_[5], r_[6], r_[7],
        ) for r_ in uniq]
        rows_buf = []
        cur = conn.cursor()
        try:
            execute_values(cur, SQL_UPSERT_HOLDING, uniq, page_size=1000)
            conn.commit()
            stat["持仓行"] += len(uniq)
        except psycopg2.Error as exc:
            # 截断后仍失败（极少见）→ 回滚再单条重试定位
            try:
                conn.rollback()
            except Exception:
                pass
            stat["持仓失败"] += 1
            if stat["持仓失败"] <= 5:
                print(f"    [HOLD SQL] 截断后批量仍失败: {str(exc).strip()[:160]}")
            # 单条 retry：每条前都先 rollback 清除中止状态
            failed_codes = []
            for r_ in uniq:
                try:
                    conn.rollback()
                    execute_values(cur, SQL_UPSERT_HOLDING, [r_], page_size=1)
                    conn.commit()
                except psycopg2.Error:
                    failed_codes.append(r_[0])
                    try:
                        conn.rollback()
                    except Exception:
                        pass
            if failed_codes and stat["持仓失败"] <= 5:
                print(f"    [HOLD SQL] 单条失败的基金: {set(failed_codes)}")

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch_holding_http, code, year, typ):
                       (code, year, typ)
                       for code, year, typ in tasks}
            for fut in as_completed(futures):
                code, year, typ = futures[fut]
                try:
                    rows, ok = fut.result()
                except Exception:
                    rows, ok = [], False
                if rows:
                    rows_buf.extend((code, *r_) for r_ in rows)
                if ok:
                    # 只有**请求确实成功**才标记完成（rows 为空 = 该基金真没这类持仓，
                    # 如债基的 jjcc，标记它可以避免无限重试空响应）
                    progress.mark(f"hy:{code}:{year}:{typ}")
                else:
                    # 失败不标记 → 下次运行自动重试，数据不会被永久漏掉
                    stat["持仓失败"] += 1
                    if stat["持仓失败"] <= 20:
                        print(f"    [HOLD] {code} {year} {typ}: 请求失败，未标记进度（下次重试）")
                done_tasks += 1
                if len(rows_buf) >= BUF_FLUSH:
                    flush_rows()
                if done_tasks % 200 == 0 or done_tasks == len(tasks):
                    progress.save(force=True)
                    elapsed = time.time() - t0
                    rate = done_tasks / elapsed if elapsed > 0 else 0
                    eta = (len(tasks) - done_tasks) / rate / 60 if rate > 0 else 0
                    print(f"    持仓任务 {done_tasks}/{len(tasks)}  "
                          f"行 {stat['持仓行']} 失败 {stat['持仓失败']}  "
                          f"({rate:.1f}任务/秒, 剩余约 {eta:.0f} 分钟)")
        flush_rows()
        progress.save(force=True)
    except Exception:
        flush_rows()
        progress.save(force=True)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="公募基金名录+规模+持仓抓取")
    ap.add_argument("--types",        default=None,  help='基金类型过滤，逗号分隔，子串匹配，如 "混合型-偏股,股票型"')
    ap.add_argument("--codes-file",   default=None,
                    help="只处理文件里的基金代码（每行一个），用于定向回补缺口")
    ap.add_argument("--request-interval", type=float, default=SLEEP,
                    help=f"全局最小请求间隔秒数（默认 {SLEEP}）。调小会触发东财 HTTP 514 限流")
    ap.add_argument("--limit",        type=int, default=None, help="限制规模/持仓抓取的基金数量（试点用）")
    ap.add_argument("--years-back",   type=int, default=0,     help="持仓回补年数（0=仅当年）")
    ap.add_argument("--bonds",        action="store_true", help="同时抓债券持仓（默认仅股票）")
    ap.add_argument("--refresh-scale", action="store_true", help="忽略 30 天节流，强制刷新规模")
    ap.add_argument("--full",         action="store_true", help="全量重写（忽略增量判断）")
    ap.add_argument("--dry-run",      action="store_true", help="只打印，不写库")
    ap.add_argument("--workers",      type=int, default=8,
                    help="并发线程数（默认 8）。设 1 退化为串行模式（兼容）")
    ap.add_argument("--reset-progress", action="store_true",
                    help="忽略现有进度文件，从零开始")
    args = ap.parse_args()

    # 全局请求间隔（东财频控防线，见 RateLimiter 与文件头说明）
    REQUEST_LIMITER.min_interval = max(0.0, args.request_interval)
    if args.request_interval < 0.5:
        print(f"  [WARN] --request-interval={args.request_interval}s < 0.5s，"
              f"大概率触发东财 HTTP 514 限流导致数据丢失")

    print("── 抓取公募基金名录 / 规模 / 持仓 ─────────────────────────────────────")

    # ── 1. 名录 ────────────────────────────────────────────────────────────
    try:
        names = ak.fund_name_em()
    except Exception as exc:
        print(f"  [FATAL] fund_name_em 失败: {exc}")
        return 1
    names = names[names["基金代码"].notna()]
    print(f"  名录 {len(names)} 只（天天基金，{names['基金类型'].nunique()} 种类型）")

    # scope 过滤
    scope = names
    if args.codes_file:
        try:
            with open(args.codes_file, "r", encoding="utf-8") as f:
                want = {ln.strip() for ln in f if ln.strip()}
        except OSError as exc:
            print(f"  [FATAL] 读取 --codes-file 失败: {exc}")
            return 1
        scope = scope[scope["基金代码"].isin(want)]
        missing = want - set(scope["基金代码"])
        print(f"  --codes-file {len(want)} 只，命中名录 {len(scope)} 只"
              + (f"，{len(missing)} 只不在名录（已清盘/更名）" if missing else ""))
        if scope.empty:
            print("  [FATAL] --codes-file 未命中任何基金")
            return 1
    if args.types:
        pats = [p.strip() for p in args.types.split(",") if p.strip()]
        mask = scope["基金类型"].fillna("").apply(
            lambda t: any(p in t for p in pats))
        scope = scope[mask]
        if scope.empty:
            print(f"  [FATAL] 类型 {args.types} 未匹配到任何基金")
            return 1
    if args.limit:
        scope = scope.head(args.limit)
    print(f"  本次规模/持仓范围: {len(scope)} 只"
          + (f"（types={args.types}）" if args.types else ""))

    today = date.today()
    years = list(range(today.year - args.years_back, today.year + 1))
    print(f"  持仓目标年份: {years}" + ("（含债券）" if args.bonds else "（仅股票）"))

    if args.dry_run:
        sample = scope.iloc[0]["基金代码"]
        print(f"\n[DRY-RUN] 跳过写库。抽样验证 {sample} 持仓解析:")
        try:
            f10 = F10Session()
            rows = f10.get_holdings(sample, today.year, "jjcc")
            for r_ in rows[:5]:
                print("   ", r_)
            print(f"    … 共 {len(rows)} 行")
        except Exception as exc:
            print(f"    [WARN] 抽样失败: {exc}")
        return 0

    # ── 2. 连库 ────────────────────────────────────────────────────────────
    print("\n连接数据库...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接: {exc}")
        return 1

    stat = {"名录": 0, "规模": 0, "规模跳过": 0, "规模失败": 0,
            "持仓行": 0, "持仓跳过基金": 0, "持仓失败": 0}

    def reconnect():
        """连接失效时重建连接（长任务防断线）。成功返回 True。"""
        nonlocal conn, cur
        try:
            conn.close()
        except Exception:
            pass
        for _ in range(5):
            time.sleep(5)
            try:
                conn = psycopg2.connect(DATABASE_URL)
                cur = conn.cursor()
                print("    [DB] 连接中断，已自动重连")
                return True
            except psycopg2.OperationalError:
                continue
        return False

    def abort_recover():
        """SQL 错误会中止当前事务，立即回滚清除中止状态（只影响当前基金）。"""
        try:
            conn.rollback()
        except Exception:
            pass

    try:
        cur = conn.cursor()
        # 名录全量 upsert（一次 execute_values，立即提交）
        now = pd.Timestamp.now(tz="UTC").to_pydatetime()
        name_rows = [(r["基金代码"], str(r["基金简称"]).strip(),
                      r["基金类型"], now) for _, r in names.iterrows()]
        execute_values(cur, SQL_UPSERT_FUND, name_rows, page_size=1000)
        stat["名录"] = len(name_rows)
        conn.commit()
        print(f"  [OK] funds 名录 upsert {len(name_rows)} 行")

        # ── 加载进度（断点续跑） ─────────────────────────────────────────
        # scope_key 包含影响结果的关键参数；改参数 → 进度自动重置
        scope_key = f"y={args.years_back}|b={int(bool(args.bonds))}"
        progress = Progress()
        if args.reset_progress:
            progress.reset()
            print("  [INFO] --reset-progress 已清空进度")
        progress.bind_scope(scope_key)
        already_in_progress = sum(1 for it in progress._items
                                   if it["k"].startswith("s:"))
        if already_in_progress:
            print(f"  [INFO] 进度文件已记录 {already_in_progress} 只规模"
                  f"、{len(progress._items)-already_in_progress} 条持仓，将跳过")

        # ── 3. 规模：按 DB + 进度计算 todo，并发抓取 ─────────────────────
        scope_codes = [r["基金代码"] for _, r in scope.iterrows()]
        cutoff_ts = time.time() - SCALE_TTL_DAYS * 86400

        if args.full or args.refresh_scale:
            todo_scale = scope_codes
        else:
            cur.execute("""
                SELECT fund_code, scale_updated_at FROM funds
                WHERE fund_code = ANY(%s) AND scale_updated_at IS NOT NULL
            """, (scope_codes,))
            done_in_db = {fc: ts for fc, ts in cur.fetchall()
                          if ts and ts.timestamp() > cutoff_ts}
            todo_scale = [c for c in scope_codes
                          if c not in done_in_db and not progress.has(f"s:{c}")]
        stat["规模跳过"] = len(scope_codes) - len(todo_scale)

        run_scale_concurrent(todo_scale, conn, progress, args.workers, stat)

        # ── 4. 持仓：直接传完整 scope，run_holdings_concurrent 内部按 DB+进度判断 ──
        run_holdings_concurrent(scope_codes, conn, progress, args.workers,
                                 years, args.bonds, today, stat)

        cur.execute("SELECT MAX(report_date) FROM fund_holdings")
        latest = cur.fetchone()[0]
        cur.execute(SQL_LOG_FETCH, (latest,))

        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"\n[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print(f"\n[OK] 基金数据更新完成：名录 {stat['名录']} / 规模更新 {stat['规模']}"
          f"（跳过 {stat['规模跳过']}，失败 {stat['规模失败']}）"
          f" / 持仓 {stat['持仓行']} 行（跳过 {stat['持仓跳过基金']} 只，"
          f"失败 {stat['持仓失败']}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
