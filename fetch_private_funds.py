#!/usr/bin/env python3
"""
Private Fund Fetcher (私募基金：中基协备案名录 + 天天基金代销池净值)
====================================================================

写入三块数据：

 1. private_managers — 私募基金管理人（~1.84 万家，中基协公示）
 2. private_funds    — 私募基金产品备案（~25 万只，中基协公示）
 3. private_fund_nav — 净值日线（**仅**天天基金高端理财代销池，1,208 只）

Data sources
------------
* 管理人:  POST https://gs.amac.org.cn/amac-infodisc/api/pof/manager
* 产品:    POST https://gs.amac.org.cn/amac-infodisc/api/pof/fund
          参数 `?rand=<随机>&page=<n>&size=<n>`，body `{}`，Referer 指到对应页面
* 净值:    GET  https://fund.eastmoney.com/gaoduan/PinzhongF10DataApi.aspx
                ?type=lsjz&fc=<备案编码>&pageSize=5000&pageIndex=1
* 代销池清单: GET https://fund.eastmoney.com/gaoduan/gaoduanDatas.js

⚠ 私募和公募不是一个物种（先说结论，省得以后白找）
------------------------------------------------------
公募那套「名录 + 规模 + 持仓 + 净值」四层，私募**只有第一层能全量对标**。
《私募投资基金募集行为管理办法》禁止公开宣传推介、禁止公开披露业绩，
所以管理规模 / 持仓 / 净值在公开渠道拿不到 —— 这是监管口径，不是技术问题。

拿得到的（中基协备案公示，官方免费）：
    产品 249,863 只、管理人 18,418 家
字段只有备案级：备案编码 / 名称 / 管理人 / 管理类型 / 运行状态 / 备案时间 /
成立时间 / 托管人 / 在管基金数量 —— **没有规模、没有净值、没有持仓**。

唯一能补净值的公开渠道是天天基金的**高端理财代销池**：
    覆盖 1,208 只 = 249,863 的 0.48%
且池子里大量是**券商资管 / 集合资管计划**（安信资管、长江资管、中航证券、
东莞证券、首创证券…），不是严格意义的私募证券基金。
**展示层必须写清这个覆盖率**，否则会让人误以为看到的是全市场私募业绩。

⚠ 中基协接口的频控（务必按这个防）
----------------------------------
连续请求十几次后开始持续返回 HTTP 400 / 500（body 分别为空 / `Server Error!`），
**等待 30s 仍不恢复**，按 IP 限流。实测 `tries=3 + sleep(2/8)` 的重试全部落空。
本脚本因此把间隔锁在 1.2s（`--amac-interval` 可调），并且**失败绝不写进度文件**。
另：`curl` 打 gs.amac.org.cn 一律 400/500，同一 URL 用 requests 就 200
（TLS 指纹问题）—— 复核探针别用 curl 下结论。

⚠ page 是 0-based，两个端点一致
--------------------------------------------
* `pof/fund`    page 从 **0** 开始，合法页号 `0 .. totalPages-1`
* `pof/manager` page 从 **0** 开始（同上）

本文档原先写「`pof/fund` page 从 1 开始（page=0 → HTTP 400）」——**错的**，
那是把**频控返回的 400** 误当语义限制。代价实测过：按 1..2500 扫会系统性漏掉
page 0 一整页（100 条），而 page 0 正是**全表最新**的 100 条备案（表按 `id` 倒序）。
`probe_amac_page0.py` 对 page 0/1/2/3 逐条查库：page 0 缺失 **100/100**，
page 1/2/3 缺失 **0/100**。已于 2026-09-17 修正。

⚠ 备案是「分页会漂移」，因此覆盖率只能靠多遍扫描逼近
------------------------------------------------
中基协 `pof/fund` 服务端**没有 ORDER BY**（响应元信息 `sort=null`），`sort=` 系参数
被静默忽略，`size` 硬顶 100，`sort`/日期区间筛选全不通 → 单遍扫描必有缺口。
唯一的旋钮是 `--passes N`（主键 upsert 天然去重）。实测收敛：
单遍 90.1% → 两遍 96.0% → 三遍 97.79% → 四遍 98.66%（分母用源自报 `totalElements`）。
另有约 700 条（0.28%）**源侧 `fundNo` 为空字符串**（2013~2016 年通道类资管计划），
我们 PK 是 `fund_no`，抓到也入不了库 —— 真实可达上限 ≈ 249,231。
详见 skill `market-datasource-layering/references/vendor-notes.md` 的 AMAC 节。

⚠ 写库必须批量 flush，进度文件只能表示「已入库」
----------------------------------------------
两条路径都是**每 N 页一次 upsert + commit**（产品 `--batch-pages 200`，管理人 50），
`progress` 只在提交成功后推进，因此 `funds_page_done` / `managers_page_done`
可以安全用于续跑。早期版本是「全部页抓完、rows 留内存、最后一次 upsert」，
那个设计的两个后果都实测踩过：
  ① 任一行某字段超长 → 整批 StringDataRightTruncation 回滚，45 分钟抓取全丢（踩过两次）；
  ② 进度文件若记「已抓页」而崩溃，续跑会跳过未写的页 → 静默缺口。

⚠ 中基协分页**没有稳定排序**，单遍扫描必然「重复若干 + 漏抓同样多」
--------------------------------------------------------------
2026-09-17 实测：紧接着请求 page=100 与 page=101，两页有 **4 条 registerNo 重叠**。
偏移量在扫描期间漂移，于是「某条被读到两次」的同时「另一条被挤到所有页之外」——
全量 185 页跑完，接口报 `totalElements=18418`，实际入库 **18,333** 行（少 85 = 重复 85），
两边数字正好对称，可以确认就是偏移漂移而非解析丢行。

试过用 `sort=` 钉死顺序，**接口静默忽略**该参数：`id,asc` / `registerNo,asc` /
`registerDate,desc` / `putOnRecordDate,asc` 四种取值返回的首三条完全一致（就是默认顺序），
而默认顺序本身就是 registerNo 升序 —— 说明服务端**根本没有 ORDER BY**。

**结论：只能用 `--passes N` 多遍扫描取并集**（upsert 天然去重）。
管理人一遍约 4 分钟，建议 3 遍；产品一遍约 45~75 分钟，视 `totalElements` 缺口决定是否补遍。
`--managers-only` / `--funds-only` 可只重跑其中一个，避免为补 80 行重扫 2500 页。

⚠ 东财净值接口的参数名踩坑
--------------------------
走势页内联 `defaults_jz = { t: "lsjz", fc: ..., pi: 1, pn: 10 }` 是**误导**：
`pn` 完全无效（传 10~5000 都只回 5 条），真正生效的是 **`pageSize`**（大小写不敏感）
和 **`pageIndex`**。`pageSize>=500` 时服务端直接返回全序列（响应里 `Pages=1`）；
个别超长历史（如 B00002 齐鲁金泰山2号 3,733 点）在 pageSize=2000 时被截断，
所以本脚本用 pageSize=5000 + **按响应的 `Pages` 兜底翻页**。

历史深度 / 成本
---------------
* 净值：抽样 15 只 **15/15 有数据**，区间最深到 2010-07-16，日频/周频混合
* 备案：全量约 2,700 请求 @1.2s ≈ 55 分钟（管理人 185 页 + 产品 2,499 页）
* 净值：1,208 请求 @0.6s ≈ 13 分钟

口径说明（写清，宁缺勿错）
--------------------------
* NAV    = 单位净值；ACCNAV = 累计净值（与公募同口径，面值 1.0 起）
* NAVCHGRT100 = 当日涨跌 **%**（如 -1.1445 表示 -1.1445%），写入 daily_return
  （源里另有 `NAVCHGRT` 是小数形式 -0.0114，**不要用**，口径易混）
* FUNDSIZE = 规模，单位**元**（如 929193350.64 = 9.29 亿）；**部分产品才有**，
  gaoduan 池里大多数券商资管不给这个字段 → 一律写 NULL，不要用 0 顶替
* 空 `NAV` 的条目直接跳过（池子里有非净值型产品），不写 0

已验证的「看着像 bug、其实是真值」——别去修
--------------------------------------------
1. `850012 海通海蓝宝润` 的 `unit_nav` 低到 0.0040，而 `acc_nav` 是 1.5513。
   看着像单位净值崩了，但**全序列 3,289 点逐日对照源侧，数值不符 0**
   （2026-09-17 实测）→ 是该产品自己做过份额折算，源侧就这么给。
2. `acc_nav` 为空的 607 行 / 5 只（B30002 / 941849 / 343074 / S23674 / S28815），
   全部落在 2013~2021 的历史段，**最新交易日无空值** → 源侧早期只披露单位净值。
   收益率走 `acc_nav > 0` 过滤，这些老点会被自动跳过，不影响「今年来 / 有数据以来」。
3. 单只点数从 1 到 3,733 都有：1 点的是刚成立的新产品（正常），
   3,733 点的是 B00002 齐鲁金泰山2号（2010-07-16 起，需 pageSize≥3000 才不被截断）。

Incremental updates
-------------------
* 备案：接口不支持增量，每次全量拉；分页进度写 progress 文件（**只记成功的页**），
  中断后可从最近的 flush 点续跑
* 净值：靠 DB 状态断点 —— 只处理 `private_fund_nav` 里还没有记录的 fund_no，
  幂等，重跑成本仅为时间

Compliance note
---------------
中基协公示数据为公开信息；天天基金 gaoduan 数据有版权声明，自建库内部研究无碍，
对外公开发布有风险（同 CCFI / 公募基金那批）。

Usage
-----
    python fetch_private_funds.py --limit-pages 2        # 小范围试点（各拉 2 页）
    python fetch_private_funds.py                         # 备案全量（约 55 分钟）
    python fetch_private_funds.py --nav                   # 代销池净值回填（约 13 分钟）
    python fetch_private_funds.py --nav --limit-nav 20    # 净值先跑 20 只试点
    python fetch_private_funds.py --dry-run               # 只抓不写库
    python fetch_private_funds.py --status                # 只看库内现状
"""

import argparse
import json
import os
import random
import re
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
BASE_DIR     = Path(__file__).parent
PROGRESS_FILE = BASE_DIR / ".workbuddy" / "private_funds_progress.json"

# 中国标准时间：源里的 ms 时间戳是 UTC 零点，必须固定偏移换算（用 localtime 在别的时区会错一天）
CST = timezone(timedelta(hours=8))

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

AMAC_BASE = "https://gs.amac.org.cn/amac-infodisc/api"
AMAC_MANAGER_URL = f"{AMAC_BASE}/pof/manager"
AMAC_FUND_URL    = f"{AMAC_BASE}/pof/fund"
AMAC_MANAGER_REF = "https://gs.amac.org.cn/amac-infodisc/res/pof/manager/index.html"
AMAC_FUND_REF    = "https://gs.amac.org.cn/amac-infodisc/res/pof/fund/index.html"

EM_GAODUAN_LIST = "https://fund.eastmoney.com/gaoduan/gaoduanDatas.js"
EM_GAODUAN_API  = "https://fund.eastmoney.com/gaoduan/PinzhongF10DataApi.aspx"
EM_REF          = "https://fund.eastmoney.com/gaoduan/"

TIMEOUT   = 40
RETRIES   = 4
PAGE_SIZE = 100          # 中基协每页条数（服务端若夹小，脚本会自适应）

DEFAULT_AMAC_INTERVAL = 1.2   # 中基协：实测限流很凶，1.2s 保守
DEFAULT_EM_INTERVAL   = 0.6   # 东财：514 频控经验值（见 fetch_funds.py docstring）


# ---------------------------------------------------------------------------
# Rate limiter —— 全局限速器（跨线程统一发牌，而不是每线程各自 sleep）
# ---------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, interval: float):
        self.interval = interval
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
                now = time.monotonic()
            self._next = now + self.interval


# ---------------------------------------------------------------------------
# Exceptions —— 区分「确定无数据」与「抓取失败」
# 铁律：只有 NoData 可以推进进度；FetchError 绝不能，否则缺口被永久跳过
# ---------------------------------------------------------------------------
class FetchError(Exception):
    """网络 / 限流 / 异常状态码 —— 不可推进进度"""


class NoData(Exception):
    """源明确表示没有数据 —— 可以推进进度"""


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _request(method: str, url: str, limiter: RateLimiter, *, params=None, referer=None,
             timeout=TIMEOUT):
    """带限速 + 指数退避重试的请求。最终失败抛 FetchError。"""
    headers = {"User-Agent": UA, "Accept-Language": "zh-CN,zh;q=0.9"}
    if referer:
        headers["Referer"] = referer
    last = None
    for attempt in range(RETRIES):
        limiter.wait()
        try:
            resp = requests.request(method, url, params=params, headers=headers,
                                    timeout=timeout, verify=False,
                                    json={} if method == "POST" else None)
            if resp.status_code == 200:
                return resp
            last = f"HTTP {resp.status_code}"
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"
        # 指数退避：中基协限流实测需要较长冷却
        time.sleep(min(2 ** attempt * 2, 30))
    raise FetchError(f"{method} {url} 失败（{RETRIES} 次重试）：{last}")


def amac_post(url: str, ref: str, page: int, size: int, limiter: RateLimiter) -> dict:
    params = {"rand": f"{random.random():.16f}", "page": str(page), "size": str(size)}
    resp = _request("POST", url, limiter, params=params, referer=ref)
    try:
        data = resp.json()
    except ValueError:
        raise FetchError(f"响应不是 JSON（可能被限流页替换）：{resp.text[:120]!r}")
    if not isinstance(data, dict) or "content" not in data:
        raise FetchError(f"响应缺 content 字段：{str(data)[:120]}")
    return data


# ---------------------------------------------------------------------------
# 解析 helpers
# ---------------------------------------------------------------------------
def ms_to_date(ms) -> date | None:
    """中基协的 ms 时间戳是 UTC 零点 → 按 UTC+8 换算成日期"""
    if ms in (None, "", 0):
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=CST).date()
    except (TypeError, ValueError, OSError):
        return None


def _num(val):
    """转 float，空值/非数字 → None（绝不返回 0 顶替）"""
    if val is None or val == "":
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return f


def _int(val):
    f = _num(val)
    return int(f) if f is not None else None


def _bool(val):
    if val is None:
        return None
    return bool(val)


def dedupe_by(rows: list[dict], key: str) -> tuple[list[dict], int]:
    """
    按主键在**批内**去重，保留最后一次出现（同名主键取后到的那条），返回 (行, 丢弃数)。

    为什么必须做：中基协分页无稳定排序 → 同一批 200 页里同一 fundNo 会出现两次。
    PostgreSQL 的 `ON CONFLICT DO UPDATE` 在**同一条命令**内不允许命中同一行两次，
    会直接 `CardinalityViolation: cannot affect row a second time` 整批回滚
    （2026-09-17 实测：pass 1 跑到 2001~2200 页那批炸掉，20,000 行回滚）。

    注意 execute_values 的 page_size 默认 100，而每页正好 100 条 —— 只有「某页
    返回不足 100 条」或「同页内自重复」导致的错位才会让两份落进同一个 100 行块。
    所以这个 bug 是**概率触发**的：前面 10 批都没事，第 11 批才炸。
    去重放在 Python 侧对 SQL 无副作用（upsert 本来就幂等），且顺带把不确定变成确定。

    ⚠ 主键为空的行会被**计入丢弃数并移除**（空主键本来就无法满足 NOT NULL/ON CONFLICT）；
    调用方应先做非空过滤并单独告警，别把这个数字当成纯「重复数」。
    """
    if not rows:
        return rows, 0
    last: dict = {}
    for r in rows:
        k = r.get(key)
        if k:
            last[k] = r
    if len(last) == len(rows):
        return rows, 0
    kept, emitted = [], set()
    for r in reversed(rows):          # 倒着走 → 每个键首次遇到的就是「最后一次出现」
        k = r.get(key)
        if not k or k in emitted:
            continue
        emitted.add(k)
        kept.append(r)
    kept.reverse()                    # 恢复原始顺序
    return kept, len(rows) - len(kept)


# ---------------------------------------------------------------------------
# 中基协：管理人 / 产品
# ---------------------------------------------------------------------------
def fetch_managers(limiter: RateLimiter, max_pages: int | None = None,
                   progress: dict | None = None, on_flush=None,
                   on_batch=None, batch_pages: int = 50) -> list[dict]:
    """
    私募基金管理人。page 从 **0** 开始。
    字段名照 akshare 的 amac_manager_classify_info（比 amac_manager_info 更全）。

    `on_batch(rows)` 每 `batch_pages` 页被调用一次，**由它负责写库并 commit**；
    调用返回后本函数会把已提交的行从 `rows` 里清掉，所以 `rows` 在返回时只装「尾部未提交」的部分。
    只有批量提交成功了才推进 `progress`，这样进度文件才真正代表「已入库」。
    """
    rows: list[dict] = []
    first = amac_post(AMAC_MANAGER_URL, AMAC_MANAGER_REF, 0, PAGE_SIZE, limiter)
    total_pages = int(first.get("totalPages") or 0)
    total_elems = first.get("totalElements")
    print(f"  [管理人] totalElements={total_elems}  totalPages={total_pages}")
    if total_pages == 0:
        return rows

    start = 0
    done_pages = (progress or {}).get("managers_page_done", 0)
    if 0 < done_pages < total_pages:
        # 进度文件只在「批量入库成功」后推进，所以这里是可信的续跑点
        start = done_pages
        print(f"  [管理人] 从进度文件续跑，已入库 {start}/{total_pages} 页")
    elif done_pages:
        print(f"  [管理人] 进度({done_pages}) 已覆盖全部页，本次从第 0 页重扫")

    end = total_pages if max_pages is None else min(max_pages, total_pages)
    for page in range(start, end):
        data = first if page == 0 else amac_post(AMAC_MANAGER_URL, AMAC_MANAGER_REF,
                                                 page, PAGE_SIZE, limiter)
        for c in data.get("content") or []:
            rows.append({
                "register_no":       (c.get("registerNo") or "").strip() or None,
                "manager_id":        _int(c.get("id")),
                "manager_name":      (c.get("managerName") or "").strip(),
                "artificial_person": c.get("artificialPersonName"),
                "invest_type":       c.get("primaryInvestType"),
                "register_province": c.get("registerProvince"),
                "office_address":    c.get("officeAdrAgg"),
                "establish_date":    ms_to_date(c.get("establishDate")),
                "register_date":     ms_to_date(c.get("registerDate")),
                "fund_count":        _int(c.get("fundCount")),
                "member_type":       c.get("memberType"),
                "has_special_tips":  _bool(c.get("hasSpecialTips")),
                "has_credit_tips":   _bool(c.get("hasCreditTips")),
            })
        if (page + 1) % 25 == 0 or page == end - 1:
            print(f"  [管理人] {page + 1}/{end} 页，累计 {len(rows)} 条")
        if on_batch is not None and (page + 1) % batch_pages == 0:
            n = len(rows)
            on_batch(rows)          # 内部负责 commit
            rows.clear()
            if progress is not None and on_flush is not None:
                progress["managers_page_done"] = page + 1
                on_flush(progress)
            print(f"  [BATCH] 管理人已入库到 {page + 1}/{end} 页（本批 {n} 行）")
    return rows


def fetch_funds(limiter: RateLimiter, max_pages: int | None = None,
                progress: dict | None = None, on_flush=None,
                on_batch=None, batch_pages: int = 200) -> list[dict]:
    """
    私募基金产品备案。page 从 **0** 开始（与 `pof/manager` 一致）。

    ⚠ 2026-09-17 修正：本文档原先写「page 从 1 开始（page=0 → HTTP 400）」，
    那是把**频控返回的 400** 误当成了语义限制。实测 page=0 正常返回 100 条
    （`number=0`），而 page 1/2/3 的首条分别是 SCR707/SEG238/SEJ763。后果很实在：
    按 1..2500 扫会**系统性漏掉 page 0 那一页**，实测该页 100 条 fund_no
    **在库内一条都没有**（同批 page 1/2/3 是 100% 在库）。全表按 id 倒序，
    page 0 就是最新的 100 条备案记录。

    `on_batch(rows)` 每 `batch_pages` 页被调用一次，**由它负责写库并 commit**；
    返回后本函数把已提交的行清掉，`rows` 在返回时只装「尾部未提交」的部分。
    25 万行一次性 INSERT 有两个坏处，都实测踩过：
      ① 任一行的某个字段超长 → 整批 StringDataRightTruncation 回滚，45 分钟抓取全丢；
      ② 单条 INSERT 拼几千个 VALUES，内存与 SQL 长度都很夸张。
    改成每 200 页（2 万行）提交一次后，最坏情况只丢当前这一批。
    """
    rows: list[dict] = []
    first = amac_post(AMAC_FUND_URL, AMAC_FUND_REF, 0, PAGE_SIZE, limiter)
    total_pages = int(first.get("totalPages") or 0)
    total_elems = first.get("totalElements")
    print(f"  [产品]   totalElements={total_elems}  totalPages={total_pages}")
    if total_pages == 0:
        return rows

    # page 0-based：合法页号是 0 .. total_pages-1
    start = 0
    done_page = (progress or {}).get("funds_page_done", 0)
    if 0 < done_page < total_pages:
        # 进度文件只在「批量入库成功」后推进，所以这里是可信的续跑点；
        # done_page = 已入库的页数，下一页的页号正好等于它
        start = done_page
        print(f"  [产品]   从进度文件续跑，已入库 {done_page}/{total_pages} 页")
    elif done_page:
        print(f"  [产品]   进度({done_page}) 已覆盖全部页，本次从第 0 页重扫")

    end = total_pages if max_pages is None else min(max_pages, total_pages)
    for page in range(start, end):
        data = first if page == 0 else amac_post(AMAC_FUND_URL, AMAC_FUND_REF,
                                                 page, PAGE_SIZE, limiter)
        for c in data.get("content") or []:
            mgr = (c.get("managersInfo") or [{}])[0] if c.get("managersInfo") else {}
            rows.append({
                "fund_no":          (c.get("fundNo") or "").strip() or None,
                "fund_name":        (c.get("fundName") or "").strip(),
                "manager_name":     c.get("managerName"),
                "manager_id":       _int(mgr.get("managerId")) if mgr else _int(c.get("managerId")),
                "manager_type":     c.get("managerType"),
                "working_state":    c.get("workingState"),
                "record_date":      ms_to_date(c.get("putOnRecordDate")),
                "establish_date":   ms_to_date(c.get("establishDate")),
                "mandator_name":    c.get("mandatorName"),
                "is_depute_manage": (_bool(c.get("isDeputeManage"))
                                     if not isinstance(c.get("isDeputeManage"), str)
                                     else c.get("isDeputeManage") == "是"),
            })
        if (page + 1) % 100 == 0 or page == end - 1:
            print(f"  [产品]   {page + 1}/{end} 页，累计 {len(rows)} 条")
        if on_batch is not None and (page + 1) % batch_pages == 0:
            n = len(rows)
            on_batch(rows)          # 内部负责 commit
            rows.clear()
            if progress is not None and on_flush is not None:
                # progress 记「已入库页数」（1-based），续跑时直接当下一页页号
                progress["funds_page_done"] = page + 1
                on_flush(progress)
            print(f"  [BATCH] 产品已入库到 {page + 1}/{end} 页（本批 {n} 行）")
    return rows


# ---------------------------------------------------------------------------
# 天天基金：代销池清单 + 净值
# ---------------------------------------------------------------------------
def fetch_gaoduan_list(limiter: RateLimiter) -> list[tuple[str, str]]:
    """代销池全量产品清单，来自 gaoduanDatas.js 内联的 [[code, name, ?, pinyin], ...]"""
    resp = _request("GET", EM_GAODUAN_LIST, limiter, referer=EM_REF)
    resp.encoding = "utf-8"
    pairs = re.findall(r'\["([A-Za-z0-9]{4,8})","([^"]+)"', resp.text)
    seen, out = set(), []
    for code, name in pairs:
        if code in seen:
            continue
        seen.add(code)
        out.append((code, name))
    if not out:
        raise FetchError("gaoduanDatas.js 未解析出任何产品码（页面结构可能已变更）")
    return out


def fetch_gaoduan_nav(code: str, limiter: RateLimiter) -> list[dict]:
    """
    单只产品全历史净值。pageSize>=500 时服务端返回全序列；
    个别超长历史会被截断，所以按响应的 Pages 兜底翻页。

    Returns [{'nav_date', 'unit_nav', 'acc_nav', 'daily_return', 'fund_size'}]
    """
    points: list[dict] = []
    page = 1
    while page <= 20:                      # 硬上限，防病态循环
        params = {"type": "lsjz", "fc": code, "pageSize": "5000", "pageIndex": str(page)}
        resp = _request("GET", EM_GAODUAN_API, limiter, params=params, referer=EM_REF)
        resp.encoding = "utf-8"
        m = re.search(r"\{.*\}", resp.text, re.S)
        if not m:
            if not resp.text.strip():
                raise NoData(f"{code} 返回空 body")
            raise FetchError(f"{code} 响应无 JSON：{resp.text[:100]!r}")
        try:
            data = json.loads(m.group(0))
        except ValueError:
            raise FetchError(f"{code} JSON 解析失败：{resp.text[:100]!r}")

        datas = data.get("Datas") or []
        if not datas:
            if page == 1:
                raise NoData(f"{code} 序列为空（ErrCode={data.get('ErrCode')}）")
            break

        for d in datas:
            nav_date = (d.get("PDATE") or "").strip()
            if not nav_date:
                continue
            points.append({
                "nav_date":     nav_date,
                "unit_nav":     _num(d.get("NAV")),
                "acc_nav":      _num(d.get("ACCNAV")),
                "daily_return": _num(d.get("NAVCHGRT100")),   # 百分数口径，见 docstring
                "fund_size":    _num(d.get("FUNDSIZE")),      # 元，可能为 None
            })

        pages = int(data.get("Pages") or 1)
        if pages <= 1 or page >= pages:
            break
        page += 1

    # 去重（翻页可能有边界重叠）+ 按日期升序
    by_date: dict[str, dict] = {}
    for p in points:
        by_date[p["nav_date"]] = p
    out = sorted(by_date.values(), key=lambda p: p["nav_date"])
    if not out:
        raise NoData(f"{code} 无有效净值点")
    return out


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS private_managers (
  register_no        varchar(40) PRIMARY KEY,
  manager_id         bigint,
  manager_name       varchar(200) NOT NULL,
  artificial_person  varchar(120),
  invest_type        varchar(80),
  register_province  varchar(80),
  office_address     varchar(300),
  establish_date     date,
  register_date      date,
  fund_count         integer,
  member_type        varchar(80),
  has_special_tips   boolean,
  has_credit_tips    boolean,
  updated_at         timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS private_funds (
  fund_no             varchar(40) PRIMARY KEY,
  fund_name           varchar(300) NOT NULL,
  manager_name        varchar(200),
  manager_id          bigint,
  manager_type        varchar(40),
  working_state       varchar(40),
  record_date         date,
  establish_date      date,
  mandator_name       varchar(200),
  is_depute_manage    boolean,
  in_registry         boolean NOT NULL DEFAULT false,
  has_nav             boolean NOT NULL DEFAULT false,
  latest_nav          numeric(14,4),
  latest_acc_nav      numeric(14,4),
  latest_nav_date     date,
  latest_daily_return numeric(10,4),
  latest_fund_size    numeric(20,2),
  nav_updated_at      timestamptz,
  updated_at          timestamptz NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS private_fund_nav (
  id           bigserial PRIMARY KEY,
  fund_no      varchar(40) NOT NULL,
  nav_date     date NOT NULL,
  unit_nav     numeric(14,4),
  acc_nav      numeric(14,4),
  daily_return numeric(10,4),
  UNIQUE (fund_no, nav_date)
);
CREATE INDEX IF NOT EXISTS idx_pf_nav_fund_date ON private_fund_nav (fund_no, nav_date);
CREATE INDEX IF NOT EXISTS idx_private_funds_state ON private_funds (working_state);
CREATE INDEX IF NOT EXISTS idx_private_funds_nav ON private_funds (has_nav);
CREATE INDEX IF NOT EXISTS idx_private_funds_registry ON private_funds (in_registry);
CREATE INDEX IF NOT EXISTS idx_private_funds_mgr ON private_funds (manager_name);
CREATE INDEX IF NOT EXISTS idx_private_managers_name ON private_managers (manager_name);

-- ⚠ 名称类字段一律放宽到 text。
--   2026-09-17 实测：中基协有个别私募名称超过 varchar(200)，
--   整批 249,831 行 INSERT 直接 StringDataRightTruncation 回滚 —— 抓了 45 分钟一条没进库。
--   报错信息只会说「值太长了(200)」而不说是哪一行哪一列，只能靠事后二分；
--   与其猜长度，不如不留长度上限（PG 里 text 与 varchar 的性能没有区别）。
--   下面这些 ALTER 是幂等的，重复执行无副作用（已是 text 时为 no-op）。
ALTER TABLE private_managers ALTER COLUMN register_no       TYPE varchar(60);
ALTER TABLE private_managers ALTER COLUMN manager_name      TYPE text;
ALTER TABLE private_managers ALTER COLUMN artificial_person TYPE text;
ALTER TABLE private_managers ALTER COLUMN invest_type       TYPE text;
ALTER TABLE private_managers ALTER COLUMN register_province TYPE text;
ALTER TABLE private_managers ALTER COLUMN office_address    TYPE text;
ALTER TABLE private_managers ALTER COLUMN member_type       TYPE text;
ALTER TABLE private_funds    ALTER COLUMN fund_no           TYPE varchar(60);
ALTER TABLE private_funds    ALTER COLUMN fund_name         TYPE text;
ALTER TABLE private_funds    ALTER COLUMN manager_name      TYPE text;
ALTER TABLE private_funds    ALTER COLUMN manager_type      TYPE text;
ALTER TABLE private_funds    ALTER COLUMN working_state     TYPE text;
ALTER TABLE private_funds    ALTER COLUMN mandator_name     TYPE text;
ALTER TABLE private_fund_nav ALTER COLUMN fund_no           TYPE varchar(60);
"""

UPSERT_MANAGERS_SQL = """
INSERT INTO private_managers (
  register_no, manager_id, manager_name, artificial_person, invest_type,
  register_province, office_address, establish_date, register_date,
  fund_count, member_type, has_special_tips, has_credit_tips, updated_at
) VALUES %s
ON CONFLICT (register_no) DO UPDATE SET
  manager_id        = COALESCE(EXCLUDED.manager_id,        private_managers.manager_id),
  manager_name      = EXCLUDED.manager_name,
  artificial_person = COALESCE(EXCLUDED.artificial_person, private_managers.artificial_person),
  invest_type       = COALESCE(EXCLUDED.invest_type,       private_managers.invest_type),
  register_province = COALESCE(EXCLUDED.register_province, private_managers.register_province),
  office_address    = COALESCE(EXCLUDED.office_address,    private_managers.office_address),
  establish_date    = COALESCE(EXCLUDED.establish_date,    private_managers.establish_date),
  register_date     = COALESCE(EXCLUDED.register_date,     private_managers.register_date),
  fund_count        = COALESCE(EXCLUDED.fund_count,        private_managers.fund_count),
  member_type       = COALESCE(EXCLUDED.member_type,       private_managers.member_type),
  has_special_tips  = COALESCE(EXCLUDED.has_special_tips,  private_managers.has_special_tips),
  has_credit_tips   = COALESCE(EXCLUDED.has_credit_tips,   private_managers.has_credit_tips),
  updated_at        = NOW()
"""

# 备案字段全部 COALESCE：净值通道只更新 has_nav / 快照列，绝不能把备案字段抹成 NULL
UPSERT_FUNDS_SQL = """
INSERT INTO private_funds (
  fund_no, fund_name, manager_name, manager_id, manager_type, working_state,
  record_date, establish_date, mandator_name, is_depute_manage, in_registry, updated_at
) VALUES %s
ON CONFLICT (fund_no) DO UPDATE SET
  fund_name        = EXCLUDED.fund_name,
  manager_name     = COALESCE(EXCLUDED.manager_name,    private_funds.manager_name),
  manager_id       = COALESCE(EXCLUDED.manager_id,      private_funds.manager_id),
  manager_type     = COALESCE(EXCLUDED.manager_type,    private_funds.manager_type),
  working_state    = COALESCE(EXCLUDED.working_state,   private_funds.working_state),
  record_date      = COALESCE(EXCLUDED.record_date,     private_funds.record_date),
  establish_date   = COALESCE(EXCLUDED.establish_date,  private_funds.establish_date),
  mandator_name    = COALESCE(EXCLUDED.mandator_name,   private_funds.mandator_name),
  is_depute_manage = COALESCE(EXCLUDED.is_depute_manage, private_funds.is_depute_manage),
  in_registry      = TRUE,
  updated_at       = NOW()
"""

# gaoduan 代销池：单行 upsert，确保产品行存在后再写快照列。
# ⚠ 顺序要紧：必须先执行这条，再执行 SNAPSHOT_NAV_SQL —— 否则 UPDATE 匹配不到行，
#   快照列会静默留空（2026-09-17 首跑踩过：行表 4200 行，快照列全 NULL）。
# in_registry 只在 INSERT 时置 FALSE；若已在备案库，CONFLICT 分支不动它（保留 TRUE）。
# fund_name 优先保留已有的（备案正式名比代销池简称完整）。
UPSERT_FUND_GAODUAN_SQL = """
INSERT INTO private_funds (fund_no, fund_name, in_registry, has_nav, updated_at)
VALUES (%s, %s, FALSE, TRUE, NOW())
ON CONFLICT (fund_no) DO UPDATE SET
  fund_name  = COALESCE(private_funds.fund_name, EXCLUDED.fund_name),
  has_nav    = TRUE,
  updated_at = NOW()
"""

UPSERT_NAV_SQL = """
INSERT INTO private_fund_nav (fund_no, nav_date, unit_nav, acc_nav, daily_return)
VALUES %s
ON CONFLICT (fund_no, nav_date) DO UPDATE SET
  unit_nav     = COALESCE(EXCLUDED.unit_nav,     private_fund_nav.unit_nav),
  acc_nav      = COALESCE(EXCLUDED.acc_nav,      private_fund_nav.acc_nav),
  daily_return = COALESCE(EXCLUDED.daily_return, private_fund_nav.daily_return)
"""

# 快照列：必须带日期守卫，别把库里的最新值打回旧值
SNAPSHOT_NAV_SQL = """
UPDATE private_funds f SET
  latest_nav          = v.unit_nav,
  latest_acc_nav      = v.acc_nav,
  latest_nav_date     = v.nav_date,
  latest_daily_return = v.daily_return,
  latest_fund_size    = COALESCE(v.fund_size, f.latest_fund_size),
  has_nav             = TRUE,
  nav_updated_at      = NOW(),
  updated_at          = NOW()
FROM (
  SELECT DISTINCT ON (fund_no) fund_no, nav_date, unit_nav, acc_nav, daily_return, fund_size
  FROM (VALUES %s) AS t(fund_no, nav_date, unit_nav, acc_nav, daily_return, fund_size)
  ORDER BY fund_no, nav_date DESC
) v
WHERE f.fund_no = v.fund_no
  AND (f.latest_nav_date IS NULL OR f.latest_nav_date <= v.nav_date)
"""


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
    conn.commit()


# ---------------------------------------------------------------------------
# Progress file（只记成功，失败绝不写）
# ---------------------------------------------------------------------------
def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            print("  [WARN] 进度文件损坏，忽略")
    return {}


def save_progress(progress: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False, indent=1),
                             encoding="utf-8")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
def print_status(conn):
    with conn.cursor() as cur:
        print("  ── 库内现状 ──")
        for title, sql in [
            ("private_managers", "SELECT COUNT(*), COUNT(fund_count) FROM private_managers"),
            ("private_funds",    "SELECT COUNT(*), COUNT(*) FILTER (WHERE in_registry), "
                                 "COUNT(*) FILTER (WHERE has_nav) FROM private_funds"),
            ("private_fund_nav", "SELECT COUNT(*), COUNT(DISTINCT fund_no), "
                                 "MIN(nav_date)::text, MAX(nav_date)::text FROM private_fund_nav"),
        ]:
            try:
                cur.execute(sql)
                print(f"    {title:20s} {cur.fetchone()}")
            except psycopg2.errors.UndefinedTable:
                conn.rollback()
                print(f"    {title:20s} 表不存在")
        cur.execute("SELECT working_state, COUNT(*) FROM private_funds "
                    "GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
        print("    ── 运行状态分布 ──")
        for st, n in cur.fetchall():
            print(f"      {str(st):12s} {n}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="私募基金数据抓取（中基协备案 + 天天基金代销池净值）")
    ap.add_argument("--nav", action="store_true", help="抓取代销池净值（默认不抓）")
    ap.add_argument("--registry-only", action="store_true", help="只抓备案，不抓净值")
    ap.add_argument("--nav-only", action="store_true", help="只抓净值，不抓备案")
    ap.add_argument("--limit-pages", type=int, default=None, help="备案每个端点最多抓几页（试点）")
    ap.add_argument("--limit-nav", type=int, default=None, help="净值最多抓几只（试点）")
    ap.add_argument("--dry-run", action="store_true", help="只抓不写库")
    ap.add_argument("--full", action="store_true", help="忽略进度文件，从第 1 页重来")
    ap.add_argument("--status", action="store_true", help="只显示库内现状")
    ap.add_argument("--amac-interval", type=float, default=DEFAULT_AMAC_INTERVAL)
    ap.add_argument("--em-interval",   type=float, default=DEFAULT_EM_INTERVAL)
    ap.add_argument("--managers-only", action="store_true", help="只抓管理人（跳过产品）")
    ap.add_argument("--funds-only",    action="store_true", help="只抓产品（跳过管理人）")
    ap.add_argument("--batch-pages", type=int, default=200,
                    help="备案每抓多少页就批量提交一次（产品默认 200 页=2 万行）。"
                         "调小可在崩溃时少丢数据；调大则提交次数少")
    ap.add_argument("--passes", type=int, default=1,
                    help="备案每个实体扫几遍并取并集。中基协分页无稳定排序，"
                         "单遍会因偏移漂移「重复若干条 + 漏抓同样多条」，多遍可收敛（建议 3）")
    args = ap.parse_args()

    requests.packages.urllib3.disable_warnings()  # noqa

    conn = None
    if not args.dry_run:
        try:
            conn = psycopg2.connect(DATABASE_URL)
            ensure_schema(conn)
        except psycopg2.OperationalError as exc:
            print(f"  [FATAL] 无法连接数据库: {exc}")
            return 1

    if args.status:
        if conn:
            print_status(conn)
            conn.close()
        return 0

    do_registry = not args.nav_only
    do_nav      = args.nav or args.nav_only
    if args.registry_only:
        do_nav = False

    amac_limiter = RateLimiter(args.amac_interval)
    em_limiter   = RateLimiter(args.em_interval)

    # ── 备案 ────────────────────────────────────────────────────────────────
    if do_registry:
        print("\n── 中基协备案公示 ──────────────────────────────────────────────")
        progress = {} if args.full else load_progress()

        # ⚠ 两个实体各自「抓完即写库」，不要合并成最后统一写。
        #   否则一旦在写库前崩溃，progress 里 *_page_done 已记满 →
        #   续跑时守卫会重抓（见 fetch_* 里的末页守卫），但更稳的是根本不留这个窗口。
        def write_managers(rows):
            keep = [r for r in rows if r["register_no"] and r["manager_name"]]
            if len(keep) != len(rows):
                print(f"  [WARN] {len(rows) - len(keep)} 条管理人缺登记编号/名称，已跳过")
            keep, dup = dedupe_by(keep, "register_no")
            if dup:
                print(f"  [DEDUP] 管理人批内主键重复 {dup} 条（源端分页无稳定排序），保留后到的那条")
            if not keep:
                return
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_MANAGERS_SQL, [
                    (m["register_no"], m["manager_id"], m["manager_name"],
                     m["artificial_person"], m["invest_type"], m["register_province"],
                     m["office_address"], m["establish_date"], m["register_date"],
                     m["fund_count"], m["member_type"], m["has_special_tips"],
                     m["has_credit_tips"], datetime.now(tz=CST))
                    for m in keep
                ])
            conn.commit()
            print(f"  [OK] private_managers 写入 {len(keep)} 行")

        def write_funds(rows):
            keep = [r for r in rows if r["fund_no"] and r["fund_name"]]
            if len(keep) != len(rows):
                print(f"  [WARN] {len(rows) - len(keep)} 条产品缺备案编码/名称，已跳过")
            keep, dup = dedupe_by(keep, "fund_no")
            if dup:
                print(f"  [DEDUP] 产品批内主键重复 {dup} 条（源端分页无稳定排序），保留后到的那条")
            if not keep:
                return
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_FUNDS_SQL, [
                    (f["fund_no"], f["fund_name"], f["manager_name"], f["manager_id"],
                     f["manager_type"], f["working_state"], f["record_date"],
                     f["establish_date"], f["mandator_name"], f["is_depute_manage"],
                     True, datetime.now(tz=CST))
                    for f in keep
                ])
            conn.commit()
            print(f"  [OK] private_funds 写入 {len(keep)} 行")

        entities = [
            ("管理人", fetch_managers, write_managers, "managers_page_done", min(50, args.batch_pages)),
            ("产品",   fetch_funds,    write_funds,    "funds_page_done",    args.batch_pages),
        ]
        if args.managers_only:
            entities = [e for e in entities if e[0] == "管理人"]
        if args.funds_only:
            entities = [e for e in entities if e[0] == "产品"]

        for label, fetch_fn, write_fn, prog_key, batch_pages in entities:
            for p in range(1, args.passes + 1):
                if args.passes > 1:
                    print(f"\n  ── {label} 第 {p}/{args.passes} 遍"
                          f"（中基协分页无稳定排序，多遍取并集补漏）──")
                try:
                    rows = fetch_fn(amac_limiter, args.limit_pages, progress, save_progress,
                                    on_batch=write_fn, batch_pages=batch_pages)
                except FetchError as exc:
                    print(f"  [FATAL] {label}抓取失败：{exc}")
                    print(f"  ⚠ 已批量入库的批次不受影响；续跑会从进度文件记录的页继续")
                    conn and conn.close()
                    return 1

                print(f"\n  抓取完成：{label} 第 {p} 遍，尾部待提交 {len(rows)} 条")

                if args.dry_run:
                    print("  [DRY-RUN] 跳过写库")
                    break
                if not conn:
                    break

                try:
                    write_fn(rows)
                except Exception as exc:
                    conn.rollback()
                    print(f"  [FATAL] {label}写库失败：{exc}")
                    import traceback; traceback.print_exc()
                    conn.close()
                    return 1
                finally:
                    del rows  # 产品一遍 25 万行，及时释放，别在两遍之间叠着占内存

                # 写库成功后才清进度（接口无增量能力，下次仍是全量）
                if not args.limit_pages:
                    progress.pop(prog_key, None)
                    save_progress(progress)

    # ── 代销池净值 ──────────────────────────────────────────────────────────
    if do_nav:
        print("\n── 天天基金高端理财代销池净值 ──────────────────────────────────")
        try:
            pool = fetch_gaoduan_list(em_limiter)
        except FetchError as exc:
            print(f"  [FATAL] 代销池清单抓取失败：{exc}")
            conn and conn.close()
            return 1
        print(f"  [清单] 代销池共 {len(pool)} 只产品")

        # 断点：只处理 private_fund_nav 里还没有记录的
        done: set[str] = set()
        if conn and not args.full:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT fund_no FROM private_fund_nav")
                done = {r[0] for r in cur.fetchall()}
            print(f"  [断点] 已有净值的基金 {len(done)} 只，将跳过")

        todo = [(c, n) for c, n in pool if c not in done]
        if args.limit_nav:
            todo = todo[:args.limit_nav]
        print(f"  [计划] 待抓 {len(todo)} 只，间隔 {args.em_interval}s "
              f"≈ {len(todo) * args.em_interval / 60:.1f} 分钟")

        if args.dry_run:
            print("  [DRY-RUN] 跳过净值写库")
            conn and conn.close()
            return 0

        ok = nodata = fail = 0
        failed_codes: list[str] = []
        t0 = time.time()

        for i, (code, name) in enumerate(todo, 1):
            try:
                pts = fetch_gaoduan_nav(code, em_limiter)
            except NoData as exc:
                nodata += 1
                print(f"  [{i}/{len(todo)}] {code:8s} 无数据（{exc}）")
                continue
            except FetchError as exc:
                fail += 1
                failed_codes.append(code)
                print(f"  [{i}/{len(todo)}] {code:8s} ✘ 抓取失败：{exc}")
                continue                      # 失败不写任何东西

            try:
                with conn.cursor() as cur:
                    # ⚠ 顺序：① 先确保产品行存在 ② 写净值行表 ③ 再更新快照列
                    #   （③ 是 UPDATE，若 ① 没先执行就会静默匹配 0 行）
                    cur.execute(UPSERT_FUND_GAODUAN_SQL, (code, name))
                    execute_values(cur, UPSERT_NAV_SQL, [
                        (code, p["nav_date"], p["unit_nav"], p["acc_nav"], p["daily_return"])
                        for p in pts
                    ])
                    execute_values(cur, SNAPSHOT_NAV_SQL, [
                        (code, p["nav_date"], p["unit_nav"], p["acc_nav"],
                         p["daily_return"], p["fund_size"])
                        for p in pts
                    ], template="(%s::varchar,%s::date,%s::numeric,%s::numeric,%s::numeric,%s::numeric)")
                conn.commit()
                ok += 1
            except Exception as exc:
                conn.rollback()
                fail += 1
                failed_codes.append(code)
                print(f"  [{i}/{len(todo)}] {code:8s} ✘ 写库失败：{exc}")
                continue

            if i % 25 == 0 or i == len(todo):
                rate = i / max(time.time() - t0, 1e-9)
                eta = (len(todo) - i) / max(rate, 1e-9)
                print(f"  [{i}/{len(todo)}] 已入库 {ok} 只，{pts[-1]['nav_date']} ~ {pts[0]['nav_date']}"
                      f"  （{rate:.2f} 只/s，剩余约 {eta / 60:.1f} 分钟）")

        print(f"\n  净值完成：成功 {ok}，无数据 {nodata}，失败 {fail}"
              f"，耗时 {(time.time() - t0) / 60:.1f} 分钟")
        if failed_codes:
            print(f"  ⚠ 失败清单（下次重跑会自动补，因为这些 fund_no 没写进库）：")
            print(f"    {', '.join(failed_codes[:20])}{' ...' if len(failed_codes) > 20 else ''}")

    if conn:
        print()
        print_status(conn)
        conn.close()

    print("\n[OK] 私募数据更新完成。")
    print("    提示：净值仅覆盖天天基金代销池（约 0.48%），非全市场私募业绩。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
