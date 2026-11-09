#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股股票基本信息（名录 + 静态属性 + 多值行业标签）
==================================================
写入 ``stocks`` / ``stock_industries`` 两张表。只做「基本信息」，历史日线在
``fetch_stock_prices.py``。两个脚本都可独立运行，DDL 都是幂等的。

数据源（**全部免费、无需登录 / 无需 API key**）
-----------------------------------------------
1) 全市场名录 + 行情快照：东方财富 clist（批量入口，60 页拿全市场，8 线程约 9 秒）
       GET https://push2.eastmoney.com/api/qt/clist/get
       fs = m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048
   ⚠ ``pz`` 被服务端**静默夹到 100**（传 10000 只返回 100 条，total=5927），
     所以必须翻页；实测 60 页 5927 行、5927 个唯一代码，无重叠。
   字段映射：   f12 代码 / f14 简称 / f2 最新价 / f3 涨跌幅 / f20 总市值(元) /
             f21 流通市值(元) / f26 上市日期 / f297 行情日期 / f13 市场(0=深/北,1=沪)
             f124 **源侧行情时刻**（Unix 秒，北京时间）→ ``stocks.quote_time``
   ⚠ **不用 f13 判交易所**：北交所也返回 0，会混进深市。一律按代码前缀分类。
   ⚠ ``f124`` 是**请求时刻的源侧行情时钟**，不是这笔价格的成交时刻：活跃股与抓取
     时刻只差几秒；**339/5562（6.1%）停牌 / 退市 / ST 股返回当日 08:00:00 占位**
     （000003 PT金田A、000004 国华退、000005 ST星源、000024 招商地产…），
     **并非**最后交易日收盘。⇒ 不能用它判断「最后成交日」，只能看源侧行情时钟有没有走动。
   ⚠ 它与 ``stocks.updated_at``（库内名录 UPSERT 写入时刻）是**两个口径**：
     前者回答「源侧行情走到几点」，后者回答「我们几点抓的」，不要混用。

2) 总股本 / 流通股本 / 细分行业 / 地域：东财 stock/get（逐只，1 请求/只）
       GET https://push2.eastmoney.com/api/qt/stock/get?secid=<1|0>.<code>
   f84 总股本(股) / f85 流通股(股) / f127 细分行业 / f128 地域板块 / f116 总市值
   ⚠ f84 与深交所官方名录的「A股总股本」逐股一致（000001 = 19,405,918,198）。
   ⚠ f128 有 "-" （境外注册公司，如 688981 中芯国际注册在开曼）→ 留 NULL，不猜。

3) 公司基本介绍：东方财富 F10 公司资料（逐只 GET，**纯 HTTP 无加密，可任意并发**）
       GET https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax?code=SH600519
   一次给 40+ 列，本项目用到：
       ORG_NAME 公司全称 / ORG_NAME_EN 英文名 / FORMERNAME 曾用简称
       ORG_PROFILE **公司简介（长文本）** / BUSINESS_SCOPE 经营范围
       INDUSTRYCSRC1 证监会行业(两级) / EM2016 东财行业(三级，拆成多值标签)
       PROVINCE **省份**（688981 这类境外注册给「境外」，比 f128 的 "-" 更明确）
       REG_CAPITAL 注册资金(万元) / REG_ADDRESS 注册地址 / ADDRESS 办公地址
       LEGAL_PERSON 法人 / ORG_WEB / ORG_EMAIL / ORG_TEL / ORG_FAX
       fxxg.FOUND_DATE 成立日期 / fxxg.LISTING_DATE 上市日期
   ⚠ 「注册资金」单位 **万元**，与 f84（股）不是一回事，单列 registered_capital。
   ⚠ code 参数要带市场前缀 SH/SZ；科创板用 SH688xxx、创业板用 SZ300xxx。

4) 多值行业 / 概念 / 地域标签：东财 slist（逐只，1 请求给该股全部板块）
       GET https://push2.eastmoney.com/api/qt/slist/get?spt=3&secid=<mkt>.<code>
   板块类别由三份目录反查（各 1~6 页）：
       m:90+t:1 → region（31 个地域板块）/ t:2 → industry（496）/ t:3 → concept（531）
   ⚠ 概念板块里混着「昨日连板」「融资融券」「HS300_」这类**交易特征/指数成分**，
     不是行业。kind 仍记 concept，但前端过滤下拉只用 industry + region。

5) 名称拼音首字母：东财搜索建议（**按代码逐只查**，1 请求/只）
       GET http://searchapi.eastmoney.com/api/suggest/get
           ?input=<6位代码>&type=14&count=10&token=<公开token>
   取 ``QuotationCodeTable.Data[]`` 里 ``Code == 代码`` 那条的 ``PinYin``：
       601872 → 招商轮船 / ZSLC，000625 → 长安汽车 / CAQC，600132 → 重庆啤酒 / CQPJ
   ⚠ **必须走 http://**：HTTPS 通道的 gzip 缓存会把响应串成 2023 年的 JSONP
     （``jQuery35108723733748578402_1693632913001({...passportWeb...})``）。
     requests 默认带 ``Accept-Encoding: gzip`` 必中招，而 curl 不带该头看着「正常」
     —— 实测同一 URL：requests/HTTPS 恒返回 JSONP，requests/HTTP 恒返回正确信封。
     响应头有 ``Vary: Accept-Encoding`` + ``Content-Encoding: gzip``，是缓存串包。
   ⚠ **不要加 ``classify=AStock``**：科创板在源侧 ``Classify='23'`` 而非 ``'AStock'``，
     加了会把 688/689 全部**静默漏掉**（120 只样本里 15 只 MISS 全是科创板）。
     正确做法 = 不加市场过滤，按返回里的 ``Code`` 精确匹配 + 名称闸门。
   ⚠ **名称闸门**：NFKC（全角Ａ→A）+ 去空白 + 去 '-' 后与库内 name 比对，
     「三 力 士」/「深振业Ａ」/「泰诺麦博-U」都能对上；对不上一律不写（宁缺勿错）。
   ⚠ 只存**简称首字母缩写**，不存全拼。特殊前缀以源侧为准：``*ST天茂 → STTM``
     （去星号）、``S佳通 → SJT``、``PT金田A → PTJTA``、``万科A → WKA``。
   ⚠ 多音字必须用源侧值，不能本地生成：重庆啤酒 = CQPJ、长安汽车 = CAQC，
     pypinyin 这类库会错成 ZQPJ / ZAQC（系统 Python 也没装，不引这个依赖）。

深度 / 覆盖 / 死路
------------------
* 范围 = 沪市主板(600/601/603/605) + 科创板(688/689) + 深市主板(000/001/002/003)
  + 创业板(300/301)。**北交所、B 股不收**（clist 的 fs 含北交所，这里按前缀剔掉）。
* 逐只端点实测 12 线程 40 只 4.9 秒、0 失败，无明显频控；仍带全局限速 + 2 次重试。
* **死路 1**：akshare 1.18.42 的 ``stock_individual_info_em`` 已随东财改版失效
  （``ValueError: Length mismatch: Expected axis has 3 elements``）；
  ``stock_individual_basic_info_xq`` 雪球接口 ``KeyError: 'data'``。
  两者都别再试，直接用上面的东财原始端点。
* **死路 2（2026-11-08 实测，别再踩）**：巨潮资讯 ``stock_profile_cninfo`` 单只可用，
  但它内部每次用 ``py_mini_racer.MiniRacer()`` 现算 Accept-Enckey，**V8 引擎在多线程下
  直接崩溃**（Windows 上打印一屏 mini_racer.dll 栈后进程被杀），8 线程 20 只的短探针
  侥幸不崩但全量必崩。曾经验证「mcode 主线程算一次、多线程共用 header」可行
  （20/20 成功），但既然东财 F10 一次给全字段且无加密，就没必要留这个依赖。
  ⇒ 巨潮整体弃用，改 F10。
* ``stock_zh_a_spot_em()`` 虽可用，但 akshare 内部按 100 行/页**串行**翻 59 页要 98 秒；
  直连 clist 并发 60 页只要 9 秒，所以不用它。
* 交易所官方名录可作交叉校验：``stock_info_sh_name_code("主板A股"/"科创板")`` 1700+618 行、
  ``stock_info_sz_name_code("A股列表")`` 2906 行（含 A股总股本 + 证监会行业）。
* **死路 3（2026-11-09 实测，别再踩）**：想靠 searchapi 的 ``count=1000`` 一次拉全市场
  拼音 —— ``count`` 被硬夹 100，且 ``pageindex`` / ``page`` / ``pn`` / ``start`` /
  ``pagesize`` 全部**无效**（返回首条不变，不报错），无法翻页；``classify=AStock``
  又会漏掉科创板。⇒ 按代码逐只查是唯一能全覆盖的入口，实测 10 线程 132 只 1.0 秒、
  0 失败、0 名称错配（含科创板与 -U/-W 后缀）。

增量语义
--------
* 默认（``refresh_all.py`` 接的就是这个）：拉全市场名录 UPSERT 动态列，然后**只对**
  ``profile_updated_at`` 或 ``boards_updated_at`` 为空、以及新上市的股票跑逐只端点。
  数据库里的这两列就是「已入库」进度，无需额外进度文件；失败不写列 → 下次自动补。
* ``--full``：忽略时间戳，全量重抓逐只详情；用 ``.workbuddy/stock_info_full_progress.json``
  断点续跑，**只有事务提交成功后才记进度**（失败绝不记）。
* 名录 UPSERT 只更新名称 / 快照 / 市值等动态列，**不碰静态属性列**；
  详情 UPDATE 一律 ``COALESCE(EXCLUDED, 原值)``，备源缺字段不会把已有值抹成 NULL。
* 拼音首字母由 ``backfill_pinyin()`` 单独补：只查 ``name_pinyin IS NULL`` 的行，
  **改名时名录 UPSERT 会把该列置回 NULL** 让它重算；按 CHUNK 分批提交，
  失败的行不写、保持 NULL → 下次自动重试，**没有进度文件、失败绝不记成进度**。
  日常增量因此只有新股 / 改名股会发请求，首次全量回填手动 ``--pinyin-only`` 单跑。

合规
----
东方财富公开行情接口与 F10 公司资料，均用于**内部研究**，不对外分发、
不用于商业转售。抓取间隔做了限速，不做高频轮询。

用法
----
    python fetch_stock_info.py                 # 增量（refresh_all 用这个）
    python fetch_stock_info.py --full          # 全量重抓详情，可断点续跑
    python fetch_stock_info.py --limit 50      # 只处理前 50 只（联调用）
    python fetch_stock_info.py --codes 600519,000001
    python fetch_stock_info.py --dry-run       # 只抓不写库
    python fetch_stock_info.py --pinyin-only   # 只补缺失的拼音首字母（可中断续跑）
    python fetch_stock_info.py --pinyin-only --pinyin-all   # 全量重算（校验/纠错用）
"""

import argparse
import json
import os
import re
import sys
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
BASE_DIR = Path(__file__).parent
PROGRESS_FILE = BASE_DIR / ".workbuddy" / "stock_info_full_progress.json"

UA = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "*/*",
}
UT = "bd1d9ddb04089700cf9c27f6f7426281"
CLIST = "https://push2.eastmoney.com/api/qt/clist/get"
SGET = "https://push2.eastmoney.com/api/qt/stock/get"
SLIST = "https://push2.eastmoney.com/api/qt/slist/get"
F10 = "https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax"
# ⚠ 必须 http://：HTTPS 的 gzip 缓存会串成 2023 年的 JSONP（见 docstring 源 5）
SUGGEST = "http://searchapi.eastmoney.com/api/suggest/get"
SUGGEST_TOKEN = "D43BF722C8E33BDC906FB84D85E326E8"
FS_A = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048"
BOARD_FS = {
    "region":   "m:90+t:1+f:!50",
    "industry": "m:90+t:2+f:!50",
    "concept":  "m:90+t:3+f:!50",
}
CLIST_FIELDS = "f12,f13,f14,f2,f3,f20,f21,f26,f297,f124"
GET_FIELDS = ("f57,f58,f84,f85,f116,f117,f127,f128")
SLIST_FIELDS = "f12,f13,f14,f3,f4,f128,f140,f141,f136,f152"

PAGE_SIZE = 100           # 东财硬上限，实测 pz=10000 也只返回 100
DEFAULT_WORKERS = 12
DEFAULT_QPS = 30
TIMEOUT = 20
CHUNK = 200               # 每 200 只提交一次事务

# ─────────────────────────────────────────────────────────────────────────────
# DDL（幂等；与 src/db/schema.ts 的 stocks / stock_industries / stock_prices 对齐）
# 本仓库无 migrations，DDL 内联在脚本里。CREATE/ALTER 全部 IF NOT EXISTS。
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

-- 股价脚本用的「已核对行情日」；本脚本的名录 UPSERT 不碰它
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS price_checked_date date;
-- 名称拼音首字母（东财 searchapi 权威值，非本地生成）；NULL = 未取到，下次自动补
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS name_pinyin varchar(32);
CREATE INDEX IF NOT EXISTS idx_stocks_name_pinyin ON stocks(name_pinyin);
-- 源侧行情时刻（东财 clist f124，北京时间）；与 updated_at 是两个口径，见 docstring
ALTER TABLE stocks ADD COLUMN IF NOT EXISTS quote_time timestamptz;
"""


# ─────────────────────────────────────────────────────────────────────────────
# 基础工具
# ─────────────────────────────────────────────────────────────────────────────
_tls = threading.local()


def session() -> requests.Session:
    if not hasattr(_tls, "s"):
        s = requests.Session()
        s.headers.update(UA)
        _tls.s = s
    return _tls.s


class RateLimiter:
    """全局发牌（不是每线程各自 sleep），限制总请求速率。"""

    def __init__(self, qps: float):
        self.gap = 1.0 / max(qps, 0.1)
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            at = max(now, self.next_at)
            self.next_at = at + self.gap
        delay = at - time.monotonic()
        if delay > 0:
            time.sleep(delay)


LIMITER = RateLimiter(DEFAULT_QPS)


def get_json(url: str, params: dict, retries: int = 2) -> dict:
    last = None
    for i in range(retries + 1):
        try:
            LIMITER.wait()
            r = session().get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            j = r.json()
            if not isinstance(j, dict):
                raise ValueError("响应不是 JSON 对象")
            return j
        except Exception as exc:          # noqa: BLE001 —— 每个错误分支都要可见
            last = exc
            if i < retries:
                time.sleep(0.6 * (2 ** i))
    raise RuntimeError(f"{type(last).__name__}: {last}")


def norm_name(s) -> str:
    """「万  科Ａ」→「万科A」：NFKC 折叠全角 + 去空白 + 大写，只用于检索键。"""
    if not s:
        return ""
    return "".join(c for c in unicodedata.normalize("NFKC", str(s))
                   if not c.isspace()).upper()


# ─────────────────────────────────────────────────────────────────────────────
# 源 5：名称拼音首字母（东财搜索建议，按代码逐只查）
# ─────────────────────────────────────────────────────────────────────────────
def name_key(s) -> str:
    """名称闸门比较键：NFKC + 去空白 + 去 '-' + 大写。
    源侧对 -U/-W 后缀写法偶有差异，比较时统一去掉；不改库里的展示名。
    """
    return norm_name(s).replace("-", "")


def suggest_pinyin(code: str, name: str) -> tuple[str | None, str]:
    """返回 ``(PinYin, 原因)``。查不到 / 信封不对 / 名称对不上 / 值不合法 → (None, 原因)。

    失败一律**留 NULL 且不记进度**，下次跑自动重试；原因字符串会进日志，不静默吞。
    """
    try:
        j = get_json(SUGGEST, {
            "input": code, "type": 14, "count": 10,
            "token": SUGGEST_TOKEN, "_": int(time.time() * 1000),
        })
    except Exception as exc:                     # noqa: BLE001
        return None, f"request:{type(exc).__name__}:{str(exc)[:60]}"
    table = j.get("QuotationCodeTable")
    if not isinstance(table, dict):
        # HTTPS gzip 缓存串成 JSONP 时 r.json() 就已经抛异常；这里是兜底，
        # 防止「响应 200 但信封换了」被当成 0 条结果记成源侧无数据。
        return None, "bad-envelope(响应不是 QuotationCodeTable)"
    data = table.get("Data") or []
    hit = next((x for x in data if str(x.get("Code") or "").strip() == code), None)
    if not hit:
        return None, "no-code-match(源侧未收录该代码)"
    if name_key(hit.get("Name")) != name_key(name):
        return None, f"name-gate:{hit.get('Name')!r}!={name!r}(拒绝错配)"
    py = str(hit.get("PinYin") or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{1,32}", py):
        return None, f"bad-pinyin:{py!r}"
    return py, "ok"


def parse_date(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in ("-", "--", "0", "1900-01-01"):
        return None
    m = re.match(r"^(\d{4})[-/]?(\d{2})[-/]?(\d{2})", s)
    if not m:
        return None
    try:
        return date(int(m[1]), int(m[2]), int(m[3]))
    except ValueError:
        return None


def num(v):
    if v is None or v == "-" or v == "":
        return None
    if isinstance(v, str):
        v = v.replace(",", "").strip()
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f            # NaN → None


CN_TZ = timezone(timedelta(hours=8))


def parse_quote_time(v):
    """东财 ``f124`` → aware datetime（UTC+8）。0 / 缺失 / 异常时间戳一律 NULL。

    ⚠ 挡住「毫秒被当成秒」：``f124`` 实测是 10 位秒级（1794204707），
      上限 4102444800（2100-01-01）之外的直接丢弃，避免写入荒谬日期。
    """
    ts = num(v)
    if not ts or ts <= 0 or ts > 4102444800:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=CN_TZ)
    except (OverflowError, OSError, ValueError):
        return None


def classify(code: str):
    """代码前缀 → (交易所, 板块, secid 市场位)。不在范围内的返回 None。"""
    if code.startswith(("600", "601", "603", "605")):
        return "上交所", "沪市主板", "1"
    if code.startswith(("688", "689")):
        return "上交所", "科创板", "1"
    if code.startswith(("000", "001", "002", "003")):
        return "深交所", "深市主板", "0"
    if code.startswith(("300", "301")):
        return "深交所", "创业板", "0"
    return None


def clean_region(v):
    if not v or v in ("-", "--"):
        return None
    s = str(v).strip()
    return s[:-2] if s.endswith("板块") else s


# ─────────────────────────────────────────────────────────────────────────────
# 源 1：全市场名录（批量分页）
# ─────────────────────────────────────────────────────────────────────────────
def clist_page(fs: str, fields: str, pn: int, pz: int = PAGE_SIZE) -> dict:
    return get_json(CLIST, {
        "pn": pn, "pz": pz, "po": 1, "np": 1, "ut": UT, "fltt": 2, "invt": 2,
        "fid": "f12", "fs": fs, "fields": fields, "_": int(time.time() * 1000),
    })


def _diff(j: dict) -> list:
    """东财 diff 有时是 list、有时是 {序号: obj} 字典，统一成 list。"""
    d = (j.get("data") or {}).get("diff") or []
    return list(d.values()) if isinstance(d, dict) else list(d)


def clist_all(fs: str, fields: str, workers: int) -> list[dict]:
    first = clist_page(fs, fields, 1)
    data = first.get("data") or {}
    total = int(data.get("total") or 0)
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    if pages <= 1:
        return _diff(first)

    def one(pn):
        return _diff(clist_page(fs, fields, pn))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        chunks = list(ex.map(one, range(2, pages + 1)))
    rows = _diff(first)
    for c in chunks:
        rows.extend(c)
    # 批内去重（东财分页偶发漂移，ON CONFLICT 同条命令命中两次会整批回滚）
    seen, out = set(), []
    for r in rows:
        k = r.get("f12")
        if k and k not in seen:
            seen.add(k)
            out.append(r)
    return out


def load_roster(workers: int) -> list[dict]:
    raw = clist_all(FS_A, CLIST_FIELDS, workers)
    rows = []
    for r in raw:
        code = str(r.get("f12") or "").strip()
        cls = classify(code)
        if not cls:
            continue                       # 北交所 / B 股 / 其他
        exchange, board, _ = cls
        rows.append({
            "code": code,
            "name": str(r.get("f14") or "").strip(),
            "exchange": exchange,
            "board": board,
            "latest_price": num(r.get("f2")),
            "change_pct": num(r.get("f3")),
            "market_cap": num(r.get("f20")),
            "float_market_cap": num(r.get("f21")),
            "listing_date": parse_date(r.get("f26")),
            "quote_date": parse_date(r.get("f297")),
            "quote_time": parse_quote_time(r.get("f124")),
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# 源 4：板块目录（给 slist 的 BK 代码分类）
# ─────────────────────────────────────────────────────────────────────────────
def load_board_catalog(workers: int) -> dict[str, tuple[str, str]]:
    catalog = {}
    for kind, fs in BOARD_FS.items():
        for r in clist_all(fs, "f12,f14", workers):
            bk = str(r.get("f12") or "").strip()
            if bk:
                catalog[bk] = (kind, str(r.get("f14") or "").strip())
    return catalog


# ─────────────────────────────────────────────────────────────────────────────
# 源 2/3/4：逐只详情
# ─────────────────────────────────────────────────────────────────────────────
def stock_get(code: str, mkt: str) -> dict:
    j = get_json(SGET, {
        "secid": f"{mkt}.{code}", "ut": UT, "fltt": 2, "invt": 2,
        "fields": GET_FIELDS, "_": int(time.time() * 1000),
    })
    return j.get("data") or {}


def stock_boards(code: str, mkt: str) -> list[dict]:
    j = get_json(SLIST, {
        "spt": 3, "fltt": 2, "invt": 2, "fields": SLIST_FIELDS,
        "secid": f"{mkt}.{code}", "pn": 1, "pz": 100, "po": 1, "np": 1,
        "ut": UT, "_": int(time.time() * 1000),
    })
    return _diff(j)


def f10_profile(code: str, exchange: str) -> dict:
    """东财 F10 公司资料。jbzl（基本资料）+ fxxg（发行相关）合并成一个扁平 dict。

    纯 GET、无加密、无需登录 → 可安全多线程并发（这是弃用巨潮的直接原因）。
    ``code`` 要带 SH/SZ 前缀；源侧对个别股票返回空 jbzl 时返回 {}，算「源侧明确无资料」。
    """
    secid = ("SH" if exchange == "上交所" else "SZ") + code
    last = None
    for i in range(3):
        try:
            LIMITER.wait()
            r = session().get(
                F10, params={"code": secid},
                headers={"Referer": "https://emweb.securities.eastmoney.com/"},
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            j = r.json()
            out = {}
            for key in ("jbzl", "fxxg"):
                arr = j.get(key) or []
                if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                    out.update(arr[0])
            return out
        except Exception as exc:                 # noqa: BLE001
            last = exc
            if i < 2:
                time.sleep(0.6 * (2 ** i))
    raise RuntimeError(f"{type(last).__name__}: {last}")


def fetch_one(item: dict, catalog: dict, args) -> dict:
    """单只股票的 3 个端点；每个端点独立重试，一个失败不影响另两个。"""
    code, mkt = item["code"], item["mkt"]
    out = {"code": code, "get": None, "profile": None, "boards": None, "errors": []}

    if not args.skip_profile:
        try:
            out["profile"] = f10_profile(code, item["exchange"])
        except Exception as exc:                 # noqa: BLE001
            out["errors"].append(f"profile:{type(exc).__name__}:{str(exc)[:60]}")
    try:
        out["get"] = stock_get(code, mkt)
    except Exception as exc:                     # noqa: BLE001
        out["errors"].append(f"get:{type(exc).__name__}:{str(exc)[:60]}")
    if not args.skip_boards:
        try:
            diffs = stock_boards(code, mkt)
            tags = []
            for d in diffs:
                bk = str(d.get("f12") or "").strip()
                nm = str(d.get("f14") or "").strip()
                if not bk or not nm:
                    continue
                kind = catalog.get(bk, ("unknown", nm))[0]
                tags.append((code, kind, nm, bk))
            out["boards"] = tags
        except Exception as exc:                 # noqa: BLE001
            out["errors"].append(f"boards:{type(exc).__name__}:{str(exc)[:60]}")
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 写库
# ─────────────────────────────────────────────────────────────────────────────
UPSERT_ROSTER = """
INSERT INTO stocks (code, name, name_search, exchange, board,
                    latest_price, change_pct, market_cap, float_market_cap,
                    listing_date, quote_date, quote_time, source)
VALUES %s
ON CONFLICT (code) DO UPDATE SET
    name              = EXCLUDED.name,
    name_search       = EXCLUDED.name_search,
    -- 改简称后旧拼音作废：置 NULL 让 backfill_pinyin 重算（失败留 NULL 下次再补）
    name_pinyin       = CASE WHEN stocks.name IS DISTINCT FROM EXCLUDED.name
                             THEN NULL ELSE stocks.name_pinyin END,
    exchange          = EXCLUDED.exchange,
    board             = EXCLUDED.board,
    latest_price      = EXCLUDED.latest_price,
    change_pct        = EXCLUDED.change_pct,
    market_cap        = EXCLUDED.market_cap,
    float_market_cap  = EXCLUDED.float_market_cap,
    listing_date      = COALESCE(stocks.listing_date, EXCLUDED.listing_date),
    quote_date        = EXCLUDED.quote_date,
    -- 源侧偶尔不给 f124：保留上一次的行情时刻，别把已有值抹成 NULL（项目铁律）
    quote_time        = COALESCE(EXCLUDED.quote_time, stocks.quote_time),
    source            = EXCLUDED.source,
    updated_at        = NOW()
"""

# 静态属性一律 COALESCE：备源缺字段不会把已有值抹成 NULL（本项目铁律）
# ⚠ VALUES 派生表必须显式转型：整列都是 NULL 时 PG 会把该列推断成 text，
#   然后 COALESCE(text, numeric/date) 直接报错。外面再套一层带 :: 的 SELECT 才稳。
UPDATE_PROFILE = """
UPDATE stocks s SET
    company_name       = COALESCE(v.company_name, s.company_name),
    name_en            = COALESCE(v.name_en, s.name_en),
    former_name        = COALESCE(v.former_name, s.former_name),
    introduction       = COALESCE(v.introduction, s.introduction),
    main_business      = COALESCE(v.main_business, s.main_business),
    business_scope     = COALESCE(v.business_scope, s.business_scope),
    registered_capital = COALESCE(v.registered_capital, s.registered_capital),
    industry_csrc      = COALESCE(v.industry_csrc, s.industry_csrc),
    founded_date       = COALESCE(v.founded_date, s.founded_date),
    listing_date       = COALESCE(v.listing_date, s.listing_date),
    legal_person       = COALESCE(v.legal_person, s.legal_person),
    registered_address = COALESCE(v.registered_address, s.registered_address),
    office_address     = COALESCE(v.office_address, s.office_address),
    website            = COALESCE(v.website, s.website),
    email              = COALESCE(v.email, s.email),
    phone              = COALESCE(v.phone, s.phone),
    profile_updated_at = NOW()
FROM (
    SELECT t.code::varchar(10) AS code,
           t.company_name::text AS company_name,
           t.name_en::text AS name_en,
           t.former_name::text AS former_name,
           t.introduction::text AS introduction,
           t.main_business::text AS main_business,
           t.business_scope::text AS business_scope,
           t.registered_capital::numeric(20,4) AS registered_capital,
           t.industry_csrc::varchar(120) AS industry_csrc,
           t.founded_date::date AS founded_date,
           t.listing_date::date AS listing_date,
           t.legal_person::varchar(100) AS legal_person,
           t.registered_address::text AS registered_address,
           t.office_address::text AS office_address,
           t.website::text AS website,
           t.email::text AS email,
           t.phone::text AS phone
    FROM (VALUES %s) AS t(code, company_name, name_en, former_name, introduction,
                          main_business, business_scope, registered_capital,
                          industry_csrc, founded_date, listing_date, legal_person,
                          registered_address, office_address, website, email, phone)
) v
WHERE s.code = v.code
"""

# ⚠ boards_updated_at **不能**在这里写：stock/get 成功但 slist 失败时，写时间戳会让
#   该股票的标签永久不再被增量挑中（失败被记成进度）。时间戳由 MARK_BOARDS 单独写。
UPDATE_GET = """
UPDATE stocks s SET
    total_shares = COALESCE(v.total_shares, s.total_shares),
    float_shares = COALESCE(v.float_shares, s.float_shares),
    industry     = COALESCE(v.industry, s.industry),
    region       = COALESCE(v.region, s.region)
FROM (
    SELECT t.code::varchar(10) AS code,
           t.total_shares::numeric(24,2) AS total_shares,
           t.float_shares::numeric(24,2) AS float_shares,
           t.industry::varchar(120) AS industry,
           t.region::varchar(40) AS region
    FROM (VALUES %s) AS t(code, total_shares, float_shares, industry, region)
) v
WHERE s.code = v.code
"""

MARK_BOARDS = "UPDATE stocks SET boards_updated_at = NOW() WHERE code = ANY(%s)"

INSERT_TAGS = """
INSERT INTO stock_industries (code, kind, name, board_code) VALUES %s
ON CONFLICT (code, kind, name) DO NOTHING
"""


def txt(v):
    if v is None or v != v:                 # NaN != NaN
        return None
    s = str(v).strip()
    return s if s and s not in ("-", "--", "nan", "None", "NaT") else None


def write_roster(cur, rows):
    vals = [(
        r["code"], r["name"], norm_name(r["name"]), r["exchange"], r["board"],
        r["latest_price"], r["change_pct"], r["market_cap"], r["float_market_cap"],
        r["listing_date"], r["quote_date"], r["quote_time"], "eastmoney_clist",
    ) for r in rows]
    execute_values(cur, UPSERT_ROSTER, vals, page_size=500)
    return len(vals)


def write_details(cur, results, args):
    prof, gets = [], []
    slist_tags, f10_tags, rebuild, board_done = [], [], set(), set()
    region_by_code = {}

    for r in results:
        code = r["code"]
        p = r["profile"]
        if p is not None and not args.skip_profile:
            # F10 的 PROVINCE 比东财地域板块 f128 覆盖更全（境外注册也能给「境外」）
            region_by_code[code] = txt(p.get("PROVINCE"))
            csrc = txt(p.get("INDUSTRYCSRC1"))
            prof.append((
                code, txt(p.get("ORG_NAME")), txt(p.get("ORG_NAME_EN")),
                txt(p.get("FORMERNAME")), txt(p.get("ORG_PROFILE")),
                None,                                   # main_business：F10 不给，宁缺勿错
                txt(p.get("BUSINESS_SCOPE")), num(p.get("REG_CAPITAL")),
                csrc.split("-")[-1].strip() if csrc else None,
                parse_date(p.get("FOUND_DATE")), parse_date(p.get("LISTING_DATE")),
                txt(p.get("LEGAL_PERSON")), txt(p.get("REG_ADDRESS")),
                txt(p.get("ADDRESS")), txt(p.get("ORG_WEB")),
                txt(p.get("ORG_EMAIL")), txt(p.get("ORG_TEL")),
            ))
            # 「所属行业可以多个」的干净来源：EM2016 三级 + INDUSTRYCSRC1 两级，
            # 按 '-' 拆成多个 industry 标签，和 slist 的东财板块合并进同一张表。
            for key in ("EM2016", "INDUSTRYCSRC1"):
                path = txt(p.get(key))
                if path:
                    for seg in path.split("-"):
                        seg = seg.strip()
                        if seg:
                            f10_tags.append((code, "industry", seg, None))
        g = r["get"]
        if g is not None:
            gets.append((
                code, num(g.get("f84")), num(g.get("f85")),
                txt(g.get("f127")),
                region_by_code.get(code) or clean_region(g.get("f128")),
            ))
        if r["boards"] is not None and not args.skip_boards:
            slist_tags.extend(r["boards"])
            rebuild.add(code)            # 空列表也是「成功抓到 0 个板块」，照样覆盖
            if g is not None:
                board_done.add(code)

    if prof:
        execute_values(cur, UPDATE_PROFILE, prof, page_size=200)
    if gets:
        execute_values(cur, UPDATE_GET, gets, page_size=200)

    # 只对 slist 成功的股票整只覆盖标签；F10 标签用 ON CONFLICT DO NOTHING 追加。
    # ⚠ slist 失败时**绝不删旧标签** —— 失败不能抹掉已有数据。
    if rebuild:
        cur.execute("DELETE FROM stock_industries WHERE code = ANY(%s)", (sorted(rebuild),))
    all_tags = [t for t in slist_tags if t[0] in rebuild] + f10_tags
    if all_tags:
        dedup = {}
        for t in all_tags:
            dedup[(t[0], t[1], t[2])] = t
        execute_values(cur, INSERT_TAGS, list(dedup.values()), page_size=500)
    # 时间戳与写库在**同一个事务**里，提交成功才算「已入库」
    if board_done:
        cur.execute(MARK_BOARDS, (sorted(board_done),))
    return {"profile": len(prof), "get": len(gets), "tags": len(all_tags)}


def select_pending(conn, all_codes, args) -> list[str]:
    """挑待抓代码。增量靠 DB 里的两个时间戳（成功入库才写 → 天然可续跑）。"""
    if args.full:
        done = set()
        if PROGRESS_FILE.exists():
            try:
                done = set(json.loads(PROGRESS_FILE.read_text(encoding="utf-8")))
            except Exception:                    # noqa: BLE001
                print("[WARN] 进度文件损坏，从头重跑（不会丢数据，只是重复抓）")
        return [c for c in all_codes if c not in done]
    if args.codes:
        wanted = set(args.codes)
        return [c for c in all_codes if c in wanted]
    conds = ["boards_updated_at IS NULL"]        # stock/get + slist 共用一个时间戳
    if not args.skip_profile:
        conds.append("profile_updated_at IS NULL")
    with conn.cursor() as cur:
        cur.execute(f"SELECT code FROM stocks WHERE {' OR '.join(conds)} ORDER BY code")
        return [r[0] for r in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# 拼音回填（独立于详情抓取；DB 里的 NULL 本身就是进度，无进度文件）
# ─────────────────────────────────────────────────────────────────────────────
PINYIN_UPSERT = """
UPDATE stocks s SET name_pinyin = v.py
FROM (
    SELECT t.code::varchar(10) AS code, t.py::varchar(32) AS py
    FROM (VALUES %s) AS t(code, py)
) v
WHERE s.code = v.code
"""


def select_pinyin_pending(conn, all_rows: bool = False) -> list[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT code, name FROM stocks "
                    + ("" if all_rows else "WHERE name_pinyin IS NULL ")
                    + "ORDER BY code")
        return [(r[0], r[1]) for r in cur.fetchall()]


def backfill_pinyin(conn, args, t0: float) -> dict:
    """逐只补拼音首字母。每 CHUNK 提交一次；失败行保持 NULL，下次自动重试。"""
    items = select_pinyin_pending(conn, args.pinyin_all)
    mode = "全量重算" if args.pinyin_all else "仅补缺失"
    print(f"      待处理 {len(items)} 只（{mode}；workers={args.workers}）", flush=True)
    stats: dict = {"ok": 0, "fail": 0, "reasons": {}}
    if args.dry_run:
        print(f"[dry-run] 将处理 {len(items)} 只，未发请求、未写库")
        return stats
    for start in range(0, len(items), CHUNK):
        chunk = items[start:start + CHUNK]
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            results = list(ex.map(lambda it: suggest_pinyin(it[0], it[1]), chunk))
        vals, fails = [], []
        for (code, _), (py, why) in zip(chunk, results):
            if py:
                vals.append((code, py))
            else:
                fails.append((code, why))
                kind = why.split(":", 1)[0].split("(", 1)[0]
                stats["reasons"][kind] = stats["reasons"].get(kind, 0) + 1
        if vals:
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, PINYIN_UPSERT, vals, page_size=200)
        stats["ok"] += len(vals)
        stats["fail"] += len(fails)
        print(f"      [{stats['ok'] + stats['fail']}/{len(items)}] 入库 {len(vals)} / "
              f"失败 {len(fails)}，累计耗时 {time.time()-t0:.1f}s", flush=True)
        for c, w in fails[:5]:
            print(f"      [WARN] {c} 拼音未入库: {w}", flush=True)
        if len(fails) > 5:
            print(f"      [WARN] …另有 {len(fails)-5} 只，均留 NULL 下次自动补", flush=True)
    if stats["reasons"]:
        print(f"      失败原因分布：{stats['reasons']}", flush=True)
    return stats


def pinyin_summary(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS total, COUNT(name_pinyin) AS with_py FROM stocks")
        total, with_py = cur.fetchone()
    return total, with_py


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="A 股股票基本信息（名录 / 简介 / 股本 / 地域 / 行业）")
    ap.add_argument("--full", action="store_true", help="全量重抓逐只详情（可断点续跑）")
    ap.add_argument("--dry-run", action="store_true", help="只抓不写库")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 只（联调用）")
    ap.add_argument("--codes", default="", help="逗号分隔的指定代码")
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--qps", type=float, default=DEFAULT_QPS)
    ap.add_argument("--skip-profile", action="store_true", help="跳过 F10 公司简介（联调用）")
    ap.add_argument("--skip-boards", action="store_true", help="跳过东财多值板块标签（联调用）")
    ap.add_argument("--pinyin-only", action="store_true",
                    help="不抓名录/详情，只补名称拼音首字母（可中断续跑）")
    ap.add_argument("--pinyin-all", action="store_true",
                    help="配合 --pinyin-only：全量重算拼音，而非只补 NULL")
    args = ap.parse_args()

    global LIMITER
    LIMITER = RateLimiter(args.qps)
    args.codes = [c.strip() for c in args.codes.split(",") if c.strip()]

    t0 = time.time()

    # 拼音首次回填 / 校验重算：单独跑，不碰名录与详情，失败可随时重来
    if args.pinyin_only:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(SQL_ENSURE)
            print("[1/2] DDL 就绪；开始补名称拼音首字母", flush=True)
            stats = backfill_pinyin(conn, args, t0)
            total, with_py = pinyin_summary(conn)
            print(f"[2/2] 库内：股票 {total} / 有拼音 {with_py} / 缺 {total - with_py}；"
                  f"本次写入 {stats['ok']} / 失败 {stats['fail']}"
                  f"{'（dry-run 未写库）' if args.dry_run else ''}")
            print(f"完成，总耗时 {time.time()-t0:.1f}s")
        finally:
            conn.close()
        return 0

    print("[1/5] 拉取全市场名录（东财 clist，并发翻页）…", flush=True)
    rows = load_roster(args.workers)
    print(f"      全市场 {len(rows)} 只（已剔除北交所/B股），耗时 {time.time()-t0:.1f}s", flush=True)
    if not rows:
        print("[FATAL] 名录为空，疑似源侧异常，直接退出（不写库）", flush=True)
        return 1

    if args.limit:
        rows = rows[:args.limit]
    by_code = {r["code"]: r for r in rows}
    all_codes = sorted(by_code)

    if args.dry_run:
        print("[dry-run] 名录样例：")
        for r in rows[:5]:
            print("   ", r)
        print(f"[dry-run] 共 {len(rows)} 行，未写库")
        return 0

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SQL_ENSURE)
        print("[2/5] DDL 就绪", flush=True)

        with conn:
            with conn.cursor() as cur:
                n = write_roster(cur, rows)
        print(f"      名录 UPSERT {n} 行", flush=True)

        print("[3/5] 补名称拼音首字母（东财搜索建议，逐只按代码查）…", flush=True)
        backfill_pinyin(conn, args, t0)

        pending = select_pending(conn, all_codes, args)
        print(f"[4/5] 待抓详情 {len(pending)} 只"
              f"（{'全量' if args.full else '增量'}；workers={args.workers}）", flush=True)

        catalog = {}
        if not args.skip_boards:
            catalog = load_board_catalog(args.workers)
            print(f"      板块目录 {len(catalog)} 个 "
                  f"({sum(1 for v in catalog.values() if v[0]=='region')} 地域 / "
                  f"{sum(1 for v in catalog.values() if v[0]=='industry')} 行业 / "
                  f"{sum(1 for v in catalog.values() if v[0]=='concept')} 概念)", flush=True)

        items = [{
            "code": c,
            "exchange": by_code[c]["exchange"],
            "mkt": "1" if by_code[c]["exchange"] == "上交所" else "0",
        } for c in pending]
        stats = {"profile": 0, "get": 0, "tags": 0, "errors": 0, "done": 0}
        done_progress = set()
        if args.full and PROGRESS_FILE.exists():
            try:
                done_progress = set(json.loads(PROGRESS_FILE.read_text(encoding="utf-8")))
            except Exception:                    # noqa: BLE001
                pass

        for start in range(0, len(items), CHUNK):
            chunk = items[start:start + CHUNK]
            with ThreadPoolExecutor(max_workers=args.workers) as ex:
                results = list(ex.map(lambda it: fetch_one(it, catalog, args), chunk))
            # 只把「所有启用端点都成功」的股票计入 done；失败的下次继续补
            ok_results = []
            for r in results:
                need = [r["get"] is not None]
                if not args.skip_profile:
                    need.append(r["profile"] is not None)
                if not args.skip_boards:
                    need.append(r["boards"] is not None)
                if all(need):
                    ok_results.append(r)
                else:
                    stats["errors"] += 1
                    print(f"      [WARN] {r['code']} 未完成: {'; '.join(r['errors']) or '端点空响应'}",
                          flush=True)
            with conn:
                with conn.cursor() as cur:
                    s = write_details(cur, ok_results, args)
                    # 单独端点成功、但整体未完成的，也把成功的部分写进去（COALESCE 安全）
                    partial = [r for r in results if r not in ok_results]
                    if partial:
                        s2 = write_details(cur, partial, args)
                        for k in s:
                            s[k] += s2[k]
            for k in ("profile", "get", "tags"):
                stats[k] += s[k]
            stats["done"] += len(ok_results)

            if args.full:
                done_progress.update(r["code"] for r in ok_results)
                PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
                PROGRESS_FILE.write_text(json.dumps(sorted(done_progress), ensure_ascii=False),
                                         encoding="utf-8")
            print(f"      [{stats['done']}/{len(items)}] 本批 {len(chunk)} 只，"
                  f"简介 {s['profile']} / 股本 {s['get']} / 标签 {s['tags']}，"
                  f"累计耗时 {time.time()-t0:.1f}s", flush=True)

        # 收尾核对：把「写了简介但没写时间戳」之类的异常显式暴露
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total,
                       COUNT(profile_updated_at) AS with_profile,
                       COUNT(boards_updated_at)  AS with_boards,
                       COUNT(total_shares)       AS with_shares,
                       COUNT(introduction)       AS with_intro,
                       COUNT(region)             AS with_region
                FROM stocks
            """)
            t = cur.fetchone()
        total, with_py = pinyin_summary(conn)
        print(f"[5/5] 库内：股票 {t[0]} / 有简介 {t[4]} / 有总股本 {t[3]} / "
              f"有地域 {t[5]} / 有标签时间戳 {t[2]} / 有拼音 {with_py}/{total}")
        print(f"      失败端点 {stats['errors']} 只（不计进度，下次自动补）")
        print(f"完成，总耗时 {time.time()-t0:.1f}s")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
