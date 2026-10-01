#!/usr/bin/env python3
"""
人民币汇率 Fetcher（外汇）
==========================
两个**口径**、两个来源，分开存、**绝不混拼**（与 private_fund_nav 的 source 分列同一原则）：

  ① 人民币汇率中间价   rate_type='mid'   source='cfets'
     来源：中国外汇交易中心（受权发布）/ 中国货币网 chinamoney.com.cn —— 官方定价
     当日：GET /r/cms/www/chinamoney/data/fx/ccpr.json
     历史：GET /ags/ms/cm-u-bk-ccpr/CcprHisNew
     节奏：每个**工作日** 约 9:15 发布；周末与法定节假日无数据（不是抓取失败）

  ② 人民币即期汇率（市场价）  rate_type='spot'  source='sina'
     历史日线：vip.stock.finance.sina.com.cn/forex/api/jsonp.php/…/NewForexService.getDayKLine
     实时快照：hq.sinajs.cn/list=fx_s{ccy}cny   → 存 fx_spot 快照表，**不写进日线序列**

统一数值口径（唯一计算口径）
----------------------------
`fx_rates.close` = **1 单位外币 兑 人民币**（CNY per 1 unit of foreign currency）。

    USD/CNY   6.7351        → 6.7351
    100JPY/CNY 4.2727       → 0.042727   （÷100）
    CNY/KRW   201.70        → 0.0049579  （取倒数）

⚠ 为什么统一：CFETS 官方对 10 个货币报「外币/人民币」、对另 15 个报「人民币/外币」。
  原样入库会让同一页出现两种方向（USD/CNY 上行=人民币贬值，CNY/KRW 上行=人民币升值），
  涨跌颜色语义互相矛盾。统一成「1 外币 = X 人民币」后：
      数值上行 = 人民币贬值（红）／数值下行 = 人民币升值（绿）
  前端 `quote_unit`（日元=100，其余=1）只影响**展示倍数**，不影响语义。
  换算关系可在详情页与 CFETS 官网逐条核对。

覆盖
----
中间价 25 对（cfets）：
  外币/人民币 10 对：USD EUR 100JPY HKD GBP AUD NZD SGD CHF CAD
  人民币/外币 15 对：MOP MYR RUB ZAR KRW AED SAR HUF PLN DKK SEK NOK TRY MXN THB
即期 19 对（sina）：上面 25 对里缺 KRW SAR HUF PLN TRY MXN；
  另有 TWD 有数据但 CFETS 未发布对应货币对 → **不纳入**（宁缺勿错）。

实测边界（2026-10-01 全部亲测，踩过才知道）
------------------------------------------
CFETS CcprHisNew：
  * **不带 `currency` 参数 = 一次返回全部 25 对**（`data.head` 给代码顺序，`records[].values` 按序对齐）
    → 25 倍请求量直接降到 1。这是本脚本能全历史回补的前提。
  * `pageSize` 上限 **50**；写 100 会返回 **HTTP 403 + HTML**（是 WAF 拦的，不是参数报错，
    别当成「接口挂了」）
  * 单次区间**不能超过 1 年**：2025-10-02~2026-10-01 正常，
    2025-10-01~2026-10-01 就 `data=''`、`total=None`（**静默返空，不报错**）→ 回补按 360 天分块
  * 官网可查历史**起点 ≈ 2006-01**：1994 / 2004 / 2005 全年均 `total=0`。默认回补起点 2006-01-01。

新浪 getDayKLine：
  * 返回 `var t=("date,open,low,high,close,|…")`
    —— **第 2 列是 low、第 3 列是 high**。用 `low <= open,close <= high` 的不等式验过；
      按 O/H/L/C 读会出现 `high < open` 的非法 K 线（EURCNY 2008-09-04 → 9.9190/9.7354/9.9436/9.7463）
  * 历史深度不均：USDCNY 1994-08 起（8028 根）、EURCNY 2008-09 起（4688 根），
    其余多数**从 2023-07/08 起且被截断在 1000 根**；`fx_skrcny` 直接 `msg: data is empty`

新浪 hq.sinajs（实时）：
  * **必须带 `Referer: https://finance.sina.com.cn/`**，否则 403；响应是 **GBK** 不是 UTF-8
  * 字段布局（两次采样差分确认，`[11] 涨跌额 == [8] 最新价 - [3] 昨收` 精确吻合）：
        [0]时间 [1]/[8]最新价 [2]报价 [3]昨收 [4]成交量 [5]今开 [6]最高 [7]最低
        [9]名称 [10]涨跌幅% [11]涨跌额 [12]? [13]备注 [14]52周高 [15]52周低 [17]日期
  * ⚠ `fx_susdcny` 的名字是「**在岸人民币**」而不是「美元兑人民币即期汇率」，
    且**休市时停更**（国庆实测：时间戳停在 `03:00:00`、涨跌 0、成交量 51）。
    这是事实而非故障 —— 所以实时值**只存快照表并带上报价时间**，不写日线序列。

合规
----
两个源都是公开行情页面。CFETS 中间价为官方公开数据；新浪行情仅供研究参考，
转载/商用前请自行确认授权。

用法
----
    python fetch_fx.py                 # 增量：中间价近 30 天 + 即期日线 + 即期实时快照
    python fetch_fx.py --full          # 全历史回补（中间价 2006 起分块 + 即期全量）
    python fetch_fx.py --days 90       # 指定中间价回看天数
    python fetch_fx.py --dry-run       # 只抓不写库
    python fetch_fx.py --status        # 只打印库内状态
"""

import argparse
import os
import re
import sys
from datetime import date, datetime, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

TIMEOUT = 30

CFETS_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Referer": "https://www.chinamoney.com.cn/",
}
SINA_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    # ⚠ 少这个 Referer 就是 403
    "Referer": "https://finance.sina.com.cn/",
}

CFETS_TODAY_URL = "https://www.chinamoney.com.cn/r/cms/www/chinamoney/data/fx/ccpr.json"
CFETS_HIST_URL  = "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcprHisNew"
SINA_DAY_URL    = ("https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/"
                   "var%20t=/NewForexService.getDayKLine?symbol={symbol}")
SINA_RT_URL     = "https://hq.sinajs.cn/list={symbols}"

# 单次查询上限：>1 年静默返空（见 docstring）→ 留足余量
CHUNK_DAYS = 360
PAGE_SIZE  = 50          # >50 触发 403
EARLIEST_MID = "2006-01-01"

RATE_TYPE_MID  = "mid"
RATE_TYPE_SPOT = "spot"

# ---------------------------------------------------------------------------
# 货币对定义
#
#   key        主键 / 前端路由参数
#   base       外币代码 + 中文名
#   quote_unit 展示倍数（日元按官方惯例用 100，其余 1）
#   inverse    CFETS 是否以「人民币/外币」方向发布（True 时入库要取倒数）
#   category   '主要货币' | '其他货币'（前端分组）
#   cfets      CFETS 的货币对代码；None = 该对没有中间价
#   sina       新浪即期代码；None = 该对没有即期
# ---------------------------------------------------------------------------
PAIRS = [
    # ── 主要货币：CFETS 以「外币/人民币」发布 ───────────────────────────────
    dict(key="usd_cny", base="USD", name="美元",         unit=1,   inverse=False, cat="主要货币", cfets="USD/CNY",    sina="fx_susdcny"),
    dict(key="eur_cny", base="EUR", name="欧元",         unit=1,   inverse=False, cat="主要货币", cfets="EUR/CNY",    sina="fx_seurcny"),
    dict(key="jpy_cny", base="JPY", name="日元",         unit=100, inverse=False, cat="主要货币", cfets="100JPY/CNY", sina="fx_sjpycny"),
    dict(key="hkd_cny", base="HKD", name="港元",         unit=1,   inverse=False, cat="主要货币", cfets="HKD/CNY",    sina="fx_shkdcny"),
    dict(key="gbp_cny", base="GBP", name="英镑",         unit=1,   inverse=False, cat="主要货币", cfets="GBP/CNY",    sina="fx_sgbpcny"),
    dict(key="aud_cny", base="AUD", name="澳元",         unit=1,   inverse=False, cat="主要货币", cfets="AUD/CNY",    sina="fx_saudcny"),
    dict(key="nzd_cny", base="NZD", name="新西兰元",     unit=1,   inverse=False, cat="主要货币", cfets="NZD/CNY",    sina="fx_snzdcny"),
    dict(key="sgd_cny", base="SGD", name="新加坡元",     unit=1,   inverse=False, cat="主要货币", cfets="SGD/CNY",    sina="fx_ssgdcny"),
    dict(key="chf_cny", base="CHF", name="瑞士法郎",     unit=1,   inverse=False, cat="主要货币", cfets="CHF/CNY",    sina="fx_schfcny"),
    dict(key="cad_cny", base="CAD", name="加元",         unit=1,   inverse=False, cat="主要货币", cfets="CAD/CNY",    sina="fx_scadcny"),
    # ── 其他货币：CFETS 以「人民币/外币」发布 → 入库取倒数 ──────────────────
    dict(key="mop_cny", base="MOP", name="澳门元",       unit=1,   inverse=True,  cat="其他货币", cfets="CNY/MOP",    sina="fx_smopcny"),
    dict(key="myr_cny", base="MYR", name="马来西亚林吉特", unit=1, inverse=True,  cat="其他货币", cfets="CNY/MYR",    sina="fx_smyrcny"),
    dict(key="rub_cny", base="RUB", name="俄罗斯卢布",   unit=1,   inverse=True,  cat="其他货币", cfets="CNY/RUB",    sina="fx_srubcny"),
    dict(key="zar_cny", base="ZAR", name="南非兰特",     unit=1,   inverse=True,  cat="其他货币", cfets="CNY/ZAR",    sina="fx_szarcny"),
    dict(key="krw_cny", base="KRW", name="韩元",         unit=1,   inverse=True,  cat="其他货币", cfets="CNY/KRW",    sina=None),
    dict(key="aed_cny", base="AED", name="阿联酋迪拉姆", unit=1,   inverse=True,  cat="其他货币", cfets="CNY/AED",    sina="fx_saedcny"),
    dict(key="sar_cny", base="SAR", name="沙特里亚尔",   unit=1,   inverse=True,  cat="其他货币", cfets="CNY/SAR",    sina=None),
    dict(key="huf_cny", base="HUF", name="匈牙利福林",   unit=1,   inverse=True,  cat="其他货币", cfets="CNY/HUF",    sina=None),
    dict(key="pln_cny", base="PLN", name="波兰兹罗提",   unit=1,   inverse=True,  cat="其他货币", cfets="CNY/PLN",    sina=None),
    dict(key="dkk_cny", base="DKK", name="丹麦克朗",     unit=1,   inverse=True,  cat="其他货币", cfets="CNY/DKK",    sina="fx_sdkkcny"),
    dict(key="sek_cny", base="SEK", name="瑞典克朗",     unit=1,   inverse=True,  cat="其他货币", cfets="CNY/SEK",    sina="fx_ssekcny"),
    dict(key="nok_cny", base="NOK", name="挪威克朗",     unit=1,   inverse=True,  cat="其他货币", cfets="CNY/NOK",    sina="fx_snokcny"),
    dict(key="try_cny", base="TRY", name="土耳其里拉",   unit=1,   inverse=True,  cat="其他货币", cfets="CNY/TRY",    sina=None),
    dict(key="mxn_cny", base="MXN", name="墨西哥比索",   unit=1,   inverse=True,  cat="其他货币", cfets="CNY/MXN",    sina=None),
    dict(key="thb_cny", base="THB", name="泰铢",         unit=1,   inverse=True,  cat="其他货币", cfets="CNY/THB",    sina="fx_sthbcny"),
]

CFETS_TO_PAIR = {p["cfets"]: p for p in PAIRS if p["cfets"]}
SINA_TO_PAIR  = {p["sina"]:  p for p in PAIRS if p["sina"]}

# ---------------------------------------------------------------------------
# DDL（本仓库无 drizzle migrations，DDL 内联在脚本里，幂等）
# ---------------------------------------------------------------------------
SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS fx_pairs (
    key            varchar(40)  PRIMARY KEY,
    base_code      varchar(8)   NOT NULL,
    base_name      varchar(40)  NOT NULL,
    quote_unit     integer      NOT NULL DEFAULT 1,
    category       varchar(20)  NOT NULL,
    official_code  varchar(24),
    spot_code      varchar(24),
    sort_order     integer      NOT NULL DEFAULT 100,
    updated_at     timestamptz  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fx_rates (
    id        bigserial PRIMARY KEY,
    pair_key  varchar(40) NOT NULL,
    rate_date date        NOT NULL,
    rate_type varchar(8)  NOT NULL,          -- 'mid' 中间价 | 'spot' 即期
    open      numeric(20,10),
    high      numeric(20,10),
    low       numeric(20,10),
    close     numeric(20,10),
    source    varchar(16) NOT NULL,          -- 'cfets' | 'sina'
    CONSTRAINT fx_rates_uniq UNIQUE (pair_key, rate_date, rate_type)
);
CREATE INDEX IF NOT EXISTS idx_fx_rates_pair_date ON fx_rates (pair_key, rate_date);
CREATE INDEX IF NOT EXISTS idx_fx_rates_type      ON fx_rates (rate_type);

-- 即期实时快照（一对一行）；与 index_spot / commodity_spot 同一模式
CREATE TABLE IF NOT EXISTS fx_spot (
    pair_key   varchar(40) PRIMARY KEY,
    price      numeric(20,10),
    prev_close numeric(20,10),
    open       numeric(20,10),
    high       numeric(20,10),
    low        numeric(20,10),
    change_pct numeric(10,4),
    change_amt numeric(20,10),
    quote_time varchar(16),
    spot_date  date,
    updated_at timestamptz NOT NULL DEFAULT NOW()
);
"""

UPSERT_PAIR_SQL = """
INSERT INTO fx_pairs (key, base_code, base_name, quote_unit, category,
                      official_code, spot_code, sort_order, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (key) DO UPDATE SET
    base_code     = EXCLUDED.base_code,
    base_name     = EXCLUDED.base_name,
    quote_unit    = EXCLUDED.quote_unit,
    category      = EXCLUDED.category,
    official_code = EXCLUDED.official_code,
    spot_code     = EXCLUDED.spot_code,
    sort_order    = EXCLUDED.sort_order,
    updated_at    = NOW()
"""

UPSERT_RATE_SQL = """
INSERT INTO fx_rates (pair_key, rate_date, rate_type, open, high, low, close, source)
VALUES %s
ON CONFLICT (pair_key, rate_date, rate_type) DO UPDATE SET
    open   = COALESCE(EXCLUDED.open,  fx_rates.open),
    high   = COALESCE(EXCLUDED.high,  fx_rates.high),
    low    = COALESCE(EXCLUDED.low,   fx_rates.low),
    close  = COALESCE(EXCLUDED.close, fx_rates.close),
    source = EXCLUDED.source
"""

UPSERT_SPOT_SQL = """
INSERT INTO fx_spot (pair_key, price, prev_close, open, high, low,
                     change_pct, change_amt, quote_time, spot_date, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (pair_key) DO UPDATE SET
    price      = EXCLUDED.price,
    prev_close = EXCLUDED.prev_close,
    open       = EXCLUDED.open,
    high       = EXCLUDED.high,
    low        = EXCLUDED.low,
    change_pct = EXCLUDED.change_pct,
    change_amt = EXCLUDED.change_amt,
    quote_time = EXCLUDED.quote_time,
    spot_date  = EXCLUDED.spot_date,
    updated_at = NOW()
"""


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def _num(val, digits: int = 10):
    """'-' / '' / None / 非数字 → None（CFETS 用 '-' 表示当日不发布）"""
    if val is None:
        return None
    s = str(val).strip().replace(",", "")
    if not s or s in ("-", "--", "---", "N/A"):
        return None
    try:
        return round(float(s), digits)
    except ValueError:
        return None


def _to_rate(raw: float, pair: dict):
    """
    CFETS 原始值 → 统一口径「1 单位外币兑人民币」。

        USD/CNY   6.7351    → 6.7351          （正向，quote_unit=1）
        100JPY/CNY 4.2727   → 0.042727        （÷ quote_unit）
        CNY/KRW   201.70    → 0.0049578572    （取倒数）

    新浪即期本身就是「1 外币兑人民币」，**不走这个函数**。
    """
    if raw is None:
        return None
    v = (1.0 / raw) if pair["inverse"] else raw
    return round(v / pair["unit"], 10)


def _chunks(start: date, end: date, days: int = CHUNK_DAYS):
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=days - 1), end)
        yield cur, stop
        cur = stop + timedelta(days=1)


# ---------------------------------------------------------------------------
# ① CFETS 中间价
# ---------------------------------------------------------------------------
def fetch_cfets_today() -> tuple[str | None, dict]:
    """
    当日中间价（ccpr.json）。

    Returns (发布日期 'YYYY-MM-DD' or None, {cfets_code: 原始值})
    """
    r = requests.get(CFETS_TODAY_URL, headers=CFETS_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    last_date = (j.get("data") or {}).get("lastDate")  # '2026-09-30 9:15'
    day = last_date.split(" ")[0] if last_date else None
    out = {}
    for rec in j.get("records") or []:
        code = rec.get("vrtEName")
        v = _num(rec.get("price"))
        if code and v is not None:
            out[code] = v
    return day, out


def fetch_cfets_range(start: date, end: date) -> dict:
    """
    分页拉取一段区间内**全部 25 对**的中间价。

    Returns {date_str: {cfets_code: raw_value}}

    关键点（都用实测换来的）：
      * 不带 currency → data.head 给代码顺序，records[].values 按序对齐
      * pageSize 最大 50，超了 HTTP 403
      * 区间 >1 年时服务端**静默返空**（data 为 ''，total 为 None），不报错 → 靠分块规避
    """
    out: dict = {}
    page = 1
    while True:
        params = {
            "startDate": start.isoformat(),
            "endDate":   end.isoformat(),
            "pageSize":  PAGE_SIZE,
            "pageNum":   page,
        }
        r = requests.get(CFETS_HIST_URL, headers=CFETS_HEADERS, params=params, timeout=TIMEOUT)
        ctype = r.headers.get("content-type", "")
        if "json" not in ctype:
            # 实测 pageSize>50 会走到这里（WAF 的 403 HTML）—— 显式报错，别静默吞
            raise RuntimeError(
                f"CFETS 返回非 JSON（HTTP {r.status_code}, {ctype}）；"
                f"pageSize/pageNum 是否越界？ body={r.text[:120]!r}"
            )
        j = r.json()
        data = j.get("data") or {}
        head = data.get("head")
        recs = j.get("records") or []
        if not head:
            raise RuntimeError(f"CFETS 响应缺少 data.head，无法对齐列：{str(j)[:200]}")
        if not recs:
            break
        for rec in recs:
            d = rec.get("date")
            vals = rec.get("values") or []
            if not d:
                continue
            row = {}
            for code, v in zip(head, vals):
                n = _num(v)
                if n is not None:
                    row[code] = n
            if row:
                out[d] = row
        page_total = data.get("pageTotal") or 1
        if page >= page_total:
            break
        page += 1
    return out


# ---------------------------------------------------------------------------
# ② 新浪即期
# ---------------------------------------------------------------------------
def fetch_sina_dayline(symbol: str) -> list[tuple]:
    """
    全历史日线。

    Returns [(date_str, open, low, high, close)]  —— 注意源侧顺序是 O/L/H/C，
    这里**已经换成 O/H/L/C** 再返回，调用方不用再记这个坑。
    """
    r = requests.get(SINA_DAY_URL.format(symbol=symbol), headers=SINA_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    m = re.search(r'var\s+t=\("(.*?)"\)', r.text, re.S)
    if not m:
        if "data is empty" in r.text:
            return []
        raise RuntimeError(f"无法解析新浪日线响应：{r.text[:160]!r}")

    rows = []
    for seg in m.group(1).split("|"):
        seg = seg.strip()
        if not seg:
            continue
        p = seg.split(",")
        if len(p) < 5:
            continue
        d = p[0].strip()
        o, lo, hi, c = (_num(p[1]), _num(p[2]), _num(p[3]), _num(p[4]))
        if not d or c is None:
            continue
        rows.append((d, o, hi, lo, c))   # 源侧 O/L/H/C → 统一成 O/H/L/C
    return rows


def fetch_sina_realtime(symbols: list[str]) -> dict:
    """
    即期实时快照。字段布局见模块 docstring（两次采样差分确认）。

    Returns {symbol: dict(price, prev_close, open, high, low, change_pct, change_amt, quote_time, spot_date)}
    """
    out = {}
    if not symbols:
        return out
    r = requests.get(SINA_RT_URL.format(symbols=",".join(symbols)),
                     headers=SINA_HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    r.encoding = "gbk"          # ⚠ 不是 utf-8，写错会得到乱码名称
    for line in r.text.strip().split("\n"):
        m = re.search(r'hq_str_(\w+)="(.*)"', line)
        if not m:
            continue
        sym, body = m.group(1), m.group(2)
        f = body.split(",")
        if len(f) < 18:
            continue
        out[sym] = {
            "quote_time": f[0].strip() or None,
            "price":      _num(f[8]),
            "prev_close": _num(f[3]),
            "open":       _num(f[5]),
            "high":       _num(f[6]),
            "low":        _num(f[7]),
            "change_pct": _num(f[10], 4),
            "change_amt": _num(f[11]),
            "spot_date":  f[17].strip() or None,
        }
    return out


# ---------------------------------------------------------------------------
# 库操作
# ---------------------------------------------------------------------------
def conn_db():
    return psycopg2.connect(DATABASE_URL)


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
    conn.commit()


def latest_dates(conn) -> dict:
    """{(pair_key, rate_type): 'YYYY-MM-DD'}"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pair_key, rate_type, MAX(rate_date)::text
            FROM fx_rates GROUP BY pair_key, rate_type
        """)
        return {(r[0], r[1]): r[2] for r in cur.fetchall()}


def upsert_pairs(conn):
    with conn.cursor() as cur:
        for i, p in enumerate(PAIRS, start=1):
            cur.execute(UPSERT_PAIR_SQL, (
                p["key"], p["base"], p["name"], p["unit"], p["cat"],
                p["cfets"], p["sina"], i * 10,
            ))
    conn.commit()


def _dedupe(entries: list) -> list:
    """
    按 (pair_key, rate_date, rate_type) 去重，保留**最后一条**。

    ⚠ 必须做：增量模式下「当日 ccpr.json」与「历史分块」必然在最后一个交易日的
    同一 (pair, date, type) 上重叠；同一批 INSERT … ON CONFLICT DO UPDATE 里
    出现重复约束键，PG 会直接报
        CardinalityViolation: ON CONFLICT DO UPDATE command cannot affect row a second time
    （--full 时因为重复项恰好落进不同 page_size 分片而侥幸没报，属静默隐患）
    保留最后一条 = 让后面的来源覆盖前面的（当日快照 > 历史分块）。
    """
    seen: dict = {}
    for e in entries:
        seen[(e[0], e[1], e[2])] = e
    return list(seen.values())


def write_rates(conn, entries: list) -> int:
    """entries: [(pair_key, date_str, rate_type, open, high, low, close, source)]"""
    entries = _dedupe(entries)
    if not entries:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_RATE_SQL, entries, page_size=1000)
    conn.commit()
    return len(entries)


def write_spot(conn, rows: list) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(UPSERT_SPOT_SQL, row)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def print_status(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fx_pairs")
        n_pairs = cur.fetchone()[0]
        print(f"  fx_pairs: {n_pairs} 个货币对")
        cur.execute("""
            SELECT rate_type, COUNT(DISTINCT pair_key), COUNT(*),
                   MIN(rate_date)::text, MAX(rate_date)::text
            FROM fx_rates GROUP BY rate_type ORDER BY rate_type
        """)
        for rt, npair, n, lo, hi in cur.fetchall():
            label = {"mid": "中间价(cfets)", "spot": "即期(sina)"}.get(rt, rt)
            print(f"  fx_rates[{rt:<4}] {label:<14} {npair:2d} 对 / {n:6d} 行   {lo} → {hi}")
        cur.execute("SELECT COUNT(*), MAX(updated_at)::text FROM fx_spot")
        n, ts = cur.fetchone()
        print(f"  fx_spot: {n} 条实时快照（最近 {ts}）")
        cur.execute("""
            SELECT p.key, p.base_name,
                   (SELECT close FROM fx_rates r WHERE r.pair_key = p.key
                      AND r.rate_type = 'mid' ORDER BY rate_date DESC LIMIT 1),
                   (SELECT MAX(rate_date)::text FROM fx_rates r WHERE r.pair_key = p.key
                      AND r.rate_type = 'mid')
            FROM fx_pairs p ORDER BY p.sort_order
        """)
        rows = cur.fetchall()
        print(f"\n  {'key':<10} {'货币':<20} {'最新中间价*':>14}  日期")
        for k, name, v, d in rows:
            print(f"  {k:<10} {name:<20} {('—' if v is None else str(v)):>14}  {d or '—'}")
        print("\n  * 统一口径：1 单位外币兑人民币（日元为 1 日元，展示时 ×100）")


def main() -> int:
    ap = argparse.ArgumentParser(description="人民币汇率抓取（CFETS 中间价 + 新浪即期）")
    ap.add_argument("--dry-run", action="store_true", help="只抓不写库")
    ap.add_argument("--full",    action="store_true", help=f"全历史回补（中间价 {EARLIEST_MID} 起）")
    ap.add_argument("--days",    type=int, default=30, help="增量模式下中间价回看天数（默认 30）")
    ap.add_argument("--status",  action="store_true", help="只打印库内状态")
    ap.add_argument("--no-spot", action="store_true", help="跳过新浪即期（只抓中间价）")
    args = ap.parse_args()

    if args.status:
        conn = conn_db()
        try:
            ensure_schema(conn)
            print("── 外汇库状态 ──────────────────────────────────────────────────")
            print_status(conn)
        finally:
            conn.close()
        return 0

    today = date.today()
    errors: list[str] = []

    # ── ① 中间价 ────────────────────────────────────────────────────────────
    print("── ① 人民币汇率中间价（中国外汇交易中心 cfets）───────────────────")
    mid_entries: list = []
    mid_ranges: list[tuple] = []

    try:
        day, today_vals = fetch_cfets_today()
        if day and today_vals:
            for code, raw in today_vals.items():
                p = CFETS_TO_PAIR.get(code)
                if not p:
                    continue
                mid_entries.append((p["key"], day, RATE_TYPE_MID, None, None, None,
                                    _to_rate(raw, p), "cfets"))
            print(f"  当日中间价 {day}：{len(today_vals)} 对 → 落库 {len(mid_entries)} 行")
        else:
            print("  [WARN] 当日中间价为空（节假日？）")
    except Exception as exc:
        errors.append(f"cfets 当日: {exc}")
        print(f"  [ERROR] 当日中间价抓取失败: {exc}")

    if args.full:
        start = datetime.strptime(EARLIEST_MID, "%Y-%m-%d").date()
    else:
        start = today - timedelta(days=args.days)
    # 历史区间截止到昨天（今天已由 ccpr.json 覆盖）
    hist_end = today - timedelta(days=1)

    if start <= hist_end:
        total_days = 0
        for c_start, c_end in _chunks(start, hist_end, CHUNK_DAYS):
            try:
                data = fetch_cfets_range(c_start, c_end)
            except Exception as exc:
                errors.append(f"cfets {c_start}~{c_end}: {exc}")
                print(f"  [ERROR] {c_start}~{c_end} 失败: {exc}")
                continue
            n = 0
            for d, row in data.items():
                for code, raw in row.items():
                    p = CFETS_TO_PAIR.get(code)
                    if not p:
                        continue
                    mid_entries.append((p["key"], d, RATE_TYPE_MID, None, None, None,
                                        _to_rate(raw, p), "cfets"))
                    n += 1
            total_days += len(data)
            print(f"  {c_start} ~ {c_end}: {len(data):3d} 个交易日 / {n:5d} 行")
        print(f"  历史合计 {total_days} 个交易日")

    if args.dry_run:
        print(f"  [DRY-RUN] 中间价待写 {len(mid_entries)} 行")

    # ── ② 即期 ──────────────────────────────────────────────────────────────
    spot_entries: list = []
    spot_rows:    list = []
    if not args.no_spot:
        print("\n── ② 人民币即期汇率（新浪财经 sina）─────────────────────────────")
        for p in PAIRS:
            if not p["sina"]:
                continue
            try:
                rows = fetch_sina_dayline(p["sina"])
            except Exception as exc:
                errors.append(f"sina {p['sina']}: {exc}")
                print(f"  [ERROR] {p['sina']:<14} 日线失败: {exc}")
                continue
            for d, o, hi, lo, c in rows:
                spot_entries.append((p["key"], d, RATE_TYPE_SPOT, o, hi, lo, c, "sina"))
            span = f"{rows[0][0]} → {rows[-1][0]}" if rows else "无数据"
            print(f"  {p['sina']:<14} {len(rows):5d} 根   {span}")

        syms = [p["sina"] for p in PAIRS if p["sina"]]
        try:
            rt = fetch_sina_realtime(syms)
            for sym, q in rt.items():
                p = SINA_TO_PAIR[sym]
                if q["price"] is None:
                    continue
                spot_rows.append((
                    p["key"], q["price"], q["prev_close"], q["open"], q["high"], q["low"],
                    q["change_pct"], q["change_amt"], q["quote_time"], q["spot_date"],
                ))
            print(f"  实时快照：{len(spot_rows)}/{len(syms)} 对")
        except Exception as exc:
            errors.append(f"sina 实时: {exc}")
            print(f"  [ERROR] 实时快照失败: {exc}")

        if args.dry_run:
            print(f"  [DRY-RUN] 即期待写 {len(spot_entries)} 行 + 快照 {len(spot_rows)} 条")

    # ── 写库 ────────────────────────────────────────────────────────────────
    if args.dry_run:
        print("\n[DRY-RUN] 未写库。")
        return 1 if errors else 0

    print("\n── 写库 ──────────────────────────────────────────────────────────")
    conn = conn_db()
    try:
        ensure_schema(conn)
        upsert_pairs(conn)
        print(f"  [OK] fx_pairs 就绪（{len(PAIRS)} 对）")

        known = latest_dates(conn)

        # 增量：即期日线只写「库内最新 - 7 天」之后的，避免每次重写 2 万行
        # （新浪 getDayKLine 每次都返回全历史，没有增量参数）
        if not args.full:
            spot_entries = [
                e for e in spot_entries
                if (e[0], RATE_TYPE_SPOT) not in known
                or e[1] >= _shift(known[(e[0], RATE_TYPE_SPOT)], -7)
            ]

        n_mid  = write_rates(conn, mid_entries)
        n_spot = write_rates(conn, spot_entries)
        n_rt   = write_spot(conn, spot_rows)
        print(f"  [OK] fx_rates 中间价 {n_mid} 行 / 即期 {n_spot} 行")
        print(f"  [OK] fx_spot  实时快照 {n_rt} 条")

        print()
        print_status(conn)
    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()

    if errors:
        print(f"\n[WARN] 有 {len(errors)} 个失败项（未写入的部分不会静默成功）：")
        for e in errors:
            print(f"       - {e}")
        return 1
    print("\n[OK] 外汇数据更新完成。")
    return 0


def _shift(date_str: str, days: int) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d").date() + timedelta(days=days)
    return d.isoformat()


if __name__ == "__main__":
    sys.exit(main())
