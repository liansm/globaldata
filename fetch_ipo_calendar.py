#!/usr/bin/env python3
"""
新股日历 Fetcher（A股 / 北交所 / 港股）
=======================================

抓取最近发行与近期上市的新股，落 ipo_calendar 表（market 列区分三个市场）。

数据源
------
1) A股 + 北交所 —— 东方财富「新股申购与中签」
   GET https://datacenter-web.eastmoney.com/api/data/v1/get
       ?reportName=RPTA_APP_IPOAPPLY&type=XGSG_LB&...
   akshare 封装：ak.stock_xgsglb_em(symbol="全部股票")
   * ⚠ 合法 symbol 是「全部股票」；传「全部A股」直接 KeyError。
   * 4032 条全历史（2010-01-04 起），**含未来申购排期**（实测到 2026-10-09）。
   * 「板块」列区分 非科创板 / 科创板 / 北交所 —— 北交所 356 条即所需市场。
   * ⚠「发行总数」列单位是**万股**，不是股（第一次就栽在这：南方乳业是
     3518.5185 万股，按「股」算会让募资额整整小 1 万倍）。乘 1e4 才是股数。
   * 募资额源侧不给，自算：发行总数(万股) × 发行价格 / 1e4 = 亿元
     （核对：14.21 × 3518.5185 / 1e4 ≈ 5.00 亿 ✓）
   * 分页由 akshare 内部翻完，一次调用拿全量。

2) 港股·即将招股 —— 财华社 IPO Center（AAStocks services1）
   GET https://services1.aastocks.com/Web/chsu/ipo/IndustryComparison.aspx
       ?Language=Chi&view=0&symbol=02590
   服务端渲染 ASP.NET，无需登录。表头：
     上市代號 | 公司名稱 | 行業 | 招股價 | 每手股數 | 入場費 | 預測市盈率 | 招股日期 | 上市日期
   * ⚠「招股日期」是**区间文本**（"2026/09/28-2026/10/06"）→ 拆成 apply_date /
     apply_end_date。这是全站唯一免费给出港股**招股起始日**的地方。
   * symbol 只是页面上下文，upcoming 列表与它无关，固定传 02590 即可。

3) 港股·招股截止 + 暗盘 —— AAStocks 新股频道
   GET https://www.aastocks.com/tc/stocks/market/ipo/upcomingipo
   服务端渲染。两张表：
     即將招股：公司名稱/代號 | 行業 | 招股價 | 每手股數 | 入場費 | 招股截止日 | 暗盤日期 | 上市日期
     今日暗盤：公司名稱/代號 | 行業 | 上市價 | 每手股數 | 入場費
   * 「暗盤日」是港股打新独有节点（上市前一日场外交易），A股无对应。

4) 港股·已上市 + 募集资金 —— 东方财富港股新股上市
   GET https://hk.eastmoney.com/ipolist.html  (+ ipolist_N.html, N=1..52)
   每页 50 条，52 页 ≈ 2600 只。字段：
     序號 | 股票代碼 | 股票名稱 | 招股價 | 招股數(股) | 募集資金(港元) | 招股日期 | 上市日期
   * 「募集資金(港元)」是免费渠道里**唯一**给出的港股募资额，但只在**已上市**
     标的上存在；未上市新股不披露 → 留空（宁缺勿错），前端显示「—」。
   * ⚠ ?code=&startdate=&enddate= 参数**被服务端忽略**（带与不带返回字节数完全一致），
     只能翻页。默认只取第 1 页（最近 50 只），--full 才翻全部 52 页。

死路（记档，别再试）
--------------------
* ak.stock_ipo_hk_ths() —— **名字带 hk，实际返回 A 股**。源码打
  data.10jqka.com.cn/ipo/xgsgyzq/hkstock/，该页已改回 A 股表头（19+19 列重复），
  数据 JS 渲染。这个坑最容易骗到人。
* 雪球 stock.xueqiu.com/v5/stock/ipo/{hk,cn,us,list}.json —— 即使 warmup 拿到
  xq_a_token / xq_r_token 仍返回 403（openresty）。死路。
* 东财 datacenter 港股报表盲试 15 个 reportName 全部「报表配置不存在」：
  RPT_HK_NEWSTOCK / RPT_HKIPO_LIST / RPT_HK_IPO_LIST / RPT_HKNEWSTOCK /
  RPT_HK_IPO_NEWSTOCK / RPT_HKIPO_APPLY / RPTA_HK_IPOAPPLY / RPT_HK_IPOLIST /
  RPT_HK_NEWLIST / RPT_HKIPO / RPT_HK_IPO / RPTA_HK_IPOLIST / RPT_HKNEWSTOCK_LIST /
  RPT_HK_IPO_ISSUE / RPT_HKIPOINFO
* 港交所 www1.hkex.com.hk/hkexwidget/data/getipo.js → 404。
* AAStocks 主站 /tc/stocks/ipo/listing 与 /ipo-summary 返回**同一 JS 壳**
  （两个不同 URL 都返回 34467 字节），无数据。
* 东财 hk.eastmoney.com/{ipo,newstock,ipolist_hk}.html → 404。

合规
----
东财 / AAStocks / 财华社均为公开网页，仅抓取公开披露的新股发行信息，用于内部
研究。AAStocks 与财华社的页面结构与使用条款请在对外发布前复核。

用法
----
    python fetch_ipo_calendar.py            # 增量（A股全量 + 港股近期）
    python fetch_ipo_calendar.py --full     # 港股东财翻全部 52 页
    python fetch_ipo_calendar.py --dry-run  # 只抓取并打印，不写库
    python fetch_ipo_calendar.py --status   # 只看库内状态
"""

import argparse
import os
import re
import sys
from datetime import date, datetime

import requests
import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9",
}
TIMEOUT = 25

EM_HK_IPO_URL   = "https://hk.eastmoney.com/ipolist{page}.html"
EM_HK_MAX_PAGE  = 52
AASTOCKS_IPO_URL = "https://www.aastocks.com/tc/stocks/market/ipo/upcomingipo"
CHSU_IPO_URL     = ("https://services1.aastocks.com/Web/chsu/ipo/"
                    "IndustryComparison.aspx?Language=Chi&view=0&symbol=02590")

# ---------------------------------------------------------------------------
# DDL —— 本仓库不用 migrations，建表语句内联在脚本里，幂等 CREATE
# ---------------------------------------------------------------------------
# 字段设计说明：
#   * market 列区分「A股 / 北交所 / 港股」，一张表统管三市场，不按市场分表
#   * 三家市场的字段重合度低，市场特有字段一律可空：
#       A股独有  → allotment_date(中签号公布) / pay_date(中签缴款) / pe_industry / win_rate
#       港股独有  → apply_end_date(招股截止) / pricing_date(定价) / refund_date(退票)
#                   / grey_date(暗盘) / lot_size(每手) / entry_fee(入场费)
#   * raise_amount 统一单位「亿」，currency 标明币种（CNY / HKD）
SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS ipo_calendar (
    id               BIGSERIAL PRIMARY KEY,
    market           VARCHAR(10)  NOT NULL,
    code             VARCHAR(16)  NOT NULL,
    name             VARCHAR(120) NOT NULL,
    exchange         VARCHAR(30),
    board            VARCHAR(20),
    industry         VARCHAR(60),
    issue_price      NUMERIC(14, 4),
    issue_price_high NUMERIC(14, 4),
    currency         VARCHAR(6),
    issue_shares     NUMERIC(24, 4),
    raise_amount     NUMERIC(20, 4),
    lot_size         NUMERIC(14, 2),
    entry_fee        NUMERIC(14, 2),
    apply_date       DATE,
    apply_end_date   DATE,
    pricing_date     DATE,
    allotment_date   DATE,
    pay_date         DATE,
    refund_date      DATE,
    grey_date        DATE,
    listing_date     DATE,
    pe_issue         NUMERIC(14, 4),
    pe_industry      NUMERIC(14, 4),
    win_rate         NUMERIC(14, 6),
    source           VARCHAR(120) NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (market, code)
);
-- 港股是三源合并，source 会用 '+' 串起全部贡献源（最长约 50 字符）。
-- 首版给的 VARCHAR(24) 会直接 StringDataRightTruncation，这里幂等放宽。
ALTER TABLE ipo_calendar ALTER COLUMN source TYPE VARCHAR(120);
ALTER TABLE ipo_calendar ALTER COLUMN name   TYPE VARCHAR(120);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_listing ON ipo_calendar (listing_date);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_apply   ON ipo_calendar (apply_date);
CREATE INDEX IF NOT EXISTS idx_ipo_calendar_market  ON ipo_calendar (market);
"""

# 多源合并写入：同一 (market, code) 可能被 2 个源先后命中，
# 用 COALESCE 保证**已有非空值不被后到的空值抹掉**（新股日历最怕字段被清空）。
UPSERT_SQL = """
INSERT INTO ipo_calendar (
    market, code, name, exchange, board, industry,
    issue_price, issue_price_high, currency,
    issue_shares, raise_amount, lot_size, entry_fee,
    apply_date, apply_end_date, pricing_date, allotment_date, pay_date,
    refund_date, grey_date, listing_date,
    pe_issue, pe_industry, win_rate, source
) VALUES %s
ON CONFLICT (market, code) DO UPDATE SET
    name             = COALESCE(EXCLUDED.name,             ipo_calendar.name),
    exchange         = COALESCE(EXCLUDED.exchange,         ipo_calendar.exchange),
    board            = COALESCE(EXCLUDED.board,            ipo_calendar.board),
    industry         = COALESCE(EXCLUDED.industry,         ipo_calendar.industry),
    issue_price      = COALESCE(EXCLUDED.issue_price,      ipo_calendar.issue_price),
    issue_price_high = COALESCE(EXCLUDED.issue_price_high, ipo_calendar.issue_price_high),
    currency         = COALESCE(EXCLUDED.currency,         ipo_calendar.currency),
    issue_shares     = COALESCE(EXCLUDED.issue_shares,     ipo_calendar.issue_shares),
    raise_amount     = COALESCE(EXCLUDED.raise_amount,     ipo_calendar.raise_amount),
    lot_size         = COALESCE(EXCLUDED.lot_size,         ipo_calendar.lot_size),
    entry_fee        = COALESCE(EXCLUDED.entry_fee,        ipo_calendar.entry_fee),
    apply_date       = COALESCE(EXCLUDED.apply_date,       ipo_calendar.apply_date),
    apply_end_date   = COALESCE(EXCLUDED.apply_end_date,   ipo_calendar.apply_end_date),
    pricing_date     = COALESCE(EXCLUDED.pricing_date,     ipo_calendar.pricing_date),
    allotment_date   = COALESCE(EXCLUDED.allotment_date,   ipo_calendar.allotment_date),
    pay_date         = COALESCE(EXCLUDED.pay_date,         ipo_calendar.pay_date),
    refund_date      = COALESCE(EXCLUDED.refund_date,      ipo_calendar.refund_date),
    grey_date        = COALESCE(EXCLUDED.grey_date,        ipo_calendar.grey_date),
    listing_date     = COALESCE(EXCLUDED.listing_date,     ipo_calendar.listing_date),
    pe_issue         = COALESCE(EXCLUDED.pe_issue,         ipo_calendar.pe_issue),
    pe_industry      = COALESCE(EXCLUDED.pe_industry,      ipo_calendar.pe_industry),
    win_rate         = COALESCE(EXCLUDED.win_rate,         ipo_calendar.win_rate),
    updated_at       = NOW()
"""

COLUMNS = [
    "market", "code", "name", "exchange", "board", "industry",
    "issue_price", "issue_price_high", "currency",
    "issue_shares", "raise_amount", "lot_size", "entry_fee",
    "apply_date", "apply_end_date", "pricing_date", "allotment_date", "pay_date",
    "refund_date", "grey_date", "listing_date",
    "pe_issue", "pe_industry", "win_rate", "source",
]


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------
def _txt(v) -> str | None:
    """单元格文本清洗；空串 / 'N/A' / '-' 一律归 None。"""
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).replace("\xa0", " ").strip()
    if s in ("", "-", "--", "N/A", "n/a", "NA", "不適用", "不适用",
             "nan", "NaN", "NaT", "None", "null", "NULL"):
        return None
    return s


def _f(v) -> float | None:
    """安全转 float，去掉千分位与货币/单位后缀（'7,058.47' / '69.880' / '1.48-1.59'）。"""
    s = _txt(v)
    if s is None:
        return None
    s = s.replace(",", "")
    s = re.sub(r"^[^\d.\-+]+", "", s)          # 去前置货币符号/文字
    s = re.sub(r"[^\d.\-+eE]+$", "", s)        # 去后置单位
    try:
        return float(s)
    except ValueError:
        return None


def _date(v) -> date | None:
    """安全转日期，兼容 2026-09-28 / 2026/09/28 / 2026.09.28 / Timestamp。"""
    if v is None:
        return None
    # ⚠ pandas NaT / numpy NaN 都「不等于自身」，必须在 isinstance 之前挡掉：
    #   pd.NaT 是 datetime 的子类，直接 isinstance 判断会放行，
    #   然后一路混到 psycopg2 才报 InvalidDatetimeFormat: "NaT"（东财表里大量 NaT）
    try:
        if v != v:
            return None
    except Exception:
        pass
    if isinstance(v, datetime):
        d = v.date()
        return None if d != d else d
    if isinstance(v, date):
        return v
    s = _txt(v)
    if s is None:
        return None
    m = re.search(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", s)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _price_range(s) -> tuple[float | None, float | None]:
    """'1.48-1.59 港元' → (1.48, 1.59)；单一价 '58.85' → (58.85, None)"""
    t = _txt(s)
    if t is None:
        return None, None
    nums = re.findall(r"\d+(?:\.\d+)?", t)
    if not nums:
        return None, None
    if len(nums) >= 2:
        return float(nums[0]), float(nums[1])
    return float(nums[0]), None


def _date_range(s) -> tuple[date | None, date | None]:
    """'2026/09/28-2026/10/06' → (date, date)；单日 → (date, None)"""
    t = _txt(s)
    if t is None:
        return None, None
    hits = re.findall(r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}", t)
    if not hits:
        return None, None
    if len(hits) >= 2:
        return _date(hits[0]), _date(hits[1])
    return _date(hits[0]), None


def _cn_amount(s) -> float | None:
    """'56.70亿' → 56.70；'17201.47万' → 1.720147（单位：亿）。"""
    t = _txt(s)
    if t is None:
        return None
    v = _f(t)
    if v is None:
        return None
    if "万" in t:
        return round(v / 1e4, 6)
    return v


def _row(**kw) -> dict:
    """构造一行，缺的键补 None，保证列顺序一致。"""
    out = {c: None for c in COLUMNS}
    out.update(kw)
    return out


# ---------------------------------------------------------------------------
# 1) A股 + 北交所
# ---------------------------------------------------------------------------
def fetch_a_share() -> list[dict]:
    import akshare as ak

    df = ak.stock_xgsglb_em(symbol="全部股票")
    if df is None or df.empty:
        raise ValueError("东财新股申购返回空表")

    rows = []
    for _, r in df.iterrows():
        code = _txt(r.get("股票代码"))
        name = _txt(r.get("股票简称"))
        if not code or not name:
            continue

        board_raw = _txt(r.get("板块")) or ""
        market = "北交所" if board_raw == "北交所" else "A股"

        price      = _f(r.get("发行价格"))
        shares_wan = _f(r.get("发行总数"))          # ⚠ 该列单位是「万股」
        shares     = round(shares_wan * 1e4, 2) if shares_wan else None
        # 募资额 = 发行总数(万股) × 发行价 / 1e4 = 亿元
        raise_amount = round(price * shares_wan / 1e4, 4) if (price and shares_wan) else None

        rows.append(_row(
            market         = market,
            code           = code,
            name           = name,
            exchange       = _txt(r.get("交易所")),
            board          = board_raw or None,
            issue_price    = price,
            currency       = "CNY",
            issue_shares   = shares,
            raise_amount   = raise_amount,
            apply_date     = _date(r.get("申购日期")),
            allotment_date = _date(r.get("中签号公布日")),
            pay_date       = _date(r.get("中签缴款日期")),
            listing_date   = _date(r.get("上市日期")),
            pe_issue       = _f(r.get("发行市盈率")),
            pe_industry    = _f(r.get("行业市盈率")),
            win_rate       = _f(r.get("中签率")),
            source         = "em_xgsglb",
        ))
    return rows


# ---------------------------------------------------------------------------
# 2) 港股 · 财华社（招股日期区间）
# ---------------------------------------------------------------------------
def _norm_hk_code(s: str | None) -> str | None:
    """'06802.HK' / '06802' → '06802'；A股 6 位码原样返回。"""
    t = _txt(s)
    if t is None:
        return None
    t = t.replace(".HK", "").replace(".hk", "").strip()
    t = re.sub(r"[^\d]", "", t)
    if not t:
        return None
    return t.zfill(5) if len(t) in (4, 5) else t


def fetch_hk_chsu() -> list[dict]:
    """财华社 IPO Center —— 提供港股招股**起始日**（区间文本）。"""
    resp = requests.get(CHSU_IPO_URL, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content.decode("utf-8", errors="ignore"), "lxml")

    rows = []
    for tab in soup.find_all("table"):
        trs = tab.find_all("tr")
        if len(trs) < 2:
            continue
        head = " ".join(c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"]))
        if "招股日期" not in head or "上市日期" not in head:
            continue

        for tr in trs[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            cells = [c for c in cells if c and c.strip()]
            if len(cells) < 8:
                continue
            raw_code, raw_name = cells[0], cells[1]
            code = _norm_hk_code(raw_code)
            name = _txt(raw_name)
            if not code or not name or not re.fullmatch(r"\d+", code):
                continue

            price_lo, price_hi = _price_range(cells[3])
            apply_from, apply_to = _date_range(cells[7])
            rows.append(_row(
                market         = "港股",
                code           = code,
                name           = name,
                exchange       = "香港交易所",
                industry       = _txt(cells[2]),
                issue_price    = price_lo,
                issue_price_high = price_hi,
                currency       = "HKD",
                lot_size       = _f(cells[4]),
                entry_fee      = _f(cells[5]),
                pe_issue       = _f(cells[6]),
                apply_date     = apply_from,
                apply_end_date = apply_to,
                listing_date   = _date(cells[8]),
                source         = "aastocks_chsu",
            ))
    if not rows:
        raise ValueError("财华社 IPO Center 未解析到任何港股新股")
    return rows


# ---------------------------------------------------------------------------
# 3) 港股 · AAStocks 新股频道（招股截止 + 暗盘日）
# ---------------------------------------------------------------------------
def fetch_hk_aastocks() -> list[dict]:
    resp = requests.get(AASTOCKS_IPO_URL, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content.decode("utf-8", errors="ignore"), "lxml")

    rows = []
    for tab in soup.find_all("table"):
        trs = tab.find_all("tr")
        if len(trs) < 2:
            continue
        head = " ".join(c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"]))

        is_upcoming = "招股截止日" in head and "上市日期" in head
        is_grey     = "上市價" in head and "暗盤" in head
        if not (is_upcoming or is_grey):
            continue

        for tr in trs[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            cells = [c for c in cells if c and c.strip()]
            if not cells:
                continue

            # 第一格是「公司名稱 + 代號」，如 '歡創科技 06802.HK 今日暗盤'
            first = cells[0]
            m_code = re.search(r"(\d{4,5})(?:\.HK)?", first)
            if not m_code:
                continue
            code = _norm_hk_code(m_code.group(1))
            name = _txt(re.sub(r"\d{4,5}(?:\.HK)?.*$", "", first).strip())
            if not code or not name:
                continue

            if is_upcoming:
                price_lo, price_hi = _price_range(cells[2])
                rows.append(_row(
                    market         = "港股",
                    code           = code,
                    name           = name,
                    exchange       = "香港交易所",
                    industry       = _txt(cells[1]),
                    issue_price    = price_lo,
                    issue_price_high = price_hi,
                    currency       = "HKD",
                    lot_size       = _f(cells[3]),
                    entry_fee      = _f(cells[4]),
                    apply_end_date = _date(cells[5]),
                    grey_date      = _date(cells[6]),
                    listing_date   = _date(cells[7]),
                    source         = "aastocks_ipo",
                ))
            else:
                rows.append(_row(
                    market         = "港股",
                    code           = code,
                    name           = name,
                    exchange       = "香港交易所",
                    industry       = _txt(cells[1]),
                    issue_price    = _f(cells[2]),   # 「上市價」= 已定价
                    currency       = "HKD",
                    lot_size       = _f(cells[3]),
                    entry_fee      = _f(cells[4]),
                    # 这张表的表头就写「今日暗盤」，源侧不单独给日期，只能认定是当天
                    grey_date      = date.today(),
                    source         = "aastocks_grey",
                ))
    if not rows:
        raise ValueError("AAStocks 新股频道未解析到任何港股新股")
    return rows


# ---------------------------------------------------------------------------
# 4) 港股 · 东财（已上市 + 募集资金）
# ---------------------------------------------------------------------------
def fetch_hk_eastmoney(full: bool = False) -> list[dict]:
    pages = range(1, EM_HK_MAX_PAGE + 1) if full else range(1, 2)
    rows, seen = [], set()

    for p in pages:
        url = EM_HK_IPO_URL.format(page="" if p == 1 else f"_{p}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
        except Exception as exc:
            print(f"  [WARN] 东财港股第 {p} 页请求失败，跳过: {exc}")
            continue

        soup = BeautifulSoup(resp.content.decode("utf-8", errors="ignore"), "lxml")
        tab = soup.find("table")
        if tab is None:
            print(f"  [WARN] 东财港股第 {p} 页未找到表格")
            continue

        for tr in tab.find_all("tr")[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            if len(cells) < 8:
                continue
            code = _norm_hk_code(cells[1])
            name = _txt(cells[2])
            if not code or not name or code in seen:
                continue
            seen.add(code)

            price_lo, price_hi = _price_range(cells[3])
            shares_wan = _f(cells[4])          # 招股數(股) 单位为「万」
            rows.append(_row(
                market         = "港股",
                code           = code,
                name           = name,
                exchange       = "香港交易所",
                issue_price    = price_lo,
                issue_price_high = price_hi,
                currency       = "HKD",
                issue_shares   = round(shares_wan * 1e4, 2) if shares_wan else None,
                raise_amount   = _cn_amount(cells[5]),
                apply_date     = _date(cells[6]),
                listing_date   = _date(cells[7]),
                source         = "em_hk_ipo",
            ))

    if not rows:
        raise ValueError("东财港股新股列表未解析到任何数据")
    return rows


# ---------------------------------------------------------------------------
# 合并（港股三源按 code 归并，先到的非空值优先）
# ---------------------------------------------------------------------------
def merge_hk(*sources: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for src in sources:
        for row in src:
            code = row["code"]
            if code not in merged:
                merged[code] = row
                continue
            base = merged[code]
            for k, v in row.items():
                if base.get(k) is None and v is not None:
                    base[k] = v
            # source 记录「贡献了主要字段的源」，多源时用 + 连接便于追溯
            if row["source"] not in (base.get("source") or ""):
                base["source"] = f"{base.get('source')}+{row['source']}"
    return list(merged.values())


# ---------------------------------------------------------------------------
# 写库
# ---------------------------------------------------------------------------
def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
    conn.commit()


def upsert(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    # 按 (market, code) 批内去重：execute_values + ON CONFLICT 遇到同批重复键会
    # 报 CardinalityViolation 并整批回滚（项目里踩过，见 private_funds 教训）
    uniq: dict[tuple, dict] = {}
    for r in rows:
        key = (r["market"], r["code"])
        if key in uniq:
            prev = uniq[key]
            for k, v in r.items():
                if prev.get(k) is None and v is not None:
                    prev[k] = v
        else:
            uniq[key] = r
    payload = [tuple(r.get(c) for c in COLUMNS) for r in uniq.values()]

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, payload, page_size=500)
    conn.commit()
    return len(payload)


def print_status():
    conn = psycopg2.connect(DATABASE_URL)
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT market,
                   COUNT(*)                        AS total,
                   COUNT(apply_date)               AS with_apply,
                   COUNT(listing_date)             AS with_listing,
                   COUNT(raise_amount)             AS with_raise,
                   MIN(apply_date)::text           AS earliest,
                   MAX(COALESCE(listing_date, apply_date))::text AS latest
            FROM ipo_calendar
            GROUP BY market ORDER BY market
        """)
        print(f"{'market':<8}{'total':>8}{'apply':>8}{'listing':>9}{'raise':>8}  {'earliest':<12}{'latest'}")
        for r in cur.fetchall():
            print(f"{r[0]:<8}{r[1]:>8}{r[2]:>8}{r[3]:>9}{r[4]:>8}  {str(r[5])[:10]:<12}{str(r[6])[:10]}")
    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="新股日历抓取（A股 / 北交所 / 港股）")
    ap.add_argument("--dry-run", action="store_true", help="只抓取并打印，不写库")
    ap.add_argument("--full",    action="store_true", help="港股东财翻全部 52 页")
    ap.add_argument("--status",  action="store_true", help="只打印库内状态")
    args = ap.parse_args()

    if args.status:
        print_status()
        return 0

    all_rows: list[dict] = []

    print("── 1/4 A股 + 北交所（东财新股申购与中签）────────────────────────")
    try:
        a_rows = fetch_a_share()
        print(f"  ✔ {len(a_rows)} 条")
        all_rows += a_rows
    except Exception as exc:
        print(f"  ✘ 失败: {exc}")

    hk_sources: list[list[dict]] = []

    print("── 2/4 港股·即将招股（财华社 IPO Center）────────────────────────")
    try:
        chsu = fetch_hk_chsu()
        print(f"  ✔ {len(chsu)} 条")
        hk_sources.append(chsu)
    except Exception as exc:
        print(f"  ✘ 失败: {exc}")

    print("── 3/4 港股·招股截止 + 暗盘（AAStocks 新股频道）──────────────────")
    try:
        aa = fetch_hk_aastocks()
        print(f"  ✔ {len(aa)} 条")
        hk_sources.append(aa)
    except Exception as exc:
        print(f"  ✘ 失败: {exc}")

    print(f"── 4/4 港股·已上市 + 募集资金（东财，{'全量 52 页' if args.full else '第 1 页'}）──")
    try:
        em = fetch_hk_eastmoney(full=args.full)
        print(f"  ✔ {len(em)} 条")
        hk_sources.append(em)
    except Exception as exc:
        print(f"  ✘ 失败: {exc}")

    if hk_sources:
        hk_rows = merge_hk(*hk_sources)
        print(f"  → 港股合并去重后 {len(hk_rows)} 条")
        all_rows += hk_rows

    if not all_rows:
        print("  [FATAL] 所有数据源均失败，未写入任何数据")
        return 1

    # 预览
    print(f"\n── 抓取合计 {len(all_rows)} 条 ──")
    head = sorted(all_rows,
                  key=lambda r: (r.get("listing_date") or r.get("apply_date") or date.min),
                  reverse=True)[:12]
    for r in head:
        print(f"  {r['market']:<5}{r['code']:<8}{r['name'][:12]:<14}"
              f"价={str(r['issue_price'] or '—'):<8}募资={str(r['raise_amount'] or '—'):<9}"
              f"申购={r['apply_date'] or '—'}  上市={r['listing_date'] or '—'}")

    if args.dry_run:
        print("\n  [dry-run] 未写库")
        return 0

    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接数据库: {exc}")
        return 1

    try:
        ensure_schema(conn)
        n = upsert(conn, all_rows)
        print(f"\n  ✔ 已 upsert {n} 条")
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
