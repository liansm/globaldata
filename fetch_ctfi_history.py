#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CTFI 历史回补 —— 从中华航运网（上海航运交易所授权转载）的报告栏目取数。

为什么需要这个脚本
------------------
上航所官网 `sse.net.cn/index/singleIndex?indexType=ctfi` **只给当期值**，历史是登录墙：
  * 任何带 `date` 的请求（GET / POST+CSRFToken）响应体字节级完全相同（26731 字节、0 个数值）
    → 服务端忽略 date；
  * 多期端点 `/index/mutipleIndex` 参数名是 `startDate`+`endDate`，传对后仍回
    `{"success":false,"message":"对不起你没有登陆!"}`。
官方 FAQ（www1.sse.net.cn/index/faqnew.jsp）明确：
  * 非订购用户**不能查询历史数据**；
  * 订购用户只能查**最近 3 年**，一般用户年费 **1.5 万元 / 指数**（订第二个起 6 折）。
  → 付费也拿不到 2013-11-28（CTFI 发布日）以来的全历史，且成本高。

本脚本走的是**免费公开的第二条路**：上海航运交易所信息部在中华航运网发布的
  * 周报《中国外贸进口油轮运输市场周度报告》  栏目 /Tanker/CTFIWeek/
  * 月报《国际油轮运输市场月度报告》          栏目 /Tanker/CTFIMon/
报告正文里直接引用官方值，且给出了航线级 WS/TCE。

覆盖深度（实测 2026-09-30）
---------------------------
  * 周报：栏目 4 页 × 15 条 = 60 篇，期号 **2025-07-17 ~ 2026-09-23**（约 14 个月）
  * 月报：栏目 4 页 × 15 条 = 60 篇，期号 **2021-09 ~ 2026-08**（约 5 年）
  * 更早的月报在搜索引擎里有索引（2024-04 等），但栏目翻页到 4 页为止
    → 想再往前需要另找枚举手段（报告 ID 近似单调递增，可窄窗扫描，未做）

⚠⚠ 口径分层（**本脚本最要紧的部分**，混拼会直接产出错数据）
------------------------------------------------------------
报告里出现的数值分两类，**绝不能写进同一个序列**：

【A 类 · 期末值 / 当日官方值】——与官网日频值**同口径**，可直接写入既有 `ctfi_*` 序列：
  * 周报 `X月Y日，上海航运交易所发布的中国进口原油综合指数（CTFI）报 Z 点`  → `ctfi_total`
  * 周报 `（CT1）报WS A`                                                     → `ctfi_ct1_ws`
  * 周报 `（CT2）报WS B`                                                     → `ctfi_ct2_ws`
  * 月报 `X月Y日，…（CTFI）报 Z 点`（当月最后发布日）                          → `ctfi_total`
  * 月报 `（CT4）包干运费月底收于 C 万美元`                                   → `ctfi_ct4`

【B 类 · 期间均值】——**不同口径，一律写入独立 `*_month_avg` / `*_week_avg` 序列**：
  * 月报 `月平均 X 点`、`月平均WS X`、`TCE月平均 X 万美元/天`、`包干运费月平均 X 万美元`
  * 周报 `CT1的N日平均为WS X`、`TCE平均 X 万美元/天`
  这些是**区间均值**，和日频点值放在一起画图会失真（会把波动抹平后再拼接到点值上）。

单位换算（B 类与 A 类都要注意）
  * 报告写「万美元 / 天」→ 落库统一按 `美元/天` × 10000（与官网 `_tce_std` 一致）
  * 报告写「万美元」（CT4 包干运费）→ 与官网 `ctfi_ct4` 的「万美元」一致，**原样存**
  * 负 TCE 是真实存在的（2021-09 报「平均-9美元/天」）→ 不做 0 截断

日期语义
  * A 类：用**实际发布日**（报告里写明的「X月Y日」；周报用期号日期）。
    报告只写「月底报」（无具体日）时**跳过不猜**——月末最后一天未必是工作日，宁缺勿错。
  * B 类：用**该月最后一天**作为期间标签（key 名与 name 已写明「月均」，无歧义）。

⚠ 增量模式的栏目日期语义陷阱（踩过，实测会静默丢整列）
  两个栏目的标题日期语义不同：周报=报告日（2026.09.23），月报=**月首**（2026.09 → 2026-09-01）。
  若用「全局最大日期 - N 天」当唯一 cutoff：日频序列每天滚动，cutoff 一直往前推，
  而月报标签恒为 M-01，**永远早于 cutoff** → 月报列每次都被整列滤掉、且不报错。
  （实测：2026-09-29 跑增量，cutoff=2026-08-15，8 月月报标签 2026-08-01 < cutoff → 0 篇命中。）
  修法：按栏目分别锚定 —— 各用「本栏目独有序列」的库内最大日期做基准
  （`WEEK_ANCHOR_KEYS` / `MONTH_ANCHOR_KEYS`，见定义处），缺口可自愈；
  该栏目库里为空时不裁该栏目（等价全量抓该栏目）。

⚠ 已知源侧数据错误（**保留原值，不擅自修**，用 `--verify` 可复现）
  判据：月报月均 ÷ 同月「周报点值 + 月报月末点」的均值。该月观测点 ≥5 个时才有判别力
  （周报栏目每月只有约 4 篇，必须把月末点算进来才够；n<5 时点值均值本身就是差代理，
   比值会散到 0.84~1.25，是抽样噪声不是错误）。
  实测 n≥5 的 12 个月比值全落在 0.93~1.04，**唯一离群：2026-03 月报 `月平均3494.40点`（比值 1.562）**。
  三条独立证据指向真值 ≈5500：
    ① 该月各周点值 6906.71 / 6396.15 / 5081.38 / 4471.99 / 4435.52（月末）→ 均值 5458
       （当月的**最低**点 4436 都高于 3494，单调下行序列的均值不可能是 3494）
    ② 次月（2026-04）月报自述「环比下跌15.2%」→ 反推 3 月月均 = 4716.66/0.848 ≈ 5562
    ③ 该篇「月平均3494.40点，**环比上涨15.6%**」与同句「较上月末**上涨15.6%**」数字**完全相同**
       —— 其余月份这两个数从来不同，像是把月末点涨幅误抄到了月均环比上
  处理：**原样入库**（这是上航所公开发布值，本脚本不是权威、不替换），
  离群事实写入 DATASOURCES.md「待拍板」，由使用者决定是否修正。

已知死路（不要重复踩）
  * `sse.net.cn/index/ctfilist`：是「时间(YYYY-MM-DD) 查询」空表页，无数据
  * `sse.net.cn/index/indexImg`：200 但 0 字节
  * `www1.chineseshipping.com.cn/cn/index/ctfinew.jsp`：403；`/resource/DailyUpdate/ctfiimg.png` 404
  * `index.sse.net.cn`：指数报送系统，需凭据登录
  * akshare：无任何 ccfi/ctfi 接口（只有 macro_china_freight_index / macro_china_bdti_index）
  * Wayback Machine：本机网络下 403 / 代理 502，取不到归档快照

用法
----
    python fetch_ctfi_history.py --dry-run     # 只解析并打印，不写库
    python fetch_ctfi_history.py               # 增量写库（默认）
    python fetch_ctfi_history.py --full        # 全量重抓（120 篇）
    python fetch_ctfi_history.py --verify      # 相邻期环比自洽性核验（不写库）
    python fetch_ctfi_history.py --status      # 查库内各序列覆盖

合规：中华航运网为上海航运交易所主办，报告为公开转载；本脚本仅作本地研究用，
      限速 0.25s/请求，不并发、不重放。
"""
import argparse
import json
import os
import re
import sys
import time
from calendar import monthrange
from datetime import date, datetime, timedelta
from urllib.parse import urljoin

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Accept-Language": "zh-CN,zh;q=0.9"}

BASE = "https://www.chineseshipping.com.cn/cninfo/marketreport/Tanker/"
COLUMNS = [("week", BASE + "CTFIWeek/"), ("month", BASE + "CTFIMon/")]
LIST_PAGE0 = "index.shtml"
LIST_PAGE_N = "index_{}.shtml"          # index_1.shtml / index_2.shtml ...
REQ_SLEEP = 0.25

# ---------------------------------------------------------------------------
# 指标定义
# ---------------------------------------------------------------------------
# A 类：写进既有日频序列（同口径）
SAME_CALIBER_KEYS = ["ctfi_total", "ctfi_ct1_ws", "ctfi_ct2_ws", "ctfi_ct4"]

# B 类：期间均值，独立序列 (key, name, unit)
AVG_KEYS = [
    ("ctfi_month_close",             "CTFI 综合指数 月末值（中华航运网月报）", "点"),
    ("ctfi_month_avg",               "CTFI 综合指数 月均（中华航运网月报）", "点"),
    ("ctfi_ct1_ws_month_avg",        "CTFI CT1 中东→宁波 WS 月均",          "WS"),
    ("ctfi_ct2_ws_month_avg",        "CTFI CT2 西非→宁波 WS 月均",          "WS"),
    ("ctfi_ct4_usd_month_avg",       "CTFI CT4 美湾→宁波 包干运费 月均",     "万美元"),
    ("ctfi_ct1_tce_std_month_avg",   "CTFI CT1 TCE 月均（标准航速）",        "美元/天"),
    ("ctfi_ct2_tce_std_month_avg",   "CTFI CT2 TCE 月均（标准航速）",        "美元/天"),
    ("ctfi_ct4_tce_std_month_avg",   "CTFI CT4 TCE 月均（标准航速）",        "美元/天"),
    ("ctfi_ct1_ws_week_avg",         "CTFI CT1 WS 期均（周报口径）",         "WS"),
    ("ctfi_ct2_ws_week_avg",         "CTFI CT2 WS 期均（周报口径）",         "WS"),
    ("ctfi_ct1_tce_std_week_avg",    "CTFI CT1 TCE 期均（周报口径）",        "美元/天"),
    ("ctfi_ct2_tce_std_week_avg",    "CTFI CT2 TCE 期均（周报口径）",        "美元/天"),
]
ALL_EXTRA_KEYS = SAME_CALIBER_KEYS + [k for k, _, _ in AVG_KEYS]

# 增量锚定分组。⚠ 两个栏目的日期语义不同，绝不能共用一个 cutoff：
#   周报标题为报告日（2026.09.23）→ 标签≈数据日
#   月报标题为月首（2026.09）→ 标签=当月 1 日，数据落在月末，且发布滞后约 1 个月
# 若用「全局最大日期 - 45d」当唯一 cutoff，月报标签恒为 M-01，
# 在以日频序列（每天滚动）锚定时会永远早于 cutoff → 月报列被整列静默滤掉。
# 故按栏目各自锚定：各用「本栏目独有序列」的库内最大日期做基准，缺口可自愈。
WEEK_ANCHOR_KEYS = [
    "ctfi_ct1_ws_week_avg", "ctfi_ct2_ws_week_avg",
    "ctfi_ct1_tce_std_week_avg", "ctfi_ct2_tce_std_week_avg",
]
MONTH_ANCHOR_KEYS = [
    "ctfi_month_close", "ctfi_month_avg",
    "ctfi_ct1_ws_month_avg", "ctfi_ct2_ws_month_avg", "ctfi_ct4_usd_month_avg",
    "ctfi_ct1_tce_std_month_avg", "ctfi_ct2_tce_std_month_avg",
    "ctfi_ct4_tce_std_month_avg",
]
# 栏目回看窗口（天）。周报：45d 足够；月报：标签落后数据约 1 个月 + 发布延迟，取 100d。
KIND_LOOKBACK = {"week": 45, "month": 100}
KIND_ANCHOR_KEYS = {"week": WEEK_ANCHOR_KEYS, "month": MONTH_ANCHOR_KEYS}

# 既有日频序列的单位（写库前统一）
SAME_CALIBER_UNIT = {
    "ctfi_total": "点", "ctfi_ct1_ws": "WS", "ctfi_ct2_ws": "WS", "ctfi_ct4": "万美元",
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS market_indices (
    key        VARCHAR(60)   PRIMARY KEY,
    symbol     VARCHAR(60)   NOT NULL,
    name       VARCHAR(200)  NOT NULL,
    market     VARCHAR(50)   NOT NULL,
    unit       VARCHAR(50),
    updated_at TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS index_prices (
    id         BIGSERIAL     PRIMARY KEY,
    index_key  VARCHAR(60)   NOT NULL REFERENCES market_indices(key) ON DELETE CASCADE,
    price_date DATE          NOT NULL,
    open       NUMERIC(16,4),
    high       NUMERIC(16,4),
    low        NUMERIC(16,4),
    close      NUMERIC(16,4),
    volume     NUMERIC(24,4),
    turnover   NUMERIC(24,4),
    UNIQUE (index_key, price_date)
);
CREATE INDEX IF NOT EXISTS idx_index_prices_key_date
    ON index_prices (index_key, price_date DESC);
"""

UPSERT_INDEX_SQL = """
INSERT INTO market_indices (key, symbol, name, market, unit, updated_at)
VALUES (%s, %s, %s, %s, %s, NOW())
ON CONFLICT (key) DO UPDATE SET
    symbol = EXCLUDED.symbol, name = EXCLUDED.name,
    market = EXCLUDED.market, unit = EXCLUDED.unit, updated_at = NOW()
"""

UPSERT_PRICES_SQL = """
INSERT INTO index_prices (index_key, price_date, close)
VALUES %s
ON CONFLICT (index_key, price_date) DO UPDATE SET close = EXCLUDED.close
"""


def db_connect():
    url = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
    conn = psycopg2.connect(url)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def http_get(session, url, retries=3):
    last = None
    for i in range(retries):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r
            last = f"HTTP {r.status_code}"
        except Exception as exc:                       # noqa: BLE001
            last = f"{type(exc).__name__}: {exc}"
        time.sleep(1.0 + i)
    raise RuntimeError(f"取数失败 {url} → {last}")


def to_text(html):
    """去 script/style/标签，压空白，得到便于正则的纯文本"""
    t = re.sub(r"<script.*?</script>", " ", html, flags=re.S | re.I)
    t = re.sub(r"<style.*?</style>", " ", t, flags=re.S | re.I)
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"\s+", " ", t).strip()


# ---------------------------------------------------------------------------
# 枚举文章
# ---------------------------------------------------------------------------
ITEM_RE = re.compile(r'newstextbt"><a href="([^"]+)"[^>]*>(.*?)</a>')
PAGE_TOTAL_RE = re.compile(r"共\s*(\d+)\s*页")
TITLE_WEEK_RE = re.compile(r"（(\d{4})\.(\d{1,2})\.(\d{1,2})）")
TITLE_MON_RE = re.compile(r"（(\d{4})\.(\d{1,2})）")


def enumerate_articles(session, kind, column_url):
    """翻完栏目所有页，返回 [(kind, period_date, url, title)]，period 降序"""
    first = http_get(session, column_url + LIST_PAGE0)
    first.encoding = "utf-8"
    total = PAGE_TOTAL_RE.search(first.text)
    pages = int(total.group(1)) if total else 1

    out, seen = [], set()
    for pg in [LIST_PAGE0] + [LIST_PAGE_N.format(i) for i in range(1, pages)]:
        r = first if pg == LIST_PAGE0 else http_get(session, column_url + pg)
        r.encoding = "utf-8"
        for rel, title in ITEM_RE.findall(r.text):
            title = to_text(title)
            m = TITLE_WEEK_RE.search(title) if kind == "week" else TITLE_MON_RE.search(title)
            if not m:
                continue
            if kind == "week":
                period = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            else:
                period = date(int(m.group(1)), int(m.group(2)), 1)   # 月报：月首作期间锚
            url = urljoin(column_url + pg, rel)
            if url in seen:
                continue
            seen.add(url)
            out.append((kind, period, url, title))
        if pg != LIST_PAGE0:
            time.sleep(REQ_SLEEP)
    out.sort(key=lambda x: x[1], reverse=True)
    return out


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------
# A 类 CTFI 期末值：'9月23日，上海航运交易所发布的中国进口原油综合指数（CTFI）报15681.93点'
#                   '…运价综合指数(CTFI)月底报677.40点'
CTFI_CLOSE_RE = re.compile(r"CTFI[)）]?[\s]*(?:月底|月末)?\s*报\s*([\d,]+\.?\d*)\s*点")
# 含 CTFI 的那一整句（用于取报告自述的环比/同比）
CTFI_SENT_RE = re.compile(r"上海航运交易所发布[^。]{0,240}。")
CHG_RE = re.compile(r"(上涨|下跌|下降|上升|持平)\s*([\d.]+)%")
# 紧贴在「上海航运交易所发布」前的发布日期
PUB_DATE_RE = re.compile(r"(\d{1,2})月(\d{1,2})日[，,、]?\s*上海航运交易所发布[^。]{0,60}$")
MONTH_AVG_RE = re.compile(r"月平均\s*([\d,]+\.?\d*)\s*点")
# 「月平均…，环比上涨 x%」——月均自己的环比（不要误取同比）
AVG_CHG_RE = re.compile(r"月平均[^。]{0,40}?环比\s*(上涨|下跌|下降|持平)\s*([\d.]+)%")

ROUTE_BRACKET = r"[（(]{}[)）]"
ROUTE_MARK_RE = re.compile(r"[（(](CT[124])[)）]")


def route_segments(text):
    """
    按「（CTn）」标记把正文划窗，返回 {code: [window, ...]}。

    为什么不用切句：航线描述经常跨句，例如
      『…（CT1）从月初WS37.5一路阴跌至下旬WS35.5。月底反弹至WS38，月平均WS36.4（…）』
    按 。切句会把「月平均」切到另一段去，导致漏解析。
    同一航线可能出现多次（叙述段 + 数据段），全部保留，取值时取第一个命中的窗口。
    """
    marks = [(m.start(), m.group(1)) for m in ROUTE_MARK_RE.finditer(text)]
    segs = {}
    for i, (pos, code) in enumerate(marks):
        end = marks[i + 1][0] if i + 1 < len(marks) else min(len(text), pos + 600)
        segs.setdefault(code, []).append(text[pos:end])
    return segs


def _f(s):
    return float(str(s).replace(",", ""))


def parse_article(text, kind, period):
    """返回 (fields, warnings)。fields 的 key 与目标序列一一对应。"""
    f, warn = {}, []

    # ---- A 类：CTFI 当期/期末值 ----
    m = CTFI_CLOSE_RE.search(text)
    # CTFI 那一整句：月均 / 环比 等所有 CTFI 相关字段都必须在这句里找。
    # ⚠ 整篇搜「月平均」会撞上正文里其它运价的「月平均环比」（实测 2025.09 被取成 0.2%，
    #   正确值是 49.5%），务必限定在这句。
    sent = CTFI_SENT_RE.search(text)
    scope = sent.group(0) if sent else text
    if m:
        val = _f(m.group(1))
        pub = PUB_DATE_RE.search(text[:m.start()])
        if pub:
            f["_ctfi_close_date"] = date(period.year, int(pub.group(1)), int(pub.group(2)))
        elif kind == "week":
            f["_ctfi_close_date"] = period          # 周报期号日期=发布日
        else:
            warn.append("CTFI 期末值无明确日期（只写『月底报』）→ 日频 ctfi_total 跳过，"
                        "值仍进月末序列 ctfi_month_close，不猜日期")
        f["_ctfi_close"] = val
        # 报告自述环比/同比（仅用于 --verify 核验，不入库）
        if sent:
            chgs = CHG_RE.findall(scope)
            if chgs:
                f["_chg_pct"] = (-1 if chgs[0][0] in ("下跌", "下降") else 1) * _f(chgs[0][1])
            if len(chgs) > 1:
                f["_yoy_pct"] = (-1 if chgs[-1][0] in ("下跌", "下降") else 1) * _f(chgs[-1][1])
    else:
        warn.append("未解析到 CTFI 期末值")

    # ---- B 类：CTFI 月均 ----
    if kind == "month":
        m = MONTH_AVG_RE.search(scope)
        if m:
            f["ctfi_month_avg"] = _f(m.group(1))
            mc = AVG_CHG_RE.search(scope)      # 月均自述环比，仅用于 --verify 核验
            if mc:
                f["_avg_chg_pct"] = (-1 if mc.group(1) in ("下跌", "下降") else 1) * _f(mc.group(2))
        else:
            warn.append("未解析到 CTFI 月平均")

    # ---- 按航线标记切片：每个航线的参数只在自己的窗口里找，避免跨航线串值 ----
    # 注意：航线句可能跨句（『…阴跌至下旬WS35.5。月底反弹…月平均WS36.4』），
    # 所以不能按 。/； 切句后再匹配，必须以「（CTn）」标记的位置划窗。
    segs = route_segments(text)

    for code, wkey, ws_avg_week, tce_week, ws_avg_mon, tce_mon in (
        ("CT1", "ctfi_ct1_ws", "ctfi_ct1_ws_week_avg", "ctfi_ct1_tce_std_week_avg",
         "ctfi_ct1_ws_month_avg", "ctfi_ct1_tce_std_month_avg"),
        ("CT2", "ctfi_ct2_ws", "ctfi_ct2_ws_week_avg", "ctfi_ct2_tce_std_week_avg",
         "ctfi_ct2_ws_month_avg", "ctfi_ct2_tce_std_month_avg"),
    ):
        windows = segs.get(code, [])
        if not windows:
            warn.append(f"正文无 {code} 段落" + ("（该期尚未发布此航线）" if code == "CT4" else ""))
            continue

        def find(pat):
            for w in windows:
                m = re.search(pat, w)
                if m:
                    return m
            return None

        if kind == "week":
            m = find(r"报\s*WS\s*([\d.]+)")
            if m:
                f[wkey] = _f(m.group(1))                       # A 类：当日值
            else:
                warn.append(f"{code} 未解析到『报WS x』")
            m = find(r"平均\s*(?:为)?\s*WS\s*([\d.]+)")
            if m:
                f[ws_avg_week] = _f(m.group(1))
            else:
                warn.append(f"{code} 未解析到期均 WS")
            m = find(r"TCE\s*平均\s*(-?[\d.]+)\s*万美元/天")
            if m:
                f[tce_week] = _f(m.group(1)) * 10000          # 万美元/天 → 美元/天
            else:
                warn.append(f"{code} 未解析到期均 TCE")
        else:
            m = find(r"月平均\s*WS\s*([\d.]+)")
            if not m:
                m = find(r"平均\s*WS\s*([\d.]+)")
            if m:
                f[ws_avg_mon] = _f(m.group(1))
            else:
                warn.append(f"{code} 未解析到月平均 WS")
            # TCE：新格式「（TCE）月平均 x 万美元/天」，CT2/CT4 常无括号；旧格式「TCE平均 370 美元/天」
            m = find(r"TCE[)）]?\s*(?:月)?平均\s*(-?[\d.]+)\s*万美元/天")
            if m:
                f[tce_mon] = _f(m.group(1)) * 10000
            else:
                m = find(r"TCE[)）]?[^。]{0,80}?平均\s*(-?[\d.]+)\s*美元/天")
                if m:
                    f[tce_mon] = _f(m.group(1))               # 旧格式，已是 美元/天
                else:
                    warn.append(f"{code} 未解析到月均 TCE")

    # ---- CT4（仅月报，且 2024 年才加入航线）----
    if kind == "month":
        windows = segs.get("CT4", [])
        if not windows:
            warn.append("该期报告无 CT4 段落（CT4 航线为后加，非解析失败）")
        else:
            m = re.search(r"月底收于\s*([\d,]+\.?\d*)\s*万美元", windows[0])
            if m:
                f["ctfi_ct4"] = _f(m.group(1))                 # A 类：月末值，与官网同口径
            m = re.search(r"月平均\s*([\d,]+\.?\d*)\s*万美元", windows[0])
            if m:
                f["ctfi_ct4_usd_month_avg"] = _f(m.group(1))
            else:
                warn.append("CT4 未解析到包干运费月平均")
            m = re.search(r"TCE[)）]?\s*(?:月)?平均\s*(-?[\d.]+)\s*万美元/天", windows[0])
            if m:
                f["ctfi_ct4_tce_std_month_avg"] = _f(m.group(1)) * 10000

    return f, warn


def period_label(kind, period):
    """B 类期间标签：月报取该月最后一天"""
    if kind == "month":
        return date(period.year, period.month, monthrange(period.year, period.month)[1])
    return period


# ---------------------------------------------------------------------------
# 抓取 + 解析
# ---------------------------------------------------------------------------
def harvest(session, articles, verbose=True, save_raw=None, from_raw=None):
    """抓取 + 解析。save_raw 落原文缓存；from_raw 直接从缓存解析（不联网）。"""
    import hashlib
    records, failures = [], []
    index = []
    if from_raw:
        os.makedirs(from_raw, exist_ok=True)
    if save_raw:
        os.makedirs(save_raw, exist_ok=True)

    for i, (kind, period, url, title) in enumerate(articles, 1):
        fn = f"{kind}_{period.isoformat()}_{hashlib.md5(url.encode()).hexdigest()[:8]}.txt"
        index.append({"file": fn, "kind": kind, "period": period.isoformat(),
                      "url": url, "title": title})
        cache = os.path.join(from_raw, fn) if from_raw else None
        try:
            if cache:
                if not os.path.exists(cache):
                    raise FileNotFoundError(f"缓存缺失 {cache}")
                with open(cache, encoding="utf-8") as fh:
                    text = fh.read()
            else:
                r = http_get(session, url)
                r.encoding = "utf-8"
                text = to_text(r.text)
                if save_raw:
                    with open(os.path.join(save_raw, fn), "w", encoding="utf-8") as fh:
                        fh.write(text)
            fields, warn = parse_article(text, kind, period)
            records.append({"kind": kind, "period": period.isoformat(), "title": title,
                            "url": url, "fields": fields, "warnings": warn})
            if verbose and warn:
                print(f"  [{i:3d}/{len(articles)}] ⚠ {title}")
                for w in warn:
                    print(f"          · {w}")
        except Exception as exc:                            # noqa: BLE001
            failures.append((url, f"{type(exc).__name__}: {exc}"))
            print(f"  [{i:3d}/{len(articles)}] ✗ {url} → {exc}")
        if not from_raw:
            time.sleep(REQ_SLEEP)
    if save_raw:
        with open(os.path.join(save_raw, "_index.json"), "w", encoding="utf-8") as fh:
            json.dump(index, fh, ensure_ascii=False, indent=1)
    return records, failures


def load_raw_index(raw_dir):
    """从 --save-raw 的缓存目录重建文章清单，供 --from-raw 免联网重跑解析"""
    p = os.path.join(raw_dir, "_index.json")
    if not os.path.exists(p):
        raise SystemExit(f"{p} 不存在：--from-raw 需要先跑过一次 --save-raw")
    with open(p, encoding="utf-8") as fh:
        return [(x["kind"], date.fromisoformat(x["period"]), x["url"], x["title"])
                for x in json.load(fh)]


# ---------------------------------------------------------------------------
# 组装写入行
# ---------------------------------------------------------------------------
def build_rows(records):
    """→ (rows, stats)；rows = [(key, date, value)]"""
    rows, stats = [], {}
    filled = {}          # (key, date) → 来源，用于查重

    def put(key, d, v):
        if v is None or d is None:
            return
        rows.append((key, d, v))
        filled.setdefault((key, d), []).append(key)

    for rec in records:
        f = rec["fields"]
        d = f.get("_ctfi_close_date")
        if f.get("_ctfi_close") is not None and d is not None:
            put("ctfi_total", d, f["_ctfi_close"])
            stats["ctfi_total"] = stats.get("ctfi_total", 0) + 1
        # 月报的月末值：即使报告只写「月底报」而没写具体日，也用月末标签收进独立序列，
        # 这样月频序列不漏期（日频 ctfi_total 仍只收有明确发布日的点，不猜日期）
        if rec["kind"] == "month" and f.get("_ctfi_close") is not None:
            lbl_m = period_label("month", date.fromisoformat(rec["period"]))
            put("ctfi_month_close", lbl_m, f["_ctfi_close"])
            stats["ctfi_month_close"] = stats.get("ctfi_month_close", 0) + 1
        if d is not None:
            for code, wkey in (("CT1", "ctfi_ct1_ws"), ("CT2", "ctfi_ct2_ws")):
                if f.get(wkey) is not None:
                    put(wkey, d, f[wkey])
                    stats[wkey] = stats.get(wkey, 0) + 1
        if f.get("ctfi_ct4") is not None and d is not None:
            put("ctfi_ct4", d, f["ctfi_ct4"])
            stats["ctfi_ct4"] = stats.get("ctfi_ct4", 0) + 1

        lbl = period_label(rec["kind"], date.fromisoformat(rec["period"]))
        for key, _, _ in AVG_KEYS:
            if f.get(key) is not None:
                put(key, lbl, f[key])
                stats[key] = stats.get(key, 0) + 1
    return rows, stats


# ---------------------------------------------------------------------------
# 核验：相邻期环比自洽
# ---------------------------------------------------------------------------
def verify(records):
    """
    用报告自己写的『较上期/上月末 上涨 x%』去校验相邻两期的实测值。

    这是本脚本唯一能做的交叉核验：上航所官网不给历史，拿不到同一天的第二来源，
    所以改为验证「120 篇独立文档之间的数值链是否自洽」——
    若解析错位，链条会立刻断掉。
    """
    print("\n=== 核验：报告自述环比 vs 相邻两期实测值之比 ===")
    print("    （偏差 = |前值×(1+自述涨幅) - 本期值| / 本期值，判定阈值 1.0%）")
    for kind, label in (("month", "月报"), ("week", "周报")):
        seq = []
        for rec in records:
            if rec["kind"] != kind:
                continue
            f = rec["fields"]
            if f.get("_ctfi_close") is None or f.get("_chg_pct") is None:
                continue
            seq.append((rec["period"], f["_ctfi_close"], f["_chg_pct"]))
        seq.sort()                                       # 旧 → 新
        bad, checked = [], 0
        for i in range(1, len(seq)):
            p0, v0, _ = seq[i - 1]
            p1, v1, chg = seq[i]
            checked += 1
            implied = v0 * (1 + chg / 100.0)
            dev = abs(implied - v1) / max(abs(v1), 1e-9) * 100
            if dev > 1.0:
                bad.append((p0, v0, p1, v1, chg, dev))
        print(f"\n  {label}：可校验 {checked} 对，一致 {checked - len(bad)} 对，"
              f"偏差>1% 的 {len(bad)} 对")
        for p0, v0, p1, v1, chg, dev in bad[:10]:
            print(f"    ⚠ {p0} {v0:>10,.2f}  →  {p1} {v1:>10,.2f}   "
                  f"自述 {chg:+.1f}%  推算 {v0 * (1 + chg / 100):>10,.2f}   偏差 {dev:.2f}%")

    # ---- 月均链自洽：月报的『月平均X点，环比上涨y%』应能串成一条链 ----
    # 2026-09-30 加这条检查时抓到真问题：2026-03 月报的月均 3494.40 与其自身 15.6% 的
    # 环比、以及 4 月月报反推的月均（~5562）、周报点值均值（~5465）都对不上 →
    # 是源侧那期报告的错误（它的月末环比与月均环比写成了同一个 15.6%，其余月份两者都不同）。
    print("\n=== 核验：月报『月平均』自述环比 vs 相邻两期月均之比 ===")
    seq = []
    for rec in records:
        if rec["kind"] != "month":
            continue
        f = rec["fields"]
        if f.get("ctfi_month_avg") is not None and f.get("_avg_chg_pct") is not None:
            seq.append((rec["period"], f["ctfi_month_avg"], f["_avg_chg_pct"]))
    seq.sort()
    checked, bad = 0, []
    for i in range(1, len(seq)):
        p0, v0, _ = seq[i - 1]
        p1, v1, chg = seq[i]
        checked += 1
        dev = abs(v0 * (1 + chg / 100.0) - v1) / max(abs(v1), 1e-9) * 100
        if dev > 1.0:
            bad.append((p0, v0, p1, v1, chg, dev))
    print(f"  可校验 {checked} 对，一致 {checked - len(bad)} 对，偏差>1% 的 {len(bad)} 对")
    for p0, v0, p1, v1, chg, dev in bad:
        print(f"    ⚠ {p0} {v0:>10,.2f}  →  {p1} {v1:>10,.2f}   "
              f"自述 {chg:+.1f}%  推算 {v0 * (1 + chg / 100):>10,.2f}   偏差 {dev:.2f}%")

    # ---- 跨系列核对：同期「周报点值均值」 vs 「月报月均」----
    # 两条线来自不同报告文档；若口径不一致（比如把期间均值误当日频点），量级会立刻偏离。
    print("\n=== 跨系列核对：该月内周报点值均值  vs  月报月均 ===")
    wk = {}
    mon_avg, mon_close = {}, {}
    for rec in records:
        f = rec["fields"]
        if rec["kind"] == "week" and f.get("_ctfi_close") is not None:
            wk.setdefault(rec["period"][:7], []).append(f["_ctfi_close"])
        if rec["kind"] == "month":
            if f.get("ctfi_month_avg") is not None:
                mon_avg[rec["period"][:7]] = f["ctfi_month_avg"]
            if f.get("_ctfi_close") is not None:
                mon_close[rec["period"][:7]] = f["_ctfi_close"]
    rows_out = []
    for ym in sorted(set(wk) & set(mon_avg)):
        # 组成该月的 CTFI 观测点：各周报点值 + 月报自带的月末点（也是真实观测，别浪费）
        pts = list(wk[ym])
        if mon_close.get(ym) is not None:
            pts.append(mon_close[ym])
        # ⚠ 样本量守卫：周报点值是「该周某一天」的瞬时值，月报月均是全月平均，
        #   两者存在系统偏差（月内趋势越陡越大）。**只有当月内有 ≥5 个观测点时**，
        #   点值均值才是个够用的月均代理 —— 实测 2026-09-30：
        #     n≥5 的 12 个月，比值落在 0.93~1.04（除 2026-03 的 1.56）
        #     n=4 的月份（如 2025-07/2026-02）也还稳，n=1~3 时会散到 0.84~1.25（抽样噪声）
        #   → 取 n≥5，低于此不参与判定，否则噪声会淹没真信号。
        #   （注意：周报栏目每月只有 4 篇上下，必须把月末点算进来才够 5 个。）
        if len(pts) < 5:
            continue
        wmean = sum(pts) / len(pts)
        a = mon_avg[ym]
        rows_out.append((ym, len(pts), wmean, a, (wmean / a - 1) * 100))
    if rows_out:
        devs = [abs(r[4]) for r in rows_out]
        print(f"  可比月份 {len(rows_out)} 个（仅计该月「周报点+月末点」≥5 的月份），"
              f"平均绝对偏差 {sum(devs)/len(devs):.2f}%，最大 {max(devs):.2f}%")
        print("  说明：两条线来自不同文档、口径本就不同（瞬时 vs 全月均），"
              "偏差在个位数百分比内属正常；")
        print("        若某月比值明显离群（>1.3 或 <0.7）= 该月月报月均或周报点值有误。")
        for ym, n, wm, a, dev in sorted(rows_out, key=lambda x: -abs(x[4]))[:6]:
            flag = "   <== 离群，需查源" if abs(dev) > 30 else ""
            print(f"    {ym}  周报点值均值(共{n}点) {wm:>9,.2f}   月报月均 {a:>9,.2f}   "
                  f"偏差 {dev:+.2f}%{flag}")
        outliers = [r for r in rows_out if abs(r[4]) > 30]
        if outliers:
            print(f"\n  ⚠ 检出 {len(outliers)} 个离群月 —— 已确认是**源侧错误**，非解析问题：")
            print("    2026-03 月报『月平均3494.40点』：① 该月各周点值 6906/6396/5081/4472/4436，"
                  "均值 5458；② 次月月报自述环比 -15.2% 反推约 5562；")
            print("    ③ 该篇『月平均环比上涨15.6%』与同篇『较上月末上涨15.6%』**字符串完全相同**"
                  "（其余月份两者不同）。")
            print("    → 三条独立证据一致指向真值 ≈5500。**本脚本保留原值不擅自修**，"
                  "已在 DATASOURCES.md 记档待拍板。")
    else:
        print("  无重叠月份可核（需该月「周报点+月末点」≥5）。")


# ---------------------------------------------------------------------------
# 写库
# ---------------------------------------------------------------------------
def write_db(conn, rows, full=False):
    if not rows:
        print("无可写入行。")
        return 0, 0
    # ⚠ 批内必须按 (key, date) 去重：ON CONFLICT 不允许同一条命令里重复命中同一行，
    #   否则整批回滚（psycopg2 CardinalityViolation）。这是本项目记过的老坑。
    #   重复时若两处数值不一致 → 是口径冲突，必须报出来，不能静默取一个。
    merged, conflicts = {}, []
    for k, d, v in rows:
        if (k, d) in merged and abs(merged[(k, d)] - v) > 1e-9:
            conflicts.append((k, d, merged[(k, d)], v))
        merged[(k, d)] = v
    rows = [(k, d, v) for (k, d), v in merged.items()]
    if conflicts:
        print(f"  ⚠ 批内有 {len(conflicts)} 个 (key,date) 由不同报告给出不同值（口径冲突需人工判断）：")
        for k, d, a, b in conflicts[:10]:
            print(f"     {k} {d}  {a:,.2f} vs {b:,.2f}")

    keys = sorted({r[0] for r in rows})
    name_of = {k: n for k, n, _ in AVG_KEYS}
    unit_of = {k: u for k, _, u in AVG_KEYS}

    ins_mask, skipped = [], 0
    with conn.cursor() as cur:
        # 增量：跳过已存在的 (key, date)
        if not full:
            cur.execute("""SELECT index_key, price_date FROM index_prices
                           WHERE index_key = ANY(%s)""", (keys,))
            existing = set(cur.fetchall())
            for k, d, v in rows:
                if (k, d) in existing:
                    skipped += 1
                    continue
                ins_mask.append((k, d, v))
        else:
            ins_mask = rows

        # 指标元数据
        for k in keys:
            if k in name_of:
                cur.execute(UPSERT_INDEX_SQL, (k, k.replace("ctfi_", "CTFI ").upper(),
                                               name_of[k], "航运", unit_of[k]))
            else:
                cur.execute("SELECT name, unit FROM market_indices WHERE key=%s", (k,))
                row = cur.fetchone()
                if not row:
                    print(f"  [WARN] 未知指标 {k}，既有序列缺失，跳过其数据")
        if ins_mask:
            from psycopg2.extras import execute_values
            execute_values(cur, UPSERT_PRICES_SQL, ins_mask)
    conn.commit()
    return len(ins_mask), skipped


def show_status(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT index_key, COUNT(*), MIN(price_date), MAX(price_date)
                       FROM index_prices WHERE index_key = ANY(%s)
                       GROUP BY 1 ORDER BY 1""", (ALL_EXTRA_KEYS,))
        rows = cur.fetchall()
    print("\n=== 库内 CTFI 相关序列 ===")
    seen = {r[0] for r in rows}
    for k in ALL_EXTRA_KEYS:
        hit = [r for r in rows if r[0] == k]
        if hit:
            _, n, lo, hi = hit[0]
            print(f"  {k:32s} n={n:5d}  {lo} ~ {hi}")
        else:
            print(f"  {k:32s} n=    0  (尚未写入)")
    return seen


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="CTFI 历史回补（中华航运网报告）")
    ap.add_argument("--dry-run", action="store_true", help="只解析打印，不写库")
    ap.add_argument("--full", action="store_true", help="全量重抓（默认增量：只抓新报告）")
    ap.add_argument("--verify", action="store_true", help="只做相邻期环比自洽核验")
    ap.add_argument("--verify-from-dump", metavar="PATH",
                    help="用已有 JSON（--dump 产物）做核验，不重新抓取")
    ap.add_argument("--status", action="store_true", help="查库内覆盖，不抓取")
    ap.add_argument("--dump", metavar="PATH", help="把解析结果写成 JSON（审计用）")
    ap.add_argument("--save-raw", metavar="DIR", help="把各篇正文缓存到目录（调解析器时免重抓）")
    ap.add_argument("--from-raw", metavar="DIR", help="从 --save-raw 的缓存解析，不联网")
    args = ap.parse_args()

    if args.status:
        show_status(db_connect())
        return

    if args.verify_from_dump:
        with open(args.verify_from_dump, encoding="utf-8") as fh:
            verify(json.load(fh))
        return

    session = requests.Session()

    print("[1/3] 枚举报告栏目（周报 / 月报）…")
    if args.from_raw:
        articles = load_raw_index(args.from_raw)
        print(f"  从原文缓存重建 {len(articles)} 篇（不联网）")
    else:
        articles = []
        for kind, col in COLUMNS:
            got = enumerate_articles(session, kind, col)
            print(f"  {kind:5s} {col.split('/Tanker/')[1]:12s} {len(got):3d} 篇  "
                  f"{got[-1][1] if got else '-'} ~ {got[0][1] if got else '-'}")
            articles += got
    articles.sort(key=lambda x: x[1], reverse=True)

    if not args.full and not args.verify:
        # 增量：按栏目分别锚定（各用本栏目独有序列的库内最大日期 - 回看窗口）。
        # 见 KIND_ANCHOR_KEYS 上方的说明：共用 cutoff 会静默滤掉整列月报。
        conn0 = db_connect()
        with conn0.cursor() as cur:
            anchors = {}
            for kind, keys in KIND_ANCHOR_KEYS.items():
                cur.execute("""SELECT MAX(price_date) FROM index_prices
                               WHERE index_key = ANY(%s)""", (keys,))
                anchors[kind] = cur.fetchone()[0]
        conn0.close()

        before = len(articles)
        kept = []
        for a in articles:
            kind, d = a[0], a[1]
            anchor = anchors.get(kind)
            # 该栏目在库里还没有任何数据 → 不裁（等价于全量抓该栏目）
            if anchor is None or d >= anchor - timedelta(days=KIND_LOOKBACK[kind]):
                kept.append(a)
        articles = kept
        desc = " / ".join(f"{k}={anchors.get(k) or '空'}" for k in KIND_ANCHOR_KEYS)
        print(f"  增量模式：栏目锚点 {desc} → 只抓 {len(articles)}/{before} 篇"
              f"（用 --full 抓全量）")

    print(f"\n[2/3] 抓取并解析 {len(articles)} 篇 …（限速 {REQ_SLEEP}s，预计 "
          f"{int(len(articles)*REQ_SLEEP)}s 以上）")
    records, failures = harvest(session, articles, verbose=True,
                               save_raw=args.save_raw, from_raw=args.from_raw)
    print(f"  完成：成功 {len(records)} 篇，失败 {len(failures)} 篇")

    if args.dump:
        with open(args.dump, "w", encoding="utf-8") as fh:
            json.dump([{**r, "period": r["period"],
                        "fields": {**r["fields"],
                                   **{k: str(v) for k, v in r["fields"].items()
                                      if isinstance(v, date)}}}
                       for r in records], fh, ensure_ascii=False, indent=1)
        print(f"  解析结果已 dump → {args.dump}")

    print("\n[3/3] 组装写入行 …")
    rows, stats = build_rows(records)
    print("  各序列解析命中数：")
    for k in ALL_EXTRA_KEYS:
        if stats.get(k):
            tag = "A 同口径→既有序列" if k in SAME_CALIBER_KEYS else "B 期间均值→独立序列"
            print(f"    {k:32s} {stats[k]:4d} 点   [{tag}]")
        else:
            print(f"    {k:32s}    0 点   [未解析到]")

    if args.verify:
        verify(records)
        return

    if args.dry_run:
        print("\n[DRY-RUN] 不写库。最新 8 行样例：")
        for k, d, v in sorted(rows, key=lambda x: (x[1], x[0]), reverse=True)[:8]:
            print(f"    {k:32s} {d}  {v:,.2f}")
        print(f"\n[DRY-RUN] 共 {len(rows)} 行未写入。")
        return

    conn = db_connect()
    written, skipped = write_db(conn, rows, full=args.full)
    print(f"\n[OK] 写入 {written} 行，跳过已存在 {skipped} 行")
    if failures:
        print(f"⚠ 有 {len(failures)} 篇抓取失败（未计入，重跑可补）：")
        for u, e in failures[:5]:
            print(f"    {u} → {e}")
    show_status(conn)
    conn.close()

    # 部分失败必须让 refresh_all 看见（它只看 exit code）：
    # 全成功 → 0；有失败 → 1，使汇总行标记为 fail，提示重跑补齐。
    if failures:
        print(f"\n[FAIL] {len(failures)}/{len(articles)} 篇未抓到 → 退出码 1，请重跑补齐")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
