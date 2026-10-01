#!/usr/bin/env python3
"""
CTFI Oil Tanker Freight Index Fetcher (中国进口原油运价指数)
=============================================================
抓取上海航运交易所的 **CTFI 中国进口原油运价指数** 及三条 VLCC 航线的
分项指标（指数点 / Worldscale / 美元每吨 / **TCE 等价期租租金 美元每天**），
写入既有的 market_indices / index_prices 表（market='航运'）。

这是跟踪 **招商轮船(601872) / 中远海能(600026)** 这类油运船东最直接的公开数据：
TCE（Time Charter Equivalent，等价期租租金）就是船东按天能赚多少钱，
是油运公司盈利的一阶代理变量。

Data source
-----------
    GET https://www.sse.net.cn/index/singleIndex?indexType=ctfi

服务端渲染，plain requests 即可，无需 JS 渲染、无需登录。

页面结构（2026-09 实测）
------------------------
    <div class="title2">
        <tr>中国进口原油运价指数 CHINA IMPORT CRUDE OIL TANKER FREIGHT INDEX</tr>
        <tr><br>2026-09-28</tr>          ← 数据日期在这里
    </div>

表格（class="lb1"）每行一条航线，首行用 rowspan 跨多条子指标行：

    航线                                载货量      船型   单位    权重   本期        与上期比涨跌
    综合指数                                                    点      100%   15526.88   -1.99
    中东湾拉斯坦努拉—中国宁波(CT1)      270000MT    VLCC   点      60%    18713.21   13.56
                                                           WS             1110.07    0.80
                                                           美元/吨        224.35     0.17
                                                           美元/天(标准航速) 1186617  66
                                                           美元/天(经济航速) 1149169  97
    西非马隆格/杰诺—中国宁波(CT2)       260000MT    VLCC   点      40%    10747.37   -25.34
    ...（同 CT1 的 4 个子指标）
    美湾 STS—中国宁波(CT4)              270000MT    VLCC   万美元  0%     5188.57    -67.43
    ...（无 WS 行）

⚠ **CT4 的「点」列单位其实是「万美元」**（运费总额），不是指数点 —— CT4 权重 0%、
  不参与综合指数计算，所以上航所给了它不同的量纲。落库时 unit 按源里的单位原样记，
  不要硬写成"点"。

Frequency & history
-------------------
* **每个工作日发布一次**，页面只给「本期」一个值（不像 CCFI 给上期+本期两期）。
* **历史回补是付费墙**（与 CCFI 同机理，但死法不同）：
    - 页面上有「指数查询（请输入指数日期）」输入框 —— 那是**登录用户**的入口。
      实测：任何带 `date` 的请求（GET 或 POST+CSRFToken；2026-09-25 / 2025-06-16 /
      2020-03-16 三个日期）**响应体字节级完全相同**（md5 34a73545、26731 字节，
      正则 `<td>数字</td>` 命中 0 个）→ 服务端直接忽略 date 参数，返回空表。
      即：不是解析错、不是 URL 拼错，是服务端不认匿名查询。
    - 多期端点 GET /index/mutipleIndex —— 参数名是 **startDate + endDate**
      （不是 startTime/start/beginDate，传错会回"起始时间不能为空!"），
      但参数正确后返回 `{"success":false,"message":"对不起你没有登陆!"}`。
  → 结论：**历史只能靠每个交易日运行本脚本累积**，跑漏的交易日永久缺失。
    这是本脚本必须进 refresh_all.py 每日刷新的原因。
    （CCFI 的历史是另找 GreenPacific 第三方源补的；CTFI 暂未找到等价第三方源。）

口径与验证
----------
* 落库只存「本期」值，**源的「与上期比涨跌」列一律丢弃** —— 它的单位随指标而变
  （指数是 %，TCE 是 美元/天 绝对值），存进去必然误导；涨跌幅由前端按相邻两期自算。
* 与独立源交叉验证：CTFI 综合指数(点) 与 BDTI 走势同向但量纲/基准不同
  （CTFI 基期以 2000 年某日 = 1000，BDTI 是波罗的海自己的基准），
  **不要拿两者互相"校验"数值**，只能对方向。

Compliance note
---------------
上海航运交易所声明其运价指数仅供浏览，未经书面许可不得转载、再分发或商用。
内部研究可用，对外发布前需另行确认。

Usage
-----
    python fetch_ctfi.py             # fetch + upsert（默认增量）
    python fetch_ctfi.py --dry-run   # 只抓取并打印，不写库
    python fetch_ctfi.py --full      # 忽略增量判断，全量重写本期
"""

import os
import re
import sys
from datetime import datetime, timezone, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

SSE_URL = "https://www.sse.net.cn/index/singleIndex?indexType=ctfi"
SSE_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9",
}
TIMEOUT = 25

MARKET = "航运"

# ---------------------------------------------------------------------------
# 航线配置
#   匹配依据：航线名里带 (CTx) 代码，比中文名稳定。
#   子指标匹配依据：第一列的「单位」文字（WS / 美元/吨 / 美元/天(标准航速) ...）。
# ---------------------------------------------------------------------------
ROUTE_CONFIGS = [
    {
        "code": "CT1",
        "key": "ctfi_ct1",
        "name": "CTFI 中东湾—中国宁波",
        "symbol": "CTFI-CT1",
        "index_unit": "点",
        "metrics": [
            ("WS",              "ws",      "WS"),
            ("美元/吨",           "usd_ton", "美元/吨"),
            ("美元/天(标准航速)",  "tce_std", "美元/天"),
            ("美元/天(经济航速)",  "tce_eco", "美元/天"),
        ],
    },
    {
        "code": "CT2",
        "key": "ctfi_ct2",
        "name": "CTFI 西非—中国宁波",
        "symbol": "CTFI-CT2",
        "index_unit": "点",
        "metrics": [
            ("WS",              "ws",      "WS"),
            ("美元/吨",           "usd_ton", "美元/吨"),
            ("美元/天(标准航速)",  "tce_std", "美元/天"),
            ("美元/天(经济航速)",  "tce_eco", "美元/天"),
        ],
    },
    {
        # ⚠ CT4 的指数列单位是「万美元」（运费总额），且没有 WS 行
        "code": "CT4",
        "key": "ctfi_ct4",
        "name": "CTFI 美湾STS—中国宁波",
        "symbol": "CTFI-CT4",
        "index_unit": "万美元",
        "metrics": [
            ("美元/吨",           "usd_ton", "美元/吨"),
            ("美元/天(标准航速)",  "tce_std", "美元/天"),
            ("美元/天(经济航速)",  "tce_eco", "美元/天"),
        ],
    },
]

ROUTE_BY_CODE = {c["code"]: c for c in ROUTE_CONFIGS}

TOTAL_CONFIG = {
    "key": "ctfi_total",
    "name": "CTFI 中国进口原油运价指数",
    "symbol": "CTFI",
    "unit": "点",
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
    symbol     = EXCLUDED.symbol,
    name       = EXCLUDED.name,
    market     = EXCLUDED.market,
    unit       = EXCLUDED.unit,
    updated_at = NOW()
"""

# 这些指标只有单值（本期），没有开高低/成交量
UPSERT_PRICES_SQL = """
INSERT INTO index_prices (index_key, price_date, close)
VALUES %s
ON CONFLICT (index_key, price_date) DO UPDATE SET
    close = EXCLUDED.close
"""

LATEST_DATE_SQL = """
SELECT index_key, MAX(price_date) FROM index_prices
WHERE index_key = ANY(%s) GROUP BY index_key
"""


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def _clean(cell: str) -> str:
    """去标签、压空白：'<p>日本航线</p>' → '日本航线'"""
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", cell)).strip()


def _safe_float(val) -> float | None:
    if val is None:
        return None
    s = str(val).replace(",", "").strip()
    if not s or s in ("-", "—", "--"):
        return None
    try:
        return round(float(s), 4)
    except (TypeError, ValueError):
        return None


def fetch_page() -> str:
    """抓 CTFI 单期页面（服务端渲染 HTML）。"""
    resp = requests.get(SSE_URL, headers=SSE_HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def parse_ctfi(html: str) -> dict:
    """
    解析 CTFI 页面。

    Returns
    -------
    {
      "data_date": "2026-09-28",
      "rows": [{"key": "ctfi_ct1_ws", "name": ..., "symbol": ..., "unit": "WS",
                "value": 1110.07, "src_chg": 0.80}, ...]
    }
    src_chg 是源的「与上期比涨跌」（仅用于日志校对，不入库）。

    Raises ValueError 当表格/日期无法定位时。
    """
    # ── 数据日期：<div class="title2"> ... <tr><br>YYYY-MM-DD</tr> ──────────
    m = re.search(
        r'<div class="title2">.*?<tr>\s*<br\s*/?>\s*(\d{4}-\d{2}-\d{2})\s*</tr>',
        html, re.S)
    if not m:
        m = re.search(r'<div class="title2">.*?(\d{4}-\d{2}-\d{2})', html, re.S)
    if not m:
        raise ValueError("未解析到数据日期（页面结构可能已变更）")
    data_date = m.group(1)

    # ── 表格：含「本期」的那张 ────────────────────────────────────────────
    table = None
    for tm in re.finditer(r"<table[^>]*>.*?</table>", html, re.S):
        if "本期" in tm.group(0):
            table = tm.group(0)
            break
    if table is None:
        raise ValueError("未找到含「本期」的数据表格")

    rows: list = []
    cur_route = None
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", table, re.S):
        cells = [_clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if not cells:
            continue

        # 表头行（7 列且首列是"航线"）
        if cells[0] == "航线":
            continue

        if len(cells) == 7:
            # 新块首行：航线名 | 载货量 | 船型 | 单位 | 权重 | 本期 | 涨跌
            label, _cap, _ship, unit, _weight, val, chg = cells
            value, src_chg = _safe_float(val), _safe_float(chg)

            if "综合" in label:
                cur_route = None
                if value is not None:
                    rows.append({
                        "key":    TOTAL_CONFIG["key"],
                        "name":   TOTAL_CONFIG["name"],
                        "symbol": TOTAL_CONFIG["symbol"],
                        "unit":   TOTAL_CONFIG["unit"],
                        "value":  value,
                        "src_chg": src_chg,
                    })
                continue

            cm = re.search(r"\((CT\d+)\)", label)
            cfg = ROUTE_BY_CODE.get(cm.group(1)) if cm else None
            if cfg is None:
                print(f"  [WARN] 未识别的航线，跳过整块: {label}")
                cur_route = None
                continue

            cur_route = cfg
            if value is not None:
                wm = re.search(r"([\d.]+)\s*%", _weight)
                rows.append({
                    "key":    cfg["key"],
                    "name":   cfg["name"],
                    "symbol": cfg["symbol"],
                    "unit":   cfg["index_unit"],   # ⚠ CT4 是"万美元"，原样记
                    "value":  value,
                    "src_chg": src_chg,
                    "weight": _safe_float(wm.group(1)) if wm else None,
                })

        elif len(cells) == 3:
            # 子指标行：单位标签 | 本期 | 涨跌
            if cur_route is None:
                continue
            mlabel, val, chg = cells
            metric = next((mm for mm in cur_route["metrics"] if mm[0] == mlabel), None)
            if metric is None:
                print(f"  [WARN] {cur_route['code']} 未识别的子指标: {mlabel}")
                continue
            value = _safe_float(val)
            if value is None:
                continue
            _, suffix, unit = metric
            rows.append({
                "key":    f"{cur_route['key']}_{suffix}",
                "name":   f"{cur_route['name']} {mlabel}",
                "symbol": f"{cur_route['symbol']}-{suffix.upper()}",
                "unit":   unit,
                "value":  value,
                "src_chg": _safe_float(chg),
            })

    if not rows:
        raise ValueError("表格中未解析到任何指标")

    # 同一 key 理论上只出现一次，保险起见按 key 去重（保留最后一次）
    dedup: dict = {}
    for r in rows:
        dedup[r["key"]] = r
    return {"data_date": data_date, "rows": list(dedup.values())}


def check_weighted_sum(rows: list) -> str:
    """
    自检：综合指数 是否等于 Σ(权重 × 各航线指数)。

    2026-09-29 实测恒等式成立：
        0.6×18713.21 + 0.4×10747.37 = 15526.80 ≈ 综合 15526.88
        0.6×(13.56)  + 0.4×(-25.34) = -1.99     = 源涨跌 -1.99
    后一条同时证明了「与上期比涨跌」是**绝对值**（与指标同单位）而非百分比。

    这个检查能抓出「解析错位」这类静默失败 —— 表格列一挪，恒等式立刻崩。
    返回一行可打印的结论；无法判定时返回说明文字。
    """
    total = next((r for r in rows if r["key"] == TOTAL_CONFIG["key"]), None)
    parts = [r for r in rows if r.get("weight") and re.fullmatch(r"ctfi_ct\d+", r["key"])]
    if total is None or not parts:
        return "[SKIP] 权重自检：缺综合指数或带权重的航线，无法判定"

    wsum = sum(r["weight"] for r in parts)
    calc = sum(r["weight"] / 100.0 * r["value"] for r in parts)
    diff = abs(calc - total["value"])
    rel  = diff / total["value"] * 100 if total["value"] else 0
    ok   = rel < 0.5 and 99.0 <= wsum <= 101.0
    tag  = "[OK]  " if ok else "[WARN]"
    return (f"{tag} 权重自检：Σ权重={wsum:.0f}%  "
            f"回算综合={calc:,.2f} vs 源综合={total['value']:,.2f}  "
            f"偏差 {rel:.3f}%" + ("" if ok else "  ← 超出容差，疑似解析错位！"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    argv    = sys.argv[1:]
    dry_run = "--dry-run" in argv
    full    = "--full" in argv

    print("── 抓取上海航运交易所 CTFI 中国进口原油运价指数 ──────────────────────")
    try:
        html = fetch_page()
    except Exception as exc:
        print(f"  [FATAL] 请求失败: {exc}")
        return 1

    try:
        data = parse_ctfi(html)
    except ValueError as exc:
        print(f"  [FATAL] 解析失败: {exc}")
        return 1

    data_date, rows = data["data_date"], data["rows"]
    print(f"  数据日期 {data_date}   解析到 {len(rows)} 个指标\n")

    # 按航线分组打印，便于肉眼核对
    order = [TOTAL_CONFIG["key"]] + [
        f"{c['key']}{sfx}"
        for c in ROUTE_CONFIGS
        for sfx in [""] + [f"_{m[1]}" for m in c["metrics"]]
    ]
    pos = {k: i for i, k in enumerate(order)}
    for r in sorted(rows, key=lambda x: pos.get(x["key"], 999)):
        chg = f"{r['src_chg']:+,.2f}" if r["src_chg"] is not None else "—"
        w   = f"权重{r['weight']:g}%" if r.get("weight") else ""
        print(f"  {r['name']:38s} {r['value']:>12,.2f} {r['unit']:>8s} {w:>8s}  (源涨跌 {chg})")

    print(f"\n  {check_weighted_sum(rows)}")

    if dry_run:
        print(f"\n[DRY-RUN] 跳过写库。将写入 {len(rows)} 行（{data_date}）。")
        return 0

    print("\n连接数据库...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        print("  [OK] 已连接，schema 就绪")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接: {exc}")
        return 1

    try:
        # 日期守卫：源端偶发返回陈旧数据时不回写旧值（此处 UPSERT 按 (key,date)，
        # 写旧日期只会多一行历史，不会覆盖最新值，因此只需提示不阻断）
        if not full:
            with conn.cursor() as cur:
                cur.execute(LATEST_DATE_SQL, ([r["key"] for r in rows],))
                latest = {k: v for k, v in cur.fetchall()}
            stale = [k for k, v in latest.items() if v.isoformat() > data_date]
            if stale:
                print(f"  [WARN] 以下序列库内最新日期晚于源端 {data_date}（疑似源端陈旧）：{stale}")

        with conn.cursor() as cur:
            for r in rows:
                cur.execute(UPSERT_INDEX_SQL,
                            (r["key"], r["symbol"], r["name"], MARKET, r["unit"]))
            print(f"  [OK] market_indices 就绪（{len(rows)} 个指标, market='{MARKET}'）")

            entries = [(r["key"], data_date, r["value"]) for r in rows]
            execute_values(cur, UPSERT_PRICES_SQL, entries)
            print(f"  [OK] index_prices 写入 {len(entries)} 行（{data_date}）")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()

    print(f"\n[OK] CTFI 更新完成：{data_date}，{len(rows)} 个指标。")
    print("    提示：CTFI 每工作日发布且只给当期值，历史靠每日运行累积 ——")
    print("          进 refresh_all.py 每日刷新，否则跑漏的交易日永久缺失。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
