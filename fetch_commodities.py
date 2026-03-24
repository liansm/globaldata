"""
Global Commodity Data Fetcher
Fetches domestic and international prices for gold, silver, copper, aluminum,
crude oil, and natural gas via akshare.
Also fetches Chinese coal prices at multiple calorific grades from sxcoal.com
and cctd.com.cn.  All data is persisted to PostgreSQL.

Domestic sources:
  - 上海黄金交易所 SGE  (gold, silver)          — spot price, CNY/g and CNY/kg
  - 新浪财经 SHFE/INE   (copper, alum, oil)      — main-contract daily close

International sources (via 新浪财经):
  - COMEX  (gold GC, silver SI, copper HG)      — USD/oz, USD/oz, 美分/磅
  - LME    (aluminum AHD)                        — USD/t
  - NYMEX  (crude oil WTI CL, natural gas NG)   — USD/bbl, USD/MMBtu

Coal sources:
  - sxcoal.com / 汾渭CCI                         — daily, no API key
  - cctd.com.cn / CCTD                           — daily spot + 7yr history API

Database (PostgreSQL):
  - commodities  — static metadata per commodity key (upserted each run)
  - prices       — daily price history (upserted by commodity_key + price_date)
  - fetch_log    — one row per commodity per run (audit trail)
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HISTORY_LIMIT = 365*20     # ~1 year of trading days to keep
DATABASE_URL  = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
SXCOAL_URL    = "https://www.sxcoal.com/hub/thermalcoal"
CCTD_URL      = "https://www.cctd.com.cn/"
CCTD_HIST_URL = "https://www.cctd.com.cn/Echarts/data/HBHCKJ_DLMQH.php"
HEADERS       = {"User-Agent": "Mozilla/5.0"}

# SGE spot configs (gold & silver)
SGE_CONFIGS = [
    {"key": "gold",   "commodity": "黄金 Au99.99", "symbol": "Au99.99", "unit": "元/克"},
    {"key": "silver", "commodity": "白银 Ag99.99", "symbol": "Ag99.99", "unit": "元/克", "divisor": 1000},
]

# Main-contract (主力连续) futures configs
FUTURES_CONFIGS = [
    {"key": "copper",             "commodity": "铜 (上期所 CU)",         "symbol": "CU0", "unit": "元/吨"},
    {"key": "aluminum",           "commodity": "铝 (上期所 AL)",         "symbol": "AL0", "unit": "元/吨"},
    {"key": "lithium_carbonate",  "commodity": "碳酸锂 (广期所 LC)",     "symbol": "LC0", "unit": "元/吨"},
    {"key": "methanol",           "commodity": "甲醇 (郑商所 MA)",       "symbol": "MA0", "unit": "元/吨"},
    {"key": "urea",               "commodity": "尿素 (郑商所 UR)",       "symbol": "UR0", "unit": "元/吨"},
    {"key": "meg",                "commodity": "乙二醇 (大商所 EG)",     "symbol": "EG0", "unit": "元/吨"},
    {"key": "styrene",            "commodity": "苯乙烯 (大商所 EB)",     "symbol": "EB0", "unit": "元/吨"},
    {"key": "polypropylene",      "commodity": "聚丙烯 (大商所 PP)",     "symbol": "PP0", "unit": "元/吨"},
    {"key": "natural_rubber",     "commodity": "天然橡胶 (上期所 RU)",   "symbol": "RU0", "unit": "元/吨"},
]

# International futures configs (via ak.futures_foreign_hist, 新浪财经)
INTL_CONFIGS = [
    {"key": "intl_gold",    "commodity": "黄金 (COMEX GC)",       "symbol": "GC",  "unit": "USD/oz"},
    {"key": "intl_silver",  "commodity": "白银 (COMEX SI)",       "symbol": "SI",  "unit": "USD/oz"},
    {"key": "intl_copper",  "commodity": "铜 (COMEX HG)",         "symbol": "HG",  "unit": "美分/磅"},
    {"key": "intl_alum",    "commodity": "铝 (LME AHD)",          "symbol": "AHD", "unit": "USD/吨"},
    {"key": "intl_oil_wti",   "commodity": "原油 WTI (NYMEX CL)",    "symbol": "CL",  "unit": "USD/桶"},
    {"key": "intl_oil_brent", "commodity": "原油 Brent (ICE OIL)",  "symbol": "OIL", "unit": "USD/桶"},
    {"key": "intl_gas",       "commodity": "天然气 (NYMEX NG)",     "symbol": "NG",  "unit": "USD/MMBtu"},
]

# CCI name  →  (file_key, human_label, calorific_kcal, grade_type)
CCI_INDEX_MAP = {
    "CCI5500":            ("coal_port_5500",        "动力煤 港口5500大卡",           5500, "港口"),
    "CCI5000":            ("coal_port_5000",        "动力煤 港口5000大卡",           5000, "港口"),
    "CCI4500":            ("coal_port_4500",        "动力煤 港口4500大卡",           4500, "港口"),
    "CCI进口5500":         ("coal_import_5500",      "进口动力煤 5500大卡",           5500, "进口"),
    "CCI进口4700":         ("coal_import_4700",      "进口动力煤 4700大卡",           4700, "进口"),
    "CCI进口3800":         ("coal_import_3800",      "进口动力煤 3800大卡",           3800, "进口"),
    "CCI进口5500FOB":      ("coal_import_5500_fob",  "进口动力煤 5500大卡 FOB",       5500, "进口FOB"),
    "CCI进口4700FOB":      ("coal_import_4700_fob",  "进口动力煤 4700大卡 FOB",       4700, "进口FOB"),
    "CCI进口3800FOB":      ("coal_import_3800_fob",  "进口动力煤 3800大卡 FOB",       3800, "进口FOB"),
    "CCI内蒙古鄂尔多斯5500": ("coal_ordos_5500",      "动力煤 鄂尔多斯5500大卡",       5500, "产地"),
    "CCI山西大同5500":      ("coal_datong_5500",      "动力煤 大同5500大卡",           5500, "产地"),
    "CCI陕西榆林5800":      ("coal_yulin_5800",       "动力煤 榆林5800大卡",           5800, "产地"),
}

# Regex: matches "汾渭CCI<name>指数为<price><元|美元>/吨"
_CCI_RE    = re.compile(r"汾渭(CCI[^，。\n]*?)指数为(\d[\d.]+)(元|美元)/吨")
# Change: "环比前一日(上涨|下降|增长)X元/吨" or "持平"
_CHANGE_RE = re.compile(r"环比前一日(?:(上涨|增长)(\d[\d.]*)|下降(\d[\d.]*)|(持平))")

# CCTD homepage price-category config:
#   Chinese label → (file_key_prefix, readable_label)
_CCTD_TYPES = {
    "环渤海现货": ("bohai",  "环渤海现货"),
    "综合交易":   ("comp",   "综合交易"),
    "现货交易":   ("spot",   "现货交易"),
    "年度长协":   ("annual", "年度长协"),
}


# ---------------------------------------------------------------------------
# Database schema & helpers
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS commodities (
    key          VARCHAR(60)   PRIMARY KEY,
    symbol       VARCHAR(60)   NOT NULL,
    commodity    VARCHAR(200)  NOT NULL,
    unit         VARCHAR(50)   NOT NULL,
    source_api   VARCHAR(200),
    price_type   VARCHAR(100),
    kcal         INTEGER,
    grade_type   VARCHAR(50),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prices (
    id            BIGSERIAL     PRIMARY KEY,
    commodity_key VARCHAR(60)   NOT NULL REFERENCES commodities(key) ON DELETE CASCADE,
    price_date    DATE          NOT NULL,
    price         NUMERIC(14,4) NOT NULL,
    UNIQUE (commodity_key, price_date)
);

CREATE INDEX IF NOT EXISTS idx_prices_ck_date
    ON prices (commodity_key, price_date DESC);

CREATE TABLE IF NOT EXISTS fetch_log (
    id             SERIAL        PRIMARY KEY,
    fetched_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    commodity_key  VARCHAR(60)   NOT NULL,
    latest_date    DATE,
    latest_price   NUMERIC(14,4),
    change_day     NUMERIC(10,4)
);
"""


def db_init(conn: psycopg2.extensions.connection) -> None:
    """Create tables if they don't exist (idempotent — safe on every run)."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def db_upsert_commodity(conn, key: str, symbol: str, commodity: str,
                         unit: str, source_api: str,
                         price_type: str = None, kcal: int = None,
                         grade_type: str = None) -> None:
    """Insert or update one row in the commodities table."""
    sql = """
    INSERT INTO commodities
        (key, symbol, commodity, unit, source_api, price_type, kcal, grade_type, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (key) DO UPDATE SET
        symbol     = EXCLUDED.symbol,
        commodity  = EXCLUDED.commodity,
        unit       = EXCLUDED.unit,
        source_api = EXCLUDED.source_api,
        price_type = EXCLUDED.price_type,
        kcal       = EXCLUDED.kcal,
        grade_type = EXCLUDED.grade_type,
        updated_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (key, symbol, commodity, unit, source_api,
                          price_type, kcal, grade_type))


def db_upsert_prices(conn, commodity_key: str, entries: list) -> int:
    """
    Bulk-upsert a list of {date, price} dicts into the prices table.
    Existing rows with the same (commodity_key, price_date) are overwritten.
    Returns the number of rows processed.
    """
    if not entries:
        return 0
    rows = [(commodity_key, e["date"], e["price"]) for e in entries]
    sql = """
    INSERT INTO prices (commodity_key, price_date, price)
    VALUES %s
    ON CONFLICT (commodity_key, price_date) DO UPDATE SET
        price = EXCLUDED.price
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    return len(rows)


def db_log_fetch(conn, commodity_key: str, latest_date,
                 latest_price, change_day=None) -> None:
    """Append one row to fetch_log for audit/monitoring."""
    sql = """
    INSERT INTO fetch_log (commodity_key, latest_date, latest_price, change_day)
    VALUES (%s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (commodity_key, latest_date, latest_price, change_day))


def _db_save(conn, key: str, symbol: str, commodity: str, unit: str,
             source: str, entries: list, change_day=None,
             price_type: str = None, kcal: int = None,
             grade_type: str = None) -> dict:
    """
    Convenience wrapper: upsert metadata + price history + fetch log in one call.
    Returns the latest entry dict {date, price} (or {}).
    """
    db_upsert_commodity(conn, key, symbol, commodity, unit, source,
                        price_type, kcal, grade_type)
    db_upsert_prices(conn, key, entries)
    latest = entries[0] if entries else {}
    db_log_fetch(conn, key, latest.get("date"), latest.get("price"), change_day)
    return latest


# ---------------------------------------------------------------------------
# HTTP helper (used for coal scraping)
# ---------------------------------------------------------------------------
def _get(url: str, params: dict, label: str) -> requests.Response | None:
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp
    except requests.exceptions.ConnectionError:
        print(f"  [ERROR] {label}: Connection error")
    except requests.exceptions.Timeout:
        print(f"  [ERROR] {label}: Request timed out")
    except requests.exceptions.HTTPError as exc:
        print(f"  [ERROR] {label}: HTTP {exc.response.status_code}")
    return None


# ---------------------------------------------------------------------------
# akshare fetchers
# ---------------------------------------------------------------------------
def sge_fetch(symbol: str, label: str):
    """
    Fetch Shanghai Gold Exchange spot price history via akshare.
    Columns: date, open, close, low, high
    Returns (entries_list, source_str) or (None, None).
    """
    try:
        df = ak.spot_hist_sge(symbol=symbol)
    except Exception as exc:
        print(f"  [ERROR] {label}: akshare error — {exc}")
        return None, None

    if df is None or df.empty:
        print(f"  [ERROR] {label}: empty data returned")
        return None, None

    entries = []
    for _, row in df.iterrows():
        try:
            price = float(row["close"])
            if pd.isna(price):
                continue
            entries.append({
                "date":  str(row["date"])[:10],
                "price": round(price, 4),
            })
        except (KeyError, ValueError, TypeError):
            continue

    if not entries:
        print(f"  [ERROR] {label}: no valid rows parsed")
        return None, None

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:HISTORY_LIMIT], f"上海黄金交易所 SGE ({symbol})"


def futures_main_fetch(symbol: str, label: str):
    """
    Fetch main-contract (主力连续) historical daily prices via akshare / 新浪财经.
    Columns: 日期, 开盘价, 最高价, 最低价, 收盘价, 成交量, 持仓量, 动态结算价
    Returns (entries_list, source_str) or (None, None).
    """
    end_date   = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=365 * 20)).strftime("%Y%m%d")

    try:
        df = ak.futures_main_sina(symbol=symbol, start_date=start_date, end_date=end_date)
    except Exception as exc:
        print(f"  [ERROR] {label}: akshare error — {exc}")
        return None, None

    if df is None or df.empty:
        print(f"  [ERROR] {label}: empty data returned")
        return None, None

    entries = []
    for _, row in df.iterrows():
        try:
            date_val = row["日期"]
            date_str = (date_val.strftime("%Y-%m-%d")
                        if hasattr(date_val, "strftime") else str(date_val)[:10])
            price = float(row["收盘价"])
            if pd.isna(price):
                continue
            entries.append({"date": date_str, "price": round(price, 4)})
        except (KeyError, ValueError, TypeError):
            continue

    if not entries:
        print(f"  [ERROR] {label}: no valid rows parsed")
        return None, None

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:HISTORY_LIMIT], f"新浪财经 ({symbol} 主力连续)"


def futures_foreign_fetch(symbol: str, label: str):
    """
    Fetch international futures historical daily prices via akshare / 新浪财经.
    Columns: date, open, high, low, close, volume, position, [settlement]
    Returns (entries_list, source_str) or (None, None).
    """
    try:
        df = ak.futures_foreign_hist(symbol=symbol)
    except Exception as exc:
        print(f"  [ERROR] {label}: akshare error — {exc}")
        return None, None

    if df is None or df.empty:
        print(f"  [ERROR] {label}: empty data returned")
        return None, None

    entries = []
    for _, row in df.iterrows():
        try:
            date_val  = row.get("date")
            close_val = row.get("close")
            if close_val is None or pd.isna(close_val):
                continue
            date_str = (date_val.strftime("%Y-%m-%d")
                        if hasattr(date_val, "strftime") else str(date_val)[:10])
            entries.append({"date": date_str, "price": round(float(close_val), 4)})
        except (ValueError, TypeError):
            continue

    if not entries:
        print(f"  [ERROR] {label}: no valid rows parsed")
        return None, None

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:HISTORY_LIMIT], f"新浪财经 国际期货 ({symbol})"


# ---------------------------------------------------------------------------
# sxcoal / 汾渭CCI fetcher
# ---------------------------------------------------------------------------
def sxcoal_cci_fetch(label: str = "sxcoal CCI"):
    """
    Fetch current CCI coal price indices from sxcoal.com (中国煤炭资源网 汾渭CCI).
    Returns list of dicts or None on failure.
    """
    resp = _get(SXCOAL_URL, {}, label)
    if resp is None:
        return None

    try:
        soup       = BeautifulSoup(resp.text, "lxml")
        script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if not script_tag:
            print(f"  [ERROR] {label}: __NEXT_DATA__ not found in page")
            return None
        page_data  = json.loads(script_tag.text)
        quick_list = page_data["props"]["pageProps"]["quickList"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        print(f"  [ERROR] {label}: page parse error — {exc}")
        return None

    seen: dict[str, dict] = {}

    for day in quick_list:
        date_str = day.get("showDate", "")
        for item in day.get("data", []):
            content = item.get("content") or item.get("summary") or ""
            m_price = _CCI_RE.search(content)
            if not m_price:
                continue

            cci_name = m_price.group(1).strip()
            price    = round(float(m_price.group(2)), 4)
            unit     = m_price.group(3) + "/吨"

            m_chg = _CHANGE_RE.search(content)
            if m_chg:
                if m_chg.group(4):
                    change = 0.0
                elif m_chg.group(1):
                    change = round(float(m_chg.group(2)), 4)
                else:
                    change = -round(float(m_chg.group(3)), 4)
            else:
                change = None

            if cci_name not in seen:
                seen[cci_name] = {"cci": cci_name, "date": date_str,
                                  "price": price, "unit": unit,
                                  "change_day": change}

    if not seen:
        print(f"  [ERROR] {label}: no CCI prices parsed from page")
        return None

    return list(seen.values())


# ---------------------------------------------------------------------------
# CCTD / 中国煤炭市场网 fetchers
# ---------------------------------------------------------------------------
def cctd_spot_fetch(label: str = "CCTD") -> list | None:
    """
    Scrape CCTD homepage (cctd.com.cn) for current thermal coal prices across
    all 4 price categories and all available calorific grades.

    Price categories captured:
      环渤海现货 (bohai)  — 5500, 5000, 4500 大卡
      综合交易   (comp)   — 5500, 5000, 4500 大卡
      现货交易   (spot)   — 5000 大卡 only
      年度长协   (annual) — 5500, 5000, 4500 大卡

    Page structure: category+grade label (e.g. "环渤海现货5500") is inside an
    HTML comment; price/change/date appear in subsequent <em> tags.
    Red CSS color (#ea4335) ≡ price increase; other colors ≡ decrease.

    Returns list of dicts (one per category×grade combination) or None on failure.
    """
    resp = _get(CCTD_URL, {}, label)
    if resp is None:
        return None

    # CCTD site is GBK-encoded; GB18030 is a superset that handles it
    for enc in ("utf-8", "gb18030"):
        try:
            html = resp.content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        html = resp.text

    year = datetime.now().strftime("%Y")

    # Each price block: "{category}{kcal}" (in HTML comment) →
    #   price <em> → "变化：" + colored change <em> → date MM-DD
    type_pattern = "|".join(re.escape(k) for k in _CCTD_TYPES)
    block_re = re.compile(
        rf"({type_pattern})(\d{{4,5}})"                     # (1) category, (2) kcal
        r".{10,600}?"                                       # comment end + wrappers
        r"<em[^>]*>(\d{3,4}(?:\.\d+)?)</em>"               # (3) price
        r".{10,600}?变化[：:]"                              # stuff + change label
        r"\s*<em[^>]*color:\s*([^;\"]+)[^>]*>"             # (4) CSS color
        r"(\d+(?:\.\d+)?)</em>"                             # (5) change absolute value
        r".{0,300}?日期[：:]\s*(\d{2}-\d{2})",             # (6) MM-DD date
        re.DOTALL,
    )

    results = []
    for m in block_re.finditer(html):
        type_cn   = m.group(1)
        kcal_str  = m.group(2)[:4]
        price     = float(m.group(3))
        color     = m.group(4).strip().lower()
        chg_abs   = float(m.group(5))
        date_mmdd = m.group(6)          # "MM-DD"

        type_prefix, type_label = _CCTD_TYPES.get(type_cn, (type_cn, type_cn))

        # Red color → price up (positive change); other → down (negative)
        is_up  = any(c in color for c in ("#ea4335", "red", "#c00", "#f00", "#ff"))
        change = round(chg_abs if is_up else -chg_abs, 4)

        results.append({
            "type_prefix": type_prefix,
            "type_label":  type_label,
            "kcal":        int(kcal_str),
            "file_key":    f"cctd_{type_prefix}_{kcal_str}",
            "symbol":      f"CCTD_{type_prefix.upper()}_{kcal_str}",
            "commodity":   f"动力煤 CCTD {type_label}{kcal_str}大卡",
            "date":        f"{year}-{date_mmdd}",
            "price":       price,
            "change":      change,
        })

    if not results:
        print(f"  [ERROR] {label}: no prices parsed from homepage")
        return None

    return results


def cctd_hist_fetch(label: str = "CCTD hist") -> tuple:
    """
    Fetch historical coal price series from CCTD JSON API.
    URL: /Echarts/data/HBHCKJ_DLMQH.php
    Returns (entries_age, entries_product) — each is a list[{date, price}] sorted
    newest-first, or None if unavailable.
      "age"     ≈ 环渤海动力煤5500大卡港口价  (2018-present, ~1046 pts)
      "product" ≈ 另一煤价参考序列            (has ~15% missing "~" values)
    """
    resp = _get(CCTD_HIST_URL, {}, label)
    if resp is None:
        return None, None

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        print(f"  [ERROR] {label}: JSON parse error")
        return None, None

    if not isinstance(data, list) or not data:
        print(f"  [ERROR] {label}: unexpected JSON structure")
        return None, None

    entries_age: list[dict] = []
    entries_product: list[dict] = []

    for item in data:
        date_str = str(item.get("name", ""))[:10]
        if not re.match(r"\d{4}-\d{2}-\d{2}", date_str):
            continue

        for field, bucket in (("age", entries_age), ("product", entries_product)):
            raw = item.get(field, "~")
            if raw == "~" or raw is None:
                continue
            try:
                bucket.append({"date": date_str, "price": round(float(raw), 4)})
            except (ValueError, TypeError):
                pass

    entries_age.sort(    key=lambda x: x["date"], reverse=True)
    entries_product.sort(key=lambda x: x["date"], reverse=True)

    if not entries_age and not entries_product:
        print(f"  [ERROR] {label}: no valid entries parsed")
        return None, None

    return entries_age or None, entries_product or None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    failed: list[str] = []

    # ── Connect to PostgreSQL ────────────────────────────────────────────────
    print(f"Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        db_init(conn)
        print(f"  [OK] Connected. Schema ready.\n")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] Cannot connect to database: {exc}")
        print(f"  DATABASE_URL = {DATABASE_URL!r}")
        print(f"  Hint: set DATABASE_URL in .env or environment.")
        return 1

    saved_count = 0

    try:
        # ── SGE spot: gold & silver ──────────────────────────────────────────
        for cfg in SGE_CONFIGS:
            print(f"Fetching {cfg['commodity']} ({cfg['symbol']}) via SGE...")
            entries, source = sge_fetch(cfg["symbol"], cfg["commodity"])
            if entries is None:
                print(f"  [SKIP] {cfg['commodity']}")
                failed.append(cfg["key"])
                continue
            divisor = cfg.get("divisor", 1)
            if divisor != 1:
                entries = [{"date": e["date"], "price": round(e["price"] / divisor, 4)}
                           for e in entries]
            latest = _db_save(conn, cfg["key"], cfg["symbol"], cfg["commodity"],
                               cfg["unit"], source, entries)
            saved_count += 1
            print(f"  [OK] {cfg['commodity']}: {latest.get('date')} "
                  f"@ {latest.get('price')} {cfg['unit']}")

        # ── 新浪财经主力连续: copper, aluminum, oil ──────────────────────────
        for cfg in FUTURES_CONFIGS:
            print(f"Fetching {cfg['commodity']} ({cfg['symbol']}) via 新浪财经...")
            entries, source = futures_main_fetch(cfg["symbol"], cfg["commodity"])
            if entries is None:
                print(f"  [SKIP] {cfg['commodity']}")
                failed.append(cfg["key"])
                continue
            latest = _db_save(conn, cfg["key"], cfg["symbol"], cfg["commodity"],
                               cfg["unit"], source, entries)
            saved_count += 1
            print(f"  [OK] {cfg['commodity']}: {latest.get('date')} "
                  f"@ {latest.get('price')} {cfg['unit']}")

        # ── 新浪财经 国际期货: gold, silver, copper, alum, oil, gas ─────────
        print()
        for cfg in INTL_CONFIGS:
            print(f"Fetching {cfg['commodity']} ({cfg['symbol']}) via 新浪财经 国际...")
            entries, source = futures_foreign_fetch(cfg["symbol"], cfg["commodity"])
            if entries is None:
                print(f"  [SKIP] {cfg['commodity']}")
                failed.append(cfg["key"])
                continue
            latest = _db_save(conn, cfg["key"], cfg["symbol"], cfg["commodity"],
                               cfg["unit"], source, entries)
            saved_count += 1
            print(f"  [OK] {cfg['commodity']}: {latest.get('date')} "
                  f"@ {latest.get('price')} {cfg['unit']}")

        # ── sxcoal / 汾渭CCI (coal by calorific grade) ──────────────────────
        print()
        print("Fetching coal prices by calorific grade (sxcoal.com 汾渭CCI)...")
        cci_list = sxcoal_cci_fetch("sxcoal CCI")

        if cci_list is None:
            print("  [SKIP] all coal CCI grades")
            failed.append("coal_cci")
        else:
            found_keys = []
            for item in cci_list:
                cci_name = item["cci"]
                if cci_name not in CCI_INDEX_MAP:
                    continue

                file_key, label, kcal, grade = CCI_INDEX_MAP[cci_name]
                entry  = {"date": item["date"], "price": item["price"]}
                latest = _db_save(
                    conn, file_key, cci_name, label, item["unit"],
                    "sxcoal.com 汾渭CCI",
                    [entry],
                    change_day = item["change_day"],
                    kcal       = kcal,
                    grade_type = grade,
                )
                found_keys.append(file_key)
                saved_count += 1
                chg = (f"{item['change_day']:+.2f}"
                       if item["change_day"] is not None else "±?")
                print(f"  [OK] {label}: {item['date']} @ {item['price']} "
                      f"{item['unit']} ({chg})")

            if not found_keys:
                print("  [SKIP] no matching CCI grades found")
                failed.append("coal_cci")

        # ── CCTD 动力煤价格 (cctd.com.cn) ────────────────────────────────────
        print()
        print("Fetching CCTD thermal coal prices (cctd.com.cn)...")
        cctd_spots             = cctd_spot_fetch("CCTD")
        hist_age, hist_product = cctd_hist_fetch("CCTD hist")

        if cctd_spots is None:
            print("  [SKIP] CCTD spot prices (homepage parse failed)")
            failed.append("cctd")
        else:
            for item in cctd_spots:
                file_key    = item["file_key"]
                kcal        = item["kcal"]
                type_prefix = item["type_prefix"]

                # historical API series only for 环渤海现货 5500大卡
                hist = hist_age if (type_prefix == "bohai" and kcal == 5500
                                    and hist_age) else []

                # Merge: homepage price is authoritative latest; prepend it so
                # it takes precedence over any same-date entry in the API series
                if hist:
                    entries = ([{"date": item["date"], "price": item["price"]}]
                               + [e for e in hist if e["date"] != item["date"]])
                else:
                    entries = [{"date": item["date"], "price": item["price"]}]

                latest = _db_save(
                    conn, file_key, item["symbol"], item["commodity"],
                    "元/吨", "中国煤炭市场网 cctd.com.cn",
                    entries,
                    change_day = item["change"],
                    price_type = item["type_label"],
                    kcal       = kcal,
                )
                saved_count += 1
                chg = f"{item['change']:+.2f}" if item["change"] is not None else "±?"
                print(f"  [OK] {item['commodity']}: {item['date']} "
                      f"@ {item['price']} 元/吨 ({chg})")

            # Second historical series (product field)
            if hist_product:
                latest = _db_save(
                    conn, "cctd_hist_b",
                    "CCTD_HIST_B", "动力煤 CCTD 综合指数B",
                    "元/吨", "中国煤炭市场网 cctd.com.cn",
                    hist_product,
                    price_type = "综合指数B",
                )
                saved_count += 1
                print(f"  [OK] 动力煤 CCTD 综合指数B: "
                      f"{latest.get('date')} @ {latest.get('price')} 元/吨")

        # ── Commit ───────────────────────────────────────────────────────────
        conn.commit()
        print(f"\nCommitted {saved_count} commodity dataset(s) to database.")

    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] Unexpected error — rolling back: {exc}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        conn.close()

    if failed:
        print(f"\n[SUMMARY] {len(failed)} item(s) failed: {', '.join(failed)}")
        return 1

    print(f"\n[SUMMARY] All {saved_count} commodities fetched and saved successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
