"""
Global Market Index & Capital Flow Fetcher
Fetches Chinese A-share indices, Hong Kong indices, and Stock Connect
capital flow data via akshare. Persists results to PostgreSQL.

A-share indices (Sina):
  sh000001 上证指数  sz399001 深证成指  sz399006 创业板指
  sh000688 科创50   bj899050 北证50

HK indices (Sina):
  HSI 恒生指数  HSCEI 恒生国企指数

Capital flows (Tushare moneyflow_hsgt):
  北向资金 (Northbound)  南向资金 (Southbound)
"""

import os
import sys
from datetime import datetime, timedelta

import akshare as ak
import tushare as ts
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN", "")
_ts_pro = None

def _get_ts_pro():
    global _ts_pro
    if _ts_pro is None:
        if not TUSHARE_TOKEN:
            raise RuntimeError("TUSHARE_TOKEN not set in environment")
        ts.set_token(TUSHARE_TOKEN)
        _ts_pro = ts.pro_api()
    return _ts_pro

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HISTORY_LIMIT = 365*20
DATABASE_URL  = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

INDEX_CONFIGS = [
    # A股 — ak.stock_zh_index_daily
    {"key": "idx_sh",    "name": "上证指数", "symbol": "sh000001", "market": "A股",  "unit": "点"},
    {"key": "idx_sz",    "name": "深证成指", "symbol": "sz399001", "market": "A股",  "unit": "点"},
    {"key": "idx_hs300", "name": "沪深300",  "symbol": "sh000300", "market": "A股",  "unit": "点"},
    {"key": "idx_cyb",   "name": "创业板指", "symbol": "sz399006", "market": "A股",  "unit": "点"},
    {"key": "idx_kc50",  "name": "科创50",   "symbol": "sh000688", "market": "A股",  "unit": "点"},
    {"key": "idx_bz50",  "name": "北证50",   "symbol": "bj899050", "market": "A股",  "unit": "点"},
    # 港股 — ak.stock_hk_index_daily_sina
    {"key": "idx_hsi",    "name": "恒生指数",   "symbol": "HSI",      "market": "港股", "unit": "点"},
    {"key": "idx_hscei",  "name": "恒生国企",   "symbol": "HSCEI",    "market": "港股", "unit": "点"},
    {"key": "idx_hstech", "name": "恒生科技",   "symbol": "HSTECH",   "market": "港股", "unit": "点"},
]

FLOW_CONFIGS = [
    {"key": "flow_north", "name": "北向资金", "symbol": "北向资金", "market": "资金流向", "unit": "亿元"},
    {"key": "flow_south", "name": "南向资金", "symbol": "南向资金", "market": "资金流向", "unit": "亿元"},
]

# 海外指数
# Source: ak.index_us_stock_sina / ak.index_global_hist_sina
#   sina_fn: "us"     → ak.index_us_stock_sina(sina_symbol)       列: date,open,high,low,close,volume,amount
#   sina_fn: "global" → ak.index_global_hist_sina(sina_symbol)    列: date,open,high,low,close,volume
GLOBAL_CONFIGS = [
    # 美股
    {"key": "idx_dji",    "name": "道琼斯",     "symbol": "道琼斯",            "market": "美股", "unit": "点",
     "sina_fn": "us",     "sina_symbol": ".DJI"},
    {"key": "idx_sp500",  "name": "标普500",    "symbol": "标普500",           "market": "美股", "unit": "点",
     "sina_fn": "us",     "sina_symbol": ".INX"},
    {"key": "idx_nasdaq", "name": "纳斯达克",   "symbol": "纳斯达克",          "market": "美股", "unit": "点",
     "sina_fn": "us",     "sina_symbol": ".IXIC"},
    # 亚太
    {"key": "idx_nikkei", "name": "日经225",    "symbol": "日经225",           "market": "亚太", "unit": "点",
     "sina_fn": "global", "sina_symbol": "日经225指数"},
    {"key": "idx_kospi",  "name": "韩国KOSPI",  "symbol": "韩国KOSPI",         "market": "亚太", "unit": "点",
     "sina_fn": "global", "sina_symbol": "首尔综合指数"},
    {"key": "idx_sensex", "name": "印度SENSEX", "symbol": "印度孟买SENSEX",    "market": "亚太", "unit": "点",
     "sina_fn": "global", "sina_symbol": "印度孟买SENSEX指数"},
    # 欧洲
    {"key": "idx_ftse",   "name": "英国富时100","symbol": "英国富时100",       "market": "欧洲", "unit": "点",
     "sina_fn": "global", "sina_symbol": "英国富时100指数"},
    {"key": "idx_dax",    "name": "德国DAX",    "symbol": "德国DAX30",         "market": "欧洲", "unit": "点",
     "sina_fn": "global", "sina_symbol": "德国DAX 30种股价指数"},
    {"key": "idx_cac40",  "name": "法国CAC40",  "symbol": "法国CAC40",         "market": "欧洲", "unit": "点",
     "sina_fn": "global", "sina_symbol": "法CAC40指数"},
]

# ---------------------------------------------------------------------------
# Database schema
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

-- Migrate existing tables: add columns if they don't exist yet
ALTER TABLE index_prices ADD COLUMN IF NOT EXISTS open     NUMERIC(16,4);
ALTER TABLE index_prices ADD COLUMN IF NOT EXISTS high     NUMERIC(16,4);
ALTER TABLE index_prices ADD COLUMN IF NOT EXISTS low      NUMERIC(16,4);

CREATE INDEX IF NOT EXISTS idx_index_prices_key_date
    ON index_prices (index_key, price_date DESC);
"""


def db_init(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def db_upsert_index(conn, key, symbol, name, market, unit):
    sql = """
    INSERT INTO market_indices (key, symbol, name, market, unit, updated_at)
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (key) DO UPDATE SET
        symbol     = EXCLUDED.symbol,
        name       = EXCLUDED.name,
        market     = EXCLUDED.market,
        unit       = EXCLUDED.unit,
        updated_at = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (key, symbol, name, market, unit))


def db_upsert_prices(conn, index_key, entries):
    if not entries:
        return 0
    rows = [
        (index_key, e["date"],
         e.get("open"), e.get("high"), e.get("low"),
         e.get("close"), e.get("volume"), e.get("turnover"))
        for e in entries
    ]
    sql = """
    INSERT INTO index_prices (index_key, price_date, open, high, low, close, volume, turnover)
    VALUES %s
    ON CONFLICT (index_key, price_date) DO UPDATE SET
        open     = EXCLUDED.open,
        high     = EXCLUDED.high,
        low      = EXCLUDED.low,
        close    = EXCLUDED.close,
        volume   = EXCLUDED.volume,
        turnover = EXCLUDED.turnover
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    return len(rows)


def _safe_float(val):
    try:
        f = float(val)
        return None if pd.isna(f) else round(f, 4)
    except (TypeError, ValueError):
        return None


def _find_col(df: "pd.DataFrame", candidates: list) -> str | None:
    """Return the first column name from *candidates* that exists in *df*."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ---------------------------------------------------------------------------
# A-share index fetcher
# Source: ak.stock_zh_index_daily (Sina)
# ---------------------------------------------------------------------------
def fetch_astock_index(cfg):
    symbol = cfg["symbol"]
    label  = cfg["name"]

    try:
        df = ak.stock_zh_index_daily(symbol=symbol)
    except Exception as exc:
        print(f"  [ERROR] {label}: {exc}")
        return None

    if df is None or df.empty:
        print(f"  [ERROR] {label}: empty data")
        return None

    date_col     = _find_col(df, ["date", "日期"])
    open_col     = _find_col(df, ["open",  "开盘"])
    high_col     = _find_col(df, ["high",  "最高"])
    low_col      = _find_col(df, ["low",   "最低"])
    close_col    = _find_col(df, ["close", "收盘"])
    volume_col   = _find_col(df, ["volume", "成交量"])
    turnover_col = _find_col(df, ["amount", "成交额", "turnover"])
    if not date_col or not close_col:
        print(f"  [ERROR] {label}: unexpected columns: {list(df.columns)}")
        return None

    entries = []
    for _, row in df.iterrows():
        date_val = row.get(date_col)
        close    = _safe_float(row.get(close_col))
        if date_val is None or close is None:
            continue
        date_str = (date_val.strftime("%Y-%m-%d")
                    if hasattr(date_val, "strftime") else str(date_val)[:10])
        entries.append({
            "date":     date_str,
            "open":     _safe_float(row.get(open_col))     if open_col     else None,
            "high":     _safe_float(row.get(high_col))     if high_col     else None,
            "low":      _safe_float(row.get(low_col))      if low_col      else None,
            "close":    close,
            "volume":   _safe_float(row.get(volume_col))   if volume_col   else None,
            "turnover": _safe_float(row.get(turnover_col)) if turnover_col else None,
        })

    if not entries:
        print(f"  [ERROR] {label}: no valid rows")
        return None

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:HISTORY_LIMIT]


# ---------------------------------------------------------------------------
# HK index fetcher
# ---------------------------------------------------------------------------
def fetch_hk_index(cfg):
    symbol = cfg["symbol"]
    label  = cfg["name"]

    try:
        df = ak.stock_hk_index_daily_sina(symbol=symbol)
    except Exception as exc:
        print(f"  [ERROR] {label}: {exc}")
        return None

    if df is None or df.empty:
        print(f"  [ERROR] {label}: empty data")
        return None

    entries = []
    for _, row in df.iterrows():
        date_val = row.get("date") or row.get("日期")
        close    = _safe_float(row.get("close") or row.get("收盘"))
        if date_val is None or close is None:
            continue
        date_str = (date_val.strftime("%Y-%m-%d")
                    if hasattr(date_val, "strftime") else str(date_val)[:10])
        entries.append({
            "date":    date_str,
            "open":    _safe_float(row.get("open")   or row.get("开盘")),
            "high":    _safe_float(row.get("high")   or row.get("最高")),
            "low":     _safe_float(row.get("low")    or row.get("最低")),
            "close":   close,
            "volume":  _safe_float(row.get("volume") or row.get("成交量")),
        })

    if not entries:
        print(f"  [ERROR] {label}: no valid rows")
        return None

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:HISTORY_LIMIT]


# ---------------------------------------------------------------------------
# Global index fetcher
# Source: ak.index_us_stock_sina / ak.index_global_hist_sina
# ---------------------------------------------------------------------------
def _parse_global_df(df, label) -> list | None:
    """Parse a DataFrame with columns date/open/high/low/close/volume into entries list."""
    date_col     = _find_col(df, ["date", "日期"])
    open_col     = _find_col(df, ["open",  "开盘"])
    high_col     = _find_col(df, ["high",  "最高"])
    low_col      = _find_col(df, ["low",   "最低"])
    close_col    = _find_col(df, ["close", "最新价"])
    volume_col   = _find_col(df, ["volume", "成交量", "vol"])
    turnover_col = _find_col(df, ["amount", "成交额", "turnover"])
    if not date_col or not close_col:
        print(f"  [WARN] {label}: unexpected columns: {list(df.columns)}")
        return None
    entries = []
    for _, row in df.iterrows():
        date_val = row.get(date_col)
        close    = _safe_float(row.get(close_col))
        if date_val is None or close is None:
            continue
        date_str = (date_val.strftime("%Y-%m-%d")
                    if hasattr(date_val, "strftime") else str(date_val)[:10])
        entries.append({
            "date":     date_str,
            "open":     _safe_float(row.get(open_col))     if open_col     else None,
            "high":     _safe_float(row.get(high_col))     if high_col     else None,
            "low":      _safe_float(row.get(low_col))      if low_col      else None,
            "close":    close,
            "volume":   _safe_float(row.get(volume_col))   if volume_col   else None,
            "turnover": _safe_float(row.get(turnover_col)) if turnover_col else None,
        })
    return entries or None


def fetch_global_index(cfg):
    label       = cfg["name"]
    sina_fn     = cfg.get("sina_fn")
    sina_symbol = cfg.get("sina_symbol")
    if not sina_fn or not sina_symbol:
        print(f"  [ERROR] {label}: no Sina source configured")
        return None

    try:
        if sina_fn == "us":
            df = ak.index_us_stock_sina(symbol=sina_symbol)
        else:  # "global"
            df = ak.index_global_hist_sina(symbol=sina_symbol)
    except Exception as exc:
        print(f"  [ERROR] {label}: {exc}")
        return None

    if df is None or df.empty:
        print(f"  [ERROR] {label}: empty data")
        return None

    entries = _parse_global_df(df, label)
    if not entries:
        print(f"  [ERROR] {label}: no valid rows")
        return None

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:HISTORY_LIMIT]


# ---------------------------------------------------------------------------
# Capital flow fetcher (Stock Connect 沪深港通)
# Source: tushare moneyflow_hsgt
# Returns north_money / south_money in 万元 → converted to 亿元 (/10000)
# ---------------------------------------------------------------------------
def fetch_capital_flow(cfg):
    label      = cfg["name"]
    flow_key   = cfg["symbol"]  # "北向资金" or "南向资金"
    net_col    = "north_money" if flow_key == "北向资金" else "south_money"

    start = (datetime.now() - timedelta(days=365 * 20)).strftime("%Y%m%d")
    end   = datetime.now().strftime("%Y%m%d")

    try:
        pro = _get_ts_pro()
        df  = pro.moneyflow_hsgt(start_date=start, end_date=end,
                                  fields="trade_date,north_money,south_money")
    except Exception as exc:
        print(f"  [ERROR] {label}: {exc}")
        return None

    if df is None or df.empty:
        print(f"  [ERROR] {label}: empty data")
        return None

    entries = []
    for _, row in df.iterrows():
        date_val = row.get("trade_date")
        net_val  = _safe_float(row.get(net_col))
        if date_val is None or net_val is None:
            continue
        date_str = str(date_val)[:10]
        if len(date_str) == 8:  # YYYYMMDD → YYYY-MM-DD
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        entries.append({"date": date_str, "close": round(net_val / 10000, 4)})

    if not entries:
        print(f"  [ERROR] {label}: no valid rows")
        return None

    entries.sort(key=lambda x: x["date"], reverse=True)
    return entries[:HISTORY_LIMIT]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    failed = []

    print("Connecting to database...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        db_init(conn)
        print("  [OK] Connected. Schema ready.\n")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] Cannot connect: {exc}")
        return 1

    saved = 0

    try:
        # ── A股指数 ──────────────────────────────────────────────────────────
        print("Fetching A-share indices...")
        for cfg in INDEX_CONFIGS:
            if cfg["market"] == "A股":
                print(f"  {cfg['name']} ({cfg['symbol']})...")
                entries = fetch_astock_index(cfg)
                if entries is None:
                    failed.append(cfg["key"])
                    continue
                db_upsert_index(conn, cfg["key"], cfg["symbol"],
                                cfg["name"], cfg["market"], cfg["unit"])
                db_upsert_prices(conn, cfg["key"], entries)
                latest = entries[0]
                print(f"    [OK] {latest['date']} @ {latest['close']:,.2f} 点  "
                      f"成交额 {latest.get('turnover') or '—'}")
                saved += 1

        # ── 港股指数 ──────────────────────────────────────────────────────────
        print("\nFetching HK indices...")
        for cfg in INDEX_CONFIGS:
            if cfg["market"] == "港股":
                print(f"  {cfg['name']} ({cfg['symbol']})...")
                entries = fetch_hk_index(cfg)
                if entries is None:
                    failed.append(cfg["key"])
                    continue
                db_upsert_index(conn, cfg["key"], cfg["symbol"],
                                cfg["name"], cfg["market"], cfg["unit"])
                db_upsert_prices(conn, cfg["key"], entries)
                latest = entries[0]
                print(f"    [OK] {latest['date']} @ {latest['close']:,.2f} 点")
                saved += 1

        # ── 海外指数 ──────────────────────────────────────────────────────────
        print("\nFetching global indices...")
        for cfg in GLOBAL_CONFIGS:
            print(f"  {cfg['name']} ({cfg['symbol']})...")
            entries = fetch_global_index(cfg)
            if entries is None:
                failed.append(cfg["key"])
                continue
            db_upsert_index(conn, cfg["key"], cfg["symbol"],
                            cfg["name"], cfg["market"], cfg["unit"])
            db_upsert_prices(conn, cfg["key"], entries)
            latest = entries[0]
            print(f"    [OK] {latest['date']} @ {latest['close']:,.2f} 点")
            saved += 1

        # ── 资金流向 ──────────────────────────────────────────────────────────
        print("\nFetching Stock Connect capital flows...")
        for cfg in FLOW_CONFIGS:
            print(f"  {cfg['name']}...")
            entries = fetch_capital_flow(cfg)
            if entries is None:
                failed.append(cfg["key"])
                continue
            db_upsert_index(conn, cfg["key"], cfg["symbol"],
                            cfg["name"], cfg["market"], cfg["unit"])
            db_upsert_prices(conn, cfg["key"], entries)
            latest = entries[0]
            sign = "+" if (latest["close"] or 0) >= 0 else ""
            print(f"    [OK] {latest['date']} 净流入 {sign}{latest['close']:.2f} 亿元")
            saved += 1

        conn.commit()
        print(f"\nCommitted {saved} index dataset(s) to database.")

    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] {exc}")
        import traceback; traceback.print_exc()
        return 1
    finally:
        conn.close()

    if failed:
        print(f"\n[SUMMARY] {len(failed)} failed: {', '.join(failed)}")
        return 1

    print(f"\n[SUMMARY] All {saved} indices fetched and saved successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
