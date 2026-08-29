#!/usr/bin/env python3
"""
Baltic / Tanker Freight Index Fetcher (BDI 系列)
================================================
Fetches the Baltic Exchange freight index family via akshare's
`macro_china_freight_index()` and upserts it into the existing
market_indices / index_prices tables with market='航运'.

Data source
-----------
    ak.macro_china_freight_index()

Returns the most recent 5000 trading days (~20 years, from 2006-07 onward),
one row per business day, 8 columns. Unlike CCFI this endpoint **does**
include history, so the whole series can be backfilled on the first run.

Column → series mapping
-----------------------
    波罗的海综合运价指数BDI            → bdi    波罗的海干散货指数 (Baltic Dry)
    波罗的海好望角型船运价指数BCI       → bci    好望角型船 (Capesize)
    波罗的海超级大灵便型船BSI指数       → bsi    超级大灵便型船 (Supramax)
    油轮运价指数原油运价指数BDTI        → bdti   原油运输 (Dirty Tanker)
    油轮运价指数成品油运价指数BCTI      → bcti   成品油运输 (Clean Tanker)

Deliberately skipped (dead series — stale for a decade+):
    灵便型船综合运价指数BHMI     只有 1 行数据 (2009-01-28)
    HRCI国际集装箱租船指数       212 行，最后更新 2011-08-23

Incremental updates
-------------------
Before writing, the script queries MAX(price_date) per series and only
upserts rows strictly newer than that, so a daily refresh inserts a handful
of rows instead of re-writing all ~25k.

Usage
-----
    python fetch_bdi.py             # incremental fetch + upsert
    python fetch_bdi.py --full      # ignore existing data, re-write everything
    python fetch_bdi.py --dry-run   # fetch + print summary, no DB write
"""

import os
import sys
from datetime import date

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

DATE_COL = "截止日期"

# akshare 列名 → (index_key, 名称, symbol)
SERIES_MAP = {
    "波罗的海综合运价指数BDI":       ("bdi",  "BDI 波罗的海干散货",  "BDI"),
    "波罗的海好望角型船运价指数BCI":  ("bci",  "BCI 好望角型船",      "BCI"),
    "波罗的海超级大灵便型船BSI指数":  ("bsi",  "BSI 超级大灵便型船",  "BSI"),
    "油轮运价指数原油运价指数BDTI":   ("bdti", "BDTI 原油运输",       "BDTI"),
    "油轮运价指数成品油运价指数BCTI": ("bcti", "BCTI 成品油运输",     "BCTI"),
}

MARKET = "航运"
UNIT   = "点"

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

# 这些指数只有单值收盘，没有开高低/成交量
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


def db_init(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def db_get_latest_dates(conn, keys: list) -> dict:
    """Return {index_key: date} of the newest stored row per series."""
    if not keys:
        return {}
    with conn.cursor() as cur:
        cur.execute(LATEST_DATE_SQL, (keys,))
        return {k: v for k, v in cur.fetchall()}


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------
def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        return None if pd.isna(f) else round(f, 4)
    except (TypeError, ValueError):
        return None


def _norm_date(val) -> str | None:
    if val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()[:10]
    return s or None


def fetch_series() -> dict:
    """
    Call akshare and return {index_key: {"name","symbol","points":[(date, close), ...]}}.
    Points are sorted ascending by date.
    """
    try:
        df = ak.macro_china_freight_index()
    except Exception as exc:
        print(f"  [FATAL] macro_china_freight_index(): {exc}")
        return {}

    if df is None or df.empty:
        print("  [FATAL] macro_china_freight_index(): 空数据")
        return {}

    if DATE_COL not in df.columns:
        print(f"  [FATAL] 未找到日期列 '{DATE_COL}'，实际列: {list(df.columns)}")
        return {}

    missing = [c for c in SERIES_MAP if c not in df.columns]
    if missing:
        print(f"  [WARN] 以下列在接口返回中不存在，跳过: {missing}")

    result: dict = {}
    for col, (key, name, symbol) in SERIES_MAP.items():
        if col not in df.columns:
            continue
        points = []
        for d, v in zip(df[DATE_COL], df[col]):
            ds, fv = _norm_date(d), _safe_float(v)
            if ds is None or fv is None:
                continue
            points.append((ds, fv))
        points.sort(key=lambda x: x[0])
        result[key] = {"name": name, "symbol": symbol, "points": points}
        if points:
            print(f"  [OK] {name:20s} 解析 {len(points):5d} 个数据点"
                  f"  ({points[0][0]} ~ {points[-1][0]})")
        else:
            print(f"  [WARN] {name:20s} 无有效数据")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    argv     = sys.argv[1:]
    dry_run  = "--dry-run" in argv
    full     = "--full" in argv

    print("── 抓取波罗的海/油轮航运指数 (akshare) ──────────────────────────────")
    series = fetch_series()
    if not series:
        print("  [FATAL] 没有解析到任何序列")
        return 1

    total = sum(len(s["points"]) for s in series.values())
    print(f"\n  共 {len(series)} 条序列 / {total} 个数据点\n")

    for key, s in series.items():
        if s["points"]:
            print(f"  {s['name']:20s} 最新 {s['points'][-1][0]} @ {s['points'][-1][1]:,.2f}")

    if dry_run:
        print(f"\n[DRY-RUN] 跳过写库。将写入 {total} 行。")
        return 0

    print("\n连接数据库...")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        db_init(conn)
        print("  [OK] 已连接，schema 就绪")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] 无法连接: {exc}")
        return 1

    try:
        with conn.cursor() as cur:
            for key, s in series.items():
                cur.execute(UPSERT_INDEX_SQL, (key, s["symbol"], s["name"], MARKET, UNIT))
            print(f"  [OK] market_indices 就绪（{len(series)} 条序列, market='{MARKET}'）")

        # 增量：只写各序列最新日期之后的数据
        latest = {} if full else db_get_latest_dates(conn, list(series.keys()))
        rows: list = []
        for key, s in series.items():
            cutoff = latest.get(key)
            for ds, close in s["points"]:
                if cutoff is None or ds > cutoff.isoformat():
                    rows.append((key, ds, close))

        if not rows:
            print("  [OK] 无新增数据（已是最新）")
        else:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_PRICES_SQL, rows, page_size=2000)
            print(f"  [OK] index_prices 写入 {len(rows)} 行"
                  f"{'（全量）' if full else '（增量）'}")

        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"\n[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()

    print(f"\n[OK] 航运指数更新完成：{len(series)} 条序列。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
