#!/usr/bin/env python3
"""
Commodity Intraday Minutes Fetcher
====================================
Fetches 1-minute OHLCV bars for domestic futures main contracts via
akshare.futures_zh_minute_sina(symbol) and upserts into commodity_minutes.

Strategy
--------
1. For each tracked domestic commodity, call futures_zh_realtime to find the
   main contract (highest open-interest row) and read its contract code.
   Some product names cause KeyError in futures_zh_realtime on Windows due to
   encoding mismatches in the internal symbol_mark dict — for those we look up
   the exact symbol string from futures_symbol_mark() using the ASCII mark value.
2. Call futures_zh_minute_sina(symbol=<contract_code>) for intraday bars.
3. Upsert only today's bars (to avoid overwriting historical data with stale
   after-hours ticks from rolled contracts).

Run this script any time during or after market hours (09:00–15:30 CST).
"""

import os
import sys
from datetime import date, datetime

import akshare as ak
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

# commodity_key → (chinese_name, sina_mark_value_or_None)
# mark value is used for products whose Chinese name causes KeyError in
# futures_zh_realtime (encoding mismatch on Windows).  None = direct name works.
# ⚠ 2026-10-31 查实：mark 原来大多是**手写猜的**，官方表里不存在、或撞上别家品种 ——
#   甲醇 mh_qh 实际是**棉花**（分时图画的是棉花分钟线），锌 xj_qh 是橡胶，
#   尿素/乙二醇/苯乙烯/橡胶 走 None 直接按中文名查（编码不符时静默失败）。
#   → 全部按 `ak.futures_symbol_mark()` 实际输出改正；查不到就 WARN 跳过，绝不猜。
DOMESTIC_MAP = {
    "copper":            ("沪铜",   "tong_qh"),
    "aluminum":          ("沪铝",   "lv_qh"),
    "lithium_carbonate": ("碳酸锂", "lc_qh"),
    "polysilicon":       ("多晶硅", "ps_qh"),
    "methanol":          ("郑醇",   "zc_qh"),   # 郑商所甲醇，官方名「郑醇」
    "urea":              ("尿素",   "ns_qh"),
    "meg":               ("乙二醇", "yec_qh"),
    "styrene":           ("苯乙烯", "byx_qh"),
    "polypropylene":     ("PP",    "jbx_qh"),
    "natural_rubber":    ("橡胶",   "xj_qh"),
    "zinc":              ("沪锌",   "xing_qh"),
    "lead":              ("沪铅",   "qian_qh"),
    "tin":               ("沪锡",   "xi_qh"),
    "nickel":            ("沪镍",   "ni_qh"),
    "corn":              ("玉米",   "hym_qh"),
    "soybean_oil":       ("豆油",   "dy_qh"),
    "eggs":              ("鸡蛋",   "jd_qh"),
    "live_hog":          ("生猪",   "lh_qh"),
    "cotton":            ("棉花",   "mh_qh"),
    "sugar":             ("白糖",   "bst_qh"),
}

# ---------------------------------------------------------------------------
# Symbol mark table (loaded once)
# ---------------------------------------------------------------------------

_mark_df: pd.DataFrame | None = None

def _get_mark_df() -> pd.DataFrame:
    global _mark_df
    if _mark_df is None:
        try:
            _mark_df = ak.futures_symbol_mark()
        except Exception as exc:
            print(f"  [WARN] futures_symbol_mark() failed: {exc}")
            _mark_df = pd.DataFrame(columns=["symbol", "mark"])
    return _mark_df


def _symbol_from_mark(mark: str) -> str | None:
    """Return the exact symbol string for a given ASCII mark value (encoding-safe)."""
    df = _get_mark_df()
    if df.empty:
        return None
    rows = df[df["mark"] == mark]
    return str(rows["symbol"].iloc[0]) if not rows.empty else None


def _safe_float(val) -> float | None:
    try:
        f = float(str(val).replace(",", "").strip())
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _last_daily_close(conn, commodity_key: str) -> float | None:
    """该品种 prices 表里最新一条收盘价 —— 用于「分时价 vs 日线」量级闸门。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT price FROM prices WHERE commodity_key = %s "
            "ORDER BY price_date DESC LIMIT 1",
            (commodity_key,),
        )
        row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else None


# ---------------------------------------------------------------------------
# Step 1: determine main contract code for each product
# ---------------------------------------------------------------------------

def get_main_contract_code(product_name: str, mark: str | None) -> str | None:
    """
    Use futures_zh_realtime to list all active contracts for a product,
    pick the one with the highest open interest (position), and return its
    lowercase contract code (e.g. 'cu2506').

    For products whose Chinese name causes a KeyError in futures_zh_realtime
    (encoding mismatch on Windows), supply the ASCII mark value so we can
    look up the exact symbol string from futures_symbol_mark().
    """
    # Resolve the symbol string to pass to futures_zh_realtime
    if mark:
        symbol = _symbol_from_mark(mark)
        if not symbol:
            print(f"    [WARN] mark '{mark}' not found in futures_symbol_mark(); "
                  f"falling back to '{product_name}'")
            symbol = product_name
    else:
        symbol = product_name

    try:
        df = ak.futures_zh_realtime(symbol=symbol)
    except Exception as exc:
        print(f"    [WARN] futures_zh_realtime({symbol!r}): {exc}")
        return None

    if df is None or df.empty:
        print(f"    [WARN] futures_zh_realtime({symbol!r}): empty")
        return None

    # Sort by open interest (position) descending → main contract is first row
    if "position" in df.columns:
        df = df.copy()
        df["position"] = pd.to_numeric(df["position"], errors="coerce")
        df = df.sort_values("position", ascending=False)

    code = str(df.iloc[0]["symbol"]).strip().lower()
    return code if code and code != "nan" else None


# ---------------------------------------------------------------------------
# Step 2: fetch minute bars for a contract
# ---------------------------------------------------------------------------

def fetch_minutes(contract_code: str, all_days: bool = False) -> pd.DataFrame | None:
    """
    Fetch 1-minute OHLCV bars via futures_zh_minute_sina.
    Returns a cleaned DataFrame with columns [dt, open, high, low, close, volume, turnover]
    or None on failure.

    ``all_days``: 默认只留「今天」的 bars（日常增量）。``--backfill`` 时保留源侧返回的
    全部日期 —— 新浪这个接口只给最近约 1000 根（≈3 个交易日），所以「补历史」最多补 3 天；
    更早的分钟线目前没有来源（未找到可回补的免费接口）。
    """
    try:
        df = ak.futures_zh_minute_sina(symbol=contract_code)
    except Exception as exc:
        print(f"    [ERROR] futures_zh_minute_sina({contract_code}): {exc}")
        return None

    if df is None or df.empty:
        print(f"    [WARN] {contract_code}: empty minute data")
        return None

    # Detect datetime column — futures_zh_minute_sina uses 'datetime' (English)
    dt_col = next(
        (c for c in df.columns if c == "datetime" or "时间" in c or "日期" in c
         or "date" in c.lower()),
        df.columns[0]
    )
    # Support both English (futures_zh_minute_sina) and Chinese column names
    col_map = {
        "open":     next((c for c in df.columns
                          if c == "open"  or "开" in c), None),
        "high":     next((c for c in df.columns
                          if c == "high"  or "高" in c), None),
        "low":      next((c for c in df.columns
                          if c == "low"   or "低" in c), None),
        "close":    next((c for c in df.columns
                          if c == "close" or "收" in c or "价" in c), None),
        "volume":   next((c for c in df.columns
                          if c == "volume" or "量" in c), None),
        "turnover": next((c for c in df.columns
                          if c in ("amount", "turnover") or "额" in c), None),
    }

    rows = []
    today_str = date.today().isoformat()

    for _, row in df.iterrows():
        raw_dt = row[dt_col]
        # Parse datetime — might be string or datetime object
        try:
            if isinstance(raw_dt, (pd.Timestamp, datetime)):
                dt = raw_dt
            else:
                raw_str = str(raw_dt).strip()
                # "2025-05-10 09:31:00" or "09:31:00" (time only)
                if len(raw_str) <= 8:
                    dt = datetime.strptime(f"{today_str} {raw_str}", "%Y-%m-%d %H:%M:%S")
                else:
                    dt = pd.to_datetime(raw_str)
        except Exception:
            continue

        # 未来时间的 bar 一律丢掉：新浪夜盘偶尔吐一条标成「下一交易日 00:00」的占位行
        # （实测 2026-10-31 出现 dt=2026-11-02 00:00），它会把「最近日期」顶到未来，
        # 详情页分时只取最近日期 → 图就剩 1 根。
        if dt > datetime.now():
            continue

        # Only keep today's bars（--backfill 时不过滤：源侧最多给 3 个交易日）
        if not all_days and dt.strftime("%Y-%m-%d") != today_str:
            continue

        # Filter out invalid/non-trading times (e.g. midnight 00:00:00 artifacts)
        # Domestic futures trade 09:00-15:30 and 21:00-02:30 only
        hhmm = dt.hour * 100 + dt.minute
        if not (900 <= hhmm <= 1530 or hhmm >= 2100 or hhmm <= 230):
            continue

        rows.append({
            "dt":       dt,
            "open":     _safe_float(row[col_map["open"]])   if col_map["open"]     else None,
            "high":     _safe_float(row[col_map["high"]])   if col_map["high"]     else None,
            "low":      _safe_float(row[col_map["low"]])    if col_map["low"]      else None,
            "close":    _safe_float(row[col_map["close"]])  if col_map["close"]    else None,
            "volume":   _safe_float(row[col_map["volume"]]) if col_map["volume"]   else None,
            "turnover": _safe_float(row[col_map["turnover"]]) if col_map["turnover"] else None,
        })

    if not rows:
        return None
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Database schema + upsert
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS commodity_minutes (
    id            BIGSERIAL PRIMARY KEY,
    commodity_key VARCHAR(60) NOT NULL REFERENCES commodities(key) ON DELETE CASCADE,
    dt            TIMESTAMP NOT NULL,
    open          NUMERIC(20,6),
    high          NUMERIC(20,6),
    low           NUMERIC(20,6),
    close         NUMERIC(20,6),
    volume        NUMERIC(24,4),
    turnover      NUMERIC(24,4),
    UNIQUE (commodity_key, dt)
);
CREATE INDEX IF NOT EXISTS idx_commodity_minutes_key_dt
    ON commodity_minutes (commodity_key, dt DESC);
"""

UPSERT_SQL = """
INSERT INTO commodity_minutes
    (commodity_key, dt, open, high, low, close, volume, turnover)
VALUES %s
ON CONFLICT (commodity_key, dt) DO UPDATE SET
    open     = EXCLUDED.open,
    high     = EXCLUDED.high,
    low      = EXCLUDED.low,
    close    = EXCLUDED.close,
    volume   = EXCLUDED.volume,
    turnover = EXCLUDED.turnover
"""


def db_upsert_minutes(conn, commodity_key: str, bars: pd.DataFrame):
    rows = [
        (commodity_key, r["dt"], r["open"], r["high"],
         r["low"], r["close"], r["volume"], r["turnover"])
        for _, r in bars.iterrows()
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    backfill = "--backfill" in sys.argv
    print("Connecting to database...")
    if backfill:
        print("  [MODE] --backfill：保留源侧返回的全部日期（新浪只给最近 ≈3 个交易日）")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()
        print("  [OK] Connected. Schema ready.\n")
    except psycopg2.OperationalError as exc:
        print(f"  [FATAL] Cannot connect: {exc}")
        return 1

    total_bars = 0
    failures = []

    for key, (product_name, mark) in DOMESTIC_MAP.items():
        print(f"── {key} ({product_name}) ─────────────────────────────────")

        # 1. Get main contract code via futures_zh_realtime
        contract = get_main_contract_code(product_name, mark)
        if not contract:
            print(f"    [SKIP] Could not determine main contract for {product_name}")
            failures.append(key)
            continue
        print(f"    main contract: {contract}")

        # 2. Fetch minute bars
        bars = fetch_minutes(contract, all_days=backfill)
        if bars is None or bars.empty:
            print(f"    [SKIP] No minute bars for {contract}")
            failures.append(key)
            continue
        print(f"    fetched {len(bars)} bars"
              + ("（源侧全部日期）" if backfill else " for today"))

        # 2.5 量级闸门：契约/品种映射错了（甲醇曾画成棉花的分钟线）这里能当场拦下。
        #     期货主力与自家日线收盘通常差 <5%，给 ±25% 余量。
        daily = _last_daily_close(conn, key)
        last_close = _safe_float(bars.iloc[-1].get("close"))
        if daily and daily > 0 and last_close and last_close > 0:
            ratio = last_close / daily
            if not (0.75 <= ratio <= 1.25):
                print(f"    [REJECT] last_close={last_close} vs 日线 {daily} "
                      f"→ 比值 {ratio:.2f}，疑似品种/合约映射错误，跳过")
                failures.append(key)
                continue
            print(f"    [OK] last_close={last_close} vs 日线 {daily}（比值 {ratio:.2f}）")

        # 3. Upsert
        try:
            n = db_upsert_minutes(conn, key, bars)
            print(f"    [DB] upserted {n} rows")
            total_bars += n
        except Exception as exc:
            conn.rollback()
            import traceback
            print(f"    [ERROR] DB upsert failed: {exc}")
            traceback.print_exc()
            failures.append(key)

    conn.close()

    print(f"\n[SUMMARY] {total_bars} minute bars upserted across "
          f"{len(DOMESTIC_MAP) - len(failures)} commodities.")
    if failures:
        print(f"[WARN] Failed: {', '.join(failures)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
