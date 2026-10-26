"""
fetch_crypto.py
加密货币日线：**历史直接回补** + 当日快照。源 = CoinGecko 免费档（无需 key）。

两个接口，缺一不可，**别只留一个**：

    ① 历史日线  GET /coins/{id}/market_chart/range?vs_currency=usd&from=&to=
       一次给 **365 天**日级序列（prices / market_caps / total_volumes）。
       这是 2026-10-26 新增的 —— 此前只抓当日快照，历史全靠每天跑一次攒，
       一旦定时任务漏跑就是永久空洞（实测出现过 12 / 13 / 18 天的断档）。
       跑一次即可把整年补齐，**且每次跑都自愈近一年的任何漏洞**。

    ② 当日快照  GET /coins/markets
       只给「此刻」的现价 + 24h 涨跌幅 + 滚动 24h 高低 —— 这三样历史接口没有
       （market_chart 只有 price / volume / market_cap，**没有 change_pct、没有 high/low**）。
       所以快照不能省，它是 change_pct / high_24h / low_24h 的唯一来源。

⚠ 三条硬约束（都是实测的，改动前先看）：

    1. **免费档时间窗上限 = 365 天**。`days=366` / `days=max` 直接 401
       `error_code 10012 "exceeds the allowed time range"`。想更早的历史要么付费，
       要么换源（见下）。
    2. **限流 ~5 次/分钟**（按 IP）。10 个币 = 10 次请求，必须节流；
       触发后返回 429 + `Retry-After: 60`，脚本按该头等待后重试。
       这也是为什么「每日跑全量 365 天」只有 ~3 分钟：瓶径是限流不是带宽。
    3. **粒度随窗口自动变**：range 跨度 < 90 天 → **小时级**（7 天返 168 点），
       ≥ 90 天 → 日级。所以**不要**用小窗口做增量，会拿到小时点，
       按日期去重时同一天被覆盖成「当天最后一个小时」，口径就与日线不一致了。
       → 增量也一律拉满 365 天，靠 upsert 幂等，别贪这点带宽。

⚠ 写入顺序**必须「先历史、后快照」**：两者都会落在同一个 price_date（今天）。
    反过来的话，历史里「今天 00:00 UTC」那个点会把刚抓的实时价覆盖回去。

⚠ 历史日线 **不能把快照字段抹成 NULL**：见 db_upsert_history 里的 COALESCE，
    否则每天跑一遍就把历次积累的 change_pct / high_24h / low_24h 全清空。

数据保留：CoinGecko 只认滚动 365 天。**比这更早的数据只能靠本站自己攒**
    （每日 upsert 累积下来的行不会被删）。想一次性补到上市日（BTC 可追到 2017-10），
    换 OKX 公开 K 线（见下方「备用深历史源」注释）。

Usage
-----
    python fetch_crypto.py               # 历史(365天) + 当日快照（日常跑这个）
    python fetch_crypto.py --backfill    # 同上，但强制全量重拉历史
    python fetch_crypto.py --no-history  # 只写当日快照（限流期的降级路径）
    python fetch_crypto.py --days 180    # 历史窗口改成 180 天（≥90 才是日级）
    python fetch_crypto.py --dry-run     # 只抓不写
"""
import os
import sys
import time
from datetime import date, datetime, timezone

import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_RANGE = "https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart/range"

# 免费档硬上限 365 天；>=90 天才返回日级粒度（<90 会给小时级，见 docstring 第 3 条）
HISTORY_DAYS_MAX = 365
HISTORY_DAYS_MIN_DAILY = 90

# 限流 ~5 次/分钟（按 IP）。13s 间隔 ≈ 4.6 次/分钟，留余量。
# 实测连续 3 次就 429（前面还有其他请求占额度），所以宁可慢一点。
THROTTLE_SEC = 13
RETRY_ON_429 = 4

# ── 备用深历史源（未接入，留作方案记录）─────────────────────────────────────
# OKX 公开 K 线 GET /api/v5/market/history-candles?instId=BTC-USDT&bar=1D&after=<ms>&limit=100
#   优点：翻页无上限，实测 BTC-USDT 可回溯到 **2017-10-10**（上市日）；
#         返回完整 OHLCV（真 K 线，能画蜡烛图，这是 CoinGecko 给不了的）。
#   缺点：只有交易对，**USDT / USDC 这两个稳定币拿不到**（它们在 USDT 计价体系里没有对）；
#         计价是 USDT 不是 USD（USDT 本身有 ±0.1% 脱锚），与现有 USD 口径不同源。
#   → 若将来要补 2017~2025 的历史，建议**只给 BTC/ETH 等主流币补**，
#     并在前端标明「2026-10 之前的数据来自 OKX（USDT 计价）」。
# 其他实测行的结论（2026-10-26）：
#   CoinPaprika 免费档同样卡 365 天（402 "before ... not allowed in this plan"）；
#   CryptoCompare 现在要 API key（401）；Binance 对本机出口 451 地区封锁；
#   Kraken 只回 720 根、Coinbase 只回 300 根（约 1~2 年，对日线够但比 OKX 浅）。

# ---------------------------------------------------------------------------
# Coins to track  (key, CoinGecko id, display symbol, display name)
# ---------------------------------------------------------------------------
COIN_CONFIGS = [
    {"key": "btc",  "cg_id": "bitcoin",       "symbol": "BTC/USD", "name": "Bitcoin"},
    {"key": "eth",  "cg_id": "ethereum",       "symbol": "ETH/USD", "name": "Ethereum"},
    {"key": "bnb",  "cg_id": "binancecoin",    "symbol": "BNB/USD", "name": "BNB"},
    {"key": "sol",  "cg_id": "solana",         "symbol": "SOL/USD", "name": "Solana"},
    {"key": "xrp",  "cg_id": "ripple",         "symbol": "XRP/USD", "name": "XRP"},
    {"key": "usdt", "cg_id": "tether",         "symbol": "USDT/USD","name": "Tether"},
    {"key": "usdc", "cg_id": "usd-coin",       "symbol": "USDC/USD","name": "USD Coin"},
    {"key": "ada",  "cg_id": "cardano",        "symbol": "ADA/USD", "name": "Cardano"},
    {"key": "doge", "cg_id": "dogecoin",       "symbol": "DOGE/USD","name": "Dogecoin"},
    {"key": "avax", "cg_id": "avalanche-2",    "symbol": "AVAX/USD","name": "Avalanche"},
]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def _ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_coins (
                key        VARCHAR(60) PRIMARY KEY,
                symbol     VARCHAR(30),
                name       VARCHAR(100),
                unit       VARCHAR(20) DEFAULT 'USD',
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crypto_prices (
                id          BIGSERIAL PRIMARY KEY,
                coin_key    VARCHAR(60) NOT NULL REFERENCES crypto_coins(key),
                price_date  DATE        NOT NULL,
                close       NUMERIC(20,6),
                change_pct  NUMERIC(8,4),
                volume_24h  NUMERIC(24,2),
                high_24h    NUMERIC(20,6),
                low_24h     NUMERIC(20,6),
                UNIQUE (coin_key, price_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS crypto_prices_key_date
                ON crypto_prices (coin_key, price_date DESC)
        """)
    conn.commit()
    print("  [OK] Schema ready.")


def db_upsert_coin(conn, key, symbol, name, unit="USD"):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO crypto_coins (key, symbol, name, unit, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (key) DO UPDATE
                SET symbol     = EXCLUDED.symbol,
                    name       = EXCLUDED.name,
                    unit       = EXCLUDED.unit,
                    updated_at = NOW()
        """, (key, symbol, name, unit))
    conn.commit()


def db_upsert_price(conn, key, price_date, close, change_pct,
                    volume_24h, high_24h, low_24h):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO crypto_prices
                (coin_key, price_date, close, change_pct, volume_24h, high_24h, low_24h)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (coin_key, price_date) DO UPDATE
                SET close      = EXCLUDED.close,
                    change_pct = EXCLUDED.change_pct,
                    volume_24h = EXCLUDED.volume_24h,
                    high_24h   = EXCLUDED.high_24h,
                    low_24h    = EXCLUDED.low_24h
        """, (key, price_date, close, change_pct, volume_24h, high_24h, low_24h))
    conn.commit()


# ---------------------------------------------------------------------------
# Fetch from CoinGecko
# ---------------------------------------------------------------------------
def fetch_coingecko(sess=None) -> dict:
    """当日快照。Returns {cg_id: row_dict} for all configured coins.

    ⚠ 这一步紧跟在 10 次历史请求之后，**大概率撞限流** —— 必须走 _get_with_retry，
    否则整个快照环节拿不到数据，而 change_pct / high_24h / low_24h 没有别的来源。
    """
    ids = ",".join(c["cg_id"] for c in COIN_CONFIGS)
    params = {
        "vs_currency": "usd",
        "ids": ids,
        "order": "market_cap_desc",
        "per_page": len(COIN_CONFIGS),
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    sess = sess or requests.Session()
    r = _get_with_retry(sess, COINGECKO_URL, params=params, timeout=20)
    if not hasattr(r, "status_code"):
        raise RuntimeError(f"请求始终失败（{type(r).__name__}）")
    if r.status_code != 200:
        raise RuntimeError(f"http {r.status_code} {r.text[:120]}")
    data = r.json()
    return {item["id"]: item for item in data}


# ---------------------------------------------------------------------------
# 历史日线（CoinGecko market_chart/range）
# ---------------------------------------------------------------------------
def _get_with_retry(sess, url, params=None, timeout=30):
    """429 时按 Retry-After 等待后重试。

    免费档限流 ~5 次/分钟，一次跑 10 个币必然撞上；不重试就会「跑完了但一半币没数据」，
    而脚本又不会报错 —— 这类静默缺失比直接失败更难发现。
    """
    last = None
    for attempt in range(RETRY_ON_429):
        try:
            r = sess.get(url, params=params, timeout=timeout)
        except Exception as exc:
            last = exc
            time.sleep(3 * (attempt + 1))
            continue

        if r.status_code == 200:
            return r
        if r.status_code == 429:
            # 优先用服务端给的 Retry-After（实测 60s）；没有就指数退避
            try:
                wait = int(r.headers.get("Retry-After", ""))
            except (TypeError, ValueError):
                wait = 0
            if not wait:
                wait = 20 * (attempt + 1)
            print(f"      429 限流，等待 {wait}s 后重试（{attempt + 1}/{RETRY_ON_429}）")
            time.sleep(wait)
            last = r
            continue
        return r          # 其他状态码交给调用方判断，别把 404 当限流重试
    return last


def fetch_history(sess, cg_id, days=HISTORY_DAYS_MAX):
    """拉最近 `days` 天的**日级**序列 → {date_str: {close, volume}}。

    `to` 取当下、`from` 取 days 天前，接口按跨度自动选粒度（>=90 天才日级）。
    返回的 ts 是 UTC 毫秒，直接取 UTC 日期 —— **不要转本地时区**，
    否则东八区会把 UTC 00:00 的点算到前一天，和库里已有的快照日期错位。
    """
    now = int(time.time())
    frm = now - days * 86400
    r = _get_with_retry(
        sess,
        COINGECKO_RANGE.format(cg_id=cg_id),
        params={"vs_currency": "usd", "from": frm, "to": now},
    )
    if not hasattr(r, "status_code"):
        raise RuntimeError(f"{cg_id}: 请求始终失败（{type(r).__name__}）")
    if r.status_code != 200:
        raise RuntimeError(f"{cg_id}: http {r.status_code} {r.text[:120]}")

    j = r.json()
    prices = j.get("prices") or []
    vols = j.get("total_volumes") or []
    if not prices:
        raise RuntimeError(f"{cg_id}: 返回空序列")

    # 同一天可能出现多个点（粒度万一掉到小时级时），保留**最后一个**（离收盘最近）
    out = {}
    for ts, px in prices:
        d = datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d")
        out[d] = {"close": px, "volume": None}
    for ts, v in vols:
        d = datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d")
        if d in out:
            out[d]["volume"] = v
    return out


def db_upsert_history(conn, key, rows):
    """历史日线 upsert。**只写 close / volume，绝不碰快照字段。**

    `SET` 列表里**故意不列** change_pct / high_24h / low_24h —— 历史接口本就没有这三样，
    一旦列进去（哪怕是写 NULL）就会把历次快照攒下来的值洗空。
    volume_24h 用 COALESCE 是因为历史序列偶尔缺点位，有值才覆盖。
    实测（事务内回滚验证）：close 被更新、change_pct / high_24h / volume 全部保持原值。
    """
    if not rows:
        return 0
    data = [(key, d, v["close"], v["volume"]) for d, v in sorted(rows.items())]
    with conn.cursor() as cur:
        from psycopg2.extras import execute_values
        execute_values(cur, """
            INSERT INTO crypto_prices (coin_key, price_date, close, volume_24h)
            VALUES %s
            ON CONFLICT (coin_key, price_date) DO UPDATE
                SET close      = EXCLUDED.close,
                    volume_24h = COALESCE(EXCLUDED.volume_24h, crypto_prices.volume_24h)
        """, data, page_size=500)
    conn.commit()
    return len(data)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    argv = sys.argv[1:]
    dry_run    = "--dry-run" in argv
    no_history = "--no-history" in argv
    force_back = "--backfill" in argv

    days = HISTORY_DAYS_MAX
    if "--days" in argv:
        try:
            days = int(argv[argv.index("--days") + 1])
        except (IndexError, ValueError):
            print("--days 需要一个整数参数（如 --days 180）")
            return 2
        if days > HISTORY_DAYS_MAX:
            print(f"--days 最大 {HISTORY_DAYS_MAX}（免费档硬上限，超过直接 401）；已按 {HISTORY_DAYS_MAX} 处理")
            days = HISTORY_DAYS_MAX
        if days < HISTORY_DAYS_MIN_DAILY:
            print(f"⚠ --days={days} < {HISTORY_DAYS_MIN_DAILY}：接口会返回**小时级**而非日级，"
                  f"同一天会被折叠成最后一个小时点，口径与日线不一致。建议 ≥ {HISTORY_DAYS_MIN_DAILY}。")

    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    _ensure_schema(conn)

    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})

    # ══ ① 历史日线（必须先跑，见 docstring 的「写入顺序」）══════════════════
    hist_rows = 0
    hist_ok, hist_fail = [], []
    if no_history:
        print("\n[1/2] 历史日线：--no-history，跳过")
    else:
        print(f"\n[1/2] 历史日线：最近 {days} 天，{len(COIN_CONFIGS)} 个币"
              f"（限流 ~5/min，预计 {len(COIN_CONFIGS) * THROTTLE_SEC // 60 + 1} 分钟）")
        for i, cfg in enumerate(COIN_CONFIGS):
            try:
                rows = fetch_history(sess, cfg["cg_id"], days)
                if dry_run:
                    print(f"  [{i+1:2d}/{len(COIN_CONFIGS)}] {cfg['name']:12s} "
                          f"{len(rows)} 天（{min(rows)} → {max(rows)}）[dry-run 不写]")
                else:
                    n = db_upsert_history(conn, cfg["key"], rows)
                    hist_rows += n
                    print(f"  [{i+1:2d}/{len(COIN_CONFIGS)}] {cfg['name']:12s} "
                          f"{n} 天（{min(rows)} → {max(rows)}）")
                hist_ok.append(cfg["key"])
            except Exception as exc:
                hist_fail.append(cfg["key"])
                print(f"  [{i+1:2d}/{len(COIN_CONFIGS)}] {cfg['name']:12s} ✗ {str(exc)[:90]}")
            # 最后一个不用等
            if i < len(COIN_CONFIGS) - 1:
                time.sleep(THROTTLE_SEC)
        print(f"  历史合计 {hist_rows} 行（成功 {len(hist_ok)} / 失败 {len(hist_fail)}）")

    # ══ ② 当日快照（change_pct / high_24h / low_24h 的唯一来源）══════════════
    today = date.today().isoformat()
    print("\n[2/2] 当日快照（CoinGecko /coins/markets）...")
    try:
        cg_data = fetch_coingecko(sess)
    except Exception as exc:
        print(f"  [ERROR] 快照请求失败: {exc}")
        print("  ⚠ 历史已写入，快照字段（涨跌幅/24h高低）本次未更新")
        conn.close()
        return 1 if hist_fail else 0

    saved = 0
    failed = []

    for i, cfg in enumerate(COIN_CONFIGS):
        row = cg_data.get(cfg["cg_id"])
        if row is None:
            print(f"  [{i+1:2d}] [SKIP] {cfg['cg_id']} — not returned by CoinGecko")
            failed.append(cfg["key"])
            continue

        close      = row.get("current_price")
        change_pct = row.get("price_change_percentage_24h")
        volume_24h = row.get("total_volume")
        high_24h   = row.get("high_24h")
        low_24h    = row.get("low_24h")

        if close is None:
            print(f"  [{i+1:2d}] [SKIP] {cfg['symbol']} — no price")
            failed.append(cfg["key"])
            continue

        if not dry_run:
            db_upsert_coin(conn, cfg["key"], cfg["symbol"], cfg["name"])
            db_upsert_price(conn, cfg["key"], today, close, change_pct,
                            volume_24h, high_24h, low_24h)

        pct_str = f"{change_pct:+.2f}%" if change_pct is not None else "N/A"
        print(f"  [{i+1:2d}] {cfg['name']:12s} ({cfg['symbol']:10s})  "
              f"{close:>14,.4f} USD  {pct_str}")
        saved += 1

    print(f"\n[SUMMARY] {today}")
    print(f"  历史日线  : {hist_rows} 行（{len(hist_ok)} 成功 / {len(hist_fail)} 失败）"
          + (" [dry-run 未写]" if dry_run else ""))
    print(f"  当日快照  : {saved}/{len(COIN_CONFIGS)} 币"
          + (" [dry-run 未写]" if dry_run else ""))
    if hist_fail:
        print(f"  历史失败  : {', '.join(hist_fail)}（多为限流，重跑一次通常就好）")
    if failed:
        print(f"  快照失败  : {', '.join(failed)}")

    conn.close()

    # 历史全挂才算失败；部分失败在限流下是常态，别把 SIG 判成坏
    if not no_history and hist_fail and len(hist_fail) == len(COIN_CONFIGS):
        return 1
    return 0


if __name__ == "__main__":
    main()
