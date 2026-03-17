"""
fetch_crypto.py
Fetches real-time cryptocurrency spot prices via CoinGecko public API
and stores daily snapshots in PostgreSQL.

Run daily (or on demand) to accumulate price history.
No API key required for the free tier.
"""
import os
from datetime import date

import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

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
def fetch_coingecko() -> dict:
    """Returns {cg_id: row_dict} for all configured coins."""
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
    resp = requests.get(COINGECKO_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return {item["id"]: item for item in data}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("Connecting to database...")
    conn = psycopg2.connect(DATABASE_URL)
    _ensure_schema(conn)

    today = date.today().isoformat()

    print("\nFetching cryptocurrency prices from CoinGecko...")
    try:
        cg_data = fetch_coingecko()
    except Exception as exc:
        print(f"  [ERROR] CoinGecko request failed: {exc}")
        conn.close()
        return

    saved = 0
    failed = []

    for cfg in COIN_CONFIGS:
        row = cg_data.get(cfg["cg_id"])
        if row is None:
            print(f"  [SKIP] {cfg['cg_id']} — not returned by CoinGecko")
            failed.append(cfg["key"])
            continue

        close      = row.get("current_price")
        change_pct = row.get("price_change_percentage_24h")
        volume_24h = row.get("total_volume")
        high_24h   = row.get("high_24h")
        low_24h    = row.get("low_24h")

        if close is None:
            print(f"  [SKIP] {cfg['symbol']} — no price")
            failed.append(cfg["key"])
            continue

        db_upsert_coin(conn, cfg["key"], cfg["symbol"], cfg["name"])
        db_upsert_price(conn, cfg["key"], today, close, change_pct,
                        volume_24h, high_24h, low_24h)

        pct_str = f"{change_pct:+.2f}%" if change_pct is not None else "N/A"
        print(f"  [OK] {cfg['name']:12s} ({cfg['symbol']:10s})  "
              f"{close:>14,.4f} USD  {pct_str}")
        saved += 1

    print(f"\n[SUMMARY] {saved}/{len(COIN_CONFIGS)} coins saved for {today}.")
    if failed:
        print(f"  Failed: {', '.join(failed)}")

    conn.close()


if __name__ == "__main__":
    main()
