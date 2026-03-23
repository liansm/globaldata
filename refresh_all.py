"""
refresh_all.py — 一键刷新所有数据

1. 打印数据库各表的当前数据状态（行数、最新日期等）
2. 按顺序运行所有 fetch 脚本，显示每个脚本的耗时和结果

用法:
    python refresh_all.py            # 先展示状态，再刷新全部
    python refresh_all.py --status   # 仅展示状态，不刷新
    python refresh_all.py --fetch    # 仅刷新，不展示状态
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
BASE_DIR     = Path(__file__).parent

# 刷新脚本执行顺序
FETCH_SCRIPTS = [
    ("fetch_markets.py",        "A股/港股指数 + 沪深港通资金流向（历史日线）"),
    ("fetch_index_spot.py",     "A股指数实时快照（stock_zh_index_spot_sina）"),
    ("fetch_market_minutes.py", "A股指数分时 1 分钟 K 线"),
    ("fetch_commodities.py",    "大宗商品价格（黄金/铜/油/煤炭等）"),
    ("fetch_crypto.py",         "加密货币价格（CoinGecko）"),
]

# ─────────────────────────────────────────────────────────────────────────────
# ANSI colors
# ─────────────────────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(s):   return f"{GREEN}{s}{RESET}"
def err(s):  return f"{RED}{s}{RESET}"
def warn(s): return f"{YELLOW}{s}{RESET}"
def hdr(s):  return f"{BOLD}{CYAN}{s}{RESET}"


# ─────────────────────────────────────────────────────────────────────────────
# Status queries
# ─────────────────────────────────────────────────────────────────────────────
STATUS_QUERIES = [
    {
        "title": "commodities  (大宗商品定义)",
        "sql": """
            SELECT COUNT(*) AS total,
                   COUNT(DISTINCT commodity) AS commodities
            FROM commodities
        """,
        "cols": ["total", "commodities"],
    },
    {
        "title": "prices  (大宗商品日线价格)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT commodity_key) AS commodities,
                   MIN(price_date)::text AS earliest,
                   MAX(price_date)::text AS latest
            FROM prices
        """,
        "cols": ["rows", "commodities", "earliest", "latest"],
    },
    {
        "title": "market_indices  (指数/资金流向定义)",
        "sql": """
            SELECT COUNT(*) AS total,
                   STRING_AGG(DISTINCT market, ' / ' ORDER BY market) AS markets
            FROM market_indices
        """,
        "cols": ["total", "markets"],
    },
    {
        "title": "index_prices  (指数历史日线)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT index_key) AS indices,
                   MIN(price_date)::text AS earliest,
                   MAX(price_date)::text AS latest
            FROM index_prices
        """,
        "cols": ["rows", "indices", "earliest", "latest"],
    },
    {
        "title": "index_spot  (A股实时快照)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   MAX(spot_date)::text AS spot_date,
                   MAX(updated_at AT TIME ZONE 'Asia/Shanghai')::text AS last_updated
            FROM index_spot
        """,
        "cols": ["rows", "spot_date", "last_updated"],
        "optional": True,
    },
    {
        "title": "index_minutes  (A股分时 1 分钟)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT index_key) AS indices,
                   MIN(DATE(dt))::text AS earliest_day,
                   MAX(DATE(dt))::text AS latest_day,
                   MAX(dt AT TIME ZONE 'UTC')::text AS latest_dt
            FROM index_minutes
        """,
        "cols": ["rows", "indices", "earliest_day", "latest_day", "latest_dt"],
        "optional": True,
    },
    {
        "title": "crypto_coins  (加密货币定义)",
        "sql": """
            SELECT COUNT(*) AS total
            FROM crypto_coins
        """,
        "cols": ["total"],
        "optional": True,
    },
    {
        "title": "crypto_prices  (加密货币日线)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT coin_key) AS coins,
                   MIN(price_date)::text AS earliest,
                   MAX(price_date)::text AS latest
            FROM crypto_prices
        """,
        "cols": ["rows", "coins", "earliest", "latest"],
        "optional": True,
    },
]


def print_status():
    print(hdr("\n══════════════  数据库状态  ══════════════\n"))
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(err(f"[FATAL] 无法连接数据库: {exc}"))
        return

    with conn:
        for q in STATUS_QUERIES:
            title    = q["title"]
            cols     = q["cols"]
            optional = q.get("optional", False)
            try:
                with conn.cursor() as cur:
                    cur.execute(q["sql"])
                    row = cur.fetchone()
                pairs = "  ".join(f"{c}={ok(str(v))}" for c, v in zip(cols, row))
                print(f"  {BOLD}{title}{RESET}")
                print(f"    {pairs}")
            except psycopg2.errors.UndefinedTable:
                if optional:
                    print(f"  {BOLD}{title}{RESET}")
                    print(f"    {warn('表不存在（尚未初始化）')}")
                else:
                    print(f"  {BOLD}{title}{RESET}")
                    print(f"    {err('表不存在')}")
                conn.rollback()
            except Exception as exc:
                print(f"  {BOLD}{title}{RESET}")
                print(f"    {err(str(exc))}")
                conn.rollback()
            print()

    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Run fetch scripts
# ─────────────────────────────────────────────────────────────────────────────
def run_fetches():
    print(hdr("\n══════════════  开始刷新数据  ══════════════\n"))
    python = sys.executable
    results = []

    for script, desc in FETCH_SCRIPTS:
        path = BASE_DIR / script
        if not path.exists():
            print(f"  {warn('[SKIP]')} {script}  ({desc})  — 文件不存在")
            results.append((script, "skip", 0))
            continue

        print(f"  {CYAN}▶ {script}{RESET}  {desc}")
        t0 = time.time()
        proc = subprocess.run(
            [python, str(path)],
            capture_output=False,   # 让输出直接打印到终端
            text=True,
        )
        elapsed = time.time() - t0
        status  = "ok" if proc.returncode == 0 else "fail"

        if proc.returncode == 0:
            print(f"    {ok('✔ 完成')}  耗时 {elapsed:.1f}s\n")
        else:
            print(f"    {err(f'✘ 失败  exit={proc.returncode}  耗时 {elapsed:.1f}s')}\n")

        results.append((script, status, elapsed))

    # 汇总
    print(hdr("══════════════  刷新汇总  ══════════════\n"))
    total_ok   = sum(1 for _, s, _ in results if s == "ok")
    total_fail = sum(1 for _, s, _ in results if s == "fail")
    total_skip = sum(1 for _, s, _ in results if s == "skip")
    total_time = sum(t for _, _, t in results)

    for script, status, elapsed in results:
        tag = ok("✔") if status == "ok" else (warn("—") if status == "skip" else err("✘"))
        print(f"  {tag}  {script:<35s}  {elapsed:.1f}s")

    print()
    print(f"  完成: {ok(str(total_ok))}  失败: {err(str(total_fail)) if total_fail else '0'}  跳过: {total_skip}  总耗时: {total_time:.1f}s")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="globaldata 数据刷新工具")
    parser.add_argument("--status", action="store_true", help="仅展示数据库状态")
    parser.add_argument("--fetch",  action="store_true", help="仅运行 fetch 脚本")
    args = parser.parse_args()

    show_status = not args.fetch   # 默认展示状态
    run_fetch   = not args.status  # 默认运行刷新

    if show_status:
        print_status()

    if run_fetch:
        run_fetches()

    if show_status and not args.fetch:
        print(hdr("\n══════════════  刷新后数据库状态  ══════════════"))
        print_status()


if __name__ == "__main__":
    main()
