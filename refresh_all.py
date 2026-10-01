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
#
# 元素格式：
#   (脚本名, 说明)
#   (脚本名, 说明, [额外参数...])
#   (脚本名, 说明, [额外参数...], {"requires_env": "环境变量名"})
#
# requires_env 的脚本在环境变量缺失时 **SKIP（不判失败）** —— 缺 key 是
# 「没配置」而不是「抓取坏了」，不该把整轮 refresh 的结论染红。
# 但会显式打印 [SKIP] 原因，绝不静默跳过。
FETCH_SCRIPTS = [
    ("fetch_markets.py",           "A股/港股指数 + 沪深港通资金流向（历史日线）"),
    ("fetch_index_spot.py",        "A股/港股指数实时快照（stock_zh/hk_index_spot_sina）"),
    ("fetch_market_minutes.py",    "A股指数分时 1 分钟 K 线"),
    ("fetch_ccfi.py",              "CCFI 中国出口集装箱运价指数（上海航交所，每周五发布）"),
    ("fetch_ccfi_history.py",      "CCFI 历史周线回补（GreenPacific，2023-04 起全航线）"),
    ("fetch_bdi.py",               "BDI 系列航运指数（akshare，日频含历史）"),
    ("fetch_ctfi.py",              "CTFI 中国进口原油运价指数 + VLCC 各航线 WS/TCE（上海航交所，日频滚存）"),
    ("fetch_ctfi_history.py",      "CTFI 历史回补（中华航运网周报/月报，月报 2021-09 起 / 周报 2025-07 起）"),
    ("fetch_commodities.py",       "大宗商品价格（黄金/铜/油/煤炭等）"),
    ("fetch_cement.py",            "水泥价格：CEMPI 指数 + P.O42.5 均价（中国水泥网，日频含历史）"),
    ("fetch_commodity_spot.py",    "期货实时快照（futures_zh_spot / futures_foreign_commodity_realtime）"),
    ("fetch_commodity_minutes.py", "期货分时 1 分钟 K 线（futures_zh_minute_sina）"),
    ("fetch_crypto.py",            "加密货币价格（CoinGecko）"),
    # 外汇：增量 = 中间价当日 + 中间价近 30 天 + 即期实时快照，秒级。
    # ⚠ 即期日线走新浪 getDayKLine（每对 1 请求、返回全历史），增量只写最近 7 天。
    #   全历史回补用 --full（中间价 2006 起分块 + 即期全量，约 1.5 分钟），手动跑：
    #   python fetch_fx.py --full
    ("fetch_fx.py",                "人民币汇率中间价（CFETS）+ 即期汇率（新浪），日频含历史"),
    ("fetch_fund_nav.py",          "公募基金最新净值快照（天天基金排行榜批量，4 个请求拿全市场）"),
    ("fetch_ipo_calendar.py",      "新股日历（东财 A股/北交所 + 财华社/AAStocks/东财 港股，秒级）"),
    ("fetch_vlcc_fleet.py",        "中国船东 VLCC 名录（招商轮船在役船队 + 中远海能 2021-06 官方快照）"),
    # VLCC 船位：主源 = HiFleet（付费，覆盖全球含波斯湾/红海/马六甲/中国沿海）。
    # 缺 HIFLEET_API_KEY 时 → SKIP，不判失败（「没配置」≠「抓取坏了」，但原因照样打印）。
    # ⚠ 计费：逐船查询，一轮约 99 次查位（首次还要 ~99 次搜索补 MMSI）。
    #   单次扣多少点官方**未公开** → 先实测：python fetch_vlcc_position_hifleet.py --limit 5 --dry-run
    #   看余额前后差，再决定刷新频率与订阅档位。分数不够会显式失败（--max-errors，不静默）。
    ("fetch_vlcc_position_hifleet.py", "VLCC 船位（HiFleet REST，逐船按 MMSI 查最新位置）",
     [], {"requires_env": "HIFLEET_API_KEY"}),
    # ⚠ aisstream 版（fetch_vlcc_position.py）**不接入**（2026-09-30 实测定论）：
    #   免费但覆盖塌陷 —— 波斯湾 3 分钟 0 条、红海/印度洋/马六甲北/南中国海/上海口 全 0 条/秒，
    #   全地球流的六成来自北海。用已学到的 MMSI 精确订阅仍是 0 条 → 当地没有接收站。
    #   保留作海外段补充，手动跑：python fetch_vlcc_position.py --minutes 10
    # 注意：fetch_funds.py 不接入本脚本，单独手动运行（数据量大、耗时长）
    #   python fetch_funds.py --types 股票型,混合型,指数型   （权益类增量）
    #   python fetch_funds.py --years-back 5                （回补近5年持仓）
    #   python fetch_funds.py --refresh-scale --full        （强制全量重写）
    # fetch_fund_nav.py 同理：日常只有上面那一行（秒级）；下面两个模式耗时长，手动跑
    #   python fetch_fund_nav.py --gap                      （逐只补 L1 覆盖不到的非 ETF/定开等）
    #   python fetch_fund_nav.py --history                  （全历史回填，约 2 小时，可断点续跑）
    # fetch_private_funds.py 同理，两个模式都耗时长，一律手动跑：
    #   python fetch_private_funds.py                        （中基协备案全量，约 55 分钟，可续跑）
    #   python fetch_private_funds.py --nav                  （代销池净值，约 13 分钟，幂等）
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
        "title": "index_prices · 航运 CCFI  (周频，只能滚存)",
        "sql": """
            SELECT COUNT(DISTINCT p.index_key) AS routes,
                   COUNT(p.id) AS rows,
                   MIN(p.price_date)::text AS earliest,
                   MAX(p.price_date)::text AS latest
            FROM index_prices p
            WHERE p.index_key LIKE 'ccfi_%'
        """,
        "cols": ["routes", "rows", "earliest", "latest"],
        "optional": True,
    },
    {
        "title": "index_prices · 航运 BDI 系列  (日频，含历史)",
        "sql": """
            SELECT COUNT(DISTINCT p.index_key) AS series,
                   COUNT(p.id) AS rows,
                   MIN(p.price_date)::text AS earliest,
                   MAX(p.price_date)::text AS latest
            FROM index_prices p
            WHERE p.index_key IN ('bdi', 'bci', 'bsi', 'bcti', 'bdti')
        """,
        "cols": ["series", "rows", "earliest", "latest"],
        "optional": True,
    },
    {
        "title": "index_prices · 航运 油运 CTFI  (当日口径：日频滚存 + 周报/月报期末值)",
        "sql": """
            SELECT COUNT(DISTINCT p.index_key) AS series,
                   COUNT(p.id) AS rows,
                   MIN(p.price_date)::text AS earliest,
                   MAX(p.price_date)::text AS latest
            FROM index_prices p
            WHERE p.index_key LIKE 'ctfi_%'
              AND p.index_key NOT LIKE '%_avg'
        """,
        "cols": ["series", "rows", "earliest", "latest"],
        "optional": True,
    },
    {
        "title": "index_prices · 航运 油运 CTFI  (期间均值：月均/周均，口径不同勿混)",
        "sql": """
            SELECT COUNT(DISTINCT p.index_key) AS series,
                   COUNT(p.id) AS rows,
                   MIN(p.price_date)::text AS earliest,
                   MAX(p.price_date)::text AS latest
            FROM index_prices p
            WHERE p.index_key LIKE 'ctfi_%_avg'
        """,
        "cols": ["series", "rows", "earliest", "latest"],
        "optional": True,
    },
    {
        "title": "index_prices · 建材 CEMPI  (日频，含历史)",
        "sql": """
            SELECT COUNT(p.id) AS rows,
                   MIN(p.price_date)::text AS earliest,
                   MAX(p.price_date)::text AS latest
            FROM index_prices p
            WHERE p.index_key = 'cement_cempi'
        """,
        "cols": ["rows", "earliest", "latest"],
        "optional": True,
    },
    {
        "title": "prices · 水泥 P.O42.5 均价  (日频，元/吨)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   MIN(price_date)::text AS earliest,
                   MAX(price_date)::text AS latest
            FROM prices
            WHERE commodity_key = 'cement_po425'
        """,
        "cols": ["rows", "earliest", "latest"],
        "optional": True,
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
        "title": "commodity_spot  (期货实时快照)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   MAX(spot_date)::text AS spot_date,
                   MAX(updated_at AT TIME ZONE 'Asia/Shanghai')::text AS last_updated
            FROM commodity_spot
        """,
        "cols": ["rows", "spot_date", "last_updated"],
        "optional": True,
    },
    {
        "title": "commodity_minutes  (期货分时 1 分钟)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT commodity_key) AS commodities,
                   MAX(DATE(dt))::text AS latest_day,
                   MAX(dt)::text AS latest_dt
            FROM commodity_minutes
        """,
        "cols": ["rows", "commodities", "latest_day", "latest_dt"],
        "optional": True,
    },
    {
        "title": "funds  (公募基金名录+规模+净值快照)",
        "sql": """
            SELECT COUNT(*) AS total,
                   COUNT(scale) AS with_scale,
                   COUNT(latest_nav) AS with_nav,
                   MAX(latest_nav_date)::text AS nav_date,
                   MAX(scale_updated_at AT TIME ZONE 'Asia/Shanghai')::date::text AS scale_updated
            FROM funds
        """,
        "cols": ["total", "with_scale", "with_nav", "nav_date", "scale_updated"],
        "optional": True,
    },
    {
        "title": "fund_nav  (基金净值日线，全历史)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT fund_code) AS funds,
                   COUNT(daily_return) AS with_chg,
                   MIN(nav_date)::text AS earliest,
                   MAX(nav_date)::text AS latest
            FROM fund_nav
        """,
        "cols": ["rows", "funds", "with_chg", "earliest", "latest"],
        "optional": True,
    },
    {
        "title": "fund_holdings  (基金季度持仓，前十大重仓)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT fund_code) AS funds,
                   MIN(report_date)::text AS earliest,
                   MAX(report_date)::text AS latest
            FROM fund_holdings
        """,
        "cols": ["rows", "funds", "earliest", "latest"],
        "optional": True,
    },
    {
        "title": "private_funds  (私募备案产品 + 代销池净值快照)",
        "sql": """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE in_registry) AS registry,
                   COUNT(*) FILTER (WHERE has_nav) AS with_nav,
                   MAX(latest_nav_date)::text AS nav_date
            FROM private_funds
        """,
        "cols": ["total", "registry", "with_nav", "nav_date"],
        "optional": True,
    },
    {
        "title": "private_managers  (私募基金管理人)",
        "sql": """
            SELECT COUNT(*) AS total,
                   COUNT(fund_count) AS with_count
            FROM private_managers
        """,
        "cols": ["total", "with_count"],
        "optional": True,
    },
    {
        "title": "private_fund_nav  (私募净值日线，仅代销池 ~0.5%)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT fund_no) AS funds,
                   MIN(nav_date)::text AS earliest,
                   MAX(nav_date)::text AS latest
            FROM private_fund_nav
        """,
        "cols": ["rows", "funds", "earliest", "latest"],
        "optional": True,
    },
    {
        "title": "ipo_calendar  (新股日历 A股/北交所/港股)",
        "sql": """
            SELECT COUNT(*) AS total,
                   STRING_AGG(DISTINCT market, ' / ' ORDER BY market) AS markets,
                   COUNT(apply_date) AS with_apply,
                   COUNT(raise_amount) AS with_raise,
                   MAX(COALESCE(listing_date, apply_date))::text AS latest
            FROM ipo_calendar
        """,
        "cols": ["total", "markets", "with_apply", "with_raise", "latest"],
        "optional": True,
    },
    {
        "title": "vlcc_vessels  (中国船东 VLCC 名录)",
        "sql": """
            SELECT COUNT(*) AS total,
                   STRING_AGG(DISTINCT owner, ' / ' ORDER BY owner) AS owners,
                   COUNT(*) FILTER (WHERE mmsi IS NOT NULL) AS with_mmsi,
                   COUNT(*) FILTER (WHERE dwt  IS NOT NULL) AS with_dwt,
                   COUNT(*) FILTER (WHERE flag IS NOT NULL) AS with_flag,
                   MAX(updated_at AT TIME ZONE 'Asia/Shanghai')::date::text AS updated
            FROM vlcc_vessels
        """,
        "cols": ["total", "owners", "with_mmsi", "with_dwt", "with_flag", "updated"],
        "optional": True,
    },
    {
        "title": "vlcc_positions  (VLCC 船位，滚存；source=hifleet/aisstream)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT name_ais) AS ships,
                   STRING_AGG(DISTINCT source, '/' ORDER BY source) AS src,
                   MIN(ts)::text AS earliest,
                   MAX(ts)::text AS latest,
                   MAX(created_at AT TIME ZONE 'Asia/Shanghai')::text AS last_run
            FROM vlcc_positions
        """,
        "cols": ["rows", "ships", "src", "earliest", "latest", "last_run"],
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
    {
        "title": "fx_pairs  (人民币汇率货币对定义)",
        "sql": """
            SELECT COUNT(*) AS pairs,
                   COUNT(*) FILTER (WHERE official_code IS NOT NULL) AS with_mid,
                   COUNT(*) FILTER (WHERE spot_code IS NOT NULL) AS with_spot
            FROM fx_pairs
        """,
        "cols": ["pairs", "with_mid", "with_spot"],
        "optional": True,
    },
    {
        # ⚠ 两个口径分开统计，**不要合成一行**（覆盖 25 vs 19 对、深度 2006 vs 1994/2023）
        "title": "fx_rates  (汇率日线；mid=中间价 cfets / spot=即期 sina)",
        "sql": """
            SELECT rate_type,
                   COUNT(DISTINCT pair_key) AS pairs,
                   COUNT(*) AS rows,
                   MIN(rate_date)::text AS earliest,
                   MAX(rate_date)::text AS latest
            FROM fx_rates
            GROUP BY rate_type
            ORDER BY rate_type
        """,
        "cols": ["type", "pairs", "rows", "earliest", "latest"],
        "optional": True,
        "multi": True,
    },
    {
        "title": "fx_spot  (即期实时快照；休市时会停更，看 quote_time)",
        "sql": """
            SELECT COUNT(*) AS pairs,
                   MAX(quote_time) AS last_quote,
                   MAX(spot_date)::text AS spot_date,
                   MAX(updated_at AT TIME ZONE 'Asia/Shanghai')::text AS last_run
            FROM fx_spot
        """,
        "cols": ["pairs", "last_quote", "spot_date", "last_run"],
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
                    rows = cur.fetchall() if q.get("multi") else [cur.fetchone()]
                print(f"  {BOLD}{title}{RESET}")
                if not rows or rows == [None]:
                    print(f"    {warn('无数据')}")
                for row in rows:
                    pairs = "  ".join(f"{c}={ok(str(v))}" for c, v in zip(cols, row))
                    print(f"    {pairs}" if not q.get("multi") else f"    [{row[0]}] " + "  ".join(
                        f"{c}={ok(str(v))}" for c, v in zip(cols[1:], row[1:])))
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
def parse_entry(entry):
    """把 FETCH_SCRIPTS 的元素规整成 (script, desc, extra_args, options)。"""
    script, desc = entry[0], entry[1]
    extra = list(entry[2]) if len(entry) > 2 and entry[2] else []
    opts  = entry[3] if len(entry) > 3 and entry[3] else {}
    return script, desc, extra, opts


def run_fetches():
    print(hdr("\n══════════════  开始刷新数据  ══════════════\n"))
    python = sys.executable
    results = []

    for entry in FETCH_SCRIPTS:
        script, desc, extra, opts = parse_entry(entry)

        # 需要环境变量的脚本：缺了就 SKIP，不判失败（原因打印出来）
        env_key = opts.get("requires_env")
        if env_key and not os.environ.get(env_key):
            print(f"  {warn('[SKIP]')} {script}  ({desc})")
            print(f"         未配置 {env_key}，跳过（这不是失败）\n")
            results.append((script, "skip", 0.0))
            continue

        path = BASE_DIR / script
        if not path.exists():
            print(f"  {warn('[SKIP]')} {script}  ({desc})  — 文件不存在")
            results.append((script, "skip", 0))
            continue

        cmd_desc = desc + (f"  [{' '.join(extra)}]" if extra else "")
        print(f"  {CYAN}▶ {script}{RESET}  {cmd_desc}")
        t0 = time.time()
        proc = subprocess.run(
            [python, str(path)] + extra,
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
# Clear all data
# ─────────────────────────────────────────────────────────────────────────────

# 按依赖顺序 TRUNCATE（先子表再父表，CASCADE 处理外键）
CLEAR_TABLES = [
    "fetch_log",
    "vlcc_positions",
    "vlcc_vessels",
    "fund_nav",
    "fund_holdings",
    "funds",
    "crypto_prices",
    "crypto_coins",
    "index_minutes",
    "index_spot",
    "index_prices",
    "market_indices",
    "commodity_minutes",
    "commodity_spot",
    "prices",
    "commodities",
]


def clear_all(force: bool = False):
    print(hdr("\n══════════════  清空所有数据  ══════════════\n"))
    if not force:
        print(warn("  ⚠  此操作将删除数据库中全部数据，且不可恢复！"))
        confirm = input("  输入 YES 确认清空，其他任意键取消: ").strip()
        if confirm != "YES":
            print("  已取消。")
            return

    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(err(f"[FATAL] 无法连接数据库: {exc}"))
        return

    try:
        with conn:
            with conn.cursor() as cur:
                for table in CLEAR_TABLES:
                    try:
                        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                        print(f"  {ok('✔')}  {table}")
                    except psycopg2.errors.UndefinedTable:
                        print(f"  {warn('—')}  {table}  （表不存在，跳过）")
                        conn.rollback()
        print(f"\n  {ok('全部完成。')}")
    except Exception as exc:
        print(err(f"\n[FATAL] {exc}"))
        import traceback; traceback.print_exc()
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="globaldata 数据刷新工具")
    parser.add_argument("--status", action="store_true", help="仅展示数据库状态")
    parser.add_argument("--fetch",  action="store_true", help="仅运行 fetch 脚本")
    parser.add_argument("--clear",       action="store_true", help="清空所有表数据（需二次确认）")
    parser.add_argument("--force_clear", action="store_true", help="强制清空所有表数据（无需确认）")
    args = parser.parse_args()

    if args.force_clear:
        clear_all(force=True)
        return

    if args.clear:
        clear_all(force=False)
        return

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
