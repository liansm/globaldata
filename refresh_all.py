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
    # 动力煤港口价（CCI5500/5000/4500）单独一条：
    #   sxcoal 原源 2026-10 起付费墙化（result 全部返回 "*"），改由 ctctc 市场快讯转载抓取，
    #   沿用原 commodity_key（coal_port_*）→ 前端无需改。属「当日滚存」，漏一天永久少一天。
    ("fetch_coal_flash.py",        "动力煤港口价 CCI5500/5000/4500（太原煤炭交易中心转载，日频滚存）"),
    # 国际动力煤 FOB（纽卡斯尔/理查兹/欧洲三港，美元/吨）单独一条：
    #   sxcoal 的 coal_import_*_fob 已付费墙化停车，本脚本抓 CCTD「国际煤炭」栏目
    #   周报（每周二，免费），新建 coal_*_fob 三个 key（6000 大卡），**不覆盖**原序列。
    #   周频 → 漏一期丢一期，但可翻 24 页回补 9 年，比日频源安全。
    ("fetch_coal_intl.py",         "国际动力煤 FOB（CCTD 国际煤炭周报，周频可回补）"),
    ("fetch_cement.py",            "水泥价格：CEMPI 指数 + P.O42.5 均价（中国水泥网，日频含历史）"),
    # AI 硬件（一个脚本两块数据）：
    #   ① 存储颗粒现货 DDR5/DDR4 —— 源站历史只有**滚动半年**，跑漏的期次永久缺失
    #      → 必须日更累积（9 品种 × 122 点、10 个请求、约 20 秒，全窗口幂等 upsert）
    #   ② GPU 云租价 —— aimultiple 聚合各家云厂商牌价，日更才有走势
    #   ③ 存储成品 NAND/SSD/HDD —— 与 ① 同站，115 个品种、115 个请求、约 2.5 分钟（全流程最慢的一条）
    #      虽慢但不接会永久丢数据（同样是滚动半年窗口）
    #      只要 DDR + GPU（约 30 秒）时手动跑：python fetch_ai_hardware.py --dram --gpu
    ("fetch_ai_hardware.py",       "AI 硬件价格：存储（DDR/NAND/SSD/HDD 每 GB 价）+ GPU 云租价"),
    ("fetch_commodity_spot.py",    "期货实时快照（futures_zh_spot / futures_foreign_commodity_realtime）"),
    ("fetch_commodity_minutes.py", "期货分时 1 分钟 K 线（futures_zh_minute_sina）"),
    # ⚠ 加密货币（2026-10-26 暂时下线，不接入定时刷新）：
    #   `fetch_crypto.py`、`/api/crypto/*`、前端 `/crypto` 路由与页面全部保留备用，
    #   只是不再跑在 refresh_all 里（CoinGecko 免费档限流 ~5 次/分钟，10 币全流程约 3 分钟）。
    #   需要时手动跑：python fetch_crypto.py            # 近 365 天日线回补 + 当日快照
    #                 python fetch_crypto.py --no-history  # 只写当日快照
    #   恢复接入：把下面这行加回 FETCH_SCRIPTS 即可
    #   ("fetch_crypto.py", "加密货币（CoinGecko：近 365 天日线回补 + 当日快照）"),
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
        # ⚠ 只应覆盖滚动半年（约 120~125 个报价日/品种）：earliest 明显早于半年前
        #   说明站方延长了窗口，早于半年且 latest 很久没动 → 停更。
        # ⚠ products 现在应为 9（DDR5 3 档 + DDR4 6 档）。库里可能还留着 DDR3 旧行，
        #   接口已过滤，统计这里不过滤 —— 数字变大不代表又在抓 DDR3。
        "title": "dram_prices  (存储颗粒现货 DDR5/DDR4)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT dram_key) AS products,
                   MIN(price_date)::text AS earliest,
                   MAX(price_date)::text AS latest
            FROM dram_prices
        """,
        "cols": ["rows", "products", "earliest", "latest"],
    },
    {
        # market_min = 全市场最低租价（源站口径：单卡 x1，不含整节点摊销）
        "title": "gpu_prices  (GPU 云租价 美元/卡·小时)",
        "sql": """
            SELECT COUNT(*) FILTER (WHERE provider = 'market_min') AS min_quotes,
                   COUNT(DISTINCT gpu_key) AS gpus,
                   COUNT(DISTINCT provider) - 1 AS providers,
                   MIN(price_date)::text AS earliest,
                   MAX(price_date)::text AS latest
            FROM gpu_prices
        """,
        "cols": ["min_quotes", "gpus", "providers", "earliest", "latest"],
    },
    {
        # ⚠ 分品类看 latest，不要只看总行数：
        #   日更（DDR/SSD 零售）→ 周级（NAND/SSD 白牌）→ **季级（HDD）**，
        #   HDD 的 latest 落后一两个月是上游不调价，不是抓取坏了。
        # ⚠ capacity_gb 已是 GB（NAND 的 Tb 已 ÷8），所以 per_gb 直接 close/capacity_gb。
        "title": "storage_prices  (NAND/SSD/HDD 存储成品)",
        "sql": """
            SELECT s.category,
                   COUNT(DISTINCT s.storage_key) AS products,
                   COUNT(*) AS rows,
                   MAX(p.price_date)::text AS latest,
                   ROUND(AVG(p.price_close / s.capacity_gb), 3)::text AS avg_per_gb
            FROM storage_products s
            JOIN storage_prices p USING (storage_key)
            WHERE s.capacity_gb > 0
              AND p.price_date = (SELECT MAX(price_date) FROM storage_prices
                                   WHERE storage_key = s.storage_key)
            GROUP BY s.category
            ORDER BY s.category
        """,
        "cols": ["category", "products", "rows", "latest", "avg_per_gb"],
        "multi": True,
    },
    {
        # ⚠ 只看港口 3 档：CCI 进口/产地 6 条的源（sxcoal）已付费墙化，
        #   停在 2026-09-30 —— 那是「源侧拿不到」，不是抓取坏了。
        "title": "prices · 煤炭 港口CCI  (5500/5000/4500，日频滚存)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT commodity_key) AS series,
                   MIN(price_date)::text AS earliest,
                   MAX(price_date)::text AS latest
            FROM prices
            WHERE commodity_key IN ('coal_port_5500', 'coal_port_5000', 'coal_port_4500')
        """,
        "cols": ["rows", "series", "earliest", "latest"],
        "optional": True,
    },
    {
        # 2026-10-29 新增：sxcoal 付费墙化的替代口径（近似，非等价）
        #   gz_*      = 广州港进口煤场地含税价（元/吨）  ← 替代 CCI进口
        #   shenhua_* = 神华巴图塔外购车板价（元/吨）    ← 替代 CCI产地
        #   *_fob     = 国际三港 FOB（美元/吨，周频）    ← 替代 CCI进口FOB
        "title": "prices · 煤炭 替代口径  (进口场地价/车板价/国际FOB)",
        "sql": """
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT commodity_key) AS series,
                   MIN(price_date)::text AS earliest,
                   MAX(price_date)::text AS latest
            FROM prices
            WHERE commodity_key LIKE 'coal_gz%'
               OR commodity_key LIKE 'coal_shenhua%'
               OR commodity_key IN ('coal_newc_fob', 'coal_richards_fob', 'coal_ara_fob')
        """,
        "cols": ["rows", "series", "earliest", "latest"],
        "optional": True,
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
    # 加密货币的 status 查询（crypto_coins / crypto_prices）随刷新一起下线，
    # 表和数据仍保留；需要看时恢复接入后加回，或直接 psql 查。
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
    # ⚠ crypto_* 仍在清空清单里（「清空所有数据」就该清干净），
    #   但加密货币已不接入刷新 —— 清完**不会被 refresh_all 自动补回**，
    #   要恢复得手动 python fetch_crypto.py
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
