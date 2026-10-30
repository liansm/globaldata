#!/usr/bin/env python3
"""
AI Hardware Prices (AI 硬件价格：存储颗粒 + 存储成品 + GPU 算力)
================================================================
一个脚本、两个数据源、三块口径完全不同的价格。**别混着讲**：

    ① 存储颗粒现货 —— dram.com.cn（中国IC交易网，深圳）
       单位 **美元/颗**，DDR5 / DDR4 两代（DDR3 已停抓：成交清淡、页面不再展示）。
       页面同时折算 **美元/GB**（density_gb 存的是 Gb，÷8 得 GB），
       这才是横向比"哪代更贵"的正确口径 —— 24Gb 颗粒每 GB 比 16Gb 便宜，
       而 DDR4 16Gb 的每 GB 价反超 DDR5，正是 DDR4 供给收缩的直接体现。

    ② GPU 云租价 —— aimultiple.com/ai-gpus（聚合各家云厂商公开牌价）
       单位 **美元/卡/小时**。这是 GPU 唯一有公开日频口径的"价格"：
       芯片本身（H100/B200/B300/Rubin 单卡）NVIDIA 不公布售价，
       零售渠道（CDW / Insight / Newegg / eBay）全部 403 或代理挂死，
       能拿到的只有媒体转述的区间 → 只作为**静态参考价**存在 gpu_products
       里，不编成日序列。

⚠ 光模块 / CPO 没有可抓的公开价（实测 2026-10）：
   FS.com 是纯客户端渲染（页面里连 price 字段都没有）、Amazon 搜 800G 会混进
   400G 与 PoE 供电模块（中位数毫无意义）、Digi-Key / Mouser / eBay / 新蛋 全 403。
   ASP 只存在于 LightCounting / Yole 付费报告与厂商财报 → **不做**，页面写明原因。

    ③ 存储成品盘 / NAND 颗粒 —— **同一个站**的另外四个分类（menuid=41 下的 sortid）：
         sortid=3  Nand Flash   晶圆/颗粒，**美元/颗，容量是 Gb**（1Tb=128GB）
         sortid=9  SSD OEM      白牌整盘，**美元/个**
         sortid=5  SSD Street   零售品牌盘，**人民币/个**
         sortid=10 HDD          机械盘，**人民币/个**（更新极慢，季度级）
       接口、解析、历史逻辑与 ① 完全一致，只是 sortid 不同。

⚠ 硬盘口径（落库前必须理解）
------------------------------
1. **币种标记不可信，一律按分类反推**（实测 2026-10-26）：
   SSD OEM 里 `White SSD 256GB PCIe 3.0` 标 `￥` 却是 38.00 —— 0.15 元/GB 比晶圆裸片还便宜，
   实为美元；HDD 里 `西数WD 1TB NB` 标 `$` 却是 810.00 —— $810 买 1TB 机械盘同样荒谬，实为人民币。
   本脚本按 `STORAGE_CATS` 的分类默认币种落库，并把「站方标记与分类默认不符」的条目打印出来。
2. **NAND 的容量是 Gb 不是 GB**：`Flash 1Tb QLC Wafer $25.50` 是 1Tb=128GB → $0.20/GB。
   当 GB 算会得出 $0.025/GB 的假便宜（比白牌成品盘还低 5 倍，这就是露馅的地方）。
   本表 `capacity_gb` 存的是**已归一化的 GB**，与 `dram_products.density_gb`（存 Gb）语义相反，
   所以 `perGb = close / capacity_gb`，**不要再 ÷8**。
3. **上游名称里的容量经常写错**，正则救不了（`Ti600-11BNVME` 是 1TB、`S100G1X0E` 是 1TB
   而型号里带 `100G`、`SU650 1TG` 是 1TB）→ 一律走 `CAPACITY_OVERRIDE`（按 prodid 写死）。
4. **更新频率差两个数量级**：NAND/SSD OEM 周级、SSD 零售日更、**HDD 半年只有 3~4 个点**。
   HDD 画出来是水平线不是抓漏，是上游真不调价 —— 页面上必须标出来。
5. 零售品牌盘里有一批**已停更**（MG麦光/威士奇停在 8 月、海盗船只 1 个点、Micron 3 个点），
   列表页仍挂着，历史不再前进。要做「零售均价」必须按容量加权，否则小容量盘会把均价抬高。

Data sources
------------
    DRAM 列表 : http://www.dram.com.cn/ajax_asp/productprice_list.asp?menuid=41
    DRAM 历史 : http://www.dram.com.cn/ajax_asp/productpriceD_list.asp?menuid=41&prodid=<id>
    NAND/SSD/HDD 列表 : 同上 + &sortid=<3|5|9|10>（历史接口 prodid 通用）
    GPU 报价  : https://aimultiple.com/ai-gpus   （单页静态 HTML，文本流可稳定解析）

⚠ DRAM 口径（落库前必须理解）
------------------------------
1. **单位是「美元/颗」**，不是内存条价。`DDR5 16G(2Gx8)` = 16Gb 颗粒 ≈ 2GB，
   做一条 16GB 内存条要 8 颗 → 与零售条价差 8 倍量级。
2. **正片(prime) 与 eTT(白牌) 必须分开**：DDR4 8Gb 两者差 5~6 倍（37.30 vs 5.95）。
3. 历史只覆盖**滚动半年**，更早的站方要卖 Excel → 「错过就没有」，靠日更累积。
4. 与 TrendForce/DRAMeChhange 全球现货差 20%+（国内深圳口径），跨源不可拼接。
5. 历史表**没有最高价**，只有 最低/开盘/收盘 → 主序列一律取 close。
6. 报价日不连续（周末无报价），上游给什么日期存什么，**不补空、不前值填充**。

⚠ GPU 口径
-----------
- 价格是**云厂商租价**（$/卡/小时），不是买断价。两者差三个数量级，不可比。
- aimultiple 的「Cost」列 = 该型号在**所有收录云厂商里的最低价**，
  存在 provider='market_min'；另有页面默认型号（当前是 B200 SXM）的
  逐家明细（Verda / Lambda / RunPod / AWS …）一并落库。
- 单卡买断参考价（UNIT_PRICE）来自媒体转述的区间中值，**不是 NVIDIA 官方 MSRP**，
  带 asof 与来源，前端必须标注「参考价，非实时行情」。

Tables
------
    dram_products / dram_prices   存储颗粒 DDR5/DDR4（已有，本脚本接管维护）
    storage_products / storage_prices  NAND颗粒 / SSD白牌 / SSD零售 / HDD（本脚本新建）
    gpu_products  / gpu_prices    GPU 算力（本脚本新建）

Usage
-----
    python fetch_ai_hardware.py             # 抓全部三块，幂等 upsert（日常跑这个）
    python fetch_ai_hardware.py --dry-run   # 只抓取打印，不写库
    python fetch_ai_hardware.py --dram      # 只跑存储颗粒
    python fetch_ai_hardware.py --storage   # 只跑 NAND/SSD/HDD（115 品种，约 2.5 分钟）
    python fetch_ai_hardware.py --force-storage  # 无视节流窗口，强制重抓存储成品盘
    python fetch_ai_hardware.py --gpu       # 只跑 GPU
    python fetch_ai_hardware.py --status    # 只回显库内现状，不抓取
    python fetch_ai_hardware.py --purge-ddr3  # 顺带删掉库里的 DDR3 旧数据（一次性）

跑多勤（2026-10-30 起）
------------------------
    ③ NAND/SSD/HDD 默认**最多每 3 天跑一次**（STORAGE_INTERVAL_DAYS），
      其余两块仍日更：

        ① DRAM 颗粒    每天（约 20s）    日更攒走势；同是滚动半年窗口，但只 20s，不值得节流
        ② GPU 云租价   每天（约 10s）    牌价日更才有走势
        ③ NAND/SSD/HDD ≤每 3 天（约 140s）115 品种 = 115 个详情请求，整轮 refresh 最大瓶颈；
                                         同是滚动半年窗口，**漏几天不丢数据**

      判断依据是库里 `storage_products.MAX(updated_at)`，不落本地状态文件：
      表空 / 连不上库 / 源站故障导致 0 品种写入 → 下一轮自动重跑，不会被节流卡死。
      被跳过时打印 `[SKIP]` 说明并 **退出码 0**（是「按计划没跑」，不是失败）。
      要立即刷新：`--force-storage`。
"""

import os
import re
import sys
from datetime import date, datetime

import psycopg2
import requests
import urllib3
from dotenv import load_dotenv
from psycopg2.extras import execute_values

urllib3.disable_warnings()
load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/131.0 Safari/537.36"),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# ===========================================================================
# ① DRAM 存储颗粒
# ===========================================================================
DRAM_BASE   = "http://www.dram.com.cn"
DRAM_MENUID = 41
DRAM_LIST   = f"{DRAM_BASE}/ajax_asp/productprice_list.asp?menuid={DRAM_MENUID}"
DRAM_DETAIL = f"{DRAM_BASE}/ajax_asp/productpriceD_list.asp?menuid={DRAM_MENUID}&prodid={{pid}}"
DRAM_SOURCE = "dram.com.cn"
DRAM_UNIT   = "USD/chip"

# 只抓这两代。DDR3 成交清淡、页面也不再展示；库里已有的 DDR3 历史保留（可回溯），
# 加回来只需往这里添一项。
DRAM_GENERATIONS = {"DDR5", "DDR4"}

DRAM_HEADERS = {
    **HEADERS,
    # 裸 requests（无 UA）会被 403 拦掉；站点无频控，实测 11 个详情连发 21 秒全 200
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{DRAM_BASE}/page/views_detail.asp?menuid={DRAM_MENUID}",
}

# ===========================================================================
# ② GPU 云租价 + 单卡买断参考价
# ===========================================================================
GPU_URL    = "https://aimultiple.com/ai-gpus"
GPU_SOURCE = "aimultiple.com"

# 白名单：(型号原名, key, 展示名, 厂商, sort_order)
# 只收 AI 训练/推理的 NVIDIA 主力卡。理由：
#   · 消费卡（4090/5090）和老卡（V100/T4）与"AI 算力价格"不是一回事；
#   · AMD MI355X 在源站总表里的 Cost 是 $20.72，与同表其他卡（$1.4~4.6）差 5 倍，
#     疑为 8 卡节点总价被当成单卡价 → **口径存疑的宁可不要**，避免往页面塞可疑数。
GPU_MODELS = [
    ("H100 SXM", "h100_sxm", "H100 SXM", "NVIDIA", 10),
    ("H100 PCIe", "h100_pcie", "H100 PCIe", "NVIDIA", 20),
    ("H100 NVL", "h100_nvl", "H100 NVL", "NVIDIA", 30),
    ("H200 SXM", "h200_sxm", "H200 SXM", "NVIDIA", 40),
    ("B200 SXM", "b200_sxm", "B200 SXM", "NVIDIA", 50),
    ("B300", "b300", "B300", "NVIDIA", 60),
]
GPU_MODEL_INDEX = {name: (key, disp, brand, order) for name, key, disp, brand, order in GPU_MODELS}

# 单卡买断参考价（美元/卡）。**不是官方 MSRP**，是媒体转述的市场区间中值，
# 只用于"这卡大概多少钱一张"的量级锚定。云端租价是 $/小时，两者不可直接比。
# 来源：neysa.ai 引 Spheron 定价汇总（2026-04）、aitechconnect.in 硬件阶梯表（2026-05）。
UNIT_PRICE = {
    "h100_sxm":  (27000, 40000, "媒体口径单卡模块 $27k~40k；二手/现货低至 $18k~22k"),
    "h100_pcie": (25000, 35000, "PCIe 版略低于 SXM"),
    "h100_nvl":  (28000, 40000, "NVL 94GB 双卡模组按单卡折算"),
    "h200_sxm":  (28000, 38000, "H200 单卡，141GB HBM3e"),
    "b200_sxm":  (35000, 40000, "Blackwell 主力，2026 新集群默认选择"),
    "b300":      (40000, 50000, "Blackwell Ultra，288GB，溢价主要在显存容量"),
}
UNIT_PRICE_ASOF = "2026-05"
UNIT_PRICE_SRC  = "媒体转述（neysa.ai 引 Spheron / aitechconnect.in），非 NVIDIA 官方报价"

# Rubin：2026-10 核实，aimultiple 全站 0 次命中，云端也没有在租实例，
# 官方亦未公布售价 → 入库一条占位，前端显示「暂无公开报价」而不是「抓取失败」。
RUBIN_NOTE = ("NVIDIA Rubin 尚未公布单卡售价，云端亦无在租实例"
              "（2026-10 核实 aimultiple 全表 0 命中）")
RUBIN_MODEL = ("Rubin", "rubin", "Rubin", "NVIDIA", 90)

# ===========================================================================
# ③ 存储成品盘 / NAND 颗粒（同站 menuid=41 的另外四个 sortid）
# ===========================================================================
STORAGE_LIST   = f"{DRAM_BASE}/ajax_asp/productprice_list.asp?menuid=41&sortid={{sid}}"
STORAGE_DETAIL = DRAM_DETAIL          # 历史接口与 DRAM 完全同一个
STORAGE_SOURCE = "dram.com.cn"

# (sortid, category, 展示名, 默认币种, 计价单位后缀, perGB 合理下限, 上限)
# ⚠ 币种是**按分类定的**，不是读站方那一列（站方标记有错，见 CCY_REASON）
# ⚠ perGB 区间只用来「抓荒谬值」告警，不用来过滤 —— 越界要打印出来人工看，不静默丢
STORAGE_CATS = [
    (3,  "nand_wafer", "NAND 闪存颗粒", "USD", "chip", 0.05, 6.0),
    (9,  "ssd_oem",    "SSD 白牌 OEM",  "USD", "disk", 0.05, 1.0),
    (5,  "ssd_street", "SSD 零售品牌",  "CNY", "disk", 0.30, 5.0),
    (10, "hdd",        "HDD 机械硬盘",  "CNY", "disk", 0.10, 2.0),
]
STORAGE_CAT_INDEX = {c[1]: c for c in STORAGE_CATS}

# ③ 跑多勤？—— 存储成品盘（115 个品种 = 115 个详情请求）默认**每 3 天跑一次**
#
# 依据：这块的源站历史是**滚动半年**窗口（DRAM 段同理），只要半年内跑过一次，
# 站方会把整段历史重新给一遍 —— 漏几天**不会丢数据**，和日频滚存（漏一天永久少一天）
# 完全两回事。日更纯属浪费：一轮 168 秒里这块占 140 秒，是整轮 refresh 的最大瓶颈。
#
# 节流靠**库里的 MAX(updated_at)** 判断，不落本地状态文件：
#   · 上次成功写入距今天数 < STORAGE_INTERVAL_DAYS → 跳过（打印原因，退出码 0）
#   · 表空 / 没连上库 / --force-storage / --dry-run → 照跑
#   · 源站故障导致 0 品种写入时 updated_at 不前进 → 下次自动重试，不会卡死
# ⚠ 所以「多久跑一次」的语义是「**最多** 3 天一次，坏了随时会提前重试」，不是固定节拍。
STORAGE_INTERVAL_DAYS = 3

CCY_REASON = {
    "nand_wafer": "晶圆/颗粒以美元计价，站方标记一致",
    "ssd_oem":    "白牌 OEM 以美元计价；站方把 3 条 PCIe3.0 标成 ￥ 是笔误（0.15 元/GB 低于晶圆裸片价）",
    "ssd_street": "零售品牌盘以人民币计价，与京东/淘宝零售价同量级",
    "hdd":        "机械盘以人民币计价；站方把「西数WD 1TB NB」标成 $ 是笔误（$810 买 1TB 不合常理）",
}

# 上游名称里容量写错/写残的，按 prodid 写死（prodid 若变，这里会打印「未命中」提醒）
# 值：(归一化后的 GB, 原始写法, 说明)
CAPACITY_OVERRIDE = {
    "466542": (500,  "500GNVME", "500G 后直接接 NVME，严格边界匹配不到"),
    "466541": (1024, "11BNVME",  "上游把 1TB 写成 11B"),
    "465596": (2048, "2TBNVME",  "2TB 后直接接 NVME"),
    "465595": (1024, "1TBNVME",  "1TB 后直接接 NVME"),
    "465594": (2048, "2TBNVME",  "2TB 后直接接 NVME"),
    "465625": (1024, "S100G1X0E", "西数型号里的 100G 不是容量（SN850X 1TB）"),
    "465624": (2048, "S200G1X0E", "西数型号里的 200G 不是容量（SN850X 2TB）"),
    "466109": (512,  "512B",     "上游把 512GB 写成 512B"),
}

# 品牌识别：按顺序匹配，命中即止（'ZHITAI致态' 必须先于其他 Z 开头项）
BRAND_ALIAS = [
    ("Kingston", "Kingston"), ("Samsung", "Samsung"), ("Micron", "Micron"),
    ("ADATA", "ADATA"), ("ZHITAI", "致态"), ("MG麦光", "麦光"),
    ("Vaseky", "威士奇"), ("海盗船", "海盗船"), ("宏基", "Acer"),
    ("西数", "西数"), ("希捷", "希捷"), ("White", "White"), ("OEM", "OEM"),
]

# 容量：**大小写必须区分** —— Gb(bit) 与 GB(byte) 差 8 倍，用 re.I 会把 NAND 颗粒
# 的 256Gb 读成 256GB，每 GB 价直接差 8 倍。
CAP_BYTE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(TB|TGB|GB|TG|G|T)(?![0-9A-Za-z])")
CAP_BIT_RE  = re.compile(r"(\d+(?:\.\d+)?)\s*(Tb|Gb)(?![0-9A-Za-z])")

INTERFACE_RULES = [
    (r"PCIe?\s*5\.0|M\.2\s*/\s*5\.0", "PCIe5.0"),
    (r"PCIe?\s*4\.0|NVME\s*4\.0|PCIe4\.0", "PCIe4.0"),
    (r"PCIe?\s*3\.0|NVME\s*3\.0|PCIe3\.0", "PCIe3.0"),
    (r"SATA\s*3|SATA$|SATA\b", "SATA3"),
]

SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS dram_products (
    dram_key    VARCHAR(60)  PRIMARY KEY,
    prodid      VARCHAR(20),
    name        VARCHAR(120) NOT NULL,
    generation  VARCHAR(10)  NOT NULL,
    density_gb  SMALLINT,
    speed       VARCHAR(30),
    grade       VARCHAR(10)  NOT NULL,
    unit        VARCHAR(20)  NOT NULL DEFAULT 'USD/chip',
    source      VARCHAR(40)  NOT NULL DEFAULT 'dram.com.cn',
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dram_prices (
    id          BIGSERIAL PRIMARY KEY,
    dram_key    VARCHAR(60) NOT NULL,
    price_date  DATE        NOT NULL,
    price_low   NUMERIC(12,3),
    price_open  NUMERIC(12,3),
    price_close NUMERIC(12,3),
    UNIQUE (dram_key, price_date)
);

CREATE INDEX IF NOT EXISTS idx_dram_prices_key_date
    ON dram_prices (dram_key, price_date DESC);

CREATE TABLE IF NOT EXISTS gpu_products (
    gpu_key        VARCHAR(40)  PRIMARY KEY,
    name           VARCHAR(60)  NOT NULL,
    vendor         VARCHAR(20)  NOT NULL,
    vram_gb        NUMERIC(8,1),
    bandwidth_tbs  NUMERIC(8,2),
    -- 单卡买断参考价（非日序列，静态锚；NULL 表示尚无公开报价）
    unit_price_low  NUMERIC(12,0),
    unit_price_high NUMERIC(12,0),
    unit_price_note VARCHAR(200),
    unit_price_asof VARCHAR(20),
    unit_price_src  VARCHAR(200),
    sort_order     SMALLINT     NOT NULL DEFAULT 100,
    source         VARCHAR(40)  NOT NULL DEFAULT 'aimultiple.com',
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gpu_prices (
    id           BIGSERIAL PRIMARY KEY,
    gpu_key      VARCHAR(40)  NOT NULL,
    price_date   DATE         NOT NULL,
    provider     VARCHAR(60)  NOT NULL,   -- 云厂商名，或 'market_min' 表示全市场最低价
    region       VARCHAR(40),
    price_hourly NUMERIC(10,4) NOT NULL,  -- 美元/**每卡**/小时（整节点报价已 ÷ 节点卡数）
    gpus_per_node SMALLINT,               -- 该报价对应的节点卡数（x1 / x8），用于还原总价
    UNIQUE (gpu_key, price_date, provider, region)
);

CREATE INDEX IF NOT EXISTS idx_gpu_prices_key_date
    ON gpu_prices (gpu_key, price_date DESC);

-- ③ 存储成品盘 / NAND 颗粒 ────────────────────────────────────────────────
-- ⚠ capacity_gb 存的是**归一化后的 GB**（NAND 的 1Tb 已 ÷8 折成 128GB），
--   与 dram_products.density_gb（存 Gb）语义**相反** —— 直接 close / capacity_gb 才是每 GB 价。
CREATE TABLE IF NOT EXISTS storage_products (
    storage_key   VARCHAR(60)  PRIMARY KEY,
    prodid        VARCHAR(20),
    name          VARCHAR(120) NOT NULL,   -- 上游原名，原样保留
    display_name  VARCHAR(120) NOT NULL,
    category      VARCHAR(20)  NOT NULL,   -- nand_wafer / ssd_oem / ssd_street / hdd
    brand         VARCHAR(30),
    capacity_gb   NUMERIC(10,1),           -- **GB**（NAND 颗粒也已 ÷8）
    capacity_raw  VARCHAR(24),             -- 上游原写法 '1Tb' / '960GB/1TB'，用于核对
    form_factor   VARCHAR(16),             -- 2.5 / M.2 / 3.5 / Wafer
    interface     VARCHAR(16),             -- SATA3 / PCIe3.0 / PCIe4.0 / PCIe5.0
    dtype         VARCHAR(10),             -- TLC / QLC / MLC（仅 NAND）
    usage         VARCHAR(16),             -- 桌面 / 监控 / 移动 / 笔记本（仅 HDD）
    currency      VARCHAR(3)   NOT NULL,   -- CNY / USD（**已按分类校正，非站方标记**）
    unit          VARCHAR(20)  NOT NULL,   -- USD/chip / USD/disk / CNY/disk
    currency_note VARCHAR(160),            -- 币种被校正的原因，前端要能显示
    sort_order    SMALLINT     NOT NULL DEFAULT 100,
    source        VARCHAR(40)  NOT NULL DEFAULT 'dram.com.cn',
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS storage_prices (
    id          BIGSERIAL PRIMARY KEY,
    storage_key VARCHAR(60) NOT NULL,
    price_date  DATE        NOT NULL,
    price_low   NUMERIC(12,2),
    price_open  NUMERIC(12,2),
    price_close NUMERIC(12,2),
    UNIQUE (storage_key, price_date)
);

CREATE INDEX IF NOT EXISTS idx_storage_prices_key_date
    ON storage_prices (storage_key, price_date DESC);
CREATE INDEX IF NOT EXISTS idx_storage_products_cat
    ON storage_products (category);
"""


# ---------------------------------------------------------------------------
# 通用
# ---------------------------------------------------------------------------
def cell_text(html: str) -> str:
    """去标签 + 压缩空白。名称里的空格要留着，否则 'DDR5 24Gb' 会变成 'DDR524Gb'。"""
    return re.sub(r"[ \t\r\n]+", " ", re.sub(r"<[^>]+>", "", html)).strip()


def to_text_lines(html: str):
    """整页 HTML → 纯文本行流（供 aimultiple 这种无 table 标签的页面用）"""
    txt = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html, flags=re.S)
    txt = re.sub(r"<[^>]+>", "\n", txt)
    return [x for x in (re.sub(r"\s+", " ", s).strip() for s in txt.split("\n")) if x]


# ---------------------------------------------------------------------------
# ① DRAM 解析
# ---------------------------------------------------------------------------
def parse_dram_list(html: str):
    """列表页 → [(prodid, raw_name), ...]"""
    out = []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        pid = re.search(r"prodid=(\d+)", row)
        cells = [cell_text(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)]
        if pid and cells and cells[0]:
            out.append((pid.group(1), cells[0]))
    return out


def parse_dram_detail(html: str):
    """详情页 → [(date, low, open, close), ...]，按日期升序"""
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        cells = [cell_text(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if len(cells) != 4 or not re.match(r"^\d{4}-\d\d-\d\d$", cells[0]):
            continue
        try:
            out.append((cells[0], *[float(x) if x else None for x in cells[1:4]]))
        except ValueError:
            continue
    return sorted(out)


def describe_dram(raw_name: str):
    """
    规格名 → (key, 展示名, generation, density_gb, speed, grade)

    站点写法随意：'DDR5 24Gb 5200/6400' / 'DDR5 16G (2Gx8) 4800/6000' /
    'DDR5 16Gb 2Gbx8 eTT' / 'DDR4 16Gb 2GbX8 2666/3200Mhz'
    → 只用「代数 / 密度 / 主频」三段做 key：pid 会变、写法会变，这三段不会。
    """
    name = re.sub(r"\s+", " ", raw_name).strip()
    gen_m = re.search(r"(DDR\d)", name, re.I)
    den_m = re.search(r"\b(\d+)\s*G(?:b|B)?\b", name)
    spd_m = re.search(r"(\d{3,4})\s*/\s*(\d{3,4})", name)
    if not gen_m or not den_m:
        return None

    gen     = gen_m.group(1).upper()
    density = int(den_m.group(1))
    speed   = f"{spd_m.group(1)}/{spd_m.group(2)}" if spd_m else None
    grade   = "ett" if re.search(r"eTT|ett|ETT", name) else "prime"

    # ⚠ 正片但上游漏写主频时要落到 'prime'，不能掉进 'ett' —— 那会让两种货同名
    if grade == "ett":
        suffix = "ett"
    elif speed:
        suffix = speed.split("/")[0]
    else:
        suffix = "prime"
    key = f"{gen.lower()}_{density}gb_{suffix}"

    if grade == "ett":
        display = f"{gen} {density}Gb eTT"
    elif speed:
        display = f"{gen} {density}Gb {speed} 正片"
    else:
        display = f"{gen} {density}Gb 正片"

    return key, display, gen, density, speed, grade


# ---------------------------------------------------------------------------
# ② GPU 解析
# ---------------------------------------------------------------------------
def parse_gpu_models(lines):
    """
    型号总表 → [(model, vram_gb, bw_tbs, cost_hourly), ...]

    表是 div 布局（没有 <table>），只能按文本流扫：
        1 / B200 SXM / 192 GB / 8.20 / $3.67
        N/A / 20 / B300 / 144 GB / 4.10 / $4.62      ← 第一列可能是排名或 N/A
    ⚠ 不能"以 $ 结尾就收一条"：总表里有若干型号（WSE-3、TPU 系列）没有 Cost 列，
      这类行不会触发收尾，buf 会一路累积到下一条记录，把别人的带宽串进来
      （实测 B300 的带宽被串成 TPU v5e 的 0.82）。改成以「xx GB」为锚点定位更稳。
    """
    try:
        start = lines.index("Rank")
    except ValueError:
        return []
    end = len(lines)
    for i in range(start, len(lines)):
        if lines[i] == "Provider Pricing by Region":
            end = i
            break
    seg = lines[start:end]

    SKIP = {"N/A", "Rank", "Model", "Avail.", "VRAM", "BW", "Cost"}
    out = []
    for i, line in enumerate(seg):
        m = re.fullmatch(r"(\d+(?:\.\d+)?) GB", line)
        if not m:
            continue
        vram = float(m.group(1))

        # 型号名：往前第一个「非关键词、非纯数字」的行
        model = None
        for j in range(i - 1, max(-1, i - 8), -1):
            c = seg[j]
            if c in SKIP or re.fullmatch(r"[\d.]+", c):
                continue
            model = c
            break
        if not model:
            continue

        # 带宽：往后第一个纯小数（遇到 $ 就停，说明这行没带宽）
        bw = None
        for j in range(i + 1, min(len(seg), i + 5)):
            if re.fullmatch(r"\d+\.\d+", seg[j]):
                bw = float(seg[j])
                break
            if seg[j].startswith("$"):
                break

        # 价格：往后第一个 $x.xx
        cost = None
        for j in range(i + 1, min(len(seg), i + 8)):
            mm = re.fullmatch(r"\$([\d,]+\.?\d*)", seg[j])
            if mm:
                cost = float(mm.group(1).replace(",", ""))
                break

        if cost is not None:
            out.append((model, vram, bw, cost))
    return out


def parse_gpu_providers(lines, models):
    """
    Provider 明细表 → (型号名, [(provider, region, 每卡时租, 节点卡数), ...])

    结构（每 4 行一组）：
        Provider Pricing by Region / Provider / / / Region / Price/hour
        Verda / / / North Europe / $3.67 (x1 GPU)

    ⚠⚠ `(x8 GPUs)` 里那个价是**整节点总价**，不是每卡价。AWS 挂 $113.93 (x8) 就是
       p5 整机一小时的价格；直接当单卡价会虚高 8 倍（把 AWS 记成 $113.93/卡·h，
       比 B200 的 $3.67 贵 30 倍，曲线一眼就可疑）。这里统一 ÷ 节点卡数折算成每卡，
       同时把节点卡数一起落库，前端好标「8 卡节点」。

    ⚠ 这块明细只对应**页面当前选中的那一个型号**（现在是 B200 SXM）。
      型号归属靠"往前找最近出现的白名单型号名"确定，找不到就不落库 ——
      宁可少存，也不能把 A 卡的价格记到 B 卡头上。
    """
    try:
        i = lines.index("Provider Pricing by Region")
    except ValueError:
        return None, []

    target = None
    for j in range(i - 1, max(0, i - 300), -1):
        if lines[j] in GPU_MODEL_INDEX:
            target = lines[j]
            break
    if not target:
        return None, []

    out = []
    for k in range(i, len(lines) - 3):
        m = re.fullmatch(r"\$([\d,]+\.?\d*) \(x(\d+) GPUs?\)", lines[k + 3])
        if not m:
            continue
        if lines[k + 1] != "/" or not lines[k]:
            continue
        total = float(m.group(1).replace(",", ""))
        n = int(m.group(2))
        out.append((
            lines[k].strip(),
            lines[k + 2].strip(),
            round(total / max(1, n), 4),   # 每卡等效价
            n,
        ))
    return target, out


# ---------------------------------------------------------------------------
# ③ 存储成品盘 / NAND 解析
# ---------------------------------------------------------------------------
def parse_storage_list(html: str):
    """列表页 → [(prodid, 原始名, 站方币种标记, 当前价), ...]"""
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        pid = re.search(r"prodid=(\d+)", tr)
        cells = [cell_text(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if pid and len(cells) >= 5 and cells[0]:
            out.append((pid.group(1), cells[0], cells[1], cells[2]))
    return out


def extract_capacity(pid: str, cat: str, name: str):
    """
    名称 → (capacity_gb, capacity_raw, note)

    ⚠ `capacity_gb` 一律是 **GB**：NAND 的 `1Tb`=1024Gb=128GB 已在此 ÷8 折好，
      下游 `perGB = close / capacity_gb` 不需要再换算。
    ⚠ 严格边界 `(?![0-9A-Za-z])` + 大小写敏感：`256Gb`≠`256GB`、`500GNVME` 抓不到
      —— 抓不到的走 CAPACITY_OVERRIDE，**不猜**。
    """
    if pid in CAPACITY_OVERRIDE:
        gb, raw, why = CAPACITY_OVERRIDE[pid]
        return float(gb), raw, why

    if cat == "nand_wafer":
        m = CAP_BIT_RE.search(name)          # Gb / Tb（小写 b = bit）
        if m:
            v, u = float(m.group(1)), m.group(2)
            bits = v * 1024 if u.lower() == "tb" else v
            return bits / 8.0, m.group(0).strip(), None

    m = CAP_BYTE_RE.search(name)             # GB / TB（大写 B = byte）
    if m:
        v, u = float(m.group(1)), m.group(2).upper()
        return (v * 1024 if u.startswith("T") else v), m.group(0).strip(), None
    return None, None, "无法识别容量"


def extract_brand(name: str):
    low = name.lower()
    for pat, brand in BRAND_ALIAS:
        if pat.lower() in low:
            return brand
    return None


def extract_interface(name: str):
    for pat, iface in INTERFACE_RULES:
        if re.search(pat, name, re.I):
            return iface
    return None


def describe_storage(pid: str, cat: str, name: str):
    """上游原名 → (key, display, brand, capacity_gb, capacity_raw, form, iface, dtype, usage, note)"""
    cap_gb, cap_raw, note = extract_capacity(pid, cat, name)
    brand = extract_brand(name)

    dtype = next((t for t in ("TLC", "QLC", "MLC", "SLC") if t in name.upper()), None)

    if cat == "nand_wafer":
        form, iface, usage = "Wafer", None, None
    elif cat == "hdd":
        form, iface = "3.5", "SATA3"
        usage = ("监控" if "监控" in name else
                 "移动" if ("移动" in name or "睿翼" in name) else
                 "笔记本" if re.search(r"\bNB\b", name) else "桌面")
        if usage in ("移动", "笔记本"):
            form = "2.5"
    else:
        form = "M.2" if re.search(r"M\.2", name, re.I) else ("2.5" if "2.5" in name else None)
        iface = extract_interface(name)
        usage = None

    # key 用名称 slug：prodid 会变、名称几乎不变；中文品牌保留可读
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]+", "_", name).strip("_").lower()
    key = f"{cat}_{slug}"[:60]
    return key, name, brand, cap_gb, cap_raw, form, iface, dtype, usage, note


def run_storage(sess, dry_run):
    """→ (products, fetched, warns)，products 行含全部元数据，fetched: key → [(date,low,open,close)]"""
    print("\n" + "=" * 72)
    print("③ 存储成品盘 / NAND 颗粒（dram.com.cn 同站的另外四个分类）")
    print("=" * 72)

    products, fetched, warns = [], {}, []
    seen_key = {}

    for sid, cat, label, ccy, unit_sfx, lo, hi in STORAGE_CATS:
        url = STORAGE_LIST.format(sid=sid)
        try:
            r = sess.get(url, timeout=30, verify=False)
            r.raise_for_status()
        except Exception as e:
            print(f"  ✗ {label} 列表失败: {type(e).__name__}: {str(e)[:80]}")
            warns.append(f"{label} 列表抓取失败")
            continue
        html = r.content.decode("utf-8", errors="replace")
        snap = re.search(r"<ins>([^<]+)</ins>", html)
        listed = parse_storage_list(html)
        print(f"\n  ── {label}（sortid={sid}）{len(listed)} 个品种"
              f" · 站方快照 {snap.group(1) if snap else '未标注'} · 计价 {ccy}")

        ccy_fix, no_cap = [], []
        for pid, name, ccy_mark, now in listed:
            d = describe_storage(pid, cat, name)
            key, disp, brand, cap_gb, cap_raw, form, iface, dtype, usage, note = d

            # 同 key 不同 prodid = 上游改了名/加了相似品种，必须让人看到
            if key in seen_key and seen_key[key] != pid:
                warns.append(f"key 冲突 {key}: prodid {seen_key[key]} vs {pid}（{name}）")
            seen_key[key] = pid

            # 站方币种标记与分类默认不一致 → 记下来（我们按分类默认落库）
            mark = (ccy_mark or "").strip()
            if mark == "$" and ccy != "USD":
                ccy_fix.append(f"{name}（站方标 $，按 {ccy}）")
            elif mark == "￥" and ccy != "CNY":
                ccy_fix.append(f"{name}（站方标 ￥，按 {ccy}）")

            if cap_gb is None:
                no_cap.append(f"{name}（{note}）")

            try:
                dd = sess.get(STORAGE_DETAIL.format(pid=pid), timeout=30, verify=False)
                dd.raise_for_status()
                rows = parse_dram_detail(dd.content.decode("utf-8", errors="replace"))
            except Exception as e:
                print(f"    ✗ {pid} {name[:34]}: {type(e).__name__}: {str(e)[:70]}")
                rows = []

            fetched[key] = rows
            products.append({
                "key": key, "prodid": pid, "name": name, "display": disp,
                "category": cat, "brand": brand, "cap_gb": cap_gb, "cap_raw": cap_raw,
                "form": form, "iface": iface, "dtype": dtype, "usage": usage,
                "currency": ccy, "unit": f"{ccy}/{unit_sfx}",
                "ccy_note": CCY_REASON[cat], "sort": 100,
            })

            # perGB 合理性：只告警，不过滤（越界往往是名称容量写错，要人工看）
            if cap_gb and rows:
                last_close = rows[-1][3]
                if last_close:
                    per_gb = last_close / cap_gb
                    if not (lo <= per_gb <= hi):
                        warns.append(f"{label} {name} 每 GB {per_gb:.3f} {ccy} "
                                     f"超出合理区间 [{lo}, {hi}]，容量 {cap_raw} 需人工核")
            elif not rows:
                warns.append(f"{label} {name} 无历史数据")

        # 每类小结
        cnt = [p for p in products if p["category"] == cat]
        pts = [len(fetched[p["key"]]) for p in cnt]
        ok = [x for x in pts if x > 0]
        print(f"     已抓 {len(cnt)} 品种 · 历史 {sum(pts)} 行 · "
              f"点数 min/中位/max = "
              f"{min(ok) if ok else '-'}/{sorted(ok)[len(ok)//2] if ok else '-'}/{max(ok) if ok else '-'}")
        if ccy_fix:
            print(f"     ⚠ 币种已按分类校正（站方标记有误）{len(ccy_fix)} 条: {ccy_fix}")
        if no_cap:
            print(f"     ⚠ 容量识别失败 {len(no_cap)} 条（perGB 留空，价格照存）: {no_cap}")

    total = sum(len(v) for v in fetched.values())
    print(f"\n  NAND/SSD/HDD 合计 {len(products)} 品种 / {total} 行")
    if warns:
        print(f"  ⚠ 告警 {len(warns)} 条：")
        for w in warns[:20]:
            print(f"     · {w}")
        if len(warns) > 20:
            print(f"     · …… 其余 {len(warns) - 20} 条")
    return products, fetched, warns


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
    conn.commit()


def upsert_dram_products(conn, products):
    sql = """
        INSERT INTO dram_products
            (dram_key, prodid, name, generation, density_gb, speed, grade, unit, source)
        VALUES %s
        ON CONFLICT (dram_key) DO UPDATE SET
            prodid     = EXCLUDED.prodid,
            name       = EXCLUDED.name,
            generation = EXCLUDED.generation,
            density_gb = EXCLUDED.density_gb,
            speed      = EXCLUDED.speed,
            grade      = EXCLUDED.grade,
            unit       = EXCLUDED.unit,
            source     = EXCLUDED.source,
            updated_at = NOW()
    """
    rows = [(k, pid, nm, gen, den, spd, gr, DRAM_UNIT, DRAM_SOURCE)
            for (k, pid, nm, gen, den, spd, gr) in products]
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=100)
    conn.commit()


def upsert_dram_prices(conn, key, rows):
    if not rows:
        return 0
    sql = """
        INSERT INTO dram_prices (dram_key, price_date, price_low, price_open, price_close)
        VALUES %s
        ON CONFLICT (dram_key, price_date) DO UPDATE SET
            price_low   = EXCLUDED.price_low,
            price_open  = EXCLUDED.price_open,
            price_close = EXCLUDED.price_close
    """
    data = [(key, d, lo, op, cl) for (d, lo, op, cl) in rows]
    with conn.cursor() as cur:
        execute_values(cur, sql, data, page_size=500)
    conn.commit()
    return len(data)


def upsert_gpu_products(conn, products):
    sql = """
        INSERT INTO gpu_products
            (gpu_key, name, vendor, vram_gb, bandwidth_tbs,
             unit_price_low, unit_price_high, unit_price_note, unit_price_asof, unit_price_src,
             sort_order, source)
        VALUES %s
        ON CONFLICT (gpu_key) DO UPDATE SET
            name = EXCLUDED.name, vendor = EXCLUDED.vendor,
            vram_gb = EXCLUDED.vram_gb, bandwidth_tbs = EXCLUDED.bandwidth_tbs,
            unit_price_low = EXCLUDED.unit_price_low,
            unit_price_high = EXCLUDED.unit_price_high,
            unit_price_note = EXCLUDED.unit_price_note,
            unit_price_asof = EXCLUDED.unit_price_asof,
            unit_price_src  = EXCLUDED.unit_price_src,
            sort_order = EXCLUDED.sort_order, source = EXCLUDED.source,
            updated_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, products, page_size=50)
    conn.commit()


def upsert_gpu_prices(conn, rows):
    if not rows:
        return 0
    sql = """
        INSERT INTO gpu_prices
            (gpu_key, price_date, provider, region, price_hourly, gpus_per_node)
        VALUES %s
        ON CONFLICT (gpu_key, price_date, provider, region) DO UPDATE SET
            price_hourly = EXCLUDED.price_hourly,
            gpus_per_node = EXCLUDED.gpus_per_node
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=500)
    conn.commit()
    return len(rows)


def upsert_storage_products(conn, products):
    sql = """
        INSERT INTO storage_products
            (storage_key, prodid, name, display_name, category, brand,
             capacity_gb, capacity_raw, form_factor, interface, dtype, usage,
             currency, unit, currency_note, sort_order, source)
        VALUES %s
        ON CONFLICT (storage_key) DO UPDATE SET
            prodid        = EXCLUDED.prodid,
            name          = EXCLUDED.name,
            display_name  = EXCLUDED.display_name,
            category      = EXCLUDED.category,
            brand         = EXCLUDED.brand,
            capacity_gb   = EXCLUDED.capacity_gb,
            capacity_raw  = EXCLUDED.capacity_raw,
            form_factor   = EXCLUDED.form_factor,
            interface     = EXCLUDED.interface,
            dtype         = EXCLUDED.dtype,
            usage         = EXCLUDED.usage,
            currency      = EXCLUDED.currency,
            unit          = EXCLUDED.unit,
            currency_note = EXCLUDED.currency_note,
            sort_order    = EXCLUDED.sort_order,
            source        = EXCLUDED.source,
            updated_at    = NOW()
    """
    rows = [(p["key"], p["prodid"], p["name"], p["display"], p["category"], p["brand"],
             p["cap_gb"], p["cap_raw"], p["form"], p["iface"], p["dtype"], p["usage"],
             p["currency"], p["unit"], p["ccy_note"], p["sort"], STORAGE_SOURCE)
            for p in products]
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=100)
    conn.commit()


def upsert_storage_prices(conn, key, rows):
    if not rows:
        return 0
    sql = """
        INSERT INTO storage_prices (storage_key, price_date, price_low, price_open, price_close)
        VALUES %s
        ON CONFLICT (storage_key, price_date) DO UPDATE SET
            price_low   = EXCLUDED.price_low,
            price_open  = EXCLUDED.price_open,
            price_close = EXCLUDED.price_close
    """
    data = [(key, d, lo, op, cl) for (d, lo, op, cl) in rows]
    with conn.cursor() as cur:
        execute_values(cur, sql, data, page_size=500)
    conn.commit()
    return len(data)


# ---------------------------------------------------------------------------
# 任务：DRAM
# ---------------------------------------------------------------------------
def run_dram(sess, dry_run):
    print("=" * 72)
    print("① 存储颗粒现货（dram.com.cn）")
    print("=" * 72)
    print(f"→ 列表: {DRAM_LIST}")

    r = sess.get(DRAM_LIST, timeout=25, verify=False)
    r.raise_for_status()
    html = r.content.decode("utf-8", errors="replace")

    snap = re.search(r"<ins>([^<]+)</ins>", html)
    print(f"  站方快照时间: {snap.group(1) if snap else '未标注'}")

    listed = parse_dram_list(html)
    print(f"  上游品种数: {len(listed)}")

    products, skipped = [], []
    for pid, raw in listed:
        d = describe_dram(raw)
        if not d:
            skipped.append(raw)
            continue
        key, display, gen, den, spd, grade = d
        if gen not in DRAM_GENERATIONS:
            continue                      # DDR3 等：不抓、不入库
        products.append((key, pid, display, gen, den, spd, grade))
        print(f"  {key:20s} ← {raw:34s} (prodid={pid})")
    if skipped:
        print(f"  ⚠ 无法解析（已跳过）: {skipped}")

    fetched = {}
    for key, pid, display, *_ in products:
        try:
            d = sess.get(DRAM_DETAIL.format(pid=pid), timeout=30, verify=False)
            d.raise_for_status()
            rows = parse_dram_detail(d.content.decode("utf-8", errors="replace"))
            fetched[key] = rows
            first = rows[0][0] if rows else "-"
            last  = rows[-1][0] if rows else "-"
            latest = rows[-1][3] if rows else None
            print(f"  {key:20s} {len(rows):4d} 行  {first} → {last}  最新收盘 {latest}")
        except Exception as e:
            print(f"  ✗ {key}: {type(e).__name__}: {str(e)[:90]}")
            fetched[key] = []

    total = sum(len(v) for v in fetched.values())
    empty = [k for k, v in fetched.items() if not v]
    print(f"\n合计 {total} 行 / {len(fetched)} 个品种" + (f"，空品种: {empty}" if empty else ""))
    return products, fetched, empty


# ---------------------------------------------------------------------------
# 任务：GPU
# ---------------------------------------------------------------------------
def run_gpu(sess, dry_run):
    print("\n" + "=" * 72)
    print("② GPU 云租价（aimultiple.com/ai-gpus）")
    print("=" * 72)
    print(f"→ 页面: {GPU_URL}")

    r = sess.get(GPU_URL, timeout=35, verify=False)
    r.raise_for_status()
    lines = to_text_lines(r.text)

    models = parse_gpu_models(lines)
    print(f"  页面型号数: {len(models)}（全表，含消费卡/老卡）")

    picked = []
    for name, vram, bw, cost in models:
        if name in GPU_MODEL_INDEX:
            key, disp, brand, order = GPU_MODEL_INDEX[name]
            picked.append((name, vram, bw, cost))
            print(f"    {disp:14s} VRAM {vram or '?':>6} GB  带宽 {bw or '?':>5} T/s  "
                  f"最低 ${cost:.2f}/卡·小时")
    missing = [n for n in GPU_MODEL_INDEX if n not in {x[0] for x in models}]
    if missing:
        print(f"  ⚠ 白名单里未出现的型号: {missing}")

    target, providers = parse_gpu_providers(lines, models)
    print(f"\n  逐家明细归属型号: {target or '未识别'}")
    if providers:
        print(f"  {'厂商':22s} {'区域':16s} {'每卡$/h':>9s}  节点")
        for p, reg, price, n in providers:
            print(f"    {p:20s} {reg:16s} {price:>9.2f}  x{n}")

    return picked, target, providers


# ---------------------------------------------------------------------------
def storage_age_days() -> float | None:
    """距上次成功写入 storage_products 的天数；表不存在/表空/连不上库 → None（当作该跑）。

    用库里 `updated_at` 而不是本地状态文件：脚本在别的机器/容器里跑也都认同一个事实，
    而且「源站故障导致这轮没写入」时时间戳不前进 → 下一轮会自动重试，不会被节流卡住。
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(updated_at))) / 86400.0 "
                "FROM storage_products"
            )
            v = cur.fetchone()[0]
        return float(v) if v is not None else None
    except Exception:
        return None          # 表还没建 → 该跑
    finally:
        conn.close()


def main() -> int:
    argv = sys.argv[1:]
    dry_run     = "--dry-run" in argv
    only_dram   = "--dram" in argv
    only_gpu    = "--gpu" in argv
    only_storage = "--storage" in argv
    force_storage = "--force-storage" in argv
    purge_ddr3  = "--purge-ddr3" in argv
    status_only = "--status" in argv
    if not any([only_dram, only_gpu, only_storage]):
        only_dram = only_gpu = only_storage = True

    # 存储成品盘节流（115 品种 / 约 140 秒，见 STORAGE_INTERVAL_DAYS）
    storage_skipped = False
    if not status_only and only_storage and not force_storage and not dry_run:
        age = storage_age_days()
        if age is not None and age < STORAGE_INTERVAL_DAYS:
            print(f"\n[SKIP] 存储成品盘 / NAND：上次成功抓取 {age:.1f} 天前，"
                  f"未到 {STORAGE_INTERVAL_DAYS} 天窗口")
            print(f"       源站历史是滚动半年窗口，隔几天跑不丢数据；"
                  f"要立即刷新加 --force-storage")
            only_storage = False
            storage_skipped = True

    sess = requests.Session()
    sess.headers.update(HEADERS)

    dram_products, dram_fetched, dram_empty = [], {}, []
    gpu_picked, gpu_target, gpu_providers = [], None, []
    sto_products, sto_fetched, sto_warns = [], {}, []

    if not status_only and only_dram:
        dram_products, dram_fetched, dram_empty = run_dram(sess, dry_run)
    if not status_only and only_storage:
        try:
            sto_products, sto_fetched, sto_warns = run_storage(sess, dry_run)
        except Exception as e:
            print(f"✗ 存储成品盘抓取失败: {type(e).__name__}: {str(e)[:120]}")
            if not only_dram and not only_gpu:
                return 1
    if not status_only and only_gpu:
        try:
            gpu_picked, gpu_target, gpu_providers = run_gpu(sess, dry_run)
        except Exception as e:
            print(f"✗ GPU 抓取失败: {type(e).__name__}: {str(e)[:120]}")
            if only_dram is False:
                return 1

    if dry_run and not status_only:
        print("\n--dry-run：未写库")
        # 只要**跑了的**那条链路有空洞就报失败，没跑的不算
        bad = bool(dram_empty) if only_dram else False
        return 1 if bad else 0
    if not status_only and not dram_products and not gpu_picked and not sto_products:
        # 只有存储段被节流跳过、其余也没数据时，是「按计划没跑」而不是失败
        if storage_skipped:
            print("\n[OK] 存储成品盘本轮按节流跳过（未到 %d 天窗口），不算失败"
                  % STORAGE_INTERVAL_DAYS)
            return 0
        print("无数据可写")
        return 1

    today = date.today().isoformat()
    conn = psycopg2.connect(DATABASE_URL)
    try:
        ensure_schema(conn)

        # ── DRAM ──
        if dram_products:
            upsert_dram_products(conn, dram_products)
            written = sum(upsert_dram_prices(conn, k, v) for k, v in dram_fetched.items())
            print(f"\n存储颗粒：写入/更新 {written} 行")

        # ── 存储成品 / NAND ──
        if sto_products:
            upsert_storage_products(conn, sto_products)
            written = sum(upsert_storage_prices(conn, k, v) for k, v in sto_fetched.items())
            print(f"存储成品盘/NAND：写入/更新 {written} 行 / {len(sto_products)} 个品种")

        # ── GPU ──
        if gpu_picked:
            prod_rows, price_rows = [], []
            for name, vram, bw, cost in gpu_picked:
                key, disp, brand, order = GPU_MODEL_INDEX[name]
                lo, hi, note = UNIT_PRICE.get(key, (None, None, None))
                prod_rows.append((key, disp, brand, vram, bw, lo, hi, note,
                                  UNIT_PRICE_ASOF if lo else None,
                                  UNIT_PRICE_SRC if lo else None,
                                  order, GPU_SOURCE))
                # 全市场最低价（源站口径：仅 x1 单卡报价，不含整节点摊销价）
                # ⚠ region 用空串不能用 NULL：UNIQUE 里多个 NULL 互不相等，
                #   upsert 的冲突检测会失效，同一型号每次跑都插一条新行。
                price_rows.append((key, today, "market_min", "", cost, 1))

            # Rubin 占位（无报价，只为让页面能显示「暂无公开报价」而不是空）
            rk, rd, rb, ro = RUBIN_MODEL[1], RUBIN_MODEL[2], RUBIN_MODEL[3], RUBIN_MODEL[4]
            prod_rows.append((rk, rd, rb, None, None, None, None, RUBIN_NOTE,
                              None, None, ro, GPU_SOURCE))

            # 逐家明细（只属于 gpu_target 这一个型号）
            if gpu_target and gpu_providers:
                k = GPU_MODEL_INDEX[gpu_target][0]
                for p, reg, price, n in gpu_providers:
                    price_rows.append((k, today, p, reg, price, n))
                print(f"GPU 逐家明细：{len(gpu_providers)} 家 → {gpu_target}")

            upsert_gpu_products(conn, prod_rows)
            n = upsert_gpu_prices(conn, price_rows)
            print(f"GPU：写入 {len(prod_rows)} 个型号元数据 / {n} 条价格")

        if purge_ddr3:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM dram_prices WHERE dram_key LIKE 'ddr3%'")
                d1 = cur.rowcount
                cur.execute("DELETE FROM dram_products WHERE generation = 'DDR3'")
                d2 = cur.rowcount
            conn.commit()
            print(f"--purge-ddr3：删除 {d1} 行价格 / {d2} 个 DDR3 品种")

        # ── 结果回显 ──
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.dram_key, pr.name, p.price_date, p.price_close,
                       pr.density_gb,
                       ROUND(p.price_close / (pr.density_gb::numeric / 8), 2) AS per_gb
                FROM dram_prices p JOIN dram_products pr USING (dram_key)
                WHERE p.price_date = (SELECT MAX(price_date) FROM dram_prices WHERE dram_key = p.dram_key)
                  AND pr.generation IN ('DDR5', 'DDR4')
                ORDER BY pr.generation DESC, pr.grade, pr.density_gb DESC
            """)
            print(f"\n颗粒最新一行（{datetime.now():%Y-%m-%d %H:%M}）:")
            print(f"  {'品种':26s} {'日期':12s} {'美元/颗':>9s} {'美元/GB':>9s}")
            for k, nm, dt, cl, den, per_gb in cur.fetchall():
                print(f"  {nm:26s} {dt} {cl:>9} {per_gb if per_gb else '-':>9}")

            cur.execute("""
                SELECT g.gpu_key, g.name, p.price_hourly
                FROM gpu_products g
                LEFT JOIN gpu_prices p
                  ON p.gpu_key = g.gpu_key AND p.provider = 'market_min'
                 AND p.price_date = (SELECT MAX(price_date) FROM gpu_prices
                                      WHERE gpu_key = g.gpu_key AND provider = 'market_min')
                ORDER BY g.sort_order
            """)
            print(f"\nGPU 最新租价（美元/卡·小时）:")
            for k, nm, price in cur.fetchall():
                print(f"  {nm:14s} {('$%.2f' % price) if price is not None else '暂无公开报价'}")

            # ── 存储成品 / NAND 现状 ──
            # 每类给「品种数 / 最新报价日 / 每 GB 价中位数」，一眼看出哪类在动、哪类停更
            cur.execute("""
                SELECT s.category, COUNT(DISTINCT s.storage_key) AS n,
                       MAX(p.price_date) AS last_date,
                       ROUND(AVG(p.price_close / s.capacity_gb), 3) AS avg_per_gb,
                       MIN(p.price_close / s.capacity_gb) AS min_pg,
                       MAX(p.price_close / s.capacity_gb) AS max_pg
                FROM storage_products s
                JOIN storage_prices p USING (storage_key)
                WHERE s.capacity_gb > 0
                  AND p.price_date = (SELECT MAX(price_date) FROM storage_prices
                                       WHERE storage_key = s.storage_key)
                GROUP BY s.category
                ORDER BY CASE s.category
                          WHEN 'nand_wafer' THEN 1 WHEN 'ssd_oem' THEN 2
                          WHEN 'ssd_street' THEN 3 ELSE 4 END
            """)
            print(f"\n存储成品 / NAND 各类现状（截至 {datetime.now():%Y-%m-%d %H:%M}）:")
            print(f"  {'品类':16s} {'品种':>4s} {'最新报价日':12s} {'每GB均值':>9s} "
                  f"{'最低':>8s} {'最高':>8s}")
            for cat, n, last, avg_pg, mn, mx in cur.fetchall():
                label = STORAGE_CAT_INDEX.get(cat, (0, cat, cat, "?", "?", 0, 0))[2]
                ccy = STORAGE_CAT_INDEX.get(cat, (0, cat, cat, "?", "?", 0, 0))[3]
                print(f"  {label:16s} {n:>4d} {str(last):12s} "
                      f"{ccy} {float(avg_pg):>6.3f} {float(mn):>8.3f} {float(mx):>8.3f}")

            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT storage_key),
                       MIN(price_date), MAX(price_date)
                FROM storage_prices
            """)
            tot, keys, d0, d1 = cur.fetchone()
            print(f"  → storage_prices 共 {tot} 行 / {keys} 品种 / {d0} → {d1}")
    finally:
        conn.close()

    return 0 if not dram_empty else 1


if __name__ == "__main__":
    sys.exit(main())
