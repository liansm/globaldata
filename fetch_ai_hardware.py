#!/usr/bin/env python3
"""
AI Hardware Prices (AI 硬件价格：存储颗粒 + GPU 算力)
=====================================================
一个脚本、两个数据源、两块口径完全不同的价格。**别混着讲**：

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

Data sources
------------
    DRAM 列表 : http://www.dram.com.cn/ajax_asp/productprice_list.asp?menuid=41
    DRAM 历史 : http://www.dram.com.cn/ajax_asp/productpriceD_list.asp?menuid=41&prodid=<id>
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
    dram_products / dram_prices   存储颗粒（已有，本脚本接管维护）
    gpu_products  / gpu_prices    GPU 算力（本脚本新建）

Usage
-----
    python fetch_ai_hardware.py             # 抓 DRAM + GPU，幂等 upsert（日常跑这个）
    python fetch_ai_hardware.py --dry-run   # 只抓取打印，不写库
    python fetch_ai_hardware.py --dram      # 只跑存储颗粒
    python fetch_ai_hardware.py --gpu       # 只跑 GPU
    python fetch_ai_hardware.py --purge-ddr3  # 顺带删掉库里的 DDR3 旧数据（一次性）
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
def main() -> int:
    argv = sys.argv[1:]
    dry_run     = "--dry-run" in argv
    only_dram   = "--dram" in argv
    only_gpu    = "--gpu" in argv
    purge_ddr3  = "--purge-ddr3" in argv
    if not only_dram and not only_gpu:
        only_dram = only_gpu = True

    sess = requests.Session()
    sess.headers.update(HEADERS)

    dram_products, dram_fetched, dram_empty = [], {}, []
    gpu_picked, gpu_target, gpu_providers = [], None, []

    if only_dram:
        dram_products, dram_fetched, dram_empty = run_dram(sess, dry_run)
    if only_gpu:
        try:
            gpu_picked, gpu_target, gpu_providers = run_gpu(sess, dry_run)
        except Exception as e:
            print(f"✗ GPU 抓取失败: {type(e).__name__}: {str(e)[:120]}")
            if only_dram is False:
                return 1

    if dry_run:
        print("\n--dry-run：未写库")
        return 0 if not dram_empty else 1
    if not dram_products and not gpu_picked:
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
    finally:
        conn.close()

    return 0 if not dram_empty else 1


if __name__ == "__main__":
    sys.exit(main())
