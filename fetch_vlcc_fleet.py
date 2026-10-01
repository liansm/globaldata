#!/usr/bin/env python3
"""
VLCC Fleet Roster Fetcher (中国船东 VLCC 船队名录)
==================================================
建立「中国船东 VLCC 船队」名录，写入 **vlcc_vessels** 表，供「油运信息」地图页使用。
配套脚本：`fetch_vlcc_position.py`（船位，aisstream.io）——本脚本只负责**名录**。

为什么名录要单独建
------------------
AIS 只认 **MMSI / 船名**，不认船东。要在地图上只显示「中国的 VLCC」，
就必须先有一份可信的船东→船名清单，再用它去过滤 AIS 流。
名录拿不到，船位就无从过滤（只能看到全世界的船）。

数据源（2026-09-30 调研，两条线）
--------------------------------
【A】中远海能（600026）—— 官方「本集团自有油轮运力」清单（PDF）
    https://energy.coscoshipping.com 投资者关系下载件，**船型列直接标 VLCC**。
    * 覆盖：109→139 行全船队，其中 VLCC **48 艘**
    * 字段全：中文船名 / 英文船名 / 权属 / 船型 / 建造时间 / 船旗
    * ⚠ **静态快照，截止 2021-06-30**，2021H2 之后交付的新船不在内
    * ⚠ 站点是 **F5 WAF**：plain curl 返回
      `<html>Please enable JavaScript to view the page content.`（5665 字节，全部路径都一样）
      → 必须用 puppeteer 过挑战才能下 PDF，见 `--help` 里的复现命令。
      因为该 PDF 是**历史存档件**（不会变），本脚本把它**内嵌为常量快照**
      `COSCO_VLCC_20210630`，不每次联网重抓 —— 要更新必须手动换快照。

【B】招商轮船（601872）—— chinashipbuild.com 船队库
    GET http://www.chinashipbuild.com/company.aspx?pklujyukkpp4JcXb[分页]
    * 覆盖：在役 **142 艘**全船队，其中 Crude Oil Tanker **59 艘**
    * 只有**英文船名**（AIS 用的就是英文名，够用）+ 载重吨 + 船厂 + 建造年月
    * ⚠ 59 艘里混着 8 艘 **阿芙拉型（10.5~11.5 万吨）**，按 **DWT ≥ 20 万** 剔除 → **VLCC 51 艘**
    * ⚠ 分页 token 是 `<base>aFLEET4<X>`（X ∈ 空/B/F/X/b/c），**必须逐页抓**，
      `Records:` 字样有但不稳，不能当分页依据

判定口径
--------
* **VLCC 定义 = 载重吨 ≥ 200,000 DWT**（业界通行下限；本库用它把阿芙拉/苏伊士剔掉）。
  中远海能源数据自带 `船型=VLCC` 标签，两套判据在本数据集上**完全一致**。
* 船东口径 = **中国船东**（招商轮船 / 中远海能），**不是**挂旗口径。
  ⚠ 这两家的 VLCC 大量挂 **香港旗 / 新加坡旗 / 巴拿马旗 / 利比里亚旗**，
  按「挂中国旗」筛会漏掉绝大多数 —— 别混。
* `name_ais` = 英文船名规范化（大写 + 折叠空白），AIS 匹配就用这个。

已知死路（不要重复踩，2026-09-30 实测）
----------------------------------------
* 招商轮船官网 `cmenergyshipping.com/about0X.html` → 502/403/404，无船队页
* 中远海能官网 plain HTTP → F5 WAF JS 挑战（见上，只能 puppeteer）
* MarineTraffic 船列表/详情 → 真实数据 XHR `403`（Cloudflare），HTML 只是空壳
* VesselFinder 搜索页/详情页 → 整体超时，IP 已被封
* 船讯网 `searchv4.shipxy.com/index.ashx?kw=` → 恒返 `{"status":0,"ship":[],"port":[]}`（要登录态）
* MarineTraffic 旧地图端点 `getData/get_data_json_4/...` → 恒返 `{"rows":[],"areaShips":0}`（已废弃）

覆盖缺口（**待补，不要假装完整**）
----------------------------------
* 中远海能：**2021-06-30 之后交付的 VLCC 不在名录里**（约 5~10 艘，如 2021H2 起的
  `新瑞洋/新隆洋/远瑞洋` 等公开报道出现的船名）→ 需人工补或换更新的官方清单
* 其它中国船东（中石油华洋/昆仑、中石化、山东海运、岚桥、振华等）**尚未纳入**
* 名录**没有 IMO/MMSI**：两个源都不给。由 `fetch_vlcc_position.py --learn` 从 AIS
  的 `ShipStaticData` 反推回写（那才是权威值，不猜）

用法
----
    python fetch_vlcc_fleet.py --dry-run     # 只抓不写，打印名录
    python fetch_vlcc_fleet.py               # 抓取 + 写入（幂等 UPSERT）
    python fetch_vlcc_fleet.py --status      # 只看库内现状
    python fetch_vlcc_fleet.py --no-scrape   # 只用内嵌快照（离线跑）

刷新中远海能快照（需要本地 chromium，走 puppeteer）：
    见 .workbuddy/tmp/vlcc_probe/cosco_pdf.cjs 的做法 —— 先 goto 首页过 WAF，
    再在页面内 `fetch(pdfUrl)` 取 bytes。**这条路已验证可用**。

合规
----
只使用：① 公开的船东官方运力清单（PDF，公司自行披露）；
② 公开的船舶建造/船队数据库页面。均为公开发布信息，不含个人信息，
不抓取受限/军事目标，不做高频请求（本脚本一次跑 6 个请求，页间 sleep 0.5s）。
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")

# VLCC 载重吨下限（业内通行口径：20 万吨）
VLCC_MIN_DWT = 200_000

CMES_BASE = "http://www.chinashipbuild.com/company.aspx?pklujyukkpp4JcXb"
CMES_PAGES = ["", "aFLEET4B", "aFLEET4F", "aFLEET4X", "aFLEET4b", "aFLEET4c"]
CMES_FLEET_TABLE_IDX = 16          # 第 16 张表 = Fleet（在役），17 = Orderbook（在建）

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# ---------------------------------------------------------------------------
# 【A】中远海能 VLCC 快照（官方「本集团自有油轮运力」PDF，截至 2021-06-30）
#     格式：中文船名|英文船名|权属|建造年|船旗
#     抽取方法见模块 docstring；**这是历史存档件，不会变，故内嵌**
# ---------------------------------------------------------------------------
COSCO_VLCC_20210630 = """
新金洋|XIN JIN YANG|中远海能|2004|CN
新宁洋|XIN NING YANG|中远海能|2005|CN
新安洋|XIN AN YANG|中远海能|2007|CN
新通洋|XIN TONG YANG|中远海能(香港)|2009|HK
新潤洋|XIN RUN YANG|中远海能(香港)|2009|HK
新岳洋|XIN YUE YANG|中远海能(香港)|2009|HK
新漢洋|XIN HAN YANG|中远海能(香港)|2009|HK
新埔洋|XIN PU YANG|中远海能|2010|CN
新甬洋|XIN YONG YANG|中远海能|2010|CN
新申洋|XIN SHEN YANG|中远海能|2010|CN
新厦洋|XIN XIA YANG|中远海能|2011|CN
新丹洋|XIN DAN YANG|中远海能(香港)|2013|HK
新连洋|XIN LIAN YANG|中远海能(香港)|2014|HK
远鲲洋|YUAN KUN YANG|中远海能(香港)|2020|HK
远鹏洋|YUAN PENG YANG|中远海能(香港)|2021|HK
新龙洋|XIN LONG YANG|中远海能(新加坡)|2017|SG
新威洋|XIN WEI YANG|中远海能(新加坡)|2017|SG
新惠洋|XIN HUI YANG|中远海能(新加坡)|2018|SG
新茂洋|XIN MAO YANG|中远海能(新加坡)|2018|SG
远洋湖|YUAN YANG HU|中远海能(海南)|2010|CN
远山湖|YUAN SHAN HU|中远海能(海南)|2010|CN
远春湖|YUAN CHUN HU|中远海能(海南)|2014|CN
远月湖|YUAN YUE HU|中远海能(海南)|2015|CN
远秋湖|YUAN QIU HU|中远海能(海南)|2015|CN
远花湖|YUAN HUA HU|中远海能(海南)|2015|CN
远华洋|YUAN HUA YANG|中远海能(海南)|2020|CN
远贵洋|YUAN GUI YANG|中远海能(海南)|2020|CN
远福洋|YUAN FU YANG|中远海能(海南)|2021|CN
远大湖|COSGREAT LAKE|中远海能(海南)|2002|PA
远荣湖|COSGLORY LAKE|中远海能(海南)|2003|PA
远明湖|COSBRIGHT LAKE|中远海能(海南)|2003|PA
远盛湖|COSGRAND LAKE|中远海能(海南)|2006|PA
远惠湖|COSGRACE LAKE|中远海能(海南)|2006|PA
远怡湖|COSMERRY LAKE|中远海能(海南)|2006|PA
远珍湖|COSPEARL LAKE|中远海能(海南)|2008|HK
远翠湖|COSJADE LAKE|中远海能(海南)|2009|HK
远金湖|COSGOLD LAKE|中远海能(海南)|2011|HK
远兴湖|COSGLAD LAKE|中远海能(海南)|2011|HK
远富湖|COSRICH LAKE|中远海能(海南)|2012|HK
远翔湖|COSFLYING LAKE|中远海能(海南)|2015|HK
远智湖|COSWISDOM LAKE|中远海能(海南)|2016|HK
远腾湖|COSRISING LAKE|中远海能(海南)|2016|HK
远尊湖|COSDIGNITY LAKE|中远海能(海南)|2017|HK
远喜湖|COSLUCKY LAKE|中远海能(海南)|2017|HK
远誉湖|COSHONOUR LAKE|中远海能(海南)|2017|HK
远旺湖|COSFLOURISH LAKE|中远海能(海南)|2017|HK
远贺湖|COSWISH LAKE|中远海能(海南)|2018|HK
远新湖|COSNEW LAKE|中远海能(海南)|2018|HK
""".strip()

COSCO_SOURCE = "中远海能官网《本集团自有油轮运力》截至2021-06-30（船型列标注 VLCC）"
CMES_SOURCE = "chinashipbuild.com 招商轮船船队库（在役，按 DWT≥20万 判 VLCC）"

# 已核实的「英文名 → 中文名」映射。**只放有独立证据的**，其余留空 —— 宁缺勿错。
#   NEW VISION  = 新海辽：2019-08-28 大船集团交付，30.8 万吨（与名录 307,434 / 2019-08 吻合）
#   NEW SPLENDOR= 凯辉  ：2023-01-04 交付，30.7 万吨
KNOWN_CN = {
    "NEW VISION": "新海辽",
    "NEW SPLENDOR": "凯辉",
}

SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS vlcc_vessels (
    id          BIGSERIAL PRIMARY KEY,
    name_ais    VARCHAR(120) NOT NULL,      -- 规范化英文船名（AIS 匹配键）
    name_en     VARCHAR(120) NOT NULL,      -- 英文船名原文
    name_cn     VARCHAR(120),               -- 中文船名（拿不到就留空，不猜）
    owner       VARCHAR(60)  NOT NULL,      -- 船东口径：招商轮船 / 中远海能
    owner_full  VARCHAR(200),               -- 权属明细（含单船公司）
    dwt         NUMERIC(14,2),              -- 载重吨
    built_year  INTEGER,
    flag        VARCHAR(12),                -- CN/HK/SG/PA/LR/MH/MT
    source      VARCHAR(200) NOT NULL,
    imo         VARCHAR(16),                -- 由 fetch_vlcc_position.py --learn 反推
    mmsi        VARCHAR(16),
    verified    BOOLEAN NOT NULL DEFAULT FALSE,  -- 是否已被 AIS 实见（ShipStaticData 佐证）
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS vlcc_vessels_name_ais_uniq ON vlcc_vessels (name_ais);
CREATE INDEX IF NOT EXISTS idx_vlcc_vessels_owner ON vlcc_vessels (owner);
"""

UPSERT_SQL = """
INSERT INTO vlcc_vessels
    (name_ais, name_en, name_cn, owner, owner_full, dwt, built_year, flag, source)
VALUES %s
ON CONFLICT (name_ais) DO UPDATE SET
    name_en    = EXCLUDED.name_en,
    name_cn    = COALESCE(EXCLUDED.name_cn, vlcc_vessels.name_cn),
    owner      = EXCLUDED.owner,
    owner_full = EXCLUDED.owner_full,
    dwt        = COALESCE(EXCLUDED.dwt, vlcc_vessels.dwt),
    built_year = COALESCE(EXCLUDED.built_year, vlcc_vessels.built_year),
    flag       = COALESCE(EXCLUDED.flag, vlcc_vessels.flag),
    source     = EXCLUDED.source,
    updated_at = NOW()
"""


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def norm_name(s: str) -> str:
    """AIS 匹配键：大写 + 折叠空白 + 去掉首尾。AIS 报文里的船名常有多余空格。"""
    return re.sub(r"\s+", " ", (s or "").strip()).upper()


def strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s.replace("&nbsp;", " ").replace("&amp;", "&")).strip()


# ---------------------------------------------------------------------------
# 【A】中远海能内嵌快照
# ---------------------------------------------------------------------------
# 快照里的短权属标签 → 官方 PDF 的完整权属表述（便于追溯）
COSCO_OWNER_FULL = {
    "中远海能": "中远海运能源运输股份有限公司",
    "中远海能(香港)": "中海发展（香港）航运有限公司之附属单船公司",
    "中远海能(新加坡)": "中远海运油品运输（新加坡）有限公司之附属单船公司",
    "中远海能(海南)": "海南中远海运能源运输有限公司之附属单船公司",
    "海南中远海运能源运输有限公司": "海南中远海运能源运输有限公司",
}


def parse_cosco_snapshot() -> list[dict]:
    out = []
    for ln in COSCO_VLCC_20210630.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        cn, en, owner, year, flag = [x.strip() for x in ln.split("|")]
        out.append({
            "name_ais": norm_name(en),
            "name_en": en,
            "name_cn": cn,
            "owner": "中远海能",
            "owner_full": COSCO_OWNER_FULL.get(owner, owner),
            "dwt": None,                     # 官方 PDF 只给船型不给吨位
            "built_year": int(year),
            "flag": flag,
            "source": COSCO_SOURCE,
        })
    return out


# ---------------------------------------------------------------------------
# 【B】招商轮船：chinashipbuild 船队库
# ---------------------------------------------------------------------------
def fetch_cmes_fleet(verbose: bool = True) -> tuple[list[dict], list[str]]:
    """抓招商轮船在役船队，返回 (VLCC 列表, 失败页描述)。失败**不静默**。"""
    seen, fleet, failures = set(), [], []
    for sfx in CMES_PAGES:
        url = CMES_BASE + sfx
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=25) as r:
                html = r.read().decode("utf-8", "ignore")
        except Exception as exc:
            failures.append(f"{sfx or 'p1'}: {exc}")
            if verbose:
                print(f"  [FAIL] 页 {sfx or 'p1'} 抓取失败: {exc}", file=sys.stderr)
            continue

        tbs = re.findall(r"<table[^>]*>(.*?)</table>", html, re.S | re.I)
        if len(tbs) <= CMES_FLEET_TABLE_IDX:
            failures.append(f"{sfx or 'p1'}: 表数量异常 {len(tbs)}")
            continue

        new = 0
        for r in re.findall(r"<tr[^>]*>(.*?)</tr>", tbs[CMES_FLEET_TABLE_IDX], re.S | re.I):
            cells = [strip_tags(c)
                     for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", r, re.S | re.I)]
            if not cells or not re.match(r"^\d+$", cells[0] or ""):
                continue
            name = cells[1]
            if name in seen:
                continue
            seen.add(name)
            fleet.append({
                "name": name,
                "type": cells[2] if len(cells) > 2 else "",
                "built": cells[4] if len(cells) > 4 else "",
            })
            new += 1
        if verbose:
            print(f"  页 {sfx or 'p1':8s} 新增 {new:3d} 艘（累计 {len(fleet)}）")
        time.sleep(0.5)

    out = []
    for f in fleet:
        m = re.search(r"Crude Oil Tanker,\s*([\d,]+)\s*tons", f["type"])
        if not m:
            continue
        dwt = int(m.group(1).replace(",", ""))
        if dwt < VLCC_MIN_DWT:          # 阿芙拉/苏伊士等，不是 VLCC
            continue
        ym = re.match(r"(\d{4})", f["built"])
        out.append({
            "name_ais": norm_name(f["name"]),
            "name_en": f["name"],
            "name_cn": KNOWN_CN.get(norm_name(f["name"])),
            "owner": "招商轮船",
            "owner_full": "招商局能源运输股份有限公司",
            "dwt": dwt,
            "built_year": int(ym.group(1)) if ym else None,
            "flag": None,               # 该源不给船旗
            "source": CMES_SOURCE,
        })
    return out, failures


# ---------------------------------------------------------------------------
# 组装 / 写库
# ---------------------------------------------------------------------------
def build_roster(no_scrape: bool = False) -> tuple[list[dict], list[str]]:
    rows = parse_cosco_snapshot()
    print(f"  [A] 中远海能（内嵌官方快照 2021-06-30）: {len(rows)} 艘")
    failures = []
    if no_scrape:
        print("  [B] 招商轮船：--no-scrape 跳过联网抓取")
    else:
        cmes, failures = fetch_cmes_fleet()
        print(f"  [B] 招商轮船（chinashipbuild）: {len(cmes)} 艘 VLCC"
              f"（≥{VLCC_MIN_DWT // 10000} 万吨）")
        rows += cmes

    # 同名去重（不同源可能撞名，保留先到的 A 类来源更权威）
    merged = {}
    for r in rows:
        merged.setdefault(r["name_ais"], r)
    rows = list(merged.values())
    print(f"  合计 {len(rows)} 艘 VLCC"
          f"（招商轮船 {sum(1 for r in rows if r['owner'] == '招商轮船')} / "
          f"中远海能 {sum(1 for r in rows if r['owner'] == '中远海能')}）")
    return rows, failures


def write_db(conn, rows: list[dict]) -> int:
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
        entries = [
            (r["name_ais"], r["name_en"], r["name_cn"], r["owner"], r["owner_full"],
             r["dwt"], r["built_year"], r["flag"], r["source"])
            for r in rows
        ]
        execute_values(cur, UPSERT_SQL, entries)
    conn.commit()
    return len(entries)


def show_status(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT owner,
                   COUNT(*)                        AS ships,
                   COUNT(mmsi)                     AS with_mmsi,
                   COUNT(*) FILTER (WHERE verified) AS verified,
                   MIN(built_year)                 AS oldest,
                   MAX(built_year)                 AS newest
            FROM vlcc_vessels GROUP BY owner ORDER BY 2 DESC
        """)
        rows = cur.fetchall()
    if not rows:
        print("  (空表)")
        return
    print(f"  {'船东':<12s}{'艘数':>5s}{'有MMSI':>8s}{'AIS实见':>9s}{'最早建':>8s}{'最新建':>8s}")
    for o, n, m, v, lo, hi in rows:
        print(f"  {o:<12s}{n:>5d}{m:>8d}{v:>9d}{str(lo or '—'):>8s}{str(hi or '—'):>8s}")


def main() -> int:
    ap = argparse.ArgumentParser(description="中国船东 VLCC 船队名录 → vlcc_vessels")
    ap.add_argument("--dry-run", action="store_true", help="只抓不写")
    ap.add_argument("--no-scrape", action="store_true", help="不联网，只用内嵌快照")
    ap.add_argument("--status", action="store_true", help="只看库内现状")
    ap.add_argument("--dump", metavar="PATH", help="把名录写成 JSON 备查")
    args = ap.parse_args()

    if args.status:
        conn = psycopg2.connect(DATABASE_URL)
        print("[vlcc_vessels 现状]")
        show_status(conn)
        conn.close()
        return 0

    print("[1/3] 组装名录")
    rows, failures = build_roster(no_scrape=args.no_scrape)

    if failures:
        # 部分页面失败 → 名录会**缺船**，必须让调用方（refresh_all）看到非 0
        print(f"\n[FAIL] {len(failures)} 个船队页未抓到，名录不完整（重跑可补）：")
        for f in failures:
            print(f"    {f}")

    if args.dump:
        with open(args.dump, "w", encoding="utf-8") as fh:
            json.dump(rows, fh, ensure_ascii=False, indent=1)
        print(f"  → {args.dump}")

    if args.dry_run:
        print("\n[2/3] --dry-run，跳过写库\n")
        for r in rows:
            print(f"  {r['owner']:<6s} {r['name_ais']:<18s} "
                  f"{r['name_cn'] or '—':<7s} {r['dwt'] or '—':>9} {r['built_year'] or '—'} "
                  f"{r['flag'] or '—'}")
        return 1 if failures else 0

    print("\n[2/3] 写入 vlcc_vessels")
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except psycopg2.OperationalError as exc:
        print(f"[FATAL] 连不上数据库: {exc}")
        return 1
    try:
        n = write_db(conn, rows)
        print(f"  [OK] 写入/更新 {n} 行")
        print("\n[3/3] 库内现状")
        show_status(conn)
    except Exception as exc:
        conn.rollback()
        print(f"[FATAL] 数据库错误: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()

    print("\n提示：名录缺 IMO/MMSI（源侧不给），跑 fetch_vlcc_position.py --learn 从 AIS 反推回写。")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
