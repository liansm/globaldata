#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
中国船东 VLCC 船位 —— HiFleet（船队在线 / 亿海蓝）REST 采集
============================================================

为什么换源（2026-09-30 实测定论，不是拍脑袋）
---------------------------------------------
前一个源 aisstream.io 是**免费但覆盖塌陷**。窄 bbox 逐区实测：
波斯湾（VLCC 主力装货区）3 分钟 **0 条**；红海/阿拉伯海/马六甲北/南中国海/上海长江口
全部 **0 条/秒**；而全地球订阅那 ~43 条/秒里，**六成来自北海**。
再用 `FiltersShipMMSI` 精确订阅已从流里学到的 NEW WISDOM(477095600) /
NEW DRAGON(477127300)，4 分钟仍是 **0 条**。
→ **不是限速、不是没 MMSI、不是 key 的问题，是当地没有接收站。**
中国 VLCC 的主力航线（波斯湾装货 → 印度洋/红海 → 马六甲 → 南中国海 → 中国卸货）
几乎全在盲区，所以免费源满足不了「查询所有中国 VLCC 的地图信息」。

`fetch_vlcc_position.py`（aisstream 版）**保留**，作海外段补充与调研存档；
本脚本是**主源**。

源
--
HiFleet API（默认基址 `https://api.hifleet.com`，可用 `HIFLEET_API_BASE` 覆盖）
  ① 船舶搜索   GET {base}/position/shipSearch?shipname=&api_key=&i18n=zh&count=50
  ② 最新船位   GET {base}/position/position/get/token?mmsi=&api_key=
  ③ 账号余额   GET {base}/openclaw/account/summary          （header `x-api-key`）
  ④ 区域船舶   GET {base}/position/gettraffic/token?bbox=... （本脚本不用，见下）

⚠⚠ 两个致命单位陷阱（踩过，记档）
----------------------------------
1. **位置接口的 `la` / `lo` 单位是「分」，不是「度」** —— 必须 ÷60。
   官方原文：「纬度（度） = parseFloat(list.la) / 60」。
   不换算会把船放到 60 倍远的地方（例：1874.115 分 = 31.23525 度）。
   **就是一次简单浮点除，不要当 DMS 去解析。**
2. **`ti` 是 UTC+8（北京时间）**，而库里的 `ts` 统一存 UTC。必须 −8h 再落库，
   否则全部船位会显示成「8 小时后更新」。aisstream 那条路给的是 UTC，
   两条路写同一张表，**这里串味了就会静默错 8 小时**。

⚠ 同一家 API 内部单位就不统一（记档，别再踩）：
  `gettraffic`（区域船舶）返回的 `lat`/`lon` **是「度」**，和位置接口的「分」相反。
  以后若要改用区域接口批量取数，务必按「度」处理。

为什么不用区域接口 `gettraffic` 批量扫
--------------------------------------
名录 99 艘分布在**全球**各航段，用 bbox 要盖住整个印度洋+大西洋+太平洋才能捞全，
既扫出几十万条无关船（按返回条数计费的话更贵），又仍需本地按船名/MMSI 过滤。
**逐船 99 次查询**成本可预测、可对账、结果恰好等于「我们关心的那 99 艘」——故选它。

计费（2026-09 控制台公开价目，实际以控制台实时为准）
----------------------------------------------------
  订阅（周期额度，月度重置）：Plus ¥49 = 50,000 点 ／ Max ¥119 = 150,000 ／ Ultra ¥469 = 600,000
  积分包（365 天有效）：     入门 ¥30 = 4,285 ／ 进阶 ¥150 = 21,430 ／ 高级 ¥500 = 71,435
  ≈ ¥0.007 / 点
⚠ **单次查位扣多少点，官方文档未公开**。本脚本每轮打印搜索段 / 查位段各自的余额差。
2026-10-01 实测（账号买的是 1000 点积分包）：
  * **查位：3 次 → 扣 0 点**；**搜索：9 次 → 扣 1 点**
  * 更有参考价值的是账号接口的 `totalUsedPoints`：**整轮全量（99 次搜索 + 49 次查位）
    累计只用了 1 点**（结束值 2.0，其中 1 点是本脚本探测产生的）
  * 官方 `fieldGuide` 原文：「`pendingDeduction` = 最近调用尚未入账的消耗，
    **小时结束后**会自动从 `accountBalance` 扣除」→ 计费疑似**按小时结算**，
    与调用次数非线性相关
⇒ **当前账号下一轮全量约 1 点**，1000 点足够极高频刷新。但结算口径官方未公开，
  以上是观测推断；**要坐实就再跑一轮、隔一小时看 `totalUsedPoints` 的增量**。

口径
----
* 只查名录里的 99 艘（口径 = **中国船东**，非挂旗），不扫区域、不囤别人的船。
* 名录两个源本身**都不给 MMSI** → 先 `shipSearch` 按船名补 MMSI/IMO，
  顺手把两个历史缺口一起补上：
    - 中远海能 48 艘的 `dwt`（原官网 PDF 没有载重吨列，之前全是 NULL）
    - 招商轮船 51 艘的 `flag`（原源无船旗，之前全是 NULL）
  补全用 `COALESCE`，**已有值绝不覆盖**（宁可留着已知的，也不被来源不明的覆盖）。
* 船名匹配三级：**全等 → 按吨位筛选 → 按 IMO 归并**（细节见 `match_ship` docstring）。
  仍不确定的一律**不猜**，逐条把原因打出来给人看。
  ⚠ **2026-10-01 实测的三条厂商行为，动匹配逻辑前必读**：
    1. **`shipname` 是精确匹配，不支持前缀/模糊** —— `NEW ACHIEVEM` 截断 → 0 条，
       而 `NEW ACHIEVEMENT` → 2 条。所以「搜不到」时换写法基本无效，
       别在这上面浪费时间（`COSBRIGHT LAKE` 试了 5 种写法全是 0）。
    2. **同名混小船是常态** —— 搜 `NEW VISION` 返回 5 条，真船 30.7 万吨，
       另有越南籍 2.3 万吨、马绍尔籍 15.7 万吨混在里面。**必须按 ≥20 万吨筛**，
       否则永远判成「多命中」而放弃（这就是 50 艘没配上 MMSI 的主因之一）。
    3. **同一艘船可能有多个 MMSI** —— `NEW WISDOM` 的 477095600 与 477095664
       **IMO 同为 9486506**。这类属于「一艘船」→ 按最新报文挑一个（`pick_freshest`），
       不算歧义；只有 **IMO 不同**才是真的两艘同名船 → 拒绝。
  ⚠ **中文名搜索是模糊匹配，会返回吨位完全不对的船**（搜「远明湖」返回
    `YUAN MAN JIN`，328 吨）→ **不能用来补名录**，只可当「确实不在索引里」的旁证。
* `ts` 用数据源给的更新时间，**不是**我们的请求时间；船在远洋旧一点是正常的。
* 位置不插值、不推算、不用目的地港坐标兜底。同一 (name_ais, ts) 唯一，重跑幂等。

⚠ 名录时效性（2026-10-01 查实，**别忽略**）
--------------------------------------------
中远海能那 48 艘来自**官网 2021-06-30 的 PDF 快照**，而中远此后在持续甩卖老旧 VLCC
（行业报道与船史源均可印证）。已逐条核实 4 艘**已转手改名**：
    COSBRIGHT LAKE (9263227) → Lake → Lake 1 → Lake 2 → SUN I
    COSGLORY  LAKE (9245782) → LILA HAIKOU → TOYOMI → FIRENZE K
    COSGRAND  LAKE (9294575) → LILA JAMNAGAR（2025-11 售出）
    COSGREAT  LAKE (9263215) → LILA ZHUHAI → WIN WIN → BIG MAG
⇒ 症状就是「按名字搜不到」或「报位停在几年前的某一天」。**这不是 bug，是名录过期。**
  要根治得把中远名录换成**当前在役**船队源；在那之前，本脚本对这类船
  如实标成「搜不到 / 报位很旧」，**不编位置、不拿别的船顶替、不假装实时**。

  这 4 艘已登记进模块顶部的 `RETIRED`：**匹配之前就挡掉，不搜、不查位**。
  ⚠ 光「清空 MMSI」是不够的 —— 下一次运行阶段一会把它们**重新学回来**
  （搜得到名字、身份就回填），阶段二再把旧报文写回图上，幽灵船位会自己长回来。
  用 `--fix-retired` 一次擦净（清身份 + 删残留船位），`--dry-run` 只列不改。

失败 vs 无数据（本项目最忌静默失败，这里分开算）
------------------------------------------------
* **请求层失败**（HTTP 错 / `result != ok` / 网络）→ 记账、跑完 exit 1，必须可见。
* **无数据**（该船查不到位置、无经纬度）→ 正常现象，逐条记录，**不判失败**。
* 请求层失败累计超过 `--max-errors`（默认 5）→ **立刻中止**：key 有问题时继续跑
  只会白烧积分。

用法
----
    python fetch_vlcc_position_hifleet.py --selftest            # 不用 key：验算 ÷60 / −8h
    python fetch_vlcc_position_hifleet.py --check               # 验 key+余额+一次搜索，不写库
    python fetch_vlcc_position_hifleet.py --limit 5 --dry-run   # 先花 5 艘试水，看扣点
    python fetch_vlcc_position_hifleet.py --search-only         # 只补名录 MMSI/DWT/船旗
    python fetch_vlcc_position_hifleet.py                       # 全量：补名录 + 取 99 艘船位
    python fetch_vlcc_position_hifleet.py --status              # 库内覆盖率/新鲜度
    python fetch_vlcc_position_hifleet.py --status --stale-days 3
    python fetch_vlcc_position_hifleet.py --fix-suspect --dry-run   # 列出可疑错配（不改）
    python fetch_vlcc_position_hifleet.py --fix-suspect             # 清掉硬可疑，下次重搜
    python fetch_vlcc_position_hifleet.py --fix-retired --dry-run   # 列出已转手船的身份/残留船位
    python fetch_vlcc_position_hifleet.py --fix-retired             # 擦净（清身份 + 删船位）

    # key 二选一：
    #   1) .env 里写  HIFLEET_API_KEY=sk_live_xxxx
    #   2) 命令行     --key sk_live_xxxx
    # 控制台（注册/套餐/积分/发票）：https://skills.hifleet.com/openclaw/console.html#/plans

合规
----
AIS 是船舶**公开播发**的自动识别信息。只取「名录内单船最新位置」这一必要范围，
不做区域扫描、不批量囤积、不高频轮询 —— 船位本身更新频率就是分钟~小时级，
按小时级刷新即可，**没有理由跑秒级**。key 只走环境变量/命令行，不落前端、不入库。
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
import requests
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
API_BASE     = (os.environ.get("HIFLEET_API_BASE") or "https://api.hifleet.com").rstrip("/")

SHIP_SEARCH     = "/position/shipSearch"
POSITION_GET    = "/position/position/get/token"
AREA_TRAFFIC    = "/position/gettraffic/token"        # 未使用，留作参考
ACCOUNT_SUMMARY = "/openclaw/account/summary"

HTTP_TIMEOUT = 20
SOURCE       = "hifleet"
MMSI_RE      = re.compile(r"^\d{9}$")

# VLCC 载重吨门槛（行业口径 ≥20 万吨），用于在同名船里挡掉小船。
# 这不是洁癖：实测搜 "NEW VISION" 返回 5 条同名，真船 30.7 万吨，
# 另外混着越南籍 2.3 万吨、马绍尔籍 15.7 万吨 —— 不按吨位筛就永远判成「多命中」。
VLCC_MIN_DWT = 200_000

# 与**名录已知吨位**的对齐容差。名录（招商那源）给了每艘的载重吨，
# 而 HiFleet 候选也带 dwt —— 两条独立来源对得上，是同名船里最硬的一刀：
# 实测 NEW ADEN 真船 306193 vs 名录 306474（差 0.09%），冒名的那条 311080（差 1.50%）。
# 容差取 1%：够放下来源间的正常尾数差异，又挡得住 1.5% 以上的冒名。
DWT_ALIGN_TOL = 0.01

# ---------------------------------------------------------------------------
# 已退役（已转手改名）的名录条目 —— **不搜、不查位**
# ---------------------------------------------------------------------------
# 为什么需要这张表，而不是「清空 MMSI 就算完」：
#   名录是**中远海能官网 2021-06-30 的 PDF 快照**，之后再没更新。清空 MMSI 只是
#   把库里的身份擦掉，下一次运行阶段一**照样会搜到、照样回填**，阶段二再把那条
#   2019/2022 年的旧报文写回图上 —— 幽灵船位会自己长回来。要让「清空」真正生效，
#   必须在**匹配之前**就把这些名字挡掉。
#
# 怎么确认的（2026-10-01）：查船史源看改名链 —— Miramar / ShipSpotting / Splash247，
# 以及中远自己的出售公告新闻。**不要靠「搜不到」推断已退役**：搜不到也可能只是
# 源侧索引缺口，两者处理方式完全不同（前者跳过，后者要换源）。
RETIRED = {
    # 名录名           : (IMO, 现名 / 改名链, 转手时间, 依据)
    "COSBRIGHT LAKE": ("9263227", "SUN I（Lake → Lake 1 → Lake 2）",       "2023-04", "Miramar / ShipSpotting"),
    "COSGLORY LAKE":  ("9245782", "FIRENZE K（LILA HAIKOU → TOYOMI）",     "2023-02", "Miramar / ShipSpotting"),
    "COSGREAT LAKE":  ("9263215", "BIG MAG（LILA ZHUHAI → WIN WIN）",      "2023-01", "Miramar / ShipSpotting"),
    "COSGRAND LAKE":  ("9294575", "LILA JAMNAGAR",                          "2025-11", "出售公告新闻（Splash247）"),
}


def retired_info(name_ais: str):
    return RETIRED.get(norm_name(name_ais))


SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS vlcc_positions (
    id         BIGSERIAL PRIMARY KEY,
    name_ais   VARCHAR(120) NOT NULL,       -- 关联 vlcc_vessels.name_ais
    mmsi       VARCHAR(16),
    ts         TIMESTAMPTZ NOT NULL,        -- 数据源给出的更新时间（统一存 UTC）
    lat        NUMERIC(10,6) NOT NULL,
    lon        NUMERIC(10,6) NOT NULL,
    sog        NUMERIC(8,2),                -- 对地航速 节
    cog        NUMERIC(8,2),                -- 对地航向 度
    heading    NUMERIC(8,2),                -- 船首向 度
    nav_status VARCHAR(40),
    dest       VARCHAR(120),                -- 目的港（AIS 填报）
    draught    NUMERIC(8,2),
    source     VARCHAR(40) NOT NULL DEFAULT 'aisstream',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS vlcc_positions_name_ts_uniq ON vlcc_positions (name_ais, ts);
CREATE INDEX IF NOT EXISTS idx_vlcc_positions_name_ts ON vlcc_positions (name_ais, ts DESC);
"""

UPSERT_POS_SQL = """
INSERT INTO vlcc_positions
    (name_ais, mmsi, ts, lat, lon, sog, cog, heading, nav_status, dest, draught, source)
VALUES %s
ON CONFLICT (name_ais, ts) DO UPDATE SET
    lat = EXCLUDED.lat, lon = EXCLUDED.lon, sog = EXCLUDED.sog, cog = EXCLUDED.cog,
    heading = EXCLUDED.heading, nav_status = EXCLUDED.nav_status,
    dest = COALESCE(EXCLUDED.dest, vlcc_positions.dest),
    draught = COALESCE(EXCLUDED.draught, vlcc_positions.draught)
"""


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------
def norm_name(s) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).upper()


def sk(s) -> str:
    """去空格与符号，用于「YUAN  GUI  YANG」这种源侧多空格的兜底比对。"""
    return re.sub(r"[^A-Z0-9]", "", norm_name(s))


def to_float(v):
    """HiFleet 用 '-' / 'NULL' / '-1' 表示「无此数据」，必须当 None，不能当 0。"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "" or s.upper() in ("NULL", "NONE", "N/A", "NA", "-", "--"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def minutes_to_deg(v):
    """⚠ HiFleet 位置接口的 la/lo 单位是「分」，÷60 才是度。"""
    f = to_float(v)
    return None if f is None else f / 60.0


def clean_sog(v):
    f = to_float(v)
    if f is None or f < 0 or f > 102.2:      # AIS 航速上限 102.2 节
        return None
    return f


def clean_angle(v):
    f = to_float(v)
    if f is None or f < 0 or f > 360:
        return None
    return f


def clean_draught(v):
    f = to_float(v)
    if f is None or f <= 0 or f > 40:        # 吃水 >40m 不可能是 VLCC 的真实值
        return None
    return f


def parse_hifleet_time(s):
    """⚠ `ti` 是 UTC+8（北京时间）→ 转成 UTC 返回。

    实测格式：'2022-04-25 10:31:53' / '2022-04-25 10:31:53.0'
    """
    if not s:
        return None
    t = str(s).strip()
    if not t or t.upper().startswith("NULL"):
        return None
    t = t.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(t, fmt)
            return (dt - timedelta(hours=8)).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def dig(obj, key):
    """在嵌套 JSON 里递归找第一个同名 key —— 账号接口的返回结构未公开，别硬编码路径。"""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            got = dig(v, key)
            if got is not None:
                return got
    elif isinstance(obj, list):
        for v in obj:
            got = dig(v, key)
            if got is not None:
                return got
    return None


def envelope_error(d) -> str | None:
    """判定响应信封，返回错误描述；None = 成功。

    ⚠ 两种信封**都真实存在**（2026-09-30 实测）：
      * 成功帧：文档写 `{"result": "ok", "num": N, "list": ...}`
      * 错误帧：实测 `{"code":"token_invalid","success":false,"message":"AI token is invalid."}`
    **不能只认 `result == "ok"`** —— 线上若把成功帧也换成 `success:true` 那套，
    只认 result 就会把成功误判成失败（或反之）。故按「有没有明确失败信号」判定。
    """
    if not isinstance(d, dict) or not d:
        return "空响应"
    if d.get("success") is True or d.get("result") == "ok":
        return None
    code = d.get("code")
    if code not in (None, "", "0", 0, "200", 200, "ok", "OK", "success"):
        return f"code={code} {d.get('message') or d.get('msg') or ''}".strip()
    if d.get("success") is False:
        return f"success=false {d.get('message') or d.get('msg') or ''}".strip()
    if d.get("result") not in (None, "ok"):
        return f"result={d.get('result')} {d.get('message') or d.get('msg') or ''}".strip()
    return None      # 没有明确失败信号 → 当成功


def pick_list(d):
    """取列表数据：优先顶层 `list`，没有就递归找（返回结构没完全公开，防御一下）。"""
    v = d.get("list")
    if v is None:
        v = dig(d, "list")
    return v


# ---------------------------------------------------------------------------
# API 客户端
# ---------------------------------------------------------------------------
class HiFleet:
    """极薄的 REST 封装。key 同时放 query 与 header —— 官方两种都写了，双保险。"""

    def __init__(self, key: str, base: str = API_BASE, sleep: float = 0.25,
                 verbose: bool = True, max_errors: int = 5):
        self.key = key
        self.base = base
        self.sleep = sleep
        self.verbose = verbose
        self.max_errors = max_errors
        self.calls = 0
        self.errors: list[tuple[str, str]] = []      # 请求层失败（HTTP/result!=ok/网络）

    # -- 内部 ---------------------------------------------------------------
    def _record_error(self, ctx: str, msg: str) -> None:
        self.errors.append((ctx, msg))
        print(f"      [ERR] {ctx}: {msg}")
        if len(self.errors) >= self.max_errors:
            raise SystemExit(
                f"[FATAL] 请求层失败已达 {len(self.errors)} 次（--max-errors "
                f"{self.max_errors}）→ 立刻中止，避免继续消耗积分。\n"
                f"        常见原因：key 无效/过期、余额为 0、接口路径变更、网络不通。\n"
                f"        查余额：--check"
            )

    def _get(self, path: str, params: dict) -> dict:
        p = dict(params)
        p["api_key"] = self.key
        try:
            r = requests.get(self.base + path, params=p,
                             headers={"x-api-key": self.key, "Accept": "application/json"},
                             timeout=HTTP_TIMEOUT)
        except Exception as exc:                       # 网络层
            self._record_error(f"GET {path}", f"网络异常 {type(exc).__name__}: {exc}")
            return {}
        finally:
            self.calls += 1
            if self.sleep:
                time.sleep(self.sleep)

        if r.status_code != 200:
            self._record_error(f"GET {path}", f"HTTP {r.status_code} {r.text[:160]}")
            return {}
        try:
            return r.json()
        except Exception:
            self._record_error(f"GET {path}", f"响应不是 JSON：{r.text[:160]}")
            return {}

    # -- 业务 ---------------------------------------------------------------
    def points(self):
        """当前可用积分；拿不到就返回 None（不阻断主流程，只是少一行对账）。"""
        try:
            r = requests.get(self.base + ACCOUNT_SUMMARY,
                             params={"api_key": self.key},
                             headers={"x-api-key": self.key, "Accept": "application/json"},
                             timeout=HTTP_TIMEOUT)
            if r.status_code != 200:
                return None
            return to_float(dig(r.json(), "availablePoints"))
        except Exception:
            return None

    def search(self, shipname: str) -> list | None:
        """按船名/MMSI 搜索。返回 list 或 None（请求失败）。"""
        d = self._get(SHIP_SEARCH, {"shipname": shipname, "i18n": "zh", "count": "50"})
        if not d:
            return None
        err = envelope_error(d)
        if err:
            self._record_error(f"shipSearch({shipname})", err)
            return None
        lst = pick_list(d)
        return lst if isinstance(lst, list) else []

    def position(self, mmsi: str) -> dict | None:
        """按 MMSI 取最新船位。返回 dict / None（该船无数据）/ 请求层失败记进 errors。"""
        d = self._get(POSITION_GET, {"mmsi": mmsi})
        if not d:
            return None
        err = envelope_error(d)
        if err:
            self._record_error(f"position({mmsi})", err)
            return None
        item = pick_list(d)
        if isinstance(item, list):                 # 文档说是对象，防御性兼容数组
            item = item[0] if item else None
        return item if isinstance(item, dict) and item else None


# ---------------------------------------------------------------------------
# 名录
# ---------------------------------------------------------------------------
def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
    conn.commit()


def load_roster(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT name_ais, name_cn, owner, mmsi, dwt, flag FROM vlcc_vessels")
        rows = cur.fetchall()
    if not rows:
        raise SystemExit("[FATAL] vlcc_vessels 是空的 —— 先跑 fetch_vlcc_fleet.py")
    return {r[0]: {"name_cn": r[1], "owner": r[2], "mmsi": r[3],
                   "dwt": r[4], "flag": r[5]} for r in rows}


def _dwt_of(c: dict):
    """候选的载重吨；'-' / 0 / 缺值 都算「不知道」，不能当 0 参与比较。"""
    v = to_float(c.get("dwt"))
    return v if v and v > 0 else None


def match_ship(roster_name: str, candidates: list,
               min_dwt: int = VLCC_MIN_DWT,
               want_dwt: float | None = None) -> tuple[dict | None, str, list]:
    """按船名从搜索结果里定出**唯一**那艘船。返回 `(hit, why, dups)`。

    - `hit`  唯一确定 → 该候选；不确定 → `None`
    - `why`  不确定的原因（直接打印给人看）
    - `dups` 同一 IMO 的多个 MMSI（同一艘船的重复上报）→ 交调用方按「最新船位」再消歧。
            本函数**不发网络请求**，保持可离线自检。

    四级判定 —— 全部由 2026-10-01 的真实返回反推出来，
    「只认唯一命中」的老规则把这 50 艘里的 44 艘白白扔掉了：

      ① **吨位闸门**：挡掉吨位明显不是 VLCC 的同名船（见 `VLCC_MIN_DWT` 注释）。小到渔船
         （1768 吨）、游艇（3713 吨），大到马绍尔籍 15.7 万吨，混在同名里是常态。
         ⚠ 只有「过滤后还有剩」才采用过滤结果；全都没吨位时退回并写明 ——
         **不能因为查不到吨位就把真船丢掉**。
      ② **与名录已知吨位对齐**（`want_dwt`，容差 ±1%）：**最有决定性的一刀**。
         名录与 HiFleet 是两个独立来源，吨位对得上才算同一条船。
      ③ **按 IMO 归并**：同一艘船常有多个 MMSI（换旗 / 重复上报 / 幽灵记录）：
         `NEW WISDOM` 的 477095600 与 477095664 **IMO 同为 9486506**，这是**一艘船**。
         ⚠ **没有 IMO 的记录不单独算一艘船** —— 实测正确的那条总是带 IMO 且在发报文，
         而同名里另一条无 IMO 的往往是早已停更的幽灵（NEW DRAGON 的 477127364 停在 2025-10）。
      ④ **剩多个不同 IMO** → 真是两艘同名 VLCC → **不猜**，返回 None 并列出 IMO 与 MMSI。

    模糊命中一律不猜；「搜索无结果」的真实原因通常是**该船已转手改名**
    （不是 HiFleet 索引缺失 —— 2026-10-01 逐条核过船史源）。中文名搜索不可用，
    它是模糊匹配，搜「远明湖」会返回 328 吨的 `YUAN MAN JIN`。
    """
    hits = [c for c in candidates if norm_name(c.get("name")) == norm_name(roster_name)]
    if not hits:                                   # 兜底：忽略空格/符号
        hits = [c for c in candidates if sk(c.get("name")) == sk(roster_name)]
    if not hits:
        if not candidates:
            # 2026-10-01 查实：这不是「HiFleet 索引缺失」。逐条核过独立船史源（Miramar /
            # ShipSpotting / Splash247），**搜不到的三艘全部是已转手改名**：
            #   COSBRIGHT LAKE(9263227) → Lake → Lake 1 → Lake 2 → **SUN I**
            #   COSGLORY  LAKE(9245782) → LILA HAIKOU → TOYOMI → **FIRENZE K**
            #   COSGRAND  LAKE(9294575) → **LILA JAMNAGAR**（2025-11 售出，有新闻）
            # 原文只说「不在索引里」会把人引向错误的排查方向，所以把真实原因写出来。
            return None, ("搜索无结果（该船名已检索不到 —— 大概率已转手改名；"
                          "中远名录是 2021-06 快照）"), []
        names = [c.get("name") for c in candidates[:4]]
        return None, f"无同名单船（返回 {len(candidates)} 条，如 {names}）", []

    raw_n = len(hits)
    notes: list[str] = []

    # ① 吨位闸门：**写明吨位却明显不是 VLCC 的候选直接剔除**（不是「用来比较」，是「剔除」）
    #   ⚠⚠ 必须对「唯一命中」也生效。2026-10-01 踩过：`COSGLORY LAKE` 只有**一条**同名记录
    #   （mmsi=413798224 / dwt=500 的残留脏记录），多命中比较逻辑根本不触发 → 它被当「唯一命中」
    #   放行，内存里记下了假 MMSI，查位阶段真的写进了一条完全错的船位。
    #   判据：**dwt 缺失=不知道，放行；dwt 写明却矛盾=不可信，剔除**。
    kept = [c for c in hits if _dwt_of(c) is None or _dwt_of(c) >= min_dwt]
    if len(kept) < len(hits):
        notes.append(f"剔除 {len(hits) - len(kept)} 条吨位明显不是 VLCC 的同名记录"
                     f"（mmsi={[c.get('mmsi') for c in hits if c not in kept]}）")
    if not kept:
        return None, "；".join(notes + ["同名记录吨位均明显不是 VLCC"
                                        "（疑同名小船或已转手船的残留记录）"]), []
    hits = kept
    if len(hits) > 1 and not any(_dwt_of(c) for c in hits):
        notes.append(f"同名 {len(hits)} 条均未给出可用吨位，无法按吨位筛")

    # ② 与名录已知吨位对齐（1% 容差）
    # ⚠ 名录的 dwt 是 psycopg2 给的 Decimal，float 减 Decimal 会 TypeError → 先归一成 float
    # ⚠ 名录值本身也可能是脏的（曾把 HiFleet 的 dwt=500 学进名录）→ **低于 VLCC 门槛
    #   的名录值一律不采信**，否则对齐会反过来去凑那个假值。
    want = to_float(want_dwt) if want_dwt is not None else None
    if want is not None and want < min_dwt:
        notes.append(f"名录 dwt={want:,.0f} 低于 VLCC 门槛，不采信（疑似历史脏值）")
        want = None
    if len(hits) > 1 and want:
        near = [c for c in hits
                if _dwt_of(c) and abs(_dwt_of(c) - want) / want <= DWT_ALIGN_TOL]
        if near and len(near) < len(hits):
            notes.append(f"与名录 {want:,.0f}DWT 差 >{DWT_ALIGN_TOL:.0%} 的 "
                         f"{len(hits) - len(near)} 条被排除")
            hits = near

    if len(hits) == 1:
        return hits[0], "；".join(notes), []

    # ③ 按 IMO 归并；**无 IMO 的不单独成组**
    with_imo: dict = {}
    without: list = []
    for c in hits:
        imo = (c.get("imonumber") or "").strip()
        if imo and imo not in ("0", "NULL", "-"):
            with_imo.setdefault(imo, []).append(c)
        else:
            without.append(c)

    if len(with_imo) == 1:                         # 唯一 IMO → 就是这艘船
        members = next(iter(with_imo.values()))
        if without:
            notes.append(f"另 {len(without)} 条同名记录无 IMO"
                         f"（{[c.get('mmsi') for c in without]}），按重复上报忽略")
        if len(members) == 1:
            return members[0], "；".join(notes), []
        return None, "；".join(notes), members     # 同一 IMO 多个 MMSI → 交新鲜度

    if not with_imo:                               # 全无 IMO：吨位已对齐过，交新鲜度
        notes.append("同名记录均无 IMO，按最新报文取")
        return None, "；".join(notes), hits

    detail = "、".join(f"IMO{k}:[{','.join(str(c.get('mmsi')) for c in v)}]"
                       for k, v in list(with_imo.items())[:3])
    notes.append(f"同名不同船 {len(with_imo)} 艘（{detail}）")
    return None, "；".join(notes), []


def pick_freshest(api, cands: list) -> tuple[dict | None, str]:
    """同一艘船的多个 MMSI 里，挑「最后报文最新」的那个。

    为什么不直接取第一个：重复 MMSI 中常有一个是历史/幽灵记录 —— 它查得到，
    但早就没报文了。按 `ti` 取最新 = 取真正在跑的那条。
    实测查位**几乎不扣积分**（2026-10-01：3 次查位余额 998→998），多发几次可接受。
    """
    best, best_ti, dead = None, None, []
    for c in cands:
        mmsi = (c.get("mmsi") or "").strip()
        if not MMSI_RE.match(mmsi):
            continue
        it = api.position(mmsi)
        ti = (it or {}).get("ti")
        if not ti or str(ti).strip() in ("-", "NULL", "None"):
            dead.append(mmsi)
            continue
        ti = str(ti).strip()          # 'YYYY-MM-DD HH:MM:SS' 字典序即时间序
        if best_ti is None or ti > best_ti:
            best, best_ti = c, ti
    if best is None:
        return None, (f"同一 IMO 的 {len(cands)} 个 MMSI 均无有效船位"
                      f"（{','.join(str(c.get('mmsi')) for c in cands)}）")
    msg = (f"同一 IMO 多个 MMSI，按最新报文取 {best.get('mmsi')}（{best_ti}）"
           + (f"，无数据：{dead}" if dead else ""))
    return best, msg


def learn_from_search(conn, name_ais: str, c: dict) -> bool:
    """把 shipSearch 得到的 MMSI/IMO/DWT/船旗回填名录。

    ⚠ 全部用 COALESCE：**不覆盖任何已有值**。已知的靠谱值优先，来源不明的靠后。
    ⚠ **吨位与 VLCC 门槛矛盾的候选整条拒收**，见下方注释 —— 这是踩过的坑。
    """
    mmsi = (c.get("mmsi") or "").strip()
    if not MMSI_RE.match(mmsi):
        return False
    imo = (c.get("imonumber") or "").strip()
    if imo in ("", "0", "NULL"):
        imo = None
    dwt = to_float(c.get("dwt"))
    if dwt is not None and dwt <= 0:
        dwt = None

    # ⚠ 2026-10-01 踩过：`COSGLORY LAKE` 在 HiFleet 里挂着一条 mmsi=413798224 / dwt=500
    # 的**脏记录**。旧逻辑「唯一同名命中即收」把它当成了船，会在图上给出一个完全错的船位，
    # 而且这个假 dwt 还会反过来污染 match_ship 的「吨位对齐」。经独立源核对，该船
    # （IMO 9245782）已于 2023 年转手改名 Firenze K，早就不在中国船东船队里了。
    # ⇒ 规则：**写明吨位却明显不是 VLCC** = 这条记录不可信 → 整条拒收，并打出来给人看。
    #   （dwt 缺失仍放行：那是「不知道」，不是「矛盾」。）
    if dwt is not None and dwt < VLCC_MIN_DWT:
        print(f"      [!] {name_ais}: 候选 dwt={dwt:,.0f} 远低于 VLCC 门槛 "
              f"{VLCC_MIN_DWT:,} → 整条拒收（疑似同名小船或已转手船的残留记录，"
              f"mmsi={mmsi} imo={imo}）")
        return False

    flag = (c.get("an") or "").strip().upper() or None
    if flag and len(flag) > 12:
        flag = flag[:12]

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE vlcc_vessels
               SET mmsi       = COALESCE(vlcc_vessels.mmsi, %s),
                   imo        = COALESCE(vlcc_vessels.imo,  %s),
                   dwt        = COALESCE(vlcc_vessels.dwt,  %s),
                   flag       = COALESCE(vlcc_vessels.flag, %s),
                   verified   = TRUE,
                   updated_at = NOW()
             WHERE name_ais = %s
        """, (mmsi, imo, dwt, flag, name_ais))
        return cur.rowcount > 0


# ---------------------------------------------------------------------------
# 解析 & 落库
# ---------------------------------------------------------------------------
def to_position_row(name_ais: str, mmsi: str, item: dict):
    """HiFleet position list → vlcc_positions 行。返回 (row, 原因)。row 为 None 表示跳过。"""
    lat = minutes_to_deg(item.get("la"))       # ⚠ 分 → 度
    lon = minutes_to_deg(item.get("lo"))
    if lat is None or lon is None:
        return None, "无经纬度"
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None, f"经纬度越界 lat={lat} lon={lon}"
    if abs(lat) < 0.005 and abs(lon) < 0.005:
        return None, "未定位占位 (0,0)"

    ts = parse_hifleet_time(item.get("ti"))
    if ts is None:
        return None, f"无有效更新时间 ti={item.get('ti')!r}"
    if ts > datetime.now(timezone.utc) + timedelta(hours=2):
        return None, f"更新时间在未来（疑似时区串味）ti={item.get('ti')!r}"

    return {
        "name_ais": name_ais,
        "mmsi": (str(item.get("m") or "").strip() or mmsi or None),
        "ts": ts,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "sog": clean_sog(item.get("sp")),
        "cog": clean_angle(item.get("co")),
        "heading": clean_angle(item.get("h")),
        "nav_status": ((item.get("status") or "").strip() or None),
        "dest": ((item.get("destination") or "").strip() or None),
        "draught": clean_draught(item.get("draught")),
    }, ""


def write_positions(conn, rows: list) -> tuple[int, int]:
    """(name_ais, ts) 批内去重后 upsert。返回 (写入行数, 去重丢弃数)。"""
    merged = {}
    for r in rows:
        if r["ts"] is None:
            continue
        merged[(r["name_ais"], r["ts"])] = r
    uniq = list(merged.values())

    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
        if uniq:
            entries = [(r["name_ais"], r["mmsi"], r["ts"], r["lat"], r["lon"],
                        r["sog"], r["cog"], r["heading"], r["nav_status"],
                        r["dest"], r["draught"], SOURCE) for r in uniq]
            execute_values(cur, UPSERT_POS_SQL, entries)
    conn.commit()
    return len(uniq), len(rows) - len(uniq)


def show_status(conn, stale_days: int = 0) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT v.owner,
                   COUNT(*)             AS ships,
                   COUNT(v.mmsi)        AS with_mmsi,
                   COUNT(v.dwt)         AS with_dwt,
                   COUNT(v.flag)        AS with_flag,
                   COUNT(p.name_ais)    AS with_pos,
                   MAX(p.ts)            AS latest
              FROM vlcc_vessels v
              LEFT JOIN (SELECT name_ais, MAX(ts) AS ts FROM vlcc_positions GROUP BY 1) p
                     ON p.name_ais = v.name_ais
             GROUP BY v.owner ORDER BY 2 DESC
        """)
        rows = cur.fetchall()

    print(f"  {'船东':<10s}{'艘数':>5s}{'有MMSI':>8s}{'有DWT':>7s}{'有船旗':>7s}"
          f"{'有船位':>8s}  最新船位")
    for o, n, m, d, f, p, latest in rows:
        print(f"  {o:<10s}{n:>5d}{m:>8d}{d:>7d}{f:>7d}{p:>8d}  {latest or '—'}")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MAX(ts) FROM vlcc_positions")
        total, latest = cur.fetchone()
        cur.execute("""SELECT source, COUNT(*) FROM vlcc_positions
                       GROUP BY 1 ORDER BY 2 DESC""")
        by_src = cur.fetchall()
    print(f"\n  vlcc_positions 共 {total} 条，最新 {latest or '—'}")
    if by_src:
        print("  来源分布：" + "、".join(f"{s}={c}" for s, c in by_src))

    # 已退役的单独列出来 —— 它们**不该**出现在「无船位」清单里被当成待办
    if RETIRED:
        with conn.cursor() as cur:
            cur.execute("""SELECT v.name_ais, v.mmsi, v.imo, p.ts
                             FROM vlcc_vessels v
                             LEFT JOIN (SELECT name_ais, MAX(ts) AS ts
                                          FROM vlcc_positions GROUP BY 1) p
                                    ON p.name_ais = v.name_ais
                            WHERE v.name_ais = ANY(%s)
                            ORDER BY v.name_ais""", (list(RETIRED),))
            ret_rows = cur.fetchall()
        live = [r for r in ret_rows if r[1] or r[2] or r[3]]
        print(f"\n  已退役 {len(RETIRED)} 艘（已转手改名，不搜不查位）：")
        for nm, mmsi, imo, ts in ret_rows:
            now_to, when, ref = RETIRED[nm][1], RETIRED[nm][2], RETIRED[nm][3]
            print(f"    {nm:<18s} → {now_to:<34s} {when}  [{ref}]")
        if live:
            print(f"    ⚠ 其中 {len(live)} 艘库里还残留身份/船位（应跑 `--fix-suspect` 或手工清）："
                  + "、".join(f"{r[0]}(mmsi={r[1] or '—'}, ts={r[3] or '—'})" for r in live))

    if stale_days > 0:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT v.name_ais, v.name_cn, v.owner, v.mmsi, p.ts
                  FROM vlcc_vessels v
                  LEFT JOIN (SELECT name_ais, MAX(ts) AS ts FROM vlcc_positions GROUP BY 1) p
                         ON p.name_ais = v.name_ais
                 WHERE p.ts IS NULL OR p.ts < NOW() - (%s || ' days')::interval
                 ORDER BY p.ts NULLS FIRST
            """, (stale_days,))
            stale = cur.fetchall()
        print(f"\n  ⚠ {len(stale)} 艘 >{stale_days} 天无船位：")
        for nm, cn, ow, mmsi, ts in stale[:40]:
            print(f"    {ow:<6s} {nm:<18s} {cn or '—':<8s} mmsi={mmsi or '—':<11s} {ts or '从未'}")
        if len(stale) > 40:
            print(f"    …… 其余 {len(stale) - 40} 艘略")


# ---------------------------------------------------------------------------
# 修数据：清掉「已配 MMSI 但吨位自相矛盾」的可疑错配
# ---------------------------------------------------------------------------
SUSPECT_HARD_SQL = """
    SELECT name_ais, owner, mmsi, imo, dwt
      FROM vlcc_vessels
     WHERE mmsi IS NOT NULL AND dwt IS NOT NULL AND dwt < %s
     ORDER BY owner, name_ais
"""

SUSPECT_SOFT_SQL = """
    SELECT name_ais, owner, mmsi, imo
      FROM vlcc_vessels
     WHERE mmsi IS NOT NULL AND dwt IS NULL
     ORDER BY owner, name_ais
"""


def fix_suspect(conn, dry_run: bool = False) -> int:
    """清理「可疑错配」的 MMSI/IMO/DWT/船旗，让它们下次重新去搜。

    ⚠ 为什么需要：**错配比不配更糟**。错的 MMSI 会在图上给出一个真实存在、
    但完全不属于这艘船的船位；「没有船位」至少是诚实的。真实案例 `COSGLORY LAKE`
    —— HiFleet 挂着 dwt=500 的残留记录，旧规则照收，而该船（IMO 9245782）
    2023 年就已转手改名 Firenze K，根本不在中国船东船队里了。

    分两档，**只清「硬」的**：
      * **硬**（会清空）：`dwt` 明确写了、但低于 VLCC 门槛 → 自相矛盾，不可信。
      * **软**（只列出）：有 MMSI 但 `dwt` 为空 → 只是「无从校验」，不构成矛盾证据，
        清掉反而可能丢掉一个正确的 MMSI，所以**不动**，仅提示。
    """
    with conn.cursor() as cur:
        cur.execute(SUSPECT_HARD_SQL, (VLCC_MIN_DWT,))
        hard = cur.fetchall()
        cur.execute(SUSPECT_SOFT_SQL)
        soft = cur.fetchall()

    if hard:
        print(f"[!] 硬可疑 {len(hard)} 艘（dwt 与 VLCC 门槛矛盾）：")
        for nm, ow, mmsi, imo, dwt in hard:
            print(f"    {ow:<6s} {nm:<18s} mmsi={mmsi} imo={imo or '—'} dwt={float(dwt):,.0f}")
        if dry_run:
            print("    （--dry-run：未改动）")
        else:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE vlcc_vessels
                       SET mmsi = NULL, imo = NULL, dwt = NULL, flag = NULL,
                           verified = FALSE, updated_at = NOW()
                     WHERE mmsi IS NOT NULL AND dwt IS NOT NULL AND dwt < %s
                """, (VLCC_MIN_DWT,))
                print(f"    已清空 {cur.rowcount} 艘 → 下次运行会重新 shipSearch")
            conn.commit()
    else:
        print("[OK] 没有硬可疑错配。")

    if soft:
        print(f"[i] 另有 {len(soft)} 艘有 MMSI 但 dwt 为空（无从校验，未改动）：")
        for nm, ow, mmsi, imo in soft[:20]:
            print(f"    {ow:<6s} {nm:<18s} mmsi={mmsi} imo={imo or '—'}")
        if len(soft) > 20:
            print(f"    …… 其余 {len(soft) - 20} 艘略")
    return len(hard)


def fix_retired(conn, dry_run: bool = False) -> int:
    """把 RETIRED 里的已转手船「退役干净」：擦掉身份 + 删掉历史船位。

    ⚠ 为什么不能只清身份：接口和前端是**按 `name_ais` 关联**名录与船位的
    （见 `src/routes/vlcc.ts`），所以只要 `vlcc_positions` 里还留着这个名字，
    即使名录的 MMSI 清了，图上照样会打一个几年前的幽灵点。
    必须两边一起处理。

    擦到什么程度：与 `--fix-suspect` 的硬档**完全一致**（mmsi/imo/dwt/flag 置空、
    verified 置假），因为那三列的来源就是「从被搜到的记录里学来的」，
    既然记录已经不可信，跟着一起作废。`built_year` / `name_en` / `source`
    是名录 PDF 自己的事实，**保留**（它们记录的是「2021-06-30 时该船在册」）。
    """
    names = list(RETIRED)
    with conn.cursor() as cur:
        cur.execute("""SELECT name_ais, mmsi, imo, dwt FROM vlcc_vessels
                        WHERE name_ais = ANY(%s) ORDER BY name_ais""", (names,))
        vrows = cur.fetchall()
        cur.execute("""SELECT name_ais, COUNT(*), MIN(ts), MAX(ts) FROM vlcc_positions
                        WHERE name_ais = ANY(%s) GROUP BY 1 ORDER BY 1""", (names,))
        prows = cur.fetchall()

    if not vrows:
        print("[OK] 名录里没有 RETIRED 列出的船名。")
        return 0

    print(f"[i] RETIRED {len(names)} 艘：")
    for nm, mmsi, imo, dwt in vrows:
        now_to, when, ref = RETIRED[nm][1], RETIRED[nm][2], RETIRED[nm][3]
        print(f"    {nm:<18s} → {now_to:<34s} {when}  [{ref}]")
        print(f"      当前 mmsi={mmsi or '—'}  imo={imo or '—'}  "
              f"dwt={f'{float(dwt):,.0f}' if dwt else '—'}")

    pos_n = sum(p[1] for p in prows)
    if prows:
        print(f"[!] 残留船位 {pos_n} 条：")
        for nm, c, lo, hi in prows:
            print(f"    {nm:<18s} {c} 条  {lo} ~ {hi}")
    else:
        print("[OK] 没有残留船位。")

    if dry_run:
        print("    （--dry-run：未改动）")
        return 0

    with conn.cursor() as cur:
        cur.execute("""UPDATE vlcc_vessels
                          SET mmsi = NULL, imo = NULL, dwt = NULL, flag = NULL,
                              verified = FALSE, updated_at = NOW()
                        WHERE name_ais = ANY(%s)""", (names,))
        n_v = cur.rowcount
        cur.execute("DELETE FROM vlcc_positions WHERE name_ais = ANY(%s)", (names,))
        n_p = cur.rowcount
    conn.commit()
    print(f"    已擦除身份 {n_v} 艘、删除船位 {n_p} 条 → 下次运行不会重新学回来（RETIRED 已在匹配前拦截）")
    return n_p


# ---------------------------------------------------------------------------
# 自检：不用 key、不花积分，用官方文档里的真实样例报文验算换算逻辑
# ---------------------------------------------------------------------------
def selftest() -> int:
    """断言核心换算与信封判定。**这是唯一能在没有 key 时验证 ÷60 与 −8h 的手段。**"""
    fails = []

    def ck(name, got, want):
        if got != want:
            fails.append(f"{name}: got={got!r} want={want!r}")
        else:
            print(f"  [OK] {name} = {got!r}")

    # ---- 官方 position 样例（ZHENRONG16，la/lo 是「分」）----------------------
    sample = {
        "m": "413829443", "n": "ZHENRONG16", "sp": "0", "co": "0",
        "ti": "2022-04-25 10:31:53", "la": "1874.115", "lo": "7088.285598",
        "h": "0", "draught": "2.3", "eta": "-", "destination": "NANTONG",
        "imonumber": "0", "callsign": "0", "type": "未知类型干货船",
        "buildyear": "NULL", "dwt": "-1", "fn": "China (Republic of)",
        "dn": "中国", "an": "CN", "l": "132", "w": "22", "rot": "0", "status": "未知",
    }
    row, why = to_position_row("ZHENRONG16", "413829443", sample)
    ck("样例解析未跳过", why, "")
    ck("纬度 1874.115 分 ÷60", round(row["lat"], 5), round(1874.115 / 60, 5))
    ck("经度 7088.285598 分 ÷60", round(row["lon"], 5), round(7088.285598 / 60, 5))
    ck("纬度落在 30~32°（不是 1874°）", 30.0 < row["lat"] < 32.0, True)
    ck("经度落在 117~119°（不是 7088°）", 117.0 < row["lon"] < 119.0, True)
    ck("ti UTC+8 10:31 → UTC 02:31", row["ts"].strftime("%Y-%m-%d %H:%M"), "2022-04-25 02:31")
    ck("ts 带 UTC tzinfo", str(row["ts"].tzinfo), "UTC")
    ck("航速 0 保留（停船，不是缺值）", row["sog"], 0.0)
    ck("吃水 2.3", row["draught"], 2.3)
    ck("目的港", row["dest"], "NANTONG")
    ck("状态", row["nav_status"], "未知")

    # ---- 占位值必须当 None，不能当 0 -----------------------------------------
    ck("'-' → None", to_float("-"), None)
    ck("'NULL' → None", to_float("NULL"), None)
    ck("'-1' 航速 → None", clean_sog("-1"), None)
    ck("'-1' 吃水 → None", clean_draught("-1"), None)
    ck("511 船首向(不可用) → None", clean_angle("511"), None)
    ck("吃水 0 → None（0 吃水不可能）", clean_draught("0"), None)

    # ---- 时间 ---------------------------------------------------------------
    ck("带 .0 后缀可解析", parse_hifleet_time("2022-04-25 10:31:53.0").strftime("%H:%M"), "02:31")
    ck("空时间 → None", parse_hifleet_time(""), None)
    ck("'NULL' 时间 → None", parse_hifleet_time("NULL"), None)

    # ---- 信封判定（两种信封都真实存在）--------------------------------------
    ck("错误帧 token_invalid 判为失败",
       bool(envelope_error({"code": "token_invalid", "success": False, "message": "AI token is invalid."})), True)
    ck("文档成功帧 result=ok 判为成功", envelope_error({"result": "ok", "num": 1, "list": {}}), None)
    ck("success:true 信封判为成功", envelope_error({"success": True, "data": {"list": []}}), None)
    ck("空响应判为失败", bool(envelope_error({})), True)

    # ---- 船名匹配：全等 → 吨位筛选 → IMO 归并（三级，都不猜）-----------------
    # 候选全部抄自 2026-10-01 shipSearch 的**真实返回**，不是编的
    cand = [{"name": "NEW WISDOM", "mmsi": "477095600"},
            {"name": "NEW DRAGON", "mmsi": "477127300"}]
    ck("全等命中", (match_ship("NEW WISDOM", cand)[0] or {}).get("mmsi"), "477095600")
    ck("忽略多空格命中", (match_ship("NEW  WISDOM", cand)[0] or {}).get("mmsi"), "477095600")
    ck("未命中返回原因", match_ship("NEW NOTEXIST", cand)[0], None)
    ck("空候选返回原因", match_ship("NEW WISDOM", [])[0], None)
    ck("空候选=已改名/不在索引(原因里说清)",
       "检索不到" in match_ship("NEW WISDOM", [])[1], True)

    # ① 同名混小船：真船 30.7 万吨，另有越南籍 2.3 万吨 / 马绍尔籍 15.7 万吨
    vis = [{"name": "NEW VISION", "mmsi": "477369300", "dwt": "307434", "imonumber": "9799202"},
           {"name": "NEW VISION", "mmsi": "574001250", "dwt": "23353",  "imonumber": "9434618"},
           {"name": "NEW VISION", "mmsi": "538007374", "dwt": "157617", "imonumber": "9804459"}]
    ck("同名混小船 → 按吨位筛出真船", (match_ship("NEW VISION", vis)[0] or {}).get("mmsi"), "477369300")
    ck("筛掉小船 ≠ 歧义（dups 为空）", match_ship("NEW VISION", vis)[2], [])

    # ② 同一艘船的多个 MMSI（IMO 相同）+ 一条 dwt=0 的幽灵记录
    wis = [{"name": "NEW WISDOM", "mmsi": "477095600", "dwt": "317960", "imonumber": "9486506"},
           {"name": "NEW WISDOM", "mmsi": "477086384", "dwt": "0",      "imonumber": None},
           {"name": "NEW WISDOM", "mmsi": "477095664", "dwt": "317960", "imonumber": "9486506"}]
    ck("同 IMO 重复 MMSI 不算歧义（hit=None）", match_ship("NEW WISDOM", wis)[0], None)
    ck("同 IMO 重复 MMSI 进 dups 待消歧", len(match_ship("NEW WISDOM", wis)[2]), 2)
    ck("dwt=0 的幽灵记录被吨位筛掉",
       "477086384" in [c.get("mmsi") for c in match_ship("NEW WISDOM", wis)[2]], False)

    # ③ 真·两艘同名 VLCC（IMO 不同、吨位都够）→ 必须拒绝
    two = [{"name": "NEW X", "mmsi": "111111111", "dwt": "300000", "imonumber": "9000001"},
           {"name": "NEW X", "mmsi": "222222222", "dwt": "310000", "imonumber": "9000002"}]
    ck("同名不同船不猜", match_ship("NEW X", two)[0], None)
    ck("同名不同船给出原因", "同名不同船" in match_ship("NEW X", two)[1], True)

    # ④ 真船没给吨位：不能因为筛不到就把船丢了（退回未过滤 → 进 dups）
    nod = [{"name": "NEW Y", "mmsi": "333333333", "dwt": "-", "imonumber": "9000003"},
           {"name": "NEW Y", "mmsi": "444444444", "dwt": "-", "imonumber": "9000003"}]
    ck("无吨位时不误丢（进 dups）", len(match_ship("NEW Y", nod)[2]), 2)
    ck("无吨位时不谎报多命中", "同名不同船" in match_ship("NEW Y", nod)[1], False)

    # ⑤ 与名录吨位对齐 —— 全部用 2026-10-01 的真实候选（冒名的偏 1.5%~12%）
    #    NEW ADEN：真船 306193 vs 名录 306474（0.09%），冒名 311080（1.50%）
    aden = [{"name": "NEW ADEN", "mmsi": "477833400", "dwt": "306193", "imonumber": "9912000"},
            {"name": "NEW ADEN", "mmsi": "477120000", "dwt": "311080", "imonumber": None}]
    ck("名录吨位对齐挑出真船", (match_ship("NEW ADEN", aden, want_dwt=306474.0)[0] or {}).get("mmsi"),
       "477833400")
    # 这一例不用吨位也能判对：冒名的那条没有 IMO，已被 ③ 当幽灵忽略（说明两道闸门各自独立有效）
    ck("无 IMO 的冒名记录不依赖吨位即被排除",
       (match_ship("NEW ADEN", aden)[0] or {}).get("mmsi"), "477833400")

    #    NEW VITALITY：真船 306752（与名录完全一致） vs 老船号 284569（偏 7.2%）
    #    这艘**两条都带各自的 IMO** → 不给名录吨位时只能拒绝；给了才能定 ← 吨位对齐的价值就在这
    vit = [{"name": "NEW VITALITY", "mmsi": "477231700", "dwt": "306752", "imonumber": "9799173"},
           {"name": "NEW VITALITY", "mmsi": "636009918", "dwt": "284569", "imonumber": "9014470"}]
    ck("两条各有 IMO → 不给吨位时拒绝", match_ship("NEW VITALITY", vit)[0], None)
    ck("拒绝原因写明同名不同船", "同名不同船" in match_ship("NEW VITALITY", vit)[1], True)
    ck("名录吨位对齐后挑出真船", (match_ship("NEW VITALITY", vit, want_dwt=306752.0)[0] or {}).get("mmsi"),
       "477231700")

    #    NEW PROSPERITY：同一 IMO 的两条都过了吨位闸 → 进 dups 交新鲜度（不猜）
    pros = [{"name": "NEW PROSPERITY", "mmsi": "477699100", "dwt": "318329", "imonumber": "9689988"},
            {"name": "NEW PROSPERITY", "mmsi": "477715484", "dwt": "318607", "imonumber": "9689988"},
            {"name": "NEW PROSPERITY", "mmsi": "636016519", "dwt": "281050", "imonumber": "9183362"}]
    ck("同 IMO 多 MMSI 走 dups", len(match_ship("NEW PROSPERITY", pros, want_dwt=318329.0)[2]), 2)
    ck("偏 12% 的老船号被排除",
       "636016519" in [c.get("mmsi") for c in match_ship("NEW PROSPERITY", pros, want_dwt=318329.0)[2]],
       False)

    # ⑥ 无 IMO 的同名记录**不单独算一艘船**（真实案例 NEW DRAGON）
    #    477127300 有 IMO 且在发报文；477127364 无 IMO，停在 2025-10
    drg = [{"name": "NEW DRAGON", "mmsi": "477127300", "dwt": "296370", "imonumber": "9379715"},
           {"name": "NEW DRAGON", "mmsi": "477127364", "dwt": "296370", "imonumber": None},
           {"name": "NEW DRAGON", "mmsi": "636022813", "dwt": "3713",   "imonumber": "9443217"}]
    ck("无 IMO 的幽灵不算另一艘船",
       (match_ship("NEW DRAGON", drg, want_dwt=296370.0)[0] or {}).get("mmsi"), "477127300")
    ck("忽略幽灵的原因要写出来", "无 IMO" in match_ship("NEW DRAGON", drg, want_dwt=296370.0)[1], True)

    # ⑦ 脏吨位护栏（真实案例 COSGLORY LAKE：HiFleet 挂着 dwt=500 的残留记录）
    ck("候选 dwt 明显不是 VLCC → 整条拒收，不写名录",
       learn_from_search(None, "COSGLORY LAKE", {"mmsi": "413798224", "dwt": "500"}), False)
    ck("名录里的脏吨位不参与对齐（不采信并说明）",
       "不采信" in match_ship("NEW ADEN", aden, want_dwt=500.0)[1], True)
    ck("脏吨位不影响真船胜出",
       (match_ship("NEW ADEN", aden, want_dwt=500.0)[0] or {}).get("mmsi"), "477833400")

    # ⑦b **单条**同名脏记录也必须被拒 —— 这就是 COSGLORY LAKE 的真实形状：
    #     全程只有 1 条同名记录，所以「多命中才比较」的逻辑根本不触发（踩过这个漏洞）
    junk1 = [{"name": "COSGLORY LAKE", "mmsi": "413798224", "dwt": "500", "imonumber": "9245782"}]
    ck("单条同名脏记录被拒（不再当唯一命中放行）", match_ship("COSGLORY LAKE", junk1)[0], None)
    ck("拒掉的原因讲清是吨位不是 VLCC",
       "吨位均明显不是 VLCC" in match_ship("COSGLORY LAKE", junk1)[1], True)
    #   对照：同一条记录若 dwt 缺失，则属「不知道」而非「矛盾」→ 仍放行
    junk2 = [{"name": "COSGLORY LAKE", "mmsi": "413798224", "dwt": "-", "imonumber": "9245782"}]
    ck("dwt 缺失不算矛盾，仍放行", (match_ship("COSGLORY LAKE", junk2)[0] or {}).get("mmsi"),
       "413798224")

    # ---- 越界 / (0,0) --------------------------------------------------------
    bad = dict(sample, la="0", lo="0")
    ck("(0,0) 占位被丢", to_position_row("X", "1", bad)[0], None)
    bad = dict(sample, la="999999", lo="1")
    ck("纬度越界被丢", to_position_row("X", "1", bad)[0], None)
    bad = dict(sample, la="", lo="")
    ck("无经纬度被丢", to_position_row("X", "1", bad)[0], None)

    # ---- RETIRED 登记表 ------------------------------------------------------
    # 这张表决定「哪些船根本不搜」，写错一个字符就等于放过或误伤一艘船，
    # 所以键必须**已经是规范化形式**（否则 retired_info 永远匹配不上，静默失效）。
    for nm, meta in RETIRED.items():
        ck(f"RETIRED[{nm}] 键已规范化", nm, norm_name(nm))
        ck(f"RETIRED[{nm}] 是 4 元组", len(meta), 4)
    ck("retired_info 大小写/空白无关", retired_info("  cosgreat lake "), RETIRED["COSGREAT LAKE"])
    ck("retired_info 未登记名返回 None", retired_info("NEW VISION"), None)

    print()
    if fails:
        print(f"[FAIL] 自检 {len(fails)} 项不通过：")
        for f in fails:
            print(f"  ✗ {f}")
        return 1
    print("[PASS] 自检全部通过 —— ÷60、−8h、占位值、信封、匹配规则均符合预期")
    return 0


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="中国船东 VLCC 船位（HiFleet / api.hifleet.com）")
    ap.add_argument("--key", help="HiFleet api_key（默认读 HIFLEET_API_KEY）")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 艘（试水/控成本）")
    ap.add_argument("--sleep", type=float, default=0.25,
                    help="每次请求后的间隔秒数，默认 0.25（礼貌限速，别打爆对方）")
    ap.add_argument("--max-errors", type=int, default=5,
                    help="请求层失败累计到此数立刻中止，默认 5（key 坏时别白烧积分）")
    ap.add_argument("--search-only", action="store_true", help="只补名录 MMSI/IMO/DWT/船旗")
    ap.add_argument("--no-search", action="store_true", help="跳过补名录，直接查已有的 MMSI")
    ap.add_argument("--refresh-search", action="store_true",
                    help="对**已有 MMSI 的船**也重搜一遍（默认只搜缺 MMSI 的）")
    ap.add_argument("--dry-run", action="store_true", help="只取不写库")
    ap.add_argument("--check", action="store_true", help="只验 key+余额+一次搜索，不写库")
    ap.add_argument("--selftest", action="store_true",
                    help="不用 key、不花积分：用官方样例报文验算 ÷60 与 −8h 等换算")
    ap.add_argument("--status", action="store_true", help="只看库内覆盖率/新鲜度")
    ap.add_argument("--fix-suspect", action="store_true",
                    help="清掉「已配 MMSI 但 dwt 明显不是 VLCC」的错配（配 --dry-run 只列不改）")
    ap.add_argument("--fix-retired", action="store_true",
                    help="把 RETIRED（已转手改名）的船擦净：清身份 + 删残留船位（配 --dry-run 只列不改）")
    ap.add_argument("--stale-days", type=int, default=0, help="配合 --status：列出 >N 天无船位的船")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    if args.status:
        conn = psycopg2.connect(DATABASE_URL)
        ensure_schema(conn)
        show_status(conn, args.stale_days)
        conn.close()
        return 0

    if args.fix_suspect:
        conn = psycopg2.connect(DATABASE_URL)
        ensure_schema(conn)
        n = fix_suspect(conn, dry_run=args.dry_run)
        conn.close()
        return 0

    if args.fix_retired:
        conn = psycopg2.connect(DATABASE_URL)
        ensure_schema(conn)
        fix_retired(conn, dry_run=args.dry_run)
        conn.close()
        return 0

    key = (args.key or os.environ.get("HIFLEET_API_KEY") or "").strip()
    if not key:
        print("[FATAL] 没有 HiFleet api_key。")
        print("        注册并取 key（邮箱验证码登录，免密码）：")
        print("          https://skills.hifleet.com/openclaw/console.html#/plans")
        print("        然后二选一：")
        print("          1) 写进 .env：HIFLEET_API_KEY=sk_live_xxxx")
        print("          2) 命令行：  --key sk_live_xxxx")
        print("        首次建议先充最小积分包（¥30 = 4,285 点），跑一轮看实际扣费。")
        return 1

    conn = psycopg2.connect(DATABASE_URL)
    try:
        ensure_schema(conn)
        roster = load_roster(conn)
        names = sorted(roster)
        if args.limit:
            names = names[: args.limit]
        have_mmsi = sum(1 for n in names if roster[n]["mmsi"])

        api = HiFleet(key, sleep=args.sleep, max_errors=args.max_errors)
        print(f"基址 = {API_BASE}")
        print(f"[1/3] 名录 {len(names)} 艘（其中 {have_mmsi} 艘已有 MMSI）")

        # ---- 账号余额（起点）------------------------------------------------
        pts0 = api.points()
        ptsA = None          # 搜索阶段结束后的余额（用于把两段成本分开算）
        if pts0 is None:
            print("      积分余额：读不到（接口结构可能变了）—— 本轮跳过成本对账")
        else:
            print(f"      积分余额（起始）：{pts0:,.0f}")

        # ---- --check：只验通路 --------------------------------------------------
        if args.check:
            probe = next((n for n in names if roster[n]["mmsi"]), names[0] if names else None)
            if not probe:
                print("[FATAL] 名录为空，无法验通路")
                return 1
            print(f"[2/3] --check：验一次搜索（{probe}）"
                  + ("+ 一次查位" if roster[probe]["mmsi"] else ""))
            cand = api.search(probe)
            if cand is None:
                print("[FAIL] 搜索请求失败 —— 看上面的 [ERR]")
                return 1
            print(f"      [OK] 搜索可用，返回 {len(cand)} 条")
            if roster[probe]["mmsi"]:
                it = api.position(roster[probe]["mmsi"])
                print(f"      [OK] 查位可用，{'有数据' if it else '该船当前无数据'}")
            pts1 = api.points()
            if pts0 is not None and pts1 is not None:
                print(f"      积分余额（结束）：{pts1:,.0f}  本轮消耗 {pts0 - pts1:,.0f} 点")
            print("[OK] key 有效、接口通路正常。")
            return 1 if api.errors else 0

        # ---- 阶段一：补名录（MMSI/IMO/DWT/船旗）------------------------------
        # 已退役的（见 RETIRED）**一律不搜** —— 否则会把旧身份重新学回来，
        # 下一轮阶段二就又把几年前的旧报文写回图上。
        retired = [n for n in names if n in RETIRED]
        active  = [n for n in names if n not in RETIRED]
        todo = [n for n in active if args.refresh_search or not roster[n]["mmsi"]]
        learned = 0
        if retired:
            print(f"      已退役（跳过）：{len(retired)} 艘 —— "
                  + "、".join(f"{n}→{RETIRED[n][1].split('（')[0]}" for n in retired))
        if args.search_only or (todo and not args.no_search):
            print(f"[2/3] 补名录：{len(todo)} 艘缺 MMSI，逐艘 shipSearch")
            for i, n in enumerate(todo, 1):
                cand = api.search(n)
                if cand is None:
                    continue
                hit, why, dups = match_ship(n, cand, want_dwt=roster[n].get("dwt"))
                if hit is None and dups:
                    # 同一艘船的多个 MMSI → 按「最后报文最新」挑（查位几乎不扣分）
                    hit, why = pick_freshest(api, dups)
                if hit is None:
                    print(f"      [{i}/{len(todo)}] {n:<20s} ✗ {why}")
                    continue
                if why:
                    print(f"      [{i}/{len(todo)}] {n:<20s} · {why}")
                # ⚠ 无论是否写库，都先在**内存**里记住 MMSI。
                # 否则 --dry-run 时阶段二会判定「一艘都没 MMSI」而直接退出，
                # 正好把「--limit 5 --dry-run 先试水看扣点」这个推荐用法废掉。
                new_mmsi = (hit.get("mmsi") or "").strip()
                if MMSI_RE.match(new_mmsi):
                    roster[n]["mmsi"] = new_mmsi
                if args.dry_run:
                    print(f"      [{i}/{len(todo)}] {n:<20s} → mmsi={hit.get('mmsi')} "
                          f"imo={hit.get('imonumber')} dwt={hit.get('dwt')} "
                          f"flag={hit.get('an')}  (dry-run，不写库)")
                    learned += 1
                    continue
                if learn_from_search(conn, n, hit):
                    learned += 1
                    print(f"      [{i}/{len(todo)}] {n:<20s} → mmsi={new_mmsi} "
                          f"dwt={hit.get('dwt') or '—'} flag={hit.get('an') or '—'}")
                else:
                    print(f"      [{i}/{len(todo)}] {n:<20s} · 无新值（已有值不覆盖）")
            conn.commit()
            print(f"      补名录完成：{learned}/{len(todo)} 艘")
            if pts0 is not None:
                ptsA = api.points()
                if ptsA is not None:
                    used = pts0 - ptsA
                    per = used / max(len(todo), 1)
                    print(f"      积分：{pts0:,.0f} → {ptsA:,.0f}（**搜索阶段**耗 {used:,.0f} 点，"
                          f"约 {per:.2f} 点/艘）")
        else:
            print("[2/3] 跳过补名录")

        if args.search_only:
            print("[3/3] --search-only，不取船位")
            show_status(conn)
            return 1 if api.errors else 0

        # ---- 阶段二：取船位 --------------------------------------------------
        # 同样排除 RETIRED：库里若还残留它们的 MMSI（清空前的历史数据），
        # 也绝不拿那个 MMSI 去查位。
        targets = [(n, roster[n]["mmsi"]) for n in names
                   if roster[n]["mmsi"] and n not in RETIRED]
        no_mmsi = [n for n in names if not roster[n]["mmsi"]]
        print(f"[3/3] 取船位：{len(targets)} 艘有 MMSI"
              + (f"（{len(no_mmsi)} 艘无 MMSI，跳过）" if no_mmsi else ""))
        if not targets:
            print("[FATAL] 一艘都没有 MMSI —— 先跑 --search-only")
            return 1

        rows, skipped = [], []
        for i, (n, mmsi) in enumerate(targets, 1):
            item = api.position(mmsi)
            if item is None:
                skipped.append((n, "源侧无位置记录"))
                continue
            row, why = to_position_row(n, mmsi, item)
            if row is None:
                skipped.append((n, why))
                continue
            rows.append(row)
            if i % 20 == 0 or i == len(targets):
                print(f"      … {i}/{len(targets)}")

        print(f"      解析出 {len(rows)} 条有效船位，跳过 {len(skipped)} 艘")

        # 成本对账：把「查位」与「搜索」两段分开。
        # 用户要用来定订阅档位的是 **单次查位的真实点数**，混在一起看是没用的。
        if pts0 is not None:
            base = ptsA if ptsA is not None else pts0
            label = "查位阶段" if ptsA is not None else "本轮合计"
            ptsB = api.points()
            if ptsB is not None:
                used = base - ptsB
                per = used / max(len(targets), 1)
                print(f"      积分余额（结束）：{ptsB:,.0f}")
                print(f"      **{label}** 耗 {used:,.0f} 点 / {len(targets)} 次查位"
                      f" → 约 **{per:.2f} 点/次**")
                if ptsA is not None:
                    print(f"      （搜索段已在上方单独打印；本轮合计 {pts0 - ptsB:,.0f} 点）")

        if args.dry_run:
            print("      --dry-run，跳过写库。样例：")
            for r in rows[:10]:
                print(f"        {r['name_ais']:<20s} {r['lat']:>9.5f},{r['lon']:>10.5f}  "
                      f"{r['sog'] if r['sog'] is not None else '—':>6} kn  "
                      f"{r['nav_status'] or '—':<8s} ts={r['ts']:%Y-%m-%d %H:%M}Z")
            if skipped:
                print("      跳过明细（源侧无数据属正常，不算失败）：")
                for n, why in skipped[:15]:
                    print(f"        {n:<20s} {why}")
            return 1 if api.errors else 0

        written, dup = write_positions(conn, rows)
        print(f"  [OK] vlcc_positions 写入/更新 {written} 行"
              + (f"（批内去重丢 {dup} 条）" if dup else ""))

        if skipped:
            print(f"  [i] {len(skipped)} 艘源侧无位置（正常）：")
            for n, why in skipped[:15]:
                print(f"        {n:<20s} {why}")
            if len(skipped) > 15:
                print(f"        …… 其余 {len(skipped) - 15} 艘略")

        print()
        show_status(conn, args.stale_days)

        if api.errors:
            print(f"\n[FAIL] 有 {len(api.errors)} 次**请求层失败**（不是「无数据」）：")
            for ctx, msg in api.errors[:10]:
                print(f"        {ctx}: {msg}")
            return 1
        return 0

    except SystemExit:
        raise
    except Exception as exc:
        conn.rollback()
        print(f"[FATAL] {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
