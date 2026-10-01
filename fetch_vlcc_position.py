#!/usr/bin/env python3
"""
VLCC Position Fetcher (中国船东 VLCC 船位，aisstream.io)
=========================================================
从 aisstream.io 的 AIS 实时流里捞「中国船东 VLCC」的船位，写 **vlcc_positions**，
供「油运信息」地图页标注。配套脚本：`fetch_vlcc_fleet.py`（名录，先跑）。

Transport
---------
    wss://stream.aisstream.io/v0/stream          （WebSocket，UTF-8 JSON 帧）
    订阅报文（连接后 **3 秒内**发出，否则被断）：
        {"APIKey": "...",
         "BoundingBoxes": [[[-90,-180],[90,180]]],      # 必填；全球
         "FiltersShipMMSI": ["477123456", ...],          # 可选，**每组 ≤200 个**
         "FilterMessageTypes": ["PositionReport","ShipStaticData"]}

    Key 从环境变量 **AISSTREAM_API_KEY** 读（或 --key）。免费注册：
        https://aisstream.io/account   （登录后在 Account 页创建/轮换 key）
    ⚠ 官方明确：**不允许浏览器直连**，key 必须放服务端/环境变量。

两种取数模式（本脚本的核心权衡）
----------------------------------
1. **MMSI 模式（默认，名录里已有 MMSI 时）**
   `FiltersShipMMSI` 精确订阅我们的船 → 流量极小（99 艘远低于 200 上限），可长时间跑。
2. **全局模式（`--global`，首次或名录没有 MMSI 时）**
   只能给全球 bbox、按 `MetaData.ShipName` 在本地和名录对名字 → 拿位置**同时反推 MMSI**。
   ⚠ 全球订阅实际只有 **约 50~80 条/秒**（不是文档暗示的数千），且**单条连接约 8,000 条
     报文后静默停供** → 脚本内部按「停顿 20 秒」自动重连。仍只适合 `--learn` 短窗口。

   两种模式都会顺带收 `ShipStaticData`（含 IMO/呼号/船型/尺度/目的港），
   用来把名录的 **IMO/MMSI 补全并把 `verified` 置真** —— 名录源侧不给这两个字段。

⚠⚠ 覆盖盲区 —— 本方案的决定性限制（2026-09-30 实测，务必先读）
------------------------------------------------------------------
aisstream 是**岸基志愿接收站拼起来的区域网，不是全球网**。用窄 bbox 各采 30s：

    北海(欧)        27.5 条/s   ← 全地球订阅那 ~43 条/s 里**六成来自这里**
    珠江口/香港      1.7 条/s
    英吉利海峡        1.5 条/s      美东(纽约)   1.4 条/s
    墨西哥湾          1.3 条/s      直布罗陀     1.1 条/s
    地中海中部        0.5 条/s      中国东海     0.5 条/s
    巴西桑托斯        0.3 条/s      西非几内亚湾 0.2 条/s
    台湾海峡          0.1 条/s
    ────────────────────────────────────────────────────────
    波斯湾 / 红海曼德 / 阿拉伯海 / 马六甲 / 南中国海 / 上海长江口
                                              **全部 0 条/s**
（波斯湾+阿拉伯海 用 20°×20° 大框单独采 3 分钟复核，仍是 **0 条**。）

**后果**：中国 VLCC 的主力航线是「波斯湾装货 → 印度洋/红海 → 马六甲 → 南中国海 → 中国卸货」，
而这条航线上的关键水域**几乎全在盲区**。因此本数据源**做不到**「跟踪中国 VLCC 全球船位」，
最好情况也只是偶尔在中国沿海附近看到少数几条。

**这不是限速也不是 key 的问题，换 MMSI 精确订阅也救不了**（没有接收站就是没有数据）。
若需求是「所有中国 VLCC 的实时船位」，必须**换商业源**（MarineTraffic / VesselFinder /
Spire / HiFleet 等），详见 `DATASOURCES.md` 的「VLCC 船位」一节。

服务器静默停供（文档未载，2026-09-30 实测）
--------------------------------------------
单条连接推完约 **8,000 条报文（≈150 秒）** 后**不再推任何数据**：socket 不断开、
不报错、`recv()` 一直超时。若照文档「连上就一直读」，脚本会空等到 `--minutes` 用完，
表现为「看起来在跑、其实一行没采」—— 本项目最忌讳的静默失败。
故 `collect()` 按 **停顿 > 20 秒** 判定停供并重连（收到过数据立刻重连，无数据则指数退避）。

文档与实际不符之处（踩过，记档）
----------------------------------
* 文档示例写 `MetaData.Latitude/Longitude`，**实际是 `MetaData.latitude/longitude`（小写）**，
  大写取回 `null`。真正可直接用的大写 `Latitude/Longitude` 在 **`Message.PositionReport`** 里。
* `websocket-client` 1.9.x **完全不支持 permessage-deflate**（源码零处 deflate），
  所以 `CompressionEnabled` 恒为 `False`。实测开压缩（用 node `ws` 对照）速率约 +27%，
  但**不是**覆盖问题的主因；要开压缩得换 WS 客户端（aiohttp / websockets / node ws）。

Limits（官方文档，2026-09 实测）
---------------------------------
* 连接的订阅数：**每账号 3 条**；每来源 IP **3 条**
* 订阅必须 **3 秒内**发出；**订阅更新每连接每秒 1 次**（更新是**替换**不是合并）
* `FiltersShipMMSI`：**每组 ≤200 个**，且必须是**九位字符串**

已知死路（同一批调研，别再试）
------------------------------
MarineTraffic 详情页真实数据 XHR → **403 Cloudflare**（HTML 只是空壳）；
VesselFinder → 整体超时（IP 被封）；船讯网 `searchv4.../index.ashx` → 恒返空数组（要登录态）；
MT 旧地图端点 `getData/get_data_json_4/...` → 恒返 `{"rows":[],"areaShips":0}`（已废弃）。
详见 `fetch_vlcc_fleet.py` docstring。

口径
----
* 只写「**能被名录对上的船**」。对不上的静默丢（不算失败）—— 全球流里 99.99% 是别人的船。
* `ts` 用 AIS 报文里的时间（`MetaData.time_utc`），**不是**本地接收时间；
  转成 UTC 存 TIMESTAMPTZ。**船在远洋可能几小时才有一条报文，`ts` 旧是正常的，不是 bug。**
* 位置不插值、不推算、不用目的地港坐标兜底 —— **宁缺勿错**。
* 同一 (name_ais, ts) 唯一，重跑幂等。

用法
----
    python fetch_vlcc_position.py --check                # 只验 key + 订阅，不写库
    python fetch_vlcc_position.py --learn --minutes 10   # 全局扫 10 分钟，学 MMSI + 存位置
    python fetch_vlcc_position.py --minutes 5            # MMSI 模式，例行刷新
    python fetch_vlcc_position.py --status               # 看库内船位覆盖率/新鲜度
    python fetch_vlcc_position.py --dry-run --minutes 2  # 不写库

    # 关键：模拟「船位静默断更」—— 名录有船但某船长期无位置，必须能看出来
    python fetch_vlcc_position.py --status --stale-days 3  # 列出 >3 天没更新的船

合规
----
只使用船位这一**公开广播的船舶自动识别信息**（AIS 本身就是为公开播发设计的），
不做个人信息采集、不做高频请求、不抓军事/受限目标。key 走环境变量，不落前端。
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

try:
    import websocket  # websocket-client
except ImportError:  # pragma: no cover
    print("[FATAL] 缺少 websocket-client：pip install websocket-client", file=sys.stderr)
    raise

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
WS_URL = "wss://stream.aisstream.io/v0/stream"
GLOBAL_BBOX = [[[-90.0, -180.0], [90.0, 180.0]]]

# AIS 航行状态码 → 中文（0-15）。15 = 未定义
NAV_STATUS = {
    0: "在航（机动）", 1: "锚泊", 2: "失控", 3: "操纵受限", 4: "吃水受限",
    5: "系泊", 6: "搁浅", 7: "捕捞中", 8: "帆行中", 9: "保留(危险品A)",
    10: "保留(危险品B)", 11: "保留(危险品C)", 12: "保留", 13: "保留",
    14: "AIS-SART", 15: "未定义",
}

SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS vlcc_positions (
    id         BIGSERIAL PRIMARY KEY,
    name_ais   VARCHAR(120) NOT NULL,       -- 关联 vlcc_vessels.name_ais
    mmsi       VARCHAR(16),
    ts         TIMESTAMPTZ NOT NULL,        -- AIS 报文时间（UTC）
    lat        NUMERIC(10,6) NOT NULL,
    lon        NUMERIC(10,6) NOT NULL,
    sog        NUMERIC(8,2),                -- 对地航速 节
    cog        NUMERIC(8,2),                -- 对地航向 度
    heading    NUMERIC(8,2),                -- 船首向 度
    nav_status VARCHAR(40),
    dest       VARCHAR(120),                -- 目的港（来自 ShipStaticData）
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


def norm_name(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).upper()


def parse_ais_time(s: str):
    """MetaData.time_utc 形如 '2026-09-30 12:34:56.789 +0000 UTC'（偶有 '2026-09-30 12:34:56'）。"""
    if not s:
        return None
    s = s.strip().replace(" UTC", "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f %z", "%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# 名录
# ---------------------------------------------------------------------------
def load_roster(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT name_ais, name_en, name_cn, owner, mmsi FROM vlcc_vessels")
        rows = cur.fetchall()
    if not rows:
        raise SystemExit("[FATAL] vlcc_vessels 是空的 —— 先跑 fetch_vlcc_fleet.py")
    by_name = {r[0]: {"name_en": r[1], "name_cn": r[2], "owner": r[3], "mmsi": r[4]}
               for r in rows}
    return by_name


# ---------------------------------------------------------------------------
# 采集
# ---------------------------------------------------------------------------
def collect(api_key: str, roster: dict, minutes: float, global_mode: bool,
            verbose: bool = True) -> tuple[list, dict, list]:
    """连 AIS 流采 N 分钟。

    返回 (位置行, 静态数据字典 name_ais->static, 我方注意到的未匹配船名集合)
    """
    known = set(roster)
    mmsi_of = {r["mmsi"]: n for n, r in roster.items() if r.get("mmsi")}
    use_mmsi = (not global_mode) and bool(mmsi_of)

    sub = {
        "APIKey": api_key,
        "BoundingBoxes": GLOBAL_BBOX,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
    if use_mmsi:
        sub["FiltersShipMMSI"] = list(mmsi_of)[:200]
        print(f"  模式 = MMSI 精确订阅（{len(sub['FiltersShipMMSI'])} 个 MMSI，"
              f"远低于 200 上限）")
    else:
        print("  模式 = **全局扫描**（无 MMSI 过滤）→ 只能按船名本地匹配。"
              "流量大，只适合短窗口 --learn。")

    positions, static = [], {}
    unmatched_hits = defaultdict(int)
    msg_seen = defaultdict(int)
    hit_names = defaultdict(int)
    sess = {"connects": 0, "rotations": 0, "errors": 0}

    # ⚠ aisstream 服务端行为（2026-09-30 实测，文档未载）：
    #   单条连接推完约 8,000 条报文（≈150 秒）后**静默停供** —— socket 不断开、
    #   不报错、就是不再来数据。坐着等满 --minutes 会变成「看起来在跑、其实一行没采」，
    #   正好踩中本项目最忌讳的静默失败。所以按「停顿」判重连。
    STALL_SECONDS = 20      # 多久没有任何报文 → 判定停供
    RECV_TIMEOUT  = 5       # recv 超时，用来周期性检查停顿

    deadline = time.monotonic() + minutes * 60

    def _session() -> str:
        """连一次并读到停供 / 总超时。返回 'deadline' | 'stalled' | 'error'。"""
        try:
            ws = websocket.create_connection(
                WS_URL, timeout=20, enable_multithread=True,
                header=["User-Agent: globaldata-vlcc/1.0"])
        except Exception as exc:
            sess["errors"] += 1
            print(f"  [WARN] 连接失败：{exc}")
            return "error"

        sess["connects"] += 1
        try:
            # ⚠⚠ 订阅报文必须在连接后 **3 秒内** 发出，否则 aisstream 直接断开连接，
            # 表现成 recv() 抛 `Connection to remote host was lost.`
            # —— 2026-09-30 踩过：漏了这行 send，无论 key 对不对都是同一个报错，
            # 让人误判成「key 无效」。**先 send 再 recv，中间不要插任何耗时操作。**
            ws.send(json.dumps(sub))
            print(f"  [{sess['connects']}] 已连接，订阅报文已发出")

            # 首帧：SubscriptionConfirmation 或服务端错误帧
            try:
                first = json.loads(ws.recv())
            except Exception as exc:
                sess["errors"] += 1
                print(f"      [WARN] 订阅未收到回执：{exc}（key/网络问题，稍后重试）")
                return "error"
            if first.get("MessageType") != "SubscriptionConfirmation":
                # 被明确拒绝 = key 或报文本身有问题，重连也没用 → 直接终止
                raise SystemExit(
                    f"[FATAL] 订阅被拒：{json.dumps(first, ensure_ascii=False)[:300]}\n"
                    f"       ① key 是否有效/未过期（.env 的 AISSTREAM_API_KEY，不要带引号）\n"
                    f"       ② 账号连接数是否已满（上限 3 条）"
                )
            comp = (first.get("Message") or {}).get("CompressionEnabled")
            print(f"      [OK] 订阅已确认（permessage-deflate={comp}）")

            ws.settimeout(RECV_TIMEOUT)
            last_msg = time.monotonic()
            got = 0
            while time.monotonic() < deadline:
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    if time.monotonic() - last_msg > STALL_SECONDS:
                        sess["rotations"] += 1
                        print(f"      [i] 服务端停供 {STALL_SECONDS}s（本轮 {got:,} 条）→ 重连")
                        return "rotated"
                    continue
                except Exception as exc:
                    # 本轮已经收到过报文 → 这是服务端配额用尽后主动断开，属**正常轮换**，
                    # 不是故障。只有「一条都没收到就被断」才算 error。
                    if got:
                        sess["rotations"] += 1
                        print(f"      [i] 服务端断开（本轮已收 {got:,} 条）→ 重连")
                        return "rotated"
                    sess["errors"] += 1
                    print(f"      [WARN] 读流中断：{exc}")
                    return "error"
                if not raw:
                    continue
                last_msg = time.monotonic()
                got += 1
                try:
                    m = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                mt = m.get("MessageType")
                meta = m.get("MetaData") or {}
                name = norm_name(meta.get("ShipName"))
                msg_seen[mt] += 1

                if mt == "PositionReport":
                    pr = (m.get("Message") or {}).get("PositionReport") or {}
                    lat, lon = pr.get("Latitude"), pr.get("Longitude")
                    if lat is None or lon is None:
                        continue
                    # AIS 里「未定位」的默认值，必须丢，否则地图上出现 (0,0)
                    if abs(lat) < 0.005 and abs(lon) < 0.005:
                        continue
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        continue
                    if name in known:
                        hit_names[name] += 1
                        positions.append({
                            "name_ais": name,
                            "mmsi": str(meta.get("MMSI") or ""),
                            "ts": parse_ais_time(meta.get("time_utc")),
                            "lat": lat, "lon": lon,
                            "sog": pr.get("Sog"), "cog": pr.get("Cog"),
                            "heading": pr.get("TrueHeading"),
                            "nav_status": NAV_STATUS.get(pr.get("NavigationalStatus")),
                        })
                    elif name:
                        unmatched_hits[name] += 1

                elif mt == "ShipStaticData":
                    sd = (m.get("Message") or {}).get("ShipStaticData") or {}
                    nm = norm_name(sd.get("Name") or meta.get("ShipName"))
                    if nm in known:
                        hit_names[nm] += 1
                        dim = sd.get("Dimension") or {}
                        static[nm] = {
                            "mmsi": str(meta.get("MMSI") or ""),
                            "imo": str(sd.get("ImoNumber") or "").strip() or None,
                            "callsign": (sd.get("CallSign") or "").strip() or None,
                            "dest": (sd.get("Destination") or "").strip() or None,
                            "draught": sd.get("MaximumStaticDraught"),
                            "type": sd.get("Type"),
                            "length": (dim.get("A") or 0) + (dim.get("B") or 0) or None,
                        }
            return "deadline"
        finally:
            try:
                ws.close()
            except Exception:
                pass

    # 重连循环：停供一次就重连一次（服务端约 8k 报文/条连接）
    backoff = 1
    while time.monotonic() < deadline:
        before = msg_seen.get("PositionReport", 0) + msg_seen.get("ShipStaticData", 0)
        outcome = _session()
        if outcome == "deadline":
            break
        after = msg_seen.get("PositionReport", 0) + msg_seen.get("ShipStaticData", 0)
        # 收到过数据 → 立刻重连；一条没收到 → 退避加倍，避免猛撞限频
        backoff = 1 if after > before else min(backoff * 2, 30)
        if time.monotonic() + backoff >= deadline:
            break
        time.sleep(backoff)

    if verbose:
        print(f"  连接 {sess['connects']} 次（正常轮换 {sess['rotations']} / 出错 {sess['errors']}）")
        print(f"  收到 PositionReport {msg_seen.get('PositionReport', 0):,} 条 / "
              f"ShipStaticData {msg_seen.get('ShipStaticData', 0):,} 条")
        print(f"  命中名录：{len(hit_names)} 艘（位置 {len(positions)} 条，"
              f"静态 {len(static)} 艘）")
        if hit_names:
            top = sorted(hit_names.items(), key=lambda x: -x[1])[:8]
            print("    最活跃：" + "、".join(f"{n}({c})" for n, c in top))
        elif not use_mmsi:
            # 0 命中时最容易被误判成「船名匹配写错了」——实则多半是覆盖问题
            print("    ⚠ 0 命中。**先别怀疑船名匹配**：aisstream 的岸基网覆盖极不均匀，")
            print("      2026-09-30 实测（30s 窗口）：北海 ~27 条/s，而波斯湾 / 红海 /")
            print("      阿拉伯海 / 马六甲 / 南中国海 / 长江口 **均为 0 条/s**。")
            print("      目标船队若常年跑这些盲区，换源才是正解（见 docstring「覆盖盲区」）。")
    return positions, static, list(unmatched_hits)


# ---------------------------------------------------------------------------
# 写库
# ---------------------------------------------------------------------------
def write_positions(conn, positions: list, static: dict, learn: bool) -> tuple[int, int]:
    # (name_ais, ts) 批内去重：同一秒内多条报文只能留一条，否则 ON CONFLICT 撞车
    merged = {}
    for p in positions:
        if p["ts"] is None:            # 报文没时间 → 宁缺勿错，整条丢
            continue
        merged[(p["name_ais"], p["ts"])] = p
    rows = list(merged.values())
    dropped = len(positions) - len(rows)

    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
        if rows:
            entries = [(r["name_ais"], r["mmsi"] or None, r["ts"], r["lat"], r["lon"],
                        r["sog"], r["cog"], r["heading"], r["nav_status"],
                        None, None, "aisstream") for r in rows]
            execute_values(cur, UPSERT_POS_SQL, entries)

        learned = 0
        if learn and static:
            for nm, s in static.items():
                if not s.get("mmsi"):
                    continue
                cur.execute("""
                    UPDATE vlcc_vessels
                       SET mmsi        = COALESCE(vlcc_vessels.mmsi, %s),
                           imo         = COALESCE(vlcc_vessels.imo, %s),
                           verified    = TRUE,
                           updated_at  = NOW()
                     WHERE name_ais = %s
                       AND (vlcc_vessels.mmsi IS DISTINCT FROM %s
                            OR vlcc_vessels.imo IS DISTINCT FROM %s
                            OR NOT vlcc_vessels.verified)
                """, (s["mmsi"], s.get("imo"), nm, s["mmsi"], s.get("imo")))
                learned += cur.rowcount
    conn.commit()
    return len(rows), dropped, learned


def ensure_schema(conn) -> None:
    """表结构内联在本脚本（本仓库无 drizzle migrations 的约定）。--status 也要先建。"""
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
    conn.commit()


def show_status(conn, stale_days: int = 0) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT v.owner,
                   COUNT(*)                                   AS ships,
                   COUNT(v.mmsi)                              AS with_mmsi,
                   COUNT(p.name_ais)                          AS with_pos,
                   MAX(p.ts)                                  AS latest
              FROM vlcc_vessels v
              LEFT JOIN (SELECT name_ais, MAX(ts) AS ts FROM vlcc_positions GROUP BY 1) p
                     ON p.name_ais = v.name_ais
             GROUP BY v.owner ORDER BY 2 DESC
        """)
        rows = cur.fetchall()
    print(f"  {'船东':<10s}{'艘数':>5s}{'有MMSI':>8s}{'有船位':>8s}  最新报文")
    for o, n, m, p, latest in rows:
        print(f"  {o:<10s}{n:>5d}{m:>8d}{p:>8d}  {latest or '—'}")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), MAX(ts) FROM vlcc_positions")
        total, latest = cur.fetchone()
    print(f"\n  vlcc_positions 共 {total} 条，最新 {latest or '—'}")

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
        print(f"\n  ⚠ {len(stale)} 艘 >{stale_days} 天无船位"
              f"（远洋无岸基 AIS 覆盖是正常的；但**一直**没有就要查是不是船名对不上）：")
        for nm, cn, ow, mmsi, ts in stale[:40]:
            print(f"    {ow:<6s} {nm:<18s} {cn or '—':<7s} mmsi={mmsi or '—':<10s} {ts or '从未'}")
        if len(stale) > 40:
            print(f"    …… 其余 {len(stale) - 40} 艘略")


def main() -> int:
    ap = argparse.ArgumentParser(description="中国船东 VLCC 船位（aisstream.io）")
    ap.add_argument("--key", help="aisstream API key（默认读 AISSTREAM_API_KEY）")
    ap.add_argument("--minutes", type=float, default=5.0, help="采集时长（分钟），默认 5")
    ap.add_argument("--global", dest="global_mode", action="store_true",
                    help="强制全局扫描（无 MMSI 过滤），用于首次 --learn")
    ap.add_argument("--learn", action="store_true",
                    help="把 ShipStaticData 里的 MMSI/IMO 回写名录并置 verified")
    ap.add_argument("--dry-run", action="store_true", help="只采不写库")
    ap.add_argument("--status", action="store_true", help="只看库内覆盖率/新鲜度")
    ap.add_argument("--stale-days", type=int, default=0,
                    help="配合 --status：列出 >N 天无船位的船")
    ap.add_argument("--check", action="store_true",
                    help="只验 key + 订阅能否建立（不采集、不写库）")
    args = ap.parse_args()

    if args.status:
        conn = psycopg2.connect(DATABASE_URL)
        ensure_schema(conn)
        show_status(conn, args.stale_days)
        conn.close()
        return 0

    key = args.key or os.environ.get("AISSTREAM_API_KEY")
    if not key:
        print("[FATAL] 没有 API key。")
        print("        免费申请：https://aisstream.io/account （登录后创建 key）")
        print("        然后二选一：")
        print("          1) 写进 .env：AISSTREAM_API_KEY=xxxx")
        print("          2) 命令行：  --key xxxx")
        return 1

    conn = psycopg2.connect(DATABASE_URL)
    try:
        ensure_schema(conn)
        roster = load_roster(conn)
        print(f"[1/3] 名录 {len(roster)} 艘 VLCC"
              f"（已有 MMSI {sum(1 for r in roster.values() if r['mmsi'])} 艘）")

        if args.check:
            print("[2/3] --check：只验订阅")
            collect(key, roster, minutes=0.02, global_mode=args.global_mode)
            print("[OK] key 有效、订阅可建立。")
            return 0

        print(f"[2/3] 采集 {args.minutes} 分钟")
        positions, static, unmatched = collect(key, roster, args.minutes,
                                               args.global_mode or args.learn)
        if unmatched:
            print(f"  旁注：看到 {len(unmatched)} 个**不在名录**的船名（可能是缺的船/同音船）")
            for n in sorted(unmatched)[:10]:
                print(f"    ? {n}")

        if args.dry_run:
            print("[3/3] --dry-run，跳过写库")
            by_ship = defaultdict(int)
            for p in positions:
                by_ship[p["name_ais"]] += 1
            print(f"      将写 {len(positions)} 条位置 / {len(by_ship)} 艘船")
            for n, c in sorted(by_ship.items(), key=lambda x: -x[1])[:20]:
                print(f"      {n:<18s} {c}")
            return 0

        print("[3/3] 写库")
        n, dropped, learned = write_positions(conn, positions, static, args.learn)
        print(f"  [OK] vlcc_positions 写入/更新 {n} 行"
              + (f"（批内同秒去重丢 {dropped} 条）" if dropped else ""))
        if args.learn:
            print(f"  [OK] 名录回填 MMSI/IMO：{learned} 艘置 verified")
        print()
        show_status(conn, args.stale_days)
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
