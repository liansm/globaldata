#!/usr/bin/env python3
"""
私募净值补充源 —— 格上理财（licai.com），免登录
============================================================

给 `private_fund_nav` 增加第三个源 `source='licai'`。

Data sources
------------
* 产品清单: POST https://www.licai.com/api/v1/data/simu/product/list
            body `{offset, num}`（num 硬顶 100，>100 直接 HTTP 500）
            ⚠ 响应 `code=1004 / msg="用户未登录"`，但 **result 里有完整数据**
              —— 这是「未登录标记」不是错误码，别按失败处理
* 净值序列: POST https://www.licai.com/api/v1/data/simu/product/netvalue
            body `{"product_id": "P378370"}` → `result.items[]`，**一次全返**（147 期）
* 产品枚举兜底: https://www.licai.com/pre/generalize/pc_sitemap_1..27.xml
            （16 万条 product URL，但**其中净值可见的极少**，见「死路」）

字段映射 → `private_fund_nav`
-----------------------------
    unit_nv          → unit_nav
    accumulated_nv   → acc_nav      ← **累计净值口径**（与 em_gaoduan 一致）
    nv_growth_rate   → daily_return（源侧已是百分数，如 "-4.17" = -4.17%）
    trading_date     → nav_date
源侧另给 `rehabilitation_nv`（复权净值，与 sppw 同口径），本表无对应列，**不落库**。

口径三条（务必守住）
-------------------
1. 同一 `fund_no` **只允许一个 source** —— 命中已有净值的产品**整只跳过**，
   绝不覆盖、绝不拼接（沿用 `load_simu_nav.py` 的不变式）。
2. 本源写的是**累计净值**，与 `source='sppw'` 的**复权净值**是两套口径，
   前端标签必须跟 `navSource` 走。
3. `daily_return` 首点留 NULL（与既有两源一致），不用 0 充数。

覆盖天花板（2026-10-30 实测）
----------------------------
* 公开产品白名单 **927 只**（`product/list` 的 total），其中 `is_show_nv=1` 的 651 只
* 参数全被服务端忽略（sort / order / strategy / product_status / is_show_nv /
  type / domainFilter 一律回 total=927；`is_hotsell=true` 反而缩到 10）
  → **927 是硬天花板，和排排网的 5,565 是同一套逻辑：榜单即白名单**
* 名称归一化后能映射到中基协备案名录 **830 只（89.5%）**，
  其中 **682 只当前无净值** → 这是本源的真实增量

死路（已复核，勿重踩）
--------------------
* **sitemap 那 16 万条 product URL 基本空转**：随机抽 65 只（跨 2020/2023/2024/2026）
  `netvalue` 全部 `total=0`。详情页 HTML 对未登录是 CSR 空壳（<title> 都空），
  **只有 netvalue 接口才是真数据**，且只对白名单内产品下发。
  → 别拿 sitemap 规模当覆盖预期。
* 详情页 `/simu/product/Pxxxxxx` 对白名单内产品**确实 SSR 直出**净值表，
  但只给最近 30 期（表格形式）；要全历史必须走 `netvalue` 接口。
* `product/list` 的 `unit_nv` / `trading_date` 字段**恒为空**，列表页不披露数值。
* 频控：0.35s 间隔实测 65 连发无异常；num>100 会 500。

合规
----
格上理财页面有版权声明；私募业绩依《私募投资基金募集行为管理办法》不得公开宣传，
本源只取站点已对匿名用户公开的数值，自建库内部研究用，不再分发。

用法
----
    python fetch_private_nav_licai.py --dry-run        # 只抓不写，打印统计
    python fetch_private_nav_licai.py --limit 20       # 试点 20 只
    python fetch_private_nav_licai.py                  # 全量增量（跳过已有净值产品）
    python fetch_private_nav_licai.py --status         # 只看库内现状
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

HERE = Path(__file__).resolve().parent
ENV = HERE / ".env"

LIST_API = "https://www.licai.com/api/v1/data/simu/product/list"
NAV_API = "https://www.licai.com/api/v1/data/simu/product/netvalue"
REFERER = "https://www.licai.com/simu/"
SOURCE = "licai"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
PAGE_SIZE = 100
DEFAULT_INTERVAL = 0.35
# 源侧空序列的产品（is_show_nv=0 那批）每次都会白跑一趟；缓存 7 天不再重试。
# 只记「确实返了 total=0」，**不记失败** —— 失败必须每轮重试，别让一次网络抖动变成长期空洞。
EMPTY_CACHE = HERE / ".workbuddy" / "licai_empty_cache.json"
EMPTY_TTL_DAYS = 7


# ---------------------------------------------------------------- 基础设施

def database_url() -> str:
    for line in ENV.read_text(encoding="utf-8").splitlines():
        if line.startswith("DATABASE_URL"):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("未在 .env 中找到 DATABASE_URL")


class RateLimited(RuntimeError):
    """被源站临时封禁（HTTP 403）。

    2026-10-30 实测：连续跑两轮（约 1,300 次 POST）后开始 403，且**当时不会自愈**
    （隔 1 分钟仍在封）。处理原则：立即中止本轮，别继续打 —— 越打封得越久。
    """


def post_json(url: str, body: dict, referer: str = REFERER, tries: int = 3) -> dict:
    data = json.dumps(body).encode()
    last = None
    for i in range(tries):
        req = urllib.request.Request(url, data=data, headers={
            "User-Agent": UA,
            "Content-Type": "application/json",
            "Referer": referer,
            "Origin": "https://www.licai.com",
        })
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as exc:
            if exc.code in (403, 429):
                raise RateLimited(f"HTTP {exc.code} {url}") from exc
            last = exc
            time.sleep(1.5 * (i + 1))
        except Exception as exc:          # noqa: BLE001
            last = exc
            time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"POST {url} 失败：{last}")


# ---------------------------------------------------------------- 名称归一化

_SUFFIXES = ("私募证券投资基金", "私募投资基金", "证券投资基金", "投资基金", "私募基金", "基金")
_SHARE_TAIL = re.compile(r"(A类|B类|C类|D类|E类|A级|B级|[A-Z])$")
_PAREN_RE = re.compile(r"[（(][^）)]{0,24}[）)]")


def norm_name(s: str | None) -> str:
    """把「XX私募证券投资基金A类」和「XX A基金」压成同一个 key。

    ⚠ **份额标记必须保留**（归一成单字母后再接回去）。
    2026-10-30 踩过：早期版本把末尾 A/B 一起剥掉，结果
    「黑翼CTA三号**A**私募证券投资基金」压成「黑翼CTA三号」，
    命中了库里 **B 类**的备案号 SEM280（成立日差 900 天）—— 典型错配。
    A/B 类是不同备案产品，剥掉份额 = 主动制造错配。

    实现：交替「剥份额 → 剥基金类型后缀」两轮，最后把份额接回基名。
    只做书写归一，不做同义替换 —— 与 `canonicalCompany()` 的边界一致。
    """
    if not s:
        return ""
    s = _PAREN_RE.sub("", s.strip()).replace(" ", "")
    share = ""
    for _ in range(2):
        m = _SHARE_TAIL.search(s)
        if m:
            share = m.group(1)[0]
            s = s[: m.start()]
            continue
        for suf in _SUFFIXES:
            if s.endswith(suf):
                s = s[: -len(suf)]
                break
    return s + share


def norm_name_loose(s: str | None) -> str:
    """在 `norm_name` 基础上再抹掉份额字母 —— 只作**唯一命中**时的兜底键。

    用途：格上给「XX A」而中基协只登记了「XX」时，严格键对不上。
    这类兜底必须过唯一性闸门（`load_registry` 的 ambiguous），否则 A/B 类又会撞车。
    """
    base = norm_name(s)
    m = _SHARE_TAIL.search(base)
    return base[: m.start()] if m else base


# ---------------------------------------------------------------- 抓取

def fetch_product_list(interval: float) -> list[dict]:
    rows, offset = [], 0
    total = None
    while True:
        d = post_json(LIST_API, {"offset": offset, "num": PAGE_SIZE})
        result = d.get("result") or {}
        total = result.get("total") or 0
        items = result.get("items") or []
        rows.extend(items)
        if not items or len(rows) >= total:
            break
        offset += PAGE_SIZE
        time.sleep(interval)
    dedup = {r["product_id"]: r for r in rows if r.get("product_id")}
    print(f"  [清单] total={total} 实得 {len(rows)} 去重后 {len(dedup)}")
    return list(dedup.values())


def _f(v):
    if v is None:
        return None
    s = str(v).strip().replace("%", "")
    if not s or s in ("-", "--"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fetch_nav(product_id: str) -> tuple[list[dict], dict]:
    d = post_json(NAV_API, {"product_id": product_id},
                  referer=f"https://www.licai.com/simu/product/{product_id}")
    result = d.get("result") or {}
    info = result.get("product_info") or {}
    out = []
    for it in result.get("items") or []:
        td = (it.get("trading_date") or "").strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", td):
            continue
        unit = _f(it.get("unit_nv"))
        acc = _f(it.get("accumulated_nv"))
        if unit is None and acc is None:
            continue
        out.append({
            "nav_date": td,
            "unit_nav": unit,
            "acc_nav": acc,
            "daily_return": _f(it.get("nv_growth_rate")),
        })
    out.sort(key=lambda r: r["nav_date"])
    # 首点 daily_return 无前值可比 → 置空（与既有两源一致）
    if out:
        out[0]["daily_return"] = None
    return out, info


# ---------------------------------------------------------------- 写库

SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS private_fund_nav (
    id BIGSERIAL PRIMARY KEY,
    fund_no VARCHAR(32) NOT NULL,
    nav_date DATE NOT NULL,
    unit_nav NUMERIC(14,4),
    acc_nav NUMERIC(14,4),
    daily_return NUMERIC(10,4),
    source VARCHAR(32) NOT NULL,
    UNIQUE (fund_no, nav_date)
);
"""

SQL_UPSERT = """
INSERT INTO private_fund_nav (fund_no, nav_date, unit_nav, acc_nav, daily_return, source)
VALUES %s
ON CONFLICT (fund_no, nav_date) DO UPDATE SET
    unit_nav     = EXCLUDED.unit_nav,
    acc_nav      = EXCLUDED.acc_nav,
    daily_return= EXCLUDED.daily_return
WHERE private_fund_nav.source = EXCLUDED.source
"""


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SQL_ENSURE)
        cur.execute("ALTER TABLE private_fund_nav ADD COLUMN IF NOT EXISTS source VARCHAR(32)")
        cur.execute("UPDATE private_fund_nav SET source = 'em_gaoduan' WHERE source IS NULL")
        cur.execute("ALTER TABLE private_fund_nav ALTER COLUMN source SET NOT NULL")
    conn.commit()


def load_registry(conn) -> tuple[dict[str, str], set[str], dict[str, str], set[str]]:
    """返回 (严格索引, 严格多义, 兜底索引, 兜底多义)。

    ⚠ 一名对多码的 key **一律不参与匹配**：中基协里大量 `SV586A` / `SSV586`、
    `S23561` / `S21508` 这种新旧编码并存，靠名字无法分辨谁是谁。
    错配比不配更糟（项目铁律），宁可少抓。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT fund_no, fund_name FROM private_funds")
        rows = cur.fetchall()

    def build(fn):
        groups: dict[str, list[str]] = defaultdict(list)
        for fund_no, name in rows:
            key = fn(name)
            if key:
                groups[key].append(fund_no)
        idx = {k: v[0] for k, v in groups.items() if len(v) == 1}
        ambiguous = {k for k, v in groups.items() if len(v) > 1}
        return idx, ambiguous

    strict_idx, strict_amb = build(norm_name)
    loose_idx, loose_amb = build(norm_name_loose)
    return strict_idx, strict_amb, loose_idx, loose_amb


def resolve_fund_no(product: dict, reg) -> str | None:
    """严格键优先；严格键歧义即放弃；再退兜底键（同样过唯一性）。"""
    strict_idx, strict_amb, loose_idx, loose_amb = reg
    full = product.get("product_full_name")
    abbr = product.get("product_abbr_name")
    keys = [k for k in (norm_name(full), norm_name(abbr)) if k]
    if any(k in strict_amb for k in keys):
        return None                     # 歧义：宁可少配
    for k in keys:
        if k in strict_idx:
            return strict_idx[k]
    loose_keys = [k for k in (norm_name_loose(full), norm_name_loose(abbr)) if k]
    for k in loose_keys:
        if k not in loose_amb and k in loose_idx:
            return loose_idx[k]
    return None


def load_nav_owners(conn) -> dict[str, str]:
    """fund_no → source（已有净值的产品）。"""
    with conn.cursor() as cur:
        cur.execute("SELECT fund_no, MIN(source) FROM private_fund_nav GROUP BY fund_no")
        return {no: src for no, src in cur.fetchall()}


def load_empty_cache() -> dict[str, str]:
    if not EMPTY_CACHE.exists():
        return {}
    try:
        return json.loads(EMPTY_CACHE.read_text(encoding="utf-8"))
    except Exception:                    # noqa: BLE001
        return {}


def save_empty_cache(cache: dict[str, str]) -> None:
    EMPTY_CACHE.parent.mkdir(parents=True, exist_ok=True)
    cutoff = (date.today() - timedelta(days=EMPTY_TTL_DAYS)).isoformat()
    cache = {k: v for k, v in cache.items() if v >= cutoff}
    EMPTY_CACHE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")


def refresh_private_funds_flags(conn, touched: list[str]) -> None:
    """把本次写到的产品回填到 private_funds 的快照列（幂等）。"""
    if not touched:
        return
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE private_funds pf SET
                has_nav            = TRUE,
                latest_nav         = s.unit_nav,
                latest_acc_nav     = s.acc_nav,
                latest_nav_date    = s.nav_date,
                latest_daily_return= s.daily_return,
                nav_updated_at     = NOW()
            FROM (
                SELECT DISTINCT ON (fund_no) fund_no, unit_nav, acc_nav, nav_date, daily_return
                FROM private_fund_nav
                WHERE fund_no = ANY(%s)
                ORDER BY fund_no, nav_date DESC
            ) s
            WHERE pf.fund_no = s.fund_no
        """, (touched,))
    conn.commit()


# ---------------------------------------------------------------- 主流程

def cmd_status(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""SELECT source, COUNT(DISTINCT fund_no), COUNT(*),
                              MIN(nav_date), MAX(nav_date)
                       FROM private_fund_nav GROUP BY 1 ORDER BY 1""")
        print("  source        产品数     行数        起          止")
        for src, n, rows, lo, hi in cur.fetchall():
            print(f"  {src:<12} {n:>7} {rows:>10}  {lo}  {hi}")
        cur.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE has_nav) FROM private_funds")
        total, has = cur.fetchone()
        print(f"  private_funds  {total} 只，有净值 {has} 只 = {100 * has / total:.2f}%")


def main() -> int:
    ap = argparse.ArgumentParser(description="私募净值补充源：格上理财（免登录）")
    ap.add_argument("--dry-run", action="store_true", help="只抓不写库")
    ap.add_argument("--limit", type=int, default=0, help="最多处理多少只（试点）")
    ap.add_argument("--status", action="store_true", help="只显示库内现状")
    ap.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="请求间隔秒")
    args = ap.parse_args()

    conn = psycopg2.connect(database_url())
    ensure_schema(conn)

    if args.status:
        cmd_status(conn)
        return 0

    print("── 格上理财 · 私募净值 ──────────────────────────────")
    products = fetch_product_list(args.interval)

    registry = load_registry(conn)
    owners = load_nav_owners(conn)

    mapped, unmapped = [], []
    skipped: dict[str, int] = defaultdict(int)
    amb_hit = 0
    seen_no: dict[str, str] = {}
    for p in products:
        keys = [k for k in (norm_name(p.get("product_full_name")),
                            norm_name(p.get("product_abbr_name"))) if k]
        if any(k in registry[1] for k in keys):
            amb_hit += 1
            continue
        fund_no = resolve_fund_no(p, registry)
        if not fund_no:
            unmapped.append(p)
            continue
        if fund_no in owners:
            skipped[owners[fund_no]] += 1
            continue
        if fund_no in seen_no:          # 同一备案号被多个 P* 命中（A/B 类份额）
            skipped["dup_pid"] += 1
            continue
        seen_no[fund_no] = p["product_id"]
        mapped.append((p, fund_no))

    print(f"  映射到备案名录 {len(mapped) + sum(skipped.values())}/{len(products)}"
          f"；已有净值跳过 {sum(skipped.values())} 只（{dict(skipped)}）"
          f"；名称多义跳过 {amb_hit} 只")
    print(f"  待抓 {len(mapped)} 只，未映射 {len(unmapped)} 只")

    if args.limit:
        mapped = mapped[:args.limit]

    written_prods, total_rows, empty, failed = [], 0, [], []
    empty_cache = load_empty_cache()
    cutoff = (date.today() - timedelta(days=EMPTY_TTL_DAYS)).isoformat()
    cached_empty = [p for p, _ in mapped if empty_cache.get(p["product_id"], "") >= cutoff]
    todo = [(p, no) for p, no in mapped if empty_cache.get(p["product_id"], "") < cutoff]
    if cached_empty:
        print(f"  空序列缓存命中 {len(cached_empty)} 只（{EMPTY_TTL_DAYS} 天内已知无净值，跳过）")

    t0 = time.time()
    aborted = False
    for i, (p, fund_no) in enumerate(todo, 1):
        pid = p["product_id"]
        try:
            navs, info = fetch_nav(pid)
        except RateLimited as exc:
            print(f"\n  ✗ 被源站限流（{exc}），已中止本轮。"
                  f"已完成 {len(written_prods)} 只，剩余 {len(todo) - i + 1} 只留待下次。")
            aborted = True
            break
        except Exception as exc:            # noqa: BLE001
            failed.append((pid, str(exc)[:80]))
            print(f"  [{i}/{len(todo)}] {pid} 失败：{str(exc)[:60]}", flush=True)
            time.sleep(args.interval)
            continue

        if not navs:
            empty.append(pid)
            empty_cache[pid] = date.today().isoformat()
            time.sleep(args.interval)
            continue

        rows = [(fund_no, n["nav_date"], n["unit_nav"], n["acc_nav"],
                 n["daily_return"], SOURCE) for n in navs]
        if not args.dry_run:
            with conn.cursor() as cur:
                execute_values(cur, SQL_UPSERT, rows, page_size=500)
            conn.commit()
            written_prods.append(fund_no)
        total_rows += len(rows)
        if i % 25 == 0 or i == len(todo):
            print(f"  [{i}/{len(todo)}] 已写 {len(written_prods)} 只 / {total_rows} 行"
                  f"  ({i / max(time.time() - t0, 1e-9):.1f} 只/s)", flush=True)
        time.sleep(args.interval)

    save_empty_cache(empty_cache)

    if not args.dry_run and written_prods:
        refresh_private_funds_flags(conn, written_prods)

    print(f"\n  {'中止' if aborted else '完成'}：入库 {len(written_prods)} 只 / {total_rows} 行；"
          f"空序列 {len(empty)} 只（已缓存 {EMPTY_TTL_DAYS} 天）；失败 {len(failed)} 只；"
          f"耗时 {(time.time() - t0) / 60:.1f} 分钟")
    if empty:
        print(f"  空序列样例：{empty[:8]}")
    if failed:
        print(f"  失败样例：{failed[:5]}")

    # 不变式：不得出现跨源同 fund_no
    with conn.cursor() as cur:
        cur.execute("""SELECT COUNT(*) FROM (
                         SELECT fund_no FROM private_fund_nav
                         GROUP BY fund_no HAVING COUNT(DISTINCT source) > 1) t""")
        mixed = cur.fetchone()[0]
    print(f"  混源 fund_no：{mixed}（必须为 0）")
    if mixed:
        print("  ✗ 出现跨源混拼，请立刻排查", file=sys.stderr)
        return 2

    if not args.dry_run:
        cmd_status(conn)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
