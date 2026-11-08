#!/usr/bin/env python3
"""
私募数据一键更新（备案增量 + 三条净值源 + 可选管理人全量）
============================================================

为什么要有这个脚本（2026-11-07 定案）
--------------------------------------
私募是本项目里**唯一一个「所有抓取脚本都不在日常刷新里」的域**，结果就是数据
慢慢烂掉而没人发现。2026-11-07 查库实况：

    private_fund_nav  sppw / em_gaoduan / licai 最新净值日统统停在 10-29~10-30
    private_funds     最新备案日 2026-09-16、24.3 万行 updated_at 已超 30 天
    private_managers  最后刷新 2026-09-17

当天源侧探针证明**不是源停更，是我们没去拿**：
    AMAC  pof/fund page 0~2 共 300 条，record_date 11-02~11-06，库内 0 条命中
    东财  lsjz 抽样末点 11-04 / 11-05 / 11-06（库内 10-29）
    格上  netvalue 抽样末点 10-30，白名单 927 → 933
    排排网 fundNavTrend 抽样末点 11-02 / 11-04 / 11-05（库内 10-30），会话仍 is_login=1

三个根因，逐个对应本脚本的步骤
--------------------------------
① `--nav` 的断点是「private_fund_nav 已有 fund_no 就跳过」，本意省全历史重抓，
   代价是**已有产品永远不再更新**。→ 统一走 `em`: `--nav-only --full` 重抓覆盖写。
② 格上默认也只补新进池产品。→ 新增 `--refresh`，把本源已有产品放回队列，
   但其他 source 的产品仍跳过（守住「同一 fund_no 只有一个 source」）。
③ 中基协无增量能力，全量重扫 55 分钟；但 `pof/fund` 默认顺序是 id 倒序
   = 备案时间倒序，所以「只扫前 200 页」就能补回新备案（约 4 分钟）。
   → `registry`: `fetch_private_funds.py --incremental 200`。

排排网（sppw，5,335 只 = 净值覆盖里最大一块）需要登录态
----------------------------------------------------------
`.workbuddy/.cdp_session.json` 是 CDP 从已登录浏览器抓的会话，会过期。
**匿名态 fundNavTrend 会返回「status=1 / 获取成功 / categories=[]」——
看起来成功、实际 0 点数据**，直接落库会把已有序列搞坏。所以每一步之前
先跑 `simu_session_check.cjs` 用榜单接口的 `extra.is_login` 判活；
失效就 SKIP 并打印重新抓会话的命令，**绝不把空序列写进库**。

口径 / 不变式（改这个脚本前必读）
----------------------------------
* `private_fund_nav` 三个 source 严禁混拼：em_gaoduan/licai = 累计净值，
  sppw = 复权净值（分红再投）。同一 fund_no 永远只有一个 source，收尾断言 = 0。
* 所有净值源都是「一次返全历史 + UPSERT 覆盖写」，所以刷新是重抓覆盖，不是追加；
  漏跑不丢历史，补跑一次就全回来（这也是它们不进每日整轮 refresh 的理由）。
* 备案增量**扫不到老产品的状态变更**（正在运作 → 提前清算发生在旧页里），
  所以 `--with-managers` 之外仍需**月度全量重扫**兜底：
  `python fetch_private_funds.py --passes 2`（约 100 分钟，手动低频）。
* 净值覆盖率天花板 6,975 / 248,630 = 2.81%，是公开渠道的物理上限
  （《私募投资基金募集行为管理办法》禁止公开披露业绩），**不是抓取缺口**，
  再往上只能买朝阳永续 / 私募云通 / Wind。别拿这个数字当 bug 修。

2026-11-07 首轮实跑复盘（改脚本前先看这两条）
--------------------------------------------
① **混源**：`--nav-only --full` 原来把断点清空，代销池清单里 `SAUW09` 撞上
   排排网已占用的备案号，因两源披露日不同而插入 305 行 `em_gaoduan` → 全表
   「一产品一源」不变式被破坏。修法见 `fetch_private_nav_licai.py` 同日注释：
   `--full` 改为「只跳过**其他源**占用的 fund_no，本源照常全量刷新」。
   已清理 SAUW09 的 92 行 sppw（保留信息更全的日频 em 305 行），断言恢复 0。
   混源断言也从 `step_sppw` 移到 `main` 最后：它是全表不变式，放单步里会把
   「代销池撞号」误判成「排排网落库失败」。
② **失败静默**：格上被限流中止时脚本仍 `return 0`，编排把它标成 OK。
   现在 `aborted` 返回 2，`step_licai` 打到 2 会打印源侧限制说明。
   格上 403 是源站对 netvalue 新增反爬（三出口 + 完整头均无效，见该脚本 docstring），
   **不是本机可修的**，其余源不受影响；隔段时间 `--only licai` 重跑即可。
   首轮结果：registry/em/sppw/managers 全部 OK，licai 完成 33 只后中止。

用法
----
    python refresh_private.py --status                 # 只看各源新鲜度，不跑
    python refresh_private.py                          # 全量一轮（约 35~45 分钟）
    python refresh_private.py --only registry --only em
    python refresh_private.py --skip-sppw              # 没登录态时快捷跳过排排网
    python refresh_private.py --with-managers          # 额外全量重扫管理人（约 8 分钟）
    python refresh_private.py --dry-run                # 只打印将执行的命令
    python refresh_all.py --fetch --with-private       # 连同整轮数据刷新一起跑

⚠ 长任务：`python -u refresh_private.py >> .workbuddy/private_refresh.log 2>&1`
   各步独立，某源失败不影响其他源，但会汇总成非 0 退出码，不静默。
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BASE = Path(__file__).parent
WB = BASE / ".workbuddy"
load_dotenv(BASE / ".env")
DSN = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
PY = sys.executable
NODE = shutil.which("node") or "node"

# 固定产物名：每轮先删再写，避免 'a' 追加模式把上轮数据重复落库，也避免磁盘越滚越大
NAV_MAIN = WB / "simu_nav_refresh.jsonl"
NAV_SHARE = WB / "simu_nav_shareclass_refresh.jsonl"


def db():
    return psycopg2.connect(DSN)


def one(sql):
    with db() as c, c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def show_status() -> int:
    print("── 私募数据新鲜度 ─────────────────────────────────────────────")
    print(f"{'源':<14}{'产品数':>8}{'行数':>12}{'最新净值日':>14}{'陈旧(天)':>10}{'30天内有更新':>14}")
    rows = one("""
        WITH last AS (SELECT fund_no, MAX(nav_date) AS d FROM private_fund_nav GROUP BY fund_no),
             src  AS (SELECT fund_no, MIN(source) AS source FROM private_fund_nav GROUP BY fund_no)
        SELECT s.source, COUNT(*), SUM(n.cnt), MAX(l.d)::text,
               CURRENT_DATE - MAX(l.d),
               COUNT(*) FILTER (WHERE l.d >= CURRENT_DATE - 30)
        FROM last l
        JOIN src s USING (fund_no)
        JOIN (SELECT fund_no, COUNT(*) AS cnt FROM private_fund_nav GROUP BY fund_no) n USING (fund_no)
        GROUP BY s.source ORDER BY 2 DESC
    """)
    for src, funds, lines, latest, stale, fresh in rows:
        print(f"{src:<14}{funds:>8}{lines:>12}{latest or '-':>14}{str(stale):>10}{fresh:>14}")

    r = one("""SELECT COUNT(*), COUNT(*) FILTER (WHERE in_registry),
                      MAX(record_date)::text, MAX(updated_at)::date::text,
                      CURRENT_DATE - MAX(updated_at)::date,
                      MAX(latest_nav_date)::text
               FROM private_funds""")[0]
    print(f"\n备案产品 {r[0]:,} 只（在册 {r[1]:,}）| 最新备案日 {r[2]} "
          f"| 最近刷新 {r[3]}（{r[4]} 天前）| 最新净值日 {r[5]}")
    r = one("SELECT COUNT(*), MAX(updated_at)::date::text, CURRENT_DATE - MAX(updated_at)::date "
            "FROM private_managers")[0]
    print(f"管理人   {r[0]:,} 家 | 最近刷新 {r[1]}（{r[2]} 天前）")
    print("\n提示：sppw/em_gaoduan/licai 陈旧 > 10 天就该跑一轮；"
          "备案最新日期落后 > 45 天建议跑一次全量重扫。")
    return 0


def stream(cmd: list[str], cwd: Path = BASE, tail: int = 60) -> tuple[int, list[str]]:
    """实时转发子进程输出，同时留最后 tail 行给调用方解析。"""
    print("  $ " + " ".join(shlex.quote(str(x)) for x in cmd), flush=True)
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    p = subprocess.Popen(
        [str(x) for x in cmd], cwd=str(cwd), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1,
    )
    recent: collections.deque[str] = collections.deque(maxlen=tail)
    for line in p.stdout:  # type: ignore[union-attr]
        line = line.rstrip()
        print("    " + line, flush=True)
        recent.append(line)
    return p.wait(), list(recent)


def jsonl_stats(path: Path) -> tuple[int, int]:
    """(行数, distinct fund_no)。产物可能上百 MB，流式读。"""
    if not path.exists():
        return 0, 0
    n, codes = 0, set()
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.strip():
                continue
            n += 1
            try:
                c = json.loads(line).get("fund_no")
                if c:
                    codes.add(c)
            except ValueError:
                pass
    return n, len(codes)


def check_no_mixed_source() -> int:
    mixed = one("""SELECT COUNT(*) FROM (
                     SELECT fund_no FROM private_fund_nav
                     GROUP BY fund_no HAVING COUNT(DISTINCT source) > 1) t""")[0][0]
    print(f"  [不变式] 跨源混拼 fund_no = {mixed}（必须为 0）", flush=True)
    return mixed


def step_registry(pages: int, dry: bool) -> str:
    print("\n══ ① 备案增量（中基协最新备案 + 状态）══", flush=True)
    before = one("SELECT COUNT(*), MAX(record_date)::text FROM private_funds")[0]
    cmd = [PY, "-u", "fetch_private_funds.py", "--incremental", str(pages)]
    if dry:
        print("  $ " + " ".join(map(str, cmd)))
        return "skip"
    rc, _ = stream(cmd)
    after = one("SELECT COUNT(*), MAX(record_date)::text FROM private_funds")[0]
    print(f"  [结果] 备案产品 {before[0]:,} → {after[0]:,}（新增 {after[0] - before[0]:,}）；"
          f"最新备案日 {before[1]} → {after[1]}", flush=True)
    return "ok" if rc == 0 else "fail"


def step_em(dry: bool) -> str:
    print("\n══ ② 天天基金代销池净值（全量刷新）══", flush=True)
    cmd = [PY, "-u", "fetch_private_funds.py", "--nav-only", "--full"]
    if dry:
        print("  $ " + " ".join(map(str, cmd)))
        return "skip"
    rc, _ = stream(cmd)
    return "ok" if rc == 0 else "fail"


def step_licai(dry: bool) -> str:
    print("\n══ ③ 格上理财净值（刷新已有 + 补新进池）══", flush=True)
    # --stale-days 3：只重抓 3 天内没更新过的本源产品。格上 11-07 在第 33 只
    # 就吃到 403，中止后重跑靠这个阈值接着补，不重复打已刷新的产品。
    cmd = [PY, "-u", "fetch_private_nav_licai.py", "--refresh", "--stale-days", "3"]
    if dry:
        print("  $ " + " ".join(map(str, cmd)))
        return "skip"
    rc, _ = stream(cmd)
    if rc == 2:
        print("  [源侧限制] 格上 netvalue 接口被反爬拦截（HTTP 403，2026-11-07 实测"
              "直连/沙箱代理/clash 三出口 + 完整浏览器头均被挡，非本机频率问题）。"
              "其余三源不受影响；隔一段时间重跑 `--only licai` 即可，"
              "`--stale-days 3` 会跳过本轮已刷新的产品接着补。", flush=True)
    return "ok" if rc == 0 else "fail"


def step_sppw(dry: bool) -> str:
    print("\n══ ④ 私募排排网净值（登录态全量重抓）══", flush=True)
    if dry:
        print("  $ node simu_session_check.cjs → relogin → load → shareclass → load")
        return "skip"

    # ① 会话判活。匿名态「成功但 0 点」是最危险的假成功，必须先挡住。
    rc, tail = stream([NODE, str(WB / "simu_session_check.cjs")], cwd=WB)
    info = {}
    for line in reversed(tail):
        if line.strip().startswith("{"):
            try:
                info = json.loads(line.strip())
                break
            except ValueError:
                continue
    if rc != 0 or not info.get("ok"):
        reason = info.get("reason", "自检未通过（无 JSON 输出，看上方日志）")
        print(f"  [SKIP] 排排网会话不可用：{reason}", flush=True)
        print("  → 在已登录 simuwang.com 的浏览器上跑 "
              "`node .workbuddy/cdp_session_grab.cjs` 重新抓会话，再跑一轮。", flush=True)
        print("  → 其余三源照常刷新；sppw 的 5,335 只暂时保留上次数据。", flush=True)
        return "skip"
    print(f"  [会话有效] 抓于 {info.get('at')}，榜单 total={info.get('total')}", flush=True)

    for p in (NAV_MAIN, NAV_SHARE):
        if p.exists():
            p.unlink()          # 脚本以 'a' 打开，不清会重复落库

    # ② 母基金净值
    rc, _ = stream([NODE, "simu_nav_relogin.cjs", "--out", NAV_MAIN.name], cwd=WB)
    n, codes = jsonl_stats(NAV_MAIN)
    print(f"  [产物] {NAV_MAIN.name}: {n:,} 行 / {codes:,} 只", flush=True)
    if rc != 0 or n < 500_000 or codes < 2_500:
        print("  [FAIL] 产物明显不完整（阈值 50 万行 / 2,500 只），**不落库**，"
              "避免用半轮数据覆盖已有序列。下轮重跑即可。", flush=True)
        return "fail"
    rc, _ = stream([PY, "-u", "load_simu_nav.py", "--jsonl", str(NAV_MAIN)], cwd=WB)
    if rc != 0:
        return "fail"

    # ③ 份额分级（1,714 只，序列取自同名 A/C 类份额，见 DATASOURCES.md）
    rc, _ = stream([NODE, "simu_nav_shareclass.cjs", "--out", NAV_SHARE.name], cwd=WB)
    n2, codes2 = jsonl_stats(NAV_SHARE)
    print(f"  [产物] {NAV_SHARE.name}: {n2:,} 行 / {codes2:,} 只", flush=True)
    if rc != 0 or n2 < 100_000 or codes2 < 1_000:
        print("  [FAIL] 份额分级产物不完整，不落库（母基金部分已安全入库）。", flush=True)
        return "fail"
    rc, _ = stream([PY, "-u", "load_simu_nav_shareclass.py", "--jsonl", str(NAV_SHARE)], cwd=WB)
    if rc != 0:
        return "fail"

    # 混源断言放在 main 最后统一做：它是全表不变式，可能由**前面任一步**破坏，
    # 放在这里会把「代销池撞号」误判成「排排网落库失败」（2026-11-07 踩过）。
    return "ok"


def step_managers(dry: bool) -> str:
    print("\n══ ⑤ 管理人名录全量重扫（低频，约 8 分钟）══", flush=True)
    cmd = [PY, "-u", "fetch_private_funds.py", "--managers-only", "--passes", "2"]
    if dry:
        print("  $ " + " ".join(map(str, cmd)))
        return "skip"
    rc, _ = stream(cmd)
    return "ok" if rc == 0 else "fail"


def main() -> int:
    ap = argparse.ArgumentParser(description="私募数据一键更新（备案 + 三源净值）")
    ap.add_argument("--status", action="store_true", help="只显示各源新鲜度，不跑")
    ap.add_argument("--dry-run", action="store_true", help="只打印将执行的命令")
    ap.add_argument("--only", action="append", choices=["registry", "em", "licai", "sppw", "managers"],
                    help="只跑指定步骤（可重复）；默认跑 registry/em/licai/sppw")
    ap.add_argument("--skip-sppw", action="store_true", help="跳过排排网（没登录态时）")
    ap.add_argument("--with-managers", action="store_true", help="额外全量重扫管理人名录")
    ap.add_argument("--recent-pages", type=int, default=200,
                    help="备案增量扫多少页（默认 200 页 = 2 万条 ≈ 200 天）")
    args = ap.parse_args()

    if args.status:
        return show_status()

    keys = args.only or ["registry", "em", "licai", "sppw"]
    if args.skip_sppw and "sppw" in keys:
        keys.remove("sppw")
    if args.with_managers and "managers" not in keys:
        keys.append("managers")

    print(f"私募更新：{', '.join(keys)} | {time.strftime('%Y-%m-%d %H:%M:%S')}"
          + ("  [DRY-RUN]" if args.dry_run else ""), flush=True)

    dispatch = {
        "registry": lambda: step_registry(args.recent_pages, args.dry_run),
        "em":       lambda: step_em(args.dry_run),
        "licai":    lambda: step_licai(args.dry_run),
        "sppw":     lambda: step_sppw(args.dry_run),
        "managers": lambda: step_managers(args.dry_run),
    }
    results: list[tuple[str, str, float]] = []
    for key in keys:
        t0 = time.time()
        try:
            status = dispatch[key]()
        except Exception as exc:  # noqa: BLE001 — 单步炸了不能拖垮其他源
            print(f"  [FAIL] {key} 异常：{type(exc).__name__}: {exc}", flush=True)
            status = "fail"
        results.append((key, status, time.time() - t0))

    print("\n══ 汇总 ══", flush=True)
    for key, status, sec in results:
        print(f"  {status.upper():<5} {key:<10} {sec / 60:.1f} 分钟", flush=True)
    failed = [k for k, s, _ in results if s == "fail"]
    skipped = [k for k, s, _ in results if s == "skip"]
    if not args.dry_run:
        show_status()
        # 全表不变式单独判：所有步骤都跑完再看，归因才准。
        if check_no_mixed_source() != 0:
            failed.append("invariant:mixed_source")
            print("  [FAIL] 出现跨源混拼 fund_no，按 DATASOURCES.md 排查并清理后重跑", flush=True)
    if failed:
        print(f"✘ 失败步骤：{', '.join(failed)}（其余源已完成；失败不写进度，下轮重跑即可）",
              flush=True)
        return 1
    if args.dry_run:
        print("— 演练结束，未执行任何抓取或写库", flush=True)
        return 0
    if skipped:
        print(f"— 跳过：{', '.join(skipped)}", flush=True)
    print("✔ 私募更新完成", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
