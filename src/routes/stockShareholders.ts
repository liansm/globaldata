import { FastifyInstance } from 'fastify'
import { sql } from 'drizzle-orm'
import { db } from '../db'

// ⚠ numeric 列在 pg 驱动里返回字符串，必须转 number（本项目踩过多次）
const toNum = (v: unknown): number | null => (v == null ? null : Number(v))
const toInt = (v: unknown): number | null => (v == null ? null : Number(v))
const day = (v: unknown): string | null => (v == null ? null : String(v).slice(0, 10))

type Row = Record<string, unknown>

const QUARTER = { '03-31': 1, '06-30': 2, '09-30': 3, '12-31': 4 } as const

/** 2026-09-30 → 2026Q3（与财报表「报告期」列同一种写法） */
const periodLabel = (d: string): string => {
  const q = QUARTER[d.slice(5) as keyof typeof QUARTER]
  return `${d.slice(0, 4)}Q${q ?? '—'}`
}

/**
 * GET /api/stocks/:code/shareholders
 *
 * 十大股东 + 十大流通股东（按报告期）。数据来自 `stock_shareholders`，
 * 由 fetch_stock_shareholders.py 按报告期批量抓取（东财 RPT_F10_EH_HOLDERS /
 * RPT_F10_EH_FREEHOLDERS）。
 *
 * ⚠ 口径（改这里前读脚本 docstring）：
 *   ① kind='top' 十大股东：`totalRatio` = 占总股本 %，`floatRatio` 恒为 null。
 *   ② kind='free' 十大流通股东：`floatRatio` = 占流通股 %，
 *      `totalRatio` = 占总股本 %。**两个比例不是一个分母，不能混着比**。
 *      中芯国际 HKSCC 2026H1：占流通股 58.56% / 占总股本 54.82%。
 *   ③ `holdChange`（股数）与 `changeText`（「不变 / 新进」）分开返回：
 *      源侧同一个字段混着数字与中文，不能当成一个数值列。
 *   ④ top 表源侧**没有披露日**（noticeDate 为 null），free 表才有。
 *   ⑤ 某期只有 top 没有 free（或反之）是源侧如此，不补齐、不用空行占位。
 *   ⑥ 详情类接口一律 200 + {found:false}（axios 对 404 直接抛异常），
 *      前端必须显式分支。
 */
export function stockShareholdersRoutes(app: FastifyInstance) {
  app.get<{
    Params: { code: string }
    Querystring: { period?: string; periods?: string }
  }>('/api/stocks/:code/shareholders', async (req) => {
    const code = req.params.code.trim()
    const periodLimit = Math.min(60, Math.max(1, parseInt(req.query.periods ?? '24', 10) || 24))
    const empty = {
      found: false, code,
      period: null as string | null,
      periods: [] as unknown[],
      top: [] as unknown[], free: [] as unknown[],
      source: 'eastmoney_datacenter_RPT_F10_EH_HOLDERS+FREEHOLDERS',
    }

    const periodRes = await db.execute(sql`
      SELECT DISTINCT report_date::text AS d
      FROM stock_shareholders WHERE code = ${code}
      ORDER BY d DESC LIMIT ${periodLimit}
    `)
    const dates = (periodRes.rows as Row[]).map(r => String(r.d).slice(0, 10))
    if (!dates.length) return empty

    // 未指定 / 指定的报告期库里没有 → 回退到最新一期，并把实际用的期返回给前端
    const wanted = (req.query.period ?? '').slice(0, 10)
    const period = dates.includes(wanted) ? wanted : dates[0]

    const rowsRes = await db.execute(sql`
      SELECT kind, holder_rank, holder_name, holder_code, hold_num,
             total_ratio, float_ratio, hold_change, change_text,
             shares_type, holder_type, is_org, is_dg, is_sjkzr,
             market_cap, total_shares, notice_date, report_name
      FROM stock_shareholders
      WHERE code = ${code} AND report_date = ${period}
      ORDER BY kind, holder_rank
    `)

    const map = (r: Row) => ({
      rank: toInt(r.holder_rank),
      name: r.holder_name ?? null,
      holderCode: r.holder_code ?? null,
      holdNum: toNum(r.hold_num),
      /** 占总股本 %（两类都有） */
      totalRatio: toNum(r.total_ratio),
      /** 占流通股 %（仅十大流通股东有） */
      floatRatio: toNum(r.float_ratio),
      /** 增减股数（源侧是数字时） */
      holdChange: toNum(r.hold_change),
      /** 增减文字：不变 / 新进 / 加仓 … */
      changeText: r.change_text ?? null,
      sharesType: r.shares_type ?? null,
      /** 股东类型（仅流通股东：其它 / 投资公司 / 基金 …） */
      holderType: r.holder_type ?? null,
      isOrg: toInt(r.is_org),
      isDg: toInt(r.is_dg),
      isSjkzr: toInt(r.is_sjkzr),
      marketCap: toNum(r.market_cap),
      totalShares: toNum(r.total_shares),
      noticeDate: day(r.notice_date),
      reportName: r.report_name ?? null,
    })

    const list = rowsRes.rows as Row[]
    return {
      found: list.length > 0,
      code,
      period,
      periods: dates.map(d => ({ date: d, label: periodLabel(d) })),
      top: list.filter(r => r.kind === 'top').map(map),
      free: list.filter(r => r.kind === 'free').map(map),
      source: 'eastmoney_datacenter_RPT_F10_EH_HOLDERS+FREEHOLDERS',
    }
  })
}
