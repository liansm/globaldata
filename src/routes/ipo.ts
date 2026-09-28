import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { ipoCalendar } from '../db/schema'
import { eq, and, gte, lte, or, sql } from 'drizzle-orm'

// numeric 列在 pg 驱动里返回的是**字符串**，必须在 API 层转 number，
// 否则前端 toFixed / 算术会直接崩（本项目踩过多次）
const toNum = (v: string | null) => (v == null ? null : parseFloat(v))

export async function ipoRoutes(app: FastifyInstance) {

  // ── GET /api/ipo-calendar ────────────────────────────────────────────────
  // 新股日历：一只新股有多个关键日期（申购/招股、中签、缴款、暗盘、上市），
  // 只要**任一关键日期**落在 [from, to] 区间内就返回，由前端按日归组渲染月历。
  //
  // Query:
  //   market = 全部(默认) | A股 | 北交所 | 港股
  //   from   = YYYY-MM-DD（默认 今天 - 30 天）
  //   to     = YYYY-MM-DD（默认 今天 + 120 天）
  //   limit  = 默认 1000，上限 5000
  app.get<{
    Querystring: { market?: string; from?: string; to?: string; limit?: string }
  }>('/api/ipo-calendar', async (req) => {
    const today = new Date()
    const iso = (d: Date) => d.toISOString().slice(0, 10)
    const shift = (days: number) => {
      const d = new Date(today)
      d.setDate(d.getDate() + days)
      return iso(d)
    }

    const from  = req.query.from  ?? shift(-30)
    const to    = req.query.to    ?? shift(120)
    const limit = Math.min(parseInt(req.query.limit ?? '1000', 10) || 1000, 5000)
    const market = req.query.market && req.query.market !== '全部' ? req.query.market : null

    // 关键日期列 —— 任一命中即纳入
    const dateCols = [
      ipoCalendar.applyDate,
      ipoCalendar.applyEndDate,
      ipoCalendar.greyDate,
      ipoCalendar.allotmentDate,
      ipoCalendar.listingDate,
    ]

    const dateHit = or(
      ...dateCols.flatMap(col => [
        and(gte(col, from), lte(col, to)),
      ]),
    )

    const where = market
      ? and(eq(ipoCalendar.market, market), dateHit)
      : dateHit

    const rows = await db
      .select()
      .from(ipoCalendar)
      .where(where)
      .orderBy(sql`COALESCE(${ipoCalendar.applyDate}, ${ipoCalendar.listingDate})`)
      .limit(limit)

    return rows.map(r => ({
      market:        r.market,
      code:          r.code,
      name:          r.name,
      exchange:      r.exchange,
      board:         r.board,
      industry:      r.industry,
      issuePrice:    toNum(r.issuePrice),
      issuePriceHigh: toNum(r.issuePriceHigh),
      currency:      r.currency,
      issueShares:   toNum(r.issueShares),
      raiseAmount:   toNum(r.raiseAmount),
      lotSize:       toNum(r.lotSize),
      entryFee:      toNum(r.entryFee),
      applyDate:     r.applyDate,
      applyEndDate:  r.applyEndDate,
      pricingDate:   r.pricingDate,
      allotmentDate: r.allotmentDate,
      payDate:       r.payDate,
      refundDate:    r.refundDate,
      greyDate:      r.greyDate,
      listingDate:   r.listingDate,
      peIssue:       toNum(r.peIssue),
      peIndustry:    toNum(r.peIndustry),
      winRate:       toNum(r.winRate),
      source:        r.source,
      updatedAt:     r.updatedAt,
    }))
  })

  // ── GET /api/ipo-calendar/stats ─────────────────────────────────────────
  // 顶部指标条：区间内各市场的待申购 / 待上市只数
  app.get<{ Querystring: { from?: string; to?: string } }>(
    '/api/ipo-calendar/stats',
    async (req) => {
      const today = new Date().toISOString().slice(0, 10)
      const from = req.query.from ?? today
      const to   = req.query.to   ?? (() => {
        const d = new Date()
        d.setDate(d.getDate() + 120)
        return d.toISOString().slice(0, 10)
      })()

      const rows = await db
        .select({
          market:        ipoCalendar.market,
          upcomingApply: sql<number>`COUNT(*) FILTER (WHERE ${ipoCalendar.applyDate} >= ${today}::date AND ${ipoCalendar.applyDate} <= ${to}::date)`,
          upcomingList:  sql<number>`COUNT(*) FILTER (WHERE ${ipoCalendar.listingDate} >= ${today}::date AND ${ipoCalendar.listingDate} <= ${to}::date)`,
          total:         sql<number>`COUNT(*)`,
        })
        .from(ipoCalendar)
        .groupBy(ipoCalendar.market)

      return rows.map(r => ({
        market:        r.market,
        upcomingApply: Number(r.upcomingApply ?? 0),
        upcomingList:  Number(r.upcomingList ?? 0),
        total:         Number(r.total ?? 0),
      }))
    },
  )
}
