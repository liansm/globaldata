import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { funds, fundHoldings } from '../db/schema'
import { eq, desc, asc, sql, and, isNotNull, ilike } from 'drizzle-orm'
import { canonicalCompany } from '../lib/company'

const toNum = (v: string | null | undefined) => (v == null ? null : parseFloat(v))

export async function fundsRoutes(app: FastifyInstance) {

  // ── GET /api/funds/types ──────────────────────────────────────────────────
  // 基金类型分布（用于列表页筛选下拉）
  app.get('/api/funds/types', async () => {
    const rows = await db
      .select({
        fundType: funds.fundType,
        count:    sql<number>`COUNT(*)::int`,
      })
      .from(funds)
      .groupBy(funds.fundType)
      .orderBy(desc(sql`COUNT(*)`))
    return rows
  })

  // ── GET /api/funds ────────────────────────────────────────────────────────
  // 公募基金名录 + 最新规模。
  // Query: type=混合型-偏股 (子串匹配) | q=名称或代码关键词 |
  //        withScale=1 (仅有规模的) | order=scale|code | page=1 | pageSize=50 (max 200)
  app.get<{
    Querystring: {
      type?: string
      q?: string
      withScale?: string
      order?: string
      page?: string
      pageSize?: string
    }
  }>('/api/funds', async (req) => {
    const { type, q, withScale, order = 'scale', page = '1', pageSize = '50' } = req.query

    const conds = []
    if (type)        conds.push(ilike(funds.fundType, `%${type}%`))
    if (q)          conds.push(sql`(${funds.fundName} ILIKE ${'%'+q+'%'} OR ${funds.fundCode} ILIKE ${'%'+q+'%'})`)
    if (withScale)   conds.push(isNotNull(funds.scale))
    const where = conds.length ? and(...conds) : undefined

    const orderBy = order === 'code'
      ? asc(funds.fundCode)
      : sql`${funds.scale} DESC NULLS LAST, ${funds.fundCode} ASC`

    const limit  = Math.min(parseInt(pageSize) || 50, 200)
    const offset = (Math.max(parseInt(page) || 1, 1) - 1) * limit

    const [{ total }] = await db
      .select({ total: sql<number>`COUNT(*)::int` })
      .from(funds)
      .where(where)

    const rows = await db
      .select({
        fundCode:    funds.fundCode,
        fundName:    funds.fundName,
        fundType:    funds.fundType,
        fundCompany: funds.fundCompany,
        fundManager: funds.fundManager,
        scale:       funds.scale,
        scaleRaw:    funds.scaleRaw,
        inceptionDate: funds.inceptionDate,
      })
      .from(funds)
      .where(where)
      .orderBy(orderBy)
      .limit(limit)
      .offset(offset)

    return {
      total,
      page: Math.max(parseInt(page) || 1, 1),
      pageSize: limit,
      items: rows.map(r => ({
        ...r,
        scale: toNum(r.scale),
        // 规范化公司短名：前端据此跳转 /company/:key（原始名有「中欧基金公司 /
        // 中欧基金管理有限公司」这类书写碎片，不能直接当链接）
        companyKey: r.fundCompany ? canonicalCompany(r.fundCompany) : null,
      })),
    }
  })

  // ── GET /api/funds/:code ──────────────────────────────────────────────────
  // 单只基金：元数据 + 持仓明细（默认最新报告期，?date=YYYY-MM-DD 指定期）。
  app.get<{
    Params: { code: string }
    Querystring: { date?: string }
  }>('/api/funds/:code', async (req, reply) => {
    const { code } = req.params
    const { date } = req.query

    const [meta] = await db.select().from(funds).where(eq(funds.fundCode, code))
    if (!meta) {
      return reply.code(404).send({ error: `Fund '${code}' not found` })
    }

    // 可用报告期列表（降序）
    const dateRows = await db
      .select({ reportDate: fundHoldings.reportDate })
      .from(fundHoldings)
      .where(eq(fundHoldings.fundCode, code))
      .groupBy(fundHoldings.reportDate)
      .orderBy(desc(fundHoldings.reportDate))

    const target = date || dateRows[0]?.reportDate || null

    const holdings = target
      ? await db
          .select()
          .from(fundHoldings)
          .where(and(
            eq(fundHoldings.fundCode, code),
            eq(fundHoldings.reportDate, target),
          ))
          .orderBy(desc(fundHoldings.ratio))
      : []

    return {
      fundCode:      meta.fundCode,
      fundName:      meta.fundName,
      fundType:      meta.fundType,
      fundCompany:   meta.fundCompany,
      companyKey:    meta.fundCompany ? canonicalCompany(meta.fundCompany) : null,
      fundManager:   meta.fundManager,
      scale:         toNum(meta.scale),
      scaleRaw:      meta.scaleRaw,
      inceptionDate: meta.inceptionDate,
      updatedAt:     meta.updatedAt,
      reportDates:   dateRows.map(d => d.reportDate),
      holdings: holdings.map(h => ({
        reportDate:   h.reportDate,
        holdingType:  h.holdingType,
        securityCode: h.securityCode,
        securityName: h.securityName,
        ratio:        toNum(h.ratio),
        shares:       toNum(h.shares),
        marketValue:  toNum(h.marketValue),
      })),
    }
  })
}
