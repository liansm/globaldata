import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { funds, fundHoldings, fundNav } from '../db/schema'
import { eq, desc, asc, sql, and, isNotNull, ilike, gte, lte } from 'drizzle-orm'
import { canonicalCompany } from '../lib/company'

const toNum = (v: string | null | undefined) => (v == null ? null : parseFloat(v))

/** 净值口径：'money' = 货币基金（单位净值列存的是万份收益(元)、累计净值列存的是七日年化(%)） */
type NavKind = 'unit' | 'money' | null

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
        latestNav:        funds.latestNav,
        latestAccNav:     funds.latestAccNav,
        latestNavDate:    funds.latestNavDate,
        latestDailyReturn:funds.latestDailyReturn,
        navKind:          funds.navKind,
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
        latestNav: toNum(r.latestNav),
        latestAccNav: toNum(r.latestAccNav),
        latestDailyReturn: toNum(r.latestDailyReturn),
        navKind: (r.navKind ?? null) as NavKind,
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
      latestNav:         toNum(meta.latestNav),
      latestAccNav:      toNum(meta.latestAccNav),
      latestNavDate:     meta.latestNavDate,
      latestDailyReturn: toNum(meta.latestDailyReturn),
      navKind:           (meta.navKind ?? null) as NavKind,
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

  // ── GET /api/funds/:code/nav ──────────────────────────────────────────────
  // 单只基金净值序列（日线，升序，供图表使用）。
  // Query: days=365 (默认近一年) | from=YYYY-MM-DD | to=YYYY-MM-DD | limit=8000
  // ⚠ navKind='money' 时 nav=万份收益(元)、accNav=七日年化(%)，不是单位净值。
  app.get<{
    Params: { code: string }
    Querystring: { days?: string; from?: string; to?: string; limit?: string }
  }>('/api/funds/:code/nav', async (req, reply) => {
    const { code } = req.params
    const days = parseInt(req.query.days ?? '365')
    const limit = Math.min(parseInt(req.query.limit ?? '8000') || 8000, 20000)

    const [meta] = await db
      .select({ fundCode: funds.fundCode, navKind: funds.navKind, latestNavDate: funds.latestNavDate })
      .from(funds)
      .where(eq(funds.fundCode, code))
    if (!meta) return reply.code(404).send({ error: `Fund '${code}' not found` })

    const iso = (d: Date) => d.toISOString().slice(0, 10)
    const conds = [eq(fundNav.fundCode, code)]
    if (req.query.from) {
      conds.push(gte(fundNav.navDate, req.query.from))
    } else if (Number.isFinite(days) && days > 0) {
      conds.push(gte(fundNav.navDate, iso(new Date(Date.now() - days * 86400000))))
    }
    if (req.query.to) conds.push(lte(fundNav.navDate, req.query.to))

    // 货币基金：源侧对个别场内/停售份额长期返回 0/0（万份收益与七日年化同时为 0）——
    // 全库共 19265 行 / 179 只，其中 12 只的最新点就是 0/0（银华活钱宝 B~E、长盛添利宝 B、
    // 江信增利 B、广发天天利 B 等）。独立源 lsjz 返回的也是 0，属「源侧不披露」而非
    // 「零收益」。直接画出来会是一条贴着 0 的假曲线，故在展示层剔除。
    // ⚠ 判据必须只剔 0/0：万份收益 0 但七日年化非 0（如 070028 嘉实安心货币A 终止尾段，
    //   0.0000 / 0.1240）是**真实披露的 0**，要保留 —— lsjz 返回同样的值。
    // fund_nav 里保留原始值以便追溯，不删数据。
    if (meta.navKind === 'money') {
      conds.push(sql`(${fundNav.unitNav} <> 0 OR COALESCE(${fundNav.accNav}, 0) <> 0)`)
    }

    const rows = await db
      .select({
        date:        fundNav.navDate,
        nav:         fundNav.unitNav,
        accNav:      fundNav.accNav,
        dailyReturn: fundNav.dailyReturn,
      })
      .from(fundNav)
      .where(and(...conds))
      .orderBy(asc(fundNav.navDate))
      .limit(limit)

    return {
      fundCode:  meta.fundCode,
      navKind:   (meta.navKind ?? null) as NavKind,
      latestNavDate: meta.latestNavDate,
      count:     rows.length,
      items: rows.map(r => ({
        date:        r.date,
        nav:         toNum(r.nav),
        accNav:      toNum(r.accNav),
        dailyReturn: toNum(r.dailyReturn),
      })),
    }
  })
}
