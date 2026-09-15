import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { funds } from '../db/schema'
import { and, asc, desc, ilike, inArray, isNotNull, sql } from 'drizzle-orm'
import {
  findCompany,
  getCompanyCache,
  type CompanySummary,
} from '../lib/company'

const toNum = (v: string | null | undefined) => (v == null ? null : parseFloat(v))

// 关联子查询里引用外层 funds 表时必须**显式限定表名**。
// drizzle 在 select 位置渲染列时会去掉表前缀，直接写 ${funds.fundCode} 会渲染成
// 无限定的 "fund_code"，在子查询里被内层表 h 的列遮罩 → 变成 h.fund_code =
// h.fund_code（恒真），返回全表 MAX(report_date) 而不是该基金的。实测表现为
// 所有产品的「最新持仓期」都显示成同一个全局最新日期。
const OUTER_FUND_CODE = sql`${sql.identifier('funds')}.${sql.identifier('fund_code')}`

/** 列表页不下发 types（每家公司数十条），只留 topFunds */
function toListItem(c: CompanySummary) {
  const { types, ...rest } = c
  return { ...rest, typeCount: types.length }
}

export async function fundCompaniesRoutes(app: FastifyInstance) {

  // ── GET /api/fund-companies ───────────────────────────────────────────────
  // 基金公司排行（按规范化短名分组，已归并同一法定名的书写差异）。
  // Query: q=关键词（匹配短名/法定名/别名）| order=scale|count|name
  //        | page=1 | pageSize=50 (max 200)
  app.get<{
    Querystring: {
      q?: string
      order?: string
      page?: string
      pageSize?: string
    }
  }>('/api/fund-companies', async (req) => {
    const { q, order = 'scale', page = '1', pageSize = '50' } = req.query

    const { list, unassigned } = await getCompanyCache()

    const kw = (q ?? '').trim().toLowerCase()
    const filtered = kw
      ? list.filter(c =>
          c.key.toLowerCase().includes(kw)
          || c.fullName.toLowerCase().includes(kw)
          || c.aliases.some(a => a.toLowerCase().includes(kw)))
      : list

    const sorted = [...filtered]
    if (order === 'count') {
      sorted.sort((a, b) => b.fundCount - a.fundCount || b.totalScale - a.totalScale)
    } else if (order === 'name') {
      sorted.sort((a, b) => a.key.localeCompare(b.key, 'zh-Hans-CN'))
    }
    // order === 'scale' 时缓存里的顺序已是规模降序

    const limit  = Math.min(Math.max(parseInt(pageSize) || 50, 1), 200)
    const pageNo = Math.max(parseInt(page) || 1, 1)
    const offset = (pageNo - 1) * limit

    return {
      total: sorted.length,
      page: pageNo,
      pageSize: limit,
      unassigned,
      items: sorted.slice(offset, offset + limit).map(toListItem),
    }
  })

  // ── GET /api/fund-companies/:key ──────────────────────────────────────────
  // 单家基金公司：统计 + 类型分布 + 基金经理 + 旗下产品（分页）。
  // Query: type=混合型-偏股 (子串匹配) | q=名称/代码 | order=scale|code|inception
  //        | page=1 | pageSize=50 (max 200)
  app.get<{
    Params: { key: string }
    Querystring: {
      type?: string
      q?: string
      order?: string
      page?: string
      pageSize?: string
    }
  }>('/api/fund-companies/:key', async (req, reply) => {
    const { key } = req.params
    const { type, q, order = 'scale', page = '1', pageSize = '50' } = req.query

    const company = await findCompany(key)
    if (!company) {
      return reply.code(404).send({ error: `Fund company '${key}' not found` })
    }

    const conds = [inArray(funds.fundCompany, company.aliases)]
    if (type) conds.push(ilike(funds.fundType, `%${type}%`))
    if (q) {
      conds.push(sql`(${funds.fundName} ILIKE ${'%' + q + '%'} OR ${funds.fundCode} ILIKE ${'%' + q + '%'})`)
    }
    const where = and(...conds)

    const orderBy = order === 'code'
      ? asc(funds.fundCode)
      : order === 'inception'
        ? sql`${funds.inceptionDate} ASC NULLS LAST, ${funds.fundCode} ASC`
        : sql`${funds.scale} DESC NULLS LAST, ${funds.fundCode} ASC`

    const limit  = Math.min(Math.max(parseInt(pageSize) || 50, 1), 200)
    const pageNo = Math.max(parseInt(page) || 1, 1)
    const offset = (pageNo - 1) * limit

    const [countRow] = await db
      .select({ total: sql<number>`COUNT(*)::int` })
      .from(funds)
      .where(where)

    const rows = await db
      .select({
        fundCode:    funds.fundCode,
        fundName:    funds.fundName,
        fundType:    funds.fundType,
        fundManager: funds.fundManager,
        scale:       funds.scale,
        scaleRaw:    funds.scaleRaw,
        inceptionDate: funds.inceptionDate,
        updatedAt:   funds.updatedAt,
        // 最新一期股票持仓报告期；NULL 表示该产品没有股票持仓明细
        latestReportDate: sql<string | null>`(
          SELECT MAX(h.report_date)::text FROM fund_holdings h
          WHERE h.fund_code = ${OUTER_FUND_CODE} AND h.holding_type = 'stock'
        )`,
      })
      .from(funds)
      .where(where)
      .orderBy(orderBy)
      .limit(limit)
      .offset(offset)

    // 基金经理（按产品数），不受 type/q 筛选影响，始终反映公司全貌
    const managers = await db
      .select({
        name:  funds.fundManager,
        count: sql<number>`COUNT(*)::int`,
        scale: sql<number>`COALESCE(SUM(${funds.scale}), 0)::float8`,
      })
      .from(funds)
      .where(and(
        inArray(funds.fundCompany, company.aliases),
        isNotNull(funds.fundManager),
        sql`${funds.fundManager} <> ''`,
      ))
      .groupBy(funds.fundManager)
      .orderBy(desc(sql`COUNT(*)`), asc(funds.fundManager))
      .limit(24)

    return {
      key:          company.key,
      name:         company.name,
      fullName:     company.fullName,
      aliases:      company.aliases,
      fundCount:    company.fundCount,
      scaleCovered: company.scaleCovered,
      totalScale:   company.totalScale,
      holdingsCovered: company.holdingsCovered,
      managerCount: company.managerCount,
      earliestInception: company.earliestInception,
      types:        company.types,
      topFunds:     company.topFunds,
      managers:     managers.map(m => ({
        name:  m.name as string,
        count: m.count,
        scale: Number(m.scale) || 0,
      })),
      total:        countRow?.total ?? 0,
      page:         pageNo,
      pageSize:     limit,
      items: rows.map(r => ({
        ...r,
        scale: toNum(r.scale),
        companyKey: company.key,
      })),
    }
  })

}
