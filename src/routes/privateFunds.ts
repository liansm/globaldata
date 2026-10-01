import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { privateFunds, privateManagers, privateFundNav } from '../db/schema'
import { eq, desc, asc, sql, and, isNotNull, ilike, gte, lte, SQL } from 'drizzle-orm'

const toNum = (v: string | null | undefined) => (v == null ? null : parseFloat(v))

/**
 * 涨跌幅 = 期末 / 基数 - 1，保留 2 位小数（%）。
 * 基数缺失或 <= 0 返回 null —— 宁缺勿错，绝不拿 0 当分母。
 */
function pctOf(end: number | null, base: number | null): number | null {
  if (end == null || base == null || base <= 0) return null
  return Math.round((end / base - 1) * 10000) / 100
}

/**
 * 取某条件下按日期排序的第 n 个累计净值（n 从 1 起）。
 * 私募与公募同口径：收益率一律以**累计净值**为基准（含分红再投）。
 * `acc_nav > 0` 是硬过滤 —— 0 值为源侧垃圾（代销池里偶见）。
 */
const accAgg = (dir: 'ASC' | 'DESC', nth = 1, dateCond?: SQL) => sql<string | null>`(
  ARRAY_AGG(${privateFundNav.accNav} ORDER BY ${privateFundNav.navDate} ${sql.raw(dir)})
    FILTER (WHERE ${privateFundNav.accNav} > 0${dateCond ? sql` AND ${dateCond}` : sql``})
)[${sql.raw(String(nth))}]`

export async function privateFundsRoutes(app: FastifyInstance) {

  /**
   * 中基协 `pof/fund` 响应里自报的 `totalElements` —— 即备案名录的**分母**。
   *
   * ⚠ 这个数会随后端实例在两套快照间波动（实测同一分钟内 249,863 / 249,931 交替），
   * 所以只能当「量级参考」，不能当精确分母。但我们扫到的 / 它，是唯一能自己算出来的
   * 覆盖率口径 —— 前端必须展示，否则用户会以为名录是全量的。
   *
   * ℹ 分母里还含约 700 条 `fundNo` 为空的 2013~2016 年通道类资管计划
   * （中基协未赋备案编码），抓到也写不进库（主键缺失）→ 可入库上限 ≈ 249,231。
   * 即 100% 是做不到的，详见 fetch_private_funds.py 的 docstring。
   */
  const AMAC_SOURCE_TOTAL = 249863

  // ── GET /api/private-funds/stats ──────────────────────────────────────────
  // 概览：备案基数、代销池覆盖情况。⚠ 两个 coverage 都必须展示给用户 ——
  //    净值只覆盖代销池（约 0.49%），不是全市场私募业绩；
  //    备案名录本身也只有源端总量的 ~98%（分页漂移 + 缺主键），不是「全量」。
  app.get('/api/private-funds/stats', async () => {
    const [m] = await db
      .select({ managers: sql<number>`COUNT(*)::int` })
      .from(privateManagers)
    const [f] = await db
      .select({
        funds:    sql<number>`COUNT(*)::int`,
        registry: sql<number>`COUNT(*) FILTER (WHERE ${privateFunds.inRegistry})::int`,
        withNav:  sql<number>`COUNT(*) FILTER (WHERE ${privateFunds.hasNav})::int`,
      })
      .from(privateFunds)
    const [n] = await db
      .select({
        navRows:       sql<number>`COUNT(*)::int`,
        latestNavDate: sql<string | null>`MAX(${privateFundNav.navDate})::text`,
      })
      .from(privateFundNav)

    const funds   = f?.funds    ?? 0
    const withNav = f?.withNav  ?? 0
    const registry = f?.registry ?? 0
    return {
      managers:      m?.managers ?? 0,
      funds,
      registry,
      registrySource: AMAC_SOURCE_TOTAL,
      withNav,
      navRows:       n?.navRows  ?? 0,
      latestNavDate: n?.latestNavDate ?? null,
      // 公开净值覆盖率（%），前端要显著展示。分子 = private_funds.has_nav（两源并集）
      navCoverage:   funds > 0 ? Math.round((withNav / funds) * 1000000) / 10000 : null,
      // 备案名录相对源端自报总量的覆盖率（%）。注意分子会随多遍扫描上升，分母会随快照波动
      registryCoverage: Math.round((registry / AMAC_SOURCE_TOTAL) * 1000000) / 10000,
    }
  })

  // ── GET /api/private-funds/states ─────────────────────────────────────────
  // 运行状态分布（列表页筛选用）
  app.get('/api/private-funds/states', async () => {
    const rows = await db
      .select({ state: privateFunds.workingState, count: sql<number>`COUNT(*)::int` })
      .from(privateFunds)
      .groupBy(privateFunds.workingState)
      .orderBy(desc(sql`COUNT(*)`))
    return rows
  })

  // ── GET /api/private-funds/managers ───────────────────────────────────────
  // 管理人列表。Query: q=名称关键词 | investType=机构类型 | province=注册地 | page | pageSize
  app.get<{
    Querystring: { q?: string; investType?: string; province?: string; page?: string; pageSize?: string }
  }>('/api/private-funds/managers', async (req) => {
    const { q, investType, province, page = '1', pageSize = '50' } = req.query

    const conds = []
    if (q)          conds.push(ilike(privateManagers.managerName, `%${q}%`))
    if (investType) conds.push(ilike(privateManagers.investType, `%${investType}%`))
    if (province)   conds.push(ilike(privateManagers.registerProvince, `%${province}%`))
    const where = conds.length ? and(...conds) : undefined

    const limit  = Math.min(parseInt(pageSize) || 50, 200)
    const offset = (Math.max(parseInt(page) || 1, 1) - 1) * limit

    const [{ total }] = await db
      .select({ total: sql<number>`COUNT(*)::int` })
      .from(privateManagers)
      .where(where)

    const rows = await db
      .select({
        registerNo:       privateManagers.registerNo,
        managerName:      privateManagers.managerName,
        artificialPerson: privateManagers.artificialPerson,
        investType:       privateManagers.investType,
        registerProvince: privateManagers.registerProvince,
        officeAddress:    privateManagers.officeAddress,
        establishDate:    privateManagers.establishDate,
        registerDate:     privateManagers.registerDate,
        fundCount:        privateManagers.fundCount,
        memberType:       privateManagers.memberType,
        hasCreditTips:    privateManagers.hasCreditTips,
      })
      .from(privateManagers)
      .where(where)
      // 注意：这里不能写成 desc(sql`... DESC NULLS LAST`)——外面再包 desc() 会生成
      // `"fund_count" DESC NULLS LAST desc`，PG 报 42601 语法错误。排序方向写在片段里。
      .orderBy(sql`${privateManagers.fundCount} DESC NULLS LAST`, asc(privateManagers.managerName))
      .limit(limit)
      .offset(offset)

    return { total, page: Math.max(parseInt(page) || 1, 1), pageSize: limit, rows }
  })

  // ── GET /api/private-funds/manager-types ──────────────────────────────────
  app.get('/api/private-funds/manager-types', async () => {
    const rows = await db
      .select({ investType: privateManagers.investType, count: sql<number>`COUNT(*)::int` })
      .from(privateManagers)
      .groupBy(privateManagers.investType)
      .orderBy(desc(sql`COUNT(*)`))
    return rows
  })

  // ── GET /api/private-funds ────────────────────────────────────────────────
  // 备案产品列表。
  // Query: q=名称/编码/管理人关键词 | state=运行状态 | hasNav=1 (仅有净值的)
  //        order=nav|record|established | page | pageSize (max 200)
  app.get<{
    Querystring: {
      q?: string; state?: string; hasNav?: string; order?: string
      page?: string; pageSize?: string
    }
  }>('/api/private-funds', async (req) => {
    const { q, state, hasNav, order = 'record', page = '1', pageSize = '50' } = req.query

    const conds = []
    if (q) conds.push(sql`(${privateFunds.fundName} ILIKE ${'%' + q + '%'}
                       OR ${privateFunds.fundNo} ILIKE ${'%' + q + '%'}
                       OR ${privateFunds.managerName} ILIKE ${'%' + q + '%'})`)
    if (state) conds.push(eq(privateFunds.workingState, state))
    if (hasNav) conds.push(eq(privateFunds.hasNav, true))
    const where = conds.length ? and(...conds) : undefined

    const orderBy = order === 'record'
      ? sql`${privateFunds.recordDate} DESC NULLS LAST, ${privateFunds.fundNo} ASC`
      : order === 'established'
        ? sql`${privateFunds.establishDate} DESC NULLS LAST, ${privateFunds.fundNo} ASC`
        : sql`${privateFunds.latestNavDate} DESC NULLS LAST, ${privateFunds.fundNo} ASC`

    const limit  = Math.min(parseInt(pageSize) || 50, 200)
    const offset = (Math.max(parseInt(page) || 1, 1) - 1) * limit

    const [{ total }] = await db
      .select({ total: sql<number>`COUNT(*)::int` })
      .from(privateFunds)
      .where(where)

    const rows = await db
      .select({
        fundNo:            privateFunds.fundNo,
        fundName:          privateFunds.fundName,
        managerName:       privateFunds.managerName,
        managerType:       privateFunds.managerType,
        workingState:      privateFunds.workingState,
        recordDate:        privateFunds.recordDate,
        establishDate:     privateFunds.establishDate,
        mandatorName:      privateFunds.mandatorName,
        inRegistry:        privateFunds.inRegistry,
        hasNav:            privateFunds.hasNav,
        latestNav:         privateFunds.latestNav,
        latestAccNav:      privateFunds.latestAccNav,
        latestNavDate:     privateFunds.latestNavDate,
        latestDailyReturn: privateFunds.latestDailyReturn,
        latestFundSize:    privateFunds.latestFundSize,
      })
      .from(privateFunds)
      .where(where)
      .orderBy(orderBy)
      .limit(limit)
      .offset(offset)

    return {
      total,
      page: Math.max(parseInt(page) || 1, 1),
      pageSize: limit,
      // ⚠ drizzle 的 numeric 列取出来是**字符串**，前端 toFixed/toLocaleString 会炸
      //   （2026-09-17 页面白屏，PAGEERR: v.toFixed is not a function）→ 出口统一转 number
      rows: rows.map((r) => ({
        ...r,
        latestNav:         toNum(r.latestNav),
        latestAccNav:      toNum(r.latestAccNav),
        latestDailyReturn: toNum(r.latestDailyReturn),
        latestFundSize:    toNum(r.latestFundSize),
      })),
    }
  })

  // ── GET /api/private-funds/:code/nav ──────────────────────────────────────
  // 净值序列（仅代销池有数据的 1,208 只；其余返回空数组）。
  // Query: from=2026-01-01（今年来用） | days=365（0 = 全部） | offset | order=asc|desc
  app.get<{
    Params: { code: string }
    Querystring: { days?: string; from?: string; offset?: string; limit?: string; order?: string }
  }>('/api/private-funds/:code/nav', async (req) => {
    const { code } = req.params
    const { days = '365', from, offset = '0', limit, order = 'desc' } = req.query
    const dayN = parseInt(days)
    // 上限 3000：走势图「全部」区间一次最多拿 3000 点，表格分页靠 offset+limit
    const lim = Math.min(Math.max(parseInt(limit || '3000') || 3000, 1), 3000)

    const conds = [eq(privateFundNav.fundNo, code)]
    if (from) {
      conds.push(gte(privateFundNav.navDate, from))
    } else if (dayN > 0) {
      const since = new Date(Date.now() - dayN * 86400000).toISOString().slice(0, 10)
      conds.push(gte(privateFundNav.navDate, since))
    }

    const [{ total }] = await db
      .select({ total: sql<number>`COUNT(*)::int` })
      .from(privateFundNav)
      .where(and(...conds))

    const rows = await db
      .select({
        navDate:     privateFundNav.navDate,
        unitNav:     privateFundNav.unitNav,
        accNav:      privateFundNav.accNav,
        dailyReturn: privateFundNav.dailyReturn,
        source:      privateFundNav.source,
      })
      .from(privateFundNav)
      .where(and(...conds))
      .orderBy(order === 'asc' ? asc(privateFundNav.navDate) : desc(privateFundNav.navDate))
      .limit(lim)
      .offset(Math.max(parseInt(offset) || 0, 0))

    return {
      total,
      rows: rows.map((r) => ({
        ...r,
        unitNav: toNum(r.unitNav),
        accNav:  toNum(r.accNav),
        dailyReturn: toNum(r.dailyReturn),
      })),
    }
  })

  // ── GET /api/private-funds/:code ──────────────────────────────────────────
  // 产品详情：备案字段 + 净值快照 + 今年来/成立以来收益（累计净值口径）
  // ⚠ 注意注册顺序：本路由带参数，必须放在所有静态子路径之后
  app.get<{ Params: { code: string } }>('/api/private-funds/:code', async (req) => {
    const { code } = req.params

    const [fund] = await db
      .select()
      .from(privateFunds)
      .where(eq(privateFunds.fundNo, code))
      .limit(1)

    if (!fund) return { found: false, code }

    const ytdStart = `${new Date().getFullYear()}-01-01`
    const [agg] = await db
      .select({
        firstAcc:        accAgg('ASC'),
        secondAcc:       accAgg('ASC', 2),
        lastAcc:         accAgg('DESC'),
        pointCount:      sql<number>`COUNT(*)::int`,
        earliest:        sql<string | null>`MIN(${privateFundNav.navDate})::text`,
        latest:          sql<string | null>`MAX(${privateFundNav.navDate})::text`,
        prevYearLastAcc: accAgg('DESC', 1, sql`${privateFundNav.navDate} < ${ytdStart}::date`),
        curYearFirstAcc: accAgg('ASC', 1, sql`${privateFundNav.navDate} >= ${ytdStart}::date`),
        curYearLastAcc:  accAgg('DESC', 1, sql`${privateFundNav.navDate} >= ${ytdStart}::date`),
        // 同一 fund_no 不会混源（写库侧对重叠标的整只跳过），所以 MIN 取到的就是该产品的唯一来源
        navSource:       sql<string | null>`MIN(${privateFundNav.source})::text`,
      })
      .from(privateFundNav)
      .where(eq(privateFundNav.fundNo, code))

    // 首日占位值守卫：首两个可用累计净值相差 >50% 时改用第二个（同公募 pickBase）
    const first = toNum(agg?.firstAcc), second = toNum(agg?.secondAcc)
    const base = (first != null && second != null && Math.abs(second / first - 1) > 0.5)
      ? second
      : first

    return {
      found: true,
      ...fund,
      latestNav:         toNum(fund.latestNav),
      latestAccNav:      toNum(fund.latestAccNav),
      latestDailyReturn: toNum(fund.latestDailyReturn),
      latestFundSize:    toNum(fund.latestFundSize),
      navPointCount:     agg?.pointCount ?? 0,
      navEarliest:       agg?.earliest ?? null,
      navLatest:         agg?.latest ?? null,
      navSource:         agg?.navSource ?? null,
      ytdReturn: pctOf(toNum(agg?.curYearLastAcc),
                       toNum(agg?.prevYearLastAcc) ?? toNum(agg?.curYearFirstAcc)),
      totalReturn: pctOf(toNum(agg?.lastAcc), base),
    }
  })
}
