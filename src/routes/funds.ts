import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { funds, fundHoldings, fundNav } from '../db/schema'
import { eq, desc, asc, sql, and, isNotNull, ilike, gte, lte, SQL } from 'drizzle-orm'
import { canonicalCompany } from '../lib/company'

const toNum = (v: string | null | undefined) => (v == null ? null : parseFloat(v))

/** 净值口径：'money' = 货币基金（单位净值列存的是万份收益(元)、累计净值列存的是七日年化(%)） */
type NavKind = 'unit' | 'money' | null

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
 * 收益率一律以**累计净值**为基准（含分红再投；单位净值因分红除权会低估收益）。
 * 两条口径约定：
 * 1. `acc_nav > 0` 是硬过滤：acc<=0 为源侧垃圾（全库仅 2 行，005627 / 180305）。
 * 2. nav_kind='money' 的 acc 是七日年化不是累计净值，调用方须先把它排除。
 */
const accAgg = (dir: 'ASC' | 'DESC', nth = 1, dateCond?: SQL) => sql<string | null>`(
  ARRAY_AGG(${fundNav.accNav} ORDER BY ${fundNav.navDate} ${sql.raw(dir)})
    FILTER (WHERE ${fundNav.accNav} > 0${dateCond ? sql` AND ${dateCond}` : sql``})
)[${sql.raw(String(nth))}]`

/**
 * 累计收益的基数。首选「首个可用累计净值」，但**首个是首日占位/承继值时改用第二个**。
 *
 * 指纹：首两个可用累计净值相差 >50%。全库命中且仅命中 8 只，逐只核过：
 *   003244/003245 摩根中国世纪QDII美元现/现钞  1.0000 → 0.2038（首日 1.0 占位，
 *                 美元份额真实面值在 0.2 量级；不修正会显示累计收益 -76.62%）
 *   004870/004874/004876 融通创业板/巨潮100/深证100 指数C、004233 中欧盛世成长C
 *               2.15~2.76 → 0.89~1.38（C 类首日 acc 填了母份额的累计值）
 *   004355 嘉实丰和灵活配置A 1.0000 → 4.1548
 *   000715 民生加银高等级信用债E 2.6530 → 1.0003（份额折算后重新起算）
 * 这 8 只用首个值当基数会算出 -50%~-80% 的假亏损或量级错位的收益。
 */
function pickBase(first: number | null, second: number | null): number | null {
  if (first == null) return second
  if (second != null && Math.abs(second / first - 1) > 0.5) return second
  return first
}

/**
 * 货币口径判定：净值型收益率对它没有意义。
 * 只看 nav_kind 不够 —— 场内货币 ETF/货币型浮动净值基金会被 L1 归成 'unit'
 * （如 511880 银华日利ETF，fund_type=货币型-普通货币，累计净值列实为 1.0→134 的
 * 「每百份累计收益」量级，直接算涨幅会得到 +13306%）。故 fund_type 再兜一层。
 */
const isMoneyLike = (navKind: string | null, fundType: string | null) =>
  navKind === 'money' || (fundType ?? '').includes('货币')

/** 今年来 / 成立以来累计收益（%），内部走一次该基金全序列聚合；货币口径返回两个 null */
async function fundReturns(code: string, moneyLike: boolean) {
  if (moneyLike) return { ytdReturn: null, totalReturn: null }

  const ytdStart = `${new Date().getFullYear()}-01-01`
  const [agg] = await db
    .select({
      firstAcc:        accAgg('ASC'),
      secondAcc:       accAgg('ASC', 2),
      lastAcc:         accAgg('DESC'),
      // 今年来的基数：上年最后一个交易日的累计净值；新基金（上年无数据）退化为今年首个
      prevYearLastAcc: accAgg('DESC', 1, sql`${fundNav.navDate} < ${ytdStart}::date`),
      curYearFirstAcc: accAgg('ASC', 1, sql`${fundNav.navDate} >= ${ytdStart}::date`),
      curYearLastAcc:  accAgg('DESC', 1, sql`${fundNav.navDate} >= ${ytdStart}::date`),
    })
    .from(fundNav)
    .where(and(eq(fundNav.fundCode, code), sql`${fundNav.navKind} <> 'money'`))

  return {
    ytdReturn: pctOf(toNum(agg?.curYearLastAcc),
                     toNum(agg?.prevYearLastAcc) ?? toNum(agg?.curYearFirstAcc)),
    // 成立以来：末个 / 首个可用累计净值。对数据起点晚于成立日的少数基金是「有数据以来」。
    totalReturn: pctOf(toNum(agg?.lastAcc), pickBase(toNum(agg?.firstAcc), toNum(agg?.secondAcc))),
  }
}

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

    // 收益率（今年来 / 成立以来）
    const { ytdReturn, totalReturn } = await fundReturns(code, isMoneyLike(meta.navKind, meta.fundType))

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
      // 今年来涨跌幅 / 成立以来累计收益（%，按累计净值算；货币基金为 null）
      ytdReturn,
      totalReturn,
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
  // 单只基金净值序列（日线，供图表与「近期净值」表格使用）。
  // Query: days=365 (默认近一年，0=全部) | from=YYYY-MM-DD | to=YYYY-MM-DD
  //        limit=8000 (max 20000) | offset=0 | order=asc(默认) | desc
  // 返回 total = 命中总行数（不受 limit/offset 影响），供分页表格用。
  // ⚠ navKind='money' 时 nav=万份收益(元)、accNav=七日年化(%)，不是单位净值。
  app.get<{
    Params: { code: string }
    Querystring: {
      days?: string; from?: string; to?: string
      limit?: string; offset?: string; order?: string
    }
  }>('/api/funds/:code/nav', async (req, reply) => {
    const { code } = req.params
    const days = parseInt(req.query.days ?? '365')
    const limit = Math.min(parseInt(req.query.limit ?? '8000') || 8000, 20000)
    const offset = Math.max(parseInt(req.query.offset ?? '0') || 0, 0)
    const dir = req.query.order === 'desc' ? desc : asc

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
    const where = and(...conds)

    const [{ total }] = await db
      .select({ total: sql<number>`COUNT(*)::int` })
      .from(fundNav)
      .where(where)

    const rows = await db
      .select({
        date:        fundNav.navDate,
        nav:         fundNav.unitNav,
        accNav:      fundNav.accNav,
        dailyReturn: fundNav.dailyReturn,
      })
      .from(fundNav)
      .where(where)
      .orderBy(dir(fundNav.navDate))
      .limit(limit)
      .offset(offset)

    return {
      fundCode:  meta.fundCode,
      navKind:   (meta.navKind ?? null) as NavKind,
      latestNavDate: meta.latestNavDate,
      total,
      count:     rows.length,
      items: rows.map(r => ({
        date:        r.date,
        nav:         toNum(r.nav),
        accNav:      toNum(r.accNav),
        dailyReturn: toNum(r.dailyReturn),
      })),
    }
  })

  // ── GET /api/funds/:code/yearly ───────────────────────────────────────────
  // 历史业绩：每个自然年的涨跌幅（%），按年份降序。
  // 年度收益 = 该年末累计净值 / 上年末累计净值 - 1；首个年度以该年首个可用累计净值为基数
  // （即成立以来）。累计净值含分红再投，与「今年来」口径一致。
  // ⚠ 货币基金没有净值涨跌幅概念（acc 是七日年化）→ 返回空数组，前端只给口径提示。
  // Query: limit=100 (max 200) —— 年度行数很少，不需要 offset 分页。
  app.get<{
    Params: { code: string }
    Querystring: { limit?: string }
  }>('/api/funds/:code/yearly', async (req, reply) => {
    const { code } = req.params
    const limit = Math.min(parseInt(req.query.limit ?? '100') || 100, 200)

    const [meta] = await db
      .select({ fundCode: funds.fundCode, navKind: funds.navKind, fundType: funds.fundType })
      .from(funds)
      .where(eq(funds.fundCode, code))
    if (!meta) return reply.code(404).send({ error: `Fund '${code}' not found` })

    const yr = sql<number>`EXTRACT(YEAR FROM ${fundNav.navDate})::int`
    // 升降序在 JS 侧按 asc 迭代算 lag 更直观，故这里恒升序取，算完再倒序输出
    const rows = isMoneyLike(meta.navKind, meta.fundType) ? [] : await db
      .select({ yr, firstAcc: accAgg('ASC'), secondAcc: accAgg('ASC', 2), lastAcc: accAgg('DESC') })
      .from(fundNav)
      .where(and(eq(fundNav.fundCode, code), sql`${fundNav.navKind} <> 'money'`))
      .groupBy(yr)
      .orderBy(asc(yr))

    // 按年份升序迭代：基数 = 上一年的年末累计净值；首个年度退化为该年首个累计净值
    // （同样要过 pickBase：首日占位/承继值会让首个年度的涨跌幅错量级）
    type YRow = { yr: number; firstAcc: string | null; secondAcc: string | null; lastAcc: string | null }
    const ys = rows as YRow[]

    // 首日占位段探测：首个可用累计净值若是费率占位/母份额承继值，该年的年末值不能当
    // 下一年的基数，否则 003244 的 2017 年会算出 -78% 的假跌（占位 1.0000 → 0.2182）。
    // 第 2 个可用值优先取首个年度内的第 2 个点；该年只有 1 个点（003244 的 2016 全年
    // 就 1 行，且到 2017-10-26 才有下一行）则顺延到下一个有数据年度的首值。
    let ph: number | null = null
    if (ys.length) {
      const fundFirst = toNum(ys[0].firstAcc)
      let fundSecond = toNum(ys[0].secondAcc)
      if (fundSecond == null) {
        for (const r of ys.slice(1)) {
          const a = toNum(r.firstAcc)
          if (a != null) { fundSecond = a; break }
        }
      }
      if (pickBase(fundFirst, fundSecond) !== fundFirst) ph = fundFirst
    }

    const list: { year: number; ret: number | null }[] = []
    let prevLast: number | null = null
    for (const r of ys) {
      const first = toNum(r.firstAcc)
      const last  = toNum(r.lastAcc)
      list.push({ year: r.yr, ret: pctOf(last, prevLast ?? pickBase(first, toNum(r.secondAcc))) })
      if (last != null && (ph == null || last !== ph)) prevLast = last
    }

    return {
      fundCode: meta.fundCode,
      navKind:  (meta.navKind ?? null) as NavKind,
      total:    list.length,
      // 升序算、降序出（前端表格按年份倒序展示）
      items: list.slice(-limit).reverse(),
    }
  })
}
