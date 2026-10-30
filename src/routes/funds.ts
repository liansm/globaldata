import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { funds, fundHoldings, fundNav } from '../db/schema'
import { eq, desc, asc, sql, and, isNotNull, ilike, gte, lte } from 'drizzle-orm'
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

const round2 = (v: number) => Math.round(v * 100) / 100

type NavPoint = { d: string; u: number | null; a: number | null; r: number | null }
type YearRet = { year: number; ret: number }

export type FundReturns = {
  /** 今年来涨跌幅（%），**复权**（分红再投）—— 主口径，与雪球/天天基金一致 */
  ytdReturn: number | null
  /** 成立以来累计收益（%），**复权** */
  totalReturn: number | null
  /** 同类指标的**累计净值口径**（现金分红不再投）—— 副口径，只用于提示里做对照 */
  ytdReturnAcc: number | null
  totalReturnAcc: number | null
  /** 分年度涨跌幅（%），升序；复权口径，与 totalReturn 同源 */
  years: YearRet[]
}

/**
 * 业绩指标的唯一出处：一次遍历算出「复权口径」与「累计净值口径」两套收益。
 *
 * 为什么主口径不能用累计净值末值/首值：累计净值恒等于 `s × 单位净值 + 累计分红`，
 * s 是份额折算累积系数。分红越多、折算越大，分母里沉积的现金占比越高，增长率被稀释
 * 得越厉害。163402 做过 1:4 份额折算，累计净值 10.7841 里 7.8488 是已分掉的现金、
 * 只有 2.94 还在运作，于是「末值/首值」只算出 978.41%，而复权口径（雪球 `nav_grbase`）
 * 是 2128.03% —— 差 2.2 倍，不是舍入误差。
 *
 * 复权日收益率的恒等式：`ΔA_i = s_前一日 · u_前一日 · m_i`（m = 当日复权收益率）。
 * 普通交易日、分红日、份额折算日都成立 —— 折算日要用**折算前**的 s，正好是手里那个。
 * 所以 `m = ΔA / (s · u_前一日)` 不需要源侧 daily_return，源侧 equityReturn 的个别
 * 错值（实测 000001 的 2026-10-15、163402 的 2026-09-22 与净值对不上）不会污染结果。
 * 实测 110022 / 519068 / 000001 与蛋卷逐位相同（180.50 / 295.71 / 529.55）。
 *
 * s 的估计（正常交易日 ΔA/Δu 恒等于 s）：
 *   · 单日偏离 s 超 20% → 分红日，比值不可用，s 不动；
 *   · 下一个有效交易日仍偏离且与当日一致 → 份额折算，s 直接换成新值。
 * 折算系数按 [0.2, 15] 设闸，越界说明模型对这只基金不成立（实测 23 只，多为
 * 分级/折算份额，acc 与 unit 的量纲根本不是 s 倍关系），此时整只基金回退到源侧
 * 日收益率连乘；两套结果若相差超过 50% 也按同样方式回退 —— 宁可差 1%，
 * 不能出一个量级错误。
 */
type AdjResult = {
  /** 逐日复权因子，`f[i] = 1 + 当日复权收益率`（`f[0]` 恒为 1） */
  f: Float64Array
  /**
   * `scales[i]` = 第 i 行**当日收盘后**生效的份额折算系数（1 = 从未折算）。
   * 只用于「分红不复投」口径把初始持有份额还原成正确量纲，别拿它算别的。
   */
  scales: Float64Array
}

/**
 * 逐日复权因子 `f[i] = 1 + 当日复权收益率`（`f[0]` 恒为 1）。
 *
 * **必须传全序列**：份额折算系数 `s` 要在整条时间轴上累积估计，只喂区间内的行
 * 会把折算前后的两段混在一条曲线里，算出来的区间涨跌是错的。
 *
 * 单列出这个函数是为了让「成立以来收益」「分年度收益」「走势图区间涨跌」共用
 * 同一份因子 —— 三处各算一遍迟早会漂。
 */
function adjustFactors(rows: NavPoint[]): AdjResult {
  const n = rows.length
  const fAcc = new Float64Array(n).fill(1) // Δ累计净值法（默认口径）
  const fDr  = new Float64Array(n).fill(1) // 源侧日增长率连乘（回退口径）
  const scales = new Float64Array(n).fill(1) // 逐日生效的份额折算系数
  const dus    = new Float64Array(n).fill(NaN)
  const das    = new Float64Array(n).fill(NaN)
  const ratios = new Float64Array(n).fill(NaN)
  const mAcc   = new Float64Array(n)

  for (let i = 1; i < n; i++) {
    const pu = rows[i - 1].u, pa = rows[i - 1].a
    const cu = rows[i].u,     ca = rows[i].a
    const dr = rows[i].r
    if (dr != null && dr > -100) fDr[i] = 1 + dr / 100
    if (pu == null || pa == null || pu <= 0 || cu == null || cu <= 0 || ca == null || ca <= 0) continue
    const du = cu - pu
    dus[i] = du
    das[i] = ca - pa
    // 只在**相对**波动 >0.5% 的交易日取样：净值只有 4 位小数，波动太小时
    // ΔA/Δu 会被舍入噪声污染（0.0021 的变动配 0.0001 的误差就是 5% 偏差）
    if (Math.abs(du) > 0.005 * pu) ratios[i] = das[i] / du
  }

  let s = 1 // 当前生效的份额折算系数（1 = 从未做过折算）
  for (let i = 1; i < n; i++) {
    if (Number.isNaN(dus[i])) { scales[i] = s; continue }
    const pu = rows[i - 1].u!
    const du = dus[i]
    let m = das[i] / (s * pu)
    if (!Number.isFinite(m) || Math.abs(m) > 0.5) {
      // 公募单日不可能 ±50%，越界只可能是 acc 跳变/净值缺失；退回净口径收益率，
      // 净口径也越界就按当日无收益处理，绝不拿脏值连乘
      const raw = du / pu
      m = Math.abs(raw) <= 0.5 ? raw : 0
    }
    mAcc[i] = m
    fAcc[i] = 1 + m

    // 单日偏离 s 超 20%：可能是分红日（次日比值回到 s），也可能是份额折算
    // （次日及以后比值＝新 s）。两道门槛，缺一不可：
    //   ① 当天净值要**真的跳变**（>2%）—— 份额折算一定是量级事件（163402 是
    //      3.9975→1.0000）。债券基金日波动只有万分之几，4 位小数的舍入就能让
    //      ΔA/Δu 变成任意值，000833 因此被误判成折算过，s 记成 0.0758，acc 算出 557%；
    //   ② 要求**连续两个**有效交易日都偏离且互相一致 —— 只看一天会被噪声带跑，
    //      s 一旦漂离 1，全序列日收益就都错：实测能把没折算过的 002459 从 96.97% 算成 187%。
    const r = ratios[i]
    if (Number.isFinite(r) && Math.abs(du / pu) > 0.02 && Math.abs(r / s - 1) > 0.2) {
      let r1 = NaN, r2 = NaN
      for (let j = i + 1; j < n; j++) {
        if (!Number.isFinite(ratios[j]) || ratios[j] <= 0) continue
        if (Number.isNaN(r1)) { r1 = ratios[j] } else { r2 = ratios[j]; break }
      }
      // r1 还要落在折算系数的合理区间 —— 分红日的舍入噪声同样能凑出 0.005 这种比值，
      // 而公募份额折算最极端也就 1:4 上下（163402 是 3.99），不可能落在 [0.2, 15] 之外
      if (Number.isFinite(r1) && Number.isFinite(r2) && r1 >= 0.2 && r1 <= 15 &&
          Math.abs(r1 / s - 1) > 0.2 && Math.abs(r1 / r2 - 1) < 0.2) s = r1
    }
    // 记「i 行收盘后」的份额：折算当天先按旧 s 算了 m（上面），这里才落到新 s
    scales[i] = s
  }

  // 选口径：默认用 Δ累计净值法 —— 它才是复权的定义式，不依赖源侧日增长率。
  // 唯一需要的自检是「折算系数 s 估对了吗」：s 若正确，算出来的日收益率 m 与源侧
  // 日增长率 dr 同量纲，逐日比值的中位数就落在 1 附近。取中位数而不是「一致天数占比」：
  //   · 对少数脏日子免疫 —— 010500 只有 3 天 dr 明显错（当天净值 +5.63% 而 dr 记 -0.35%），
  //     占 1452 天的 0.2%，按天数算自洽率 99.7% 看着极可信，可累计被带偏 4.6pp
  //     （dr 连乘 -6.25%，雪球 -1.70%，Δ累计净值法 -1.70%）；
  //   · 不受 dr 大段 0 影响 —— 010519 这类定开品种，dr 串里丢了几个大跌日会算成 +5.19%，
  //     而净值实际 -10.95%；比值里 0 直接不进样本，中位数仍为 1。
  // 两条否决线（任一不满足就回退源侧连乘）：
  //   ① s 越界 [0.2, 15] —— 分级/场内份额的 acc 与 unit 不是 s 倍关系，模型不成立，
  //      实测会算出 2.6 亿%、102 亿% 这种数；
  //   ② 中位比值偏离 1 超 15% —— s 整体估错，m 被均匀缩放，此时信源侧。
  // 样本不足（债券基金日波动普遍 <0.3%，取不到样本）不否决，仍用 Δ累计净值法。
  const q: number[] = []
  for (let i = 1; i < n; i++) {
    const dr = rows[i].r
    if (dr == null || Math.abs(dr) < 0.3) continue
    const k = mAcc[i] / (dr / 100)
    if (Number.isFinite(k) && k > 0) q.push(k)
  }
  q.sort((a, b) => a - b)
  const sOK = s >= 0.2 && s <= 15
  const med = q.length ? q[q.length >> 1] : 1
  return { f: sOK && Math.abs(med - 1) <= 0.15 ? fAcc : fDr, scales }
}

function computeReturns(rows: NavPoint[]): FundReturns {
  const thisYear = new Date().getFullYear()
  const ytdStart = `${thisYear}-01-01`

  // ── 副口径：累计净值（现金分红不再投）────────────────────────────────────
  type AccRow = { d: string; a: number }
  const accRows = rows.filter(r => r.a != null && r.a > 0) as unknown as AccRow[]
  const lastAcc = accRows.length ? accRows[accRows.length - 1].a : null
  const totalReturnAcc = pctOf(lastAcc, pickBase(accRows[0]?.a ?? null, accRows[1]?.a ?? null))

  const beforeYtd = accRows.filter(r => r.d < ytdStart)
  const inYtd     = accRows.filter(r => r.d >= ytdStart)
  const ytdReturnAcc = pctOf(
    inYtd.length ? inYtd[inYtd.length - 1].a : null,
    (beforeYtd.length ? beforeYtd[beforeYtd.length - 1].a : null) ?? (inYtd[0]?.a ?? null),
  )

  // ── 主口径：复权（分红再投）──────────────────────────────────────────────
  const n = rows.length
  const { f } = adjustFactors(rows)
  const perYear = new Map<number, number>()
  let total = 1
  for (let i = 1; i < n; i++) {
    const v = f[i]
    if (!(v > 0)) continue
    total *= v
    const yr = Number(rows[i].d.slice(0, 4))
    perYear.set(yr, (perYear.get(yr) ?? 1) * v)
  }

  const years = [...perYear.entries()]
    .sort((x, y) => x[0] - y[0])
    .map(([year, fac]) => ({ year, ret: round2((fac - 1) * 100) }))
  if (!years.length) {
    return { ytdReturn: null, totalReturn: null, ytdReturnAcc, totalReturnAcc, years }
  }
  const cur = perYear.get(thisYear)

  return {
    ytdReturn:   cur == null ? null : round2((cur - 1) * 100),
    totalReturn: round2((total - 1) * 100),
    ytdReturnAcc,
    totalReturnAcc,
    years,
  }
}

/**
 * 单只基金的全部业绩指标（一次取序列，两套口径一起算）。
 * 货币基金没有净值涨跌幅概念 → 全 null / 空数组。
 */
async function fundReturns(code: string, moneyLike: boolean): Promise<FundReturns> {
  if (moneyLike) {
    return { ytdReturn: null, totalReturn: null, ytdReturnAcc: null, totalReturnAcc: null, years: [] }
  }
  const rows = await db
    .select({ d: fundNav.navDate, u: fundNav.unitNav, a: fundNav.accNav, r: fundNav.dailyReturn })
    .from(fundNav)
    .where(and(eq(fundNav.fundCode, code), sql`${fundNav.navKind} <> 'money'`))
    .orderBy(asc(fundNav.navDate))

  return computeReturns(rows.map(r => ({ d: r.d, u: toNum(r.u), a: toNum(r.a), r: toNum(r.r) })))
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

    // 收益率：主口径 = 复权（分红再投，与雪球/天天基金一致）；副口径 = 累计净值（分红不再投）
    const ret = await fundReturns(code, isMoneyLike(meta.navKind, meta.fundType))

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
      // 今年来涨跌幅 / 成立以来累计收益（%）
      //   主口径 = 复权（分红再投），与雪球 `nav_grbase` 一致，差异 <1%
      //   *Acc  = 累计净值口径（现金分红不再投），分红多的基金会明显低于主口径
      // 货币基金两种口径都没有意义 → 全部 null
      ytdReturn:        ret.ytdReturn,
      totalReturn:      ret.totalReturn,
      ytdReturnAcc:     ret.ytdReturnAcc,
      totalReturnAcc:   ret.totalReturnAcc,
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

  // ── GET /api/funds/:code/nav-summary ──────────────────────────────────────
  // 走势图的区间摘要：区间涨跌幅（复权 / 分红不复投）+ 区间最高最低净值。
  //
  // 为什么不塞进 /nav 的返回：/nav 带分页参数（limit/offset/order），去掉分页算摘要
  // 跟分页取行是两套扫描；而且「全部净值」弹窗翻页时根本不需要摘要。
  //
  // ⚠ 复权因子必须在**全序列**上算：份额折算系数 s 是沿时间轴累积估计的，只喂区间内
  //   的行会把折算前后混成一条曲线（163402 在 2007-05 做过 1:4 折算）。
  //   故这里查全表再按 from 切，而不是给 SQL 加时间下限。
  // Query: days=365（默认近一年，0=全部）| from=YYYY-MM-DD | to=YYYY-MM-DD
  app.get<{
    Params: { code: string }
    Querystring: { days?: string; from?: string; to?: string }
  }>('/api/funds/:code/nav-summary', async (req, reply) => {
    const { code } = req.params
    const days = parseInt(req.query.days ?? '365')

    const [meta] = await db
      .select({ fundCode: funds.fundCode, navKind: funds.navKind })
      .from(funds)
      .where(eq(funds.fundCode, code))
    if (!meta) return reply.code(404).send({ error: `Fund '${code}' not found` })

    const money = meta.navKind === 'money'
    const conds = [eq(fundNav.fundCode, code)]
    if (money) {
      conds.push(sql`(${fundNav.unitNav} <> 0 OR COALESCE(${fundNav.accNav}, 0) <> 0)`)
    }
    const raw = await db
      .select({
        date:        fundNav.navDate,
        nav:         fundNav.unitNav,
        accNav:      fundNav.accNav,
        dailyReturn: fundNav.dailyReturn,
      })
      .from(fundNav)
      .where(and(...conds))
      .orderBy(asc(fundNav.navDate))

    const full: NavPoint[] = raw.map(r => ({
      d: r.date, u: toNum(r.nav), a: toNum(r.accNav), r: toNum(r.dailyReturn),
    }))
    if (!full.length) {
      return {
        fundCode: meta.fundCode, navKind: (meta.navKind ?? null) as NavKind,
        from: null, to: null, baseDate: null, days: 0,
        adjReturn: null, accReturn: null, navMax: null, navMin: null,
      }
    }

    // 区间起点：from 优先，否则按 days 回推（与 /nav 同一套语义）
    const iso = (d: Date) => d.toISOString().slice(0, 10)
    const fromStr = req.query.from
      ?? (Number.isFinite(days) && days > 0 ? iso(new Date(Date.now() - days * 86400000)) : null)
    const i0 = fromStr ? full.findIndex(r => r.d >= fromStr) : 0
    // 区间终点：to 之后的第一行（不含）
    let i1 = full.length
    if (req.query.to) {
      const k = full.findIndex(r => r.d > req.query.to!)
      if (k >= 0) i1 = k
    }
    // 起点落在序列之外（区间早于成立日）时返回空，别把成立以来的算成区间收益
    if (i0 < 0 || i0 >= i1) {
      return {
        fundCode: meta.fundCode, navKind: (meta.navKind ?? null) as NavKind,
        from: null, to: null, baseDate: null, days: 0,
        adjReturn: null, accReturn: null, navMax: null, navMin: null,
      }
    }

    // 复权：对全序列因子在 [i0, i1) 内连乘。
    // ⚠ 起点取 i0 而不是 i0+1 —— f[i] 是「第 i 行相对第 i-1 行」的收益，从 i0 起连乘
    //   等于「以区间首日的**前一个**披露日净值为基数」，也就是把区间首日当天的涨跌算进来。
    //   行业惯例如此（天天基金的「近 3 月」就是 3 个月前最近净值做基数），而且必须这样，
    //   否则「今年来」跟首屏对不上：首屏按自然年分组连乘，2026-01-05 相对 2025-12-31 的
    //   涨幅是算在 2026 里的；摘要若从 01-06 起算就漏掉年初第一天
    //   （110022 实测 -18.53% 对 -19.60%）。i0=0 时 f[0]=1，正好不影响全部历史。
    const adj = money ? null : adjustFactors(full)
    let cum = 1
    if (adj) {
      for (let i = i0; i < i1; i++) {
        const v = adj.f[i]
        if (v > 0) cum *= v
      }
    }

    // ── 分红不复投（累计净值口径）────────────────────────────────────────────
    // 基数行与复权口径一致，取区间首日的**前一个**披露日（j0），否则两个口径起点不同。
    // 首值可能是占位/承继值（见 pickBase），所以要按「首个有效行 + 差值指纹」挑。
    const j0 = i0 > 0 ? i0 - 1 : 0
    const validA = (k: number) => full[k].a != null && full[k].a! > 0
    let jA = j0
    while (jA < i1 && !validA(jA)) jA++
    if (jA < i1 && i1 - jA > 1) {
      let k = jA + 1
      while (k < i1 && !validA(k)) k++
      if (k < i1 && Math.abs(full[k].a! / full[jA].a! - 1) > 0.5) jA = k
    }
    let jB = i1 - 1
    while (jB > jA && !validA(jB)) jB--

    // 收益 = Δ累计净值 / (s_base · u_base)。
    //
    // 为什么不是「累计净值末值/首值」：累计净值 A = s·u + 已分现金 C，s 是份额折算系数。
    // 拿 A 的首末比当区间收益，会把 s 一起带进分母 —— 163402 做过 1:4 折算（s=4），
    // 单位净值跌 1 分累计净值跌 4 分，但分母也被放大 4 倍，相对跌幅被稀释：
    // 近 3 月复权 -13.72%（真实亏损），累计净值口径只有 -4.14%，差 9.6pp，
    // 完全不是「分红没再投少赚了多少」的意思。
    //
    // 恒等式：期末总价值 = s_base·u_base + ΔA
    //   （份额从 s_base 变到 s_end，期末持有 s_end·u_end；累计现金 = c_end - c_base
    //    = ΔA - (s_end·u_end - s_base·u_base)；两者相加，s_end·u_end 恰好抵消）。
    // 所以收益 = ΔA / (s_base · u_base)，与 s 是否变过无关。
    // 自洽性：无分红的区间 ΔA = s·Δu，结果退化成单位净值涨幅；全部历史区间
    // s_base=1、u_base=面值 1.0，结果退化成累计净值口径的成立以来收益（163402 = 978.41%）。
    const uA = full[jA]?.u
    const aA = full[jA]?.a
    const aB = full[jB]?.a
    const sA = adj?.scales[jA] ?? 1
    // 区间恰好覆盖全序列时，这一格在用户眼里就是首屏那个「分红不复投」，
    // 必须一模一样 → 直接用「累计净值末值/首值」。
    // 绝大多数基金成立首日 acc = unit = 1.0，两式本就相等；只有 001885 这类
    // 源侧序列不从面值起算的（首日 u=1.5676 / a=2.2776）才有差：那种情况下
    // "净值序列涨幅"才是用户认得的那个数，而"从首日净值买入的收益"反而对不上。
    const isFullRange = i0 === 0 && i1 === full.length
    const accReturn = money || uA == null || uA <= 0 || aA == null || aB == null || !(sA > 0)
      ? null
      : isFullRange
        ? pctOf(aB, aA)
        : round2(((aB - aA) / (sA * uA)) * 100)

    // 区间最高最低：只看单位净值（累计净值含已分掉的现金，不是可比的"高低水位"）
    let navMax: { v: number; d: string } | null = null
    let navMin: { v: number; d: string } | null = null
    if (!money) {
      for (let i = i0; i < i1; i++) {
        const v = full[i].u
        if (v == null || v <= 0) continue
        if (!navMax || v > navMax.v) navMax = { v, d: full[i].d }
        if (!navMin || v < navMin.v) navMin = { v, d: full[i].d }
      }
    }

    return {
      fundCode: meta.fundCode,
      navKind: (meta.navKind ?? null) as NavKind,
      from: full[i0].d,
      to: full[i1 - 1].d,
      /** 涨跌幅的基数日：区间首日的**前一个**披露日（区间首日无前一日时 = from） */
      baseDate: full[jA]?.d ?? full[j0].d,
      days: i1 - i0,
      adjReturn: money || !(cum > 0) ? null : round2((cum - 1) * 100),
      accReturn,
      navMax,
      navMin,
    }
  })

  // ── GET /api/funds/:code/yearly ───────────────────────────────────────────
  // 历史业绩：每个自然年的涨跌幅（%），按年份降序。
  // 口径 = **复权净值（分红再投）**，与首屏「累计收益」同源，也与雪球/天天基金的年度榜一致。
  // 首个年度即「有数据以来」——该年第一个净值只当基数、不产生收益。
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

    const list = isMoneyLike(meta.navKind, meta.fundType)
      ? []
      : (await fundReturns(code, false)).years

    return {
      fundCode: meta.fundCode,
      navKind:  (meta.navKind ?? null) as NavKind,
      total:    list.length,
      // 升序算、降序出（前端表格按年份倒序展示）
      items: list.slice(-limit).reverse(),
    }
  })
}
