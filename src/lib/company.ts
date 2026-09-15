import { db } from '../db'
import { sql } from 'drizzle-orm'

// ---------------------------------------------------------------------------
// 基金公司名规范化
// ---------------------------------------------------------------------------
// funds.fund_company 的原始值来自雪球 keeper_name，同一家公司在库里存在多种
// 写法，直接 GROUP BY 会把一家公司拆成好几个。实测 178 个原始值里有 30 多个
// 是这样的碎片，例如：
//
//   中欧基金公司 / 中欧基金管理公司 / 中欧基金管理有限公司     → 中欧基金
//   鹏华基金公司 / 鹏华基金管理公司 / 鹏华基金管理有限公司     → 鹏华基金
//   长城基金公司 / 长城基金管理公司 / 长城基金管理有限公司     → 长城基金
//   摩根基金管理(中国)有限公司 / 摩根基金管理（中国）有限公司  → 摩根基金(中国)
//   华泰证券(上海)资产管理有限公司 / ...（全角括号）            → 华泰证券资管(上海)
//
// canonicalCompany() 只归并「同一法定名称的书写差异」（公司形式后缀、全角/
// 半角括号、空白），**不做更名合并**（如 上投摩根 → 摩根、中融 → 国联），
// 那属于实体消歧，需要人工确认，自动合并风险太大。
//
// 返回值同时用作「分组键」和「URL key」。
// ---------------------------------------------------------------------------

/** 公司形式后缀 → 归一化类型词。顺序敏感：基金类在前，证券资产管理在资产管理之前 */
const FORM_RULES: Array<[RegExp, string]> = [
  [/^(.+?)基金管理(?:股份)?(?:有限)?(?:责任)?公司$/, '基金'],
  [/^(.+?)基金管理公司$/,                         '基金'],
  [/^(.+?)基金有限公司$/,                         '基金'],
  [/^(.+?)基金公司$/,                             '基金'],
  [/^(.+?)基金$/,                                 '基金'],
  [/^(.+?)证券资产管理(?:有限)?(?:责任)?公司$/,   '证券资管'],
  [/^(.+?)资产管理(?:有限)?(?:责任)?公司$/,       '资管'],
  [/^(.+?)证券(?:股份)?(?:有限)?(?:责任)?公司$/,  '证券'],
  [/^(.+?)证券$/,                                 '证券'],
]

/** 原始公司名 → 规范化短名（分组键）。空值返回空串。 */
export function canonicalCompany(raw: string | null | undefined): string {
  if (!raw) return ''
  let s = String(raw).replace(/[\s\u3000]+/g, '')
  if (!s) return ''
  // 全角括号统一为半角，避免 摩根基金管理(中国) 被拆成两家
  s = s.replace(/（/g, '(').replace(/）/g, ')')

  // 抽出括号限定词（(中国) / (上海) …），归一化后拼回末尾，
  // 这样 华泰证券(上海)资产管理有限公司 也能落到 华泰证券资管(上海)
  const parens = (s.match(/\([^)]*\)/g) ?? []).join('')
  const base = s.replace(/\([^)]*\)/g, '')

  for (const [re, kind] of FORM_RULES) {
    const m = base.match(re)
    if (m) return m[1] + kind + parens
  }
  return base + parens
}

// ---------------------------------------------------------------------------
// 聚合（带内存缓存）
// ---------------------------------------------------------------------------
// 公司维度的统计全部来自 funds / fund_holdings 两张表，数据一天最多变一次
// （fetch_funds.py 跑完才动），但页面上每次翻页、筛选都会请求，所以在内存里
// 缓存聚合结果。明细列表（产品）仍然实时查库。
// ---------------------------------------------------------------------------

const CACHE_TTL_MS = 120_000

export type CompanyTypeBreakdown = {
  fundType: string
  count: number
  scale: number
}

export type CompanyTopFund = {
  fundCode: string
  fundName: string
  scale: number | null
}

export type CompanySummary = {
  /** 规范化短名，同时是 URL key，如「中欧基金」 */
  key: string
  /** 展示名（= key） */
  name: string
  /** 库里最完整的法定名，如「中欧基金管理有限公司」 */
  fullName: string
  /** 归并到该公司的全部原始写法 */
  aliases: string[]
  fundCount: number
  /** 有规模数据的产品数 */
  scaleCovered: number
  /** 合计规模（亿元），仅累加有规模数据的产品 */
  totalScale: number
  /** 有股票持仓明细的产品数 */
  holdingsCovered: number
  managerCount: number
  earliestInception: string | null
  types: CompanyTypeBreakdown[]
  /** 代表产品：规模前 3（货币型排最后，避免每家公司都是货币基金） */
  topFunds: CompanyTopFund[]
}

export type UnassignedStat = {
  /** 未采集到基金公司的产品数 */
  fundCount: number
  /** 未采集到基金公司的产品中，已采集到规模的合计（亿元） */
  totalScale: number
}

type CacheShape = {
  at: number
  list: CompanySummary[]
  byKey: Map<string, CompanySummary>
  byAlias: Map<string, CompanySummary>
  unassigned: UnassignedStat
}

let cache: CacheShape | null = null

const num = (v: unknown): number => {
  if (v == null) return 0
  const n = typeof v === 'number' ? v : parseFloat(String(v))
  return Number.isFinite(n) ? n : 0
}

type RawAggRow = {
  fund_company: string
  fund_count: number
  scale_covered: number
  total_scale: number
  manager_count: number
  earliest: string | null
}

async function buildCache(): Promise<CacheShape> {
  // 1) 按原始公司名的聚合
  const aggQ = db.execute<RawAggRow>(sql`
    SELECT fund_company,
           COUNT(*)::int                                   AS fund_count,
           COUNT(scale)::int                               AS scale_covered,
           COALESCE(SUM(scale), 0)::float8                 AS total_scale,
           COUNT(DISTINCT fund_manager)::int               AS manager_count,
           MIN(inception_date)::text                       AS earliest
    FROM funds
    WHERE fund_company IS NOT NULL AND fund_company <> ''
    GROUP BY fund_company
  `)

  // 2) 类型分布（原始公司名 × 类型）
  const typeQ = db.execute<{
    fund_company: string
    fund_type: string | null
    c: number
    s: number
  }>(sql`
    SELECT fund_company,
           fund_type,
           COUNT(*)::int                   AS c,
           COALESCE(SUM(scale), 0)::float8 AS s
    FROM funds
    WHERE fund_company IS NOT NULL AND fund_company <> ''
    GROUP BY fund_company, fund_type
  `)

  // 3) 每家公司的「代表产品」：规模前 3，但货币型排在最后。
  //    纯按规模排的话，几乎所有公司的前三都是货币基金（易方达增金宝、广发天天红…），
  //    列表里每行看起来都一样、也没有辨识度。货币型只是排在后面而非剔除，
  //    所以只有货币产品的公司仍会正常返回货币基金。
  const topQ = db.execute<{
    fund_company: string
    fund_code: string
    fund_name: string
    scale: number | null
    is_money: boolean
  }>(sql`
    SELECT fund_company, fund_code, fund_name, scale, is_money FROM (
      SELECT fund_company, fund_code, fund_name, scale, is_money,
             ROW_NUMBER() OVER (
               PARTITION BY fund_company
               ORDER BY is_money, scale DESC NULLS LAST, fund_code
             ) AS rn
      FROM (
        SELECT fund_company, fund_code, fund_name, scale,
               COALESCE(fund_type, '') LIKE '货币型%' AS is_money
        FROM funds
        WHERE fund_company IS NOT NULL AND fund_company <> ''
      ) x
    ) t WHERE rn <= 3
  `)

  // 4) 有股票持仓明细的产品数（按原始公司名）
  const holdQ = db.execute<{ fund_company: string; c: number }>(sql`
    WITH st AS (
      SELECT DISTINCT fund_code FROM fund_holdings WHERE holding_type = 'stock'
    )
    SELECT f.fund_company, COUNT(*)::int AS c
    FROM funds f JOIN st ON st.fund_code = f.fund_code
    WHERE f.fund_company IS NOT NULL AND f.fund_company <> ''
    GROUP BY f.fund_company
  `)

  // 5) 未归属任何公司的产品（雪球对「暂不销售」的基金不返回 keeper_name）
  const unQ = db.execute<{ c: number; s: number }>(sql`
    SELECT COUNT(*)::int                   AS c,
           COALESCE(SUM(scale), 0)::float8 AS s
    FROM funds
    WHERE fund_company IS NULL OR fund_company = ''
  `)

  const [aggR, typeR, topR, holdR, unR] = await Promise.all([aggQ, typeQ, topQ, holdQ, unQ])

  // ── 按规范化 key 归并 ────────────────────────────────────────────────────
  const map = new Map<string, CompanySummary>()

  const ensure = (raw: string): CompanySummary | null => {
    const key = canonicalCompany(raw)
    if (!key) return null
    let c = map.get(key)
    if (!c) {
      c = {
        key,
        name: key,
        fullName: raw,
        aliases: [],
        fundCount: 0,
        scaleCovered: 0,
        totalScale: 0,
        holdingsCovered: 0,
        managerCount: 0,
        earliestInception: null,
        types: [],
        topFunds: [],
      }
      map.set(key, c)
    }
    return c
  }

  for (const r of aggR.rows) {
    const c = ensure(r.fund_company)
    if (!c) continue
    c.aliases.push(r.fund_company)
    c.fundCount += num(r.fund_count)
    c.scaleCovered += num(r.scale_covered)
    c.totalScale += num(r.total_scale)
    c.managerCount += num(r.manager_count)   // 同一经理跨写法可能重复计数，仅作展示
    const e = r.earliest
    if (e && (c.earliestInception === null || e < c.earliestInception)) {
      c.earliestInception = e
    }
    // fullName 取最长的原始写法：书写碎片总是短于完整法定名。
    // 比较与展示都用去掉空白的形式（库里存在「长城基金管理有限公司\u3000」
    // 这种带全角尾空格的脏值；aliases 仍保留原值，SQL 要用它精确匹配）
    const display = r.fund_company.replace(/[\s\u3000]+/g, '')
    if (display.length > c.fullName.length) c.fullName = display
  }

  for (const r of typeR.rows) {
    const c = ensure(r.fund_company)
    if (!c) continue
    const t = r.fund_type ?? '未分类'
    const hit = c.types.find(x => x.fundType === t)
    if (hit) {
      hit.count += num(r.c)
      hit.scale += num(r.s)
    } else {
      c.types.push({ fundType: t, count: num(r.c), scale: num(r.s) })
    }
  }

  // topFunds 是「候选池」（每家公司 ≤3 条），下面按 key 归并后再统一排序，
  // 所以这里先把排序依据（is_money）挂在临时结构上，最后一步再转成公开类型。
  const topBuf = new Map<string, Array<CompanyTopFund & { isMoney: boolean }>>()
  for (const r of topR.rows) {
    const c = ensure(r.fund_company)
    if (!c) continue
    const bucket = topBuf.get(c.key) ?? []
    bucket.push({
      fundCode: r.fund_code,
      fundName: r.fund_name,
      scale: r.scale == null ? null : num(r.scale),
      isMoney: r.is_money === true,
    })
    topBuf.set(c.key, bucket)
  }

  for (const r of holdR.rows) {
    const c = ensure(r.fund_company)
    if (c) c.holdingsCovered += num(r.c)
  }

  const list = [...map.values()]
  for (const c of list) {
    c.types.sort((a, b) => b.scale - a.scale || b.count - a.count)
    // 非货币优先，再按规模降序 —— 必须与 SQL 里的 ROW_NUMBER 排序一致，
    // 否则这行 JS 排序会把 SQL 的「货币型排最后」直接覆盖掉
    c.topFunds = (topBuf.get(c.key) ?? [])
      .sort((a, b) =>
        (a.isMoney ? 1 : 0) - (b.isMoney ? 1 : 0)
        || (b.scale ?? -1) - (a.scale ?? -1)
        || a.fundCode.localeCompare(b.fundCode))
      .slice(0, 3)
      .map(({ isMoney: _isMoney, ...rest }) => rest)
    c.aliases.sort((a, b) => b.length - a.length)
  }
  list.sort((a, b) => b.totalScale - a.totalScale || b.fundCount - a.fundCount)

  const byKey = new Map<string, CompanySummary>()
  const byAlias = new Map<string, CompanySummary>()
  for (const c of list) {
    byKey.set(c.key, c)
    for (const a of c.aliases) byAlias.set(a, c)
  }

  return {
    at: Date.now(),
    list,
    byKey,
    byAlias,
    unassigned: {
      fundCount: num(unR.rows[0]?.c),
      totalScale: num(unR.rows[0]?.s),
    },
  }
}

/** 取聚合缓存（超过 TTL 才重建）。并发请求只触发一次构建。 */
let inflight: Promise<CacheShape> | null = null

export async function getCompanyCache(): Promise<CacheShape> {
  if (cache && Date.now() - cache.at < CACHE_TTL_MS) return cache
  if (inflight) return inflight
  inflight = buildCache()
    .then(c => { cache = c; return c })
    .finally(() => { inflight = null })
  return inflight
}

/** key（或原始公司名）→ 公司汇总。找不到返回 null。 */
export async function findCompany(key: string): Promise<CompanySummary | null> {
  const c = await getCompanyCache()
  const raw = decodeURIComponent(key ?? '').trim()
  if (!raw) return null

  // 先按 URL key 匹配，再按原始写法兜底（用户可能直接粘了库里的全名）
  return c.byKey.get(raw) ?? c.byAlias.get(raw) ?? c.byKey.get(canonicalCompany(raw)) ?? null
}
