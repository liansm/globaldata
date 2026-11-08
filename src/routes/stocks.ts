import { FastifyInstance } from 'fastify'
import { sql, SQL } from 'drizzle-orm'
import { db } from '../db'
import { stocks, stockPrices } from '../db/schema'

// ⚠ numeric 列在 pg 驱动里返回**字符串**，必须在 API 层转 number，
//   否则前端 toFixed / 算术直接崩（本项目踩过多次）
const toNum = (v: unknown): number | null => (v == null ? null : Number(v))
const day = (v: unknown): string | null => (v == null ? null : String(v).slice(0, 10))

/** 与 Python fetch_stock_info.py::norm_name 同规则：NFKC + 去空白 + 大写 */
function normName(s: string): string {
  return s.normalize('NFKC').replace(/\s+/g, '').toUpperCase()
}

const EXCHANGES = ['上交所', '深交所']
const BOARDS = ['沪市主板', '科创板', '深市主板', '创业板']
const PERIODS: Record<string, string> = {
  day: 'day', week: 'week', month: 'month', year: 'year',
}

type StockRow = Record<string, unknown>

function mapStock(r: StockRow) {
  return {
    code:             r.code,
    name:             r.name,
    exchange:         r.exchange,
    board:            r.board,
    region:           r.region,
    industry:         r.industry,
    industryCsrc:     r.industry_csrc,
    totalShares:      toNum(r.total_shares),
    floatShares:      toNum(r.float_shares),
    marketCap:        toNum(r.market_cap),
    floatMarketCap:   toNum(r.float_market_cap),
    // 财务快照（列表接口才有；详情接口走独立的 fundamentals 对象）
    peTtm:            toNum(r.pe_ttm),
    dividendYield:    toNum(r.dividend_yield),
    latestPrice:      toNum(r.latest_price),
    changePct:        toNum(r.change_pct),
    quoteDate:        day(r.quote_date),
    latestPriceDate:  day(r.latest_price_date),
    listingDate:      day(r.listing_date),
  }
}

/** 每股财务指标快照。numeric 列同样要转 number；没有记录时整个对象为 null。 */
function mapFundamentals(r: StockRow | undefined) {
  if (!r) return null
  return {
    epsTtm:          toNum(r.eps_ttm),
    epsReport:       toNum(r.eps_report),
    reportDate:      day(r.report_date),
    peTtm:           toNum(r.pe_ttm),
    peDynamic:       toNum(r.pe_dynamic),
    peStatic:        toNum(r.pe_static),
    bvps:            toNum(r.bvps),
    pb:              toNum(r.pb),
    dividend:        toNum(r.dividend),
    dividendYield:   toNum(r.dividend_yield),
    dividendPlans:   r.dividend_plans == null ? null : Number(r.dividend_plans),
    dividendLastEx:  day(r.dividend_last_ex),
    price:           toNum(r.price),
    quoteDate:       day(r.quote_date),
    source:          r.source,
    updatedAt:       r.updated_at,
  }
}

function mapDetail(r: StockRow) {
  return {
    ...mapStock(r),
    companyName:       r.company_name,
    nameEn:            r.name_en,
    formerName:        r.former_name,
    introduction:      r.introduction,
    mainBusiness:      r.main_business,
    businessScope:     r.business_scope,
    registeredCapital: toNum(r.registered_capital),
    foundedDate:       day(r.founded_date),
    legalPerson:       r.legal_person,
    registeredAddress: r.registered_address,
    officeAddress:     r.office_address,
    website:           r.website,
    email:             r.email,
    phone:             r.phone,
    updatedAt:         r.updated_at,
  }
}

export async function stocksRoutes(app: FastifyInstance) {

  // ── GET /api/stocks/filters ──────────────────────────────────────────────
  // 下拉选项。交易所/板块是**固定枚举**（代码前缀分类，不随数据漂移）；
  // 地域/行业/标签从库里去重，没跑抓取时自然返回空数组。
  app.get('/api/stocks/filters', async () => {
    const [regions, industries, tags, total] = await Promise.all([
      db.execute(sql`SELECT DISTINCT region FROM stocks
                     WHERE region IS NOT NULL ORDER BY region`),
      db.execute(sql`SELECT DISTINCT industry_csrc FROM stocks
                     WHERE industry_csrc IS NOT NULL ORDER BY industry_csrc`),
      db.execute(sql`SELECT name, COUNT(DISTINCT code)::int AS n
                     FROM stock_industries
                     WHERE kind IN ('industry', 'concept')
                     GROUP BY name HAVING COUNT(DISTINCT code) >= 3
                     ORDER BY n DESC, name LIMIT 400`),
      db.execute(sql`SELECT COUNT(*)::int AS n FROM stocks`),
    ])
    return {
      exchanges: EXCHANGES,
      boards: BOARDS,
      regions: (regions.rows as StockRow[]).map(r => r.region),
      industries: (industries.rows as StockRow[]).map(r => r.industry_csrc),
      tags: (tags.rows as StockRow[]).map(r => ({ name: r.name, count: r.n })),
      total: (total.rows as StockRow[])[0]?.n ?? 0,
    }
  })

  // ── GET /api/stocks ──────────────────────────────────────────────────────
  // 代码 / 名称搜索 + 交易所 / 板块 / 地域 / 行业 / 标签过滤 + 分页。
  // 过滤用 `${param}::text IS NULL OR ...` 的参数化写法，避免拼 SQL 字符串。
  app.get<{
    Querystring: {
      q?: string; exchange?: string; board?: string; region?: string
      industry?: string; tag?: string; sort?: string
      page?: string; pageSize?: string
    }
  }>('/api/stocks', async (req) => {
    const q        = (req.query.q || '').trim() || null
    const exchange = (req.query.exchange || '').trim() || null
    const board    = (req.query.board || '').trim() || null
    const region   = (req.query.region || '').trim() || null
    const industry = (req.query.industry || '').trim() || null
    const tag      = (req.query.tag || '').trim() || null
    const sort     = req.query.sort || 'code'
    const page     = Math.max(1, parseInt(req.query.page ?? '1', 10) || 1)
    const pageSize = Math.min(200, Math.max(1, parseInt(req.query.pageSize ?? '20', 10) || 20))
    const offset   = (page - 1) * pageSize
    const likeQ    = q ? `%${q}%` : null
    const likeN    = q ? `%${normName(q)}%` : null

    const cond: SQL = sql`
        (${q}::text IS NULL OR s.code LIKE ${q}::text || '%'
              OR s.name ILIKE ${likeQ}::text OR s.name_search LIKE ${likeN}::text)
    AND (${exchange}::text IS NULL OR s.exchange = ${exchange})
    AND (${board}::text    IS NULL OR s.board    = ${board})
    AND (${region}::text   IS NULL OR s.region   = ${region})
    AND (${industry}::text IS NULL OR s.industry_csrc = ${industry})
    AND (${tag}::text IS NULL OR EXISTS (
          SELECT 1 FROM stock_industries si
          WHERE si.code = s.code AND si.name = ${tag}))
    `

    const [list, cnt] = await Promise.all([
      db.execute(sql`
        SELECT s.code, s.name, s.exchange, s.board, s.region, s.industry,
               s.industry_csrc, s.total_shares, s.float_shares,
               s.market_cap, s.float_market_cap, s.latest_price, s.change_pct,
               s.quote_date, s.latest_price_date, s.listing_date,
               f.pe_ttm AS pe_ttm, f.dividend_yield AS dividend_yield
        FROM ${stocks} s
        LEFT JOIN stock_fundamentals f ON f.code = s.code
        WHERE ${cond}
        ORDER BY CASE WHEN ${sort} = 'marketCap' THEN s.market_cap END DESC NULLS LAST,
                 CASE WHEN ${sort} = 'changePct' THEN s.change_pct END DESC NULLS LAST,
                 s.code ASC
        LIMIT ${pageSize} OFFSET ${offset}
      `),
      db.execute(sql`SELECT COUNT(*)::int AS n FROM ${stocks} s WHERE ${cond}`),
    ])
    return {
      rows: (list.rows as StockRow[]).map(mapStock),
      total: (cnt.rows as StockRow[])[0]?.n ?? 0,
      page, pageSize,
    }
  })

  // ── GET /api/stocks/:code ────────────────────────────────────────────────
  // ⚠ 详情接口一律 200 + {found:false}（别返 404：axios 对 404 直接抛异常 →
  //   前端会误报「加载失败，请检查后端服务」）。并补齐 rows/total/page/pageSize。
  app.get<{ Params: { code: string } }>('/api/stocks/:code', async (req) => {
    const code = req.params.code.trim()
    const empty = {
      found: false, stock: null, industries: { industry: [], concept: [], region: [] },
      priceStats: null, fundamentals: null, rows: [], total: 0, page: 1, pageSize: 20,
    }
    const res = await db.execute(sql`SELECT * FROM ${stocks} WHERE code = ${code} LIMIT 1`)
    const row = (res.rows as StockRow[])[0]
    if (!row) return empty

    const [tags, stat, fin] = await Promise.all([
      db.execute(sql`SELECT kind, name, board_code FROM stock_industries
                     WHERE code = ${code} ORDER BY kind, name`),
      db.execute(sql`SELECT COUNT(*)::int AS n,
                            MIN(trade_date)::text AS earliest,
                            MAX(trade_date)::text AS latest
                     FROM stock_prices WHERE code = ${code}`),
      db.execute(sql`SELECT * FROM stock_fundamentals WHERE code = ${code} LIMIT 1`),
    ])
    const grouped: { industry: string[]; concept: string[]; region: string[] } = {
      industry: [], concept: [], region: [],
    }
    for (const t of tags.rows as StockRow[]) {
      const k = String(t.kind)
      if (k === 'industry' || k === 'concept' || k === 'region') grouped[k].push(String(t.name))
    }
    const s = (stat.rows as StockRow[])[0] ?? {}
    return {
      found: true,
      stock: mapDetail(row),
      industries: grouped,
      priceStats: { rows: s.n ?? 0, earliest: s.earliest ?? null, latest: s.latest ?? null },
      fundamentals: mapFundamentals((fin.rows as StockRow[])[0]),
      rows: [], total: 0, page: 1, pageSize: 20,
    }
  })

  // ── GET /api/stocks/:code/prices ─────────────────────────────────────────
  // 日 / 周 / 月 / 年 K 线。周月年**不落库**，直接由日线聚合：
  //   open = 区间首个非空开盘，close = 区间最后一个非空收盘，
  //   high/low = 区间极值，volume/turnover = 区间求和。
  // ⚠ date_trunc('week') 以周一为起点，与国内周线口径一致。
  app.get<{
    Params: { code: string }
    Querystring: { period?: string; from?: string; to?: string; limit?: string }
  }>('/api/stocks/:code/prices', async (req) => {
    const code   = req.params.code.trim()
    const period = PERIODS[req.query.period || 'day'] ?? 'day'
    const from   = req.query.from || '1990-01-01'
    const to     = req.query.to || new Date().toISOString().slice(0, 10)
    const limit  = Math.min(8000, Math.max(1, parseInt(req.query.limit ?? '1200', 10) || 1200))

    let res
    if (period === 'day') {
      res = await db.execute(sql`
        SELECT * FROM (
          SELECT trade_date, open, high, low, close, volume, turnover,
                 amplitude, change_pct, change_amt, turnover_rate
          FROM ${stockPrices}
          WHERE code = ${code} AND trade_date >= ${from}::date AND trade_date <= ${to}::date
          ORDER BY trade_date DESC LIMIT ${limit}
        ) t ORDER BY trade_date ASC
      `)
    } else {
      res = await db.execute(sql`
        SELECT * FROM (
          SELECT date_trunc(${period}, trade_date::timestamp)::date AS trade_date,
                 (array_agg(open  ORDER BY trade_date) FILTER (WHERE open  IS NOT NULL))[1] AS open,
                 (array_agg(close ORDER BY trade_date DESC) FILTER (WHERE close IS NOT NULL))[1] AS close,
                 MAX(high) AS high,
                 MIN(low)  AS low,
                 SUM(volume)   AS volume,
                 SUM(turnover) AS turnover
          FROM ${stockPrices}
          WHERE code = ${code} AND trade_date >= ${from}::date AND trade_date <= ${to}::date
          GROUP BY 1
          ORDER BY 1 DESC LIMIT ${limit}
        ) t ORDER BY trade_date ASC
      `)
    }
    const rows = (res.rows as StockRow[]).map(r => ({
      date:         day(r.trade_date),
      open:         toNum(r.open),
      high:         toNum(r.high),
      low:          toNum(r.low),
      close:        toNum(r.close),
      volume:       toNum(r.volume),
      turnover:     toNum(r.turnover),
      amplitude:    toNum(r.amplitude),
      changePct:    toNum(r.change_pct),
      changeAmt:    toNum(r.change_amt),
      turnoverRate: toNum(r.turnover_rate),
    }))
    return { period, code, rows, total: rows.length, page: 1, pageSize: limit }
  })

  // ── GET /api/stocks/:code/reports ─────────────────────────────────────────
  // 季度财报 PDF 链接，按**报告期**倒序：2026 Q3 → 2026 H1 → 2026 Q1 → 2025 FY …
  // ⚠ report_year 是报告期年份（「2025年年度报告」2026 年才披露），不是披露年份。
  //    只返回链接字符串，后端不下载、不代理 PDF。
  app.get<{
    Params: { code: string }
    Querystring: { quarter?: string; limit?: string }
  }>('/api/stocks/:code/reports', async (req) => {
    const code = req.params.code.trim()
    const limit = Math.min(200, Math.max(1, parseInt(req.query.limit ?? '100', 10) || 100))
    const quarter = req.query.quarter?.trim() || null
    const where = sql`code = ${code} AND (${quarter}::text IS NULL OR quarter = ${quarter})`
    const [res, cnt] = await Promise.all([
      db.execute(sql`
        SELECT code, exchange, report_year, quarter, quarter_num, title,
               pdf_url, publish_date, source
        FROM stock_reports
        WHERE ${where}
        ORDER BY report_year DESC, quarter_num DESC, publish_date DESC NULLS LAST
        LIMIT ${limit}
      `),
      db.execute(sql`SELECT COUNT(*)::int AS n FROM stock_reports WHERE ${where}`),
    ])
    const rows = (res.rows as StockRow[]).map(r => ({
      code:         r.code,
      exchange:     r.exchange,
      reportYear:   r.report_year == null ? null : Number(r.report_year),
      quarter:      r.quarter,
      quarterNum:   r.quarter_num == null ? null : Number(r.quarter_num),
      title:        r.title,
      pdfUrl:       r.pdf_url,
      publishDate:  day(r.publish_date),
      source:       r.source,
    }))
    return {
      code, rows,
      total: (cnt.rows as StockRow[])[0]?.n ?? rows.length,
      page: 1, pageSize: limit,
    }
  })
}
