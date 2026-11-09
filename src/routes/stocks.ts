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
    updatedAt:         r.updated_at_text ?? null,   // 北京时间 'YYYY-MM-DD HH:mm:ss'
    quoteTime:         r.quote_time_text ?? null,   // 源侧行情时刻，同格式
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
    // LIKE 通配符统一转义（ESCAPE '!'）：否则输入 % 或 _ 会让 code/name 条件命中全表。
    const escLike  = (s: string) => s.replace(/[!%_]/g, c => `!${c}`)
    const codeQ    = q ? escLike(q) : null
    const likeQ    = q ? `%${escLike(q)}%` : null
    const likeN    = q ? `%${escLike(normName(q))}%` : null
    // 拼音搜索两列：name_pinyin = 东财首字母缩写（ZSLC），
    // name_pinyin_full = 本地生成的全拼（ZHAOSHANGLUNCHUAN，逐字用东财权威首字母
    // 约束多音字）。两者都按**前缀**匹配，所以 zs / zhaos / zhaoshang 都能命中招商系。
    // 仅纯字母数字输入启用，中文/符号查询不白扫拼音列，也带不进 % / _ 通配符。
    const pinyinQ  = q && /^[A-Za-z0-9]+$/.test(q) ? q.toUpperCase() : null

    const cond: SQL = sql`
        (${q}::text IS NULL OR s.code LIKE ${codeQ}::text || '%' ESCAPE '!'
              OR s.name ILIKE ${likeQ}::text ESCAPE '!'
              OR s.name_search LIKE ${likeN}::text ESCAPE '!'
              OR (${pinyinQ}::text IS NOT NULL
                  AND (s.name_pinyin LIKE ${pinyinQ}::text || '%'
                       OR s.name_pinyin_full LIKE ${pinyinQ}::text || '%')))
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
        -- 拼音完整命中（简拼 ZSLC 或全拼 ZHAOSHANGLUNCHUAN）优先置顶；
        -- 其余仍按选定排序。未回填的行两列均为 NULL，不影响其位置。
        ORDER BY CASE WHEN ${pinyinQ}::text IS NOT NULL
                           AND (s.name_pinyin = ${pinyinQ}::text
                                OR s.name_pinyin_full = ${pinyinQ}::text) THEN 0 ELSE 1 END,
                 CASE WHEN ${sort} = 'marketCap' THEN s.market_cap END DESC NULLS LAST,
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
    // ⚠ timestamptz 别丢给 JS 算本地时间：在 SQL 里按北京时间格式化成字符串，
    //   避免部署环境 / 浏览器时区漂移（本项目踩过 date 列被 +8h 推到第二天的坑）。
    //   两个时间**口径不同**，别混用：
    //     updated_at  = fetch_stock_info.py 名录 UPSERT 的写入时刻（我们几点抓的）
    //     quote_time  = 东财 clist f124 源侧行情时刻（源侧行情走到几点）
    //   活跃股两者只差几秒；339 只停牌 / 退市 / ST 股源侧给当日 08:00:00 占位时钟，
    //   **不是**最后成交时刻，不能拿它判断最后交易日。
    const res = await db.execute(sql`
      SELECT *, to_char(updated_at AT TIME ZONE 'Asia/Shanghai', 'YYYY-MM-DD HH24:MI:SS')
               AS updated_at_text,
               to_char(quote_time AT TIME ZONE 'Asia/Shanghai', 'YYYY-MM-DD HH24:MI:SS')
               AS quote_time_text
      FROM ${stocks} WHERE code = ${code} LIMIT 1
    `)
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

  // ── GET /api/stocks/:code/holdings ────────────────────────────────────────
  // 反查「哪些公募基金持有这只股票」，按**持股数（万股）降序**。数据来自
  // fetch_funds.py 的 fund_holdings（东财 F10 季报披露的重仓股）。
  //
  // ⚠ 口径（前端也要如实标注，别粉饰）：
  //   * 季报只披露**前十大重仓股**，半年报/年报才可能给完整持仓 → 不是全部持仓。
  //   * shares 单位是**万股**、market_value 是**万元**、ratio 是占净值比例 %。
  //     已交叉验证：2026-09-30 招商轮船 market_value/shares = 20.15 = 当日收盘价。
  //   * funds 名录是**份额口径**（A/C 类分开），所以同一产品的 A/C 份额会各占一行；
  //     汇总里的 funds 是「基金份额只数」，不是基金产品只数。
  //   * **只统计季度末**（03-31 / 06-30 / 09-30 / 12-31）。源侧 F10 另有 2630 行
  //     （267 只基金 / 141 个日期、占全库 0.18%）带的是**具体日期标签**而非季度标签，
  //     96.2% 来自指数型-股票基金（新建 ETF 的建仓/上市组合快照），
  //     fetch_funds.py::get_holdings 优先取标签里的 YYYY-MM-DD 就会落进库。
  //     它们与季报口径不同，混进报告期下拉会让人误以为是季度数据 ⇒ 全部排除，
  //     被忽略的行数由 ignoredNonQuarter 返回，前端明示，不静默吞。
  //   * **名称闸门**：东财把港股代码补零成 6 位，与深市 A 股撞号 —— 实测 251 个
  //     A 股代码在 fund_holdings 里同时挂着港股名称（000020 深华发A 仅 2 条，
  //     同代码「商汤-W」163 条；000670 盈方微底下 163 条全是「中国东方航空股份」）。
  //     只按 security_code 反查会把港股持仓算到 A 股头上。⇒ 只保留 security_name
  //     与 stocks.name 归一化后一致的行。代价是改名前的旧名（*ST柳化 ← 柳化股份）
  //     也会被排除，宁缺勿错；被排除的名称与行数一并返回，不静默吞。
  //     ⚠ 例外：带 -U/-W/-UW/-WD 后缀的简称（全库 47 只）季报用去后缀名，
  //       闸门额外接受「去后缀别名」（九号公司-WD ← 九号公司），matchedAlias 标注。
  //       实测这些代码下只有 1 个 security_name，放宽后 0 个多命中，不会串入港股。
  // ⚠ 详情类接口一律 200 + {found:false}（别返 404，axios 会抛异常），
  //   并补齐 rows/total/page/pageSize，前端必须显式分支。
  app.get<{
    Params: { code: string }
    Querystring: { reportDate?: string; page?: string; pageSize?: string }
  }>('/api/stocks/:code/holdings', async (req) => {
    const code     = req.params.code.trim()
    const page     = Math.max(1, parseInt(req.query.page ?? '1', 10) || 1)
    const pageSize = Math.min(200, Math.max(1, parseInt(req.query.pageSize ?? '20', 10) || 20))
    const offset   = (page - 1) * pageSize

    // 先定「这个代码下哪些 security_name 才是本股票」，后面报告期 / 列表 / 汇总
    // 全部套用同一闸门，避免报告期列表也被港股记录主导。名称候选本身也只看季末，
    // 免得某只股票只有非季末的旧名记录时被误判成可查。
    const quarterMd = sql`'03-31','06-30','09-30','12-31'`
    const [nameRes, stockRes, ignoredRes] = await Promise.all([
      db.execute(sql`
        SELECT security_name, COUNT(*)::int AS n
        FROM fund_holdings
        WHERE security_code = ${code} AND holding_type = 'stock'
          AND security_name IS NOT NULL
          AND to_char(report_date, 'MM-DD') IN (${quarterMd})
        GROUP BY 1 ORDER BY 2 DESC
      `),
      db.execute(sql`SELECT name FROM ${stocks} WHERE code = ${code} LIMIT 1`),
      // 被排除的非季末记录数（含源侧 ETF 建仓 / 临时披露），仅用于透明展示
      db.execute(sql`
        SELECT COUNT(*)::int AS n FROM fund_holdings
        WHERE security_code = ${code} AND holding_type = 'stock'
          AND to_char(report_date, 'MM-DD') NOT IN (${quarterMd})
      `),
    ])
    const ignoredNonQuarter = Number((ignoredRes.rows as StockRow[])[0]?.n ?? 0)
    const stockName = (stockRes.rows as StockRow[])[0]?.name
    const nameKey = (s: unknown) => normName(String(s ?? '')).replace(/[*-]/g, '')
    const rawName = stockName == null ? '' : String(stockName)
    const target  = stockName == null ? null : nameKey(rawName)
    // ⚠ 科创板 / 创业板简称带 -U / -W / -UW / -WD 等后缀（未盈利、特殊表决权标记），
    //   基金季报里用的是**不带后缀**的简称：九号公司-WD → 九号公司、
    //   星环科技-U → 星环科技。全库 47 只带此类后缀，实测它们同代码下都只有
    //   1 个 security_name，放宽后 0 个多命中 ⇒ 不会串入港股。
    //   生成「去后缀别名」一并参与名称闸门，matchedAlias 返回给前端透明标注。
    const stripped = rawName.replace(/-[A-Z]{1,3}$/, '')
    const alias = stripped !== rawName ? nameKey(stripped) : null
    const targets = new Set<string>()
    if (target) targets.add(target)
    if (alias) targets.add(alias)
    const allNames = (nameRes.rows as StockRow[]).map(r => ({
      name: String(r.security_name), count: Number(r.n),
    }))
    const accepted = targets.size
      ? allNames.filter(x => targets.has(nameKey(x.name))).map(x => x.name) : []
    const excluded = targets.size
      ? allNames.filter(x => !targets.has(nameKey(x.name))) : allNames
    const matchedAlias = alias && accepted.some(x => nameKey(x) === alias && nameKey(x) !== target)
      ? stripped : null

    const base = {
      code, securityName: stockName ?? null, excluded, ignoredNonQuarter, matchedAlias,
      summary: { funds: 0, shares: null, marketValue: null },
      rows: [], total: 0, page, pageSize,
    }
    if (!accepted.length) {
      return { found: false, ...base, reportDate: null, reportDates: [] }
    }

    // ⚠ 别把 JS 数组直接塞进 ANY(${...}::text[])：pg 驱动会把它序列化成
    //   "招商轮船" 这种畸形数组常量，报 22P02（2026-11-09 实测）。
    //   改成传 JSON 字符串，SQL 侧用 json_array_elements_text 展开成参数化 IN。
    const nameCond = sql`h.security_name IN (
      SELECT value FROM json_array_elements_text(${JSON.stringify(accepted)}::json)
    )`
    const whereCode = sql`h.security_code = ${code} AND h.holding_type = 'stock'
                          AND ${nameCond}
                          AND to_char(h.report_date, 'MM-DD') IN (${quarterMd})`

    const datesRes = await db.execute(sql`
      SELECT h.report_date::text AS d, COUNT(*)::int AS n
      FROM fund_holdings h
      WHERE ${whereCode}
      GROUP BY 1 ORDER BY 1 DESC
    `)
    const dateRows = (datesRes.rows as StockRow[]).map(r => ({
      date:  String(r.d).slice(0, 10),
      count: Number(r.n),
    }))
    // 报告期列表已只含季末，第一个就是最新季度（不需要再挑季末）
    const latestQuarter = dateRows[0]?.date ?? null
    const requested = (req.query.reportDate || '').trim() || null
    // 非法/不存在的报告期静默回退到默认，不报错也不返回空页
    const reportDate = (requested && dateRows.some(r => r.date === requested))
      ? requested
      : (latestQuarter ?? dateRows[0]?.date ?? null)

    if (!reportDate) {
      return { found: false, ...base, reportDate: null, reportDates: dateRows }
    }

    const cond = sql`${whereCode} AND h.report_date = ${reportDate}::date`
    const [list, agg] = await Promise.all([
      db.execute(sql`
        SELECT h.fund_code, f.fund_name, f.fund_type, f.fund_company,
               h.ratio, h.shares, h.market_value
        FROM fund_holdings h
        LEFT JOIN funds f ON f.fund_code = h.fund_code
        WHERE ${cond}
        ORDER BY h.shares DESC NULLS LAST, h.fund_code ASC
        LIMIT ${pageSize} OFFSET ${offset}
      `),
      db.execute(sql`
        SELECT COUNT(*)::int AS n, SUM(h.shares) AS shares, SUM(h.market_value) AS market_value
        FROM fund_holdings h WHERE ${cond}
      `),
    ])
    const a = (agg.rows as StockRow[])[0] ?? {}
    return {
      found: true, code, reportDate, reportDates: dateRows,
      securityName: stockName ?? null, excluded, ignoredNonQuarter, matchedAlias,
      summary: {
        funds:       Number(a.n ?? 0),
        shares:      toNum(a.shares),
        marketValue: toNum(a.market_value),
      },
      rows: (list.rows as StockRow[]).map(r => ({
        fundCode:    String(r.fund_code),
        fundName:    r.fund_name ?? null,
        fundType:    r.fund_type ?? null,
        fundCompany: r.fund_company ?? null,
        ratio:       toNum(r.ratio),
        shares:      toNum(r.shares),
        marketValue: toNum(r.market_value),
      })),
      total: Number(a.n ?? 0),
      page, pageSize,
    }
  })
}
