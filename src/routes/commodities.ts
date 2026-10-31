import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { commodities, prices, commoditySpot, commodityMinutes } from '../db/schema'
import { eq, desc, gte, lte, lt, and, sql } from 'drizzle-orm'

// Helper: NUMERIC columns come back as strings from pg — convert to number
const toNum = (v: string | null) => (v == null ? null : parseFloat(v))

export async function commoditiesRoutes(app: FastifyInstance) {

  // ── GET /api/commodities ─────────────────────────────────────────────────
  // Returns all commodities with their latest price + real-time spot data.
  // When commodity_spot has a record, spotPrice / spotChangePct are included;
  // the frontend shows them preferentially with an "实时" badge.
  app.get('/api/commodities', async () => {
    const rows = await db
      .select({
        key:        commodities.key,
        symbol:     commodities.symbol,
        commodity:  commodities.commodity,
        unit:       commodities.unit,
        priceType:  commodities.priceType,
        kcal:       commodities.kcal,
        gradeType:  commodities.gradeType,
        updatedAt:  commodities.updatedAt,
        // Daily fallback (index_prices)
        latestDate:  sql<string>`(
          SELECT price_date FROM prices
          WHERE commodity_key = ${commodities.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('latest_date'),
        latestPrice: sql<string>`(
          SELECT price FROM prices
          WHERE commodity_key = ${commodities.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('latest_price'),
        // Real-time spot (commodity_spot)
        spotPrice:     sql<string>`(
          SELECT price FROM commodity_spot
          WHERE commodity_key = ${commodities.key}
        )`.as('spot_price'),
        spotChangePct: sql<string>`(
          SELECT change_pct FROM commodity_spot
          WHERE commodity_key = ${commodities.key}
        )`.as('spot_change_pct'),
        spotDate: sql<string>`(
          SELECT to_char(spot_date, 'YYYY-MM-DD') FROM commodity_spot
          WHERE commodity_key = ${commodities.key}
        )`.as('spot_date'),
        spotUpdatedAt: sql<string>`(
          SELECT to_char(updated_at AT TIME ZONE 'Asia/Shanghai', 'YYYY-MM-DD HH24:MI')
          FROM commodity_spot
          WHERE commodity_key = ${commodities.key}
        )`.as('spot_updated_at'),
        // 'intraday' = 盘中/夜盘实时（fetch_commodity_spot.py 在交易时段写入）
        // 'close'    = 收市静态价（日期已校正成最近交易日，周末不会是周六）
        spotKind: sql<string>`(
          SELECT COALESCE(snapshot_kind, 'intraday') FROM commodity_spot
          WHERE commodity_key = ${commodities.key}
        )`.as('spot_kind'),
        spotIsToday: sql<boolean>`(
          SELECT spot_date = (NOW() AT TIME ZONE 'Asia/Shanghai')::date
          FROM commodity_spot
          WHERE commodity_key = ${commodities.key}
        )`.as('spot_is_today'),
        // Whether this commodity has any intraday minute bars for today
        // （`COUNT(*) > 0` 返回 boolean，之前误标 sql<string> 导致 tsc 报既有错误）
        hasMinutes: sql<boolean>`(
          SELECT COUNT(*) > 0 FROM commodity_minutes
          WHERE commodity_key = ${commodities.key}
            AND DATE(dt) = (
              SELECT DATE(dt) FROM commodity_minutes
              WHERE commodity_key = ${commodities.key}
              ORDER BY dt DESC LIMIT 1
            )
        )`.as('has_minutes'),
      })
      .from(commodities)
      .orderBy(commodities.key)

    return rows.map(r => {
      const latestPrice  = toNum(r.latestPrice)
      const spotPrice    = toNum(r.spotPrice)
      const spotChangePct = toNum(r.spotChangePct)

      // ── spot 能不能当现价用 ────────────────────────────────────────────────
      // 见上面 spotKind / spotIsToday 的注释。两种情况都可用，但含义不同：
      //
      // ① intraday：必须来自「今天」。之前这里是 `useSpot = spotPrice != null`，
      //    完全不看新旧，于是 2026-10-31（周六）卡片显示「今天 11:34 · 实时」，
      //    而价格其实是周五夜盘收盘 —— 期货根本没开市。
      // ② close：`spot_date` 已被脚本校正成最近交易日，只要不早于日线最新日期，
      //    它就是「最近交易日的收盘」，直接用。
      const kind = r.spotKind
      const closeIsCurrent =
        kind === 'close' && r.spotDate != null
        && (r.latestDate == null || r.spotDate >= r.latestDate)
      const intradayIsCurrent = kind !== 'close' && r.spotIsToday === true

      const useSpot = spotPrice != null && (closeIsCurrent || intradayIsCurrent)

      return {
        ...r,
        latestDate:    useSpot ? (r.spotDate ?? r.latestDate) : r.latestDate,
        latestPrice:   useSpot ? spotPrice : latestPrice,
        spotChangePct: useSpot ? spotChangePct : null,
        // 'intraday' → 前端显示「HH:MM · 实时」；'close' → 只显示「收盘」
        spotSnapshotKind: useSpot ? (kind === 'close' ? 'close' : 'intraday') : null,
        // 收盘快照的 updated_at 是抓取时刻（周末跑就是周末的时刻），不能当"收盘时间"用
        spotUpdatedAt: useSpot && kind !== 'close' ? (r.spotUpdatedAt ?? null) : null,
        hasMinutes:    r.hasMinutes === true,
        // Remove raw spot fields from response
        spotPrice:    undefined,
        spotDate:     undefined,
      }
    })
  })

  // ── GET /api/commodities/:key ────────────────────────────────────────────
  // Returns commodity metadata + price history + spot snapshot
  app.get<{
    Params: { key: string }
    Querystring: { days?: string; from?: string; to?: string }
  }>('/api/commodities/:key', async (req, reply) => {
    const { key } = req.params
    const { days = '30', from, to } = req.query

    const [meta] = await db
      .select()
      .from(commodities)
      .where(eq(commodities.key, key))

    if (!meta) {
      return reply.code(404).send({ error: `Commodity '${key}' not found` })
    }

    const toDate   = to   ? to   : new Date().toISOString().split('T')[0]
    const fromDate = from ? from : (() => {
      const d = new Date(toDate)
      d.setDate(d.getDate() - Math.min(parseInt(days), 7300))
      return d.toISOString().split('T')[0]
    })()

    const [spotRow] = await db
      .select()
      .from(commoditySpot)
      .where(eq(commoditySpot.commodityKey, key))

    const history = await db
      .select({ date: prices.priceDate, price: prices.price })
      .from(prices)
      .where(and(
        eq(prices.commodityKey, key),
        gte(prices.priceDate, fromDate),
        lte(prices.priceDate, toDate),
      ))
      .orderBy(desc(prices.priceDate))

    // 区间涨跌幅的基期：fromDate **之前**最近一条收盘价。
    // 没有它，前端只能拿「区间内首日收盘」当基准，会漏掉首日当天的涨跌 ——
    // 周六看「本周」时等于只算了 4 个交易日，看着像少一天。含首日才符合
    // 「区间涨跌幅」的常规口径（期初前收 → 期末）。
    const [baseline] = await db
      .select({ date: prices.priceDate, price: prices.price })
      .from(prices)
      .where(and(eq(prices.commodityKey, key), lt(prices.priceDate, fromDate)))
      .orderBy(desc(prices.priceDate))
      .limit(1)

    return {
      key:       meta.key,
      symbol:    meta.symbol,
      commodity: meta.commodity,
      unit:      meta.unit,
      priceType: meta.priceType,
      kcal:      meta.kcal,
      gradeType: meta.gradeType,
      updatedAt: meta.updatedAt,
      spot: spotRow ? {
        price:      toNum(spotRow.price),
        changePct:  toNum(spotRow.changePct),
        changeAmt:  toNum(spotRow.changeAmt),
        prevClose:  toNum(spotRow.prevClose),
        volume:     toNum(spotRow.volume),
        turnover:   toNum(spotRow.turnover),
        spotDate:   spotRow.spotDate,
        updatedAt:  spotRow.updatedAt,
        snapshotKind: spotRow.snapshotKind ?? 'intraday',
      } : null,
      history: history.map(h => ({
        date:  h.date,
        price: toNum(h.price),
      })),
      // 区间基期（区间起点前最后一个交易日收盘价）；序列在区间内才有数据时为 null
      baseline: baseline ? { date: baseline.date, price: toNum(baseline.price) } : null,
    }
  })

  // ── GET /api/commodities/:key/minutes ────────────────────────────────────
  // Returns 1-minute intraday bars for a commodity (domestic futures only).
  // Query param: date=YYYY-MM-DD (defaults to most recent date with bars)
  app.get<{
    Params: { key: string }
    Querystring: { date?: string }
  }>('/api/commodities/:key/minutes', async (req, reply) => {
    const { key } = req.params
    const { date: dateParam } = req.query

    const [meta] = await db
      .select()
      .from(commodities)
      .where(eq(commodities.key, key))

    if (!meta) {
      return reply.code(404).send({ error: `Commodity '${key}' not found` })
    }

    // Resolve target date
    // ⚠ 用 SQL 取日期，别在 JS 里对 `TIMESTAMP` 做 getFullYear()：pg 把无时区的
    //   timestamp 当 UTC 解析，本地（UTC+8）一加就跨天 —— 甲醇夜盘末根 10-30 23:00
    //   会被算成 10-31，接口去查一个没有数据的日子，分时图直接空白。
    let targetDate = dateParam
    if (!targetDate) {
      const [latest] = await db
        .select({ date: sql<string>`to_char(MAX(${commodityMinutes.dt}), 'YYYY-MM-DD')` })
        .from(commodityMinutes)
        .where(eq(commodityMinutes.commodityKey, key))
      if (!latest?.date) {
        return { key, commodity: meta.commodity, date: null, minutes: [] }
      }
      targetDate = latest.date
    }

    const rows = await db
      .select({
        time:     sql<string>`to_char(${commodityMinutes.dt}, 'YYYY-MM-DD HH24:MI')`,
        open:     commodityMinutes.open,
        high:     commodityMinutes.high,
        low:      commodityMinutes.low,
        close:    commodityMinutes.close,
        volume:   commodityMinutes.volume,
        turnover: commodityMinutes.turnover,
      })
      .from(commodityMinutes)
      .where(and(
        eq(commodityMinutes.commodityKey, key),
        sql`DATE(${commodityMinutes.dt}) = ${targetDate}::date`,
      ))
      .orderBy(commodityMinutes.dt)

    return {
      key,
      commodity: meta.commodity,
      unit:      meta.unit,
      date:      targetDate,
      minutes:   rows.map(m => ({
        time:     m.time,
        open:     toNum(m.open),
        high:     toNum(m.high),
        low:      toNum(m.low),
        close:    toNum(m.close),
        volume:   toNum(m.volume),
        turnover: toNum(m.turnover),
      })),
    }
  })

  // ── GET /api/commodities/:key/latest ────────────────────────────────────
  app.get<{ Params: { key: string } }>(
    '/api/commodities/:key/latest',
    async (req, reply) => {
      const { key } = req.params
      const [row] = await db
        .select({
          key:       commodities.key,
          commodity: commodities.commodity,
          unit:      commodities.unit,
          date:      prices.priceDate,
          price:     prices.price,
        })
        .from(prices)
        .innerJoin(commodities, eq(prices.commodityKey, commodities.key))
        .where(eq(prices.commodityKey, key))
        .orderBy(desc(prices.priceDate))
        .limit(1)

      if (!row) {
        return reply.code(404).send({ error: `Commodity '${key}' not found` })
      }
      return { ...row, price: toNum(row.price) }
    }
  )
}
