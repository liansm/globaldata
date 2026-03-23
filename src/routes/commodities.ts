import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { commodities, prices, commoditySpot, commodityMinutes } from '../db/schema'
import { eq, desc, gte, lte, and, sql } from 'drizzle-orm'

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
        // Whether this commodity has any intraday minute bars for today
        hasMinutes: sql<string>`(
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

      // Use real-time spot when available (spot date >= daily date or spot exists)
      const useSpot = spotPrice != null

      return {
        ...r,
        latestDate:    useSpot ? (r.spotDate ?? r.latestDate) : r.latestDate,
        latestPrice:   useSpot ? spotPrice : latestPrice,
        spotChangePct: useSpot ? spotChangePct : null,
        spotUpdatedAt: useSpot ? (r.spotUpdatedAt ?? null) : null,
        hasMinutes:    r.hasMinutes === 'true' || r.hasMinutes === true,
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
      } : null,
      history: history.map(h => ({
        date:  h.date,
        price: toNum(h.price),
      })),
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
    let targetDate = dateParam
    if (!targetDate) {
      const [latest] = await db
        .select({ dt: commodityMinutes.dt })
        .from(commodityMinutes)
        .where(eq(commodityMinutes.commodityKey, key))
        .orderBy(desc(commodityMinutes.dt))
        .limit(1)
      if (!latest) {
        return { key, commodity: meta.commodity, date: null, minutes: [] }
      }
      const d = latest.dt
      const pad = (n: number) => String(n).padStart(2, '0')
      targetDate = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
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
