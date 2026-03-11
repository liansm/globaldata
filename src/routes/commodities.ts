import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { commodities, prices } from '../db/schema'
import { eq, desc, gte, lte, and, sql } from 'drizzle-orm'

// Helper: NUMERIC columns come back as strings from pg — convert to number
const toNum = (v: string | null) => (v == null ? null : parseFloat(v))

export async function commoditiesRoutes(app: FastifyInstance) {

  // ── GET /api/commodities ─────────────────────────────────────────────────
  // Returns all commodities with their latest price
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
      })
      .from(commodities)
      .orderBy(commodities.key)

    return rows.map(r => ({
      ...r,
      latestPrice: toNum(r.latestPrice),
    }))
  })

  // ── GET /api/commodities/:key ────────────────────────────────────────────
  // Returns commodity metadata + price history
  // Query params:
  //   days=30   — how many days of history (default 30, max 365)
  //   from=YYYY-MM-DD  — start date (overrides days)
  //   to=YYYY-MM-DD    — end date (default today)
  app.get<{
    Params: { key: string }
    Querystring: { days?: string; from?: string; to?: string }
  }>('/api/commodities/:key', async (req, reply) => {
    const { key } = req.params
    const { days = '30', from, to } = req.query

    // Fetch metadata
    const [meta] = await db
      .select()
      .from(commodities)
      .where(eq(commodities.key, key))

    if (!meta) {
      return reply.code(404).send({ error: `Commodity '${key}' not found` })
    }

    // Build date filters
    const toDate   = to   ? to   : new Date().toISOString().split('T')[0]
    const fromDate = from ? from : (() => {
      const d = new Date(toDate)
      d.setDate(d.getDate() - Math.min(parseInt(days), 365))
      return d.toISOString().split('T')[0]
    })()

    const history = await db
      .select({
        date:  prices.priceDate,
        price: prices.price,
      })
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
      history:   history.map(h => ({
        date:  h.date,
        price: toNum(h.price),
      })),
    }
  })

  // ── GET /api/commodities/:key/latest ────────────────────────────────────
  // Returns just the single most recent price (lightweight for dashboards)
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
