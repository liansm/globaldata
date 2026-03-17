import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { marketIndices, indexPrices } from '../db/schema'
import { eq, desc, gte, lte, and, sql } from 'drizzle-orm'

const toNum = (v: string | null) => (v == null ? null : parseFloat(v))

export async function marketsRoutes(app: FastifyInstance) {

  // ── GET /api/markets ──────────────────────────────────────────────────────
  // Returns all indices with their latest close price and date
  app.get('/api/markets', async () => {
    const rows = await db
      .select({
        key:       marketIndices.key,
        symbol:    marketIndices.symbol,
        name:      marketIndices.name,
        market:    marketIndices.market,
        unit:      marketIndices.unit,
        updatedAt: marketIndices.updatedAt,
        latestDate: sql<string>`(
          SELECT price_date FROM index_prices
          WHERE index_key = ${marketIndices.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('latest_date'),
        latestClose: sql<string>`(
          SELECT close FROM index_prices
          WHERE index_key = ${marketIndices.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('latest_close'),
        latestTurnover: sql<string>`(
          SELECT turnover FROM index_prices
          WHERE index_key = ${marketIndices.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('latest_turnover'),
        prevClose: sql<string>`(
          SELECT close FROM index_prices
          WHERE index_key = ${marketIndices.key}
          ORDER BY price_date DESC
          OFFSET 1 LIMIT 1
        )`.as('prev_close'),
      })
      .from(marketIndices)
      .orderBy(marketIndices.market, marketIndices.key)

    return rows.map(r => {
      const latestClose    = toNum(r.latestClose)
      const prevClose      = toNum(r.prevClose)
      const latestTurnover = toNum(r.latestTurnover)
      const changePct = (latestClose != null && prevClose != null && prevClose !== 0)
        ? parseFloat(((latestClose - prevClose) / prevClose * 100).toFixed(2))
        : null
      return { ...r, latestClose, latestTurnover, changePct }
    })
  })

  // ── GET /api/markets/:key ─────────────────────────────────────────────────
  // Returns index metadata + price history (close + volume + turnover)
  // Query params: days=30 | from=YYYY-MM-DD | to=YYYY-MM-DD
  app.get<{
    Params: { key: string }
    Querystring: { days?: string; from?: string; to?: string }
  }>('/api/markets/:key', async (req, reply) => {
    const { key } = req.params
    const { days = '90', from, to } = req.query

    const [meta] = await db
      .select()
      .from(marketIndices)
      .where(eq(marketIndices.key, key))

    if (!meta) {
      return reply.code(404).send({ error: `Index '${key}' not found` })
    }

    const toDate   = to   ? to   : new Date().toISOString().split('T')[0]
    const fromDate = from ? from : (() => {
      const d = new Date(toDate)
      d.setDate(d.getDate() - Math.min(parseInt(days), 365))
      return d.toISOString().split('T')[0]
    })()

    const history = await db
      .select({
        date:     indexPrices.priceDate,
        close:    indexPrices.close,
        volume:   indexPrices.volume,
        turnover: indexPrices.turnover,
      })
      .from(indexPrices)
      .where(and(
        eq(indexPrices.indexKey, key),
        gte(indexPrices.priceDate, fromDate),
        lte(indexPrices.priceDate, toDate),
      ))
      .orderBy(desc(indexPrices.priceDate))

    return {
      key:       meta.key,
      symbol:    meta.symbol,
      name:      meta.name,
      market:    meta.market,
      unit:      meta.unit,
      updatedAt: meta.updatedAt,
      history: history.map(h => ({
        date:     h.date,
        close:    toNum(h.close),
        volume:   toNum(h.volume),
        turnover: toNum(h.turnover),
      })),
    }
  })
}
