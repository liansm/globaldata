import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { cryptoCoins, cryptoPrices } from '../db/schema'
import { eq, desc, gte, lte, and, sql } from 'drizzle-orm'

const toNum = (v: string | null | undefined) => (v == null ? null : parseFloat(v))

export async function cryptoRoutes(app: FastifyInstance) {

  // ── GET /api/crypto ───────────────────────────────────────────────────────
  // Returns all coins with their latest snapshot (price, changePct, volume, high, low)
  app.get('/api/crypto', async () => {
    const rows = await db
      .select({
        key:       cryptoCoins.key,
        symbol:    cryptoCoins.symbol,
        name:      cryptoCoins.name,
        unit:      cryptoCoins.unit,
        updatedAt: cryptoCoins.updatedAt,
        latestDate: sql<string>`(
          SELECT price_date FROM crypto_prices
          WHERE coin_key = ${cryptoCoins.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('latest_date'),
        latestClose: sql<string>`(
          SELECT close FROM crypto_prices
          WHERE coin_key = ${cryptoCoins.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('latest_close'),
        changePct: sql<string>`(
          SELECT change_pct FROM crypto_prices
          WHERE coin_key = ${cryptoCoins.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('change_pct'),
        volume24h: sql<string>`(
          SELECT volume_24h FROM crypto_prices
          WHERE coin_key = ${cryptoCoins.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('volume_24h'),
        high24h: sql<string>`(
          SELECT high_24h FROM crypto_prices
          WHERE coin_key = ${cryptoCoins.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('high_24h'),
        low24h: sql<string>`(
          SELECT low_24h FROM crypto_prices
          WHERE coin_key = ${cryptoCoins.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('low_24h'),
      })
      .from(cryptoCoins)
      .orderBy(cryptoCoins.key)

    // Return in the predefined display order
    const ORDER = ['btc', 'eth', 'bnb', 'sol', 'xrp', 'usdt', 'usdc', 'ada', 'doge', 'avax']
    const map = Object.fromEntries(rows.map(r => [r.key, r]))

    return ORDER
      .filter(k => map[k])
      .map(k => {
        const r = map[k]
        return {
          ...r,
          latestClose: toNum(r.latestClose),
          changePct:   toNum(r.changePct),
          volume24h:   toNum(r.volume24h),
          high24h:     toNum(r.high24h),
          low24h:      toNum(r.low24h),
        }
      })
  })

  // ── GET /api/crypto/:key ──────────────────────────────────────────────────
  // Returns coin metadata + snapshot history
  // Query params: days=90 | from=YYYY-MM-DD | to=YYYY-MM-DD
  app.get<{
    Params: { key: string }
    Querystring: { days?: string; from?: string; to?: string }
  }>('/api/crypto/:key', async (req, reply) => {
    const { key } = req.params
    const { days = '90', from, to } = req.query

    const [meta] = await db
      .select()
      .from(cryptoCoins)
      .where(eq(cryptoCoins.key, key))

    if (!meta) {
      return reply.code(404).send({ error: `Coin '${key}' not found` })
    }

    const toDate   = to   ? to   : new Date().toISOString().split('T')[0]
    const fromDate = from ? from : (() => {
      const d = new Date(toDate)
      d.setDate(d.getDate() - Math.min(parseInt(days), 365))
      return d.toISOString().split('T')[0]
    })()

    const history = await db
      .select({
        date:      cryptoPrices.priceDate,
        close:     cryptoPrices.close,
        changePct: cryptoPrices.changePct,
        volume24h: cryptoPrices.volume24h,
        high24h:   cryptoPrices.high24h,
        low24h:    cryptoPrices.low24h,
      })
      .from(cryptoPrices)
      .where(and(
        eq(cryptoPrices.coinKey, key),
        gte(cryptoPrices.priceDate, fromDate),
        lte(cryptoPrices.priceDate, toDate),
      ))
      .orderBy(desc(cryptoPrices.priceDate))

    return {
      key:       meta.key,
      symbol:    meta.symbol,
      name:      meta.name,
      unit:      meta.unit,
      updatedAt: meta.updatedAt,
      history: history.map(h => ({
        date:      h.date,
        close:     toNum(h.close),
        changePct: toNum(h.changePct),
        volume24h: toNum(h.volume24h),
        high24h:   toNum(h.high24h),
        low24h:    toNum(h.low24h),
      })),
    }
  })
}
