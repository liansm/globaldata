import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { marketIndices, indexPrices, indexMinutes, indexSpot } from '../db/schema'
import { eq, desc, gte, lte, and, sql } from 'drizzle-orm'

const toNum = (v: string | null) => (v == null ? null : parseFloat(v))

export async function marketsRoutes(app: FastifyInstance) {

  // ── GET /api/markets ──────────────────────────────────────────────────────
  // Returns all indices with latest close, turnover, and change%.
  // For A-share indices: if index_spot has a record (populated by
  // fetch_index_spot.py via stock_zh_index_spot_sina), use it directly for
  // price / change_pct / turnover. Otherwise fall back to index_prices.
  app.get('/api/markets', async () => {
    const rows = await db
      .select({
        key:       marketIndices.key,
        symbol:    marketIndices.symbol,
        name:      marketIndices.name,
        market:    marketIndices.market,
        unit:      marketIndices.unit,
        updatedAt: marketIndices.updatedAt,
        // Daily fallback fields (index_prices)
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
        // Real-time spot fields (index_spot) — A股 + 港股
        spotPrice: sql<string>`(
          SELECT price FROM index_spot
          WHERE index_key = ${marketIndices.key}
        )`.as('spot_price'),
        spotChangePct: sql<string>`(
          SELECT change_pct FROM index_spot
          WHERE index_key = ${marketIndices.key}
        )`.as('spot_change_pct'),
        spotTurnover: sql<string>`(
          SELECT turnover FROM index_spot
          WHERE index_key = ${marketIndices.key}
        )`.as('spot_turnover'),
        spotDate: sql<string>`(
          SELECT to_char(spot_date, 'YYYY-MM-DD') FROM index_spot
          WHERE index_key = ${marketIndices.key}
        )`.as('spot_date'),
        spotUpdatedAt: sql<string>`(
          SELECT to_char(updated_at AT TIME ZONE 'Asia/Shanghai', 'YYYY-MM-DD HH24:MI')
          FROM index_spot
          WHERE index_key = ${marketIndices.key}
        )`.as('spot_updated_at'),
      })
      .from(marketIndices)
      .orderBy(marketIndices.market, marketIndices.key)

    return rows.map(r => {
      const latestClose    = toNum(r.latestClose)
      const prevClose      = toNum(r.prevClose)
      const latestTurnover = toNum(r.latestTurnover)
      const spotPrice      = toNum(r.spotPrice)
      const spotChangePct  = toNum(r.spotChangePct)
      const spotTurnover   = toNum(r.spotTurnover)

      // Use real-time spot data when available for A股 and 港股
      const useSpot = (r.market === 'A股' || r.market === '港股') && spotPrice != null

      const displayClose = useSpot ? spotPrice : latestClose

      // change%: spot provides it directly; daily: compute from prevClose
      const changePct = useSpot
        ? (spotChangePct != null ? parseFloat(spotChangePct.toFixed(2)) : null)
        : (displayClose != null && prevClose != null && prevClose !== 0)
          ? parseFloat(((displayClose - prevClose) / prevClose * 100).toFixed(2))
          : null

      return {
        ...r,
        latestDate:     useSpot ? (r.spotDate ?? r.latestDate) : r.latestDate,
        latestClose:    displayClose,
        latestTurnover: useSpot ? spotTurnover : latestTurnover,
        changePct,
        // Non-null when spot data is active — used by frontend to show "实时" badge + time
        latestSpotUpdatedAt: useSpot ? (r.spotUpdatedAt ?? null) : null,
      }
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
      d.setDate(d.getDate() - Math.min(parseInt(days), 7300))
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

  // ── GET /api/markets/:key/minutes ─────────────────────────────────────────
  // Returns 1-minute intraday bars for A-share indices (latest trading day by default)
  // Query params: date=YYYY-MM-DD
  app.get<{
    Params: { key: string }
    Querystring: { date?: string }
  }>('/api/markets/:key/minutes', async (req, reply) => {
    const { key } = req.params
    const { date } = req.query

    const [meta] = await db
      .select()
      .from(marketIndices)
      .where(eq(marketIndices.key, key))

    if (!meta) {
      return reply.code(404).send({ error: `Index '${key}' not found` })
    }
    if (meta.market !== 'A股') {
      return reply.code(400).send({ error: 'Minute data only available for A-share indices' })
    }

    // Resolve target date
    let targetDate = date
    if (!targetDate) {
      const [latest] = await db
        .select({ dt: indexMinutes.dt })
        .from(indexMinutes)
        .where(eq(indexMinutes.indexKey, key))
        .orderBy(desc(indexMinutes.dt))
        .limit(1)
      if (!latest) {
        return { key, name: meta.name, market: meta.market, date: null, minutes: [] }
      }
      // dt is stored as China local time — format as YYYY-MM-DD directly
      const d = latest.dt
      const pad = (n: number) => String(n).padStart(2, '0')
      targetDate = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
    }

    // Prefer index_spot.prev_close (updated by fetch_index_spot.py from real-time feed)
    // Fall back to latest record in index_prices before targetDate
    const [spotRow] = await db
      .select({ prevClose: indexSpot.prevClose })
      .from(indexSpot)
      .where(eq(indexSpot.indexKey, key))
      .limit(1)
    let prevClose = spotRow ? toNum(spotRow.prevClose) : null
    if (prevClose == null) {
      const [prevRow] = await db
        .select({ close: indexPrices.close })
        .from(indexPrices)
        .where(and(
          eq(indexPrices.indexKey, key),
          sql`${indexPrices.priceDate} < ${targetDate}::date`,
        ))
        .orderBy(desc(indexPrices.priceDate))
        .limit(1)
      prevClose = prevRow ? toNum(prevRow.close) : null
    }

    const rows = await db
      .select({
        time:     sql<string>`to_char(${indexMinutes.dt}, 'YYYY-MM-DD HH24:MI')`,
        open:     indexMinutes.open,
        high:     indexMinutes.high,
        low:      indexMinutes.low,
        close:    indexMinutes.close,
        volume:   indexMinutes.volume,
        turnover: indexMinutes.turnover,
      })
      .from(indexMinutes)
      .where(and(
        eq(indexMinutes.indexKey, key),
        sql`DATE(${indexMinutes.dt}) = ${targetDate}::date`,
      ))
      .orderBy(indexMinutes.dt)

    return {
      key,
      name:      meta.name,
      market:    meta.market,
      date:      targetDate,
      prevClose,
      minutes: rows.map(m => ({
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
}
