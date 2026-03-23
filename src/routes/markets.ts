import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { marketIndices, indexPrices, indexMinutes } from '../db/schema'
import { eq, desc, gte, lte, and, sql } from 'drizzle-orm'

const toNum = (v: string | null) => (v == null ? null : parseFloat(v))

export async function marketsRoutes(app: FastifyInstance) {

  // ── GET /api/markets ──────────────────────────────────────────────────────
  // Returns all indices with their latest close price and date.
  // For A-share indices: if intraday (index_minutes) data is more recent than
  // the latest daily close, the intraday last-bar close is used as the price
  // and the change% is computed against the previous daily close.
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
        // Intraday: latest minute bar (only populated for A-share indices)
        latestMinuteClose: sql<string>`(
          SELECT close FROM index_minutes
          WHERE index_key = ${marketIndices.key}
          ORDER BY dt DESC LIMIT 1
        )`.as('latest_minute_close'),
        latestMinuteDt: sql<string>`(
          SELECT to_char(dt, 'YYYY-MM-DD HH24:MI') FROM index_minutes
          WHERE index_key = ${marketIndices.key}
          ORDER BY dt DESC LIMIT 1
        )`.as('latest_minute_dt'),
        // Sum of turnover for all bars on the latest intraday day
        latestMinuteTurnover: sql<string>`(
          SELECT SUM(turnover) FROM index_minutes
          WHERE index_key = ${marketIndices.key}
            AND DATE(dt) = (
              SELECT DATE(dt) FROM index_minutes
              WHERE index_key = ${marketIndices.key}
              ORDER BY dt DESC LIMIT 1
            )
        )`.as('latest_minute_turnover'),
      })
      .from(marketIndices)
      .orderBy(marketIndices.market, marketIndices.key)

    return rows.map(r => {
      const latestClose           = toNum(r.latestClose)
      const prevClose             = toNum(r.prevClose)
      const latestTurnover        = toNum(r.latestTurnover)
      const latestMinuteClose     = toNum(r.latestMinuteClose)
      const latestMinuteDt        = r.latestMinuteDt ?? null
      const latestMinuteTurnover  = toNum(r.latestMinuteTurnover)

      // Use intraday price when: A-share index AND minute date is newer than daily date
      const minuteDay = latestMinuteDt ? latestMinuteDt.slice(0, 10) : null
      const dailyDay  = r.latestDate   ? r.latestDate.slice(0, 10)   : null
      const useMinute = r.market === 'A股'
                        && latestMinuteClose != null
                        && minuteDay != null && dailyDay != null
                        && minuteDay > dailyDay

      const displayClose = useMinute ? latestMinuteClose : latestClose
      // Baseline for change%:
      //   - intraday  → previous daily close (latestClose = last record in index_prices)
      //   - daily     → the record before that (prevClose = OFFSET 1)
      const baseClose = useMinute ? latestClose : prevClose
      const changePct = (displayClose != null && baseClose != null && baseClose !== 0)
        ? parseFloat(((displayClose - baseClose) / baseClose * 100).toFixed(2))
        : null

      return {
        ...r,
        // When using intraday, update latestDate to the intraday day (YYYY-MM-DD)
        latestDate:     useMinute ? minuteDay : r.latestDate,
        latestClose:    displayClose,
        latestTurnover: useMinute ? latestMinuteTurnover : latestTurnover,
        changePct,
        // Non-null only when intraday data is actually being shown
        latestMinuteDt: useMinute ? latestMinuteDt : null,
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
      name:    meta.name,
      market:  meta.market,
      date:    targetDate,
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
