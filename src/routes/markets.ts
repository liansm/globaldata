import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { marketIndices, indexPrices, indexMinutes, indexSpot } from '../db/schema'
import { eq, desc, gte, lte, and, sql } from 'drizzle-orm'

const toNum = (v: string | null) => (v == null ? null : parseFloat(v))

/**
 * 当前是否在该市场的交易时段内（Asia/Shanghai）。用于判断 index_spot 的当日快照
 * 还能不能当「实时」用 —— 收盘后它就不再刷新，继续用会把收盘价显示成盘中价。
 *
 * ⚠ 只判「工作日 + 时段」，**不判节假日**（没有交易日历）。节假日会误判成 true，
 *   但那种情况 `spot_date` 是节前最后一天、不等于今天，`spotIsToday` 会先把它挡掉。
 * ⚠ 收盘时间留 5~10 分钟余量：让收盘后最后一批快照（接近收盘价）能被用上，
 *   之后日线入库就切回日线。
 */
function inTradingWindow(market: string, now = new Date()): boolean {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    weekday: 'short', hour: '2-digit', minute: '2-digit', hour12: false,
  }).formatToParts(now)
  const get = (t: string) => parts.find(p => p.type === t)?.value ?? ''
  const wd = get('weekday')
  if (wd === 'Sat' || wd === 'Sun') return false
  const hm = parseInt(get('hour'), 10) * 60 + parseInt(get('minute'), 10)
  // A股 09:15 集合竞价起 / 15:05 收盘余量；港股 09:30 起 / 16:10 收盘余量
  const [open, close] = market === '港股'
    ? [9 * 60 + 15, 16 * 60 + 10]
    : [9 * 60 + 15, 15 * 60 + 5]
  return hm >= open && hm <= close
}

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
        latestVolume: sql<string>`(
          SELECT volume FROM index_prices
          WHERE index_key = ${marketIndices.key}
          ORDER BY price_date DESC LIMIT 1
        )`.as('latest_volume'),
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
        // ── spot 是否还能当「实时」用 ─────────────────────────────────────
        // index_spot 的行**会残留**：fetch_index_spot.py 的 _is_today_snapshot()
        // 在「最新价 == 某历史收盘价」时故意跳过写库（防盘前把昨日收盘记成今天），
        // 所以收盘后再怎么跑都不会刷新这一行 —— 最后一次盘中快照会一直躺在表里。
        // 直接拿它当现价，卡片就会显示**上一个交易日盘中的价格**：
        // 实测 2026-10-31（周六）上证卡片 3615.4043 / -0.99%，那是 10-30 09:44 的盘中价，
        // 而 10-30 真实收盘是 3599.0170 / -1.43%。时间没错，价格是错的。
        spotIsToday: sql<boolean>`(
          SELECT spot_date = (NOW() AT TIME ZONE 'Asia/Shanghai')::date
          FROM index_spot
          WHERE index_key = ${marketIndices.key}
        )`.as('spot_is_today'),
        // 日线已经有 spot 那天的收盘 → 当天已结算，盘中价作废，必须用日线收盘
        dailyCoversSpotDate: sql<boolean>`(
          EXISTS (
            SELECT 1 FROM index_spot s
            JOIN index_prices p
              ON p.index_key = s.index_key AND p.price_date = s.spot_date
            WHERE s.index_key = ${marketIndices.key}
          )
        )`.as('daily_covers_spot_date'),
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

      // Use real-time spot data when available for A股 and 港股。
      // ⚠ 见上面 spotIsToday / dailyCoversSpotDate 的注释。三个条件：
      //   ① spot 必须来自「今天」（否则是上一个交易日的盘中残留）
      //   ② 要么现在就在交易时段内；要么当天日线还没收盘入库
      //      （收盘后几分钟里日线可能还没跑到，这时 spot 是最后一个快照，比「昨天的收盘」准）
      const spotUsable = r.spotIsToday === true
        && (inTradingWindow(r.market) || r.dailyCoversSpotDate !== true)
      const useSpot =
        (r.market === 'A股' || r.market === '港股') && spotPrice != null && spotUsable

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
        // 成交额缺失时前端回退显示成交量（新浪美股 amount 恒为 0 → turnover 为 NULL）
        latestVolume:   toNum(r.latestVolume),
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
