import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { fxPairs, fxRates, fxSpot } from '../db/schema'
import { eq, desc, gte, lte, and, sql } from 'drizzle-orm'

const toNum = (v: string | null | undefined) => (v == null ? null : parseFloat(v))

/**
 * 库内 `fx_rates.close` 的统一口径是「1 单位外币兑人民币」，
 * 展示口径要乘 `quote_unit`（日元 100，其余 1）。
 * ⚠ 这是**换算的唯一出处**，前端不要再自己乘一遍。
 */
const PER_MILLIONS = 1e8
function toDisplay(v: number | null, unit: number): number | null {
  if (v == null) return null
  return Math.round(v * unit * PER_MILLIONS) / PER_MILLIONS
}

/** 涨跌（display 空间）。返回 { change, changePct }，任一缺失则为 null */
function diff(cur: number | null, prev: number | null) {
  if (cur == null || prev == null || prev === 0) return { change: null, changePct: null }
  return {
    change:    Math.round((cur - prev) * PER_MILLIONS) / PER_MILLIONS,
    changePct: Math.round((cur - prev) / prev * 100 * 10000) / 10000,
  }
}

// 子查询片段：取某口径下的最新 / 前一条
// ⚠ 外层列必须用 drizzle 的列对象（会渲染成 "fx_pairs"."key" 全限定），
//   否则会被内层 fx_rates 的列遮罩成恒真（见项目记忆「drizzle 三坑」①）
export async function fxRoutes(app: FastifyInstance) {

  // ── GET /api/fx ───────────────────────────────────────────────────────────
  // 全部货币对 + 两个口径的最新值 + 即期实时快照
  // 数值一律为 **展示口径**（日元已 ×100）；口径语义见 schema.ts 的 fx_pairs 注释
  app.get('/api/fx', async () => {
    const rows = await db
      .select({
        key:          fxPairs.key,
        baseCode:     fxPairs.baseCode,
        baseName:     fxPairs.baseName,
        quoteUnit:    fxPairs.quoteUnit,
        category:     fxPairs.category,
        officialCode: fxPairs.officialCode,
        spotCode:     fxPairs.spotCode,
        sortOrder:    fxPairs.sortOrder,
        updatedAt:    fxPairs.updatedAt,

        // ── 中间价（cfets）────────────────────────────────────────────────
        midDate: sql<string>`(
          SELECT to_char(rate_date, 'YYYY-MM-DD')
          FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'mid'
          ORDER BY rate_date DESC LIMIT 1
        )`.as('mid_date'),
        midClose: sql<string>`(
          SELECT close FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'mid'
          ORDER BY rate_date DESC LIMIT 1
        )`.as('mid_close'),
        midPrevClose: sql<string>`(
          SELECT close FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'mid'
          ORDER BY rate_date DESC OFFSET 1 LIMIT 1
        )`.as('mid_prev_close'),
        midCount: sql<string>`(
          SELECT COUNT(*) FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'mid'
        )`.as('mid_count'),

        // ── 即期日线（sina）──────────────────────────────────────────────
        spotDate: sql<string>`(
          SELECT to_char(rate_date, 'YYYY-MM-DD')
          FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'spot'
          ORDER BY rate_date DESC LIMIT 1
        )`.as('spot_date'),
        spotClose: sql<string>`(
          SELECT close FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'spot'
          ORDER BY rate_date DESC LIMIT 1
        )`.as('spot_close'),
        spotPrevClose: sql<string>`(
          SELECT close FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'spot'
          ORDER BY rate_date DESC OFFSET 1 LIMIT 1
        )`.as('spot_prev_close'),
        spotEarliest: sql<string>`(
          SELECT to_char(MIN(rate_date), 'YYYY-MM-DD')
          FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'spot'
        )`.as('spot_earliest'),
        spotCount: sql<string>`(
          SELECT COUNT(*) FROM fx_rates
          WHERE pair_key = ${fxPairs.key} AND rate_type = 'spot'
        )`.as('spot_count'),

        // ── 即期实时快照（fx_spot）───────────────────────────────────────
        livePrice:     sql<string>`(SELECT price      FROM fx_spot WHERE pair_key = ${fxPairs.key})`.as('live_price'),
        livePrevClose: sql<string>`(SELECT prev_close FROM fx_spot WHERE pair_key = ${fxPairs.key})`.as('live_prev_close'),
        liveChangePct: sql<string>`(SELECT change_pct FROM fx_spot WHERE pair_key = ${fxPairs.key})`.as('live_change_pct'),
        liveChangeAmt: sql<string>`(SELECT change_amt FROM fx_spot WHERE pair_key = ${fxPairs.key})`.as('live_change_amt'),
        liveQuoteTime: sql<string>`(SELECT quote_time FROM fx_spot WHERE pair_key = ${fxPairs.key})`.as('live_quote_time'),
        liveDate: sql<string>`(
          SELECT to_char(spot_date, 'YYYY-MM-DD') FROM fx_spot WHERE pair_key = ${fxPairs.key}
        )`.as('live_date'),
      })
      .from(fxPairs)
      // sort_order 在写入时已按「主要货币在前」排好，直接用它就是稳定顺序
      .orderBy(fxPairs.sortOrder)

    return rows.map(r => {
      const unit = r.quoteUnit ?? 1

      const midClose  = toDisplay(toNum(r.midClose), unit)
      const midPrev   = toDisplay(toNum(r.midPrevClose), unit)
      const spotClose = toDisplay(toNum(r.spotClose), unit)
      const spotPrev  = toDisplay(toNum(r.spotPrevClose), unit)

      const mid  = diff(midClose, midPrev)
      const spot = diff(spotClose, spotPrev)

      return {
        key:          r.key,
        baseCode:     r.baseCode,
        baseName:     r.baseName,
        quoteUnit:    unit,
        category:     r.category,
        officialCode: r.officialCode,
        spotCode:     r.spotCode,
        updatedAt:    r.updatedAt,

        mid: {
          date:      r.midDate ?? null,
          close:     midClose,
          prevClose: midPrev,
          change:    mid.change,
          changePct: mid.changePct,
          points:    r.midCount != null ? parseInt(r.midCount, 10) : 0,
        },
        spot: {
          date:      r.spotDate ?? null,
          close:     spotClose,
          prevClose: spotPrev,
          change:    spot.change,
          changePct: spot.changePct,
          earliest:  r.spotEarliest ?? null,
          points:    r.spotCount != null ? parseInt(r.spotCount, 10) : 0,
        },
        live: r.livePrice == null ? null : {
          price:     toDisplay(toNum(r.livePrice), unit),
          prevClose: toDisplay(toNum(r.livePrevClose), unit),
          change:    toDisplay(toNum(r.liveChangeAmt), unit),
          changePct: toNum(r.liveChangePct),
          quoteTime: r.liveQuoteTime ?? null,
          date:      r.liveDate ?? null,
        },
      }
    })
  })

  // ── GET /api/fx/:key ──────────────────────────────────────────────────────
  // 货币对元数据 + 指定口径的历史序列
  // Query: rateType=mid|spot（默认 mid）| days=365 | from=YYYY-MM-DD | to=YYYY-MM-DD
  app.get<{
    Params: { key: string }
    Querystring: { rateType?: string; days?: string; from?: string; to?: string }
  }>('/api/fx/:key', async (req, reply) => {
    const { key } = req.params
    const { rateType = 'mid', days = '365', from, to } = req.query

    if (rateType !== 'mid' && rateType !== 'spot') {
      return reply.code(400).send({ error: `rateType 只能是 'mid' 或 'spot'，收到 '${rateType}'` })
    }

    const [meta] = await db.select().from(fxPairs).where(eq(fxPairs.key, key))
    if (!meta) {
      return reply.code(404).send({ error: `Currency pair '${key}' not found` })
    }

    // 该对实际有哪几个口径（决定前端要不要显示切换按钮）
    const typeRows = await db
      .select({
        rateType: fxRates.rateType,
        n:        sql<string>`COUNT(*)`,
        earliest: sql<string>`to_char(MIN(${fxRates.rateDate}), 'YYYY-MM-DD')`,
        latest:   sql<string>`to_char(MAX(${fxRates.rateDate}), 'YYYY-MM-DD')`,
      })
      .from(fxRates)
      .where(eq(fxRates.pairKey, key))
      .groupBy(fxRates.rateType)

    const byType = Object.fromEntries(
      typeRows.map(t => [t.rateType, {
        points:   parseInt(t.n, 10),
        earliest: t.earliest ?? null,
        latest:   t.latest ?? null,
      }]),
    )

    const toDate = to ?? new Date().toISOString().split('T')[0]
    const fromDate = from ?? (() => {
      const d = new Date(toDate)
      d.setDate(d.getDate() - Math.min(parseInt(days, 10) || 365, 12000))
      return d.toISOString().split('T')[0]
    })()

    const history = await db
      .select({
        date:  sql<string>`to_char(${fxRates.rateDate}, 'YYYY-MM-DD')`,
        open:  fxRates.open,
        high:  fxRates.high,
        low:   fxRates.low,
        close: fxRates.close,
      })
      .from(fxRates)
      .where(and(
        eq(fxRates.pairKey, key),
        eq(fxRates.rateType, rateType),
        gte(fxRates.rateDate, fromDate),
        lte(fxRates.rateDate, toDate),
      ))
      .orderBy(desc(fxRates.rateDate))

    const unit = meta.quoteUnit ?? 1
    const points = history.map(h => ({
      date:  h.date,
      open:  toDisplay(toNum(h.open), unit),   // mid 口径源侧没有开高低 → 保持 null，别用 close 充数
      high:  toDisplay(toNum(h.high), unit),
      low:   toDisplay(toNum(h.low),   unit),
      close: toDisplay(toNum(h.close), unit),
    }))

    const closes = points.map(p => p.close).filter((v): v is number => v != null)
    const rangeChangePct = (closes.length >= 2 && closes[closes.length - 1] !== 0)
      ? Math.round((closes[0] - closes[closes.length - 1]) / closes[closes.length - 1] * 100 * 10000) / 10000
      : null

    return {
      key:          meta.key,
      baseCode:     meta.baseCode,
      baseName:     meta.baseName,
      quoteUnit:    unit,
      category:     meta.category,
      officialCode: meta.officialCode,
      spotCode:     meta.spotCode,
      rateType,
      source:       rateType === 'mid' ? 'cfets' : 'sina',
      updatedAt:    meta.updatedAt,
      availableTypes: byType,          // { mid: {...}, spot: {...} }
      range: { from: fromDate, to: toDate },
      stats: {
        points:        points.length,
        latest:        points.length ? points[0] : null,
        earliest:      points.length ? points[points.length - 1] : null,
        rangeChangePct,
      },
      history: points,
    }
  })
}
