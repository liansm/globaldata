import { FastifyInstance } from 'fastify'
import { db } from '../db'
import {
  dramProducts, dramPrices, gpuProducts, gpuPrices,
  storageProducts, storagePrices,
} from '../db/schema'
import { eq, gte, asc, inArray, and, desc } from 'drizzle-orm'

const toNum = (v: string | null | undefined) => (v == null ? null : parseFloat(v))

/**
 * AI 硬件 —— 两块**性质完全不同**的价格，页面必须分开讲：
 *   ① /api/ai/dram  DRAM 颗粒**现货报价**（美元/颗，源 dram.com.cn）
 *   ② /api/ai/gpu   GPU **云租价**（美元/卡·小时，源 aimultiple 聚合各家云厂商）
 *
 * ⚠ 口径陷阱（改这个文件前先看）：
 *   · 颗粒只回 DDR5 / DDR4。DDR3 库里还有历史（DDR3 只是不再抓），这里过滤掉，
 *     免得页面多出一组没人看的序列。
 *   · `density_gb` 存的是 **Gb（Gigabit）**，换算 GB 要 ÷8。$/GB 才算得对：
 *     DDR5 24Gb $58.50 → $19.50/GB；若直接 ÷24 会得到 $2.44/GB，差 8 倍。
 *   · GPU 的 `price_hourly` 已是**每卡**价（整节点报价落库时已 ÷ 节点卡数）。
 *   · `provider='market_min'` 是源站口径的「单卡最低价」，不含整节点摊销价 ——
 *     所以会出现「最低 $3.67，但明细里 Vultr 摊 $3.20」，这是正常的，前端要说清。
 *   · GPU 是**租价**不是买断价；买断参考价在 unitPrice* 字段，静态、非日更。
 */
export async function aiRoutes(app: FastifyInstance) {

  // ── GET /api/ai/dram ──────────────────────────────────────────────────────
  // 全部颗粒品种（DDR5 / DDR4）+ 最新报价 + 涨跌 + 走势序列
  //
  // ⚠ 报价日不连续（周末/节假日无报价），**只回源站给了的日期**，
  //   绝不按自然日补齐或前值填充 —— 补出来的点是假的。
  // ⚠ 源站历史表没有最高价，`high` 一律为 null，前端不要用 close 冒充。
  app.get('/api/ai/dram', async (req) => {
    const q = req.query as { days?: string }
    const days = Math.min(Math.max(parseInt(q.days ?? '500', 10) || 500, 30), 3650)
    const from = new Date(Date.now() - days * 86400_000).toISOString().slice(0, 10)

    const rows = await db
      .select({
        key:        dramProducts.key,
        name:       dramProducts.name,
        generation: dramProducts.generation,
        densityGb:  dramProducts.densityGb,
        speed:      dramProducts.speed,
        grade:      dramProducts.grade,
        unit:       dramProducts.unit,
        source:     dramProducts.source,
        priceDate:  dramPrices.priceDate,
        open:       dramPrices.open,
        close:      dramPrices.close,
        low:        dramPrices.low,
      })
      .from(dramProducts)
      .innerJoin(dramPrices, eq(dramPrices.key, dramProducts.key))
      .where(and(
        inArray(dramProducts.generation, ['DDR5', 'DDR4']),
        gte(dramPrices.priceDate, from),
      ))
      // ⚠ 方向写在这里，外面不要再套 desc()（项目记忆「drizzle 三坑」②）
      .orderBy(asc(dramProducts.key), asc(dramPrices.priceDate))

    type Series = { date: string; close: number; perGb: number | null; low: number | null; open: number | null }
    const map = new Map<string, {
      key: string; name: string; generation: string; densityGb: number | null
      speed: string | null; grade: string; unit: string; source: string
      series: Series[]
    }>()

    for (const r of rows) {
      let item = map.get(r.key)
      if (!item) {
        item = {
          key: r.key, name: r.name, generation: r.generation,
          densityGb: r.densityGb, speed: r.speed, grade: r.grade,
          unit: r.unit, source: r.source, series: [],
        }
        map.set(r.key, item)
      }
      const close = toNum(r.close)
      if (close == null) continue      // 收盘缺失的行不进序列（源站极少，但不排除）
      // densityGb 是 Gb，÷8 得 GB；颗粒少于 1Gb 或缺失时不折算，避免出现荒谬倍数
      const gb = r.densityGb && r.densityGb > 0 ? r.densityGb / 8 : null
      item.series.push({
        date:  String(r.priceDate),
        close,
        perGb: gb ? Math.round((close / gb) * 100) / 100 : null,
        low:   toNum(r.low),
        open:  toNum(r.open),
      })
    }

    return [...map.values()].map((p) => {
      const s = p.series
      const last = s[s.length - 1]
      const prev = s[s.length - 2]
      const change = last && prev ? last.close - prev.close : null
      const changePct = change != null && prev && prev.close !== 0
        ? (change / prev.close) * 100
        : null
      // 每 GB 的区间涨跌与「每颗」完全相同（密度是常数），不重复给百分比，
      // 只给两端绝对值，前端做横向比较用。
      const perGbs = s.map(x => x.perGb).filter((x): x is number => x != null)
      return {
        ...p,
        latestDate:  last?.date ?? null,
        latestClose: last?.close ?? null,
        latestPerGb: last?.perGb ?? null,
        latestLow:   last?.low ?? null,
        latestOpen:  last?.open ?? null,
        prevClose:   prev?.close ?? null,
        // 源站给的「涨跌幅度」是绝对美元差额，这里统一按相邻两个收盘现算，
        // 避免出现「页面显示 +0.30 但百分比算不出来」的两套口径
        change:      change == null ? null : Math.round(change * 1000) / 1000,
        changePct:   changePct == null ? null : Math.round(changePct * 100) / 100,
        perGbMin:    perGbs.length ? Math.min(...perGbs) : null,
        perGbMax:    perGbs.length ? Math.max(...perGbs) : null,
        points:      s.length,
        firstDate:   s[0]?.date ?? null,
      }
    })
  })

  // ── GET /api/ai/dram/:key ─────────────────────────────────────────────────
  // 单品种明细（同一条序列 + 统计），便于将来做深链
  app.get('/api/ai/dram/:key', async (req, reply) => {
    const { key } = req.params as { key: string }
    const meta = await db
      .select()
      .from(dramProducts)
      .where(eq(dramProducts.key, key))
      .limit(1)
    if (meta.length === 0) return reply.code(404).send({ found: false, key })

    const rows = await db
      .select({
        date:  dramPrices.priceDate,
        open:  dramPrices.open,
        close: dramPrices.close,
        low:   dramPrices.low,
      })
      .from(dramPrices)
      .where(eq(dramPrices.key, key))
      .orderBy(asc(dramPrices.priceDate))

    const m = meta[0]
    const gb = m.densityGb && m.densityGb > 0 ? m.densityGb / 8 : null
    const series = rows
      .map(r => {
        const close = toNum(r.close)
        return {
          date:  String(r.date),
          open:  toNum(r.open),
          close,
          perGb: close != null && gb ? Math.round((close / gb) * 100) / 100 : null,
          low:   toNum(r.low),
        }
      })
      .filter(r => r.close != null)

    const closes = series.map(s => s.close as number)
    return {
      found: true,
      key: m.key, name: m.name, generation: m.generation,
      densityGb: m.densityGb, speed: m.speed, grade: m.grade,
      unit: m.unit, source: m.source,
      series,
      stats: closes.length
        ? {
            points: series.length,
            firstDate: series[0].date,
            lastDate: series[series.length - 1].date,
            min: Math.min(...closes),
            max: Math.max(...closes),
            latest: closes[closes.length - 1],
          }
        : null,
      // 序列长度变化要能解释：源站半年窗口约 120~125 点，明显少于这个数就是有洞
    }
  })

  // ── GET /api/ai/gpu ───────────────────────────────────────────────────────
  // GPU 型号 + 最新云租价 + 走势（market_min 序列）+ 各云厂商最新明细
  //
  // ⚠ 走势用 market_min 而不是某一家：能凑成日序列的只有它。
  //   逐家明细目前只有源站「当前选中型号」（B200 SXM）有，别的型号 providers 为空，
  //   这是上游给的数据范围，不是抓漏。
  app.get('/api/ai/gpu', async (req) => {
    const q = req.query as { days?: string }
    const days = Math.min(Math.max(parseInt(q.days ?? '365', 10) || 365, 7), 3650)
    const from = new Date(Date.now() - days * 86400_000).toISOString().slice(0, 10)

    const products = await db
      .select()
      .from(gpuProducts)
      .orderBy(asc(gpuProducts.sortOrder))

    const priceRows = await db
      .select({
        key:         gpuPrices.key,
        priceDate:   gpuPrices.priceDate,
        provider:    gpuPrices.provider,
        region:      gpuPrices.region,
        priceHourly: gpuPrices.priceHourly,
        gpusPerNode: gpuPrices.gpusPerNode,
      })
      .from(gpuPrices)
      .where(gte(gpuPrices.priceDate, from))
      .orderBy(asc(gpuPrices.key), asc(gpuPrices.priceDate))

    type GpuSeries = { date: string; price: number }
    const seriesMap = new Map<string, GpuSeries[]>()
    // provider → 最新一天的那一行（同一天多行时后写的覆盖前面的）
    const providerMap = new Map<string, Map<string, {
      provider: string; region: string; priceHourly: number; gpusPerNode: number | null; date: string
    }>>()

    for (const r of priceRows) {
      const price = toNum(r.priceHourly)
      if (price == null) continue
      if (r.provider === 'market_min') {
        const arr = seriesMap.get(r.key) ?? []
        arr.push({ date: String(r.priceDate), price })
        seriesMap.set(r.key, arr)
        continue
      }
      const m = providerMap.get(r.key) ?? new Map()
      const prev = m.get(r.provider)
      // 同一 provider 只保留最新日期的报价（priceRows 按日期升序）
      if (!prev || String(r.priceDate) >= prev.date) {
        m.set(r.provider, {
          provider: r.provider,
          region: r.region ?? '',
          priceHourly: price,
          gpusPerNode: r.gpusPerNode,
          date: String(r.priceDate),
        })
      }
      providerMap.set(r.key, m)
    }

    return products.map((p) => {
      const s = seriesMap.get(p.key) ?? []
      const last = s[s.length - 1]
      const prev = s[s.length - 2]
      const changePct = last && prev && prev.price !== 0
        ? ((last.price - prev.price) / prev.price) * 100
        : null
      const ps = s.map(x => x.price)
      const providers = [...(providerMap.get(p.key)?.values() ?? [])]
        .map(({ provider, region, priceHourly, gpusPerNode }) =>
          ({ provider, region, priceHourly, gpusPerNode }))
        .sort((a, b) => a.priceHourly - b.priceHourly)
      return {
        key:          p.key,
        name:         p.name,
        vendor:       p.vendor,
        vramGb:       toNum(p.vramGb),
        bandwidthTbs: toNum(p.bandwidthTbs),
        // 单卡买断参考价：NULL 表示尚无公开报价（Rubin），前端要显示「暂无」而不是 0
        unitPriceLow:  toNum(p.unitPriceLow),
        unitPriceHigh: toNum(p.unitPriceHigh),
        unitPriceNote: p.unitPriceNote,
        unitPriceAsof: p.unitPriceAsof,
        unitPriceSrc:  p.unitPriceSrc,
        latestDate:   last?.date ?? null,
        latestHourly: last?.price ?? null,
        prevHourly:   prev?.price ?? null,
        changePct:    changePct == null ? null : Math.round(changePct * 100) / 100,
        minHourly:    ps.length ? Math.min(...ps) : null,
        maxHourly:    ps.length ? Math.max(...ps) : null,
        points:       s.length,
        firstDate:    s[0]?.date ?? null,
        series:       s,
        providers,
      }
    })
  })

  // ── GET /api/ai/gpu/:key ──────────────────────────────────────────────────
  // 单型号全历史（含逐家明细的全部日期），供详情页 / 深链用
  app.get('/api/ai/gpu/:key', async (req, reply) => {
    const { key } = req.params as { key: string }
    const meta = await db.select().from(gpuProducts).where(eq(gpuProducts.key, key)).limit(1)
    if (meta.length === 0) return reply.code(404).send({ found: false, key })

    const rows = await db
      .select()
      .from(gpuPrices)
      .where(eq(gpuPrices.key, key))
      .orderBy(asc(gpuPrices.priceDate), desc(gpuPrices.priceHourly))

    const m = meta[0]
    return {
      found: true,
      key: m.key, name: m.name, vendor: m.vendor,
      vramGb: toNum(m.vramGb), bandwidthTbs: toNum(m.bandwidthTbs),
      unitPriceLow:  toNum(m.unitPriceLow),
      unitPriceHigh: toNum(m.unitPriceHigh),
      unitPriceNote: m.unitPriceNote,
      unitPriceAsof: m.unitPriceAsof,
      series: rows.map(r => ({
        date:         String(r.priceDate),
        provider:     r.provider,
        region:       r.region,
        priceHourly:  toNum(r.priceHourly),
        gpusPerNode:  r.gpusPerNode,
      })),
    }
  })

  // ── GET /api/ai/storage ───────────────────────────────────────────────────
  // 存储成品盘 + NAND 颗粒（源 dram.com.cn，与 DRAM 同站同接口不同 sortid）
  //   nand_wafer  NAND 晶圆/颗粒   USD/颗   容量是 Gb，落库已折 GB
  //   ssd_oem     白牌整盘         USD/个
  //   ssd_street  零售品牌盘       CNY/个   ← 66 个品种，另给一条加权均价曲线
  //   hdd         机械盘           CNY/个   ← 半年只有 3~4 个点
  //
  // ⚠ `capacity_gb` 是 **GB（已归一化）**，每 GB 价 = close / capacity_gb，
  //   **不要像 dram 那样再 ÷8** —— 两个表语义相反，混了会差 8 倍。
  // ⚠ `currency` 是按分类校正过的，别拿站方标记当准（详见脚本 CCY_REASON）。
  // ⚠ `avg_ssd_street` 是**虚拟品种**（库里没有），按当日有报价的品种做容量加权：
  //   Σ价格 / Σ容量。它反映「买 1GB 平均花多少钱」，不做前值填充，
  //   参与品种数随上游报价日浮动 → 每个点带 `samples`，前端要能说明这一点。
  app.get('/api/ai/storage', async (req) => {
    const q = req.query as { days?: string }
    const days = Math.min(Math.max(parseInt(q.days ?? '500', 10) || 500, 30), 3650)
    const from = new Date(Date.now() - days * 86400_000).toISOString().slice(0, 10)

    const products = await db
      .select()
      .from(storageProducts)
      .orderBy(asc(storageProducts.category), asc(storageProducts.sortOrder), asc(storageProducts.key))

    const rows = await db
      .select({
        key:       storagePrices.key,
        priceDate: storagePrices.priceDate,
        open:      storagePrices.open,
        close:     storagePrices.close,
        low:       storagePrices.low,
      })
      .from(storagePrices)
      .where(gte(storagePrices.priceDate, from))
      // ⚠ 方向写这里，外面别再套 desc()（drizzle 坑②）
      .orderBy(asc(storagePrices.key), asc(storagePrices.priceDate))

    type Pt = { date: string; close: number; perGb: number | null; low: number | null; open: number | null }
    const seriesMap = new Map<string, Pt[]>()
    for (const r of rows) {
      const close = toNum(r.close)
      if (close == null) continue
      const arr = seriesMap.get(r.key) ?? []
      arr.push({ date: String(r.priceDate), close, perGb: null, low: toNum(r.low), open: toNum(r.open) })
      seriesMap.set(r.key, arr)
    }

    // 每 GB 价在这里补（依赖品类自己的 capacity_gb，不能用全局常量）
    const items = products
      .map((p) => {
        const capGb = toNum(p.capacityGb)
        const s = (seriesMap.get(p.key) ?? []).map(pt => ({
          ...pt,
          perGb: capGb && capGb > 0 ? Math.round((pt.close / capGb) * 10000) / 10000 : null,
        }))
        const last = s[s.length - 1]
        const prev = s[s.length - 2]
        const changePct = last && prev && prev.close !== 0
          ? ((last.close - prev.close) / prev.close) * 100
          : null
        return {
          key:          p.key,
          name:         p.name,
          displayName:  p.displayName,
          category:     p.category,
          brand:        p.brand,
          capacityGb:   capGb,
          capacityRaw:  p.capacityRaw,
          formFactor:   p.formFactor,
          interface:    p.interface,
          dtype:        p.dtype,
          usage:        p.usage,
          currency:     p.currency,
          unit:         p.unit,
          currencyNote: p.currencyNote,
          virtual:      false,
          latestDate:   last?.date ?? null,
          latestClose:  last?.close ?? null,
          latestPerGb:  last?.perGb ?? null,
          prevClose:    prev?.close ?? null,
          changePct:    changePct == null ? null : Math.round(changePct * 100) / 100,
          points:       s.length,
          firstDate:    s[0]?.date ?? null,
          series:       s,
        }
      })
      .filter(it => it.points > 0)   // 一条价都没抓到的品种不进页面（但库里留着）

    // ── 虚拟品种：SSD 零售容量加权指数 ──
    //
    // ⚠⚠ 这里**不能**按「当天有报价的品种」直接做 Σ价格/Σ容量 —— 实测会得到一条锯齿：
    //   上游对不同品牌录价的日期不统一，参与品种数在 10 / 44 / 58 之间反复跳，
    //   而「58 个那天」天然含更多便宜白牌 → 曲线在 ¥0.88~1.03 之间跳，
    //   **跳的是篮子成分，不是价格**。这不是噪声，是假信号，会直接误导人。
    //
    // 改成**链式加权指数**（标准价格指数做法）：
    //   每日只取「当日与前一日报价日**都有报价**」的品种，算容量加权价比 a/b，
    //   再链式累乘 index *= a/b。品种进出不会改变 index 的连续性，
    //   只有同一批品种的价格变化会。最后用首日实际加权均价标定量级。
    const street = items.filter(i => i.category === 'ssd_street' && i.capacityGb && i.capacityGb > 0)
    if (street.length) {
      const capOf = new Map(street.map(p => [p.key, p.capacityGb as number]))
      const perDate = new Map<string, Map<string, number>>()   // date → key → perGb
      for (const p of street) {
        for (const pt of p.series) {
          if (pt.perGb == null) continue
          let m = perDate.get(pt.date)
          if (!m) { m = new Map(); perDate.set(pt.date, m) }
          m.set(p.key, pt.perGb)
        }
      }
      const dates = [...perDate.keys()].sort()

      let idx = 1
      const raw: { date: string; idx: number; samples: number }[] = []
      for (let k = 0; k < dates.length; k++) {
        const cur = perDate.get(dates[k])!
        if (k > 0) {
          const prev = perDate.get(dates[k - 1])!
          let a = 0
          let b = 0
          for (const [key, v] of cur) {
            const pv = prev.get(key)
            if (pv == null) continue          // 只在**两日都有报价**的品种上算，缺一天就跳过
            const c = capOf.get(key)!
            a += c * v
            b += c * pv
          }
          if (b > 0) idx *= a / b
        }
        // samples = 当日报价品种数（只作覆盖度参考，**不参与**指数计算）
        raw.push({ date: dates[k], idx, samples: cur.size })
      }

      // 标定：让首日指数等于「首日实际加权均价」，这样量级仍可读成 ¥/GB
      const firstMap = perDate.get(dates[0])!
      let s0 = 0
      let c0 = 0
      for (const [key, v] of firstMap) {
        const c = capOf.get(key)!
        s0 += c * v
        c0 += c
      }
      const base = c0 > 0 ? s0 / c0 : 1

      const series = raw.map(r => {
        const perGb = r.idx * base
        return {
          date: r.date,
          close: perGb,                 // 虚拟品种没有"单价"，close 与 perGb 同值；
          perGb: Math.round(perGb * 10000) / 10000,
          low: null,
          open: null,
          samples: r.samples,
        }
      })

      const last = series[series.length - 1]
      const prev = series[series.length - 2]
      const first = series[0]
      items.push({
        key:          'avg_ssd_street',
        name:         'SSD 零售均价（容量加权链式指数）',
        displayName:  'SSD 零售均价',
        category:     'ssd_street',
        brand:        null,
        capacityGb:   null,
        capacityRaw:  null,
        formFactor:   null,
        interface:    null,
        dtype:        null,
        usage:        null,
        currency:     'CNY',
        unit:         'CNY/GB',
        currencyNote: '由零售品牌盘逐日链式加权（只用当日与前一日报价日都有价的品种），非上游直接给出',
        virtual:      true,
        latestDate:   last?.date ?? null,
        latestClose:  last?.close ?? null,
        latestPerGb:  last?.perGb ?? null,
        prevClose:    prev?.close ?? null,
        changePct:    last && prev && prev.perGb
          ? Math.round(((last.perGb - prev.perGb) / prev.perGb) * 10000) / 100
          : null,
        points:       series.length,
        firstDate:    first?.date ?? null,
        series,
      } as (typeof items)[number])
    }

    // 分类目录（前端分组唯一出处 = 后端，加品类只改脚本 STORAGE_CATS）
    const CAT_META: Record<string, { label: string; cadence: string }> = {
      nand_wafer: { label: 'NAND 闪存颗粒', cadence: '周级' },
      ssd_oem:    { label: 'SSD 白牌 OEM',  cadence: '周级' },
      ssd_street: { label: 'SSD 零售品牌',  cadence: '日更' },
      hdd:        { label: 'HDD 机械硬盘',  cadence: '季更' },
    }
    const categories = ['nand_wafer', 'ssd_oem', 'ssd_street', 'hdd']
      .map((k) => {
        const list = items.filter(i => i.category === k)
        const dates = list.map(i => i.latestDate).filter(Boolean) as string[]
        return {
          key: k,
          label: CAT_META[k].label,
          cadence: CAT_META[k].cadence,
          count: list.filter(i => !i.virtual).length,
          currency: list[0]?.currency ?? null,
          latestDate: dates.length ? dates.sort()[dates.length - 1] : null,
        }
      })
      .filter(c => c.count > 0)

    return { categories, items }
  })

  // ── GET /api/ai/storage/:key ──────────────────────────────────────────────
  // 单品种全历史（虚拟品种 avg_ssd_street 走 /storage 列表，不在这里）
  app.get('/api/ai/storage/:key', async (req, reply) => {
    const { key } = req.params as { key: string }
    const meta = await db.select().from(storageProducts).where(eq(storageProducts.key, key)).limit(1)
    if (meta.length === 0) return reply.code(404).send({ found: false, key })

    const rows = await db
      .select()
      .from(storagePrices)
      .where(eq(storagePrices.key, key))
      .orderBy(asc(storagePrices.priceDate))

    const m = meta[0]
    const capGb = toNum(m.capacityGb)
    const series = rows
      .map(r => {
        const close = toNum(r.close)
        return {
          date:  String(r.priceDate),
          open:  toNum(r.open),
          close,
          low:   toNum(r.low),
          perGb: close != null && capGb && capGb > 0
            ? Math.round((close / capGb) * 10000) / 10000
            : null,
        }
      })
      .filter(r => r.close != null)

    const closes = series.map(s => s.close as number)
    return {
      found: true,
      key: m.key, name: m.name, displayName: m.displayName, category: m.category,
      brand: m.brand, capacityGb: capGb, capacityRaw: m.capacityRaw,
      formFactor: m.formFactor, interface: m.interface, dtype: m.dtype, usage: m.usage,
      currency: m.currency, unit: m.unit, currencyNote: m.currencyNote,
      series,
      stats: closes.length
        ? {
            points: series.length,
            firstDate: series[0].date,
            lastDate: series[series.length - 1].date,
            min: Math.min(...closes),
            max: Math.max(...closes),
            latest: closes[closes.length - 1],
          }
        : null,
    }
  })
}
