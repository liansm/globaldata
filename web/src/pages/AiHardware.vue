<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { TooltipComponent, GridComponent, DataZoomComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { fetchDram, fetchGpu, fetchStorage } from '@/api/ai'
import AnnotatedLineChart from '@/components/AnnotatedLineChart.vue'
import type { DramProduct, GpuProduct, StorageItem, StorageCategoryMeta } from '@/types/ai'

use([CanvasRenderer, LineChart, TooltipComponent, GridComponent, DataZoomComponent])

// ── AI 硬件 ──────────────────────────────────────────────────────────────────
// 两个 tab，**数据性质完全不同**：
//   ① 存储 —— dram.com.cn 的**真实现货报价**（DDR 颗粒 / NAND 颗粒 / SSD / HDD）
//   ② GPU 算力 —— 各家云厂商的**公开租价**（美元/卡·小时）
//
// ⚠ 四个容易搞错的地方，页面上都写了提示：
//   · 颗粒是"颗"不是"条"：DDR5 16Gb = 2GB 一颗，做 16GB 内存条要 8 颗；
//     所以主口径用 **每 GB 价**，用「每颗」横向比会被容量差异带偏。
//   · NAND 的 `1Tb` 是 **bit**（=128GB），接口已折好 GB —— 前端不要再 ÷8。
//   · SSD 零售有 66 个品牌盘，页面只出一条**容量加权均价**，不逐条画；
//     HDD 半年只有 3~4 个点，曲线接近水平线**不是抓漏**，是上游不调价。
//   · GPU 的 $/卡·小时 是**租价**，不是买断价；CPO / 光模块**没有**公开报价。

const route = useRoute()
const router = useRouter()

type TabName = 'memory' | 'gpu'
const tab = ref<TabName>(route.query.tab === 'gpu' ? 'gpu' : 'memory')
watch(() => route.query.tab, t => { tab.value = t === 'gpu' ? 'gpu' : 'memory' })
function onTabChange(name: string | number) {
  const wantGpu = name === 'gpu'
  if (wantGpu === (route.query.tab === 'gpu')) return
  router.replace(wantGpu
    ? { name: 'ai-hardware', query: { tab: 'gpu' } }
    : { name: 'ai-hardware' })
}

// ── 数据 ─────────────────────────────────────────────────────────────────────
const dram = ref<DramProduct[]>([])
const storage = ref<StorageItem[]>([])
const storageCats = ref<StorageCategoryMeta[]>([])
const gpus = ref<GpuProduct[]>([])
const loading = ref(false)
const error = ref('')

onMounted(async () => {
  loading.value = true
  try {
    // 三条线互不阻断：GPU 抓不到不该把存储一起打成空白
    const [dramRows, stoRes, gpuRows] = await Promise.all([
      fetchDram().catch(() => [] as DramProduct[]),
      fetchStorage().catch(() => ({ categories: [], items: [] })),
      fetchGpu().catch(() => [] as GpuProduct[]),
    ])
    dram.value = dramRows
    storage.value = stoRes.items
    storageCats.value = stoRes.categories
    gpus.value = gpuRows
    if (!dramRows.length && !stoRes.items.length && !gpuRows.length) {
      error.value = '数据加载失败，请检查后端服务（localhost:3000）是否启动'
    }
  } finally {
    loading.value = false
  }
})

// ═══════════════════════════════════════════════════════════════════════════
// ① 存储（DDR 颗粒 + NAND 颗粒 + SSD + HDD）
// ═══════════════════════════════════════════════════════════════════════════
// 六个分组口径完全不同（美元/颗 · 美元/个 · 人民币/个），这里统一成 PriceItem，
// 渲染与图表只写一套。**主口径一律「每 GB 价」** —— 只有它跨品类可比。

interface PriceItem {
  key:         string
  group:       string         // 分组 key（决定归属哪一段）
  title:       string
  badge:       string | null
  virtual:     boolean        // 接口合成的均价（没有单价、不能切口径）
  currency:    'USD' | 'CNY'
  perGb:       number | null
  price:       number | null
  priceUnit:   string         // '/颗' | '/个' | ''
  changePct:   number | null
  date:        string | null
  points:      number
  sortCap:     number         // 容量 GB，用于组内**容量降序**（大容量在前）
  meta:        string         // 规格行
  series:      { date: string; perGb: number | null; close: number }[]
}

// ⚠ perGb / capacityGb 都已由接口算好，这里**只做映射、不做二次换算**
function dramToItem(d: DramProduct): PriceItem {
  const gb = d.densityGb ? d.densityGb / 8 : null
  return {
    key: d.key,
    group: d.generation === 'DDR5' ? 'ddr5' : 'ddr4',
    title: d.name,
    badge: d.grade === 'ett' ? '白牌' : null,
    virtual: false,
    currency: 'USD',
    perGb: d.latestPerGb,
    price: d.latestClose,
    priceUnit: '/颗',
    changePct: d.changePct,
    date: d.latestDate,
    points: d.points,
    sortCap: gb ?? 0,
    meta: `${d.generation} · ${d.densityGb ?? '—'}Gb（≈${gb ? gb.toFixed(1) : '—'}GB）· `
        + `${d.speed ?? '—'} · ${d.grade === 'ett' ? 'eTT 白牌' : '原厂正片'}`,
    series: d.series.map(s => ({ date: s.date, perGb: s.perGb, close: s.close })),
  }
}

function storageToItem(s: StorageItem): PriceItem {
  const isWafer = s.category === 'nand_wafer'
  const priceUnit = s.virtual ? '' : (s.unit.includes('chip') ? '/颗' : '/个')
  // 规格行按品类拼：颗粒讲 dtype/晶圆，整盘讲接口/用途
  let meta: string
  if (s.virtual) {
    meta = '由零售品牌盘逐日链式加权（只用相邻两日都有报价的品种），非上游直接给出'
  } else if (isWafer) {
    const gb = s.capacityGb
    meta = `${s.dtype ?? '—'} · ${s.capacityRaw ?? '—'} 颗粒`
         + `${gb ? `（=${gb}GB）` : ''} · 晶圆`
  } else if (s.category === 'hdd') {
    meta = `${s.brand ?? '—'} · ${s.capacityRaw ?? '—'} · ${s.usage ?? '—'}盘 · ${s.formFactor ?? '—'} 寸`
  } else {
    meta = `${s.brand ?? '—'} · ${s.capacityRaw ?? '—'} · ${s.interface ?? '—'}`
         + `${s.formFactor ? ` · ${s.formFactor}` : ''}`
  }
  return {
    key: s.key,
    group: s.category,
    title: s.displayName,
    badge: s.virtual ? '加权均价' : (s.category === 'hdd' ? (s.usage ?? null) : s.dtype),
    virtual: s.virtual,
    currency: s.currency,
    perGb: s.latestPerGb,
    price: s.latestClose,
    priceUnit,
    changePct: s.changePct,
    date: s.latestDate,
    points: s.points,
    sortCap: s.capacityGb ?? 0,
    meta,
    series: s.series.map(p => ({ date: p.date, perGb: p.perGb, close: p.close })),
  }
}

const allItems = computed<PriceItem[]>(() => [
  ...dram.value.map(dramToItem),
  ...storage.value.map(storageToItem),
])

// 分组顺序：DDR 两代在前（历史最长、最常看），NAND 次之，接着整盘，HDD 最后（季更）
const GROUP_ORDER: { key: string; title: string; note: string; open: boolean }[] = [
  { key: 'ddr5', title: 'DDR5 存储颗粒', note: '服务器 / 新一代 PC 主流。24Gb 大容量颗粒每 GB 反而更便宜', open: true },
  { key: 'ddr4', title: 'DDR4 存储颗粒', note: '存量产线仍在出货。每 GB 价已反超 DDR5 —— 原厂转产 DDR5 后供给收缩', open: true },
  { key: 'nand_wafer', title: 'NAND 闪存颗粒', note: '晶圆 / 颗粒报价，美元计价。SSD 的成本源头，比成品盘早一步反映涨跌', open: true },
  { key: 'ssd_oem', title: 'SSD 白牌 OEM', note: '无品牌整盘，美元计价，$/GB 明显低于零售品牌盘', open: false },
  { key: 'ssd_street', title: 'SSD 零售品牌', note: '66 个品牌盘合成一条容量加权均价 —— 逐条画品牌线会把趋势淹没在噪声里', open: true },
  { key: 'hdd', title: 'HDD 机械硬盘', note: '人民币计价。上游几乎不调价，半年只有 3~4 个报价点，曲线接近水平线属正常', open: false },
]

const expanded = ref<Set<string>>(new Set(GROUP_ORDER.filter(g => g.open).map(g => g.key)))
function toggleGroup(k: string) {
  const s = new Set(expanded.value)
  s.has(k) ? s.delete(k) : s.add(k)
  expanded.value = s
}

const groups = computed(() => {
  const byKey = new Map<string, PriceItem[]>()
  for (const it of allItems.value) {
    // ⚠ SSD 零售有 66 个品牌盘，卡片列表里**只出「均价」一张**：
    //   逐条铺 66 张会把别的品类挤出视野，而且这批里有停更的（MG麦光/威士奇停在 8 月、
    //   海盗船只 1 个点），铺出来只会被当成"当前价"。数据仍全在库里、接口也全返回，
    //   要查单个型号看接口 /api/ai/storage。
    if (it.group === 'ssd_street' && !it.virtual) continue
    const arr = byKey.get(it.group) ?? []
    arr.push(it)
    byKey.set(it.group, arr)
  }
  const catMap = new Map(storageCats.value.map(c => [c.key as string, c]))

  return GROUP_ORDER
    .map((g) => {
      const raw = byKey.get(g.key) ?? []
      // 组内排序：均价卡置顶（它代表整组），其余**按容量降序**（16GB → 8GB → 4GB），
      // 同容量正片在前、白牌在后。⚠ 不能按 points 或标题字符串排 ——
      // 字符串会把 16Gb 排在 4Gb 前面但把 8Gb 排到最后（"1"<"4"<"8"），一眼就乱。
      const items = [...raw].sort((a, b) => {
        if (a.virtual) return -1
        if (b.virtual) return 1
        if (b.sortCap !== a.sortCap) return b.sortCap - a.sortCap
        // 同容量：原厂正片（无「白牌」徽标）在前
        const ae = a.badge === '白牌' ? 1 : 0
        const be = b.badge === '白牌' ? 1 : 0
        return ae - be || a.title.localeCompare(b.title)
      })
      const cat = catMap.get(g.key)
      return {
        ...g,
        items,
        cadence: cat?.cadence ?? '',
        // 品种数以接口为准：SSD 零售是 66，卡片只有 1 张，不能拿 items.length 冒充
        realCount: cat?.count ?? items.length,
        latest: items.reduce<string | null>((acc, i) => (i.date && (!acc || i.date > acc) ? i.date : acc), null),
      }
    })
    .filter(g => g.items.length > 0)
})

const selectedKey = ref<string>('')
const selected = computed<PriceItem | null>(() => {
  const hit = allItems.value.find(d => d.key === selectedKey.value)
  return hit ?? allItems.value[0] ?? null
})
watch(allItems, rows => {
  if (!rows.length || selectedKey.value) return
  // 默认选 DDR5 24Gb（涨价最猛、最能体现「每 GB 更便宜」）；
  // 没有 DDR 数据时退回第一条
  selectedKey.value = rows.some(d => d.key === 'ddr5_24gb_5200')
    ? 'ddr5_24gb_5200'
    : rows[0].key
})

// 图表口径：每 GB（默认，跨品类可比）↔ 每颗/每个（源站原始口径）
const dramUnit = ref<'perGb' | 'perChip'>('perGb')

// 区间：源站只有滚动半年，所以最多就是「全部」，其余是切片
const RANGES = [
  { key: 'all', label: '全部（半年窗口）', days: 9999 },
  { key: '3m',  label: '近 3 个月', days: 92 },
  { key: '1m',  label: '近 1 个月', days: 31 },
] as const
const dramRange = ref<'all' | '3m' | '1m'>('all')

const dramPoints = computed(() => {
  const p = selected.value
  if (!p) return []
  const cfg = RANGES.find(r => r.key === dramRange.value)!
  if (cfg.key === 'all') return p.series
  const cutoff = new Date(Date.now() - cfg.days * 86400_000).toISOString().slice(0, 10)
  return p.series.filter(s => s.date >= cutoff)
})

// 币种符号跟着品类走：NAND/SSD 白牌是美元，SSD 零售/HDD 是人民币
const ccySign = computed(() => (selected.value?.currency === 'CNY' ? '¥' : '$'))
// 「按单位」那一档的单位怎么称呼：颗粒/晶圆是「每颗」，整盘是「每个」
// ⚠ 只用来描述**单价**；「每 GB」是另一档，别混进这个 computed（会两边都错）
const itemUnitLabel = computed(() => (selected.value?.priceUnit === '/颗' ? '每颗' : '每个'))

/**
 * 走势图 option。
 * `lg = true` 是「放大弹窗」那一份：容器宽度约为小栏的 3 倍，所以 x 轴标签、
 * 边距、字号都跟着放大 —— 只把同一份数据画大而不调刻度，宽屏上只会稀稀拉拉几个点，
 * 看起来像数据变少了。
 */
function buildDramOption(lg = false) {
  const pts = dramPoints.value
  if (!pts.length) return {}
  const perGb = dramUnit.value === 'perGb'
  const sign = ccySign.value
  const vals = pts.map(s => (perGb ? s.perGb : s.close)).filter((v): v is number => v != null)
  if (!vals.length) return {}

  // 122 个报价日塞进 408px，标签必然重叠；显式按「最多 6 个」抽稀，
  // 光靠 hideOverlap 不够（它只隐藏，不保证留下的几个间距够）
  const step = Math.max(1, Math.ceil(pts.length / (lg ? 14 : 6)))
  // 刻度精度必须跟着**极差**走，不能只看绝对值：
  //   · 均价曲线整段 ¥0.85~0.99，toFixed(0) 会把 6 个刻度全挤成 "¥1"；
  //   · HDD 的 ¥/GB 在 0.222~0.225 之间，2 位小数会让 6 个刻度全变成 "¥0.23"。
  // 用极差 + 量级两个维度定，才不会出现"刻度全一样"的废图。
  const vMax = Math.max(...vals)
  const vMin = Math.min(...vals)
  const span = vMax - vMin
  const mag = Math.max(Math.abs(vMax), Math.abs(vMin))
  const tickDec = span < 0.01 ? 3 : span < 0.1 ? 2 : mag < 5 ? 2 : mag < 50 ? 1 : 0

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const x = params[0]
        if (!x) return ''
        const pt = pts[x.dataIndex]
        // 两个口径一起给：每 GB 价只在同口径内可比，两个都在才不会误读
        return `${x.axisValue}<br/>`
          + `<b>${sign}${Number(x.value).toFixed(2)}</b> /${perGb ? 'GB' : '单位'}<br/>`
          + `<span style="color:#98a0b0">${
              perGb ? `${sign}${(pt?.close ?? 0).toFixed(2)} /单位` : `${sign}${(pt?.perGb ?? 0).toFixed(2)} /GB`
            }</span>`
      },
    },
    grid: lg
      ? { top: 26, right: 34, bottom: 58, left: 64 }
      : { top: 18, right: 26, bottom: 46, left: 54 },
    xAxis: {
      type: 'category', data: pts.map(s => s.date),
      axisLabel: { rotate: 30, fontSize: lg ? 11.5 : 10.5, color: '#8a94a6', interval: step - 1 },
      axisLine: { lineStyle: { color: '#e4e7ee' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      // 交给 echarts 自己取整刻度：硬设 min/max 会得到 `$21 $20 $18 $16` 这种
      // 不等距刻度（padding 是按极差算的，不是按「好看的数字」算的）
      scale: true,
      splitNumber: 5,
      axisLabel: { fontSize: lg ? 11.5 : 10.5, color: '#8a94a6', formatter: (v: number) => `${sign}${v.toFixed(tickDec)}` },
      splitLine: { lineStyle: { color: '#f0f2f7' } },
    },
    dataZoom: [
      { type: 'inside', throttle: 60 },
      {
        type: 'slider', height: lg ? 20 : 16, bottom: lg ? 10 : 8,
        borderColor: 'transparent', backgroundColor: '#f5f7fb',
        fillerColor: 'rgba(64,158,255,.12)',
        handleStyle: { color: '#409eff' },
        textStyle: { fontSize: 10, color: '#a8b0be' },
      },
    ],
    series: [{
      type: 'line',
      data: pts.map(s => (perGb ? s.perGb : s.close)),
      smooth: false,
      showSymbol: pts.length <= 20,
      symbolSize: 5,
      lineStyle: { width: 2, color: perGb ? '#7c5cff' : '#409eff' },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: perGb ? 'rgba(124,92,255,.20)' : 'rgba(64,158,255,.22)' },
            { offset: 1, color: perGb ? 'rgba(124,92,255,0)' : 'rgba(64,158,255,0)' },
          ],
        },
      },
    }],
  }
}
const dramChartOption = computed(() => buildDramOption(false))

const dramStats = computed(() => {
  const p = selected.value
  const pts = dramPoints.value
  if (!p || !pts.length) return null
  const perGb = dramUnit.value === 'perGb'
  const vals = pts.map(s => (perGb ? s.perGb : s.close)).filter((v): v is number => v != null)
  if (!vals.length) return null
  const first = vals[0]
  const last = vals[vals.length - 1]
  return {
    min: Math.min(...vals),
    max: Math.max(...vals),
    // 区间涨跌 = 首末两点之比，**不是「今年来」** —— 源站只给滚动半年
    periodPct: first ? ((last - first) / first) * 100 : null,
    firstDate: pts[0].date,
    lastDate: pts[pts.length - 1].date,
  }
})

// 均价品种是指数、没有「单价」，切到它的「按单位」会画出一条和每 GB 完全重合的线
// → 选中虚拟品种时强制回到每 GB
watch(selected, (s) => {
  if (s?.virtual) dramUnit.value = 'perGb'
})

// 更新节奏悬殊（日更 ↔ 季更），卡片上直接标出来，免得把「没调价」当成「没抓到」
const CADENCE_BY_GROUP = computed(() => {
  const m = new Map<string, string>()
  for (const c of storageCats.value) m.set(c.key, c.cadence)
  return m
})
function groupCadence(k: string) {
  if (k === 'ddr5' || k === 'ddr4') return '日更'
  return CADENCE_BY_GROUP.value.get(k) ?? ''
}

// ═══════════════════════════════════════════════════════════════════════════
// ② GPU 算力
// ═══════════════════════════════════════════════════════════════════════════
// 顺序与后端 fetch_ai_hardware.py::GPU_MODELS 的 sort_order 一致；卡片按接口顺序渲染，
// 这里只做「默认选中哪一张」。
const gpuSelectedKey = ref<string>('')
const gpuSelected = computed<GpuProduct | null>(() => {
  const hit = gpus.value.find(g => g.key === gpuSelectedKey.value)
  return hit ?? gpus.value[0] ?? null
})
watch(gpus, rows => {
  if (!rows.length || gpuSelectedKey.value) return
  // 默认选 B200：当前 AI 集群的主流选择，也是逐家明细最全的那个
  gpuSelectedKey.value = rows.some(g => g.key === 'b200_sxm') ? 'b200_sxm' : rows[0].key
})

function buildGpuOption(lg = false) {
  const g = gpuSelected.value
  if (!g || g.series.length < 2) return {}
  const vals = g.series.map(s => s.price)
  const minVal = Math.min(...vals)
  const maxVal = Math.max(...vals)
  const padding = (maxVal - minVal) * 0.18 || maxVal * 0.1
  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const x = params[0]
        if (!x) return ''
        return `${x.axisValue}<br/><b>$${Number(x.value).toFixed(2)}</b> /卡·小时`
      },
    },
    grid: lg
      ? { top: 26, right: 34, bottom: 44, left: 70 }
      : { top: 18, right: 26, bottom: 30, left: 56 },
    xAxis: {
      type: 'category', data: g.series.map(s => s.date),
      axisLabel: { rotate: 35, fontSize: lg ? 11.5 : 10.5, color: '#8a94a6', hideOverlap: true },
      axisLine: { lineStyle: { color: '#e4e7ee' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      min: Math.max(0, minVal - padding),
      max: maxVal + padding,
      axisLabel: { fontSize: lg ? 11.5 : 10.5, color: '#8a94a6', formatter: (v: number) => `$${v.toFixed(2)}` },
      splitLine: { lineStyle: { color: '#f0f2f7' } },
      scale: true,
    },
    series: [{
      type: 'line',
      data: vals,
      smooth: false,
      showSymbol: vals.length <= 20,
      symbolSize: 5,
      lineStyle: { width: 2, color: '#0ea5a5' },
      itemStyle: { color: '#0ea5a5' },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: 'rgba(14,165,165,.18)' },
            { offset: 1, color: 'rgba(14,165,165,0)' },
          ],
        },
      },
    }],
  }
}
const gpuChartOption = computed(() => buildGpuOption(false))

// ── 放大查看（弹窗）──────────────────────────────────────────────────────────
// 右栏固定 430px，图只有约 400×272，122 个报价日挤在里面看不清细节。
// 弹窗不是「另外画一张图」，而是同一份 option 换个尺寸重画（buildXxxOption(true)
// 只是把刻度密度、边距、字号调大）—— 口径 / 区间状态与小栏共用，
// 在弹窗里切了区间，小栏也跟着变，不会出现两张图不一致。
const zoomOpen = ref(false)
const zoomIsDram = computed(() => tab.value === 'memory')
const zoomOption = computed(() => (zoomIsDram.value ? buildDramOption(true) : buildGpuOption(true)))
const zoomAvailable = computed(() => Object.keys(zoomOption.value).length > 0)
const zoomTitle = computed(() => zoomIsDram.value
  ? `${selected.value?.title ?? ''}　${dramUnit.value === 'perGb' ? '每 GB 价' : '单位价'}`
  : `${gpuSelected.value?.name ?? ''}　云租价（美元/卡·小时）`)
const zoomSubtitle = computed(() => zoomIsDram.value
  ? `${ccySign.value}${fmt(dramUnit.value === 'perGb' ? selected.value?.perGb : selected.value?.price)}`
    + ` ${dramUnit.value === 'perGb' ? '/GB' : `/${itemUnitLabel.value}`}`
    + ` · ${dramPoints.value.length} 个报价日 · ${fmtDate(dramPoints.value[0]?.date)} → ${fmtDate(dramPoints.value[dramPoints.value.length - 1]?.date)}`
  : `$${fmt(gpuSelected.value?.latestHourly)} /卡·小时 · ${gpuSelected.value?.points ?? 0} 个报价日 · 截至 ${fmtDate(gpuSelected.value?.latestDate)}`)

// ── 展示工具 ─────────────────────────────────────────────────────────────────
function fmt(v: number | null | undefined, d = 2) {
  if (v == null) return '—'
  return v.toLocaleString('zh-CN', { minimumFractionDigits: d, maximumFractionDigits: d })
}
// 每 GB 价跨了三个数量级：DDR 颗粒 $19.50、白牌 SSD $0.185、HDD ¥0.47
// → 小于 1 的用 3 位小数，否则 2 位，否则 $0.19 和 $0.185 看不出差别
function fmtGb(v: number | null | undefined) {
  if (v == null) return '—'
  return fmt(v, Math.abs(v) < 1 ? 3 : 2)
}
function fmtDate(d: string | null | undefined) {
  return d ? d.slice(0, 10) : '—'
}
function fmtPct(v: number | null | undefined) {
  if (v == null) return '—'
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
}
function pctClass(v: number | null | undefined) {
  // 0 走中性灰：DDR4 多数品种长期不动，「+0.00%」涂成红色会让人以为在涨
  if (v == null || v === 0) return ''
  return v > 0 ? 'up' : 'down'
}
// 买断参考价：$27,000~40,000 这种量级，用 k 更易读
function fmtK(v: number | null | undefined) {
  if (v == null) return '—'
  return v >= 10000 ? `$${(v / 1000).toFixed(0)}k` : `$${v.toFixed(0)}`
}
function vramText(g: GpuProduct) {
  if (g.vramGb == null) return 'VRAM 未公布'
  return `${g.vramGb} GB · ${g.vendor}`
}

const latestStoreDate = computed(() => {
  const ds = allItems.value.map(d => d.date).filter(Boolean) as string[]
  return ds.length ? ds.sort()[ds.length - 1] : null
})
// 头部计数只算**真实品种**（115 存储 + 9 DDR），不含接口合成的均价卡
const storeRealCount = computed(() => allItems.value.filter(d => !d.virtual).length)
const latestGpuDate = computed(() => {
  const ds = gpus.value.map(g => g.latestDate).filter(Boolean) as string[]
  return ds.length ? ds.sort()[ds.length - 1] : null
})
const gpuCount = computed(() => gpus.value.filter(g => g.latestHourly != null).length)
</script>

<template>
  <div class="ai-page">
    <header class="page-head">
      <div>
        <h1>AI 硬件</h1>
        <p class="subtitle">
          存储现货（DDR 颗粒 / NAND 颗粒 / SSD / HDD，统一按每 GB 价）
          <span class="dot">·</span> GPU 算力租价（美元/卡·小时）
        </p>
      </div>
      <div class="head-stats">
        <div class="hstat"><b>{{ storeRealCount }}</b><span>存储品种</span></div>
        <div class="hstat"><b>{{ gpuCount }}</b><span>GPU 型号</span></div>
        <div class="hstat"><b>{{ fmtDate(latestStoreDate) }}</b><span>存储报价日</span></div>
        <div class="hstat"><b>{{ fmtDate(latestGpuDate) }}</b><span>GPU 报价日</span></div>
      </div>
    </header>

    <div class="tabs-wrap">
      <el-tabs v-model="tab" class="ai-tabs" @tab-change="onTabChange">
        <!-- ══ ① 存储 ═════════════════════════════════════════════════════ -->
        <el-tab-pane name="memory">
          <template #label><span class="tab-label">💾 存储</span></template>

          <div class="pane split">
            <el-alert
              v-if="error" :title="error" type="error" show-icon :closable="false"
              style="margin-bottom: 14px"
            />

            <div v-if="loading" class="skeleton-wrap"><el-skeleton :rows="8" animated /></div>

            <template v-else-if="allItems.length">
              <!-- 左：分组卡片（可折叠，默认只展开主要的几组） -->
              <div class="dram-list">
                <section v-for="g in groups" :key="g.key" class="group">
                  <div class="group-head clickable" @click="toggleGroup(g.key)">
                    <span class="chev" :class="{ open: expanded.has(g.key) }">▸</span>
                    <h2 class="group-title">{{ g.title }}</h2>
                    <span v-if="g.cadence" class="cadence" :class="g.cadence">{{ g.cadence }}</span>
                    <span class="group-count">
                      <template v-if="g.realCount !== g.items.length">
                        {{ g.realCount }} 品种 · 均价
                      </template>
                      <template v-else>{{ g.items.length }} 档</template>
                    </span>
                  </div>
                  <p v-if="g.note && expanded.has(g.key)" class="group-note">{{ g.note }}</p>

                  <div v-if="expanded.has(g.key)" class="card-grid">
                    <div
                      v-for="item in g.items" :key="item.key"
                      class="card" :class="{ active: item.key === selected?.key, virtual: item.badge === '加权均价' }"
                      @click="selectedKey = item.key"
                    >
                      <div class="card-top">
                        <span class="card-name" :title="item.title">{{ item.title }}</span>
                        <span v-if="item.badge" class="tag-ett">{{ item.badge }}</span>
                      </div>
                      <!-- 主口径：每 GB 价（唯一能跨品类比的口径） -->
                      <div class="card-price">
                        {{ item.currency === 'CNY' ? '¥' : '$' }}{{ fmtGb(item.perGb) }}
                        <span class="card-unit">/GB</span>
                      </div>
                      <div v-if="item.price != null" class="card-sub">
                        {{ item.currency === 'CNY' ? '¥' : '$' }}{{ fmt(item.price) }}
                        <template v-if="item.priceUnit">{{ item.priceUnit }}</template>
                      </div>
                      <div v-else class="card-sub">—</div>
                      <div class="card-pct" :class="pctClass(item.changePct)"
                           title="基于上一报价日；源站周末与节假日不报价，相邻两点可能隔数天">
                        {{ fmtPct(item.changePct) }}
                      </div>
                      <div class="card-footer">
                        <span class="card-date">{{ fmtDate(item.date) }}</span>
                        <span class="card-pts">{{ item.points }} 点</span>
                      </div>
                    </div>
                  </div>
                </section>

                <p class="foot-note">
                  数据源 <code>dram.com.cn</code>（中国IC交易网，深圳现货），四个分类：
                  NAND 颗粒 / SSD 白牌 / SSD 零售 / HDD，与 DDR 颗粒同一个站、同一套接口。<br>
                  <b>主口径一律「每 GB 价」</b> —— 只有它能跨品类比。量级上大致是：
                  NAND 晶圆（裸片）&lt; 白牌 SSD &lt; 零售品牌 SSD ≈ 略高于 HDD。<br>
                  ⚠ <b>颗粒 ≠ 内存条、晶圆 ≠ 成品盘</b>：<code>DDR5 16Gb</code> 是 16Gb（≈2GB）一颗，
                  做 16GB 内存条要 8 颗；NAND 的 <code>1Tb</code> 同样是 bit（=128GB），已折算成 GB 显示。<br>
                  ⚠ <b>更新节奏差两个数量级</b>：DDR / SSD 零售日更，NAND / SSD 白牌周更，
                  <b>HDD 季更</b>（半年只有 3~4 个报价点，曲线接近水平线属正常，不是抓漏）。<br>
                  ⚠ <b>币种按品类校正过</b>：站方那列有 4 条标错（SSD 白牌 3 条标 ￥ 实为美元、
                  HDD 1 条标 $ 实为人民币），已按品类统一，不要拿站方标记做汇率换算。<br>
                  报价日历不连续（周末无报价），图表按上游给的日期逐点连接，<b>不做补空</b>；
                  历史仅保留滚动半年，更早的站方要付费 —— 因此本站需日更累积。
                </p>
              </div>

              <!-- 右：走势 + 明细 -->
              <aside class="dram-detail">
                <div class="detail-head">
                  <div class="detail-name">{{ selected?.title }}</div>
                  <div class="detail-price">
                    {{ ccySign }}{{ fmt(dramUnit === 'perGb' ? selected?.perGb : selected?.price) }}
                    <span class="detail-unit">{{ dramUnit === 'perGb' ? '每 GB' : itemUnitLabel }}</span>
                  </div>
                  <div class="detail-sub">
                    <span class="detail-pct" :class="pctClass(selected?.changePct)">
                      {{ fmtPct(selected?.changePct) }}
                    </span>
                    <span class="detail-date">{{ fmtDate(selected?.date) }}</span>
                  </div>
                </div>

                <!-- 口径 + 区间切换 -->
                <div class="chart-toolbar">
                  <el-radio-group v-model="dramUnit" size="small">
                    <el-radio-button value="perGb">每 GB</el-radio-button>
                    <!-- 均价是指数、没有"单价"概念 → 禁用，而不是让人点了看到一条平的线 -->
                    <el-radio-button value="perChip" :disabled="selected?.virtual">按单位</el-radio-button>
                  </el-radio-group>
                  <div class="toolbar-right">
                    <el-select v-model="dramRange" size="small" style="width: 150px">
                      <el-option v-for="r in RANGES" :key="r.key" :label="r.label" :value="r.key" />
                    </el-select>
                    <!-- 放大入口放在工具栏里，不浮在图上：涨势图的最高点标注就在右上角，
                         悬浮按钮会和 $19.80 那个数字正面撞上 -->
                    <button
                      v-if="Object.keys(dramChartOption).length"
                      class="chart-zoom" type="button" title="放大查看走势图" @click="zoomOpen = true"
                    ><el-icon><FullScreen /></el-icon></button>
                  </div>
                </div>

                <div class="chart-box">
                  <AnnotatedLineChart
                    v-if="Object.keys(dramChartOption).length"
                    :option="dramChartOption" :decimals="2" autoresize
                    style="width: 100%; height: 272px"
                  />
                </div>

                <div v-if="dramStats" class="detail-stats">
                  <div class="dstat">
                    <span class="ds-label">区间最高</span>
                    <b class="ds-val">{{ ccySign }}{{ fmt(dramStats.max) }}</b>
                  </div>
                  <div class="dstat">
                    <span class="ds-label">区间最低</span>
                    <b class="ds-val">{{ ccySign }}{{ fmt(dramStats.min) }}</b>
                  </div>
                  <div class="dstat">
                    <span class="ds-label">区间涨跌</span>
                    <b class="ds-val" :class="pctClass(dramStats.periodPct)">
                      {{ fmtPct(dramStats.periodPct) }}
                    </b>
                  </div>
                  <!-- 「按单位」那一格：虚拟均价没有单价，price 只是 perGb 的副本 → 直接不出，
                       否则会出现「每个 ¥0.85」这种根本不存在的口径 -->
                  <div v-if="!selected?.virtual" class="dstat">
                    <span class="ds-label">{{ dramUnit === 'perGb' ? itemUnitLabel : '每 GB 价' }}</span>
                    <b class="ds-val">
                      {{ ccySign }}{{ fmt(dramUnit === 'perGb' ? selected?.price : selected?.perGb) }}
                    </b>
                  </div>
                  <div class="dstat wide">
                    <span class="ds-label">区间（源站滚动半年）</span>
                    <b class="ds-val small">
                      {{ dramStats.firstDate }} → {{ dramStats.lastDate }}
                      （{{ dramPoints.length }} 个报价日）
                    </b>
                  </div>
                  <div class="dstat wide">
                    <span class="ds-label">规格</span>
                    <b class="ds-val small">{{ selected?.meta }}</b>
                  </div>
                </div>

                <p class="detail-note">
                  区间涨跌 = 首末两个报价日之比，<b>不是「今年来」</b> —— 源站只给滚动半年。
                  <template v-if="selected?.virtual">
                    <br>均价是<b>链式加权指数</b>：每天只取「当日与前一报价日<b>都有报价</b>」的品种
                    算容量加权价比，再逐日累乘。这样品种进出不会让曲线跳，
                    <b>跳的才是真价格变化</b>。
                  </template>
                </p>
              </aside>
            </template>

            <div v-else-if="!error" class="empty">暂无存储数据</div>
          </div>
        </el-tab-pane>

        <!-- ══ ② GPU 算力 ═════════════════════════════════════════════════ -->
        <el-tab-pane name="gpu">
          <template #label><span class="tab-label">🧠 GPU 算力</span></template>

          <div class="pane scroll">
            <el-alert type="info" :closable="false" show-icon class="proxy-alert">
              <template #title>
                GPU 芯片<b>没有公开的实时售价</b>，这里给的是<b>云厂商租价</b>与<b>买断参考价</b>两套口径
              </template>
              NVIDIA 不公布单卡售价，零售渠道（CDW / Insight / Newegg / eBay）全部反爬。
              能公开拿到的：① 各家云厂商的 <b>租价（美元/卡·小时）</b>，日更、可比；
              ② 媒体转述的 <b>单卡买断区间</b>，静态、仅供量级参考。
              两者差三个数量级，<b>不要互相换算成「回本周期」</b>。
            </el-alert>

            <div v-if="gpus.length" class="gpu-body">
              <!-- 型号卡片 -->
              <section class="group">
                <div class="group-head">
                  <h2 class="group-title">NVIDIA 数据中心 GPU</h2>
                  <span class="group-count">{{ gpus.length }} 款</span>
                </div>
                <div class="card-grid gpu-grid">
                  <div
                    v-for="g in gpus" :key="g.key"
                    class="card gpu-card" :class="{ active: g.key === gpuSelected?.key, empty: g.latestHourly == null }"
                    @click="gpuSelectedKey = g.key"
                  >
                    <div class="card-top">
                      <span class="card-name" :title="g.name">{{ g.name }}</span>
                    </div>
                    <div class="card-spec">{{ vramText(g) }}</div>

                    <template v-if="g.latestHourly != null">
                      <div class="card-price">
                        ${{ fmt(g.latestHourly) }}
                        <span class="card-unit">/卡·小时</span>
                      </div>
                      <div class="card-pct" :class="pctClass(g.changePct)">
                        {{ fmtPct(g.changePct) }}
                      </div>
                    </template>
                    <div v-else class="card-na">暂无公开报价</div>

                    <div class="card-buy">
                      买断参考
                      <b v-if="g.unitPriceLow != null">
                        {{ fmtK(g.unitPriceLow) }}~{{ fmtK(g.unitPriceHigh) }}
                      </b>
                      <b v-else class="na">—</b>
                    </div>
                    <div class="card-footer">
                      <span class="card-date">{{ fmtDate(g.latestDate) }}</span>
                    </div>
                  </div>
                </div>
              </section>

              <!-- 选中型号：走势 + 厂商明细 -->
              <section class="gpu-detail">
                <div class="detail-head">
                  <div class="detail-name">{{ gpuSelected?.name }} · 云租价</div>
                  <div class="detail-price">
                    <template v-if="gpuSelected?.latestHourly != null">
                      ${{ fmt(gpuSelected.latestHourly) }}
                      <span class="detail-unit">美元/卡·小时</span>
                    </template>
                    <template v-else>
                      <span class="detail-na">暂无公开报价</span>
                    </template>
                  </div>
                  <div class="detail-sub">
                    <span class="detail-pct" :class="pctClass(gpuSelected?.changePct)">
                      {{ fmtPct(gpuSelected?.changePct) }}
                    </span>
                    <span class="detail-date">{{ fmtDate(gpuSelected?.latestDate) }}</span>
                  </div>
                </div>

                <!-- 单卡买断参考价 -->
                <div class="buy-row">
                  <span class="buy-label">单卡买断参考价</span>
                  <b v-if="gpuSelected?.unitPriceLow != null" class="buy-val">
                    {{ fmtK(gpuSelected.unitPriceLow) }} ~ {{ fmtK(gpuSelected.unitPriceHigh) }}
                  </b>
                  <b v-else class="buy-val na">尚无公开报价</b>
                </div>
                <p v-if="gpuSelected?.unitPriceNote" class="buy-note">
                  {{ gpuSelected.unitPriceNote }}
                  <template v-if="gpuSelected.unitPriceAsof">
                    · 口径时点 {{ gpuSelected.unitPriceAsof }}
                  </template>
                </p>

                <!-- 走势：新库只有 1~2 个点，说清楚而不是画空图 -->
                <div v-if="Object.keys(gpuChartOption).length" class="chart-toolbar">
                  <span class="toolbar-hint">各云厂商公开报价的日度走势</span>
                  <button
                    class="chart-zoom" type="button" title="放大查看走势图" @click="zoomOpen = true"
                  ><el-icon><FullScreen /></el-icon></button>
                </div>
                <div class="chart-box">
                  <AnnotatedLineChart
                    v-if="Object.keys(gpuChartOption).length"
                    :option="gpuChartOption" :decimals="2" autoresize
                    style="width: 100%; height: 200px"
                  />
                  <div v-else class="chart-placeholder">
                    <template v-if="gpuSelected?.latestHourly != null">
                      该型号目前只有 <b>{{ gpuSelected.points }}</b> 个报价日（{{ fmtDate(gpuSelected.latestDate) }}），
                      走势图从第二个报价日起才有意义 —— 脚本每天跑一次就会逐步累积。
                    </template>
                    <template v-else>该型号暂无云厂商公开报价</template>
                  </div>
                </div>

                <!-- 各云厂商明细 -->
                <div v-if="gpuSelected?.providers.length" class="provider-block">
                  <div class="provider-head">
                    <span>各云厂商报价（{{ gpuSelected.providers.length }} 家）</span>
                    <span class="provider-hint">已折算为每卡；「8 卡节点」是按整机总价 ÷ 8</span>
                  </div>
                  <div class="provider-list">
                    <div
                      v-for="p in gpuSelected.providers" :key="p.provider + p.region"
                      class="provider-row"
                    >
                      <span class="pv-name">{{ p.provider }}</span>
                      <span class="pv-region">{{ p.region || '—' }}</span>
                      <span v-if="p.gpusPerNode && p.gpusPerNode > 1" class="pv-node">x{{ p.gpusPerNode }} 节点</span>
                      <span v-else class="pv-node single">单卡</span>
                      <b class="pv-price">${{ fmt(p.priceHourly) }}</b>
                    </div>
                  </div>
                </div>
                <div v-else class="provider-block">
                  <div class="provider-head"><span>各云厂商报价</span></div>
                  <p class="provider-empty">
                    上游只对<b>当前主推型号</b>（目前是 B200 SXM）给出逐家明细，
                    其他型号只有全市场最低价，所以这里没有列表 —— <b>是数据源给的就这么多，不是抓漏</b>。
                  </p>
                </div>
              </section>

              <p class="foot-note">
                租价来源 <code>aimultiple.com/ai-gpus</code>（聚合各家云厂商公开牌价）·
                买断参考价来源 <code>{{ gpuSelected?.unitPriceSrc ?? '媒体转述' }}</code>。<br>
                <b>租价最低价</b>取源站口径的「单卡（x1）最低」，不含整节点摊销价 ——
                所以会出现「最低价 $3.67，但明细里 Vultr 摊到 $3.20」，后者是 8 卡整机的每卡价，属正常。<br>
                卡片上的显存 / 带宽按<b>源站口径</b>，与实机有出入（源站把 B300 记 144GB、实机 288GB；
                H100 SXM 记 94GB、实机 80GB），仅用于识别型号，<b>不作为规格依据</b>。<br>
                <b>关于 CPO / 光模块</b>：确实没有可抓的公开报价（实测 2026-10）——
                FS.com 是纯客户端渲染、Amazon 搜 800G 会混进 400G 与 PoE 模块、
                Digi-Key / Mouser / eBay / 新蛋 全部 403。光模块 ASP 只存在于
                LightCounting / Yole 付费报告与厂商财报里，因此本站不提供这块数据，<b>不做估算值</b>。
              </p>
            </div>

            <div v-else-if="!error" class="empty">暂无 GPU 报价数据（先跑 fetch_ai_hardware.py）</div>
          </div>
        </el-tab-pane>
      </el-tabs>

      <!-- 放大查看：不是另画一张图，而是同一份数据换个尺寸重画。
           口径 / 区间与小栏共用同一组 ref —— 弹窗里切了，小栏跟着变。 -->
      <el-dialog
        v-model="zoomOpen"
        :title="zoomTitle"
        width="min(1380px, 94vw)"
        top="7vh"
        append-to-body
        destroy-on-close
        class="chart-zoom-dialog"
      >
        <div class="zoom-sub">{{ zoomSubtitle }}</div>

        <div v-if="zoomIsDram" class="zoom-toolbar">
          <el-radio-group v-model="dramUnit" size="small">
            <el-radio-button value="perGb">每 GB</el-radio-button>
            <el-radio-button value="perChip" :disabled="selected?.virtual">按单位</el-radio-button>
          </el-radio-group>
          <el-select v-model="dramRange" size="small" style="width: 156px">
            <el-option v-for="r in RANGES" :key="r.key" :label="r.label" :value="r.key" />
          </el-select>
        </div>

        <AnnotatedLineChart
          v-if="zoomAvailable" :option="zoomOption" :decimals="2" autoresize
          class="zoom-chart"
        />

        <p class="zoom-foot">
          区间最高 / 最低点的标注会随可见窗口自动重算；鼠标滚轮或底部滑块可缩放。
          <template v-if="zoomIsDram">
            源站只保留滚动半年，所以「全部」最多也就这些点，再往前要付费。
          </template>
        </p>
      </el-dialog>
    </div>
  </div>
</template>

<style scoped>
/* 整屏布局与 Shipping.vue 同源：页面占一屏，内部 flex 纵向分配，
   tabs 吃掉剩余高度，tab 内部自己滚动。高度用 vh 不用 100%（原因见 Shipping.vue）。 */
.ai-page {
  padding: 16px 22px 18px;
  height: 100vh;
  display: flex;
  flex-direction: column;
}

.page-head {
  display: flex; align-items: flex-end; justify-content: space-between;
  gap: 16px; flex-wrap: wrap; margin-bottom: 8px; flex-shrink: 0;
}
.page-head h1 { margin: 0; font-size: 22px; font-weight: 700; letter-spacing: -0.4px; }
.subtitle { margin: 4px 0 0; font-size: 12.5px; color: #8a94a6; }
.subtitle .dot { margin: 0 4px; color: #c9cfda; }

.head-stats { display: flex; gap: 8px; }
.hstat {
  background: #fff; border: 1px solid #eef0f5; border-radius: 10px;
  padding: 6px 12px; display: flex; flex-direction: column; min-width: 74px;
}
.hstat b { font-size: 15px; line-height: 1.2; color: #1a1a2e; }
.hstat span { font-size: 11px; color: #98a0b0; }

/* ── tabs 撑满剩余高度（少一环就 0 高，见 Shipping.vue） ─────────────── */
.tabs-wrap { flex: 1; min-height: 0; display: flex; flex-direction: column; }
.ai-tabs { flex: 1; min-height: 0; display: flex; flex-direction: column; }
:deep(.el-tabs__header) { margin-bottom: 12px; flex-shrink: 0; }
:deep(.el-tabs__content) { flex: 1; min-height: 0; overflow: hidden; }
:deep(.el-tab-pane) { height: 100%; }
.tab-label { font-size: 13.5px; font-weight: 500; }

.pane { height: 100%; min-height: 0; }
.pane.scroll { overflow-y: auto; padding-right: 4px; }

/* 颗粒：左列表（自己滚） + 右图表（常驻可见） */
.pane.split { display: grid; grid-template-columns: minmax(0, 1fr) 430px; gap: 18px; }
@media (max-width: 1180px) {
  .pane.split { grid-template-columns: 1fr; overflow-y: auto; }
  .dram-list { overflow: visible !important; }
}
.dram-list { overflow-y: auto; min-height: 0; padding-right: 6px; }
.dram-detail {
  background: #fff; border: 1px solid #eef0f5; border-radius: 12px;
  padding: 14px 16px 12px; min-height: 0; overflow-y: auto;
  display: flex; flex-direction: column;
}

/* ── 分组 ─────────────────────────────────────────────────────────────── */
.group { margin-bottom: 16px; }
.group-head { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
.group-head.clickable { cursor: pointer; user-select: none; padding: 2px 0; border-radius: 6px; }
.group-head.clickable:hover .group-title { color: #409eff; }
/* 折叠箭头：默认朝右，展开时转 90°（纯 CSS，不引图标） */
.chev {
  font-size: 10px; color: #b6bdc9; transition: transform .16s;
  width: 10px; flex-shrink: 0;
}
.chev.open { transform: rotate(90deg); color: #409eff; }
.group-title { margin: 0; font-size: 14.5px; font-weight: 600; color: #1a1a2e; }
.group-count { margin-left: auto; font-size: 12px; color: #aaa; }
/* 更新节奏：日更 / 周级 / 季更 —— 不标出来会把「没调价」误读成「没抓到」 */
.cadence {
  font-size: 10px; line-height: 1; padding: 3px 6px; border-radius: 4px;
  background: #f0f2f7; color: #8a94a6;
}
.cadence.季更 { background: #fff4e6; color: #c8811a; }
.cadence.周级 { background: #eef6ff; color: #5a8dc0; }
.group-note { margin: 0 0 10px; font-size: 11.5px; color: #9aa3b2; line-height: 1.6; }

.card-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(196px, 1fr)); gap: 10px; }
.card {
  background: #fff; border: 1px solid #e8e8f0; border-radius: 12px;
  padding: 12px 14px 10px; cursor: pointer;
  transition: box-shadow .18s, transform .18s, border-color .18s, background .18s;
  display: flex; flex-direction: column; gap: 4px;
}
.card:hover { box-shadow: 0 6px 20px rgba(0,0,0,.09); transform: translateY(-2px); border-color: #409eff; }
.card.active { border-color: #409eff; box-shadow: 0 0 0 1px rgba(64,158,255,.25) inset; background: #fafdff; }
/* 均价卡是接口合成的、不代表单个商品：淡紫底 + 虚线边，跟真实品种区分开 */
.card.virtual { background: #faf8ff; border-style: dashed; border-color: #d9d0f5; }
.card.virtual.active { background: #f6f2ff; border-style: solid; border-color: #7c5cff; }

.card-top { display: flex; align-items: flex-start; gap: 6px; }
.card-name {
  font-size: 12.5px; font-weight: 600; color: #1a1a2e; line-height: 1.35;
  display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
  overflow: hidden; word-break: break-word; min-height: 2.7em;
}
.tag-ett {
  flex-shrink: 0; font-size: 10px; line-height: 1; padding: 3px 5px;
  color: #98a0b0; background: #f0f2f7; border-radius: 4px; margin-top: 1px;
}
.card-price {
  font-size: 19px; font-weight: 700; color: #1a1a2e;
  font-variant-numeric: tabular-nums; line-height: 1.15;
}
.card-unit { font-size: 11.5px; font-weight: 400; color: #98a0b0; margin-left: 1px; }
.card-sub { font-size: 11.5px; color: #98a0b0; font-variant-numeric: tabular-nums; }
.card-na { font-size: 14px; font-weight: 600; color: #c0c6d2; padding: 2px 0; }
.card-pct {
  font-size: 12.5px; font-weight: 600; font-variant-numeric: tabular-nums;
  color: #c9cfda; display: flex; align-items: baseline; gap: 5px;
}
.card-footer { display: flex; align-items: center; gap: 6px; margin-top: auto; padding-top: 2px; }
.card-date { font-size: 11px; color: #b6bdc9; }
.card-pts { font-size: 10px; color: #ccd2dc; margin-left: auto; }

/* 红涨绿跌（中国习惯） */
.up { color: #f56c6c; }
.down { color: #67c23a; }

/* ── 右栏明细 ─────────────────────────────────────────────────────────── */
.detail-head { border-bottom: 1px solid #f0f2f7; padding-bottom: 10px; margin-bottom: 8px; }
.detail-name { font-size: 13.5px; font-weight: 600; color: #1a1a2e; line-height: 1.4; }
.detail-price {
  font-size: 27px; font-weight: 700; color: #1a1a2e;
  font-variant-numeric: tabular-nums; line-height: 1.25; margin-top: 3px;
}
.detail-unit { font-size: 12px; font-weight: 400; color: #98a0b0; margin-left: 4px; }
.detail-na { font-size: 17px; color: #c0c6d2; }
.detail-sub { display: flex; align-items: baseline; gap: 10px; }
.detail-pct { font-size: 13px; font-weight: 600; }
.detail-date { font-size: 11px; color: #b6bdc9; }

.chart-toolbar {
  display: flex; align-items: center; justify-content: space-between;
  gap: 8px; margin: 2px 0 6px; flex-wrap: wrap;
}
.toolbar-right { display: flex; align-items: center; gap: 6px; }
.chart-box { margin: 2px -6px 2px; }
/* 放大按钮放进工具栏，不浮在图右上角 —— 涨势图的最高点标注正好落在那里，
   悬浮按钮会和 $19.80 那个数字正面撞上（实测过） */
.chart-zoom {
  display: inline-flex; align-items: center; justify-content: center;
  width: 26px; height: 26px; padding: 0; flex-shrink: 0;
  border: 1px solid #e4e7ee; border-radius: 6px; background: #fff;
  color: #8a94a6; cursor: pointer; transition: all .15s;
}
.chart-zoom:hover { color: #409eff; border-color: #b9d6ff; background: #f5f9ff; }
.chart-zoom .el-icon { font-size: 13px; }
.toolbar-hint { font-size: 11.5px; color: #a8b0be; }
.chart-placeholder {
  font-size: 11.5px; color: #a8b0be; line-height: 1.7; text-align: center;
  padding: 34px 18px; background: #fafbfe; border-radius: 10px;
}

/* ── 放大弹窗 ─────────────────────────────────────────────────────────── */
.zoom-sub { font-size: 12px; color: #8a94a6; margin: -6px 0 8px; }
.zoom-toolbar {
  display: flex; align-items: center; justify-content: space-between;
  gap: 10px; flex-wrap: wrap; margin-bottom: 6px;
}
/* 用 min() 而不是固定 vh：屏高小时别把 footer 顶出视口 */
.zoom-chart { width: 100%; height: min(62vh, 620px); }
.zoom-foot { font-size: 11.5px; color: #a8b0be; line-height: 1.7; margin: 6px 0 0; }

.detail-stats { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 4px; }
.dstat {
  background: #fafbfe; border-radius: 8px; padding: 7px 10px;
  display: flex; flex-direction: column; gap: 2px;
}
.dstat.wide { grid-column: 1 / -1; }
.ds-label { font-size: 10.5px; color: #9aa3b2; }
.ds-val { font-size: 13.5px; font-weight: 600; color: #1a1a2e; font-variant-numeric: tabular-nums; }
.ds-val.small { font-size: 11.5px; font-weight: 500; color: #4a5261; }
.detail-note { font-size: 11px; color: #a8b0be; line-height: 1.65; margin: 10px 0 2px; }

/* ── GPU ──────────────────────────────────────────────────────────────── */
.gpu-body { display: flex; flex-direction: column; }
/* 7 款型号要挤进一行（1440 宽下），minmax 收到 150px；太窄就自然换行 */
.gpu-grid { grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); }
.gpu-card { gap: 3px; }
.gpu-card.empty { background: #fbfbfc; border-style: dashed; }
.card-spec { font-size: 10.5px; color: #9aa3b2; }
.card-buy {
  font-size: 11px; color: #a8b0be; margin-top: 1px;
  display: flex; align-items: baseline; gap: 4px;
}
.card-buy b { color: #5a6377; font-weight: 600; font-size: 11.5px; }
.card-buy .na, .buy-val.na { color: #c0c6d2; font-weight: 500; }

.gpu-detail {
  background: #fff; border: 1px solid #eef0f5; border-radius: 12px;
  padding: 14px 16px 12px; margin-bottom: 14px;
}
.buy-row {
  display: flex; align-items: baseline; gap: 8px;
  background: #fafbfe; border-radius: 8px; padding: 8px 12px; margin-bottom: 6px;
}
.buy-label { font-size: 11.5px; color: #9aa3b2; }
.buy-val { font-size: 15px; font-weight: 700; color: #1a1a2e; font-variant-numeric: tabular-nums; }
.buy-note { margin: 0 0 8px; font-size: 11px; color: #a8b0be; line-height: 1.6; }

.provider-block { margin-top: 10px; }
.provider-head {
  display: flex; align-items: baseline; justify-content: space-between;
  gap: 10px; font-size: 12px; font-weight: 600; color: #1a1a2e;
  padding-bottom: 6px; border-bottom: 1px solid #f0f2f7; flex-wrap: wrap;
}
.provider-hint { font-size: 10.5px; font-weight: 400; color: #a8b0be; }
/* 一家一行。原来用 3 列卡片网格，视觉上「Verda 的单卡」和「Together.ai 的价格」
   会挤在一起，读串行 → 改成单列的固定列宽表格，价格永远在最右侧同一位置。 */
.provider-list { display: flex; flex-direction: column; margin-top: 2px; }
.provider-row {
  display: grid;
  grid-template-columns: 150px minmax(0, 1fr) 62px 74px;
  align-items: baseline; gap: 10px;
  padding: 5px 0; border-bottom: 1px dashed #f4f6fa;
  font-size: 11.5px;
}
.pv-name { color: #37405a; font-weight: 500; }
.pv-region { color: #a8b0be; font-size: 10.5px; }
.pv-node {
  color: #b6bdc9; font-size: 10px; background: #f5f7fb;
  border-radius: 3px; padding: 1px 4px; justify-self: start;
}
.pv-node.single { background: #eef6ff; color: #7aa7cc; }
.pv-price { color: #1a1a2e; font-variant-numeric: tabular-nums; text-align: right; }
.provider-empty { font-size: 11.5px; color: #a8b0be; line-height: 1.7; margin: 8px 0 0; }

/* ── 其它 ─────────────────────────────────────────────────────────────── */
.proxy-alert { margin-bottom: 14px; }
.proxy-alert :deep(.el-alert__title) { font-size: 13px; }
.proxy-alert :deep(.el-alert__description) { font-size: 12px; line-height: 1.7; margin-top: 4px; }

.foot-note { font-size: 11.5px; color: #a8b0be; line-height: 1.75; margin: 6px 0 10px; }
.foot-note code { background: #f5f7fb; padding: 1px 5px; border-radius: 4px; }

.skeleton-wrap { padding: 16px 0; }
.empty { font-size: 13px; color: #98a0b0; padding: 40px 0; text-align: center; }
</style>
