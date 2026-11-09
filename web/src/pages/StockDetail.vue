<script setup lang="ts">
import { ref, computed, watch, onMounted, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { CandlestickChart, BarChart, LineChart } from 'echarts/charts'
import {
  GridComponent, TooltipComponent, DataZoomComponent,
  LegendComponent, AxisPointerComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'
import dayjs from 'dayjs'
import { fetchStockDetail, fetchStockPrices, fetchStockReports, fetchStockHoldings } from '@/api/stocks'
import type {
  StockDetailResult, PricePeriod, PriceBar, StockReport,
  StockFundHolding, HoldingReportDate, StockHoldingsResult,
} from '@/types/stocks'

use([CanvasRenderer, CandlestickChart, BarChart, LineChart, GridComponent,
     TooltipComponent, DataZoomComponent, LegendComponent, AxisPointerComponent])

// A 股习惯：涨红跌绿
const UP = '#e04b4b'
const DOWN = '#17a673'

const route = useRoute()
const router = useRouter()
const code = computed(() => String(route.params.code || '').trim())

const loading = ref(false)
const error = ref('')
const detail = ref<StockDetailResult | null>(null)

const period = ref<PricePeriod>('day')
type RangeKey = 'ytd' | '1y' | '3y' | '5y' | 'all'
const range = ref<RangeKey>('1y')
const bars = ref<PriceBar[]>([])
const priceLoading = ref(false)

const PERIOD_OPTIONS: { label: string; value: PricePeriod }[] = [
  { label: '日线', value: 'day' },
  { label: '周线', value: 'week' },
  { label: '月线', value: 'month' },
  { label: '年线', value: 'year' },
]
const RANGE_OPTIONS: { label: string; value: RangeKey }[] = [
  { label: '今年来', value: 'ytd' },
  { label: '1年', value: '1y' },
  { label: '3年', value: '3y' },
  { label: '5年', value: '5y' },
  { label: '全部', value: 'all' },
]
// 年线一根就是一年，区间太短会只剩 1~2 根 → 切年线时自动放宽到全部
function effectiveRange(): RangeKey {
  return period.value === 'year' ? 'all' : range.value
}
function rangeFrom(k: RangeKey): string {
  if (k === 'all') return '1990-01-01'
  if (k === 'ytd') return dayjs().startOf('year').format('YYYY-MM-DD')
  return dayjs().subtract(Number(k.replace('y', '')), 'year').format('YYYY-MM-DD')
}

async function loadDetail() {
  loading.value = true
  error.value = ''
  try {
    detail.value = await fetchStockDetail(code.value)
    await loadPrices()
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
    detail.value = null
  } finally {
    loading.value = false
  }
}

async function loadPrices() {
  priceLoading.value = true
  try {
    const r = await fetchStockPrices(code.value, {
      period: period.value,
      from: rangeFrom(effectiveRange()),
      limit: 3000,
    })
    bars.value = r.rows
  } catch {
    bars.value = []
  } finally {
    priceLoading.value = false
  }
}

// ── 财报链接 tab（只展示交易所公告 PDF 链接，不下载正文）────────────────────
const QUARTER_LABEL: Record<string, string> = {
  Q1: '第一季度', H1: '中报', Q3: '第三季度', FY: '年报',
}
const QUARTER_OPTIONS = [
  { label: '第一季度', value: 'Q1' }, { label: '中报', value: 'H1' },
  { label: '第三季度', value: 'Q3' }, { label: '年报', value: 'FY' },
]
const activeTab = ref<'profile' | 'reports' | 'holdings'>('profile')
const reports = ref<StockReport[]>([])
const reportsTotal = ref(0)
const reportsLoading = ref(false)
const reportsError = ref('')
const reportQuarter = ref('')
let reportsLoaded = false

// ── 基金持仓 tab：反查 fund_holdings，按持股数（万股）降序，切到 tab 才加载 ──
const holdings = ref<StockFundHolding[]>([])
const holdingsTotal = ref(0)
const holdingsLoading = ref(false)
const holdingsError = ref('')
const holdingsFound = ref(false)
const holdingsSecurityName = ref<string | null>(null)
const holdingsExcluded = ref<{ name: string; count: number }[]>([])
const ignoredNonQuarter = ref(0)
const matchedAlias = ref<string | null>(null)
const holdingsPage = ref(1)
const holdingsPageSize = ref(20)
const holdingReportDate = ref('')
const holdingReportDates = ref<HoldingReportDate[]>([])
// 同代码撞号（东财港股补零）或改名前旧名，被名称闸门排除的记录要透明展示
const excludedText = computed(() => {
  const xs = holdingsExcluded.value
  const total = xs.reduce((s, x) => s + x.count, 0)
  if (!total) return ''
  const names = xs.slice(0, 3).map(x => `${x.name} ${x.count} 条`).join('、')
  const more = xs.length > 3 ? ` 等 ${xs.length} 个名称` : ''
  return `已按当前简称「${holdingsSecurityName.value || '—'}」过滤掉同代码的其他名称记录 ${total} 条（${names}${more}）：东财港股代码补零成 6 位会与 A 股撞号，基金季报也可能沿用改名前的旧简称；这些记录无法确认属于本股票，故不计入。`
})
const holdingsSummary = ref<StockHoldingsResult['summary']>({
  funds: 0, shares: null, marketValue: null,
})
let holdingsLoaded = false

async function loadHoldings() {
  holdingsLoading.value = true
  holdingsError.value = ''
  try {
    const r = await fetchStockHoldings(code.value, {
      reportDate: holdingReportDate.value || undefined,
      page: holdingsPage.value,
      pageSize: holdingsPageSize.value,
    })
    holdings.value = r.rows
    holdingsTotal.value = r.total
    holdingsSummary.value = r.summary
    holdingsFound.value = r.found
    holdingsSecurityName.value = r.securityName
    holdingsExcluded.value = r.excluded
    ignoredNonQuarter.value = r.ignoredNonQuarter
    matchedAlias.value = r.matchedAlias
    holdingReportDates.value = r.reportDates
    // 后端会把非法 / 不存在的报告期回退到默认，前端一律以返回值为准
    holdingReportDate.value = r.reportDate ?? ''
    holdingsLoaded = true
  } catch {
    holdingsError.value = '加载失败，请检查后端服务是否启动'
    holdings.value = []
    holdingsTotal.value = 0
    holdingsFound.value = false
  } finally {
    holdingsLoading.value = false
  }
}
function onHoldingDateChange() { holdingsPage.value = 1; loadHoldings() }
function onHoldingsPage(p: number) { holdingsPage.value = p; loadHoldings() }
function fmtWan(v: number | null | undefined): string {
  return v == null ? '—' : v.toLocaleString('zh-CN', {
    minimumFractionDigits: 2, maximumFractionDigits: 2,
  })
}
function fmtRatio(v: number | null): string {
  return v == null ? '—' : `${v.toFixed(2)}%`
}

async function loadReports() {
  reportsLoading.value = true
  reportsError.value = ''
  try {
    const r = await fetchStockReports(code.value, {
      quarter: reportQuarter.value || undefined, limit: 200,
    })
    reports.value = r.rows
    reportsTotal.value = r.total
    reportsLoaded = true
  } catch {
    reportsError.value = '加载失败，请检查后端服务是否启动'
  } finally {
    reportsLoading.value = false
  }
}

function periodLabel(r: StockReport) {
  return `${r.reportYear ?? '—'} ${QUARTER_LABEL[r.quarter] ?? r.quarter}`
}

// 财报列表按需加载；切回行情 tab 时逼 ECharts 重算宽度（隐藏 pane 里宽度会变 0）
watch(activeTab, async v => {
  if (v === 'reports' && !reportsLoaded) await loadReports()
  if (v === 'holdings' && !holdingsLoaded) await loadHoldings()
  if (v === 'profile') {
    await nextTick()
    window.dispatchEvent(new Event('resize'))
  }
})

onMounted(() => {
  loadDetail()
  if (route.query.tab === 'reports') activeTab.value = 'reports'
  if (route.query.tab === 'holdings') activeTab.value = 'holdings'
})
watch(code, () => {
  period.value = 'day'
  range.value = '1y'
  reportsLoaded = false
  reports.value = []
  holdingsLoaded = false
  holdings.value = []
  holdingsFound.value = false
  holdingsExcluded.value = []
  holdingsSecurityName.value = null
  ignoredNonQuarter.value = 0
  matchedAlias.value = null
  holdingReportDate.value = ''
  holdingReportDates.value = []
  holdingsPage.value = 1
  loadDetail()
  if (activeTab.value === 'holdings') loadHoldings()
})
// 年线一根就是一年，选「1年」只会剩一根 K 线 —— 切到年线时把区间同步成「全部」，
// 避免 radio 高亮「1年」而图里其实是全历史（标签与数据不能自相矛盾）。
watch([period, range], () => {
  if (period.value === 'year' && range.value !== 'all') {
    range.value = 'all'      // 改了 range 会再触发本 watch，到那时才真正取数
    return
  }
  loadPrices()
})

// ── 展示格式化 ──────────────────────────────────────────────────────────────
function fmtShares(v: number | null | undefined) {
  if (v == null) return '—'
  if (v >= 1e8) return `${(v / 1e8).toFixed(2)} 亿股`
  if (v >= 1e4) return `${(v / 1e4).toFixed(2)} 万股`
  return `${v.toLocaleString('zh-CN')} 股`
}
function fmtCap(v: number | null | undefined) {
  if (v == null) return '—'
  if (v >= 1e12) return `¥${(v / 1e12).toFixed(2)} 万亿`
  if (v >= 1e8) return `¥${(v / 1e8).toFixed(2)} 亿`
  return `¥${v.toLocaleString('zh-CN')}`
}
function fmtPct(v: number | null | undefined) {
  if (v == null) return '—'
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`
}
function pctClass(v: number | null | undefined) {
  if (v == null || v === 0) return 'flat'
  return v > 0 ? 'up' : 'down'
}
function fmtNum(v: number | null | undefined, digits = 2) {
  return v == null ? '—' : v.toFixed(digits)
}

const stock = computed(() => detail.value?.stock ?? null)

// 两个时间口径不同：updatedAt = 我们几点抓的，quoteTime = 源侧行情走到几点。
// 页面严格只显示一行「数据更新」，行情时刻与口径说明放悬停提示，避免日期重复。
const timeTitle = computed(() => {
  const s = stock.value
  if (!s) return ''
  return `源侧行情时刻 ${s.quoteTime || '—'}（东财 f124）｜ 库内快照写入 ${s.updatedAt || '—'}。`
    + '「数据更新」是名录刷新时刻，非交易所实时行情时间；'
    + '停牌 / 退市股的源侧时刻可能是当日 08:00 占位，不是最后成交时刻。'
})
const fin = computed(() => detail.value?.fundamentals ?? null)
const lastBar = computed(() => bars.value.length ? bars.value[bars.value.length - 1] : null)

/** 财务指标一律「空值显示 —」，不用 0 充数（无分红 / 亏损 / 停牌） */
function fmtFin(v: number | null | undefined, digits = 2, unit = '') {
  return v == null ? '—' : `${v.toFixed(digits)}${unit}`
}

const INFO_FIELDS = computed(() => {
  const s = stock.value
  if (!s) return []
  return [
    { label: '公司全称', value: s.companyName },
    { label: '英文名称', value: s.nameEn },
    { label: '曾用名', value: s.formerName },
    { label: '法人代表', value: s.legalPerson },
    { label: '成立日期', value: s.foundedDate },
    { label: '上市日期', value: s.listingDate },
    { label: '注册资本', value: s.registeredCapital == null ? null : `${s.registeredCapital.toLocaleString('zh-CN')} 万元` },
    { label: '总股本', value: fmtShares(s.totalShares) },
    { label: '流通股本', value: fmtShares(s.floatShares) },
    { label: '注册地址', value: s.registeredAddress },
    { label: '办公地址', value: s.officeAddress },
    { label: '公司网站', value: s.website },
    { label: '联系电话', value: s.phone },
    { label: '电子邮箱', value: s.email },
    { label: '经营范围', value: s.businessScope },
  ].filter(x => x.value != null && x.value !== '' && x.value !== '—') as { label: string; value: string }[]
})

// ── ECharts K 线 + 成交量 ───────────────────────────────────────────────────
function movingAvg(values: (number | null)[], n: number) {
  const out: (number | '-')[] = []
  let sum = 0
  let valid = 0
  for (let i = 0; i < values.length; i++) {
    const v = values[i]
    if (v != null) { sum += v; valid++ }
    if (i >= n) {
      const old = values[i - n]
      if (old != null) { sum -= old; valid-- }
    }
    out.push(i >= n - 1 && valid === n && n > 0 ? Math.round(sum / n * 100) / 100 : '-')
  }
  return out
}

const chartOption = computed(() => {
  const list = bars.value
  const dates = list.map(b => b.date ?? '')
  const ohlc = list.map(b => [
    b.open ?? '-', b.close ?? '-', b.low ?? '-', b.high ?? '-',
  ])
  const closes = list.map(b => b.close)
  const volumes = list.map(b => ({
    value: b.volume ?? 0,
    itemStyle: { color: (b.close ?? 0) >= (b.open ?? 0) ? UP : DOWN },
  }))
  // 默认显示最近约 120 根，太长则整段一起看
  const start = dates.length > 120 ? Math.max(0, 100 - 120 / dates.length * 100) : 0
  return {
    animation: false,
    textStyle: { fontFamily: 'inherit' },
    legend: {
      data: ['K线', 'MA5', 'MA10', 'MA20', '成交量'],
      top: 0, left: 8, itemWidth: 14, itemHeight: 8,
      textStyle: { color: '#666', fontSize: 11 },
    },
    tooltip: {
      trigger: 'axis', axisPointer: { type: 'cross' },
      backgroundColor: 'rgba(255,255,255,.96)', borderColor: '#e6e8ee', borderWidth: 1,
      textStyle: { color: '#333', fontSize: 12 },
    },
    grid: [
      { left: 66, right: 18, top: 32, height: '55%' },
      { left: 66, right: 18, top: '70%', height: '15%' },
    ],
    xAxis: [
      { type: 'category', data: dates, gridIndex: 0, boundaryGap: true,
        axisLine: { lineStyle: { color: '#ddd' } }, axisTick: { show: false },
        axisLabel: { show: false }, splitLine: { show: false } },
      { type: 'category', data: dates, gridIndex: 1, boundaryGap: true,
        axisLine: { lineStyle: { color: '#ddd' } }, axisTick: { show: false },
        axisLabel: { color: '#999', fontSize: 10 }, splitLine: { show: false } },
    ],
    yAxis: [
      { scale: true, gridIndex: 0, position: 'left',
        axisLine: { show: false }, axisTick: { show: false },
        axisLabel: { color: '#999', fontSize: 10 },
        splitLine: { lineStyle: { color: '#f1f2f5' } } },
      { scale: true, gridIndex: 1, splitNumber: 2,
        axisLine: { show: false }, axisTick: { show: false },
        axisLabel: { color: '#999', fontSize: 10, formatter: (v: number) =>
          v >= 1e8 ? `${(v / 1e8).toFixed(1)}亿手` : `${(v / 1e4).toFixed(0)}万手` },
        splitLine: { show: false } },
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], start, end: 100, bottom: 2, height: 16,
        borderColor: '#e6e8ee', fillerColor: 'rgba(64,158,255,.12)',
        handleStyle: { color: '#409eff' }, textStyle: { color: '#999', fontSize: 10 } },
    ],
    series: [
      { name: 'K线', type: 'candlestick', data: ohlc,
        itemStyle: { color: UP, color0: DOWN, borderColor: UP, borderColor0: DOWN } },
      { name: 'MA5', type: 'line', data: movingAvg(closes, 5), showSymbol: false,
        smooth: true, lineStyle: { width: 1, color: '#f0a020' }, itemStyle: { color: '#f0a020' } },
      { name: 'MA10', type: 'line', data: movingAvg(closes, 10), showSymbol: false,
        smooth: true, lineStyle: { width: 1, color: '#409eff' }, itemStyle: { color: '#409eff' } },
      { name: 'MA20', type: 'line', data: movingAvg(closes, 20), showSymbol: false,
        smooth: true, lineStyle: { width: 1, color: '#9b59b6' }, itemStyle: { color: '#9b59b6' } },
      { name: '成交量', type: 'bar', xAxisIndex: 1, yAxisIndex: 1, data: volumes,
        barMaxWidth: 8 },
    ],
  }
})
</script>

<template>
  <div class="detail-page" v-loading="loading">
    <div class="top-row">
      <el-button @click="router.push('/stocks')">← 返回股票列表</el-button>
      <span class="crumb">股票信息 / {{ code }}</span>
    </div>

    <div v-if="error" class="error-box">{{ error }}</div>

    <!-- ⚠ 详情接口是 200 + found:false，必须显式分支，别渲染成全「—」空白页 -->
    <div v-else-if="detail && !detail.found" class="empty-box">
      <h2>未找到股票「{{ code }}」</h2>
      <p>请检查代码，或回到列表重新搜索。</p>
    </div>

    <template v-else-if="stock">
      <div class="head-card">
        <div class="head-left">
          <div class="title-line">
            <h1>{{ stock.name }}</h1>
            <span class="code">{{ stock.code }}</span>
            <el-tag size="small">{{ stock.exchange }}</el-tag>
            <el-tag size="small" type="success">{{ stock.board }}</el-tag>
            <el-tag v-if="stock.region" size="small" type="info">{{ stock.region }}</el-tag>
          </div>
          <div class="sub-line">
            {{ stock.companyName || '—' }}
            <span v-if="stock.industryCsrc"> · {{ stock.industryCsrc }}</span>
            <span v-if="stock.industry"> · {{ stock.industry }}</span>
          </div>
        </div>
        <div class="head-price">
          <div class="big-price" :class="pctClass(stock.changePct)">{{ fmtNum(stock.latestPrice) }}</div>
          <div class="price-sub" :class="pctClass(stock.changePct)">{{ fmtPct(stock.changePct) }}</div>
          <div class="price-note" :title="timeTitle">数据更新 {{ stock.updatedAt || '—' }}</div>
        </div>
      </div>

      <el-tabs v-model="activeTab" class="stock-tabs">
        <el-tab-pane label="行情与资料" name="profile">
      <div class="metric-row">
        <div class="metric"><span>总股本</span><b>{{ fmtShares(stock.totalShares) }}</b></div>
        <div class="metric"><span>总市值</span><b>{{ fmtCap(stock.marketCap) }}</b></div>
        <div class="metric"><span>流通市值</span><b>{{ fmtCap(stock.floatMarketCap) }}</b></div>
        <div class="metric"><span>上市日期</span><b>{{ stock.listingDate || '—' }}</b></div>
        <div class="metric"><span>日线覆盖</span><b>{{ detail?.priceStats?.rows ?? 0 }} 根</b></div>
        <div class="metric"><span>最新交易日</span><b>{{ detail?.priceStats?.latest || '—' }}</b></div>
      </div>

      <!-- 每股财务指标：紧跟顶部「总股本 / 总市值」指标条，首屏就能看到；长口径说明默认折叠 -->
      <div class="card fin-card">
        <div class="card-head">
          <h2>每股财务指标 <small>按最新收盘价 {{ fin ? fmtFin(fin.price) + ' 元' : '—' }} 计算</small></h2>
          <span v-if="fin?.quoteDate" class="fin-date">行情日 {{ fin.quoteDate }} · 报告期 {{ fin.reportDate || '—' }}</span>
        </div>

        <div v-if="fin" class="fin-grid">
          <div class="fin-item">
            <span>每股收益 <i>TTM</i></span>
            <b>{{ fmtFin(fin.epsTtm, 2, ' 元') }}</b>
            <small>滚动 12 个月，= 最新价 ÷ 市盈率(TTM)</small>
          </div>
          <div class="fin-item">
            <span>市盈率 <i>TTM</i></span>
            <b>{{ fmtFin(fin.peTtm, 2, ' 倍') }}</b>
            <small>动态 {{ fmtFin(fin.peDynamic, 2) }} / 静态 {{ fmtFin(fin.peStatic, 2) }} 倍</small>
          </div>
          <div class="fin-item">
            <span>股息 <i>近12月</i></span>
            <b>{{ fmtFin(fin.dividend, 3, ' 元/股') }}</b>
            <small>已实施现金分红合计，税前</small>
          </div>
          <div class="fin-item">
            <span>股息率</span>
            <b>{{ fmtFin(fin.dividendYield, 2, '%') }}</b>
            <small>= 股息 ÷ 最新价</small>
          </div>
          <div class="fin-item">
            <span>每股净资产</span>
            <b>{{ fmtFin(fin.bvps, 2, ' 元') }}</b>
            <small>{{ fin.reportDate || '最新' }} 报告期</small>
          </div>
          <div class="fin-item">
            <span>市净率</span>
            <b>{{ fmtFin(fin.pb, 2, ' 倍') }}</b>
            <small>东方财富源侧值</small>
          </div>
        </div>
        <div v-else class="no-data">
          暂无财务数据 —— 请先运行 <code>python fetch_stock_fundamentals.py</code>。
        </div>

        <details v-if="fin" class="fin-note">
          <summary>口径说明（股息率与东方财富页面可能不同，点击展开）</summary>
          <p>
            <b>股息口径</b>：{{ fin.dividendPlans ? `近 12 个月已实施 ${fin.dividendPlans} 次现金分红，最近除权除息日 ${fin.dividendLastEx || '—'}` : '近 12 个月无已实施现金分红' }}；
            股息率 = 税前每股股息 ÷ 最新收盘价。⚠ 东方财富页面上的「股息率」口径不统一
            （部分股票只算最近一次分红），本页统一按近 12 个月合计，数值可能与该页面不同。
          </p>
          <p>
            <b>每股收益口径</b>：主数值为 TTM（滚动 12 个月）；最新报告期<b>累计未年化</b>为
            {{ fmtFin(fin.epsReport, 2, ' 元') }}（{{ fin.reportDate || '—' }}），一季报仅约全年 1/4，两者不能混用。
            市盈率同时给 TTM / 动态 / 静态三种口径，主数值为 TTM。
          </p>
          <p>
            <b>市净率</b>为东方财富源侧值，个别股票（多为科创板，近期股本变动）与
            「最新价 ÷ 每股净资产」不完全对应，属源侧口径差异，未强行改写。
            无分红 / 亏损 / 停牌的指标显示「—」，不用 0 充数。
          </p>
          <p class="src-note">数据来源：东方财富 clist 行情 / 数据中心分红明细 / 业绩报表（免费公开接口）。</p>
        </details>
      </div>

      <!-- K 线：日/周/月/年，周月年由后端聚合日线生成 -->
      <div class="card chart-card">
        <div class="card-head">
          <h2>价格走势 <small>不复权 · 成交量单位「手」· 成交额单位「元」</small></h2>
          <div class="switches">
            <el-radio-group v-model="period" size="small">
              <el-radio-button v-for="p in PERIOD_OPTIONS" :key="p.value" :value="p.value">
                {{ p.label }}
              </el-radio-button>
            </el-radio-group>
            <el-radio-group v-model="range" size="small" :disabled="period === 'year'">
              <el-radio-button v-for="r in RANGE_OPTIONS" :key="r.value" :value="r.value">
                {{ r.label }}
              </el-radio-button>
            </el-radio-group>
          </div>
        </div>
        <div v-if="bars.length" class="chart-wrap">
          <v-chart :option="chartOption" autoresize class="chart" />
          <div class="chart-foot">
            区间 {{ effectiveRange() === 'all' ? '全部' : RANGE_OPTIONS.find(r => r.value === effectiveRange())?.label }}
            · {{ bars.length }} 根 {{ PERIOD_OPTIONS.find(p => p.value === period)?.label }}
            · 最新收盘 <b>{{ fmtNum(lastBar?.close) }}</b>
            <span v-if="lastBar?.volume != null"> · 成交量 {{ fmtNum(lastBar.volume / 1e4, 2) }} 万手</span>
            <span v-if="lastBar?.turnover != null"> · 成交额 {{ fmtNum(lastBar.turnover / 1e8, 2) }} 亿元</span>
          </div>
        </div>
        <div v-else class="no-data">
          <template v-if="priceLoading">正在加载 K 线…</template>
          <template v-else>该区间暂无日线数据 —— 请先运行 <code>python fetch_stock_prices.py</code> 回填历史。</template>
        </div>
      </div>

      <div class="card">
        <div class="card-head"><h2>公司基本信息</h2></div>
        <div class="info-grid">
          <div v-for="f in INFO_FIELDS" :key="f.label" class="info-item" :class="{ wide: f.label === '经营范围' }">
            <span>{{ f.label }}</span><b>{{ f.value }}</b>
          </div>
        </div>
        <div v-if="stock.introduction" class="intro">
          <h3>公司简介</h3>
          <p>{{ stock.introduction }}</p>
        </div>
      </div>

      <div class="card" v-if="detail?.industries">
        <div class="card-head"><h2>所属行业 / 概念（多值）</h2></div>
        <div class="tag-block" v-if="detail.industries.industry.length">
          <span class="tag-label">行业</span>
          <el-tag v-for="t in detail.industries.industry" :key="t" size="small" class="tag">{{ t }}</el-tag>
        </div>
        <div class="tag-block" v-if="detail.industries.concept.length">
          <span class="tag-label">概念</span>
          <el-tag v-for="t in detail.industries.concept.slice(0, 40)" :key="t" size="small" type="info" class="tag">{{ t }}</el-tag>
          <span v-if="detail.industries.concept.length > 40" class="muted">等 {{ detail.industries.concept.length }} 个</span>
        </div>
        <div class="tag-block" v-if="detail.industries.region.length">
          <span class="tag-label">地域板块</span>
          <el-tag v-for="t in detail.industries.region" :key="t" size="small" type="success" class="tag">{{ t }}</el-tag>
        </div>
        <p class="src-note">
          数据来源：东方财富 clist / F10 / 板块行情（免费公开接口）；行业为东财多层级分类与概念标签，
          公司资料来自 F10。行情快照为最近交易日，日线为不复权价。
        </p>
      </div>
        </el-tab-pane>

        <el-tab-pane label="财报链接" name="reports">
          <div class="card">
            <div class="card-head">
              <h2>定期报告 PDF <small>第一季度 / 中报 / 第三季度 / 年报，按报告期倒序</small></h2>
              <el-select v-model="reportQuarter" placeholder="全部报告期" clearable
                         style="width: 150px" @change="loadReports">
                <el-option v-for="q in QUARTER_OPTIONS" :key="q.value" :label="q.label" :value="q.value" />
              </el-select>
            </div>
            <div v-if="reportsError" class="error-box">{{ reportsError }}</div>
            <el-table v-else :data="reports" v-loading="reportsLoading" empty-text="暂无财报链接">
              <el-table-column label="报告期" width="140">
                <template #default="{ row }"><b>{{ periodLabel(row) }}</b></template>
              </el-table-column>
              <el-table-column label="报告名称" min-width="280">
                <template #default="{ row }">{{ row.title || '—' }}</template>
              </el-table-column>
              <el-table-column label="披露日期" width="130">
                <template #default="{ row }">{{ row.publishDate || '—' }}</template>
              </el-table-column>
              <el-table-column label="来源" width="100">
                <template #default="{ row }">{{ row.source === 'sse' ? '上交所' : '深交所' }}</template>
              </el-table-column>
              <el-table-column label="链接" width="130">
                <template #default="{ row }">
                  <a :href="row.pdfUrl" target="_blank" rel="noopener noreferrer" class="pdf-link">
                    查看 PDF ↗
                  </a>
                </template>
              </el-table-column>
            </el-table>
            <p class="src-note report-note">
              共 {{ reportsTotal }} 条 · 只提供交易所公告的 PDF 链接，不下载财报内容。
              沪市链接来自上交所公告、深市来自深交所公告；「报告期」是财报所属年度/季度，
              与披露日期不同（例如 2025 年年报通常在 2026 年披露）。
            </p>
          </div>
        </el-tab-pane>

        <el-tab-pane label="基金持仓" name="holdings">
          <div class="card">
            <div class="card-head">
              <h2>持有该股的公募基金 <small>按持股数降序 · 季度末报告期 {{ holdingReportDate || '—' }}</small></h2>
              <el-select v-model="holdingReportDate" placeholder="报告期" style="width: 190px"
                         @change="onHoldingDateChange">
                <el-option v-for="d in holdingReportDates" :key="d.date"
                           :label="d.date + ' (' + d.count + ')'" :value="d.date" />
              </el-select>
            </div>

            <div v-if="holdingsError" class="error-box">{{ holdingsError }}</div>
            <template v-else-if="holdingsFound">
              <div class="holding-summary">
                <div><span>持有基金份额</span><b>{{ holdingsSummary.funds.toLocaleString('zh-CN') }} 只</b></div>
                <div><span>合计持股</span><b>{{ fmtWan(holdingsSummary.shares) }} 万股</b></div>
                <div><span>合计持仓市值</span><b>{{ fmtWan(holdingsSummary.marketValue) }} 万元</b></div>
              </div>

              <el-table :data="holdings" v-loading="holdingsLoading"
                        empty-text="该报告期暂无公募基金持仓披露">
                <el-table-column label="基金代码" width="110">
                  <template #default="{ row }">
                    <router-link :to="{ name: 'fund-detail', params: { code: row.fundCode } }"
                                 class="fund-link">{{ row.fundCode }}</router-link>
                  </template>
                </el-table-column>
                <el-table-column label="基金名称" min-width="220">
                  <template #default="{ row }">{{ row.fundName || '—' }}</template>
                </el-table-column>
                <el-table-column label="类型" width="160">
                  <template #default="{ row }">{{ row.fundType || '—' }}</template>
                </el-table-column>
                <el-table-column label="基金公司" min-width="180">
                  <template #default="{ row }">{{ row.fundCompany || '—' }}</template>
                </el-table-column>
                <el-table-column label="持股数（万股）" width="150" align="right">
                  <template #default="{ row }"><b>{{ fmtWan(row.shares) }}</b></template>
                </el-table-column>
                <el-table-column label="持仓市值（万元）" width="160" align="right">
                  <template #default="{ row }">{{ fmtWan(row.marketValue) }}</template>
                </el-table-column>
                <el-table-column label="占净值比" width="110" align="right">
                  <template #default="{ row }">{{ fmtRatio(row.ratio) }}</template>
                </el-table-column>
              </el-table>

              <div v-if="holdingsTotal > holdingsPageSize" class="pager">
                <el-pagination background layout="total, prev, pager, next, jumper"
                               :total="holdingsTotal" :page-size="holdingsPageSize"
                               :current-page="holdingsPage" @current-change="onHoldingsPage" />
              </div>
            </template>
            <div v-else class="empty-note">
              <template v-if="holdingsLoading">加载中…</template>
              <template v-else>
                该股暂无与当前简称匹配的季度末持仓披露
                （可能未被任何基金列入前十大重仓，或记录来自改名前的旧名）。
                <span v-if="excludedText" class="excluded-note">{{ excludedText }}</span>
              </template>
            </div>

            <p class="src-note report-note">
              口径：东财 F10 季报披露的<strong>前十大重仓股</strong>（不是全部持仓），
              持股数单位为<strong>万股</strong>、持仓市值为<strong>万元</strong>、占比为占基金净值比例。
              按<strong>基金份额</strong>统计，同一产品 A / C 类分开计（如鹏华双债加利 A、C 各占一行），
              因此「持有基金份额」不等于基金产品只数。
              <strong>只统计季度末报告期</strong>（03-31 / 06-30 / 09-30 / 12-31），
              共 {{ holdingsTotal }} 条记录。
              <span v-if="ignoredNonQuarter" class="excluded-note">
                已忽略 {{ ignoredNonQuarter }} 条非季末披露：源侧 F10 对部分新建 ETF 等给出带具体日期的
                建仓 / 临时组合快照（如 2026-03-26），与季报口径不同，不计入统计。
              </span>
              <span v-if="matchedAlias" class="excluded-note">
                该股简称带科创板 / 创业板后缀标记，基金季报使用去后缀简称「{{ matchedAlias }}」，
                已按该别名匹配持仓记录（同代码下无其他证券名称，不会混入港股）。
              </span>
              <span v-if="holdingsFound && excludedText" class="excluded-note">{{ excludedText }}</span>
            </p>
          </div>
        </el-tab-pane>
      </el-tabs>
    </template>
  </div>
</template>

<style scoped>
.detail-page { padding: 20px 28px 48px; max-width: 1400px; }
.stock-tabs { margin-top: 4px; }
.stock-tabs :deep(.el-tabs__content) { overflow: visible; }
.pdf-link { color: #409eff; font-weight: 600; }
.pdf-link:hover { text-decoration: underline; }
.report-note { margin-top: 12px; font-size: 11.5px; color: #aaa; line-height: 1.7; }

/* 基金持仓 tab */
.holding-summary {
  display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 12px;
}
.holding-summary > div {
  background: #f8fafc; border: 1px solid #eef1f6; border-radius: 8px;
  padding: 10px 14px; display: flex; flex-direction: column; gap: 4px;
}
.holding-summary span { font-size: 12px; color: #999; }
.holding-summary b { font-size: 16px; color: #1a1a2e; font-weight: 650; }
.fund-link {
  color: #409eff; font-family: ui-monospace, Consolas, monospace;
  font-weight: 600; text-decoration: none;
}
.fund-link:hover { text-decoration: underline; }
.empty-note {
  padding: 24px; text-align: center; color: #999; font-size: 13px;
  background: #fafbfc; border-radius: 8px;
}
.excluded-note { display: block; margin-top: 8px; color: #a67c13; text-align: left; line-height: 1.7; }
.pager { display: flex; justify-content: flex-end; margin-top: 14px; }
.top-row { display: flex; align-items: center; gap: 14px; margin-bottom: 16px; }
.crumb { font-size: 12px; color: #9aa0ac; }
.error-box { padding: 14px 18px; background: #fff2f0; color: #c0392b; border-radius: 8px; }
.empty-box { padding: 60px 20px; text-align: center; background: #fff; border-radius: 10px; }
.empty-box h2 { margin: 0 0 10px; font-size: 18px; }
.empty-box p { color: #888; }

.head-card {
  display: flex; justify-content: space-between; align-items: flex-start;
  background: #fff; border: 1px solid #eceef3; border-radius: 10px;
  padding: 18px 22px; margin-bottom: 12px;
}
.title-line { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.title-line h1 { margin: 0; font-size: 24px; font-weight: 650; }
.code { font-family: ui-monospace, Consolas, monospace; font-size: 16px; color: #409eff; font-weight: 600; }
.sub-line { margin-top: 10px; font-size: 13px; color: #777; }
.head-price { text-align: right; min-width: 140px; }
.big-price { font-size: 30px; font-weight: 700; line-height: 1.1; }
.price-sub { font-size: 15px; font-weight: 600; margin-top: 4px; }
.price-note { font-size: 11px; color: #aaa; margin-top: 6px; cursor: help; }
.up { color: #e04b4b; } .down { color: #17a673; } .flat { color: #333; }

.metric-row {
  display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; margin-bottom: 12px;
}
.metric {
  background: #fff; border: 1px solid #eceef3; border-radius: 8px;
  padding: 12px 14px; display: flex; flex-direction: column; gap: 6px;
}
.metric span { font-size: 12px; color: #999; }
.metric b { font-size: 15px; color: #1a1a2e; font-weight: 600; }

.card { background: #fff; border: 1px solid #eceef3; border-radius: 10px; padding: 16px 20px; margin-bottom: 12px; }
.card-head { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; margin-bottom: 12px; }
.card-head h2 { margin: 0; font-size: 16px; font-weight: 650; }
.card-head small { font-size: 11px; font-weight: 400; color: #aaa; margin-left: 8px; }
.switches { display: flex; gap: 10px; flex-wrap: wrap; }
.chart-wrap { width: 100%; }
.chart { width: 100%; height: 460px; }
.chart-foot { font-size: 12px; color: #888; padding: 4px 4px 0; }
.chart-foot b { color: #333; }
.no-data { padding: 70px 0; text-align: center; color: #aaa; font-size: 13px; }
.no-data code { background: #f5f6fa; padding: 2px 5px; border-radius: 4px; color: #666; }

.info-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0 26px; }
.info-item { display: flex; gap: 12px; padding: 9px 0; border-bottom: 1px solid #f4f5f8; font-size: 13px; }
.info-item.wide { grid-column: 1 / -1; }
.info-item span { color: #999; min-width: 84px; flex-shrink: 0; }
.info-item b { color: #333; font-weight: 500; word-break: break-all; }
.intro { margin-top: 18px; }
.intro h3 { margin: 0 0 8px; font-size: 14px; font-weight: 600; }
.intro p { margin: 0; font-size: 13px; line-height: 1.9; color: #555; white-space: pre-wrap; }

.fin-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
.fin-item {
  background: #fafbfd; border: 1px solid #eef0f5; border-radius: 8px;
  padding: 12px 14px; display: flex; flex-direction: column; gap: 5px;
}
.fin-item > span { font-size: 12px; color: #888; }
.fin-item > span i {
  font-style: normal; font-size: 10px; color: #409eff; background: #e8f3ff;
  padding: 1px 5px; border-radius: 4px; margin-left: 4px;
}
.fin-item > b { font-size: 20px; font-weight: 650; color: #1a1a2e; }
.fin-item > small { font-size: 11px; color: #aaa; line-height: 1.5; }
.fin-date { font-size: 11px; color: #9aa0ac; }
.fin-note { margin-top: 14px; border-top: 1px dashed #e8eaf0; padding-top: 12px; }
.fin-note summary {
  cursor: pointer; font-size: 12px; color: #409eff;
  outline: none; user-select: none;
}
.fin-note summary:hover { color: #1f7fd6; }
.fin-note[open] summary { margin-bottom: 10px; }
.fin-note p { margin: 0 0 7px; font-size: 11.5px; line-height: 1.75; color: #777; }
.fin-note b { color: #555; }
.fin-note .src-note { color: #aaa; margin-top: 10px; }
@media (max-width: 1100px) { .fin-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }

.tag-block { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; margin-bottom: 10px; }
.tag-label { font-size: 12px; color: #999; min-width: 64px; }
.tag { margin: 2px; }
.muted { color: #aaa; font-size: 12px; }
.src-note { margin: 14px 0 0; font-size: 11px; color: #aaa; line-height: 1.7; }

@media (max-width: 1100px) {
  .metric-row { grid-template-columns: repeat(3, 1fr); }
  .info-grid { grid-template-columns: 1fr; }
}
</style>
