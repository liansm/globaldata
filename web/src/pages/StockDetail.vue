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
import { fetchStockDetail, fetchStockPrices, fetchStockReports, fetchStockHoldings,
         fetchStockFinancials, fetchStockStatement } from '@/api/stocks'
import type {
  StockDetailResult, PricePeriod, PriceBar, StockReport,
  StockFundHolding, HoldingReportDate, StockHoldingsResult,
  StockFinancialRow, StockFinancialsResult,
  StockStatementResult, StmtType, StmtFilter, StmtLineRow,
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
const activeTab = ref<'profile' | 'reports' | 'holdings' | 'financials'>('profile')
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
  totalShares: null, floatShares: null, shareRatio: null, floatShareRatio: null,
})

// 股本用亿股 / 万股自适应，比一律「万股」好读（807,453.85 万股 → 80.75 亿股）
function fmtShareCount(v: number | null): string {
  if (v == null) return '—'
  return Math.abs(v) >= 1e8
    ? `${(v / 1e8).toFixed(2)} 亿股`
    : `${(v / 1e4).toFixed(2)} 万股`
}

// 占比悬浮说明：把分子分母和口径一次讲清，避免被当成「全部公募持仓」或「报告期时点股本」
const holdingsRatioTitle = computed(() => {
  const s = holdingsSummary.value
  if (s.shareRatio == null) return ''
  const parts = [
    `合计持股 ${fmtWan(s.shares)} 万股 ÷ 最新总股本 ${fmtShareCount(s.totalShares)}`,
  ]
  // 总股本 = 流通股时（全流通）再说「占流通股」是重复信息
  if (s.floatShareRatio != null && Math.abs(s.floatShareRatio - s.shareRatio) >= 0.01) {
    parts.push(`相当于占流通股 ${s.floatShareRatio.toFixed(2)}%`)
  }
  parts.push('股本取当前快照，历史报告期的占比未按当时股本回溯；分子仅为季报披露的公募基金前十大重仓合计')
  return parts.join('；')
})
let holdingsLoaded = false

// ── 财报三表 tab：逐季度对比（累计值 / 单季差分 + 派生指标）──────────────────
// 口径：利润表与现金流量表是「年初至今累计」，单季由后端视图 LAG 差分；
// 资产负债表是季末时点值，不差分；金融业毛利率 / 速动比不适用（后端置 null）。
const financials = ref<StockFinancialRow[]>([])
const financialsResult = ref<StockFinancialsResult | null>(null)
const financialsLoading = ref(false)
const financialsError = ref('')
const financialsFound = ref(false)
// 季度对比的指标清单 = 表格列顺序：前 11 项单季、后 2 项累计。
// unit 决定图表 Y 轴是「亿元」「%」还是「数量」——比率 / 户数不能再除 1e8。
// ⚠ 「其他收益」接口仍返回（qOtherIncome），2026-11-09 用户要求先从表格/图表里拿掉，
//   数据与接口都保留，要显示回来只需在这里加一行 + 表格加一列。
const FIN_METRICS = [
  { label: '总收入',         value: 'qRevenue',        unit: 'money' },
  { label: '毛利润',         value: 'qGrossProfit',    unit: 'money' },
  { label: '毛利率',         value: 'qGrossMargin',    unit: 'percent' },
  { label: '销售费用',       value: 'qSaleExpense',    unit: 'money' },
  { label: '管理费用',       value: 'qManageExpense',  unit: 'money' },
  { label: '研发费用',       value: 'qResearchExpense', unit: 'money' },
  { label: '财务费用',       value: 'qFinanceExpense', unit: 'money' },
  { label: '营业利润',       value: 'qOperateProfit',  unit: 'money' },
  { label: '所得税',         value: 'qIncomeTax',      unit: 'money' },
  { label: '净利润',         value: 'qNetprofit',      unit: 'money' },
  // 单季净利率 = 单季净利润 ÷ 单季营业总收入（2026-11-10 用户要求，紧跟净利润）
  { label: '净利率',         value: 'qNetMargin',      unit: 'percent' },
  { label: '累计总收入',     value: 'revenue',         unit: 'money' },
  { label: '累计归母净利',   value: 'parentNetprofit', unit: 'money' },
] as const

// 「季度对比(其他)」：资产负债 / 现金流 / 股本结构（2026-11-10 用户指定，顺序勿改）。
// ⚠ 时效不同：资产负债类是**季末时点值**（不差分）；经营活动现金流是**单季差分**；
//   员工数量是**年度快照**（季报不披露，按报告期年份匹配）；股东数量是季度内最后一个统计截止日。
const FIN_METRICS_OTHER = [
  { label: '流动资产',       value: 'totalCurrentAssets', unit: 'money' },
  { label: '流动负债',       value: 'totalCurrentLiab',   unit: 'money' },
  { label: '负债合计',       value: 'totalLiabilities',   unit: 'money' },
  { label: '资产合计',       value: 'totalAssets',        unit: 'money' },
  { label: '归母所有者权益', value: 'totalParentEquity',  unit: 'money' },
  { label: '短期借款',       value: 'shortLoan',          unit: 'money' },
  { label: '长期借款',       value: 'longLoan',           unit: 'money' },
  { label: '应收账款',       value: 'accountsRece',       unit: 'money' },
  { label: '应付账款',       value: 'accountsPayable',    unit: 'money' },
  { label: '存货',           value: 'inventory',          unit: 'money' },
  { label: '货币资金',       value: 'monetaryFunds',      unit: 'money' },
  { label: '经营活动现金流', value: 'qNetcashOperate',    unit: 'money' },
  { label: '期末现金',       value: 'endCce',             unit: 'money' },
  { label: '员工数量',       value: 'employeeNum',        unit: 'count' },
  { label: '股东数量',       value: 'holderNum',          unit: 'count' },
] as const

type FinMetricKey = typeof FIN_METRICS[number]['value']
type FinMetricOtherKey = typeof FIN_METRICS_OTHER[number]['value']
type AnyFinMetricKey = FinMetricKey | FinMetricOtherKey
type MetricDef = { label: string; value: AnyFinMetricKey; unit: 'money' | 'percent' | 'count' }

// as const 的元组需要放宽成统一结构才能在同一处图表逻辑里互换
const FIN_DEFS: MetricDef[] = FIN_METRICS.map(m => ({ ...m }))
const FIN_DEFS_OTHER: MetricDef[] = FIN_METRICS_OTHER.map(m => ({ ...m }))

const finMetric = ref<FinMetricKey>('qRevenue')
const finMetricOther = ref<FinMetricOtherKey>('totalCurrentAssets')
// 图表形态：柱状（逐季对比高低）/ 曲线（看趋势与拐点）。
// 默认柱状 —— 费用、税费这类科目单季波动大，柱更容易读绝对值；趋势判断再切曲线。
const finChartType = ref<'bar' | 'line'>('bar')
const finChartTypeOther = ref<'bar' | 'line'>('bar')
let financialsLoaded = false

async function loadFinancials() {
  financialsLoading.value = true
  financialsError.value = ''
  try {
    const r = await fetchStockFinancials(code.value, { limit: 60 })
    financialsResult.value = r
    financials.value = r.rows
    financialsFound.value = r.found
    financialsLoaded = true
    await nextTick()
    window.dispatchEvent(new Event('resize'))  // 隐藏 pane 里 ECharts 宽度会算成 0
  } catch {
    financialsError.value = '加载失败，请检查后端服务是否启动'
    financials.value = []
    financialsFound.value = false
  } finally {
    financialsLoading.value = false
  }
}

function fmtYi(v: number | null | undefined): string {
  return v == null ? '—' : (v / 1e8).toFixed(2)
}

// ── 逐科目三表（雪球式）：行 = 报表科目，列 = 报告期，可切年报/中报/季报并翻页 ──
// 数据来自 /statement（直接读 raw 里的 200~319 个字段），与上面的「季度对比」同一批数据。
// summary = 季度对比(财务)；summaryOther = 季度对比(其他，资产负债/现金流/股本)
type FinSubTab = 'BALANCE' | 'INCOME' | 'CASHFLOW' | 'summary' | 'summaryOther'
const finSubTab = ref<FinSubTab>('BALANCE')
/** 两个「季度对比」子页签共用同一批 financials 行，只是表格列与图表指标不同 */
const isCompareTab = computed(() =>
  finSubTab.value === 'summary' || finSubTab.value === 'summaryOther')
/** 当前对比页签的指标清单 / 选中指标 / 图表形态（两套各自记忆，切页签不互相覆盖） */
const activeMetricDefs = computed<MetricDef[]>(() =>
  finSubTab.value === 'summaryOther' ? FIN_DEFS_OTHER : FIN_DEFS)
const activeMetric = computed<AnyFinMetricKey>({
  get: () => (finSubTab.value === 'summaryOther' ? finMetricOther.value : finMetric.value),
  set: v => {
    if (finSubTab.value === 'summaryOther') finMetricOther.value = v as FinMetricOtherKey
    else finMetric.value = v as FinMetricKey
  },
})
const activeChartType = computed<'bar' | 'line'>({
  get: () => (finSubTab.value === 'summaryOther' ? finChartTypeOther.value : finChartType.value),
  set: v => {
    if (finSubTab.value === 'summaryOther') finChartTypeOther.value = v
    else finChartType.value = v
  },
})
const STMT_FILTERS: { value: StmtFilter; label: string }[] = [
  { value: 'all', label: '全部' },
  { value: 'annual', label: '年报' },
  { value: 'h1', label: '中报' },
  { value: 'q1', label: '一季报' },
  { value: 'q3', label: '三季报' },
]
const stmtFilter = ref<StmtFilter>('all')
const stmtPage = ref(1)
const STMT_PAGE_SIZE = 5          // 一屏 5 期，和雪球一致
const stmtResult = ref<StockStatementResult | null>(null)
const stmtLoading = ref(false)
const stmtError = ref('')
const stmtCache = new Map<string, StockStatementResult>()

const stmtPeriods = computed(() => stmtResult.value?.periods ?? [])
const stmtTotalPages = computed(() =>
  Math.max(1, Math.ceil((stmtResult.value?.totalPeriods ?? 0) / STMT_PAGE_SIZE)))

/** 把后端的分组结构拍平成表格行，插入一行假的分组标题（kind='section'）。 */
const stmtTableRows = computed<StmtLineRow[]>(() => {
  const out: StmtLineRow[] = []
  for (const sec of stmtResult.value?.sections ?? []) {
    if (sec.title) out.push({ key: `#${sec.title}`, label: sec.title, kind: 'section', fmt: 'money', values: [] })
    out.push(...sec.rows)
  }
  return out
})

const stmtCurrency = computed(() => stmtResult.value?.currency || 'CNY')

/** 空状态要分两种：一只股票根本没有财报（多为已退市）vs 当前筛选条件下没有。 */
const stmtEmptyText = computed(() => {
  const name = finSubTab.value === 'BALANCE' ? '资产负债表'
    : finSubTab.value === 'INCOME' ? '利润表' : '现金流量表'
  const total = stmtResult.value?.totalPeriods ?? 0
  if (!total) {
    return financialsFound.value
      ? `该股票在此筛选条件下没有${name}数据，换个报告期类型试试。`
      : '该股票暂无财务报表数据（源侧未披露，或为已退市 / 吸收合并主体）。'
  }
  return `该股票在此筛选条件下没有${name}数据，换个报告期类型试试。`
})

/** 金额：≥1 亿用亿、≥1 万用万（雪球同款），单位逐格自适应；空值 — */
function fmtStmt(v: number | null | undefined, fmt: 'money' | 'plain' = 'money'): string {
  if (v == null) return '—'
  if (fmt === 'plain') return v.toFixed(4)
  const a = Math.abs(v)
  if (a >= 1e8) return `${(v / 1e8).toFixed(2)}亿`
  if (a >= 1e4) return `${(v / 1e4).toFixed(2)}万`
  return v.toFixed(2)
}

function stmtRowClass({ row }: { row: StmtLineRow }): string {
  return `stmt-row-${row.kind}`
}

async function loadStatement() {
  if (isCompareTab.value) return
  const type = finSubTab.value as StmtType
  const key = `${code.value}|${type}|${stmtFilter.value}|${stmtPage.value}`
  const cached = stmtCache.get(key)
  if (cached) { stmtResult.value = cached; return }

  stmtLoading.value = true
  stmtError.value = ''
  try {
    const r = await fetchStockStatement(code.value, {
      type, filter: stmtFilter.value, page: stmtPage.value, pageSize: STMT_PAGE_SIZE,
    })
    stmtCache.set(key, r)
    stmtResult.value = r
    // 过滤到某类报告期后可能越界（如只有 12 期年报却停在第 4 页）
    if (stmtPage.value > stmtTotalPages.value) {
      stmtPage.value = stmtTotalPages.value
      await loadStatement()
    }
  } catch {
    stmtError.value = '加载失败，请检查后端服务是否启动'
    stmtResult.value = null
  } finally {
    stmtLoading.value = false
  }
}

function setStmtFilter(f: StmtFilter) {
  if (stmtFilter.value === f) return
  stmtFilter.value = f
  stmtPage.value = 1
  stmtResult.value = null
  loadStatement()
}

function gotoStmtPage(p: number) {
  if (p < 1 || p > stmtTotalPages.value) return
  stmtPage.value = p
  loadStatement()
}

watch(finSubTab, async () => {
  if (!isCompareTab.value) {
    stmtPage.value = 1
    stmtResult.value = null
    await loadStatement()
  } else {
    await nextTick()
    window.dispatchEvent(new Event('resize'))
  }
})
function fmtCount(v: number | null | undefined): string {
  return v == null ? '—' : v.toLocaleString('zh-CN')
}
function periodText(r: StockFinancialRow): string {
  return `${r.reportYear ?? '—'} Q${r.quarterNum ?? '—'}`
}

/**
 * 季度对比表的「年份分组」提示（用户 2026-11-09：每新的一年略有区分）。
 * ① `fin-row-alt`：按报告期年的**奇偶**交替底色 —— 相邻两年必然不同色，
 *    扫表时一眼看出年份区块（2026 白 / 2025 淡灰 / 2024 白…）。
 * ② `fin-row-year-start`：年份切换的那一行加深上边框。表格是报告期**倒序**，
 *    所以「上一行年份 ≠ 本行年份」正好落在新一年的最上面那行（如 2025 Q4 紧跟 2026 Q1）。
 * rowIndex 是当前 data 数组下标（= financials 原序），直接取上一行比年份即可。
 */
function finRowClass({ row, rowIndex }:
  { row: StockFinancialRow; rowIndex: number }): string {
  const prev = rowIndex > 0 ? financials.value[rowIndex - 1] : null
  const cls = ['fin-row']
  if (row.reportYear != null && Math.abs(row.reportYear) % 2 === 1) cls.push('fin-row-alt')
  if (prev && prev.reportYear !== row.reportYear) cls.push('fin-row-year-start')
  return cls.join(' ')
}

const finChartOption = computed(() => {
  const rows = financials.value.slice(0, 20).slice().reverse()
  const labels = rows.map(periodText)
  const key = activeMetric.value
  const unit = (activeMetricDefs.value.find(m => m.value === key) ?? activeMetricDefs.value[0]).unit
  const pct = unit === 'percent'
  const cnt = unit === 'count'
  const values = rows.map(r => {
    const raw = r[key]
    if (raw == null) return null
    // 亿元 / % / 数量（户、人）三种量纲，只有金额要除 1e8
    return unit === 'money' ? raw / 1e8 : raw
  })
  const rotated = labels.length > 12
  // 柱状按正负分色（涨红跌绿），曲线只有一条线 → 统一用红 + 淡红渐隐面积，
  // 靠 Y 轴 0 线判断正负（负数会自然落到 0 线以下）。两种形态共用同一份网格与坐标轴。
  const asLine = activeChartType.value === 'line'
  const series = asLine
    ? [{
        type: 'line', smooth: true, smoothMonotone: 'x',
        symbol: 'circle', symbolSize: 7,
        // 缺期就断开，不要 connectNulls 拉一条假直线
        connectNulls: false,
        data: values,
        itemStyle: { color: UP },
        lineStyle: { color: UP, width: 2 },
        areaStyle: {
          color: {
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(224,75,75,0.26)' },
              { offset: 1, color: 'rgba(224,75,75,0.02)' },
            ],
          },
        },
      }]
    : [{
        type: 'bar', barMaxWidth: 28,
        // A 股习惯：正（盈利 / 增长）红、负绿
        data: values.map(v => v == null ? null : ({
          value: v,
          itemStyle: { color: v >= 0 ? UP : DOWN },
        })),
      }]
  return {
    backgroundColor: 'transparent',
    // ⚠ 下边距要跟 X 轴标签倾斜一起变：标签转 40° 后纵向占 ~40px，
    //   bottom:34 会把「2021 Q4」这类标签切掉下半截（2026-11-09 截图验收发现）。
    grid: { left: 64, right: 24, top: 36, bottom: rotated ? 62 : 36 },
    tooltip: {
      trigger: 'axis',
      valueFormatter: (v: unknown) => v == null ? '—'
        : pct ? `${Number(v).toFixed(2)}%`
        : cnt ? `${Number(v).toLocaleString('zh-CN')}`
        : `${Number(v).toFixed(2)} 亿元`,
    },
    xAxis: {
      type: 'category', data: labels,
      axisLabel: { color: '#888', fontSize: 11, interval: 0, rotate: rotated ? 40 : 0,
                   margin: 10 },
      axisLine: { lineStyle: { color: '#dfe3ea' } },
    },
    yAxis: {
      type: 'value', name: pct ? '%' : cnt ? '数量' : '亿元',
      // nameGap 太小会让「亿元」压在容器顶边被切掉（截图验收踩过），配 top:36
      nameGap: 12,
      axisLabel: { color: '#888', fontSize: 11 },
      nameTextStyle: { color: '#999', fontSize: 11, padding: [0, 0, 0, -18] },
      splitLine: { lineStyle: { color: '#eef1f6' } },
    },
    series,
  }
})

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
  if (v === 'financials' && !financialsLoaded) await loadFinancials()
  if (v === 'financials') await loadStatement()
  if (v === 'profile') {
    await nextTick()
    window.dispatchEvent(new Event('resize'))
  }
})

onMounted(() => {
  loadDetail()
  if (route.query.tab === 'reports') activeTab.value = 'reports'
  if (route.query.tab === 'holdings') activeTab.value = 'holdings'
  if (route.query.tab === 'financials') activeTab.value = 'financials'
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
  financialsLoaded = false
  financials.value = []
  financialsResult.value = null
  financialsFound.value = false
  // 逐科目三表：换股票必须清缓存，否则会拿上一只的科目数据渲染
  stmtCache.clear()
  stmtResult.value = null
  stmtPage.value = 1
  stmtError.value = ''
  loadDetail()
  if (activeTab.value === 'holdings') loadHoldings()
  if (activeTab.value === 'financials') {
    loadFinancials()
    loadStatement()
  }
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

        <el-tab-pane label="财务报表" name="financials">
          <div class="card">
            <div class="card-head">
              <h2>
                财务报表
                <small v-if="finSubTab === 'summary'">季度对比(财务) · 累计值 / 单季差分</small>
                <small v-else-if="finSubTab === 'summaryOther'">季度对比(其他) · 资产负债 / 现金流 / 股本</small>
                <small v-else>逐科目明细 · 金额自动按亿/万显示</small>
              </h2>
              <div v-if="isCompareTab" class="fin-head-tools">
                <el-radio-group v-model="activeChartType" size="small">
                  <el-radio-button value="bar">柱状图</el-radio-button>
                  <el-radio-button value="line">曲线图</el-radio-button>
                </el-radio-group>
                <el-select v-model="activeMetric" style="width: 175px" size="small">
                  <el-option v-for="m in activeMetricDefs" :key="m.value" :label="m.label" :value="m.value" />
                </el-select>
              </div>
            </div>
            <div v-if="financialsError" class="error-box">{{ financialsError }}</div>
            <template v-else>
              <div v-if="financialsFound && financials.length" class="fin-summary">
                <div><span>最新报告期</span><b>{{ financialsResult?.latestReportDate || '—' }}</b></div>
                <div><span>报表模板</span><b>{{ financialsResult?.orgType || '—' }}</b></div>
                <div><span>员工人数</span><b>{{ fmtCount(financialsResult?.employee?.empNum) }} 人</b></div>
                <div><span>最新季股东户数</span><b>{{ fmtCount(financials[0]?.holderNum) }} 户</b></div>
              </div>

              <div class="stmt-tabs">
                <button :class="['stmt-tab', { active: finSubTab === 'BALANCE' }]"
                        @click="finSubTab = 'BALANCE'">资产负债表</button>
                <button :class="['stmt-tab', { active: finSubTab === 'INCOME' }]"
                        @click="finSubTab = 'INCOME'">利润表</button>
                <button :class="['stmt-tab', { active: finSubTab === 'CASHFLOW' }]"
                        @click="finSubTab = 'CASHFLOW'">现金流量表</button>
                <button :class="['stmt-tab', { active: finSubTab === 'summary' }]"
                        @click="finSubTab = 'summary'">季度对比(财务)</button>
                <button :class="['stmt-tab', { active: finSubTab === 'summaryOther' }]"
                        @click="finSubTab = 'summaryOther'">季度对比(其他)</button>
              </div>

              <!-- ── 逐科目三表（雪球式：行 = 科目，列 = 报告期） ── -->
              <div v-if="!isCompareTab" class="stmt-pane">
                <div class="stmt-toolbar">
                  <div class="stmt-filters">
                    <button v-for="f in STMT_FILTERS" :key="f.value"
                            :class="['stmt-chip', { active: stmtFilter === f.value }]"
                            @click="setStmtFilter(f.value)">{{ f.label }}</button>
                  </div>
                  <div class="stmt-pager">
                    <span class="stmt-currency">财报币种：{{ stmtCurrency }}</span>
                    <button class="stmt-nav" :disabled="stmtPage <= 1 || stmtLoading"
                            @click="gotoStmtPage(stmtPage - 1)">上一页</button>
                    <span class="stmt-page">{{ stmtPage }} / {{ stmtTotalPages }}</span>
                    <button class="stmt-nav" :disabled="stmtPage >= stmtTotalPages || stmtLoading"
                            @click="gotoStmtPage(stmtPage + 1)">下一页</button>
                  </div>
                </div>

                <div v-if="stmtError" class="error-box">{{ stmtError }}</div>
                <div v-else-if="!stmtLoading && !stmtTableRows.length" class="empty-note">
                  {{ stmtEmptyText }}
                </div>
                <el-table v-else :data="stmtTableRows" v-loading="stmtLoading" size="small"
                          :row-class-name="stmtRowClass" class="stmt-table"
                          empty-text="暂无报表数据">
                  <el-table-column label="报告期" width="320" fixed>
                    <template #default="{ row }">
                      <span :class="['stmt-label', `lv-${row.kind}`]">{{ row.label }}</span>
                    </template>
                  </el-table-column>
                  <el-table-column v-for="(p, i) in stmtPeriods" :key="p.reportDate"
                                   :label="p.label" min-width="146" align="right">
                    <template #default="{ row }">
                      <span v-if="row.kind === 'section'" class="stmt-section-cell"></span>
                      <span v-else :class="['stmt-num', { neg: row.values[i] != null && row.values[i] < 0 }]">
                        {{ fmtStmt(row.values[i], row.fmt) }}
                      </span>
                    </template>
                  </el-table-column>
                </el-table>

                <p class="src-note report-note">
                  数据来源：东方财富 datacenter F10 结构化接口（不解析 PDF），报告期 2016 年起。
                  <b>{{ finSubTab === 'BALANCE' ? '资产负债表是季末时点值' : '利润表 / 现金流量表是年初至今累计值' }}</b>，
                  与交易所披露口径一致，不是单季；要看单季请切到「季度对比(财务)」。
                  单位逐格自适应（≥1 亿显示亿、≥1 万显示万），每股收益为元/股。
                  资产减值损失、信用减值损失按报表习惯显示为<b>正的损失额</b>（源侧存的是带符号收益）。
                  银行 / 证券 / 保险使用各自报表模板，通用科目为空的行已自动隐藏。
                </p>
              </div>

              <!-- ── 季度对比(财务)：43 行 + 柱状图 / 曲线图 ── -->
              <template v-else-if="finSubTab === 'summary'">
                <v-chart v-if="financials.length" :option="finChartOption" autoresize class="fin-chart" />
                <div v-else-if="!financialsLoading && !financialsFound" class="empty-note">
                  该股票暂无财报三表数据（源侧未披露或不在采集范围）。
                </div>
                <el-table :data="financials" v-loading="financialsLoading" size="small"
                          empty-text="暂无财报数据" class="fin-table"
                          :row-class-name="finRowClass">
                  <el-table-column label="报告期" width="104" fixed>
                    <template #default="{ row }"><b>{{ periodText(row) }}</b></template>
                  </el-table-column>
                  <!-- 前 11 列为单季值，后 2 列为累计值（顺序 = 用户指定，勿随意调整） -->
                  <el-table-column label="总收入" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qRevenue) }}</template>
                  </el-table-column>
                  <el-table-column label="毛利润" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qGrossProfit) }}</template>
                  </el-table-column>
                  <el-table-column label="毛利率" min-width="76" align="right">
                    <template #default="{ row }">{{ fmtRatio(row.qGrossMargin) }}</template>
                  </el-table-column>
                  <el-table-column label="销售费用" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qSaleExpense) }}</template>
                  </el-table-column>
                  <el-table-column label="管理费用" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qManageExpense) }}</template>
                  </el-table-column>
                  <el-table-column label="研发费用" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qResearchExpense) }}</template>
                  </el-table-column>
                  <el-table-column label="财务费用" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qFinanceExpense) }}</template>
                  </el-table-column>
                  <el-table-column label="营业利润" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qOperateProfit) }}</template>
                  </el-table-column>
                  <el-table-column label="所得税" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qIncomeTax) }}</template>
                  </el-table-column>
                  <el-table-column label="净利润" min-width="86" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qNetprofit) }}</template>
                  </el-table-column>
                  <el-table-column label="净利率" min-width="76" align="right">
                    <template #default="{ row }">{{ fmtRatio(row.qNetMargin) }}</template>
                  </el-table-column>
                  <el-table-column label="累计总收入" min-width="92" align="right">
                    <template #default="{ row }">{{ fmtYi(row.revenue) }}</template>
                  </el-table-column>
                  <el-table-column label="累计归母净利" min-width="110" align="right">
                    <template #default="{ row }">{{ fmtYi(row.parentNetprofit) }}</template>
                  </el-table-column>
                </el-table>
                <p class="src-note report-note">
                  数据来源：东方财富 datacenter 结构化财报接口（无需解析 PDF），报告期从 2016 年起。
                  表格<b>前 11 列为单季值</b>：一季报直接取累计值，二 / 三 / 四季报由年内相邻报告期差分，
                  缺上一期时显示「—」；<b>后 2 列为年初至今累计值</b>。
                  「净利润」是<b>合并口径（含少数股东损益）</b>，与「累计归母净利」不同口径，
                  两者差额即少数股东损益。
                  毛利润 = 单季营业收入 − 单季营业成本；毛利率 = 毛利润 / 单季营业收入
                  （分母是营业收入，不是营业总收入）。
                  净利率 = 单季净利润 / 单季营业总收入（分母为 0 或缺失时显示「—」）。
                  「财务费用」为负 = 利息收入大于支出，按源侧原样显示，不取绝对值。
                  行按<b>报告期年</b>分组：奇数年淡底、年份切换处加深上边框。
                  上方图表可在<b>柱状图 / 曲线图</b>之间切换：柱状看单季高低（按正负分色），
                  曲线看趋势与拐点（缺期断开，不插值连线）。
                  银行 / 证券 / 保险不披露营业成本与毛利，相关列显示「—」。
                  资产负债、经营现金流、股东户数、员工数等看<b>「季度对比(其他)」</b>。
                </p>
              </template>

              <!-- ── 季度对比(其他)：资产负债 / 现金流 / 股本结构（2026-11-10 新增） ── -->
              <template v-else>
                <v-chart v-if="financials.length" :option="finChartOption" autoresize class="fin-chart" />
                <div v-else-if="!financialsLoading && !financialsFound" class="empty-note">
                  该股票暂无财报三表数据（源侧未披露或不在采集范围）。
                </div>
                <el-table :data="financials" v-loading="financialsLoading" size="small"
                          empty-text="暂无财报数据" class="fin-table"
                          :row-class-name="finRowClass">
                  <el-table-column label="报告期" width="88" fixed>
                    <template #default="{ row }"><b>{{ periodText(row) }}</b></template>
                  </el-table-column>
                  <!-- 列顺序 = 用户 2026-11-10 指定，勿随意调整 -->
                  <!-- 16 列要塞进 1400px 容器（.detail-page max-width），列宽按内容压到最小以免横向滚动 -->
                  <el-table-column label="流动资产" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.totalCurrentAssets) }}</template>
                  </el-table-column>
                  <el-table-column label="流动负债" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.totalCurrentLiab) }}</template>
                  </el-table-column>
                  <el-table-column label="负债合计" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.totalLiabilities) }}</template>
                  </el-table-column>
                  <el-table-column label="资产合计" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.totalAssets) }}</template>
                  </el-table-column>
                  <el-table-column label="归母所有者权益" min-width="112" align="right">
                    <template #default="{ row }">{{ fmtYi(row.totalParentEquity) }}</template>
                  </el-table-column>
                  <el-table-column label="短期借款" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.shortLoan) }}</template>
                  </el-table-column>
                  <el-table-column label="长期借款" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.longLoan) }}</template>
                  </el-table-column>
                  <el-table-column label="应收账款" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.accountsRece) }}</template>
                  </el-table-column>
                  <el-table-column label="应付账款" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.accountsPayable) }}</template>
                  </el-table-column>
                  <el-table-column label="存货" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.inventory) }}</template>
                  </el-table-column>
                  <el-table-column label="货币资金" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.monetaryFunds) }}</template>
                  </el-table-column>
                  <el-table-column label="经营活动现金流" min-width="112" align="right">
                    <template #default="{ row }">{{ fmtYi(row.qNetcashOperate) }}</template>
                  </el-table-column>
                  <el-table-column label="期末现金" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtYi(row.endCce) }}</template>
                  </el-table-column>
                  <el-table-column label="员工数量" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtCount(row.employeeNum) }}</template>
                  </el-table-column>
                  <el-table-column label="股东数量" min-width="74" align="right">
                    <template #default="{ row }">{{ fmtCount(row.holderNum) }}</template>
                  </el-table-column>
                </el-table>
                <p class="src-note report-note">
                  数据来源：东方财富 datacenter 结构化财报接口，报告期从 2016 年起。
                  本表是<b>资产负债表视角</b>：前 12 个金额列中，除「经营活动现金流」外
                  都是<b>季末时点值</b>（余额，不做差分）；<b>「经营活动现金流」是单季差分值</b>
                  （一季报取累计值，二 / 三 / 四季报由年内相邻报告期相减，缺上一期显示「—」）。
                  「期末现金」= 期末现金及现金等价物余额（时点值）。
                  「归母所有者权益」= 归属于母公司股东权益合计，不含少数股东权益；
                  与「资产合计 − 负债合计」的差额即少数股东权益。
                  「短期借款 / 长期借款」源侧不披露时显示「—」（例如无有息负债），不代表 0。
                  「员工数量」是<b>年度快照</b>（源侧不披露季报员工数），按报告期所属年份匹配，
                  同一年内四个报告期显示同一数值；库里 2026 年起才逐年累积，更早的报告期为「—」。
                  「股东数量」取该报告期所在季度内<b>最后一个统计截止日</b>的股东户数，
                  不一定等于季末。
                  银行 / 证券 / 保险不按流动 / 非流动划分资产，流动资产、流动负债、存货等列显示「—」。
                </p>
              </template>
            </template>
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
                <div>
                  <span>合计持股</span>
                  <b>{{ fmtWan(holdingsSummary.shares) }} 万股</b>
                  <i v-if="holdingsSummary.shareRatio != null" class="holding-ratio"
                     :title="holdingsRatioTitle">
                    占总股本 {{ holdingsSummary.shareRatio.toFixed(2) }}%
                  </i>
                  <i v-else class="holding-ratio muted" title="源侧未取到该股总股本，无法计算占比">
                    占总股本 —
                  </i>
                </div>
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
              <strong>占总股本</strong> = 合计持股 ÷ <strong>最新总股本</strong>
              （stocks 当前快照，<strong>未</strong>按报告期回溯送转 / 增发前后股本），
              分子同样只含上述披露口径的合计持股，不等于公募基金对该股的全部持仓。
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
.report-note b { color: #888; font-weight: 600; }

/* 财报三表 tab */
.fin-head-tools { display: flex; align-items: center; gap: 10px; }
.fin-summary {
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 12px;
}
.fin-summary > div {
  background: #f8fafc; border: 1px solid #eef1f6; border-radius: 8px;
  padding: 10px 14px; display: flex; flex-direction: column; gap: 4px;
}
.fin-summary span { font-size: 12px; color: #999; }
.fin-summary b { font-size: 16px; color: #1a1a2e; font-weight: 650; }
.fin-chart { height: 280px; width: 100%; margin-bottom: 14px; }
.fin-table {
  width: 100%;
  /* 悬停改暖黄（用户 2026-11-09 指定）：Element Plus 默认是 --el-fill-color-light 冷灰，
     和上面的年份淡底撞色，悬停几乎看不出来。改这一个变量就能让库自带的
     `.el-table--enable-row-hover tr:hover>td` 跟着变。 */
  --el-table-row-hover-bg-color: #fff3cd;
}

/* 逐科目三表（雪球式） */
.stmt-tabs {
  display: flex; gap: 2px; margin: 14px 0 0; border-bottom: 1px solid #e6e9ef;
}
.stmt-tab {
  appearance: none; border: 0; background: transparent; cursor: pointer;
  padding: 8px 16px; font-size: 13.5px; color: #5c6470; border-bottom: 2px solid transparent;
  transition: color .15s, border-color .15s;
}
.stmt-tab:hover { color: #1a1a2e; }
.stmt-tab.active { color: #1554d1; font-weight: 650; border-bottom-color: #1554d1; }

.stmt-toolbar {
  display: flex; align-items: center; justify-content: space-between;
  gap: 12px; flex-wrap: wrap; padding: 12px 0 10px;
}
.stmt-filters { display: flex; gap: 6px; }
.stmt-chip {
  appearance: none; cursor: pointer; font-size: 12.5px; padding: 4px 12px;
  border: 1px solid #e1e5ec; border-radius: 14px; background: #fff; color: #5c6470;
  transition: all .15s;
}
.stmt-chip:hover { border-color: #b9c4d6; color: #1a1a2e; }
.stmt-chip.active { background: #eef3ff; border-color: #1554d1; color: #1554d1; font-weight: 600; }

.stmt-pager { display: flex; align-items: center; gap: 10px; font-size: 12.5px; color: #8a919c; }
.stmt-currency { margin-right: 4px; }
.stmt-nav {
  appearance: none; cursor: pointer; font-size: 12.5px; padding: 4px 10px;
  border: 1px solid #e1e5ec; border-radius: 6px; background: #fff; color: #4a5160;
}
.stmt-nav:hover:not(:disabled) { border-color: #1554d1; color: #1554d1; }
.stmt-nav:disabled { cursor: not-allowed; opacity: .45; }
.stmt-page { color: #4a5160; font-variant-numeric: tabular-nums; }

.stmt-table {
  width: 100%;
  /* 与季度对比表统一悬停色（否则同一个 tab 里两个表两种手感） */
  --el-table-row-hover-bg-color: #fff3cd;
}
.stmt-table :deep(.el-table__cell) { padding: 5px 0; }
.stmt-label { font-size: 12.5px; color: #333b4a; }
.stmt-label.lv-sub { padding-left: 20px; color: #5c6470; }
.stmt-label.lv-total { font-weight: 650; color: #1a1a2e; }
.stmt-num { font-variant-numeric: tabular-nums; font-size: 12.5px; color: #1f2530; }
/* 负数只用负号区分，**不做涨跌色**：A 股绿色代表下跌，财报里的「-184.00万」是
   损失额而不是跌幅，染成绿色会被误读（雪球原文也是纯黑）。 */
/* 分组标题行：灰底加粗、数值列留空 */
.stmt-table :deep(.stmt-row-section) { background: #f6f8fa; }
.stmt-table :deep(.stmt-row-section td) { background: #f6f8fa; }
.stmt-table :deep(.stmt-row-section .stmt-label) { font-weight: 650; color: #1a1a2e; }
.stmt-table :deep(.stmt-row-total .stmt-label) { font-weight: 650; }
.stmt-table :deep(.stmt-row-total) { background: #fcfdff; }
/* 季度对比表：按「报告期年」分组提示（用户 2026-11-09 要求每新的一年略有区分）
   ① 奇数年淡底、偶数年纯白 —— 相邻两年底色必然不同，扫表时能看出年份区块；
   ② 年份切换的第一行加深上边框。表格是报告期倒序，所以边框落在新一年的最上面那行。
   ⚠ 底色必须够淡（#f5f7fb 级）但要肉眼可辨：这是分组提示不是高亮，
     太深会和「负数/合计」抢注意力，太浅（#fafbfe）在 125% 缩放下等于没有。 */
.fin-table :deep(.el-table__body tr.fin-row-alt > td.el-table__cell) { background: #f5f7fb; }
.fin-table :deep(.el-table__body tr.fin-row-year-start > td.el-table__cell) {
  border-top: 2px solid #dfe4ee;
}
.fin-table :deep(.el-table__body tr.fin-row-year-start .cell) { padding-top: 7px; }
/* 悬停黄底必须写在 fin-row-alt 之后：两者选择器权重相同（4 类 + 2 元素），
   只能靠源码顺序胜出，否则奇数年那几行的淡底会把悬停色盖掉（只有隔行会变黄，看着像 bug）。 */
.fin-table :deep(.el-table__body tr:hover > td.el-table__cell) {
  background-color: var(--el-table-row-hover-bg-color);
}
.empty-note { padding: 28px; text-align: center; color: #999; font-size: 13px; }

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
/* 合计持股占总股本：与股数同格，小字灰蓝，鼠标悬浮给出分子分母与口径 */
.holding-ratio {
  font-size: 12px; font-style: normal; color: #3f5568; font-weight: 500;
  font-variant-numeric: tabular-nums; cursor: help;
}
.holding-ratio.muted { color: #bbb; cursor: default; }
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
