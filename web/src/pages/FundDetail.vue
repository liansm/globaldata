<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DataZoomComponent,
  LegendComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { fetchFundDetail, fetchFundNav, fetchFundNavSummary, fetchFundYearly } from '@/api/funds'
import AnnotatedLineChart from '@/components/AnnotatedLineChart.vue'
import { ytdFrom } from '@/lib/dateRange'
import type {
  FundDetailResp, FundHolding, FundNavPoint, FundNavSummaryResp, FundYearlyItem,
} from '@/types/fund'

use([
  CanvasRenderer,
  LineChart,
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DataZoomComponent,
  LegendComponent,
])

const route  = useRoute()
const router = useRouter()

const loading   = ref(true)
const error     = ref('')
const detail    = ref<FundDetailResp | null>(null)
const reportDate = ref<string>('')

// ── 净值 ────────────────────────────────────────────────────────────────────
const navPoints  = ref<FundNavPoint[]>([])
const navLoading = ref(false)

/** 区间：天数（0 = 全部历史）或 'ytd'（今年来，按自然年取 1 月 1 日起） */
type NavRange = number | 'ytd'
const navRange = ref<NavRange>(365)

/** 货币基金口径不同：nav=万份收益(元)、accNav=七日年化(%)，没有「单位净值」 */
const isMoney = computed(() => detail.value?.navKind === 'money')

/** 列名随口径切换（图表 legend 与业绩表格共用） */
const navLabel = computed(() => (isMoney.value ? '万份收益' : '单位净值'))
const accLabel = computed(() => (isMoney.value ? '七日年化' : '累计净值'))
/** 表格列头带上单位，值只给数字 */
const navColLabel = computed(() => (isMoney.value ? '万份收益 (元)' : '单位净值'))
const accColLabel = computed(() => (isMoney.value ? '七日年化 (%)' : '累计净值'))

/**
 * 收益率的**副口径**（累计净值 / 现金分红不再投）。
 * 只在两口径确有差别时返回数值 —— 没分过红的基金两者恒等，再摆一行只是噪音。
 * 首屏那行小字与 tooltip 共用它，保证「显示出来的」和「提示里说的」永远是同一个数。
 */
function accAlt(kind: 'ytd' | 'total'): number | null {
  const d = detail.value
  const main = kind === 'ytd' ? d?.ytdReturn : d?.totalReturn
  const acc = kind === 'ytd' ? d?.ytdReturnAcc : d?.totalReturnAcc
  if (acc == null || main == null) return null
  return Math.abs(acc - main) < 0.01 ? null : acc
}
const ytdAccAlt = computed(() => accAlt('ytd'))
const totalAccAlt = computed(() => accAlt('total'))

/**
 * 收益率口径提示：主数字是**复权**（分红再投），下面那行小字是**分红不复投**。
 * 分红越多的基金两者差得越远 —— 163402 复权 2128% / 分红不复投只有 978%，
 * 只说一个数会让用户以为算错了。
 */
function returnTip(kind: 'ytd' | 'total'): string {
  const d = detail.value
  const main = kind === 'ytd' ? d?.ytdReturn : d?.totalReturn
  const acc = kind === 'ytd' ? d?.ytdReturnAcc : d?.totalReturnAcc
  const head = kind === 'ytd'
    ? '今年以来涨跌幅，按复权净值计算（分红再投），与雪球/天天基金一致'
    : '成立以来累计收益，按复权净值计算（分红再投），与雪球/天天基金一致'
  // 没分过红（累计净值恒等于单位净值）时两口径相同，别给一句「差额来自分红」的空话
  if (acc == null || main == null || Math.abs(acc - main) < 0.01) {
    return `${head}。该基金没有分过红，两种口径一致`
  }
  return `${head}；下面「分红不复投」按累计净值口径计算为 ${acc.toFixed(2)}%，`
    + '差额来自分红没有参与复利'
}

/** 走势图区间摘要：区间涨跌（双口径）与区间最高最低 */
const navSummary = ref<FundNavSummaryResp | null>(null)

async function loadNav(code: string) {
  navLoading.value = true
  const opt = navRange.value === 'ytd' ? { from: ytdFrom() } : { days: navRange.value as number }
  try {
    // 摘要与序列并行取：摘要只依赖区间，跟序列的分页无关。
    // 摘要失败不该拖垮图表（区间涨跌条少一行而已），单独 catch 掉。
    const [resp, sum] = await Promise.all([
      fetchFundNav(code, opt),
      fetchFundNavSummary(code, opt).catch(() => null),
    ])
    navPoints.value = resp.items
    navSummary.value = sum
  } catch {
    navPoints.value = []
    navSummary.value = null
  } finally {
    navLoading.value = false
  }
}

/**
 * 区间涨跌的副口径（分红不复投）。与首屏 `accAlt()` 同一套判断：
 * 两口径差 <0.01 就不显示 —— 短期（近 3 月）分红影响常常小到看不出，
 * 摆一个和主数字一模一样的数只是噪音。
 */
const rangeAccAlt = computed(() => {
  const s = navSummary.value
  if (!s || s.adjReturn == null || s.accReturn == null) return null
  return Math.abs(s.accReturn - s.adjReturn) < 0.01 ? null : s.accReturn
})

/** 区间选择器当前选项的中文名，用在涨跌条开头 */
const navRangeLabel = computed(() => {
  const r = navRange.value
  if (r === 'ytd') return '今年来'
  if (r === 0) return '全部历史'
  if (r === 90) return '近 3 月'
  if (r === 365) return '近 1 年'
  if (r === 1095) return '近 3 年'
  if (r === 1825) return '近 5 年'
  return `近 ${r} 天`
})

/** 空图提示：选「今年来」但该基金今年没有净值时，把最新数据日说清楚（多为已终止/停更） */
const emptyNavHint = computed(() => {
  if (navRange.value !== 'ytd') {
    return '暂无净值数据（该基金未抓取到净值，或未在所选区间内）'
  }
  const y = ytdFrom().slice(0, 4)
  const d = detail.value?.latestNavDate
  return d
    ? `该基金 ${y} 年暂无净值（最新数据为 ${d.slice(0, 10)}）`
    : `该基金 ${y} 年暂无净值数据`
})

// ── 基金业绩（近期净值 / 历史业绩） ─────────────────────────────────────────
// 与上面的走势图相互独立：走势图按所选区间取数，这里固定取「最近 N 条」和全年度，
// 两者不共用 navPoints。
const SUMMARY_ROWS  = 10   // 概况区默认展示条数（净值 / 年度都取 10）
const DIALOG_PAGE   = 50   // 「全部净值」每页条数

const perfTab = ref<'nav' | 'yearly'>('nav')

const recentNav      = ref<FundNavPoint[]>([])
const recentNavTotal = ref(0)

const navDlgVisible = ref(false)
const navDlgLoading = ref(false)
const navDlgItems   = ref<FundNavPoint[]>([])
const navDlgTotal   = ref(0)
const navPage       = ref(1)
const navPageSize   = ref(DIALOG_PAGE)

const yearly = ref<FundYearlyItem[]>([])
const yearDlgVisible = ref(false)

async function loadPerf(code: string) {
  try {
    const [navResp, yearResp] = await Promise.all([
      // days=0 → 全历史，配合 order=desc + limit 拿最近 N 条，total 拿到总条数
      fetchFundNav(code, { days: 0, limit: SUMMARY_ROWS, order: 'desc' }),
      fetchFundYearly(code),
    ])
    recentNav.value      = navResp.items
    recentNavTotal.value = navResp.total
    yearly.value         = yearResp.items
  } catch {
    recentNav.value = []
    recentNavTotal.value = 0
    yearly.value = []
  }
}

async function loadNavDialog() {
  const code = route.params.code as string
  if (!code) return
  navDlgLoading.value = true
  try {
    const resp = await fetchFundNav(code, {
      days: 0,
      limit: navPageSize.value,
      offset: (navPage.value - 1) * navPageSize.value,
      order: 'desc',
    })
    navDlgItems.value = resp.items
    navDlgTotal.value = resp.total
  } catch {
    navDlgItems.value = []
    navDlgTotal.value = 0
  } finally {
    navDlgLoading.value = false
  }
}

function openNavDialog() {
  navPage.value = 1
  navDlgVisible.value = true
  loadNavDialog()
}

function onNavPageChange() {
  loadNavDialog()
}

function onNavSizeChange(size: number) {
  navPageSize.value = size
  navPage.value = 1
  loadNavDialog()
}

async function load(code: string) {
  loading.value = true
  error.value = ''
  try {
    detail.value = await fetchFundDetail(code, reportDate.value || undefined)
    // 报告期不在列表中（如 URL 带了无效 date）时回落到最新
    if (detail.value.reportDates.length && !detail.value.reportDates.includes(reportDate.value)) {
      reportDate.value = detail.value.reportDates[0]
    }
    await loadNav(code)
    await loadPerf(code)
  } catch (e: any) {
    const msg = e?.response?.data?.error
    error.value = msg ?? '加载失败，请检查后端服务是否启动'
    detail.value = null
  } finally {
    loading.value = false
  }
}

watch(
  () => route.params.code as string,
  (code) => { if (code) load(code) },
  { immediate: true },
)

watch(reportDate, () => {
  const code = route.params.code as string
  if (code) load(code)
})

watch(navRange, () => {
  const code = route.params.code as string
  if (code) loadNav(code)
})

const stockHoldings = computed<FundHolding[]>(() =>
  (detail.value?.holdings ?? []).filter(h => h.holdingType === 'stock'))
const bondHoldings = computed<FundHolding[]>(() =>
  (detail.value?.holdings ?? []).filter(h => h.holdingType === 'bond'))

/** 重仓合计占净值比例（股票） */
const stockRatioSum = computed(() =>
  stockHoldings.value.reduce((s, h) => s + (h.ratio ?? 0), 0))

/** 比例条最大值（用于归一化宽度） */
const maxRatio = computed(() => {
  const all = [...stockHoldings.value, ...bondHoldings.value]
  return Math.max(1, ...all.map(h => h.ratio ?? 0))
})

function goBack() {
  router.push('/funds-info')
}

function goCompany(key: string) {
  router.push(`/company/${encodeURIComponent(key)}`)
}

// ── 净值走势图 ──────────────────────────────────────────────────────────────
const NAV_COLOR = '#e8534a'   // 涨色（中式约定）
const ACC_COLOR = '#378ADD'

/** 是否有累计净值（/七日年化）数据：决定右图是否显示 */
const hasAcc = computed(() => navPoints.value.some(p => p.accNav != null))

/**
 * 单列净值折线图配置：单位净值 / 累计净值各画一张，左右并排，互不干扰。
 * 之前试过一张图双 Y 轴，但分红多的基金（如 163402 单位 2.94 / 累计 10.78）
 * 单位净值会被压成平线，量级差到了看不出波动的程度 → 拆成两张图更直观。
 *
 * @param title  轴名 / 系列名 / 图标题（单位净值、累计净值、万份收益、七日年化）
 * @param data   该列数值
 * @param color  曲线与轴标签同色，保证「看哪条线读哪侧」
 * @param unit   数值单位后缀（货币：万份收益→' 元'，七日年化→' %'）
 */
function buildNavChart(title: string, data: (number | null)[], color: string, unit: string): any {
  const pts = navPoints.value
  const dates = pts.map(p => p.date)
  const fmt = (v: number) => v.toLocaleString('zh-CN', { maximumFractionDigits: 4 })

  return {
    legend: { top: 0, right: 8, itemWidth: 14, itemHeight: 8, textStyle: { fontSize: 12, color: '#666' } },
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const p = pts[params?.[0]?.dataIndex]
        if (!p) return ''
        const v = params?.[0]?.value
        const lines = [p.date]
        if (v != null) lines.push(`${title}：<b>${fmt(v)}</b>${unit}`)
        if (p.dailyReturn != null) lines.push(`日涨跌：${p.dailyReturn}%`)
        return lines.join('<br/>')
      },
    },
    grid: { top: 36, right: 26, bottom: 62, left: 64 },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: { rotate: 30, fontSize: 11, color: '#888' },
      axisLine: { lineStyle: { color: '#ddd' } },
    },
    yAxis: {
      type: 'value',
      scale: true,                 // 净值在 1 附近，绝不能从 0 起
      name: title,
      nameLocation: 'middle',      // 轴名放轴中段竖排，避开右上角图例
      nameGap: 44,
      nameRotate: 90,
      nameTextStyle: { fontSize: 11, fontWeight: 500, color },
      axisLabel: { fontSize: 11, color, formatter: (v: number) => fmt(v) },
      splitLine: { lineStyle: { color: '#f0f0f0' } },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, height: 22, bottom: 4 },
    ],
    series: [{
      name: title,
      type: 'line',
      data,
      showSymbol: false,
      connectNulls: true,
      lineStyle: { width: 1.6, color },
      itemStyle: { color },
    }],
  }
}

const navChartOption = computed(() => {
  const pts = navPoints.value
  if (!pts.length) return {}
  const unit = isMoney.value ? ' 元' : ''
  return buildNavChart(navLabel.value, pts.map(p => p.nav), NAV_COLOR, unit)
})

/** 右图：累计净值 / 七日年化。无数据（罕见）则不显示 */
const accChartOption = computed(() => {
  const pts = navPoints.value
  if (!pts.length) return {}
  const accs = pts.map(p => p.accNav)
  if (!accs.some(v => v != null)) return {}
  const unit = isMoney.value ? ' %' : ''
  return buildNavChart(accLabel.value, accs, ACC_COLOR, unit)
})

// ── Formatters ──────────────────────────────────────────────────────────────
function fmtScale(v: number | null) {
  if (v == null) return '—'
  if (v >= 100) return v.toFixed(0)
  if (v >= 1)   return v.toFixed(2)
  return v.toFixed(4)
}

function fmtRatio(v: number | null) {
  return v == null ? '—' : v.toFixed(2) + '%'
}

function fmtShares(v: number | null) {
  if (v == null) return '—'
  if (v >= 10_000) return (v / 10_000).toFixed(2) + ' 亿股'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }) + ' 万'
}

function fmtMv(v: number | null) {
  if (v == null) return '—'
  if (v >= 10_000) return (v / 10_000).toFixed(2) + ' 亿'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }) + ' 万'
}

function fmtDate(d: string | null | undefined) {
  return d ? d.slice(0, 10) : '—'
}

function fmtNav(v: number | null) {
  return v == null
    ? '—'
    : v.toLocaleString('zh-CN', { minimumFractionDigits: 4, maximumFractionDigits: 4 })
}

function fmtPct(v: number | null) {
  if (v == null) return '—'
  return (v > 0 ? '+' : '') + v.toFixed(2) + '%'
}

/** 涨红跌绿（中式约定）。0 与 null 都返回空串 */
function pctClass(v: number | null | undefined) {
  if (v == null || v === 0) return ''
  return v > 0 ? 'up' : 'down'
}

/** 「全部净值」弹窗当前页覆盖的行号范围，如 51–100 / 共 6008 */
const navRangeText = computed(() => {
  if (!navDlgTotal.value) return '0 条'
  const from = (navPage.value - 1) * navPageSize.value + 1
  const to   = Math.min(navPage.value * navPageSize.value, navDlgTotal.value)
  return `${from}–${to} / 共 ${navDlgTotal.value.toLocaleString('zh-CN')} 条`
})
</script>

<template>
  <div class="fund-detail-page">
    <div class="page-header">
      <el-button text @click="goBack" class="back-btn">
        <el-icon style="margin-right: 4px"><ArrowLeft /></el-icon>返回列表
      </el-button>
    </div>

    <el-alert
      v-if="error"
      :title="error"
      type="error"
      show-icon
      :closable="false"
      style="margin-bottom: 20px"
    />

    <div v-if="loading" class="skeleton-wrap">
      <el-skeleton :rows="8" animated />
    </div>

    <template v-else-if="detail">
      <!-- 基金信息头 -->
      <section class="meta-card">
        <div class="meta-top">
          <h1 class="fund-title">{{ detail.fundName }}</h1>
          <el-tag size="small" type="info" class="fund-code">{{ detail.fundCode }}</el-tag>
        </div>

        <div class="meta-stats">
          <!-- 收益率排在最前：这是看一只基金最先要的两个数。
               分红多的基金两种口径差得很远（163402 复权 2128% / 分红不复投 978%），
               所以副口径直接摆在主数字下面，不让人靠悬停才发现。 -->
          <div
            class="stat"
            v-if="detail.ytdReturn != null"
            :title="returnTip('ytd')"
          >
            <span class="stat-label">今年来<span class="stat-flag">复权</span></span>
            <span class="stat-value" :class="pctClass(detail.ytdReturn)">{{ fmtPct(detail.ytdReturn) }}</span>
            <span class="stat-sub" v-if="ytdAccAlt != null">
              分红不复投
              <b :class="pctClass(ytdAccAlt)">{{ fmtPct(ytdAccAlt) }}</b>
            </span>
          </div>
          <div
            class="stat"
            v-if="detail.totalReturn != null"
            :title="returnTip('total')"
          >
            <span class="stat-label">累计收益<span class="stat-flag">复权</span></span>
            <span class="stat-value" :class="pctClass(detail.totalReturn)">{{ fmtPct(detail.totalReturn) }}</span>
            <span class="stat-sub" v-if="totalAccAlt != null">
              分红不复投
              <b :class="pctClass(totalAccAlt)">{{ fmtPct(totalAccAlt) }}</b>
            </span>
          </div>
          <div class="stat" v-if="detail.latestNav != null">
            <span class="stat-label">{{ isMoney ? '万份收益' : '单位净值' }}</span>
            <span class="stat-value">
              {{ fmtNav(detail.latestNav) }}<span class="stat-unit">{{ isMoney ? '元' : '' }}</span>
            </span>
          </div>
          <div class="stat" v-if="detail.latestAccNav != null">
            <span class="stat-label">{{ isMoney ? '七日年化' : '累计净值' }}</span>
            <span class="stat-value">
              {{ fmtNav(detail.latestAccNav) }}<span class="stat-unit">{{ isMoney ? '%' : '' }}</span>
            </span>
          </div>
          <div class="stat" v-if="detail.latestDailyReturn != null">
            <span class="stat-label">日涨跌</span>
            <span class="stat-value" :class="detail.latestDailyReturn > 0 ? 'up' : detail.latestDailyReturn < 0 ? 'down' : ''">
              {{ fmtPct(detail.latestDailyReturn) }}
            </span>
          </div>
          <div class="stat" v-if="detail.latestNavDate">
            <span class="stat-label">净值日期</span>
            <span class="stat-value">{{ fmtDate(detail.latestNavDate) }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">最新规模</span>
            <span class="stat-value">{{ fmtScale(detail.scale) }}<span class="stat-unit">亿元</span></span>
          </div>
          <div class="stat">
            <span class="stat-label">类型</span>
            <span class="stat-value text">{{ detail.fundType ?? '—' }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">基金经理</span>
            <span class="stat-value text">{{ detail.fundManager ?? '—' }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">基金公司</span>
            <span
              v-if="detail.companyKey"
              class="stat-value text link"
              :title="`查看 ${detail.fundCompany} 的全部产品`"
              @click="goCompany(detail.companyKey)"
            >{{ detail.companyKey }}</span>
            <span v-else class="stat-value text">{{ detail.fundCompany ?? '—' }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">成立日期</span>
            <span class="stat-value">{{ fmtDate(detail.inceptionDate) }}</span>
          </div>
          <div class="stat" v-if="detail.reportDates.length">
            <span class="stat-label">前十大重仓合计</span>
            <span class="stat-value">{{ stockRatioSum.toFixed(2) }}%</span>
          </div>
        </div>
      </section>

      <!-- 净值走势 -->
      <section class="section">
        <div class="section-header">
          <h2 class="section-title">
            净值走势
            <span class="section-sub" v-if="detail.latestNavDate">
              （数据至 {{ fmtDate(detail.latestNavDate) }}）
            </span>
          </h2>
          <!-- 「今年来」放首位，与 MarketDetail.vue / Detail.vue 的区间选择器一致 -->
          <el-select v-model="navRange" size="small" style="width: 120px">
            <el-option value="ytd" label="今年来" />
            <el-option :value="90" label="近 3 月" />
            <el-option :value="365" label="近 1 年" />
            <el-option :value="1095" label="近 3 年" />
            <el-option :value="1825" label="近 5 年" />
            <el-option :value="0" label="全部历史" />
          </el-select>
        </div>

        <!-- 区间涨跌：随上面选的区间变，所以放在图上方而不是首屏。
             两个口径都摆出来 —— 分红多的基金同一个区间能差一倍（163402 全部历史
             复权 +2128% / 分红不复投 +978%），只给一个数会让人以为另一个算错了。
             货币基金不显示：它的两列是万份收益与七日年化，没有「区间涨跌」概念。 -->
        <div
          v-if="!navLoading && navSummary && navSummary.days > 0 && !isMoney"
          class="range-strip"
        >
          <span
            class="rs-scope"
            :title="`区间 ${navSummary.from} 至 ${navSummary.to}（${navSummary.days} 个披露日）；涨跌幅以 ${navSummary.baseDate} 的净值为基数`"
          >
            {{ navRangeLabel }}<i>{{ navSummary.days }} 日</i>
          </span>
          <span class="rs-cell">
            <span class="rs-k">区间涨跌<em class="stat-flag">复权</em></span>
            <b class="rs-v" :class="pctClass(navSummary.adjReturn)">{{ fmtPct(navSummary.adjReturn) }}</b>
          </span>
          <span
            v-if="rangeAccAlt != null"
            class="rs-cell"
            title="现金分红不再投（不参与复利）。无分红的区间与复权口径相同；分红越多差得越远"
          >
            <span class="rs-k">分红不复投</span>
            <b class="rs-v" :class="pctClass(rangeAccAlt)">{{ fmtPct(rangeAccAlt) }}</b>
          </span>
          <span v-if="navSummary.navMax" class="rs-cell">
            <span class="rs-k">区间最高</span>
            <b class="rs-v">{{ fmtNav(navSummary.navMax.v) }}</b>
            <i class="rs-d">{{ navSummary.navMax.d }}</i>
          </span>
          <span v-if="navSummary.navMin" class="rs-cell">
            <span class="rs-k">区间最低</span>
            <b class="rs-v">{{ fmtNav(navSummary.navMin.v) }}</b>
            <i class="rs-d">{{ navSummary.navMin.d }}</i>
          </span>
        </div>

        <!-- 口径提示：货币基金量纲与普通基金完全不同，且源侧有「节假日合并披露」特性 -->
        <p v-if="isMoney && navPoints.length" class="nav-caveat">
          货币基金口径：<b>左图</b>为<b>万份收益</b>（元/万份），<b>右图</b>为<b>七日年化</b>（%）。
          源侧把节假日期间的收益合并到节后首个披露日，个别日期会明显偏高，
          属口径特性而非数据异常；部分场内/停售份额源侧不单独披露收益，此类点已剔除。
        </p>

        <div v-if="navLoading" class="chart-loading">
          <el-skeleton :rows="5" animated />
        </div>

        <el-empty
          v-else-if="!navPoints.length"
          :description="emptyNavHint"
        />

        <div v-else class="chart-card nav-charts">
          <div class="nav-chart-pane">
            <h4 class="nav-chart-title" :style="{ color: NAV_COLOR }">{{ navLabel }}</h4>
            <AnnotatedLineChart :option="navChartOption" autoresize style="width:100%;height:360px" />
          </div>
          <div v-if="hasAcc" class="nav-chart-pane">
            <h4 class="nav-chart-title" :style="{ color: ACC_COLOR }">{{ accLabel }}</h4>
            <AnnotatedLineChart :option="accChartOption" autoresize style="width:100%;height:360px" />
          </div>
        </div>
      </section>

      <!-- 基金业绩：近期净值 / 历史业绩 -->
      <section class="section">
        <div class="section-header">
          <h2 class="section-title">基金业绩</h2>
        </div>

        <el-tabs v-model="perfTab" class="perf-tabs">
          <!-- ① 近期净值：默认最近 10 条，点「查看全部」进分页 -->
          <el-tab-pane label="近期净值" name="nav">
            <div class="perf-bar">
              <span class="perf-hint">
                共 {{ recentNavTotal.toLocaleString('zh-CN') }} 个披露日，此处显示最近 {{ recentNav.length }} 条
              </span>
              <el-button
                v-if="recentNavTotal > recentNav.length"
                text
                type="primary"
                size="small"
                @click="openNavDialog"
              >查看全部 →</el-button>
            </div>

            <el-empty
              v-if="!recentNav.length"
              :description="isMoney ? '暂无万份收益数据' : '暂无净值数据'"
            />
            <el-table
              v-else
              :data="recentNav"
              class="perf-table"
              :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
            >
              <el-table-column prop="date" label="净值日期" width="130" />
              <el-table-column :label="navColLabel" align="right" min-width="130">
                <template #default="{ row }">{{ fmtNav(row.nav) }}</template>
              </el-table-column>
              <el-table-column :label="accColLabel" align="right" min-width="130">
                <template #default="{ row }">{{ fmtNav(row.accNav) }}</template>
              </el-table-column>
              <el-table-column label="日涨跌" align="right" min-width="110">
                <template #default="{ row }">
                  <span :class="pctClass(row.dailyReturn)">{{ fmtPct(row.dailyReturn) }}</span>
                </template>
              </el-table-column>
            </el-table>
          </el-tab-pane>

          <!-- ② 历史业绩：每年涨跌幅 -->
          <el-tab-pane label="历史业绩" name="yearly">
            <p v-if="isMoney" class="nav-caveat">
              货币基金没有净值涨跌幅概念（单位净值列存的是万份收益、累计净值列存的是七日年化），
              故不列年度业绩，「近期净值」页可看万份收益与七日年化。
            </p>
            <template v-else>
              <div class="perf-bar">
                <span class="perf-hint">
                  共 {{ yearly.length }} 个年度，此处显示最近 {{ Math.min(yearly.length, SUMMARY_ROWS) }} 个
                </span>
                <el-button
                  v-if="yearly.length > SUMMARY_ROWS"
                  text
                  type="primary"
                  size="small"
                  @click="yearDlgVisible = true"
                >查看全部 →</el-button>
              </div>

              <el-empty v-if="!yearly.length" description="暂无年度业绩数据" />
              <el-table
                v-else
                :data="yearly.slice(0, SUMMARY_ROWS)"
                class="perf-table"
                :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
              >
                <el-table-column label="年份" width="130">
                  <template #default="{ row }">{{ row.year }} 年</template>
                </el-table-column>
                <el-table-column label="年度涨跌" align="right" min-width="130">
                  <template #default="{ row }">
                    <span :class="pctClass(row.ret)">{{ fmtPct(row.ret) }}</span>
                  </template>
                </el-table-column>
              </el-table>
            </template>
          </el-tab-pane>
        </el-tabs>

        <p class="data-note" v-if="!isMoney">
          年度涨跌幅按<b>复权净值</b>计算（分红再投），与首屏「累计收益」同一口径，
          也与雪球/天天基金的年度榜一致；基金成立当年即「有数据以来」，该年第一个净值只当基数。
          <template
            v-if="detail.totalReturnAcc != null && detail.totalReturn != null
                  && Math.abs(detail.totalReturnAcc - detail.totalReturn) >= 0.01"
          >
            若改用分红不复投（累计净值口径），成立以来为 <b>{{ fmtPct(detail.totalReturnAcc) }}</b>。
          </template>
        </p>

        <!-- 全部净值（分页） -->
        <el-dialog v-model="navDlgVisible" title="全部净值" width="640px" top="6vh">
          <div class="perf-bar">
            <span class="perf-hint">显示 {{ navRangeText }}</span>
            <span class="perf-hint">每页 {{ navPageSize }} 条</span>
          </div>
          <el-table
            v-loading="navDlgLoading"
            :data="navDlgItems"
            height="60vh"
            :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
          >
            <el-table-column prop="date" label="净值日期" width="130" />
            <el-table-column :label="navColLabel" align="right" min-width="130">
              <template #default="{ row }">{{ fmtNav(row.nav) }}</template>
            </el-table-column>
            <el-table-column :label="accColLabel" align="right" min-width="130">
              <template #default="{ row }">{{ fmtNav(row.accNav) }}</template>
            </el-table-column>
            <el-table-column label="日涨跌" align="right" min-width="110">
              <template #default="{ row }">
                <span :class="pctClass(row.dailyReturn)">{{ fmtPct(row.dailyReturn) }}</span>
              </template>
            </el-table-column>
          </el-table>
          <el-pagination
            v-model:current-page="navPage"
            :page-size="navPageSize"
            :page-sizes="[50, 100, 200]"
            :total="navDlgTotal"
            layout="total, sizes, prev, pager, next, jumper"
            class="perf-pager"
            @current-change="onNavPageChange"
            @size-change="onNavSizeChange"
          />
        </el-dialog>

        <!-- 历史业绩（全部年度） -->
        <el-dialog v-model="yearDlgVisible" title="历史业绩 · 全部年度" width="420px" top="8vh">
          <el-table
            :data="yearly"
            height="64vh"
            :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
          >
            <el-table-column label="年份" width="130">
              <template #default="{ row }">{{ row.year }} 年</template>
            </el-table-column>
            <el-table-column label="年度涨跌" align="right" min-width="130">
              <template #default="{ row }">
                <span :class="pctClass(row.ret)">{{ fmtPct(row.ret) }}</span>
              </template>
            </el-table-column>
          </el-table>
        </el-dialog>
      </section>

      <!-- 持仓 -->
      <section class="section">
        <div class="section-header">
          <h2 class="section-title">重仓持仓</h2>
          <el-select
            v-if="detail.reportDates.length"
            v-model="reportDate"
            size="small"
            style="width: 150px"
          >
            <el-option
              v-for="d in detail.reportDates"
              :key="d"
              :label="d"
              :value="d"
            />
          </el-select>
        </div>

        <el-empty
          v-if="!detail.reportDates.length"
          description="暂无持仓数据（季报未披露或未抓取）"
        />

        <template v-else>
          <h3 v-if="stockHoldings.length" class="sub-title">股票持仓 · 前十大重仓</h3>
          <el-table
            v-if="stockHoldings.length"
            :data="stockHoldings"
            style="width: 100%"
            class="holding-table"
            :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
          >
            <el-table-column type="index" label="#" width="44" align="center" />
            <el-table-column prop="securityCode" label="股票代码" width="90">
              <template #default="{ row }">
                <span class="sec-code">{{ row.securityCode }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="securityName" label="股票名称" min-width="130">
              <template #default="{ row }">
                <span class="sec-name">{{ row.securityName ?? '—' }}</span>
              </template>
            </el-table-column>
            <el-table-column label="占净值比例" min-width="180" align="right">
              <template #default="{ row }">
                <div class="ratio-cell">
                  <span class="ratio-text">{{ fmtRatio(row.ratio) }}</span>
                  <div class="ratio-bar-track">
                    <div
                      class="ratio-bar"
                      :style="{ width: ((row.ratio ?? 0) / maxRatio * 100).toFixed(1) + '%' }"
                    />
                  </div>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="持股数 (万)" width="120" align="right">
              <template #default="{ row }">{{ fmtShares(row.shares) }}</template>
            </el-table-column>
            <el-table-column label="持仓市值 (万)" width="120" align="right">
              <template #default="{ row }">{{ fmtMv(row.marketValue) }}</template>
            </el-table-column>
          </el-table>

          <h3 v-if="bondHoldings.length" class="sub-title">债券持仓 · 前五大重仓</h3>
          <el-table
            v-if="bondHoldings.length"
            :data="bondHoldings"
            style="width: 100%"
            class="holding-table"
            :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
          >
            <el-table-column type="index" label="#" width="44" align="center" />
            <el-table-column prop="securityCode" label="债券代码" width="100">
              <template #default="{ row }">
                <span class="sec-code">{{ row.securityCode }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="securityName" label="债券名称" min-width="220" show-overflow-tooltip>
              <template #default="{ row }">{{ row.securityName ?? '—' }}</template>
            </el-table-column>
            <el-table-column label="占净值比例" min-width="180" align="right">
              <template #default="{ row }">
                <div class="ratio-cell">
                  <span class="ratio-text">{{ fmtRatio(row.ratio) }}</span>
                  <div class="ratio-bar-track">
                    <div
                      class="ratio-bar bond"
                      :style="{ width: ((row.ratio ?? 0) / maxRatio * 100).toFixed(1) + '%' }"
                    />
                  </div>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="持仓市值 (万)" width="130" align="right">
              <template #default="{ row }">{{ fmtMv(row.marketValue) }}</template>
            </el-table-column>
          </el-table>
        </template>
      </section>

      <p class="data-note">
        持仓来源于基金季报披露（前十大重仓股 / 前五大重仓债券），完整持仓以半年报/年报为准。
      </p>
    </template>
  </div>
</template>

<style scoped>
.fund-detail-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 24px 20px 60px;
}

.page-header {
  margin-bottom: 12px;
}

.back-btn {
  font-size: 13px;
  color: #666;
  padding-left: 0;
}

/* ── 信息头 ─────────────────────────────────────────────────────────────── */
.meta-card {
  background: #fff;
  border: 1px solid #e8e8f0;
  border-radius: 12px;
  padding: 22px 26px;
  margin-bottom: 28px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}

.meta-top {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 18px;
}

.fund-title {
  margin: 0;
  font-size: 22px;
  font-weight: 700;
  color: #1a1a2e;
}

.fund-code {
  font-variant-numeric: tabular-nums;
}

.meta-stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 16px;
  border-top: 1px solid #f0f0f5;
  padding-top: 16px;
}

.stat {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.stat-label {
  font-size: 11px;
  color: #aaa;
  text-transform: uppercase;
  letter-spacing: 0.4px;
}

/* 标签后面的小徽标，标明这行是哪个口径（复权 = 分红再投） */
.stat-flag {
  margin-left: 4px;
  padding: 0 3px;
  border: 1px solid #e4e4ee;
  border-radius: 3px;
  font-size: 10px;
  color: #9a9aa8;
  letter-spacing: 0;
  text-transform: none;
}

/* 副口径行：分红不复投的净值涨幅，与主数字上下对齐成一列便于对照 */
.stat-sub {
  font-size: 11px;
  color: #8a8a96;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
}

.stat-sub b { font-weight: 600; }
.stat-sub .up   { color: #e8534a; }
.stat-sub .down { color: #26a17b; }

.stat-value {
  font-size: 16px;
  font-weight: 700;
  color: #1a1a2e;
  font-variant-numeric: tabular-nums;
}

.stat-value.text {
  font-size: 14px;
  font-weight: 500;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.stat-value.link {
  color: #409eff;
  cursor: pointer;
}

.stat-value.link:hover {
  text-decoration: underline;
}

.stat-unit {
  font-size: 12px;
  font-weight: 400;
  color: #999;
  margin-left: 3px;
}

/* 涨红跌绿（中式约定，与站点其他页面一致） */
.stat-value.up   { color: #e8534a; }
.stat-value.down { color: #26a17b; }

/* 业绩表格里的涨跌幅（pctClass 返回 up/down，与上面的 stat-value 同色系） */
.up   { color: #e8534a; }
.down { color: #26a17b; }

/* ── 基金业绩 ───────────────────────────────────────────────────────────── */
.perf-tabs {
  margin-bottom: 4px;
}

.perf-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin: 0 0 8px;
  min-height: 24px;
}

.perf-hint {
  font-size: 12px;
  color: #999;
}

.perf-table {
  border-radius: 10px;
  overflow: hidden;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}

.perf-pager {
  margin-top: 12px;
  justify-content: flex-end;
}

/* ── 净值走势 ───────────────────────────────────────────────────────────── */
.section-sub {
  font-size: 12px;
  font-weight: 400;
  color: #aaa;
}

/* 区间涨跌条：一行横向排布，窄屏自动折行 */
.range-strip {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 5px 22px;
  margin-bottom: 12px;
  padding: 9px 14px;
  background: #fafafc;
  border: 1px solid #f0f0f4;
  border-radius: 8px;
}

.rs-scope {
  font-size: 13px;
  font-weight: 600;
  color: #1a1a2e;
}

/* 披露日数跟在区间名后面，弱化处理 */
.rs-scope i {
  margin-left: 5px;
  font-style: normal;
  font-weight: 400;
  font-size: 11px;
  color: #9a9aa8;
}

.rs-cell {
  display: inline-flex;
  align-items: baseline;
  gap: 6px;
}

.rs-k {
  font-size: 12px;
  color: #8a8a96;
}

.rs-v {
  font-size: 14px;
  font-weight: 700;
  color: #1a1a2e;
  font-variant-numeric: tabular-nums;
}

/* 最高/最低的发生日期：只给 MM-DD，完整日期放 title */
.rs-d {
  font-style: normal;
  font-size: 11px;
  color: #9a9aa8;
  font-variant-numeric: tabular-nums;
}

.chart-card {
  background: #fff;
  border: 1px solid #eef0f3;
  border-radius: 10px;
  padding: 12px 8px 4px;
}

/* 净值走势：单位净值 / 累计净值左右两张图，各占一半 */
.nav-charts {
  display: flex;
  gap: 14px;
}
.nav-chart-pane {
  flex: 1 1 50%;
  min-width: 0;            /* 防止 flex 子项把图表撑破导致 ECharts 不渲染 */
}
.nav-chart-title {
  margin: 0 0 4px;
  font-size: 13px;
  font-weight: 600;
  text-align: center;
}

.nav-caveat {
  margin: 0 0 10px;
  padding: 7px 11px;
  font-size: 12px;
  line-height: 1.65;
  color: #8a6d3b;
  background: #fdf8ec;
  border: 1px solid #f5e6c8;
  border-radius: 6px;
}
.nav-caveat b {
  font-weight: 600;
  color: #7a5c22;
}

.chart-loading {
  padding: 8px 4px;
}

/* ── 持仓区块 ───────────────────────────────────────────────────────────── */
.section {
  margin-bottom: 24px;
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 14px;
}

.section-title {
  margin: 0;
  font-size: 17px;
  font-weight: 600;
  color: #1a1a2e;
}

.sub-title {
  margin: 18px 0 10px;
  font-size: 14px;
  font-weight: 600;
  color: #555;
}

.holding-table {
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}

.sec-code {
  color: #555;
  font-variant-numeric: tabular-nums;
}

.sec-name {
  font-weight: 500;
  color: #1a1a2e;
}

/* 比例条 */
.ratio-cell {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 8px;
}

.ratio-text {
  font-variant-numeric: tabular-nums;
  font-weight: 600;
  color: #1a1a2e;
  white-space: nowrap;
}

.ratio-bar-track {
  width: 80px;
  height: 6px;
  background: #f0f2f5;
  border-radius: 3px;
  overflow: hidden;
  flex-shrink: 0;
}

.ratio-bar {
  height: 100%;
  border-radius: 3px;
  background: linear-gradient(90deg, #79bbff, #409eff);
}

.ratio-bar.bond {
  background: linear-gradient(90deg, #b3e19d, #67c23a);
}

.data-note {
  font-size: 12px;
  color: #bbb;
  margin-top: 20px;
}

.skeleton-wrap { padding: 16px 0; }
</style>
