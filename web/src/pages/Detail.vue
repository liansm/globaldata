<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DataZoomComponent,
  MarkLineComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { fetchCommodityDetail, fetchCommodityMinutes } from '@/api/commodities'
import { todayCn, ytdFrom, wtdFrom } from '@/lib/dateRange'
import AnnotatedLineChart from '@/components/AnnotatedLineChart.vue'
import type { CommodityDetail, CommodityMinutes, DaysOption } from '@/types/commodity'

use([
  CanvasRenderer,
  LineChart,
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DataZoomComponent,
  MarkLineComponent,
])

const route  = useRoute()
const router = useRouter()

const key       = computed(() => route.params.key as string)
const loading   = ref(false)
const error     = ref('')
const detail    = ref<CommodityDetail | null>(null)
const days      = ref<DaysOption>('ytd')

// Intraday
const showIntraday   = ref(false)
const minutesData    = ref<CommodityMinutes | null>(null)
const minutesLoading = ref(false)

const daysOptions: { label: string; value: DaysOption }[] = [
  { label: '今年来', value: 'ytd' },
  { label: '本周',   value: 'wtd' },
  { label: '30天',   value: 30   },
  { label: '90天',   value: 90   },
  { label: '180天',  value: 180  },
  { label: '1年',    value: 365  },
  { label: '2年',    value: 730  },
  { label: '5年',    value: 1825 },
  { label: '10年',   value: 3650 },
  { label: '20年',   value: 7300 },
]

async function loadDetail() {
  loading.value = true
  error.value = ''
  try {
    // 'wtd'/'ytd' 走 from（自然周期），数字走滚动天数
    const params = days.value === 'ytd' ? { from: ytdFrom() }
      : days.value === 'wtd' ? { from: wtdFrom() }
      : { days: days.value as number }
    detail.value = await fetchCommodityDetail(key.value, params)
  } catch {
    error.value = '加载失败，请稍后重试'
  } finally {
    loading.value = false
  }
}

async function loadMinutes() {
  minutesLoading.value = true
  try {
    minutesData.value = await fetchCommodityMinutes(key.value)
    if (minutesData.value?.minutes.length) {
      showIntraday.value = true
    }
  } catch {
    minutesData.value = null  // 静默失败
  } finally {
    minutesLoading.value = false
  }
}

function switchToIntraday() { showIntraday.value = true }

function switchToDaily(val: DaysOption) {
  showIntraday.value = false
  days.value = val
}

onMounted(() => { loadDetail(); loadMinutes() })
watch(key, () => {
  showIntraday.value = false
  minutesData.value  = null
  loadDetail()
  loadMinutes()
})
watch(days, loadDetail)

// ── Historical chart option ──────────────────────────────────────────────
const chartOption = computed(() => {
  if (!detail.value) return {}

  const sorted = [...detail.value.history].reverse()
  const dates  = sorted.map(p => p.date)
  const pricesArr = sorted.map(p => p.price)

  const validPrices = pricesArr.filter((p): p is number => p !== null)
  const minVal = validPrices.length ? Math.min(...validPrices) : 0
  const maxVal = validPrices.length ? Math.max(...validPrices) : 0
  const padding = (maxVal - minVal) * 0.1

  // 1 年区间有 ~258 个点，靠 ECharts 的 auto interval 会按「未旋转的字宽」估间隔，
  // 但 label 是 rotate:30 的，实际占位更宽 → 标签会挤成一片。按点数显式限流。
  const labelStep = Math.max(1, Math.ceil(dates.length / 10))

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: { axisValue: string; value: number | null }[]) => {
        const p = params[0]
        if (!p) return ''
        return `${p.axisValue}<br/><b>${p.value != null
          ? p.value.toLocaleString('zh-CN', { maximumFractionDigits: 4 })
          : '—'}</b> ${detail.value?.unit ?? ''}`
      },
    },
    // bottom 要给「rotate 30° 的日期标签 + 滑块」留够位置，否则标签会压到滑块上
    grid: { top: 20, right: 24, bottom: 78, left: 70 },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: {
        rotate: 30,
        fontSize: 11,
        color: '#888',
        interval: labelStep - 1,
        hideOverlap: true,
      },
      axisLine: { lineStyle: { color: '#ddd' } },
    },
    yAxis: {
      type: 'value',
      min: Math.max(0, minVal - padding),
      max: maxVal + padding,
      splitNumber: 5,
      axisLabel: {
        fontSize: 11,
        color: '#888',
        // 手动设了 min/max（给极值留白），而 max 往往不是刻度值，
        // ECharts 会在轴端补一个 label（白银 123.52 与 120 叠成两行）。关掉轴端标签。
        showMaxLabel: false,
        showMinLabel: false,
        formatter: (v: number) =>
          v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }),
      },
      splitLine: { lineStyle: { color: '#f0f0f0' } },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      {
        type: 'slider', start: 0, end: 100, height: 16, bottom: 8,
        // 258 个点的 dataShadow 会挤成一条脏色带，且和 x 轴标签叠在一起
        showDataShadow: false,
        borderColor: 'transparent',
        backgroundColor: '#f5f7fb',
        fillerColor: 'rgba(64,158,255,.12)',
        handleStyle: { color: '#409eff' },
        textStyle: { fontSize: 10, color: '#a8b0be' },
      },
    ],
    series: [{
      name: detail.value.commodity,
      type: 'line',
      data: pricesArr,
      smooth: false,
      symbol: 'none',
      lineStyle: { color: '#409eff', width: 2 },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: 'rgba(64,158,255,0.2)' },
            { offset: 1, color: 'rgba(64,158,255,0)' },
          ],
        },
      },
    }],
  }
})

// ── Intraday (minutes) chart option ─────────────────────────────────────
const minutesChartOption = computed(() => {
  const md = minutesData.value
  if (!md || !md.minutes.length) return {}

  const times  = md.minutes.map(m => m.time.slice(11, 16))  // "HH:MM"
  const closes = md.minutes.map(m => m.close)

  const validCloses = closes.filter((c): c is number => c !== null)
  const minVal = validCloses.length ? Math.min(...validCloses) : 0
  const maxVal = validCloses.length ? Math.max(...validCloses) : 0
  const padding = (maxVal - minVal) * 0.05

  // Baseline: yesterday's close from spot, or first bar's open
  const baseline = detail.value?.spot?.prevClose ?? md.minutes[0]?.open ?? null

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: { axisValue: string; value: number | null }[]) => {
        const p = params[0]
        if (!p) return ''
        return `${p.axisValue}<br/><b>${p.value != null
          ? p.value.toLocaleString('zh-CN', { maximumFractionDigits: 4 })
          : '—'}</b> ${md.unit ?? ''}`
      },
    },
    grid: { top: 20, right: 24, bottom: 40, left: 80 },
    xAxis: {
      type: 'category',
      data: times,
      axisLabel: {
        interval: Math.floor(times.length / 8),
        fontSize: 11,
        color: '#888',
      },
      axisLine: { lineStyle: { color: '#ddd' } },
    },
    yAxis: {
      type: 'value',
      min: Math.max(0, minVal - padding),
      max: maxVal + padding,
      axisLabel: {
        fontSize: 11,
        color: '#888',
        formatter: (v: number) =>
          v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }),
      },
      splitLine: { lineStyle: { color: '#f0f0f0' } },
    },
    series: [
      {
        name: '最新价',
        type: 'line',
        data: closes,
        smooth: false,
        symbol: 'none',
        lineStyle: { color: '#409eff', width: 1.5 },
        areaStyle: {
          color: {
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(64,158,255,0.15)' },
              { offset: 1, color: 'rgba(64,158,255,0)' },
            ],
          },
        },
        ...(baseline != null ? {
          markLine: {
            silent: true,
            symbol: 'none',
            lineStyle: { color: '#aaa', type: 'dashed', width: 1 },
            data: [{ yAxis: baseline, name: '昨结算' }],
            label: {
              formatter: `昨结算 ${baseline.toLocaleString('zh-CN', { maximumFractionDigits: 2 })}`,
              fontSize: 10,
              color: '#aaa',
            },
          },
        } : {}),
      },
    ],
  }
})

// ── Helpers ─────────────────────────────────────────────────────────────
function fmt(v: number | null, decimals = 2) {
  if (v == null) return '—'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: decimals })
}

/**
 * 这条 spot 快照能不能当「最新价」用。
 *
 * 与后端 /api/commodities 同口径（那边是 spotSnapshotKind 判定）：
 *   · kind='close'（收市静态价）：spotDate 不早于日线最新日期 → 就是最近交易日的收盘
 *   · kind='intraday'（盘中/夜盘）：spotDate 必须是今天，否则是过期的盘中残留
 *
 * 早期这里只判 `spot.price != null`，2026-10-31（周六）就会把周五夜盘的价
 * 配上「实时」徽标显示出来 —— 期货当时根本没开市。
 */
function spotUsable() {
  const s = detail.value?.spot
  if (!s || s.price == null || !s.spotDate) return false
  if (s.snapshotKind === 'close') {
    const latest = detail.value?.history?.[0]?.date ?? null
    return latest == null || s.spotDate >= latest
  }
  return s.spotDate === todayCn()
}

function latestPrice() {
  // Prefer spot price（仅当快照确实属于「最近」，否则回落日线）
  if (spotUsable() && detail.value?.spot?.price != null) return detail.value.spot.price
  const h = detail.value?.history
  if (!h || !h.length) return null
  return h[0].price
}

function spotChangePct() {
  return spotUsable() ? (detail.value?.spot?.changePct ?? null) : null
}

function changePctClass(v: number | null) {
  if (v == null) return ''
  return v >= 0 ? 'up' : 'down'
}

/**
 * 快照时间 "HH:MM"（Asia/Shanghai）。
 * ⚠ 详情接口返回的 updatedAt 是 ISO 串（UTC），直接 slice(11,16) 拿到的是 UTC 小时
 *   —— 实测量出来是 05:20 而实际是 13:20，差 8 小时。必须按上海时区格式化。
 */
function fmtSpotTime(dt: string | null) {
  if (!dt) return null
  const d = new Date(dt)
  if (Number.isNaN(d.getTime())) return String(dt).slice(11, 16)
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(d)
}

/** 完整快照时间 "YYYY-MM-DD HH:MM"（Asia/Shanghai），同上，不能用 slice */
function fmtSpotDateTime(dt: string | Date | null | undefined) {
  if (!dt) return '—'
  const d = new Date(dt)
  if (Number.isNaN(d.getTime())) return String(dt).slice(0, 16)
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(d).replace(',', '')
}

// Extract exchange label from commodity name parentheses, or infer from key
function exchangeLabel(): string | null {
  const d = detail.value
  if (!d) return null
  const match = d.commodity.match(/[（(]([^)）]+)[)）]/)
  if (match) return match[1].trim().split(/\s+/)[0]
  if (d.key === 'gold' || d.key === 'silver') return 'SGE'
  return null
}

function exchangeTagType(label: string | null) {
  if (!label) return 'info'
  if (label === 'SGE') return 'warning'
  if (label === 'COMEX' || label === 'LME' || label === 'NYMEX' || label === 'ICE') return 'danger'
  return 'info'
}
</script>

<template>
  <div class="detail-page">
    <!-- 顶部导航 -->
    <el-button link @click="router.push('/')" class="back-btn">
      ← 返回列表
    </el-button>

    <template v-if="detail">
      <!-- 标题 + 元信息 -->
      <div class="header">
        <div class="title-block">
          <h1>{{ detail.commodity }}</h1>
          <div class="meta-tags">
            <el-tag size="small" type="info">{{ detail.key }}</el-tag>
            <el-tag v-if="detail.symbol" size="small">{{ detail.symbol }}</el-tag>
            <el-tag
              v-if="exchangeLabel()"
              size="small"
              :type="exchangeTagType(exchangeLabel())"
            >{{ exchangeLabel() }}</el-tag>
            <el-tag
              v-if="detail.priceType"
              size="small"
              :type="detail.priceType === 'futures' ? 'warning' : 'success'"
            >
              {{ detail.priceType === 'futures' ? '期货' : '现货' }}
            </el-tag>
            <el-tag v-if="detail.unit" size="small" type="primary">{{ detail.unit }}</el-tag>
          </div>
        </div>

        <!-- 最新价格 + 涨跌幅 -->
        <div class="price-block">
          <div class="latest-price">
            {{ fmt(latestPrice(), 4) }}
            <span class="unit">{{ detail.unit }}</span>
          </div>

          <!-- 实时涨跌幅（来自 spot） -->
          <div
            v-if="spotChangePct() != null"
            class="change"
            :class="changePctClass(spotChangePct())"
          >
            {{ spotChangePct()! >= 0 ? '▲' : '▼' }}
            {{ Math.abs(spotChangePct()!).toFixed(2) }}%
            <span class="change-label">（今日）</span>
          </div>

          <!-- 快照标记：盘中 →「HH:MM · 实时」；收市静态价 →「收盘」 -->
          <div v-if="spotUsable()" class="spot-time">
            <template v-if="detail.spot?.snapshotKind === 'close'">
              <span class="spot-badge is-close">收盘</span>
            </template>
            <template v-else>
              <span class="spot-badge">实时</span>
              {{ fmtSpotTime(
                  detail.spot?.updatedAt && typeof detail.spot.updatedAt === 'string'
                    ? detail.spot.updatedAt
                    : null
                 ) }}
            </template>
          </div>
        </div>
      </div>

      <!-- 工具栏：分时按钮（有数据才显示）+ 时间范围 -->
      <div class="chart-toolbar">
        <el-button
          v-if="minutesData && minutesData.minutes.length"
          size="small"
          :type="showIntraday ? 'primary' : 'default'"
          class="intraday-btn"
          @click="switchToIntraday"
        >
          分时
        </el-button>

        <el-radio-group
          :model-value="(showIntraday || minutesLoading) ? undefined : days"
          size="small"
          @update:model-value="val => switchToDaily(val as DaysOption)"
        >
          <el-radio-button v-for="opt in daysOptions" :key="String(opt.value)" :value="opt.value">
            {{ opt.label }}
          </el-radio-button>
        </el-radio-group>
      </div>

      <!-- 分时图 -->
      <div v-if="showIntraday" class="chart-wrap" v-loading="minutesLoading">
        <div v-if="minutesData?.date" class="intraday-date">
          {{ minutesData.date }} · {{ minutesData.minutes.length }} 根分钟 bars
          <template v-if="detail.spot?.prevClose">
            · 昨结算 {{ fmt(detail.spot.prevClose, 4) }} {{ detail.unit }}
          </template>
        </div>
        <AnnotatedLineChart
          v-if="minutesData?.minutes.length"
          :option="minutesChartOption"
          autoresize
          style="width:100%;height:380px"
        />
      </div>

      <!-- 历史走势图 -->
      <!-- ⚠ 必须给空态：选「本周」遇国庆/春节整周休市时 history 为 0 行，
           不提示的话只是一张白底空轴图，看着像页面坏了（区间本身没错）。 -->
      <div v-else class="chart-wrap" v-loading="loading">
        <AnnotatedLineChart
          v-if="detail.history.length"
          :option="chartOption" autoresize style="width:100%;height:380px"
        />
        <div v-else-if="!loading" class="chart-empty">
          该区间没有数据 —— 多为整周休市（国庆 / 春节）或该品种尚未入库。<br />
          换「今年来」或更长的区间看看。
        </div>
      </div>

      <!-- 详细信息卡片 -->
      <el-descriptions title="商品信息" :column="3" border size="small" class="desc-card">
        <el-descriptions-item label="热值(kcal)">
          {{ detail.kcal != null ? fmt(detail.kcal, 0) : '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="品级">
          {{ detail.gradeType ?? '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="数据更新">
          {{ detail.updatedAt ? detail.updatedAt.slice(0, 10) : '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="历史记录条数">
          {{ detail.history.length }} 条
        </el-descriptions-item>
        <el-descriptions-item label="最早日期">
          {{ detail.history.length ? detail.history[detail.history.length - 1].date : '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="最新日期">
          {{ detail.history.length ? detail.history[0].date : '—' }}
        </el-descriptions-item>
        <!-- 实时快照信息 -->
        <template v-if="detail.spot">
          <el-descriptions-item label="昨结算">
            {{ fmt(detail.spot.prevClose, 4) }} {{ detail.unit }}
          </el-descriptions-item>
          <el-descriptions-item label="今日涨跌">
            {{ fmt(detail.spot.changeAmt, 4) }}
          </el-descriptions-item>
          <el-descriptions-item
            :label="detail.spot.snapshotKind === 'close' ? '快照抓取时间' : '实时更新时间'"
          >
            {{ fmtSpotDateTime(detail.spot.updatedAt) }}
          </el-descriptions-item>
        </template>
      </el-descriptions>
    </template>

    <div v-else-if="loading" v-loading="true" style="height:300px" />
    <el-alert v-else-if="error" :title="error" type="error" show-icon :closable="false" />
  </div>
</template>

<style scoped>
.detail-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 24px 20px 48px;
}

.back-btn {
  margin-bottom: 20px;
  font-size: 14px;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 24px;
  margin-bottom: 20px;
  flex-wrap: wrap;
}

h1 {
  margin: 0 0 10px;
  font-size: 24px;
  font-weight: 700;
  color: #1a1a2e;
}

.meta-tags {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.price-block {
  text-align: right;
  flex-shrink: 0;
}

.latest-price {
  font-size: 32px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  color: #1a1a2e;
}

.unit {
  font-size: 14px;
  font-weight: 400;
  color: #888;
  margin-left: 4px;
}

.change {
  font-size: 15px;
  font-weight: 600;
  margin-top: 4px;
}
.up   { color: #f56c6c; }
.down { color: #67c23a; }

.change-label {
  font-size: 12px;
  font-weight: 400;
  color: #999;
}

.spot-time {
  margin-top: 4px;
  font-size: 12px;
  color: #bbb;
}

.spot-badge {
  display: inline-block;
  padding: 0 5px;
  font-size: 10px;
  line-height: 16px;
  border-radius: 4px;
  background: #e8f4ff;
  color: #409eff;
  font-weight: 600;
  margin-right: 4px;
  vertical-align: middle;
}

/* 收市静态价：用中性灰和「实时」的蓝色区分开 */
.spot-badge.is-close {
  background: #f4f4f5;
  color: #909399;
}

.chart-toolbar {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}

.intraday-btn {
  flex-shrink: 0;
}

.intraday-date {
  font-size: 13px;
  color: #888;
  margin-bottom: 4px;
  padding-left: 4px;
}

.chart-wrap {
  background: #fff;
  border: 1px solid #eee;
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 24px;
  min-height: 360px;
}

/* 区间内无数据（整周休市等）：居中一段说明，而不是留一张白底空轴图 */
.chart-empty {
  height: 356px;
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  line-height: 1.9;
  color: #999;
  font-size: 13px;
}

.desc-card {
  margin-top: 8px;
}
</style>
