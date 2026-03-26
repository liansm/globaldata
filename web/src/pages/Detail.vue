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
import VChart from 'vue-echarts'
import { fetchCommodityDetail, fetchCommodityMinutes } from '@/api/commodities'
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

// Intraday tab
const activeTab    = ref<'history' | 'minutes'>('history')
const minutesData  = ref<CommodityMinutes | null>(null)
const minutesLoading = ref(false)
const minutesError   = ref('')

const daysOptions: { label: string; value: DaysOption }[] = [
  { label: '今年来', value: 'ytd' },
  { label: '30天',   value: 30   },
  { label: '90天',   value: 90   },
  { label: '180天',  value: 180  },
  { label: '1年',    value: 365  },
  { label: '2年',    value: 730  },
  { label: '5年',    value: 1825 },
  { label: '10年',   value: 3650 },
  { label: '20年',   value: 7300 },
]

function ytdFrom() {
  return `${new Date().getFullYear()}-01-01`
}

async function loadDetail() {
  loading.value = true
  error.value = ''
  try {
    const params = days.value === 'ytd'
      ? { from: ytdFrom() }
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
  minutesError.value = ''
  try {
    minutesData.value = await fetchCommodityMinutes(key.value)
    // 有数据时自动切到分时 tab
    if (minutesData.value?.minutes.length) {
      activeTab.value = 'minutes'
    }
  } catch {
    minutesError.value = '分时数据加载失败'
  } finally {
    minutesLoading.value = false
  }
}

function onTabChange(tab: string) {
  activeTab.value = tab as 'history' | 'minutes'
}

onMounted(() => { loadDetail(); loadMinutes() })
watch(key, () => { loadDetail(); loadMinutes() })
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
    grid: { top: 20, right: 24, bottom: 60, left: 70 },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: { rotate: 30, fontSize: 11, color: '#888' },
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
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, height: 24, bottom: 4 },
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

function latestPrice() {
  // Prefer spot price
  if (detail.value?.spot?.price != null) return detail.value.spot.price
  const h = detail.value?.history
  if (!h || !h.length) return null
  return h[0].price
}

function spotChangePct() {
  return detail.value?.spot?.changePct ?? null
}

function changePctClass(v: number | null) {
  if (v == null) return ''
  return v >= 0 ? 'up' : 'down'
}

function fmtSpotTime(dt: string | null) {
  return dt ? dt.slice(11, 16) : null
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

          <!-- 实时更新时间 badge -->
          <div v-if="detail.spot?.updatedAt" class="spot-time">
            <span class="spot-badge">实时</span>
            {{ fmtSpotTime(
                typeof detail.spot.updatedAt === 'string'
                  ? detail.spot.updatedAt
                  : null
               ) }}
          </div>
        </div>
      </div>

      <!-- Tab 切换：分时图（有数据时在前）/ 历史走势 -->
      <el-tabs v-model="activeTab" class="chart-tabs" @tab-change="onTabChange">
        <!-- 分时图：有数据才显示，且置于首位 -->
        <el-tab-pane
          v-if="minutesData && minutesData.minutes.length"
          label="分时图"
          name="minutes"
        >
          <div class="minutes-meta">
            {{ minutesData.date }} · {{ minutesData.minutes.length }} 根分钟 bars
            <template v-if="detail.spot?.prevClose">
              · 昨结算 {{ fmt(detail.spot.prevClose, 4) }} {{ detail.unit }}
            </template>
          </div>
          <div class="chart-wrap">
            <v-chart :option="minutesChartOption" autoresize style="width:100%;height:360px" />
          </div>
        </el-tab-pane>

        <el-tab-pane label="历史走势" name="history">
          <!-- 时间范围切换 -->
          <div class="chart-toolbar">
            <el-radio-group v-model="days" size="small">
              <el-radio-button
                v-for="opt in daysOptions"
                :key="opt.value"
                :value="opt.value"
              >
                {{ opt.label }}
              </el-radio-button>
            </el-radio-group>
          </div>

          <div class="chart-wrap" v-loading="loading">
            <v-chart :option="chartOption" autoresize style="width:100%;height:380px" />
          </div>
        </el-tab-pane>
      </el-tabs>

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
          <el-descriptions-item label="实时更新时间">
            {{ detail.spot.updatedAt
               ? String(detail.spot.updatedAt).slice(0, 16)
               : '—' }}
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

.chart-tabs {
  margin-bottom: 16px;
}

.chart-toolbar {
  margin-bottom: 12px;
}

.chart-wrap {
  background: #fff;
  border: 1px solid #eee;
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 24px;
  min-height: 360px;
}

.minutes-meta {
  font-size: 12px;
  color: #aaa;
  margin-bottom: 8px;
}

.desc-card {
  margin-top: 8px;
}
</style>
