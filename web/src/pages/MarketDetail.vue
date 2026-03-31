<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { LineChart, BarChart } from 'echarts/charts'
import {
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DataZoomComponent,
  MarkLineComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'
import { fetchMarketDetail, fetchMarketMinutes } from '@/api/markets'
import type { MarketDetail, MarketMinutes } from '@/types/market'

use([CanvasRenderer, LineChart, BarChart, TitleComponent, TooltipComponent, GridComponent, DataZoomComponent, MarkLineComponent])

const route  = useRoute()
const router = useRouter()

const key = computed(() => route.params.key as string)

// ── Daily chart state ──────────────────────────────────────────────────────
const loading = ref(false)
const error   = ref('')
const detail  = ref<MarketDetail | null>(null)
const days    = ref<number | 'ytd'>('ytd')

const daysOptions: { label: string; value: number | 'ytd' }[] = [
  { label: '今年来', value: 'ytd'  },
  { label: '30天',   value: 30    },
  { label: '90天',   value: 90    },
  { label: '180天',  value: 180   },
  { label: '1年',    value: 365   },
  { label: '2年',    value: 730   },
  { label: '5年',    value: 1825  },
  { label: '10年',   value: 3650  },
  { label: '20年',   value: 7300  },
]

// ── Intraday state ─────────────────────────────────────────────────────────
const isAShare       = computed(() => detail.value?.market === 'A股')
const showIntraday   = ref(true)   // default to intraday; switched off for non-A-share
const minuteLoading  = ref(false)
const minuteData     = ref<MarketMinutes | null>(null)

function ytdFrom() {
  return `${new Date().getFullYear()}-01-01`
}

async function loadDetail() {
  loading.value = true
  error.value   = ''
  try {
    const params = days.value === 'ytd'
      ? { from: ytdFrom() }
      : { days: days.value }
    detail.value = await fetchMarketDetail(key.value, params)
    // Non-A-share indices have no minute data — fall back to daily view
    if (detail.value?.market !== 'A股') showIntraday.value = false
  } catch {
    error.value = '加载失败，请稍后重试'
  } finally {
    loading.value = false
  }
}

async function loadMinutes() {
  minuteLoading.value = true
  try {
    minuteData.value = await fetchMarketMinutes(key.value)
  } catch {
    minuteData.value = null   // silently fail (non-A-share returns 400)
  } finally {
    minuteLoading.value = false
  }
}

function switchToIntraday() {
  showIntraday.value = true
  if (!minuteData.value) loadMinutes()
}

function switchToDaily(val: number | 'ytd') {
  showIntraday.value = false
  days.value = val
}

function isFlowKey(k: string) { return k.startsWith('flow_') }

onMounted(() => {
  loadDetail()
  if (!isFlowKey(key.value)) loadMinutes()
})
watch(key, () => {
  showIntraday.value = !isFlowKey(key.value)
  minuteData.value   = null
  loadDetail()
  if (!isFlowKey(key.value)) loadMinutes()
})
watch(days, loadDetail)

// ── Computed helpers ───────────────────────────────────────────────────────
const isFlow = computed(() => detail.value?.market === '资金流向')

function fmt(v: number | null, decimals = 2) {
  if (v == null) return '—'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: decimals })
}
function latestClose() {
  const h = detail.value?.history
  return h?.length ? h[0].close : null
}
function rangeChange() {
  const h = detail.value?.history
  if (!h || h.length < 2) return null
  const latest = h[0].close
  const prev   = h[h.length - 1].close
  if (latest == null || prev == null || prev === 0) return null
  return ((latest - prev) / prev) * 100
}

// ── Daily chart option ─────────────────────────────────────────────────────
const chartOption = computed(() => {
  if (!detail.value) return {}
  const sorted  = [...detail.value.history].reverse()
  const dates   = sorted.map(p => p.date)
  const closes  = sorted.map(p => p.close)
  const volumes = sorted.map(p => p.volume)
  const valid   = closes.filter((v): v is number => v !== null)
  const minVal  = valid.length ? Math.min(...valid) : 0
  const maxVal  = valid.length ? Math.max(...valid) : 0
  const padding = (maxVal - minVal) * 0.1
  const unitLabel = detail.value.unit ?? (isFlow.value ? '亿元' : '点')
  const lineColor = isFlow.value ? '#f56c6c' : '#409eff'
  const areaStart = isFlow.value ? 'rgba(245,108,108,0.2)' : 'rgba(64,158,255,0.2)'
  const areaEnd   = isFlow.value ? 'rgba(245,108,108,0)'   : 'rgba(64,158,255,0)'
  const hasVolume = !isFlow.value && volumes.some(v => v !== null)

  const fmtVol = (v: number) =>
    v >= 1e8 ? `${(v / 1e8).toFixed(2)} 亿手`
    : v >= 1e4 ? `${(v / 1e4).toFixed(2)} 万手`
    : `${v.toFixed(0)} 手`

  if (!hasVolume) {
    // 无成交量：单图布局（资金流向 / volume 全为 null）
    return {
      tooltip: {
        trigger: 'axis',
        formatter: (params: any[]) => {
          const p = params[0]
          if (!p) return ''
          const val = p.value != null ? p.value.toLocaleString('zh-CN', { maximumFractionDigits: 2 }) : '—'
          return `${p.axisValue}<br/><b>${val}</b> ${unitLabel}`
        },
      },
      grid: { top: 20, right: 24, bottom: 60, left: 80 },
      xAxis: {
        type: 'category', data: dates,
        axisLabel: { rotate: 30, fontSize: 11, color: '#888' },
        axisLine: { lineStyle: { color: '#ddd' } },
      },
      yAxis: {
        type: 'value',
        min: Math.max(0, minVal - padding), max: maxVal + padding,
        axisLabel: { fontSize: 11, color: '#888',
          formatter: (v: number) => v.toLocaleString('zh-CN', { maximumFractionDigits: 0 }) },
        splitLine: { lineStyle: { color: '#f0f0f0' } },
      },
      dataZoom: [
        { type: 'inside', start: 0, end: 100 },
        { type: 'slider', start: 0, end: 100, height: 24, bottom: 4 },
      ],
      series: [{
        name: detail.value.name, type: 'line', data: closes,
        smooth: false, symbol: 'none',
        lineStyle: { color: lineColor, width: 2 },
        areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [{ offset: 0, color: areaStart }, { offset: 1, color: areaEnd }] } },
      }],
    }
  }

  // 有成交量：上下双区块布局
  return {
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const price = params.find((p: any) => p.seriesIndex === 0)
        const vol   = params.find((p: any) => p.seriesIndex === 1)
        if (!price) return ''
        const priceStr = price.value != null
          ? price.value.toLocaleString('zh-CN', { maximumFractionDigits: 2 }) : '—'
        const lines = [`${price.axisValue}`, `<b>${priceStr}</b> ${unitLabel}`]
        if (vol?.value != null) lines.push(`成交量：${fmtVol(vol.value)}`)
        return lines.join('<br/>')
      },
    },
    grid: [
      { top: 20,  right: 24, bottom: 130, left: 80 },  // 价格区
      { top: 270, right: 24, bottom: 34,  left: 80 },  // 成交量区
    ],
    xAxis: [
      { gridIndex: 0, type: 'category', data: dates,
        axisLabel: { show: false }, axisLine: { lineStyle: { color: '#ddd' } } },
      { gridIndex: 1, type: 'category', data: dates,
        axisLabel: { rotate: 30, fontSize: 11, color: '#888' },
        axisLine: { lineStyle: { color: '#ddd' } } },
    ],
    yAxis: [
      { gridIndex: 0, type: 'value',
        min: Math.max(0, minVal - padding), max: maxVal + padding,
        axisLabel: { fontSize: 11, color: '#888',
          formatter: (v: number) => v.toLocaleString('zh-CN', { maximumFractionDigits: 0 }) },
        splitLine: { lineStyle: { color: '#f0f0f0' } } },
      { gridIndex: 1, type: 'value',
        axisLabel: { show: false }, splitLine: { show: false }, axisLine: { show: false } },
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 0, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], start: 0, end: 100, height: 24, bottom: 4 },
    ],
    series: [
      {
        name: detail.value.name, type: 'line',
        xAxisIndex: 0, yAxisIndex: 0,
        data: closes, smooth: false, symbol: 'none',
        lineStyle: { color: lineColor, width: 2 },
        areaStyle: { color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [{ offset: 0, color: areaStart }, { offset: 1, color: areaEnd }] } },
      },
      {
        name: '成交量', type: 'bar',
        xAxisIndex: 1, yAxisIndex: 1,
        data: volumes, barMaxWidth: 8,
        itemStyle: { color: 'rgba(150,150,150,0.5)' },
      },
    ],
  }
})

// ── Intraday chart option ──────────────────────────────────────────────────
const intradayOption = computed(() => {
  const data = minuteData.value
  if (!data?.minutes.length) return {}

  const times     = data.minutes.map(m => m.time.slice(11, 16))   // HH:MM
  const closes    = data.minutes.map(m => m.close)
  const valid     = closes.filter((v): v is number => v !== null)
  const minVal    = valid.length ? Math.min(...valid) : 0
  const maxVal    = valid.length ? Math.max(...valid) : 0
  const padding   = (maxVal - minVal) * 0.05 || minVal * 0.001
  const prevClose = data.prevClose

  // 最后一个有效收盘价（兼容旧浏览器，不用 findLast）
  let lastClose: number | null = null
  for (let i = closes.length - 1; i >= 0; i--) {
    if (closes[i] !== null) { lastClose = closes[i] as number; break }
  }
  const isUp    = prevClose == null || lastClose == null || lastClose >= prevClose
  const fillRgb = isUp ? '232,83,74' : '38,161,123'

  return {
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const p = params.find((p: any) => p.seriesIndex === 0) || params[0]
        if (!p) return ''
        const m         = data.minutes[p.dataIndex]
        const fmt2      = (v: number | null) =>
          v != null ? v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }) : '—'
        const close     = m?.close ?? null
        const vol       = m?.volume ?? null
        const changeAmt = close != null && prevClose != null ? close - prevClose : null
        const changePct = changeAmt != null && prevClose    ? changeAmt / prevClose * 100 : null
        const color     = changeAmt == null ? '#888' : changeAmt >= 0 ? '#e8534a' : '#26a17b'
        const sign      = changeAmt != null && changeAmt >= 0 ? '+' : ''
        const volStr    = vol == null ? '—'
          : vol >= 1e8 ? `${(vol / 1e8).toFixed(2)} 亿手`
          : vol >= 1e4 ? `${(vol / 1e4).toFixed(2)} 万手`
          : `${vol.toFixed(0)} 手`
        return [
          `<span style="color:#888">${data.date} ${p.axisValue}</span>`,
          `点数：<b>${fmt2(close)}</b>`,
          `<span style="color:${color}">涨跌额：${sign}${fmt2(changeAmt)}</span>`,
          `<span style="color:${color}">涨跌幅：${sign}${changePct != null ? changePct.toFixed(2) : '—'}%</span>`,
          `成交量：${volStr}`,
        ].join('<br/>')
      },
    },
    grid: [
      { top: 20,  right: 24, bottom: 130, left: 80 },  // 价格区
      { top: 270, right: 24, bottom: 34,  left: 80 },  // 成交量区
    ],
    xAxis: [
      { gridIndex: 0, type: 'category', data: times,
        axisLabel: { show: false }, axisLine: { lineStyle: { color: '#ddd' } } },
      { gridIndex: 1, type: 'category', data: times,
        axisLabel: { fontSize: 11, color: '#888', interval: 29 },
        axisLine: { lineStyle: { color: '#ddd' } } },
    ],
    yAxis: [
      { gridIndex: 0, type: 'value',
        min: minVal - padding, max: maxVal + padding,
        axisLabel: { fontSize: 11, color: '#888',
          formatter: (v: number) => v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }) },
        splitLine: { lineStyle: { color: '#f0f0f0' } } },
      { gridIndex: 1, type: 'value',
        axisLabel: { show: false }, splitLine: { show: false }, axisLine: { show: false } },
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 0, end: 100 },
      { type: 'slider', xAxisIndex: [0, 1], start: 0, end: 100, height: 24, bottom: 4 },
    ],
    series: [
      {
        name: detail.value?.name,
        type: 'line',
        xAxisIndex: 0, yAxisIndex: 0,
        data: closes,
        smooth: false,
        symbol: 'none',
        lineStyle: { color: `rgb(${fillRgb})`, width: 1.5 },
        areaStyle: {
          color: {
            type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: `rgba(${fillRgb},0.15)` },
              { offset: 1, color: `rgba(${fillRgb},0)` },
            ],
          },
        },
        ...(prevClose != null ? {
          markLine: {
            silent: true,
            symbol: 'none',
            lineStyle: { color: '#aaa', type: 'dashed', width: 1 },
            label: {
              formatter: `昨收 ${prevClose.toLocaleString('zh-CN', { maximumFractionDigits: 2 })}`,
              position: 'insideEndTop',
              fontSize: 11,
              color: '#888',
            },
            data: [{ yAxis: prevClose }],
          },
        } : {}),
      },
      {
        name: '成交量', type: 'bar',
        xAxisIndex: 1, yAxisIndex: 1,
        data: data.minutes.map(m => m.volume),
        barMaxWidth: 4,
        itemStyle: { color: `rgba(${fillRgb},0.4)` },
      },
    ],
  }
})
</script>

<template>
  <div class="detail-page">
    <el-button link @click="router.push('/markets')" class="back-btn">
      ← 返回全球股市
    </el-button>

    <template v-if="detail">
      <div class="header">
        <div class="title-block">
          <h1>{{ detail.name }}</h1>
          <div class="meta-tags">
            <el-tag size="small" type="info">{{ detail.key }}</el-tag>
            <el-tag v-if="detail.symbol" size="small">{{ detail.symbol }}</el-tag>
            <el-tag size="small" type="primary">{{ detail.market }}</el-tag>
            <el-tag v-if="detail.unit" size="small" type="warning">{{ detail.unit }}</el-tag>
          </div>
        </div>

        <div class="price-block">
          <div class="latest-price">
            {{ fmt(latestClose()) }}
            <span class="unit">{{ detail.unit ?? (isFlow ? '亿元' : '点') }}</span>
          </div>
          <div
            v-if="!showIntraday && rangeChange() != null"
            class="change"
            :class="rangeChange()! >= 0 ? 'up' : 'down'"
          >
            {{ rangeChange()! >= 0 ? '▲' : '▼' }}
            {{ Math.abs(rangeChange()!).toFixed(2) }}%
            <span class="change-label">（区间）</span>
          </div>
        </div>
      </div>

      <!-- 工具栏：分时按钮 + 时间范围 -->
      <div class="chart-toolbar">
        <!-- 分时按钮（仅 A股） -->
        <el-button
          v-if="isAShare"
          size="small"
          :type="showIntraday ? 'primary' : 'default'"
          class="intraday-btn"
          @click="switchToIntraday"
        >
          分时
        </el-button>

        <el-radio-group
          :model-value="showIntraday ? null : days"
          size="small"
          @update:model-value="val => switchToDaily(val as number | 'ytd')"
        >
          <el-radio-button v-for="opt in daysOptions" :key="String(opt.value)" :value="opt.value">
            {{ opt.label }}
          </el-radio-button>
        </el-radio-group>
      </div>

      <!-- 分时图 -->
      <div v-if="showIntraday" class="chart-wrap" v-loading="minuteLoading">
        <div v-if="minuteData?.date" class="intraday-date">
          {{ minuteData.date }} 分时走势（{{ minuteData.minutes.length }} 根 1分钟 K线）
        </div>
        <v-chart
          v-if="minuteData?.minutes.length"
          :option="intradayOption"
          autoresize
          style="width:100%;height:380px"
        />
        <div v-else-if="!minuteLoading" class="no-data">暂无分时数据，请先运行 fetch_market_minutes.py</div>
      </div>

      <!-- 日线走势图 -->
      <div v-else class="chart-wrap" v-loading="loading">
        <v-chart :option="chartOption" autoresize style="width:100%;height:380px" />
      </div>

      <el-descriptions :title="isFlow ? '资金流向信息' : '指数信息'" :column="3" border size="small" class="desc-card">
        <el-descriptions-item label="市场">{{ detail.market }}</el-descriptions-item>
        <el-descriptions-item label="代码">{{ detail.symbol ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="数据更新">
          {{ detail.updatedAt ? detail.updatedAt.slice(0, 10) : '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="历史记录条数">{{ detail.history.length }} 条</el-descriptions-item>
        <el-descriptions-item label="最早日期">
          {{ detail.history.length ? detail.history[detail.history.length - 1].date : '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="最新日期">
          {{ detail.history.length ? detail.history[0].date : '—' }}
        </el-descriptions-item>
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

/* ── Toolbar ────────────────────────────────────────────────────────────── */
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

/* ── Chart ──────────────────────────────────────────────────────────────── */
.chart-wrap {
  background: #fff;
  border: 1px solid #eee;
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 24px;
  min-height: 400px;
}

.intraday-date {
  font-size: 13px;
  color: #888;
  margin-bottom: 4px;
  padding-left: 4px;
}

.no-data {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 300px;
  color: #aaa;
  font-size: 14px;
}

.desc-card {
  margin-top: 8px;
}
</style>
