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
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'
import { fetchMarketDetail } from '@/api/markets'
import type { MarketDetail } from '@/types/market'

use([
  CanvasRenderer,
  LineChart,
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DataZoomComponent,
])

const route = useRoute()
const router = useRouter()

const key = computed(() => route.params.key as string)
const loading = ref(false)
const error = ref('')
const detail = ref<MarketDetail | null>(null)
const days = ref(365)

const daysOptions = [
  { label: '30天',  value: 30 },
  { label: '90天',  value: 90 },
  { label: '180天', value: 180 },
  { label: '1年',   value: 365 },
  { label: '2年',   value: 730 },
  { label: '5年',   value: 1825 },
]

async function loadDetail() {
  loading.value = true
  error.value = ''
  try {
    detail.value = await fetchMarketDetail(key.value, { days: days.value })
  } catch {
    error.value = '加载失败，请稍后重试'
  } finally {
    loading.value = false
  }
}

onMounted(loadDetail)
watch([key, days], loadDetail)

const isFlow = computed(() => detail.value?.market === '资金流向')

const chartOption = computed(() => {
  if (!detail.value) return {}

  const sorted = [...detail.value.history].reverse()
  const dates = sorted.map(p => p.date)
  const closes = sorted.map(p => p.close)

  const valid = closes.filter((v): v is number => v !== null)
  const minVal = valid.length ? Math.min(...valid) : 0
  const maxVal = valid.length ? Math.max(...valid) : 0
  const padding = (maxVal - minVal) * 0.1

  const unitLabel = detail.value.unit ?? (isFlow.value ? '亿元' : '点')
  const lineColor = isFlow.value ? '#f56c6c' : '#409eff'

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: { axisValue: string; value: number | null }[]) => {
        const p = params[0]
        if (!p) return ''
        const val = p.value != null
          ? p.value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
          : '—'
        return `${p.axisValue}<br/><b>${val}</b> ${unitLabel}`
      },
    },
    grid: { top: 20, right: 24, bottom: 60, left: 80 },
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
        formatter: (v: number) => v.toLocaleString('zh-CN', { maximumFractionDigits: 0 }),
      },
      splitLine: { lineStyle: { color: '#f0f0f0' } },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, height: 24, bottom: 4 },
    ],
    series: [
      {
        name: detail.value.name,
        type: 'line',
        data: closes,
        smooth: false,
        symbol: 'none',
        lineStyle: { color: lineColor, width: 2 },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: isFlow.value ? 'rgba(245,108,108,0.2)' : 'rgba(64,158,255,0.2)' },
              { offset: 1, color: isFlow.value ? 'rgba(245,108,108,0)' : 'rgba(64,158,255,0)' },
            ],
          },
        },
      },
    ],
  }
})

function fmt(v: number | null, decimals = 2) {
  if (v == null) return '—'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: decimals })
}

function latestClose() {
  const h = detail.value?.history
  if (!h || !h.length) return null
  return h[0].close
}

function rangeChange() {
  const h = detail.value?.history
  if (!h || h.length < 2) return null
  const latest = h[0].close
  const prev = h[h.length - 1].close
  if (latest == null || prev == null || prev === 0) return null
  return ((latest - prev) / prev) * 100
}
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
            {{ fmt(latestClose(), isFlow ? 2 : 2) }}
            <span class="unit">{{ detail.unit ?? (isFlow ? '亿元' : '点') }}</span>
          </div>
          <div
            v-if="rangeChange() != null"
            class="change"
            :class="rangeChange()! >= 0 ? 'up' : 'down'"
          >
            {{ rangeChange()! >= 0 ? '▲' : '▼' }}
            {{ Math.abs(rangeChange()!).toFixed(2) }}%
            <span class="change-label">（区间）</span>
          </div>
        </div>
      </div>

      <div class="chart-toolbar">
        <el-radio-group v-model="days" size="small">
          <el-radio-button v-for="opt in daysOptions" :key="opt.value" :value="opt.value">
            {{ opt.label }}
          </el-radio-button>
        </el-radio-group>
      </div>

      <div class="chart-wrap" v-loading="loading">
        <v-chart :option="chartOption" autoresize style="width:100%;height:380px" />
      </div>

      <el-descriptions title="指数信息" :column="3" border size="small" class="desc-card">
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

.chart-toolbar {
  margin-bottom: 12px;
}

.chart-wrap {
  background: #fff;
  border: 1px solid #eee;
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 24px;
  min-height: 400px;
}

.desc-card {
  margin-top: 8px;
}
</style>
