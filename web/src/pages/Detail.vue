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
import { fetchCommodityDetail } from '@/api/commodities'
import type { CommodityDetail, DaysOption } from '@/types/commodity'

use([
  CanvasRenderer,
  LineChart,
  TitleComponent,
  TooltipComponent,
  GridComponent,
  DataZoomComponent,
  MarkLineComponent,
])

const route = useRoute()
const router = useRouter()

const key = computed(() => route.params.key as string)
const loading = ref(false)
const error = ref('')
const detail = ref<CommodityDetail | null>(null)
const days = ref<DaysOption>('ytd')

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
  } catch (e) {
    error.value = '加载失败，请稍后重试'
  } finally {
    loading.value = false
  }
}

onMounted(loadDetail)
watch([key, days], loadDetail)

// ECharts 配置
const chartOption = computed(() => {
  if (!detail.value) return {}

  // history 是按日期降序排列的，图表需要升序
  const sorted = [...detail.value.history].reverse()
  const dates = sorted.map(p => p.date)
  const prices = sorted.map(p => p.price)

  const validPrices = prices.filter((p): p is number => p !== null)
  const minVal = validPrices.length ? Math.min(...validPrices) : 0
  const maxVal = validPrices.length ? Math.max(...validPrices) : 0
  const padding = (maxVal - minVal) * 0.1

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: { axisValue: string; value: number | null }[]) => {
        const p = params[0]
        if (!p) return ''
        return `${p.axisValue}<br/><b>${p.value != null ? p.value.toLocaleString('zh-CN', { maximumFractionDigits: 4 }) : '—'}</b> ${detail.value?.unit ?? ''}`
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
        formatter: (v: number) => v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }),
      },
      splitLine: { lineStyle: { color: '#f0f0f0' } },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, height: 24, bottom: 4 },
    ],
    series: [
      {
        name: detail.value.commodity,
        type: 'line',
        data: prices,
        smooth: false,
        symbol: 'none',
        lineStyle: { color: '#409eff', width: 2 },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(64,158,255,0.2)' },
              { offset: 1, color: 'rgba(64,158,255,0)' },
            ],
          },
        },
      },
    ],
  }
})

// 格式化辅助
function fmt(v: number | null, decimals = 2) {
  if (v == null) return '—'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: decimals })
}

function latestPrice() {
  const h = detail.value?.history
  if (!h || !h.length) return null
  return h[0].price   // history 降序，第一条是最新
}

function priceChange() {
  const h = detail.value?.history
  if (!h || h.length < 2) return null
  const latest = h[0].price
  const prev = h[h.length - 1].price
  if (latest == null || prev == null || prev === 0) return null
  return ((latest - prev) / prev) * 100
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
          <div
            v-if="priceChange() != null"
            class="change"
            :class="priceChange()! >= 0 ? 'up' : 'down'"
          >
            {{ priceChange()! >= 0 ? '▲' : '▼' }}
            {{ Math.abs(priceChange()!).toFixed(2) }}%
            <span class="change-label">（区间）</span>
          </div>
        </div>
      </div>

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

      <!-- ECharts 价格走势图 -->
      <div class="chart-wrap" v-loading="loading">
        <v-chart :option="chartOption" autoresize style="width:100%;height:380px" />
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
      </el-descriptions>
    </template>

    <!-- 加载中占位 -->
    <div v-else-if="loading" v-loading="true" style="height:300px" />

    <!-- 错误 -->
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
