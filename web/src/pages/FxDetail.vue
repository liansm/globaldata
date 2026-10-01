<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import {
  TitleComponent, TooltipComponent, GridComponent,
  DataZoomComponent, MarkLineComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { fetchFxDetail } from '@/api/fx'
import AnnotatedLineChart from '@/components/AnnotatedLineChart.vue'
import type { FxDetail, FxRateType } from '@/types/fx'

use([CanvasRenderer, LineChart, TitleComponent, TooltipComponent,
     GridComponent, DataZoomComponent, MarkLineComponent])

const route  = useRoute()
const router = useRouter()

const key = computed(() => route.params.key as string)

const loading = ref(false)
const error   = ref('')
const detail  = ref<FxDetail | null>(null)

type RangeKey = 'ytd' | 'all' | number
const range    = ref<RangeKey>('ytd')
const rateType = ref<FxRateType>('mid')

const RANGE_OPTIONS: { label: string; value: RangeKey }[] = [
  { label: '今年来', value: 'ytd' },
  { label: '1月',   value: 30    },
  { label: '3月',   value: 90    },
  { label: '1年',   value: 365   },
  { label: '3年',   value: 1095  },
  { label: '5年',   value: 1825  },
  { label: '10年',  value: 3650  },
  { label: '全部',  value: 'all' },
]

// ── 口径术语（唯一出处）────────────────────────────────────────────────────
// 后端已把数值换算成展示口径（1 单位外币 = X 人民币，日元 ×100），前端只负责格式化。
const TYPE_LABEL: Record<FxRateType, string> = { mid: '中间价', spot: '即期' }
const TYPE_SOURCE: Record<FxRateType, string> = { mid: '中国外汇交易中心（CFETS）', spot: '新浪财经' }
const TYPE_NOTE: Record<FxRateType, string> = {
  mid:  '每个工作日 9:15 发布，官方定价；周末与法定节假日无数据。',
  spot: '市场汇率日线（含开高低收）；历史深度各货币对不一，多数 2023-07 起、USDCNY 1994 起。',
}

function fmtRate(v: number | null): string {
  if (v == null) return '—'
  return v.toFixed(Math.abs(v) >= 1 ? 4 : 6)
}
function fmtPct(v: number | null): string {
  if (v == null) return '—'
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`
}
function signClass(v: number | null): string {
  if (v == null || v === 0) return 'flat'
  return v > 0 ? 'up' : 'down'
}

/** 该对实际可用的口径（有些货币对只有中间价，没有即期） */
const availableTypes = computed<FxRateType[]>(() => {
  const t = detail.value?.availableTypes ?? {}
  return (['mid', 'spot'] as FxRateType[]).filter(k => (t[k]?.points ?? 0) > 0)
})

function ytdFrom() {
  return `${new Date().getFullYear()}-01-01`
}

async function load() {
  loading.value = true
  error.value = ''
  try {
    // 「全部」需要该口径的历史起点，只有先拿到 availableTypes 才知道 → 首屏先常规查询
    const cfg = detail.value
    let params: any
    if (range.value === 'ytd') {
      params = { rateType: rateType.value, from: ytdFrom() }
    } else if (range.value === 'all') {
      const earliest = cfg?.availableTypes?.[rateType.value]?.earliest
      params = earliest
        ? { rateType: rateType.value, from: earliest }
        : { rateType: rateType.value, days: 3650 }
    } else {
      params = { rateType: rateType.value, days: range.value }
    }

    const d = await fetchFxDetail(key.value, params)
    detail.value = d

    // 目标口径没有数据（如韩元没有即期）→ 回退到有数据的口径，别渲染成空页
    if ((d.availableTypes?.[rateType.value]?.points ?? 0) === 0) {
      const fallback = (['mid', 'spot'] as FxRateType[])
        .find(t => (d.availableTypes?.[t]?.points ?? 0) > 0)
      if (fallback && fallback !== rateType.value) {
        rateType.value = fallback
        return   // watch(rateType) 会重新 load
      }
    }
  } catch (e: any) {
    error.value = e?.response?.status === 404 ? `未找到货币对「${key.value}」` : '加载失败，请稍后重试'
    detail.value = null
  } finally {
    loading.value = false
  }
}

onMounted(load)
watch(key, () => { range.value = 'ytd'; rateType.value = 'mid'; load() })
watch(rateType, load)
watch(range, load)

// ── 最新值与涨跌（前端从 history 推算，与列表页同口径：最新 vs 前一条）──────
const latest = computed(() => detail.value?.history?.[0] ?? null)
const prev   = computed(() => detail.value?.history?.[1] ?? null)

const dayChange = computed(() => {
  const c = latest.value?.close, p = prev.value?.close
  if (c == null || p == null || p === 0) return { amt: null as number | null, pct: null as number | null }
  return { amt: c - p, pct: (c - p) / p * 100 }
})

const unitLabel = computed(() => {
  const d = detail.value
  if (!d) return ''
  return d.quoteUnit === 100 ? `100 ${d.baseCode}` : `1 ${d.baseCode}`
})

/** 当前区间的最高/最低收盘（图表标注同源，避免两处数字不一致） */
const closes = computed(() =>
  (detail.value?.history ?? [])
    .map(p => p.close)
    .filter((v): v is number => v != null),
)
const maxClose = computed(() => (closes.value.length ? Math.max(...closes.value) : null))
const minClose = computed(() => (closes.value.length ? Math.min(...closes.value) : null))

// 图表 y 轴 / 标注的小数位（跟随量级）
const digits = computed(() => {
  const v = latest.value?.close
  if (v == null) return 4
  return Math.abs(v) >= 1 ? 4 : 6
})

// ── 图表 ──────────────────────────────────────────────────────────────────
// 中间价源侧只有一个值 → open/high/low 恒为 null，所以这里只画 close 折线，
// 不画 K 线、不用 close 充数（见 schema.ts 的 fx_rates 注释）。
const chartOption = computed(() => {
  const d = detail.value
  if (!d || !d.history.length) return {}

  const sorted     = [...d.history].reverse()          // 接口返倒序 → 图表要正序
  const dates      = sorted.map(p => p.date)
  const closeSeries = sorted.map(p => p.close)
  const valid      = closeSeries.filter((v): v is number => v != null)
  if (!valid.length) return {}

  const minVal = Math.min(...valid)
  const maxVal = Math.max(...valid)
  const pad    = (maxVal - minVal) * 0.08 || Math.abs(maxVal) * 0.002 || 0.0001

  const rangePct = d.stats.rangeChangePct
  const isUp     = rangePct == null || rangePct >= 0
  const rgb      = isUp ? '232,83,74' : '38,161,123'   // 区间内人民币贬值→红 / 升值→绿

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const p = params[0]
        if (!p) return ''
        const v = p.value
        return [
          `<span style="color:#888">${p.axisValue}</span>`,
          `${unitLabel.value} = <b>${v == null ? '—' : v.toFixed(digits.value)}</b> CNY`,
        ].join('<br/>')
      },
    },
    grid: { top: 34, right: 34, bottom: 74, left: 84 },
    xAxis: {
      type: 'category',
      data: dates,
      boundaryGap: false,
      axisLabel: { rotate: 30, fontSize: 11, color: '#888' },
      axisLine: { lineStyle: { color: '#ddd' } },
    },
    yAxis: {
      type: 'value',
      scale: true,
      min: minVal - pad,
      max: maxVal + pad,
      axisLabel: {
        fontSize: 11,
        color: '#888',
        formatter: (v: number) => v.toFixed(digits.value),
      },
      splitLine: { lineStyle: { color: '#f0f0f0' } },
    },
    dataZoom: [
      { type: 'inside', start: 0, end: 100 },
      { type: 'slider', start: 0, end: 100, height: 22, bottom: 6 },
    ],
    series: [{
      name: TYPE_LABEL[d.rateType],
      type: 'line',
      data: closeSeries,
      smooth: false,
      symbol: 'none',
      connectNulls: false,
      lineStyle: { color: `rgb(${rgb})`, width: 2 },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: `rgba(${rgb},0.18)` },
            { offset: 1, color: `rgba(${rgb},0)` },
          ],
        },
      },
    }],
  }
})
</script>

<template>
  <div class="fx-detail">
    <el-button link @click="router.push('/fx')" class="back-btn">← 返回外汇列表</el-button>

    <template v-if="detail">
      <div class="header">
        <div class="title-block">
          <h1>{{ detail.baseName }} <span class="pair">{{ unitLabel }} / CNY</span></h1>
          <div class="meta-tags">
            <el-tag size="small" type="info">{{ detail.baseCode }}</el-tag>
            <el-tag v-if="detail.officialCode" size="small">
              中间价代码 {{ detail.officialCode }}
            </el-tag>
            <el-tag v-if="detail.spotCode" size="small" type="success">
              即期代码 {{ detail.spotCode }}
            </el-tag>
            <el-tag size="small" type="primary">{{ detail.category }}</el-tag>
          </div>
        </div>

        <div class="price-block">
          <div class="latest-price">
            {{ fmtRate(latest?.close ?? null) }}
            <span class="unit">CNY</span>
          </div>
          <div class="change" :class="signClass(dayChange.pct)">
            {{ dayChange.amt != null && dayChange.amt > 0 ? '▲' : dayChange.amt != null && dayChange.amt < 0 ? '▼' : '—' }}
            {{ dayChange.amt != null ? Math.abs(dayChange.amt).toFixed(digits) : '—' }}
            （{{ fmtPct(dayChange.pct) }}）
            <span class="change-label">较前一日</span>
          </div>
          <div class="price-date">数据日期 {{ latest?.date ?? '—' }}</div>
        </div>
      </div>

      <!-- 工具栏：口径 + 区间 -->
      <div class="chart-toolbar">
        <el-radio-group v-model="rateType" size="small" v-if="availableTypes.length > 1">
          <el-radio-button v-for="t in availableTypes" :key="t" :value="t">
            {{ TYPE_LABEL[t] }}
          </el-radio-button>
        </el-radio-group>
        <el-tag v-else size="small" type="warning">
          该货币对仅有{{ TYPE_LABEL[availableTypes[0] ?? 'mid'] }}数据
        </el-tag>

        <el-radio-group v-model="range" size="small" class="range-group">
          <el-radio-button v-for="opt in RANGE_OPTIONS" :key="String(opt.value)" :value="opt.value">
            {{ opt.label }}
          </el-radio-button>
        </el-radio-group>
      </div>

      <p class="type-note">{{ TYPE_NOTE[detail.rateType] }}</p>

      <!-- 图表 -->
      <div class="chart-wrap" v-loading="loading">
        <AnnotatedLineChart
          v-if="detail.history.length"
          :option="chartOption"
          :decimals="digits"
          autoresize
          style="width:100%;height:400px"
        />
        <div v-else class="no-data">
          该区间无数据，请先运行 <code>python fetch_fx.py</code> 补齐
        </div>
      </div>

      <!-- 区间统计 -->
      <div class="stat-row">
        <div class="stat-card">
          <span class="stat-k">区间涨跌</span>
          <span class="stat-v" :class="signClass(detail.stats.rangeChangePct)">
            {{ fmtPct(detail.stats.rangeChangePct) }}
          </span>
          <span class="stat-h">数值上行 = 人民币贬值</span>
        </div>
        <div class="stat-card">
          <span class="stat-k">区间高</span>
          <span class="stat-v">{{ fmtRate(maxClose) }}</span>
          <span class="stat-h">closes 最大值</span>
        </div>
        <div class="stat-card">
          <span class="stat-k">区间低</span>
          <span class="stat-v">{{ fmtRate(minClose) }}</span>
          <span class="stat-h">closes 最小值</span>
        </div>
      </div>

      <!-- 详情 -->
      <el-descriptions :title="`${TYPE_LABEL[detail.rateType]}口径信息`" :column="3" border size="small" class="desc-card">
        <el-descriptions-item label="口径">{{ TYPE_LABEL[detail.rateType] }}</el-descriptions-item>
        <el-descriptions-item label="数据来源">{{ TYPE_SOURCE[detail.rateType] }}</el-descriptions-item>
        <el-descriptions-item label="源侧代码">
          {{ detail.rateType === 'mid' ? (detail.officialCode ?? '—') : (detail.spotCode ?? '—') }}
        </el-descriptions-item>
        <el-descriptions-item label="该口径历史起点">
          {{ detail.availableTypes[detail.rateType]?.earliest ?? '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="该口径总条数">
          {{ detail.availableTypes[detail.rateType]?.points ?? 0 }} 条
        </el-descriptions-item>
        <el-descriptions-item label="库内更新">
          {{ detail.updatedAt ? detail.updatedAt.slice(0, 10) : '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="当前区间">
          {{ detail.range.from }} ~ {{ detail.range.to }}
        </el-descriptions-item>
        <el-descriptions-item label="区间条数">{{ detail.history.length }} 条</el-descriptions-item>
        <el-descriptions-item label="另一口径">
          <template v-if="detail.rateType === 'mid'">
            即期 {{ detail.availableTypes.spot?.points ? `${detail.availableTypes.spot.points} 条（${detail.availableTypes.spot.earliest} 起）` : '无' }}
          </template>
          <template v-else>
            中间价 {{ detail.availableTypes.mid?.points ? `${detail.availableTypes.mid.points} 条（${detail.availableTypes.mid.earliest} 起）` : '无' }}
          </template>
        </el-descriptions-item>
      </el-descriptions>
    </template>

    <div v-else-if="loading" v-loading="true" style="height:340px" />
    <el-alert v-else-if="error" :title="error" type="error" show-icon :closable="false" />
  </div>
</template>

<style scoped>
.fx-detail {
  max-width: 1100px;
  margin: 0 auto;
  padding: 24px 20px 48px;
}

.back-btn { margin-bottom: 20px; font-size: 14px; }

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
.pair {
  font-size: 14px;
  font-weight: 400;
  color: #98a2ae;
  font-family: ui-monospace, Menlo, Consolas, monospace;
}

.meta-tags { display: flex; gap: 8px; flex-wrap: wrap; }

.price-block { text-align: right; flex-shrink: 0; }
.latest-price {
  font-size: 32px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  color: #1a1a2e;
}
.unit { font-size: 14px; font-weight: 400; color: #888; margin-left: 4px; }

.change { font-size: 14px; font-weight: 600; margin-top: 4px; font-variant-numeric: tabular-nums; }
.change-label { font-size: 12px; font-weight: 400; color: #999; }
.price-date { font-size: 12px; color: #b0b8c2; margin-top: 2px; }

.up   { color: #e8534a; }
.down { color: #26a17b; }
.flat { color: #8a94a0; }

.chart-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
  flex-wrap: wrap;
}
.range-group { margin-left: auto; }

.type-note {
  font-size: 12px;
  color: #a8b0bb;
  margin: 0 0 12px;
}

.chart-wrap {
  background: #fff;
  border: 1px solid #eee;
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 20px;
  min-height: 420px;
}
.no-data {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 380px;
  color: #aaa;
  font-size: 14px;
}
.no-data code {
  background: #f5f6f8;
  padding: 1px 6px;
  border-radius: 4px;
  margin: 0 4px;
}

.stat-row {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 14px;
  margin-bottom: 22px;
}
.stat-card {
  background: #fff;
  border: 1px solid #eef1f4;
  border-radius: 10px;
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.stat-k { font-size: 12px; color: #a8b0bb; }
.stat-v { font-size: 19px; font-weight: 700; color: #1a1a2e; font-variant-numeric: tabular-nums; }
.stat-h { font-size: 11px; color: #c3c9d2; }

.desc-card { margin-top: 8px; }
</style>
