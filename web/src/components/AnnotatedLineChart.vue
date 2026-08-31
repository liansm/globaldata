<script setup lang="ts">
/**
 * AnnotatedLineChart — 带最高/最低点标注的折线图包装组件
 *
 * 在传入的 ECharts option 中，为每个 line 系列自动注入 markPoint：
 *   - 最高点：红色圆点 + 数值标注（上方）
 *   - 最低点：绿色圆点 + 数值标注（下方）
 *
 * 标注跟随 dataZoom 联动：缩放窗口变化时，按「当前可见区间」重算最高/最低。
 * 用法与 <v-chart> 一致（option / autoresize / style 均透传）：
 *   <AnnotatedLineChart :option="chartOption" autoresize style="width:100%;height:380px" />
 */
import { ref, computed } from 'vue'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { MarkPointComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import VChart from 'vue-echarts'

// markPoint 依赖 MarkPointComponent（其余组件由各页面自行注册）
use([CanvasRenderer, LineChart, MarkPointComponent])

const props = defineProps<{ option: any }>()

const MAX_COLOR = '#e8534a'   // 最高（红，涨跌色约定）
const MIN_COLOR = '#26a17b'   // 最低（绿）

// 当前数据窗口（百分比），随 dataZoom 更新
const zoom = ref({ start: 0, end: 100 })

function onDataZoom(e: any) {
  const s = Array.isArray(e?.batch) ? e.batch[0] : e
  const start = Number(s?.start)
  const end   = Number(s?.end)
  if (!Number.isFinite(start) || !Number.isFinite(end)) return
  if (start !== zoom.value.start || end !== zoom.value.end) {
    zoom.value = { start, end }
  }
}

/**
 * 计算 values[startPct..endPct] 可见区间内的最高/最低点，生成 markPoint 配置。
 * values 中允许 null（缺数），null 不参与比较。
 */
function buildMarkPoint(
  values: (number | null)[],
  labels: string[],
  startPct: number,
  endPct: number,
) {
  const n = values.length
  if (!n) return undefined

  let i0 = Math.floor((startPct / 100) * (n - 1))
  let i1 = Math.ceil((endPct / 100) * (n - 1))
  i0 = Math.max(0, Math.min(n - 1, i0))
  i1 = Math.max(0, Math.min(n - 1, i1))
  if (i1 < i0) { const t = i0; i0 = i1; i1 = t }

  let maxI = -1, minI = -1
  let maxV = NaN, minV = NaN
  for (let i = i0; i <= i1; i++) {
    const v = values[i]
    if (v == null) continue
    if (maxI < 0 || v > maxV) { maxI = i; maxV = v }
    if (minI < 0 || v < minV) { minI = i; minV = v }
  }
  if (maxI < 0 || minI < 0) return undefined

  const fmtV = (v: number) => v.toLocaleString('zh-CN', { maximumFractionDigits: 2 })

  const point = (i: number, name: string, color: string, position: string) => ({
    name,
    coord: [labels[i] ?? i, values[i]],
    symbol: 'circle' as const,
    symbolSize: 7,
    itemStyle: { color, borderColor: '#fff', borderWidth: 1.5 },
    label: {
      show: true,
      position,
      distance: 6,
      formatter: fmtV(values[i] as number),
      fontSize: 11,
      fontWeight: 600,
      color,
    },
  })

  return {
    silent: true,
    data: [
      point(maxI, '最高', MAX_COLOR, 'top'),
      point(minI, '最低', MIN_COLOR, 'bottom'),
    ],
  }
}

// 注入 markPoint + 同步 dataZoom 状态（避免 setOption 时缩放被重置）
const enriched = computed(() => {
  const opt = props.option
  if (!opt || !Array.isArray(opt.series)) return opt

  const xAxisArr = Array.isArray(opt.xAxis) ? opt.xAxis : [opt.xAxis].filter(Boolean)
  const dzArr = Array.isArray(opt.dataZoom) ? opt.dataZoom
    : opt.dataZoom ? [opt.dataZoom] : []

  const series = opt.series.map((s: any) => {
    if (s?.type !== 'line' || !Array.isArray(s.data)) return s
    const axis = xAxisArr[s.xAxisIndex ?? 0]
    const labels: string[] = axis?.data ?? []
    const mp = buildMarkPoint(s.data, labels, zoom.value.start, zoom.value.end)
    return mp ? { ...s, markPoint: mp } : s
  })

  const dataZoom = dzArr.map((dz: any) => ({
    ...dz, start: zoom.value.start, end: zoom.value.end,
  }))

  return { ...opt, series, dataZoom }
})
</script>

<template>
  <v-chart :option="enriched" @datazoom="onDataZoom" />
</template>
