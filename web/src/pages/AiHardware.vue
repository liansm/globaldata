<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { TooltipComponent, GridComponent, DataZoomComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { fetchDram, fetchGpu } from '@/api/ai'
import AnnotatedLineChart from '@/components/AnnotatedLineChart.vue'
import type { DramProduct, GpuProduct } from '@/types/ai'

use([CanvasRenderer, LineChart, TooltipComponent, GridComponent, DataZoomComponent])

// ── AI 硬件 ──────────────────────────────────────────────────────────────────
// 两个 tab，**数据性质完全不同**：
//   ① 存储颗粒 —— dram.com.cn 的**真实现货报价**（美元/颗、美元/GB，滚动半年）
//   ② GPU 算力 —— 各家云厂商的**公开租价**（美元/卡·小时）
//
// ⚠ 三个容易搞错的地方，页面上都写了提示：
//   · 颗粒是"颗"不是"条"：DDR5 16Gb = 2GB 一颗，做 16GB 内存条要 8 颗；
//     所以主口径用 **$/GB**，用 $/颗 横向比会被容量差异带偏
//     （DDR5 24Gb $19.50/GB vs 16Gb $27.85/GB，大容量反而便宜）。
//   · GPU 的 $/卡·小时 是**租价**，不是买断价；买断参考价是另一个数量级，单独标。
//   · CPO / 光模块**没有**公开报价（详见 tab2 底部说明），不是抓漏。

const route = useRoute()
const router = useRouter()

type TabName = 'memory' | 'gpu'
const tab = ref<TabName>(route.query.tab === 'gpu' ? 'gpu' : 'memory')
watch(() => route.query.tab, t => { tab.value = t === 'gpu' ? 'gpu' : 'memory' })
function onTabChange(name: string | number) {
  const wantGpu = name === 'gpu'
  if (wantGpu === (route.query.tab === 'gpu')) return
  router.replace(wantGpu
    ? { name: 'ai-hardware', query: { tab: 'gpu' } }
    : { name: 'ai-hardware' })
}

// ── 数据 ─────────────────────────────────────────────────────────────────────
const dram = ref<DramProduct[]>([])
const gpus = ref<GpuProduct[]>([])
const loading = ref(false)
const error = ref('')

onMounted(async () => {
  loading.value = true
  try {
    // 两条线互不阻断：GPU 抓不到不该把颗粒一起打成空白
    const [dramRows, gpuRows] = await Promise.all([
      fetchDram().catch(() => [] as DramProduct[]),
      fetchGpu().catch(() => [] as GpuProduct[]),
    ])
    dram.value = dramRows
    gpus.value = gpuRows
    if (!dramRows.length && !gpuRows.length) {
      error.value = '数据加载失败，请检查后端服务（localhost:3000）是否启动'
    }
  } finally {
    loading.value = false
  }
})

// ═══════════════════════════════════════════════════════════════════════════
// ① 存储颗粒
// ═══════════════════════════════════════════════════════════════════════════
// 分组顺序 = 一代一行：DDR5 → DDR4（DDR3 已不抓，库里历史也不再展示）。
// 代际内正片在前、eTT 在后：eTT（白牌）同规格与正片差 5~6 倍，必须能一眼分开。
const DRAM_ORDER: { gen: string; keys: string[] }[] = [
  { gen: 'DDR5', keys: ['ddr5_24gb_5200', 'ddr5_16gb_4800', 'ddr5_16gb_ett'] },
  { gen: 'DDR4', keys: ['ddr4_16gb_2666', 'ddr4_16gb_ett', 'ddr4_8gb_2666', 'ddr4_8gb_ett', 'ddr4_4gb_2400', 'ddr4_4gb_ett'] },
]

const GEN_NOTE: Record<string, string> = {
  DDR5: '服务器 / 新一代 PC 主流。24Gb 大容量颗粒每 GB 反而更便宜，是这轮涨价里最先见顶的一档',
  DDR4: '存量产线仍在大量出货。注意每 GB 价已反超 DDR5 —— 原厂转产 DDR5 后 DDR4 供给收缩，这是最直接的信号',
}

const dramGroups = computed(() => {
  const map = new Map(dram.value.map(d => [d.key, d]))
  const used = new Set<string>()
  const out: { gen: string; title: string; note: string; items: DramProduct[] }[] = []

  for (const g of DRAM_ORDER) {
    const items: DramProduct[] = []
    for (const k of g.keys) {
      const hit = map.get(k)
      if (hit) { items.push(hit); used.add(k) }
    }
    // 同代际但未列入白名单的品种接在后面，避免上游加品种时静默漏掉
    for (const d of dram.value) {
      if (d.generation === g.gen && !used.has(d.key)) { items.push(d); used.add(d.key) }
    }
    if (items.length) {
      out.push({ gen: g.gen, title: `${g.gen} 存储颗粒`, note: GEN_NOTE[g.gen] ?? '', items })
    }
  }
  const rest = dram.value.filter(d => !used.has(d.key))
  if (rest.length) out.push({ gen: '其他', title: '其他存储颗粒', note: '', items: rest })
  return out
})

const selectedKey = ref<string>('')
const selected = computed<DramProduct | null>(() => {
  const hit = dram.value.find(d => d.key === selectedKey.value)
  return hit ?? dram.value[0] ?? null
})
watch(dram, rows => {
  if (!rows.length || selectedKey.value) return
  // 默认选 DDR5 24Gb（涨价最猛、也最能体现「每 GB 更便宜」）
  selectedKey.value = rows.some(d => d.key === 'ddr5_24gb_5200') ? 'ddr5_24gb_5200' : rows[0].key
})

// 图表口径：$/GB（默认，横向可比）↔ $/颗（源站原始口径）
const dramUnit = ref<'perGb' | 'perChip'>('perGb')

// 区间：源站只有滚动半年，所以最多就是「全部」，其余是切片
const RANGES = [
  { key: 'all', label: '全部（源站半年窗口）', days: 9999 },
  { key: '3m',  label: '近 3 个月', days: 92 },
  { key: '1m',  label: '近 1 个月', days: 31 },
] as const
const dramRange = ref<'all' | '3m' | '1m'>('all')

const dramPoints = computed(() => {
  const p = selected.value
  if (!p) return []
  const cfg = RANGES.find(r => r.key === dramRange.value)!
  if (cfg.key === 'all') return p.series
  const cutoff = new Date(Date.now() - cfg.days * 86400_000).toISOString().slice(0, 10)
  return p.series.filter(s => s.date >= cutoff)
})

const dramChartOption = computed(() => {
  const pts = dramPoints.value
  if (!pts.length) return {}
  const perGb = dramUnit.value === 'perGb'
  const vals = pts.map(s => (perGb ? s.perGb : s.close)).filter((v): v is number => v != null)
  if (!vals.length) return {}

  // 122 个报价日塞进 408px，标签必然重叠；显式按「最多 6 个」抽稀，
  // 光靠 hideOverlap 不够（它只隐藏，不保证留下的几个间距够）
  const step = Math.max(1, Math.ceil(pts.length / 6))

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const x = params[0]
        if (!x) return ''
        const pt = pts[x.dataIndex]
        // 两个口径一起给：$ 的绝对值只在同口径内可比，两个都在才不会误读
        return `${x.axisValue}<br/>`
          + `<b>$${Number(x.value).toFixed(2)}</b> /${perGb ? 'GB' : '颗'}<br/>`
          + `<span style="color:#98a0b0">${
              perGb ? `$${(pt?.close ?? 0).toFixed(2)} /颗` : `$${(pt?.perGb ?? 0).toFixed(2)} /GB`
            }</span>`
      },
    },
    grid: { top: 18, right: 26, bottom: 46, left: 54 },
    xAxis: {
      type: 'category', data: pts.map(s => s.date),
      axisLabel: { rotate: 30, fontSize: 10.5, color: '#8a94a6', interval: step - 1 },
      axisLine: { lineStyle: { color: '#e4e7ee' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      // 交给 echarts 自己取整刻度：硬设 min/max 会得到 `$21 $20 $18 $16` 这种
      // 不等距刻度（padding 是按极差算的，不是按「好看的数字」算的）
      scale: true,
      splitNumber: 5,
      axisLabel: { fontSize: 10.5, color: '#8a94a6', formatter: (v: number) => `$${v.toFixed(0)}` },
      splitLine: { lineStyle: { color: '#f0f2f7' } },
    },
    dataZoom: [
      { type: 'inside', throttle: 60 },
      {
        type: 'slider', height: 16, bottom: 8,
        borderColor: 'transparent', backgroundColor: '#f5f7fb',
        fillerColor: 'rgba(64,158,255,.12)',
        handleStyle: { color: '#409eff' },
        textStyle: { fontSize: 10, color: '#a8b0be' },
      },
    ],
    series: [{
      type: 'line',
      data: pts.map(s => (perGb ? s.perGb : s.close)),
      smooth: false,
      showSymbol: pts.length <= 20,
      symbolSize: 5,
      lineStyle: { width: 2, color: perGb ? '#7c5cff' : '#409eff' },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: perGb ? 'rgba(124,92,255,.20)' : 'rgba(64,158,255,.22)' },
            { offset: 1, color: perGb ? 'rgba(124,92,255,0)' : 'rgba(64,158,255,0)' },
          ],
        },
      },
    }],
  }
})

const dramStats = computed(() => {
  const p = selected.value
  const pts = dramPoints.value
  if (!p || !pts.length) return null
  const perGb = dramUnit.value === 'perGb'
  const vals = pts.map(s => (perGb ? s.perGb : s.close)).filter((v): v is number => v != null)
  if (!vals.length) return null
  const first = vals[0]
  const last = vals[vals.length - 1]
  return {
    min: Math.min(...vals),
    max: Math.max(...vals),
    // 区间涨跌 = 首末两点之比，**不是「今年来」** —— 源站只给滚动半年
    periodPct: first ? ((last - first) / first) * 100 : null,
    firstDate: pts[0].date,
    lastDate: pts[pts.length - 1].date,
  }
})

// ═══════════════════════════════════════════════════════════════════════════
// ② GPU 算力
// ═══════════════════════════════════════════════════════════════════════════
// 顺序与后端 fetch_ai_hardware.py::GPU_MODELS 的 sort_order 一致；卡片按接口顺序渲染，
// 这里只做「默认选中哪一张」。
const gpuSelectedKey = ref<string>('')
const gpuSelected = computed<GpuProduct | null>(() => {
  const hit = gpus.value.find(g => g.key === gpuSelectedKey.value)
  return hit ?? gpus.value[0] ?? null
})
watch(gpus, rows => {
  if (!rows.length || gpuSelectedKey.value) return
  // 默认选 B200：当前 AI 集群的主流选择，也是逐家明细最全的那个
  gpuSelectedKey.value = rows.some(g => g.key === 'b200_sxm') ? 'b200_sxm' : rows[0].key
})

const gpuChartOption = computed(() => {
  const g = gpuSelected.value
  if (!g || g.series.length < 2) return {}
  const vals = g.series.map(s => s.price)
  const minVal = Math.min(...vals)
  const maxVal = Math.max(...vals)
  const padding = (maxVal - minVal) * 0.18 || maxVal * 0.1
  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const x = params[0]
        if (!x) return ''
        return `${x.axisValue}<br/><b>$${Number(x.value).toFixed(2)}</b> /卡·小时`
      },
    },
    grid: { top: 18, right: 26, bottom: 30, left: 56 },
    xAxis: {
      type: 'category', data: g.series.map(s => s.date),
      axisLabel: { rotate: 35, fontSize: 10.5, color: '#8a94a6', hideOverlap: true },
      axisLine: { lineStyle: { color: '#e4e7ee' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      min: Math.max(0, minVal - padding),
      max: maxVal + padding,
      axisLabel: { fontSize: 10.5, color: '#8a94a6', formatter: (v: number) => `$${v.toFixed(2)}` },
      splitLine: { lineStyle: { color: '#f0f2f7' } },
      scale: true,
    },
    series: [{
      type: 'line',
      data: vals,
      smooth: false,
      showSymbol: vals.length <= 20,
      symbolSize: 5,
      lineStyle: { width: 2, color: '#0ea5a5' },
      itemStyle: { color: '#0ea5a5' },
      areaStyle: {
        color: {
          type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
          colorStops: [
            { offset: 0, color: 'rgba(14,165,165,.18)' },
            { offset: 1, color: 'rgba(14,165,165,0)' },
          ],
        },
      },
    }],
  }
})

// ── 展示工具 ─────────────────────────────────────────────────────────────────
function fmt(v: number | null | undefined, d = 2) {
  if (v == null) return '—'
  return v.toLocaleString('zh-CN', { minimumFractionDigits: d, maximumFractionDigits: d })
}
function fmtDate(d: string | null | undefined) {
  return d ? d.slice(0, 10) : '—'
}
function fmtPct(v: number | null | undefined) {
  if (v == null) return '—'
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
}
function pctClass(v: number | null | undefined) {
  // 0 走中性灰：DDR4 多数品种长期不动，「+0.00%」涂成红色会让人以为在涨
  if (v == null || v === 0) return ''
  return v > 0 ? 'up' : 'down'
}
// 买断参考价：$27,000~40,000 这种量级，用 k 更易读
function fmtK(v: number | null | undefined) {
  if (v == null) return '—'
  return v >= 10000 ? `$${(v / 1000).toFixed(0)}k` : `$${v.toFixed(0)}`
}
function vramText(g: GpuProduct) {
  if (g.vramGb == null) return 'VRAM 未公布'
  return `${g.vramGb} GB · ${g.vendor}`
}

const latestDramDate = computed(() => {
  const ds = dram.value.map(d => d.latestDate).filter(Boolean) as string[]
  return ds.length ? ds.sort()[ds.length - 1] : null
})
const latestGpuDate = computed(() => {
  const ds = gpus.value.map(g => g.latestDate).filter(Boolean) as string[]
  return ds.length ? ds.sort()[ds.length - 1] : null
})
const gpuCount = computed(() => gpus.value.filter(g => g.latestHourly != null).length)
</script>

<template>
  <div class="ai-page">
    <header class="page-head">
      <div>
        <h1>AI 硬件</h1>
        <p class="subtitle">
          存储颗粒现货（DDR5 / DDR4，美元/GB）
          <span class="dot">·</span> GPU 算力租价（美元/卡·小时）
        </p>
      </div>
      <div class="head-stats">
        <div class="hstat"><b>{{ dram.length }}</b><span>颗粒品种</span></div>
        <div class="hstat"><b>{{ gpuCount }}</b><span>GPU 型号</span></div>
        <div class="hstat"><b>{{ fmtDate(latestDramDate) }}</b><span>颗粒报价日</span></div>
        <div class="hstat"><b>{{ fmtDate(latestGpuDate) }}</b><span>GPU 报价日</span></div>
      </div>
    </header>

    <div class="tabs-wrap">
      <el-tabs v-model="tab" class="ai-tabs" @tab-change="onTabChange">
        <!-- ══ ① 存储颗粒 ═════════════════════════════════════════════════ -->
        <el-tab-pane name="memory">
          <template #label><span class="tab-label">💾 存储颗粒</span></template>

          <div class="pane split">
            <el-alert
              v-if="error" :title="error" type="error" show-icon :closable="false"
              style="margin-bottom: 14px"
            />

            <div v-if="loading" class="skeleton-wrap"><el-skeleton :rows="8" animated /></div>

            <template v-else-if="dram.length">
              <!-- 左：分组卡片 -->
              <div class="dram-list">
                <section v-for="g in dramGroups" :key="g.gen" class="group">
                  <div class="group-head">
                    <h2 class="group-title">{{ g.title }}</h2>
                    <span class="group-count">{{ g.items.length }} 档</span>
                  </div>
                  <p v-if="g.note" class="group-note">{{ g.note }}</p>

                  <div class="card-grid">
                    <div
                      v-for="item in g.items" :key="item.key"
                      class="card" :class="[{ active: item.key === selected?.key }, item.grade]"
                      @click="selectedKey = item.key"
                    >
                      <div class="card-top">
                        <span class="card-name" :title="item.name">{{ item.name }}</span>
                        <span v-if="item.grade === 'ett'" class="tag-ett">白牌</span>
                      </div>
                      <!-- 主口径：美元/GB（横向可比） -->
                      <div class="card-price">
                        ${{ fmt(item.latestPerGb) }}
                        <span class="card-unit">/GB</span>
                      </div>
                      <div class="card-sub">
                        ${{ fmt(item.latestClose) }} /颗
                      </div>
                      <div class="card-pct" :class="pctClass(item.changePct)"
                           title="基于上一报价日；源站周末与节假日不报价，相邻两点可能隔 2~4 天">
                        {{ fmtPct(item.changePct) }}
                      </div>
                      <div class="card-footer">
                        <span class="card-date">{{ fmtDate(item.latestDate) }}</span>
                      </div>
                    </div>
                  </div>
                </section>

                <p class="foot-note">
                  数据源 <code>dram.com.cn</code>（中国IC交易网，深圳现货）。
                  <b>单位是美元/颗（颗粒），不是内存条成品价</b> —— <code>DDR5 16Gb</code> 指
                  16Gb 颗粒（≈2GB），做成 16GB 内存条需 8 颗，所以卡片主口径用
                  <b>美元/GB</b>：只有这个口径才能横向比「哪一代、哪一档更贵」。<br>
                  报价日历不连续（周末无报价），图表按上游给的日期逐点连接，<b>不做补空</b>；
                  历史仅保留滚动半年，早期数据站方需付费购买 —— 因此本站需日更累积。<br>
                  正片与 <b>eTT（白牌）</b>同规格差 5~6 倍，用灰色虚线卡片区分，<b>不可混为一条曲线</b>。
                </p>
              </div>

              <!-- 右：走势 + 明细 -->
              <aside class="dram-detail">
                <div class="detail-head">
                  <div class="detail-name">{{ selected?.name }}</div>
                  <div class="detail-price">
                    ${{ fmt(dramUnit === 'perGb' ? selected?.latestPerGb : selected?.latestClose) }}
                    <span class="detail-unit">{{ dramUnit === 'perGb' ? '美元/GB' : '美元/颗' }}</span>
                  </div>
                  <div class="detail-sub">
                    <span class="detail-pct" :class="pctClass(selected?.changePct)">
                      {{ fmtPct(selected?.changePct) }}
                    </span>
                    <span class="detail-date">{{ fmtDate(selected?.latestDate) }}</span>
                  </div>
                </div>

                <!-- 口径 + 区间切换 -->
                <div class="chart-toolbar">
                  <el-radio-group v-model="dramUnit" size="small">
                    <el-radio-button value="perGb">美元/GB</el-radio-button>
                    <el-radio-button value="perChip">美元/颗</el-radio-button>
                  </el-radio-group>
                  <el-select v-model="dramRange" size="small" style="width: 132px">
                    <el-option v-for="r in RANGES" :key="r.key" :label="r.label" :value="r.key" />
                  </el-select>
                </div>

                <div class="chart-box">
                  <AnnotatedLineChart
                    v-if="Object.keys(dramChartOption).length"
                    :option="dramChartOption" :decimals="dramUnit === 'perGb' ? 2 : 2" autoresize
                    style="width: 100%; height: 272px"
                  />
                </div>

                <div v-if="dramStats" class="detail-stats">
                  <div class="dstat">
                    <span class="ds-label">区间最高</span>
                    <b class="ds-val">${{ fmt(dramStats.max) }}</b>
                  </div>
                  <div class="dstat">
                    <span class="ds-label">区间最低</span>
                    <b class="ds-val">${{ fmt(dramStats.min) }}</b>
                  </div>
                  <div class="dstat">
                    <span class="ds-label">区间涨跌</span>
                    <b class="ds-val" :class="pctClass(dramStats.periodPct)">
                      {{ fmtPct(dramStats.periodPct) }}
                    </b>
                  </div>
                  <div class="dstat">
                    <span class="ds-label">{{ dramUnit === 'perGb' ? '每颗价' : '每 GB 价' }}</span>
                    <b class="ds-val">
                      ${{ fmt(dramUnit === 'perGb' ? selected?.latestClose : selected?.latestPerGb) }}
                    </b>
                  </div>
                  <div class="dstat wide">
                    <span class="ds-label">区间（源站滚动半年）</span>
                    <b class="ds-val small">
                      {{ dramStats.firstDate }} → {{ dramStats.lastDate }}
                      （{{ dramPoints.length }} 个报价日）
                    </b>
                  </div>
                  <div class="dstat wide">
                    <span class="ds-label">颗粒规格</span>
                    <b class="ds-val small">
                      {{ selected?.generation }} · {{ selected?.densityGb }}Gb（≈{{ ((selected?.densityGb ?? 0) / 8).toFixed(1) }}GB）·
                      {{ selected?.speed ?? '—' }} ·
                      {{ selected?.grade === 'ett' ? 'eTT 白牌' : '原厂正片' }}
                    </b>
                  </div>
                </div>

                <p class="detail-note">
                  区间涨跌 = 首末两个报价日之比，<b>不是「今年来」</b> —— 源站只给滚动半年。
                </p>
              </aside>
            </template>

            <div v-else-if="!error" class="empty">暂无存储颗粒数据</div>
          </div>
        </el-tab-pane>

        <!-- ══ ② GPU 算力 ═════════════════════════════════════════════════ -->
        <el-tab-pane name="gpu">
          <template #label><span class="tab-label">🧠 GPU 算力</span></template>

          <div class="pane scroll">
            <el-alert type="info" :closable="false" show-icon class="proxy-alert">
              <template #title>
                GPU 芯片<b>没有公开的实时售价</b>，这里给的是<b>云厂商租价</b>与<b>买断参考价</b>两套口径
              </template>
              NVIDIA 不公布单卡售价，零售渠道（CDW / Insight / Newegg / eBay）全部反爬。
              能公开拿到的：① 各家云厂商的 <b>租价（美元/卡·小时）</b>，日更、可比；
              ② 媒体转述的 <b>单卡买断区间</b>，静态、仅供量级参考。
              两者差三个数量级，<b>不要互相换算成「回本周期」</b>。
            </el-alert>

            <div v-if="gpus.length" class="gpu-body">
              <!-- 型号卡片 -->
              <section class="group">
                <div class="group-head">
                  <h2 class="group-title">NVIDIA 数据中心 GPU</h2>
                  <span class="group-count">{{ gpus.length }} 款</span>
                </div>
                <div class="card-grid gpu-grid">
                  <div
                    v-for="g in gpus" :key="g.key"
                    class="card gpu-card" :class="{ active: g.key === gpuSelected?.key, empty: g.latestHourly == null }"
                    @click="gpuSelectedKey = g.key"
                  >
                    <div class="card-top">
                      <span class="card-name" :title="g.name">{{ g.name }}</span>
                    </div>
                    <div class="card-spec">{{ vramText(g) }}</div>

                    <template v-if="g.latestHourly != null">
                      <div class="card-price">
                        ${{ fmt(g.latestHourly) }}
                        <span class="card-unit">/卡·小时</span>
                      </div>
                      <div class="card-pct" :class="pctClass(g.changePct)">
                        {{ fmtPct(g.changePct) }}
                      </div>
                    </template>
                    <div v-else class="card-na">暂无公开报价</div>

                    <div class="card-buy">
                      买断参考
                      <b v-if="g.unitPriceLow != null">
                        {{ fmtK(g.unitPriceLow) }}~{{ fmtK(g.unitPriceHigh) }}
                      </b>
                      <b v-else class="na">—</b>
                    </div>
                    <div class="card-footer">
                      <span class="card-date">{{ fmtDate(g.latestDate) }}</span>
                    </div>
                  </div>
                </div>
              </section>

              <!-- 选中型号：走势 + 厂商明细 -->
              <section class="gpu-detail">
                <div class="detail-head">
                  <div class="detail-name">{{ gpuSelected?.name }} · 云租价</div>
                  <div class="detail-price">
                    <template v-if="gpuSelected?.latestHourly != null">
                      ${{ fmt(gpuSelected.latestHourly) }}
                      <span class="detail-unit">美元/卡·小时</span>
                    </template>
                    <template v-else>
                      <span class="detail-na">暂无公开报价</span>
                    </template>
                  </div>
                  <div class="detail-sub">
                    <span class="detail-pct" :class="pctClass(gpuSelected?.changePct)">
                      {{ fmtPct(gpuSelected?.changePct) }}
                    </span>
                    <span class="detail-date">{{ fmtDate(gpuSelected?.latestDate) }}</span>
                  </div>
                </div>

                <!-- 单卡买断参考价 -->
                <div class="buy-row">
                  <span class="buy-label">单卡买断参考价</span>
                  <b v-if="gpuSelected?.unitPriceLow != null" class="buy-val">
                    {{ fmtK(gpuSelected.unitPriceLow) }} ~ {{ fmtK(gpuSelected.unitPriceHigh) }}
                  </b>
                  <b v-else class="buy-val na">尚无公开报价</b>
                </div>
                <p v-if="gpuSelected?.unitPriceNote" class="buy-note">
                  {{ gpuSelected.unitPriceNote }}
                  <template v-if="gpuSelected.unitPriceAsof">
                    · 口径时点 {{ gpuSelected.unitPriceAsof }}
                  </template>
                </p>

                <!-- 走势：新库只有 1~2 个点，说清楚而不是画空图 -->
                <div class="chart-box">
                  <AnnotatedLineChart
                    v-if="Object.keys(gpuChartOption).length"
                    :option="gpuChartOption" :decimals="2" autoresize
                    style="width: 100%; height: 200px"
                  />
                  <div v-else class="chart-placeholder">
                    <template v-if="gpuSelected?.latestHourly != null">
                      该型号目前只有 <b>{{ gpuSelected.points }}</b> 个报价日（{{ fmtDate(gpuSelected.latestDate) }}），
                      走势图从第二个报价日起才有意义 —— 脚本每天跑一次就会逐步累积。
                    </template>
                    <template v-else>该型号暂无云厂商公开报价</template>
                  </div>
                </div>

                <!-- 各云厂商明细 -->
                <div v-if="gpuSelected?.providers.length" class="provider-block">
                  <div class="provider-head">
                    <span>各云厂商报价（{{ gpuSelected.providers.length }} 家）</span>
                    <span class="provider-hint">已折算为每卡；「8 卡节点」是按整机总价 ÷ 8</span>
                  </div>
                  <div class="provider-list">
                    <div
                      v-for="p in gpuSelected.providers" :key="p.provider + p.region"
                      class="provider-row"
                    >
                      <span class="pv-name">{{ p.provider }}</span>
                      <span class="pv-region">{{ p.region || '—' }}</span>
                      <span v-if="p.gpusPerNode && p.gpusPerNode > 1" class="pv-node">x{{ p.gpusPerNode }} 节点</span>
                      <span v-else class="pv-node single">单卡</span>
                      <b class="pv-price">${{ fmt(p.priceHourly) }}</b>
                    </div>
                  </div>
                </div>
                <div v-else class="provider-block">
                  <div class="provider-head"><span>各云厂商报价</span></div>
                  <p class="provider-empty">
                    上游只对<b>当前主推型号</b>（目前是 B200 SXM）给出逐家明细，
                    其他型号只有全市场最低价，所以这里没有列表 —— <b>是数据源给的就这么多，不是抓漏</b>。
                  </p>
                </div>
              </section>

              <p class="foot-note">
                租价来源 <code>aimultiple.com/ai-gpus</code>（聚合各家云厂商公开牌价）·
                买断参考价来源 <code>{{ gpuSelected?.unitPriceSrc ?? '媒体转述' }}</code>。<br>
                <b>租价最低价</b>取源站口径的「单卡（x1）最低」，不含整节点摊销价 ——
                所以会出现「最低价 $3.67，但明细里 Vultr 摊到 $3.20」，后者是 8 卡整机的每卡价，属正常。<br>
                卡片上的显存 / 带宽按<b>源站口径</b>，与实机有出入（源站把 B300 记 144GB、实机 288GB；
                H100 SXM 记 94GB、实机 80GB），仅用于识别型号，<b>不作为规格依据</b>。<br>
                <b>关于 CPO / 光模块</b>：确实没有可抓的公开报价（实测 2026-10）——
                FS.com 是纯客户端渲染、Amazon 搜 800G 会混进 400G 与 PoE 模块、
                Digi-Key / Mouser / eBay / 新蛋 全部 403。光模块 ASP 只存在于
                LightCounting / Yole 付费报告与厂商财报里，因此本站不提供这块数据，<b>不做估算值</b>。
              </p>
            </div>

            <div v-else-if="!error" class="empty">暂无 GPU 报价数据（先跑 fetch_ai_hardware.py）</div>
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>

<style scoped>
/* 整屏布局与 Shipping.vue 同源：页面占一屏，内部 flex 纵向分配，
   tabs 吃掉剩余高度，tab 内部自己滚动。高度用 vh 不用 100%（原因见 Shipping.vue）。 */
.ai-page {
  padding: 16px 22px 18px;
  height: 100vh;
  display: flex;
  flex-direction: column;
}

.page-head {
  display: flex; align-items: flex-end; justify-content: space-between;
  gap: 16px; flex-wrap: wrap; margin-bottom: 8px; flex-shrink: 0;
}
.page-head h1 { margin: 0; font-size: 22px; font-weight: 700; letter-spacing: -0.4px; }
.subtitle { margin: 4px 0 0; font-size: 12.5px; color: #8a94a6; }
.subtitle .dot { margin: 0 4px; color: #c9cfda; }

.head-stats { display: flex; gap: 8px; }
.hstat {
  background: #fff; border: 1px solid #eef0f5; border-radius: 10px;
  padding: 6px 12px; display: flex; flex-direction: column; min-width: 74px;
}
.hstat b { font-size: 15px; line-height: 1.2; color: #1a1a2e; }
.hstat span { font-size: 11px; color: #98a0b0; }

/* ── tabs 撑满剩余高度（少一环就 0 高，见 Shipping.vue） ─────────────── */
.tabs-wrap { flex: 1; min-height: 0; display: flex; flex-direction: column; }
.ai-tabs { flex: 1; min-height: 0; display: flex; flex-direction: column; }
:deep(.el-tabs__header) { margin-bottom: 12px; flex-shrink: 0; }
:deep(.el-tabs__content) { flex: 1; min-height: 0; overflow: hidden; }
:deep(.el-tab-pane) { height: 100%; }
.tab-label { font-size: 13.5px; font-weight: 500; }

.pane { height: 100%; min-height: 0; }
.pane.scroll { overflow-y: auto; padding-right: 4px; }

/* 颗粒：左列表（自己滚） + 右图表（常驻可见） */
.pane.split { display: grid; grid-template-columns: minmax(0, 1fr) 430px; gap: 18px; }
@media (max-width: 1180px) {
  .pane.split { grid-template-columns: 1fr; overflow-y: auto; }
  .dram-list { overflow: visible !important; }
}
.dram-list { overflow-y: auto; min-height: 0; padding-right: 6px; }
.dram-detail {
  background: #fff; border: 1px solid #eef0f5; border-radius: 12px;
  padding: 14px 16px 12px; min-height: 0; overflow-y: auto;
  display: flex; flex-direction: column;
}

/* ── 分组 ─────────────────────────────────────────────────────────────── */
.group { margin-bottom: 20px; }
.group-head { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
.group-title { margin: 0; font-size: 14.5px; font-weight: 600; color: #1a1a2e; }
.group-count { margin-left: auto; font-size: 12px; color: #aaa; }
.group-note { margin: 0 0 10px; font-size: 11.5px; color: #9aa3b2; line-height: 1.6; }

.card-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(196px, 1fr)); gap: 10px; }
.card {
  background: #fff; border: 1px solid #e8e8f0; border-radius: 12px;
  padding: 12px 14px 10px; cursor: pointer;
  transition: box-shadow .18s, transform .18s, border-color .18s, background .18s;
  display: flex; flex-direction: column; gap: 4px;
}
.card:hover { box-shadow: 0 6px 20px rgba(0,0,0,.09); transform: translateY(-2px); border-color: #409eff; }
.card.active { border-color: #409eff; box-shadow: 0 0 0 1px rgba(64,158,255,.25) inset; background: #fafdff; }
/* eTT 是白牌料，与正片价差 5~6 倍：用灰底 + 虚线边区分，避免被当成同档货 */
.card.ett { background: #fbfbfc; border-style: dashed; }
.card.ett.active { background: #f7fafd; border-style: solid; }

.card-top { display: flex; align-items: flex-start; gap: 6px; }
.card-name {
  font-size: 12.5px; font-weight: 600; color: #1a1a2e; line-height: 1.35;
  display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
  overflow: hidden; word-break: break-word; min-height: 2.7em;
}
.tag-ett {
  flex-shrink: 0; font-size: 10px; line-height: 1; padding: 3px 5px;
  color: #98a0b0; background: #f0f2f7; border-radius: 4px; margin-top: 1px;
}
.card-price {
  font-size: 19px; font-weight: 700; color: #1a1a2e;
  font-variant-numeric: tabular-nums; line-height: 1.15;
}
.card-unit { font-size: 11.5px; font-weight: 400; color: #98a0b0; margin-left: 1px; }
.card-sub { font-size: 11.5px; color: #98a0b0; font-variant-numeric: tabular-nums; }
.card-na { font-size: 14px; font-weight: 600; color: #c0c6d2; padding: 2px 0; }
.card-pct {
  font-size: 12.5px; font-weight: 600; font-variant-numeric: tabular-nums;
  color: #c9cfda; display: flex; align-items: baseline; gap: 5px;
}
.card-footer { display: flex; align-items: center; gap: 6px; margin-top: auto; padding-top: 2px; }
.card-date { font-size: 11px; color: #b6bdc9; }

/* 红涨绿跌（中国习惯） */
.up { color: #f56c6c; }
.down { color: #67c23a; }

/* ── 右栏明细 ─────────────────────────────────────────────────────────── */
.detail-head { border-bottom: 1px solid #f0f2f7; padding-bottom: 10px; margin-bottom: 8px; }
.detail-name { font-size: 13.5px; font-weight: 600; color: #1a1a2e; line-height: 1.4; }
.detail-price {
  font-size: 27px; font-weight: 700; color: #1a1a2e;
  font-variant-numeric: tabular-nums; line-height: 1.25; margin-top: 3px;
}
.detail-unit { font-size: 12px; font-weight: 400; color: #98a0b0; margin-left: 4px; }
.detail-na { font-size: 17px; color: #c0c6d2; }
.detail-sub { display: flex; align-items: baseline; gap: 10px; }
.detail-pct { font-size: 13px; font-weight: 600; }
.detail-date { font-size: 11px; color: #b6bdc9; }

.chart-toolbar {
  display: flex; align-items: center; justify-content: space-between;
  gap: 8px; margin: 2px 0 6px; flex-wrap: wrap;
}
.chart-box { margin: 2px -6px 2px; }
.chart-placeholder {
  font-size: 11.5px; color: #a8b0be; line-height: 1.7; text-align: center;
  padding: 34px 18px; background: #fafbfe; border-radius: 10px;
}

.detail-stats { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 4px; }
.dstat {
  background: #fafbfe; border-radius: 8px; padding: 7px 10px;
  display: flex; flex-direction: column; gap: 2px;
}
.dstat.wide { grid-column: 1 / -1; }
.ds-label { font-size: 10.5px; color: #9aa3b2; }
.ds-val { font-size: 13.5px; font-weight: 600; color: #1a1a2e; font-variant-numeric: tabular-nums; }
.ds-val.small { font-size: 11.5px; font-weight: 500; color: #4a5261; }
.detail-note { font-size: 11px; color: #a8b0be; line-height: 1.65; margin: 10px 0 2px; }

/* ── GPU ──────────────────────────────────────────────────────────────── */
.gpu-body { display: flex; flex-direction: column; }
/* 7 款型号要挤进一行（1440 宽下），minmax 收到 150px；太窄就自然换行 */
.gpu-grid { grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); }
.gpu-card { gap: 3px; }
.gpu-card.empty { background: #fbfbfc; border-style: dashed; }
.card-spec { font-size: 10.5px; color: #9aa3b2; }
.card-buy {
  font-size: 11px; color: #a8b0be; margin-top: 1px;
  display: flex; align-items: baseline; gap: 4px;
}
.card-buy b { color: #5a6377; font-weight: 600; font-size: 11.5px; }
.card-buy .na, .buy-val.na { color: #c0c6d2; font-weight: 500; }

.gpu-detail {
  background: #fff; border: 1px solid #eef0f5; border-radius: 12px;
  padding: 14px 16px 12px; margin-bottom: 14px;
}
.buy-row {
  display: flex; align-items: baseline; gap: 8px;
  background: #fafbfe; border-radius: 8px; padding: 8px 12px; margin-bottom: 6px;
}
.buy-label { font-size: 11.5px; color: #9aa3b2; }
.buy-val { font-size: 15px; font-weight: 700; color: #1a1a2e; font-variant-numeric: tabular-nums; }
.buy-note { margin: 0 0 8px; font-size: 11px; color: #a8b0be; line-height: 1.6; }

.provider-block { margin-top: 10px; }
.provider-head {
  display: flex; align-items: baseline; justify-content: space-between;
  gap: 10px; font-size: 12px; font-weight: 600; color: #1a1a2e;
  padding-bottom: 6px; border-bottom: 1px solid #f0f2f7; flex-wrap: wrap;
}
.provider-hint { font-size: 10.5px; font-weight: 400; color: #a8b0be; }
/* 一家一行。原来用 3 列卡片网格，视觉上「Verda 的单卡」和「Together.ai 的价格」
   会挤在一起，读串行 → 改成单列的固定列宽表格，价格永远在最右侧同一位置。 */
.provider-list { display: flex; flex-direction: column; margin-top: 2px; }
.provider-row {
  display: grid;
  grid-template-columns: 150px minmax(0, 1fr) 62px 74px;
  align-items: baseline; gap: 10px;
  padding: 5px 0; border-bottom: 1px dashed #f4f6fa;
  font-size: 11.5px;
}
.pv-name { color: #37405a; font-weight: 500; }
.pv-region { color: #a8b0be; font-size: 10.5px; }
.pv-node {
  color: #b6bdc9; font-size: 10px; background: #f5f7fb;
  border-radius: 3px; padding: 1px 4px; justify-self: start;
}
.pv-node.single { background: #eef6ff; color: #7aa7cc; }
.pv-price { color: #1a1a2e; font-variant-numeric: tabular-nums; text-align: right; }
.provider-empty { font-size: 11.5px; color: #a8b0be; line-height: 1.7; margin: 8px 0 0; }

/* ── 其它 ─────────────────────────────────────────────────────────────── */
.proxy-alert { margin-bottom: 14px; }
.proxy-alert :deep(.el-alert__title) { font-size: 13px; }
.proxy-alert :deep(.el-alert__description) { font-size: 12px; line-height: 1.7; margin-top: 4px; }

.foot-note { font-size: 11.5px; color: #a8b0be; line-height: 1.75; margin: 6px 0 10px; }
.foot-note code { background: #f5f7fb; padding: 1px 5px; border-radius: 4px; }

.skeleton-wrap { padding: 16px 0; }
.empty { font-size: 13px; color: #98a0b0; padding: 40px 0; text-align: center; }
</style>
