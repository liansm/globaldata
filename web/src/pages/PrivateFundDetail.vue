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
  LegendComponent,
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { fetchPrivateFundDetail, fetchPrivateNav } from '@/api/privateFunds'
import AnnotatedLineChart from '@/components/AnnotatedLineChart.vue'
import type { PrivateFundDetail, PrivateNavPoint } from '@/types/privateFund'

use([CanvasRenderer, LineChart, TitleComponent, TooltipComponent,
     GridComponent, DataZoomComponent, LegendComponent])

const route  = useRoute()
const router = useRouter()

const loading = ref(true)
const error   = ref('')
const detail  = ref<PrivateFundDetail | null>(null)

// ── 净值 ────────────────────────────────────────────────────────────────────
const navPoints  = ref<PrivateNavPoint[]>([])
const navLoading = ref(false)

/**
 * 该产品的净值是否来自私募排排网（复权净值口径）。
 * 决定图表线名与提示文案 —— 排排网那套是「分红再投复权」，把它标成「单位净值」是错的。
 * 同一 fund_no 只会有一个 source（写库时对重叠标的整只跳过，不混源），所以取首点的 source 即可。
 */
const isSppwSource = computed(() => navPoints.value.some(p => p.source === 'sppw'))

/** 区间：天数（0 = 全部）或 'ytd'（今年来）。默认「今年来」，与 MarketDetail / Detail 一致 */
type NavRange = number | 'ytd'
const navRange = ref<NavRange>('ytd')

const rangeOptions: { label: string; value: NavRange }[] = [
  { label: '今年来', value: 'ytd' },
  { label: '近 3 月', value: 90 },
  { label: '近 6 月', value: 180 },
  { label: '近 1 年', value: 365 },
  { label: '全部',   value: 0 },
]

/**
 * 「今年来」的起点 = 本自然年 1 月 1 日（同 Detail.vue / MarketDetail.vue / FundDetail.vue 的 ytdFrom 规约）。
 * 刻意不用最新净值日的年份：代销池里有已清算/停更的产品，快照停在往年，
 * 按数据年算会把旧数据标成「今年来」——那是粉饰。按系统年算，这类产品只会得到空图。
 */
function ytdFrom() {
  return `${new Date().getFullYear()}-01-01`
}

async function loadNav(code: string) {
  navLoading.value = true
  try {
    const resp = navRange.value === 'ytd'
      ? await fetchPrivateNav(code, { from: ytdFrom(), order: 'asc' })
      : await fetchPrivateNav(code, { days: navRange.value as number, order: 'asc' })
    navPoints.value = resp.rows
  } catch {
    navPoints.value = []
  } finally {
    navLoading.value = false
  }
}

const emptyNavHint = computed(() => {
  if (!navPoints.value.length) {
    if (!detail.value?.hasNav) {
      return '该产品不在天天基金代销池、也不在私募排排网可见名录内，无公开净值（备案名录仍可查）'
    }
    if (navRange.value === 'ytd') {
      const y = ytdFrom().slice(0, 4)
      const d = detail.value?.latestNavDate
      return d ? `该产品 ${y} 年暂无净值（最新数据为 ${d.slice(0, 10)}）` : `该产品 ${y} 年暂无净值数据`
    }
  }
  return ''
})

// ── 图表 ────────────────────────────────────────────────────────────────────
const chartOption = computed(() => {
  const labels = navPoints.value.map(p => p.navDate)
  // 排排网源只有一列「复权净值（分红再投）」，不存在单位/累计之分；
  // 代销池源才有 单位净值 / 累计净值 两列。标签必须跟着源走，否则等于给复权值挂「单位净值」的名。
  const sppw   = isSppwSource.value
  const unit   = navPoints.value.map(p => (sppw ? p.accNav : p.unitNav))
  const acc    = navPoints.value.map(p => p.accNav)

  // 累计净值与单位净值完全重合时只画一条，避免两条线叠在一起看不出区别
  const sameSeries = sppw || acc.every((v, i) => v === unit[i])

  const series: any[] = [{
    name: sppw ? '复权净值(分红再投)' : '单位净值',
    type: 'line',
    data: unit,
    smooth: false,
    showSymbol: navPoints.value.length <= 120,
    symbolSize: 3,
    lineStyle: { width: 1.5, color: '#409eff' },
    itemStyle: { color: '#409eff' },
    connectNulls: false,
  }]
  if (!sameSeries) {
    series.push({
      name: '累计净值',
      type: 'line',
      data: acc,
      smooth: false,
      showSymbol: false,
      lineStyle: { width: 1.2, color: '#e6a23c' },
      itemStyle: { color: '#e6a23c' },
      connectNulls: false,
    })
  }

  return {
    tooltip: {
      trigger: 'axis',
      valueFormatter: (v: number | null) => (v == null ? '—' : v.toFixed(4)),
    },
    legend: { data: series.map(s => s.name), right: 10, top: 0, textStyle: { fontSize: 12 } },
    grid: { left: 56, right: 24, top: 36, bottom: 56 },
    xAxis: {
      type: 'category',
      data: labels,
      boundaryGap: false,
      axisLabel: { fontSize: 11, color: '#888' },
      axisLine: { lineStyle: { color: '#e5e5e5' } },
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLabel: { fontSize: 11, color: '#888', formatter: (v: number) => v.toFixed(3) },
      splitLine: { lineStyle: { color: '#f0f0f0' } },
    },
    dataZoom: [
      { type: 'inside' },
      { type: 'slider', height: 18, bottom: 12 },
    ],
    series,
  }
})

// ── 净值明细表（desc，分页） ────────────────────────────────────────────────
const tableRows  = ref<PrivateNavPoint[]>([])
const tableTotal = ref(0)
const tableLoading = ref(false)
const tablePage = ref(1)
const tablePageSize = ref(50)

async function loadTable() {
  const code = route.params.code as string
  if (!code) return
  tableLoading.value = true
  try {
    const resp = await fetchPrivateNav(code, {
      days: 0,
      order: 'desc',
      limit: tablePageSize.value,
      offset: (tablePage.value - 1) * tablePageSize.value,
    })
    tableRows.value  = resp.rows
    tableTotal.value = resp.total
  } catch {
    tableRows.value = []
    tableTotal.value = 0
  } finally {
    tableLoading.value = false
  }
}

// ── 加载 ────────────────────────────────────────────────────────────────────
// 后端对不存在的编码返回 200 + { found:false, code }（软失败，不是 404）。
// 若不显式分支，页面会渲染成一张全 "—" 的空白页，看不出是「没这只产品」还是「加载失败」。
const notFound = ref(false)

async function loadAll() {
  const code = route.params.code as string
  if (!code) return
  loading.value = true
  error.value = ''
  notFound.value = false
  try {
    const d = await fetchPrivateFundDetail(code)
    if ((d as { found?: boolean }).found === false) {
      notFound.value = true
      detail.value = null
      navPoints.value = []
      tableRows.value = []
      tableTotal.value = 0
      return
    }
    detail.value = d
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
  } finally {
    loading.value = false
  }
  await Promise.all([loadNav(code), loadTable()])
}

onMounted(loadAll)
watch(() => route.params.code, () => {
  navRange.value = 'ytd'
  tablePage.value = 1
  loadAll()
})
watch(navRange, () => {
  const code = route.params.code as string
  if (code) loadNav(code)
})
watch(tablePage, loadTable)

// ── Formatters ──────────────────────────────────────────────────────────────
function fmtDate(d: string | null | undefined) { return d ? d.slice(0, 10) : '—' }

function fmtNav(v: number | null | undefined) {
  return v == null ? '—'
    : v.toLocaleString('zh-CN', { minimumFractionDigits: 4, maximumFractionDigits: 4 })
}

function fmtPct(v: number | null | undefined) {
  if (v == null) return '—'
  return (v > 0 ? '+' : '') + v.toFixed(2) + '%'
}

function fmtSize(v: number | null | undefined) {
  if (v == null) return '—'
  const yi = v / 1e8
  if (yi >= 1) return yi.toFixed(2) + ' 亿元'
  if (yi >= 0.01) return yi.toFixed(3) + ' 亿元'
  return Math.round(v).toLocaleString('zh-CN') + ' 元'
}

function pctClass(v: number | null | undefined) {
  return v == null ? '' : v > 0 ? 'up' : v < 0 ? 'down' : ''
}

function stateTagType(s: string | null | undefined): 'success' | 'warning' | 'info' {
  if (!s) return 'info'
  if (s.includes('正在运作')) return 'success'
  if (s.includes('清算'))    return 'warning'
  return 'info'
}
</script>

<template>
  <div class="pfd-page">
    <el-alert v-if="error" :title="error" type="error" show-icon
              :closable="false" style="margin-bottom: 20px" />

    <!-- 后端对不存在的备案编码返回 found:false（不是 404）。不显式处理会渲染出一张
         全 “—” 的空白页，与「加载失败」无法区分。 -->
    <el-result v-if="notFound" icon="warning" title="未找到该产品"
               :sub-title="`备案编码 ${route.params.code} 不在库中。可能是备案编码写错，或该产品尚未被采集（名录覆盖率见列表页说明）。`">
      <template #extra>
        <el-button type="primary" @click="router.push('/private-funds')">返回私募列表</el-button>
      </template>
    </el-result>


    <div v-loading="loading" v-show="!notFound">
      <!-- ── 头部 ──────────────────────────────────────────────────────────── -->
      <div class="page-header">
        <div class="header-main">
          <el-button link @click="router.push('/private-funds')" class="back">
            ← 返回列表
          </el-button>
          <h1>{{ detail?.fundName ?? '—' }}</h1>
          <div class="meta-line">
            <span class="code-chip">{{ detail?.fundNo }}</span>
            <el-tag v-if="detail?.workingState" size="small" :type="stateTagType(detail?.workingState)" effect="light">
              {{ detail?.workingState }}
            </el-tag>
            <el-tag v-if="detail?.hasNav" size="small" type="primary" effect="plain">
              {{ detail?.navSource === 'sppw' ? '有排排网净值(复权)' : '有代销池净值' }}
            </el-tag>
            <el-tag v-else size="small" type="info" effect="plain">无公开净值</el-tag>
          </div>
        </div>
      </div>

      <!-- ── 收益率 ────────────────────────────────────────────────────────── -->
      <div class="stat-grid">
        <div class="stat-card">
          <div class="stat-label">今年来</div>
          <div class="stat-value" :class="pctClass(detail?.ytdReturn)">{{ fmtPct(detail?.ytdReturn) }}</div>
          <div class="stat-sub">{{ isSppwSource ? '复权净值口径' : '累计净值口径' }}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">有数据以来</div>
          <div class="stat-value" :class="pctClass(detail?.totalReturn)">{{ fmtPct(detail?.totalReturn) }}</div>
          <div class="stat-sub">自 {{ fmtDate(detail?.navEarliest) }}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">{{ isSppwSource ? '最新复权净值' : '最新单位净值' }}</div>
          <div class="stat-value">{{ fmtNav(detail?.latestNav) }}</div>
          <div class="stat-sub">{{ fmtDate(detail?.latestNavDate) }}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">规模</div>
          <div class="stat-value size-v">{{ fmtSize(detail?.latestFundSize) }}</div>
          <div class="stat-sub">源侧多数产品不披露</div>
        </div>
      </div>

      <!-- ── 基础信息 ──────────────────────────────────────────────────────── -->
      <div class="info-card">
        <div class="info-grid">
          <div class="info-item">
            <span class="info-label">管理人</span>
            <span class="info-value">{{ detail?.managerName ?? '—' }}</span>
          </div>
          <div class="info-item">
            <span class="info-label">管理类型</span>
            <span class="info-value">{{ detail?.managerType ?? '—' }}</span>
          </div>
          <div class="info-item">
            <span class="info-label">托管人</span>
            <span class="info-value">{{ detail?.mandatorName ?? '—' }}</span>
          </div>
          <div class="info-item">
            <span class="info-label">成立时间</span>
            <span class="info-value">{{ fmtDate(detail?.establishDate) }}</span>
          </div>
          <div class="info-item">
            <span class="info-label">备案时间</span>
            <span class="info-value">{{ fmtDate(detail?.recordDate) }}</span>
          </div>
          <div class="info-item">
            <span class="info-label">净值点数</span>
            <span class="info-value">
              {{ detail?.navPointCount ? detail.navPointCount.toLocaleString() : '—' }}
            </span>
          </div>
        </div>
      </div>

      <!-- ── 净值走势 ──────────────────────────────────────────────────────── -->
      <div class="section-header">
        <h2>净值走势</h2>
        <el-radio-group v-model="navRange" size="small">
          <el-radio-button v-for="o in rangeOptions" :key="String(o.value)" :value="o.value">
            {{ o.label }}
          </el-radio-button>
        </el-radio-group>
      </div>

      <div class="chart-card" v-loading="navLoading">
        <AnnotatedLineChart v-if="navPoints.length"
                            :option="chartOption" autoresize
                            style="width: 100%; height: 380px" />
        <div v-else class="empty-hint">{{ emptyNavHint || '暂无净值数据' }}</div>
      </div>
      <p v-if="navPoints.length" class="chart-foot">
        净值区间 {{ navPoints[0]?.navDate }} ~ {{ navPoints[navPoints.length - 1]?.navDate }}，
        共 {{ navPoints.length }} 个点（私募披露频率低于公募，日频/周频混合）
      </p>

      <!-- ── 净值明细 ──────────────────────────────────────────────────────── -->
      <div class="section-header"><h2>净值明细</h2></div>
      <el-table :data="tableRows" v-loading="tableLoading" style="width: 100%"
                class="pf-table"
                :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }">
        <el-table-column prop="navDate" label="净值日期" width="140" />
        <!-- 排排网源只有一列复权净值（unit_nav 为空），不渲染「单位净值」空列 -->
        <el-table-column v-if="!isSppwSource" label="单位净值" align="right" width="150">
          <template #default="{ row }"><span class="num">{{ fmtNav(row.unitNav) }}</span></template>
        </el-table-column>
        <el-table-column :label="isSppwSource ? '复权净值' : '累计净值'" align="right" width="150">
          <template #default="{ row }"><span class="num">{{ fmtNav(row.accNav) }}</span></template>
        </el-table-column>
        <el-table-column label="日涨跌" align="right">
          <template #default="{ row }">
            <span v-if="row.dailyReturn != null" :class="pctClass(row.dailyReturn)">
              {{ fmtPct(row.dailyReturn) }}
            </span>
            <span v-else class="dash">—</span>
          </template>
        </el-table-column>
      </el-table>

      <div class="pagination-wrap">
        <el-pagination v-model:current-page="tablePage" :page-size="tablePageSize"
                       :page-sizes="[20, 50, 100, 200]" :total="tableTotal"
                       layout="total, sizes, prev, pager, next, jumper"
                       @size-change="(s: number) => { tablePageSize = s; tablePage = 1; loadTable() }" />
      </div>

      <!-- ── 口径说明 ──────────────────────────────────────────────────────── -->
      <div class="scope-note">
        <strong>口径说明</strong>
        <ul>
          <li v-if="isSppwSource">
            本产品净值来自<strong>私募排排网可见名录</strong>，是<strong>复权净值（分红再投）</strong>口径；
            该名录全市场仅约 5,565 只，即公开渠道能拿到净值的<strong>全部天花板</strong>
            （占备案名录 2% 量级）—— <strong>不代表全市场私募业绩</strong>。
          </li>
          <li v-else>
            本产品净值来自<strong>天天基金「高端理财」代销池</strong>，本列为<strong>累计净值</strong>口径（单位净值另列）；
            该池全市场仅约 1,208 只，池内主体是券商资管 / 集合资管计划 —— <strong>不代表全市场私募业绩</strong>。
          </li>
          <li>收益率以<strong>含分红口径净值</strong>为基准（排排网源=复权净值，代销池源=累计净值）；日涨跌由相邻两点自算。</li>
          <li>其余产品只有中基协<strong>备案级</strong>信息（名称 / 管理人 / 托管人 / 运行状态 / 备案时间）。</li>
          <li>
            按监管要求，私募基金<strong>不得公开披露净值与持仓</strong>，因此不存在「全市场私募净值库」。
            ⚠ 两个源口径不同（复权净值 vs 累计净值），在「有分红」标的上终身收益最多差数倍，
            故<strong>同一产品只取单一来源、不跨源混拼序列</strong>。
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<style scoped>
.pfd-page { max-width: 1200px; margin: 0 auto; padding: 36px 20px 60px; }

.page-header { margin-bottom: 22px; }
.header-main h1 { margin: 6px 0 10px; font-size: 24px; font-weight: 700; color: #1a1a2e; line-height: 1.35; }
.back { padding: 0; color: #888; }
.back:hover { color: #409eff; }

.meta-line { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.code-chip {
  font-variant-numeric: tabular-nums;
  background: #f2f3f5;
  color: #555;
  border-radius: 4px;
  padding: 2px 8px;
  font-size: 13px;
  font-weight: 600;
}

/* ── 统计卡 ─────────────────────────────────────────────────────────────── */
.stat-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  gap: 12px;
  margin-bottom: 20px;
}
.stat-card { background: #f7f8fa; border-radius: 10px; padding: 14px 16px; }
.stat-label { font-size: 13px; color: #888; margin-bottom: 6px; }
.stat-value {
  font-size: 24px; font-weight: 500; color: #1a1a2e;
  font-variant-numeric: tabular-nums;
}
.stat-value.size-v { font-size: 20px; }
.stat-sub { font-size: 12px; color: #aaa; margin-top: 2px; }

.up   { color: #e8534a; }
.down { color: #26a17b; }

/* ── 基础信息 ───────────────────────────────────────────────────────────── */
.info-card {
  background: #fff;
  border: 1px solid #eef0f2;
  border-radius: 12px;
  padding: 18px 20px;
  margin-bottom: 26px;
}
.info-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 14px 24px;
}
.info-item { display: flex; flex-direction: column; gap: 4px; }
.info-label { font-size: 12px; color: #999; }
.info-value { font-size: 14px; color: #1a1a2e; font-weight: 500; word-break: break-all; }

/* ── 区块 ───────────────────────────────────────────────────────────────── */
.section-header {
  display: flex; align-items: center; justify-content: space-between;
  margin: 0 0 14px;
}
.section-header h2 { margin: 0; font-size: 17px; font-weight: 600; color: #1a1a2e; }

.chart-card {
  background: #fff;
  border: 1px solid #eef0f2;
  border-radius: 12px;
  padding: 12px 8px 4px;
  margin-bottom: 8px;
  min-height: 200px;
}
.empty-hint {
  height: 380px;
  display: flex; align-items: center; justify-content: center;
  color: #bbb; font-size: 14px;
}
.chart-foot { margin: 0 0 26px; font-size: 12px; color: #999; }

.pf-table {
  background: #fff; border-radius: 12px; overflow: hidden;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}
.num { font-variant-numeric: tabular-nums; font-weight: 600; color: #1a1a2e; }
.dash { color: #ccc; }

.pagination-wrap { display: flex; justify-content: flex-end; margin-top: 16px; }

/* ── 口径说明 ───────────────────────────────────────────────────────────── */
.scope-note {
  margin-top: 28px;
  background: #fff8e6;
  border: 1px solid #f5d99b;
  border-radius: 10px;
  padding: 12px 18px;
  font-size: 13px;
  color: #6b5316;
  line-height: 1.8;
}
.scope-note strong { color: #8a6100; }
.scope-note ul { margin: 6px 0 0; padding-left: 18px; }
.scope-note li { margin-bottom: 3px; }
</style>
