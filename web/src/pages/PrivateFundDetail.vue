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
import { fetchPrivateFundDetail, fetchPrivateNav, fetchPrivateStats } from '@/api/privateFunds'
import AnnotatedLineChart from '@/components/AnnotatedLineChart.vue'
import { ytdFrom } from '@/lib/dateRange'
import type {
  PrivateFundDetail, PrivateNavPoint, PrivateNavRange, PrivateFundStats,
} from '@/types/privateFund'

use([CanvasRenderer, LineChart, TitleComponent, TooltipComponent,
     GridComponent, DataZoomComponent, LegendComponent])

const route  = useRoute()
const router = useRouter()

const loading = ref(true)
const error   = ref('')
const detail  = ref<PrivateFundDetail | null>(null)
// 无净值产品需要给出「全库有多少产品有净值」，否则用户会以为是自己这一只抓漏了。
const stats   = ref<PrivateFundStats | null>(null)
const navCoverageText = computed(() => {
  const s = stats.value
  if (!s || s.navCoverage == null) return ''
  return `全库 ${s.withNav.toLocaleString()} / ${s.funds.toLocaleString()} 只产品有公开净值（${s.navCoverage.toFixed(2)}%）`
})

// ── 净值 ────────────────────────────────────────────────────────────────────
const navPoints  = ref<PrivateNavPoint[]>([])
const navLoading = ref(false)
/** 当前区间的涨跌幅摘要。切换区间时立刻作废，避免「新标签配旧数字」。 */
const navSummary = ref<PrivateNavRange | null>(null)

/**
 * 该产品的净值是否来自私募排排网（复权净值口径）。
 * 决定图表线名与提示文案 —— 排排网那套是「分红再投复权」，把它标成「单位净值」是错的。
 * 同一 fund_no 只会有一个 source（写库时对重叠标的整只跳过，不混源），所以取首点的 source 即可。
 */
const isSppwSource = computed(() => navPoints.value.some(p => p.source === 'sppw'))

/**
 * 净值出处标签 —— 必须跟 source 走，不能笼统写「有代销池净值」。
 * em_gaoduan / licai 都是**累计净值**口径，sppw 是**复权净值**口径，三者不可混说。
 */
const navSourceLabel = computed(() => {
  switch (detail.value?.navSource) {
    case 'sppw':   return '有排排网净值(复权)'
    case 'licai':  return '有格上净值(累计)'
    case 'em_gaoduan': return '有代销池净值'
    default: return '有公开净值'
  }
})

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

const rangeLabel = computed(() => rangeOptions.find(o => o.value === navRange.value)?.label ?? '')
/** 涨跌幅用的那条序列：排排网源只有复权净值，代销池/格上源用累计净值。
 *  ⚠ 图上主蓝线（代销池源）画的是单位净值，涨跌幅不跟它算 —— 分红会让两者差很远，
 *    标签必须写清口径，否则「线跌了但涨跌幅是正的」会被当成 bug。 */
const rangeNavLabel = computed(() => detail.value?.navSource === 'sppw' ? '复权净值' : '累计净值')

async function loadNav(code: string) {
  navLoading.value = true
  navSummary.value = null   // 旧区间的涨跌幅立刻作废，别让「近 3 月」配「今年来」的数字
  try {
    const resp = navRange.value === 'ytd'
      ? await fetchPrivateNav(code, { from: ytdFrom(), order: 'desc', limit: 3000, withRange: true })
      : await fetchPrivateNav(code, { days: navRange.value as number, order: 'desc', limit: 3000, withRange: true })
    // ⚠ /nav 有 3000 点上限。原来 asc+limit 取的是**最早** 3000 点，库里有 7 只长历史
    //   产品（最多 3,733 点）会让图表末端停在几年前，而区间涨跌幅按完整序列的最新点算
    //   → 图与数字对不上。改成 desc 取最新 3000 点再反转成 asc，末点一定对齐。
    navPoints.value = resp.rows.slice().reverse()
    navSummary.value = resp.range ?? null
  } catch {
    navPoints.value = []
    navSummary.value = null
  } finally {
    navLoading.value = false
  }
}

const emptyNavHint = computed(() => {
  if (!navPoints.value.length) {
    if (!detail.value?.hasNav) {
      return '该产品不在天天基金代销池、私募排排网可见名录、格上理财可见名录内，无公开净值（备案名录仍可查）'
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
      navSummary.value = null
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

onMounted(() => {
  loadAll()
  // 非阻塞：拿不到覆盖率也只是少一行说明，不能挡住详情页
  fetchPrivateStats().then(s => { stats.value = s }).catch(() => {})
})
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
        <el-button type="primary" @click="router.push('/funds-info?tab=private')">返回私募列表</el-button>
      </template>
    </el-result>


    <div v-loading="loading" v-show="!notFound">
      <!-- ── 头部 ──────────────────────────────────────────────────────────── -->
      <div class="page-header">
        <div class="header-main">
          <el-button link @click="router.push('/funds-info?tab=private')" class="back">
            ← 返回列表
          </el-button>
          <h1>{{ detail?.fundName ?? '—' }}</h1>
          <div class="meta-line">
            <span class="code-chip">{{ detail?.fundNo }}</span>
            <el-tag v-if="detail?.workingState" size="small" :type="stateTagType(detail?.workingState)" effect="light">
              {{ detail?.workingState }}
            </el-tag>
            <el-tag v-if="detail?.hasNav" size="small" type="primary" effect="plain">
              {{ navSourceLabel }}
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
          <div class="stat-sub">
            {{ !detail?.hasNav ? '无公开净值' : (isSppwSource ? '复权净值口径' : '累计净值口径') }}
          </div>
        </div>
        <div class="stat-card">
          <div class="stat-label">有数据以来</div>
          <div class="stat-value" :class="pctClass(detail?.totalReturn)">{{ fmtPct(detail?.totalReturn) }}</div>
          <div class="stat-sub">
            {{ detail?.hasNav ? '自 ' + fmtDate(detail?.navEarliest) : '无公开净值' }}
          </div>
        </div>
        <div class="stat-card">
          <div class="stat-label">{{ isSppwSource ? '最新复权净值' : '最新单位净值' }}</div>
          <div class="stat-value">{{ fmtNav(detail?.latestNav) }}</div>
          <div class="stat-sub">{{ detail?.hasNav ? fmtDate(detail?.latestNavDate) : '未披露' }}</div>
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
            <span class="info-value">
              <!-- 能关联到 private_managers 才给链接；否则纯文本，避免点进「管理人不存在」 -->
              <router-link v-if="detail?.managerRegisterNo"
                           :to="{ path: `/private-manager/${detail.managerRegisterNo}`, query: { from: 'funds' } }"
                           class="mgr-link">
                {{ detail.managerName ?? '—' }}
              </router-link>
              <template v-else>{{ detail?.managerName ?? '—' }}</template>
            </span>
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
        <!-- 无净值时切区间也只会得到空白，反而让人以为「换个区间就有了」 -->
        <el-radio-group v-if="detail?.hasNav" v-model="navRange" size="small">
          <el-radio-button v-for="o in rangeOptions" :key="String(o.value)" :value="o.value">
            {{ o.label }}
          </el-radio-button>
        </el-radio-group>
      </div>

      <!-- 区间涨跌：随上面选的区间变，所以放在图上方而不是首屏统计卡。
           ⚠ 口径必须与首屏「今年来 / 有数据以来」一致（含分红口径净值 accNav、
           基期取区间起点前一披露日），否则同一个「今年来」会出现两个数字。 -->
      <div v-if="!navLoading && navSummary && navPoints.length" class="range-strip">
        <span class="rs-scope"
              :title="`净值区间 ${navSummary.from} ~ ${navSummary.to}（${navSummary.points} 个有效净值日）`">
          {{ rangeLabel }}<i>{{ navSummary.points }} 个披露日</i>
        </span>
        <span class="rs-cell">
          <span class="rs-k">区间涨跌<em class="stat-flag">{{ rangeNavLabel }}</em></span>
          <b class="rs-v" :class="pctClass(navSummary.returnPct)">{{ fmtPct(navSummary.returnPct) }}</b>
        </span>
        <span v-if="navSummary.baseNav != null" class="rs-cell"
              :title="`以 ${navSummary.baseDate} 的${rangeNavLabel} ${fmtNav(navSummary.baseNav)} 为基数，末值 ${fmtNav(navSummary.endNav)}`">
          <span class="rs-k">基数</span>
          <b class="rs-v">{{ fmtNav(navSummary.baseNav) }}</b>
          <i class="rs-d">{{ navSummary.baseDate }}</i>
        </span>
        <span class="rs-note">
          {{ navSummary.hasBaseline ? '含区间首日涨跌（基期取区间前一披露日）' : '以区间首个披露日为基期' }}
        </span>
        <!-- /nav 单次上限 3000 点；超出时图表只画最近 3000 点，但涨跌幅按完整区间算，必须说破 -->
        <span v-if="navSummary.points > navPoints.length" class="rs-note rs-warn">
          图表仅显示最近 {{ navPoints.length }} 个披露日，涨跌幅按完整区间 {{ navSummary.points }} 点计算
        </span>
      </div>

      <div class="chart-card" v-loading="navLoading">
        <AnnotatedLineChart v-if="navPoints.length"
                            :option="chartOption" autoresize
                            style="width: 100%; height: 380px" />
        <!-- 无净值 ≠ 加载失败：占满 380px 的空白 + 一行浅灰小字会被当成页面坏了。
             这里必须把「为什么没有」和「备案信息仍然完整」说清楚。 -->
        <div v-else-if="!navLoading && detail && !detail.hasNav" class="nav-empty">
          <div class="ne-icon"><el-icon :size="30"><Document /></el-icon></div>
          <h3>该产品暂无公开净值</h3>
          <p>按《私募证券投资基金运作指引》第六条，私募净值只能向合格投资者、基金评价机构
             及签约代销机构提供，第三方平台<strong>只能展示其代销的产品</strong>。本站只采集
             天天基金代销池、私募排排网可见名录、格上理财可见名录三处已公开的数值，
             本产品不在这三份名单内。</p>
          <p>因此净值、收益率和走势图都没有数据；<strong>上方的管理人、托管人、成立/备案时间等
             备案信息是完整的</strong>，这不是加载失败或抓取遗漏。</p>
          <p v-if="navCoverageText" class="ne-stat">
            {{ navCoverageText }}，这是公开渠道的上限；换付费数据库也不保证覆盖本产品，
            且净值展示另受监管与授权限制。
          </p>
        </div>
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
            该名录<strong>登录态全量重扫</strong>后为 5,498 只，即公开渠道能拿到净值的<strong>全部天花板</strong>
            （占备案名录 2% 量级）—— <strong>不代表全市场私募业绩</strong>。
          </li>
          <li v-else-if="detail?.navSource === 'licai'">
            本产品净值来自<strong>格上理财可见名录</strong>，是<strong>累计净值</strong>口径（单位净值另列）；
            该名录全市场仅约 927 只 —— <strong>不代表全市场私募业绩</strong>。
          </li>
          <li v-else>
            本产品净值来自<strong>天天基金「高端理财」代销池</strong>，本列为<strong>累计净值</strong>口径（单位净值另列）；
            该池全市场仅约 1,226 只，池内主体是券商资管 / 集合资管计划 —— <strong>不代表全市场私募业绩</strong>。
          </li>
          <li>收益率以<strong>含分红口径净值</strong>为基准（排排网源=复权净值，代销池/格上源=累计净值）；日涨跌由相邻两点自算。</li>
          <li v-if="isSppwSource">
            排排网名录里有 2,027 只产品<strong>不带备案号</strong>（形如「XX一号A类份额」）。
            这些已按<strong>母基金名唯一 + 首点不早于备案成立日</strong>两道闸门映射回备案主体，
            <strong>序列取自该份额本身</strong> —— 费率结构与备案主体可能略有差异，
            故同名产品的收益率与其他渠道口径不完全等值。
          </li>
          <li>其余产品只有中基协<strong>备案级</strong>信息（名称 / 管理人 / 托管人 / 运行状态 / 备案时间）。</li>
          <li>
            按《私募证券投资基金运作指引》第六条，私募净值只对合格投资者、基金评价机构及
            签约代销机构披露；管理人对评价机构也是<strong>自愿提供、可以不给</strong>，
            因此不存在「全市场私募净值库」，<strong>付费数据库同样如此</strong>。
            ⚠ 三个源口径不同（复权净值 vs 累计净值），在「有分红」标的上终身收益最多差数倍，
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
.mgr-link { color: #409eff; text-decoration: none; }
.mgr-link:hover { text-decoration: underline; }

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
/* 无公开净值的空状态：说清原因，别让 380px 空白看起来像加载失败 */
.nav-empty {
  min-height: 380px; padding: 28px 40px;
  display: flex; flex-direction: column; align-items: center; justify-content: center;
  text-align: center; gap: 10px;
}
.ne-icon {
  width: 58px; height: 58px; border-radius: 50%; margin-bottom: 4px;
  background: #f4f6fa; color: #a8b0c0;
  display: flex; align-items: center; justify-content: center;
}
.nav-empty h3 { margin: 0; font-size: 17px; font-weight: 600; color: #303133; }
.nav-empty p  { margin: 0; max-width: 660px; font-size: 13px; line-height: 1.85; color: #7a8291; }
.nav-empty strong { color: #4a5262; font-weight: 600; }
.nav-empty .ne-stat {
  margin-top: 6px; padding: 7px 14px; border-radius: 6px;
  background: #f7f9fc; color: #6b7688; font-size: 12px;
}
.chart-foot { margin: 0 0 26px; font-size: 12px; color: #999; }

.pf-table {
  background: #fff; border-radius: 12px; overflow: hidden;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}
.num { font-variant-numeric: tabular-nums; font-weight: 600; color: #1a1a2e; }
.dash { color: #ccc; }

.pagination-wrap { display: flex; justify-content: flex-end; margin-top: 16px; }

/* ── 区间涨跌条：一行横向排布，窄屏自动折行（与公募详情页同款） ───────── */
.range-strip {
  display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 20px;
  margin: 0 0 12px; padding: 9px 14px;
  background: #fafafc; border: 1px solid #f0f0f4; border-radius: 8px;
}
.rs-scope { font-size: 13px; font-weight: 600; color: #1a1a2e; }
.rs-scope i { margin-left: 6px; font-style: normal; font-weight: 400; font-size: 11px; color: #9a9aa8; }
.rs-cell { display: inline-flex; align-items: baseline; gap: 6px; }
.rs-k { font-size: 12px; color: #888; }
.rs-k .stat-flag {
  margin-left: 4px; font-style: normal; font-size: 11px;
  color: #409eff; background: #e8f3ff; border-radius: 3px; padding: 1px 5px;
}
.rs-v { font-size: 15px; font-weight: 600; font-variant-numeric: tabular-nums; color: #1a1a2e; }
.rs-v.up   { color: #e8534a; }   /* 涨红跌绿（中式约定） */
.rs-v.down { color: #26a17b; }
.rs-d { font-style: normal; font-size: 11px; color: #aaa; font-variant-numeric: tabular-nums; }
.rs-note { font-size: 11px; color: #aaa; }
.rs-warn { color: #b08900; }

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
