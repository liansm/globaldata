<script setup lang="ts">
/**
 * CryptoDetail — 单币走势详情
 *
 * ⚠ 这个页面是**补建**的：`/api/crypto/:key` 接口和 `fetchCryptoDetail` 一直存在，
 *   但列表页 `Crypto.vue` 的卡片既没有 @click 也没有对应路由，所以「点了没反应」。
 *   不是没历史数据 —— 库里每个币都有 4 个月左右的日线（见页面底部「数据密度」说明）。
 *
 * 口径要点：
 *   · `close` 是**日线收盘**：近一年由脚本用 CoinGecko `market_chart/range` 直接回补
 *     （UTC 00:00 的日点），当天那一点之后会被实时快照覆盖。仍**没有** open /
 *     逐日 high/low；`high24h`/`low24h` 是「最近 24 小时」的滚动高低，不是当日区间，
 *     别当 K 线画。
 *   · `changePct` 是源站给的 24h 涨跌幅，只有**当天**那行有（历史接口不提供），
 *     所以老数据这一列为空属正常，不要补成 0。
 *   · 断档：回补后近一年应为连续日更；万一仍有缺口，图里**显式断线**而不是把两点
 *     连成一条直线 —— 连起来会被读成「这段时间持续阴跌」。断档位置动态计算，不写死。
 */
import { ref, computed, watch, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { use } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { TooltipComponent, GridComponent, DataZoomComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { fetchCryptoDetail } from '@/api/crypto'
import { mtdFrom, wtdFrom } from '@/lib/dateRange'
import AnnotatedLineChart from '@/components/AnnotatedLineChart.vue'
import type { CryptoDetail } from '@/types/crypto'

use([CanvasRenderer, LineChart, TooltipComponent, GridComponent, DataZoomComponent])

const route  = useRoute()
const router = useRouter()

const key     = computed(() => route.params.key as string)
const loading = ref(false)
const error   = ref('')
const detail  = ref<CryptoDetail | null>(null)

// 源站只做日更，最长深度就是「全部」；接口 days 上限 365，够覆盖 2026-04 起的全部
// 'wtd' = 本周一至今（自然周），'mtd' = 本月 1 号至今（自然月）。
// 两者都和「近 N 月」那种滚动窗口不同族：加密货币 24/7 撮合，周六看本周是 6 个交易点、不是 5 个；
// 而月初 3 号选「本月」只有 3 天，选「近 1 月」仍是整月。
type RangeKey = 'wtd' | 'mtd' | 30 | 90 | 180 | 365
const range = ref<RangeKey>(90)
const RANGE_OPTIONS: { label: string; value: RangeKey }[] = [
  { label: '本周',    value: 'wtd' },
  { label: '本月',    value: 'mtd' },
  { label: '近 1 月', value: 30 },
  { label: '近 3 月', value: 90 },
  { label: '近 6 月', value: 180 },
  { label: '全部',   value: 365 },
]

/** 相邻两点超过这个天数就当断档处理（日更品种，容忍周末/偶发漏采） */
const MAX_GAP_DAYS = 5

async function load() {
  loading.value = true
  error.value = ''
  try {
    const params = range.value === 'wtd' ? { from: wtdFrom() }
      : range.value === 'mtd' ? { from: mtdFrom() }
      : { days: range.value as number }
    detail.value = await fetchCryptoDetail(key.value, params)
  } catch (e: any) {
    error.value = e?.response?.status === 404
      ? `未找到币种「${key.value}」`
      : '加载失败，请检查后端服务（localhost:3000）是否启动'
    detail.value = null
  } finally {
    loading.value = false
  }
}

onMounted(load)
watch(key, () => { range.value = 90; load() })
watch(range, load)

// ── 格式化 ──────────────────────────────────────────────────────────────────
function fmtPrice(v: number | null, d?: number): string {
  if (v == null) return '—'
  if (d != null) {
    return v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d })
  }
  if (v >= 10_000) return v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  if (v >= 1)      return v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })
  return v.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 6 })
}
function fmtPct(v: number | null): string {
  if (v == null) return '—'
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`
}
function fmtVolume(v: number | null): string {
  if (v == null) return '—'
  if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B'
  if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M'
  if (v >= 1e3) return (v / 1e3).toFixed(2) + 'K'
  return v.toFixed(2)
}
function signClass(v: number | null): string {
  if (v == null || v === 0) return 'flat'
  return v > 0 ? 'up' : 'down'
}

/** 小数位跟随量级：BTC 0 位足够，小额币需要 4~6 位 */
const digits = computed(() => {
  const v = latest.value?.close
  if (v == null) return 2
  return Math.abs(v) >= 1000 ? 0 : Math.abs(v) >= 1 ? 2 : 4
})

// ── 数据 ────────────────────────────────────────────────────────────────────
/** 接口按日期倒序返回；图表要正序 */
const ascending = computed(() => [...(detail.value?.history ?? [])].reverse())

const latest = computed(() => detail.value?.history?.[0] ?? null)

/** 当前区间首末两点之比 —— 与列表页「24h 涨跌」是两个不同口径，标签分开写 */
const rangeStats = computed(() => {
  const rows = ascending.value
  if (rows.length < 2) return null
  const first = rows[0], last = rows[rows.length - 1]
  const closes = rows.map(r => r.close).filter((v): v is number => v != null)
  if (!closes.length) return null
  const hi = Math.max(...closes), lo = Math.min(...closes)
  return {
    pct:     first.close && last.close ? ((last.close - first.close) / first.close) * 100 : null,
    max:     hi,
    min:     lo,
    maxDate: rows.find(r => r.close === hi)?.date ?? null,
    minDate: rows.find(r => r.close === lo)?.date ?? null,
    from:    first.date,
    to:      last.date,
    points:  rows.length,
  }
})

/** 区间内的断档（相邻两点 > MAX_GAP_DAYS），用于图表断线与页面提示 */
const gaps = computed(() => {
  const rows = ascending.value
  const out: { after: string; before: string; days: number }[] = []
  for (let i = 1; i < rows.length; i++) {
    const d = (Date.parse(rows[i].date) - Date.parse(rows[i - 1].date)) / 86400_000
    if (d > MAX_GAP_DAYS) out.push({ after: rows[i - 1].date, before: rows[i].date, days: Math.round(d) })
  }
  return out
})

/**
 * 图表序列：在断档处插一个 null 点，让折线**断开**。
 * 直接连起来会把「78 天没采集」画成「78 天持续阴跌」，是这类图最容易骗人的地方。
 */
const chartSeries = computed(() => {
  const rows = ascending.value
  const xs: string[] = []
  const ys: (number | null)[] = []
  for (let i = 0; i < rows.length; i++) {
    if (i > 0) {
      const gapDays = (Date.parse(rows[i].date) - Date.parse(rows[i - 1].date)) / 86400_000
      if (gapDays > MAX_GAP_DAYS) {
        const mid = new Date((Date.parse(rows[i - 1].date) + Date.parse(rows[i].date)) / 2)
        xs.push(mid.toISOString().slice(0, 10))
        ys.push(null)
      }
    }
    xs.push(rows[i].date)
    ys.push(rows[i].close)
  }
  return { xs, ys }
})

// ── 图表 ────────────────────────────────────────────────────────────────────
const chartOption = computed(() => {
  const { xs, ys } = chartSeries.value
  if (!xs.length) return {}
  const valid = ys.filter((v): v is number => v != null)
  if (!valid.length) return {}

  const minVal = Math.min(...valid)
  const maxVal = Math.max(...valid)
  const pad    = (maxVal - minVal) * 0.08 || Math.abs(maxVal) * 0.002 || 1

  // 涨红跌绿（A 股 / 国内习惯），跟随区间方向而不是当日方向
  const pct = rangeStats.value?.pct
  const rgb = pct == null || pct >= 0 ? '232,83,74' : '26,161,123'

  // 54 个点塞进 400px，全显示标签会糊；按「最多 8 个」抽稀
  const step = Math.max(1, Math.ceil(xs.length / 8))

  const byDate = new Map(ascending.value.map(r => [r.date, r]))

  return {
    tooltip: {
      trigger: 'axis',
      formatter: (params: any[]) => {
        const p = params[0]
        if (!p || p.value == null) return ''       // 断档占位点不弹提示
        const row = byDate.get(p.axisValue)
        if (!row) return ''
        const chg = row.changePct
        return [
          `<span style="color:#8a94a6">${p.axisValue}</span>`,
          `<b>$${fmtPrice(row.close)}</b>`,
          chg == null ? '' : `<span style="color:${chg >= 0 ? '#e8534a' : '#26a17b'}">24h ${fmtPct(chg)}</span>`,
          row.volume24h == null ? '' : `<span style="color:#98a0b0">成交额 ${fmtVolume(row.volume24h)}</span>`,
        ].filter(Boolean).join('<br/>')
      },
    },
    grid: { top: 20, right: 30, bottom: 62, left: 76 },
    xAxis: {
      type: 'category',
      data: xs,
      boundaryGap: false,
      axisLabel: { rotate: 30, fontSize: 10.5, color: '#8a94a6', interval: step - 1 },
      axisLine: { lineStyle: { color: '#e4e7ee' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      scale: true,
      min: minVal - pad,
      max: maxVal + pad,
      splitNumber: 5,
      axisLabel: {
        fontSize: 10.5,
        color: '#8a94a6',
        formatter: (v: number) => (v >= 1000
          ? `${(v / 1000).toFixed(1)}k`
          : v.toFixed(digits.value)),
      },
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
      name: '收盘价',
      type: 'line',
      data: ys,
      smooth: false,
      connectNulls: false,          // 断档必须断开，不能连成直线
      // ⚠ 必须一直显示 symbol：断档两侧若各自只剩 1 个点（如 2026-04-14 孤点被
      //   两个 null 夹住），关掉 symbol 后这条线段无从画起、那个点会**彻底消失**，
      //   但「区间涨跌」还在拿它当首点 —— 图上看不到、数字却依赖它。
      showSymbol: true,
      symbolSize: 3.5,
      lineStyle: { width: 2, color: `rgb(${rgb})` },
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
  <div class="crypto-detail">
    <el-button link @click="router.push('/crypto')" class="back-btn">← 返回加密货币列表</el-button>

    <template v-if="detail">
      <div class="header">
        <div class="title-block">
          <h1>
            {{ detail.name }}
            <span class="symbol">{{ detail.symbol }}</span>
          </h1>
          <div class="meta-tags">
            <el-tag size="small" type="info">{{ detail.key.toUpperCase() }}</el-tag>
            <el-tag size="small">计价 {{ detail.unit }}</el-tag>
            <el-tag size="small" type="success">每日现货快照</el-tag>
          </div>
        </div>

        <div class="price-block">
          <div class="latest-price">
            {{ fmtPrice(latest?.close ?? null) }}
            <span class="unit">{{ detail.unit }}</span>
          </div>
          <div class="change" :class="signClass(latest?.changePct ?? null)">
            {{ fmtPct(latest?.changePct ?? null) }}
            <span class="change-label">24h 涨跌</span>
          </div>
          <div class="price-date">数据日期 {{ latest?.date ?? '—' }}</div>
        </div>
      </div>

      <!-- 区间切换 -->
      <div class="chart-toolbar">
        <el-radio-group v-model="range" size="small">
          <el-radio-button v-for="opt in RANGE_OPTIONS" :key="opt.value" :value="opt.value">
            {{ opt.label }}
          </el-radio-button>
        </el-radio-group>
        <span class="toolbar-hint">
          库内共 {{ detail.history.length }} 个报价日（接口按所选区间返回）
        </span>
      </div>

      <!-- 图表 -->
      <div class="chart-wrap" v-loading="loading">
        <AnnotatedLineChart
          v-if="detail.history.length > 1"
          :option="chartOption"
          :decimals="digits"
          autoresize
          style="width:100%;height:400px"
        />
        <div v-else class="no-data">
          该区间只有 {{ detail.history.length }} 个报价点，画不成曲线，请切到更长的区间
        </div>
      </div>

      <!-- 断档提示：只在真有大缺口时出现，不用固定文案吓人 -->
      <el-alert
        v-if="gaps.length"
        type="warning" :closable="false" show-icon class="gap-alert"
      >
        <template #title>
          该区间内有 {{ gaps.length }} 处采集断档（图中已断开，未连线）
        </template>
        <div class="gap-list">
          <span v-for="g in gaps.slice(0, 4)" :key="g.after + g.before">
            {{ g.after }} → {{ g.before }}（{{ g.days }} 天）
          </span>
          <span v-if="gaps.length > 4">…共 {{ gaps.length }} 处</span>
        </div>
      </el-alert>

      <!-- 区间统计 -->
      <div v-if="rangeStats" class="stat-row">
        <div class="stat-card">
          <span class="stat-k">区间涨跌</span>
          <span class="stat-v" :class="signClass(rangeStats.pct)">{{ fmtPct(rangeStats.pct) }}</span>
          <span class="stat-h">{{ rangeStats.from }} → {{ rangeStats.to }}</span>
        </div>
        <div class="stat-card">
          <span class="stat-k">区间最高</span>
          <span class="stat-v">{{ fmtPrice(rangeStats.max) }}</span>
          <span class="stat-h">{{ rangeStats.maxDate }}</span>
        </div>
        <div class="stat-card">
          <span class="stat-k">区间最低</span>
          <span class="stat-v">{{ fmtPrice(rangeStats.min) }}</span>
          <span class="stat-h">{{ rangeStats.minDate }}</span>
        </div>
        <div class="stat-card">
          <span class="stat-k">最新成交额</span>
          <span class="stat-v">{{ fmtVolume(latest?.volume24h ?? null) }}</span>
          <span class="stat-h">{{ detail.unit }} · 24h</span>
        </div>
      </div>

      <!-- 口径信息 -->
      <el-descriptions title="口径信息" :column="3" border size="small" class="desc-card">
        <el-descriptions-item label="数据来源">CoinGecko（api.coingecko.com）</el-descriptions-item>
        <el-descriptions-item label="字段口径">日收盘快照（非 K 线）</el-descriptions-item>
        <el-descriptions-item label="库内更新">
          {{ detail.updatedAt ? detail.updatedAt.slice(0, 10) : '—' }}
        </el-descriptions-item>
        <el-descriptions-item label="当前区间点数">{{ detail.history.length }} 个</el-descriptions-item>
        <el-descriptions-item label="区间起">{{ ascending[0]?.date ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="区间止">{{ latest?.date ?? '—' }}</el-descriptions-item>
      </el-descriptions>

      <p class="foot-note">
        ⚠ <b>这不是 K 线图</b>。源站只给「抓取时刻的现货价 + 最近 24h 高/低」，没有开盘价、
        也没有逐日 high/low 序列 —— 所以只能画收盘折线，<b>不拿 high24h/low24h 冒充当日区间</b>。<br>
        24h 涨跌幅取自源站字段，与「区间涨跌」（首末两个收盘价之比）是两个不同口径，标签已分开展示。<br>
        库内最早 <b>{{ ascending[0]?.date ?? '—' }}</b>，共 <b>{{ ascending.length }}</b> 个报价日。
        其中近 <b>一年</b>由源站日线接口<b>直接回补</b>（每次运行都会自愈近一年的漏洞，不再依赖每日积累）；
        更早的数据靠本站历次快照攒下 —— CoinGecko 免费档只认滚动 365 天，超出窗口的部分接口不提供。<br>
        若有断档，图中<b>显式断开</b>，不做前值填充、不连线，具体位置见上方提示条。
      </p>
    </template>

    <div v-else-if="loading" v-loading="true" style="height:340px" />
    <el-alert v-else-if="error" :title="error" type="error" show-icon :closable="false" />
  </div>
</template>

<style scoped>
.crypto-detail {
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
.symbol {
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
  margin-bottom: 10px;
  flex-wrap: wrap;
}
.toolbar-hint { font-size: 12px; color: #a8b0bb; margin-left: auto; }

.chart-wrap {
  background: #fff;
  border: 1px solid #eee;
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 16px;
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

.gap-alert { margin-bottom: 18px; }
.gap-list {
  display: flex;
  gap: 14px;
  flex-wrap: wrap;
  font-size: 12px;
  margin-top: 4px;
  font-variant-numeric: tabular-nums;
}

.stat-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
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

.foot-note {
  margin-top: 20px;
  font-size: 12px;
  line-height: 1.8;
  color: #98a2ae;
}

@media (max-width: 900px) {
  .stat-row { grid-template-columns: repeat(2, 1fr); }
}
</style>
