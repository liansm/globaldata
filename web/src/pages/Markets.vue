<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { fetchMarkets } from '@/api/markets'
import type { MarketIndex } from '@/types/market'

const router = useRouter()
const loading = ref(false)
const error   = ref('')
const list    = ref<MarketIndex[]>([])

onMounted(async () => {
  loading.value = true
  try {
    list.value = await fetchMarkets()
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
  } finally {
    loading.value = false
  }
})

const SECTIONS = [
  {
    title: 'A股',
    icon:  '🇨🇳',
    keys:  ['idx_sh', 'idx_sz', 'idx_cyb', 'idx_kc50', 'idx_bz50'],
  },
  {
    title: '港股',
    icon:  '🇭🇰',
    keys:  ['idx_hsi', 'idx_hscei'],
  },
  {
    title: '沪深港通资金流向',
    icon:  '💰',
    keys:  ['flow_north', 'flow_south'],
  },
  {
    title: '美股',
    icon:  '🇺🇸',
    keys:  ['idx_dji', 'idx_sp500', 'idx_nasdaq'],
  },
  {
    title: '亚太',
    icon:  '🌏',
    keys:  ['idx_nikkei', 'idx_kospi', 'idx_sensex', 'idx_sti', 'idx_vni'],
  },
  {
    title: '欧洲',
    icon:  '🇪🇺',
    keys:  ['idx_ftse', 'idx_dax', 'idx_cac40'],
  },
]

const sections = computed(() => {
  const map = new Map(list.value.map(m => [m.key, m]))
  return SECTIONS.map(s => ({
    ...s,
    items: s.keys.map(k => map.get(k)).filter(Boolean) as MarketIndex[],
  })).filter(s => s.items.length > 0)
})

function fmtPoint(v: number | null) {
  if (v == null) return '—'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
}

function fmtTurnover(v: number | null, market = '') {
  if (v == null) return null
  // 成交额单位是元（A股）或港元（港股），转换为亿元显示
  const yi = v / 1e8
  const unitLabel = market === '港股' ? '亿港元' : '亿'
  return yi >= 1
    ? yi.toLocaleString('zh-CN', { maximumFractionDigits: 0 }) + ' ' + unitLabel
    : v.toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}

function fmtDate(d: string | null) {
  return d ? d.slice(0, 10) : '—'
}

/** 分时时间戳 "YYYY-MM-DD HH:MM" → "HH:MM" */
function fmtMinuteTime(dt: string | null) {
  return dt ? dt.slice(11, 16) : null
}

// 资金流向正负颜色
function flowClass(v: number | null) {
  if (v == null) return ''
  return v >= 0 ? 'positive' : 'negative'
}

function fmtFlow(v: number | null) {
  if (v == null) return '—'
  const sign = v >= 0 ? '+' : ''
  return `${sign}${v.toLocaleString('zh-CN', { maximumFractionDigits: 2 })} 亿元`
}

function isFlow(item: MarketIndex) {
  return item.market === '资金流向'
}

function goDetail(item: MarketIndex) {
  router.push(`/market/${item.key}`)
}

function fmtChangePct(v: number | null | undefined) {
  if (v == null) return null
  const sign = v >= 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}%`
}

function changePctClass(v: number | null | undefined) {
  if (v == null) return ''
  return v >= 0 ? 'pct-up' : 'pct-down'
}
</script>

<template>
  <div class="markets-page">
    <div class="page-header">
      <div>
        <h1>全球股市</h1>
        <p class="subtitle">A股 · 港股 · 沪深港通资金流向</p>
      </div>
    </div>

    <el-alert
      v-if="error"
      :title="error"
      type="error"
      show-icon
      :closable="false"
      style="margin-bottom: 20px"
    />

    <div v-if="loading" class="skeleton-wrap">
      <el-skeleton :rows="6" animated />
    </div>

    <template v-else>
      <section v-for="sec in sections" :key="sec.title" class="section">
        <div class="section-header">
          <span class="section-icon">{{ sec.icon }}</span>
          <h2 class="section-title">{{ sec.title }}</h2>
          <span class="section-count">{{ sec.items.length }} 个</span>
        </div>

        <div class="card-grid">
          <div
            v-for="item in sec.items"
            :key="item.key"
            class="card clickable"
            @click="goDetail(item)"
          >
            <div class="card-top">
              <span class="card-name">{{ item.name }}</span>
              <el-tag size="small" type="info" class="card-tag">{{ item.key }}</el-tag>
            </div>

            <!-- 资金流向卡片 -->
            <template v-if="isFlow(item)">
              <div class="card-price" :class="flowClass(item.latestClose)">
                {{ fmtFlow(item.latestClose) }}
              </div>
              <div class="card-footer">
                <span class="card-date">{{ fmtDate(item.latestDate) }}</span>
                <span class="card-arrow">→</span>
              </div>
            </template>

            <!-- 指数卡片 -->
            <template v-else>
              <div class="card-price-row">
                <span class="card-price">
                  {{ fmtPoint(item.latestClose) }}
                  <span class="card-unit">{{ item.unit ?? '点' }}</span>
                </span>
                <span
                  v-if="fmtChangePct(item.changePct)"
                  class="card-pct"
                  :class="changePctClass(item.changePct)"
                >{{ fmtChangePct(item.changePct) }}</span>
              </div>
              <div v-if="fmtTurnover(item.latestTurnover, item.market)" class="card-turnover">
                成交额 {{ fmtTurnover(item.latestTurnover, item.market) }}
              </div>
            </template>

            <div class="card-footer">
              <span class="card-date">{{ fmtDate(item.latestDate) }}</span>
              <template v-if="item.latestMinuteDt">
                <span class="card-intraday-sep">·</span>
                <span class="card-intraday-time">{{ fmtMinuteTime(item.latestMinuteDt) }}</span>
                <span class="card-intraday-badge">分时</span>
              </template>
            </div>
          </div>
        </div>
      </section>
    </template>
  </div>
</template>

<style scoped>
.markets-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 36px 20px 60px;
}

.page-header {
  margin-bottom: 32px;
}

h1 {
  margin: 0 0 4px;
  font-size: 26px;
  font-weight: 700;
  color: #1a1a2e;
}

.subtitle {
  margin: 0;
  color: #888;
  font-size: 14px;
}

/* ── Section ──────────────────────────────────────────────────────────── */
.section {
  margin-bottom: 36px;
}

.section-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 14px;
}

.section-icon { font-size: 18px; line-height: 1; }

.section-title {
  margin: 0;
  font-size: 17px;
  font-weight: 600;
  color: #1a1a2e;
}

.section-count {
  margin-left: auto;
  font-size: 12px;
  color: #aaa;
}

/* ── Card grid ────────────────────────────────────────────────────────── */
.card-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 14px;
}

.card {
  background: #fff;
  border: 1px solid #e8e8f0;
  border-radius: 12px;
  padding: 16px 18px 14px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  transition: box-shadow 0.18s, transform 0.18s, border-color 0.18s;
}

.card.clickable {
  cursor: pointer;
}

.card.clickable:hover {
  box-shadow: 0 6px 20px rgba(0, 0, 0, 0.08);
  transform: translateY(-2px);
  border-color: #409eff;
}

.card-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
}

.card-name {
  font-size: 14px;
  font-weight: 600;
  color: #1a1a2e;
}

.card-tag { flex-shrink: 0; font-size: 11px; }

.card-price-row {
  display: flex;
  align-items: baseline;
  gap: 8px;
  flex-wrap: wrap;
}

.card-price {
  font-size: 22px;
  font-weight: 700;
  color: #1a1a2e;
  font-variant-numeric: tabular-nums;
  line-height: 1.15;
}

.card-pct {
  font-size: 13px;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}

.pct-up   { color: #e8534a; }   /* 红色 = 上涨（中国习惯） */
.pct-down { color: #4caf82; }   /* 绿色 = 下跌 */

.card-price.positive { color: #e8534a; }   /* 红色 = 流入（中国习惯） */
.card-price.negative { color: #4caf82; }   /* 绿色 = 流出 */

.card-unit {
  font-size: 12px;
  font-weight: 400;
  color: #888;
  margin-left: 3px;
}

.card-turnover {
  font-size: 12px;
  color: #999;
}

.card-footer {
  margin-top: auto;
}

.card-date {
  font-size: 12px;
  color: #bbb;
}

.card-intraday-sep {
  font-size: 12px;
  color: #ddd;
  margin: 0 3px;
}

.card-intraday-time {
  font-size: 12px;
  color: #bbb;
  font-variant-numeric: tabular-nums;
}

.card-intraday-badge {
  display: inline-block;
  margin-left: 4px;
  padding: 0 4px;
  font-size: 10px;
  line-height: 16px;
  border-radius: 4px;
  background: #e8f4ff;
  color: #409eff;
  font-weight: 500;
  vertical-align: middle;
}

/* ── Skeleton ─────────────────────────────────────────────────────────── */
.skeleton-wrap { padding: 16px 0; }
</style>
