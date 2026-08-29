<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { fetchCommodities } from '@/api/commodities'
import { fetchMarkets } from '@/api/markets'
import type { Commodity } from '@/types/commodity'
import type { MarketIndex } from '@/types/market'

const router = useRouter()
const loading = ref(false)
const error = ref('')
const list = ref<Commodity[]>([])
// 航运运价指数（BDI / BCI / BSI / BCTI / BDTI / CCFI 综合 + 12 条分航线），来自 /api/markets (market='航运')
const shipping = ref<MarketIndex[]>([])

onMounted(async () => {
  loading.value = true
  try {
    const [commodities] = await Promise.all([fetchCommodities()])
    list.value = commodities
    // 航运数据拉取失败不应阻断大宗商品页面，单独容错
    fetchMarkets()
      .then(rows => { shipping.value = rows.filter(r => r.market === '航运') })
      .catch(() => { /* 航运 section 静默不显示 */ })
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
  } finally {
    loading.value = false
  }
})

// 航运数据拆分为两个 section：CCFI 出口集装箱运价 + BDI 波罗的海/油轮运价
const CCFI_ORDER = [
  'ccfi_total', 'ccfi_europe', 'ccfi_med', 'ccfi_wc_america', 'ccfi_ec_america',
  'ccfi_japan', 'ccfi_korea', 'ccfi_se_asia', 'ccfi_anz',
  'ccfi_south_africa', 'ccfi_south_america', 'ccfi_we_africa', 'ccfi_persian_gulf',
]
const BDI_ORDER = ['bdi', 'bci', 'bsi', 'bcti', 'bdti']

const shippingSections = computed(() => {
  const map = new Map(shipping.value.map(s => [s.key, s]))
  return [
    { title: 'CCFI 出口集装箱运价', icon: '🚢', keys: CCFI_ORDER },
    { title: 'BDI 波罗的海 / 油轮运价', icon: '⚓', keys: BDI_ORDER },
  ].map(s => ({
    ...s,
    items: s.keys.map(k => map.get(k)).filter(Boolean) as MarketIndex[],
  })).filter(s => s.items.length > 0)
})

// Section definitions — order matters, first match wins
const SECTIONS = [
  {
    title: '贵金属',
    icon: '🥇',
    keys: ['gold', 'silver', 'intl_gold', 'intl_silver', 'lme_gold', 'lme_silver'],
  },
  {
    title: '能源',
    icon: '⛽',
    keys: ['intl_oil_wti', 'intl_oil_brent', 'intl_gas', 'gold_oil_ratio',
           '__row_break__',
           'coal_port_5500', 'coal_port_5000', 'coal_port_4500',
           '__row_break__',
           'lithium_carbonate', 'polysilicon'],
  },
  {
    title: '有色金属',
    icon: '⚙️',
    keys: [
      'copper',   'lme_copper',
      'aluminum', 'intl_alum',
      'zinc',     'lme_zinc',
      'lead',     'lme_lead',
      'tin',      'lme_tin',
      'nickel',   'lme_nickel',
    ],
  },
  {
    title: '化工品',
    icon: '🧪',
    keys: ['methanol', 'urea', 'meg', 'styrene', 'polypropylene', 'natural_rubber'],
  },
  {
    title: '农产品',
    icon: '🌾',
    keys: ['corn', 'soybean_oil', 'eggs', 'live_hog', 'cotton', 'sugar'],
  },
]

const sections = computed(() => {
  const map = new Map(list.value.map(c => [c.key, c]))
  return SECTIONS.map(s => ({
    ...s,
    items: s.keys
      .map(k => k === '__row_break__' ? { key: '__row_break__' } as Commodity : map.get(k))
      .filter(Boolean) as Commodity[],
  })).filter(s => s.items.length > 0)
})

function fmt(price: number | null) {
  if (price == null) return '—'
  return price.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
}

function fmtDate(d: string | null) {
  return d ? d.slice(0, 10) : '—'
}

/** 实时更新时间 "YYYY-MM-DD HH:MM" → "HH:MM" */
function fmtSpotTime(dt: string | null) {
  return dt ? dt.slice(11, 16) : null
}

function changePctClass(v: number | null | undefined) {
  if (v == null) return ''
  return v >= 0 ? 'up' : 'down'
}

function fmtChangePct(v: number | null | undefined) {
  if (v == null) return null
  const sign = v >= 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}%`
}

function goDetail(row: Commodity) {
  router.push({ name: 'detail', params: { key: row.key } })
}

// 航运运价指数走 /market/:key 详情页（与 Markets.vue 一致）
function goMarketDetail(row: MarketIndex) {
  router.push({ name: 'market-detail', params: { key: row.key } })
}

// Derive display name: strip exchange/symbol suffix for cleaner labels
function displayName(c: Commodity) {
  return c.commodity.replace(/\s*[（(][^)）]+[)）]/, '').trim() || c.commodity
}

// Extract exchange label from commodity name parentheses, or infer from key
function exchangeLabel(c: Commodity): string | null {
  const match = c.commodity.match(/[（(]([^)）]+)[)）]/)
  if (match) {
    // "COMEX GC" → "COMEX", "LME AHD" → "LME", "上期所 CU" → "上期所"
    return match[1].trim().split(/\s+/)[0]
  }
  if (c.key === 'gold' || c.key === 'silver') return 'SGE'
  return null
}

function exchangeTagType(label: string | null) {
  if (!label) return 'info'
  if (label === 'SGE') return 'warning'
  if (label === 'COMEX' || label === 'LME' || label === 'NYMEX' || label === 'ICE') return 'danger'
  return 'info'
}
</script>

<template>
  <div class="home-page">
    <div class="page-header">
      <div>
        <h1>全球大宗商品价格</h1>
        <p class="subtitle">实时追踪主要商品现货 &amp; 期货行情 · 航运运价（BDI / CCFI）</p>
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
      <section
        v-for="sec in sections"
        :key="sec.title"
        class="section"
      >
        <div class="section-header">
          <span class="section-icon">{{ sec.icon }}</span>
          <h2 class="section-title">{{ sec.title }}</h2>
          <span class="section-count">{{ sec.items.filter(i => i.key !== '__row_break__').length }} 个品种</span>
        </div>

        <div class="card-grid">
          <template v-for="item in sec.items" :key="item.key">
            <div v-if="item.key === '__row_break__'" class="grid-row-break" />
            <div
              v-else
              class="card"
              @click="goDetail(item)"
            >
              <div class="card-top">
                <span class="card-name">{{ displayName(item) }}</span>
                <el-tag
                  v-if="exchangeLabel(item)"
                  size="small"
                  class="card-tag"
                  :type="exchangeTagType(exchangeLabel(item))"
                >{{ exchangeLabel(item) }}</el-tag>
              </div>

              <div class="card-price">
                {{ fmt(item.latestPrice) }}
                <span class="card-unit">{{ item.unit ?? '' }}</span>
              </div>

              <!-- 涨跌幅（仅实时数据有） -->
              <div
                v-if="item.spotChangePct != null"
                class="card-change"
                :class="changePctClass(item.spotChangePct)"
              >
                {{ fmtChangePct(item.spotChangePct) }}
              </div>

              <div class="card-footer">
                <span class="card-date">{{ fmtDate(item.latestDate) }}</span>
                <template v-if="item.spotUpdatedAt">
                  <span class="card-sep">·</span>
                  <span class="card-spot-time">{{ fmtSpotTime(item.spotUpdatedAt) }}</span>
                  <span class="card-spot-badge">实时</span>
                </template>
                <span v-if="item.hasMinutes" class="card-minutes-icon" title="有分时图">📈</span>
                <span v-else class="card-arrow">→</span>
              </div>
            </div>
          </template>
        </div>
      </section>

      <!-- 航运数据：拆分为 CCFI 与 BDI 两个 section，来自 market_indices(market='航运') -->
      <section v-for="sec in shippingSections" :key="sec.title" class="section">
        <div class="section-header">
          <span class="section-icon">{{ sec.icon }}</span>
          <h2 class="section-title">{{ sec.title }}</h2>
          <span class="section-count">{{ sec.items.length }} 个指数</span>
        </div>

        <div class="card-grid">
          <div
            v-for="item in sec.items"
            :key="item.key"
            class="card ship-card clickable"
            @click="goMarketDetail(item)"
          >
            <div class="card-top">
              <span class="card-name">{{ item.name }}</span>
              <el-tag size="small" type="info" class="card-tag">{{ item.key }}</el-tag>
            </div>

            <div class="card-price-row">
              <span class="card-price">
                {{ fmt(item.latestClose) }}
                <span class="card-unit">{{ item.unit ?? '点' }}</span>
              </span>
            </div>

            <div
              v-if="fmtChangePct(item.changePct)"
              class="card-pct"
              :class="changePctClass(item.changePct)"
            >{{ fmtChangePct(item.changePct) }}</div>

            <div class="card-footer">
              <span class="card-date">{{ fmtDate(item.latestDate) }}</span>
              <span class="card-arrow">→</span>
            </div>
          </div>
        </div>
      </section>
    </template>
  </div>
</template>

<style scoped>
.home-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 36px 20px 60px;
}

/* ── Header ─────────────────────────────────────────────────────── */
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

/* ── Section ─────────────────────────────────────────────────────── */
.section {
  margin-bottom: 36px;
}

.section-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 14px;
}

.section-icon {
  font-size: 18px;
  line-height: 1;
}

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

/* ── Card grid ───────────────────────────────────────────────────── */
.card-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(210px, 1fr));
  gap: 14px;
}

.card {
  background: #fff;
  border: 1px solid #e8e8f0;
  border-radius: 12px;
  padding: 16px 18px 14px;
  cursor: pointer;
  transition: box-shadow 0.18s, transform 0.18s, border-color 0.18s;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.card:hover {
  box-shadow: 0 6px 20px rgba(0, 0, 0, 0.09);
  transform: translateY(-2px);
  border-color: #c0c4d6;
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
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.card-tag {
  flex-shrink: 0;
  font-size: 11px;
}

.card-price {
  font-size: 22px;
  font-weight: 700;
  color: #1a1a2e;
  font-variant-numeric: tabular-nums;
  line-height: 1.1;
}

.card-unit {
  font-size: 12px;
  font-weight: 400;
  color: #888;
  margin-left: 3px;
}

.card-change {
  font-size: 14px;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}
.card-change.up   { color: #f56c6c; }
.card-change.down { color: #67c23a; }

/* ── Shipping card (航运数据 section, market indices) ─────────────── */
.ship-card.clickable {
  cursor: pointer;
}
.ship-card.clickable:hover {
  border-color: #409eff;
}

.card-price-row {
  display: flex;
  align-items: baseline;
}

.card-pct {
  font-size: 14px;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  margin-top: 2px;
}
.pct-up   { color: #f56c6c; }   /* 红色 = 上涨（中国习惯） */
.pct-down { color: #67c23a; }   /* 绿色 = 下跌 */

.card-footer {
  display: flex;
  align-items: center;
  gap: 3px;
  margin-top: auto;
}

.card-date {
  font-size: 12px;
  color: #aaa;
}

.card-sep {
  font-size: 12px;
  color: #ddd;
  margin: 0 2px;
}

.card-spot-time {
  font-size: 12px;
  color: #bbb;
  font-variant-numeric: tabular-nums;
}

.card-spot-badge {
  display: inline-block;
  margin-left: 3px;
  padding: 0 4px;
  font-size: 10px;
  line-height: 16px;
  border-radius: 4px;
  background: #e8f4ff;
  color: #409eff;
  font-weight: 500;
  vertical-align: middle;
}

.card-minutes-icon {
  margin-left: auto;
  font-size: 14px;
}

.card-arrow {
  margin-left: auto;
  font-size: 13px;
  color: #409eff;
}

/* ── Row break (forces coal items onto a new grid row) ───────────── */
.grid-row-break {
  grid-column: 1 / -1;
  height: 0;
  margin: 0;
  padding: 0;
}

/* ── Skeleton ────────────────────────────────────────────────────── */
.skeleton-wrap {
  padding: 16px 0;
}
</style>
