<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { fetchCrypto } from '@/api/crypto'
import type { CryptoCoin } from '@/types/crypto'

const coins   = ref<CryptoCoin[]>([])
const loading = ref(true)
const error   = ref<string | null>(null)

onMounted(async () => {
  try {
    coins.value = await fetchCrypto()
  } catch (e: any) {
    error.value = e?.message ?? 'Failed to load crypto data'
  } finally {
    loading.value = false
  }
})

// ── Formatters ──────────────────────────────────────────────────────────────

/** Price: dynamic decimal places based on magnitude */
function fmtPrice(v: number | null): string {
  if (v == null) return '—'
  if (v >= 10_000) return v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
  if (v >= 1)      return v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })
  return v.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 6 })
}

/** Volume: convert to K / M / B */
function fmtVolume(v: number | null): string {
  if (v == null) return '—'
  if (v >= 1_000_000_000) return (v / 1_000_000_000).toFixed(2) + 'B'
  if (v >= 1_000_000)     return (v / 1_000_000).toFixed(2) + 'M'
  if (v >= 1_000)         return (v / 1_000).toFixed(2) + 'K'
  return v.toFixed(2)
}

/** Change pct with sign */
function fmtPct(v: number | null): string | null {
  if (v == null) return null
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
}

function pctClass(v: number | null): string {
  if (v == null) return ''
  return v >= 0 ? 'pct-up' : 'pct-down'
}

/** Coin symbol → emoji icon */
const COIN_ICONS: Record<string, string> = {
  btc: '₿', eth: 'Ξ', bnb: '◈', sol: '◎', xrp: '✕',
  usdt: '₮', usdc: '$', ada: '₳', doge: 'Ð', avax: '🔺',
}
function coinIcon(key: string): string {
  return COIN_ICONS[key] ?? '●'
}
</script>

<template>
  <div class="crypto-page">
    <header class="page-header">
      <h1 class="page-title">₿ 加密货币</h1>
      <p class="page-sub">主流加密货币实时行情 · 数据来源 jin10.com</p>
    </header>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" style="margin-bottom:20px" />

    <!-- Loading skeleton -->
    <div v-if="loading" class="coin-grid">
      <el-skeleton v-for="i in 10" :key="i" :rows="3" animated style="padding:20px;background:#fff;border-radius:12px" />
    </div>

    <!-- Coin cards -->
    <section v-else class="section">
      <h2 class="section-title">主流加密货币</h2>
      <div class="coin-grid">
        <div
          v-for="coin in coins"
          :key="coin.key"
          class="coin-card"
        >
          <!-- Header row -->
          <div class="card-header">
            <span class="coin-icon">{{ coinIcon(coin.key) }}</span>
            <div class="coin-meta">
              <span class="coin-name">{{ coin.name }}</span>
              <span class="coin-symbol">{{ coin.symbol }}</span>
            </div>
            <span
              v-if="fmtPct(coin.changePct)"
              :class="['pct-badge', pctClass(coin.changePct)]"
            >{{ fmtPct(coin.changePct) }}</span>
          </div>

          <!-- Price -->
          <div class="card-price">
            <span class="price-value">{{ fmtPrice(coin.latestClose) }}</span>
            <span class="price-unit">USD</span>
          </div>

          <!-- Stats row -->
          <div class="card-stats">
            <div class="stat">
              <span class="stat-label">24H 高</span>
              <span class="stat-value">{{ fmtPrice(coin.high24h) }}</span>
            </div>
            <div class="stat">
              <span class="stat-label">24H 低</span>
              <span class="stat-value">{{ fmtPrice(coin.low24h) }}</span>
            </div>
            <div class="stat">
              <span class="stat-label">成交量</span>
              <span class="stat-value">{{ fmtVolume(coin.volume24h) }}</span>
            </div>
          </div>

          <!-- Date -->
          <div class="card-footer">
            <span class="card-date">{{ coin.latestDate ?? '—' }}</span>
          </div>
        </div>
      </div>
    </section>
  </div>
</template>

<style scoped>
/* ── Page layout ─────────────────────────────────────────────────────────── */
.crypto-page {
  padding: 28px 32px;
  max-width: 1280px;
}

.page-header {
  margin-bottom: 28px;
}
.page-title {
  font-size: 22px;
  font-weight: 700;
  color: #1a1a2e;
  margin: 0 0 4px;
}
.page-sub {
  font-size: 13px;
  color: #999;
  margin: 0;
}

/* ── Section ─────────────────────────────────────────────────────────────── */
.section {
  margin-bottom: 36px;
}
.section-title {
  font-size: 15px;
  font-weight: 600;
  color: #555;
  margin: 0 0 14px;
  padding-left: 10px;
  border-left: 3px solid #409eff;
}

/* ── Grid ────────────────────────────────────────────────────────────────── */
.coin-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(210px, 1fr));
  gap: 16px;
}

/* ── Card ────────────────────────────────────────────────────────────────── */
.coin-card {
  background: #fff;
  border-radius: 12px;
  padding: 18px 18px 14px;
  box-shadow: 0 1px 4px rgba(0,0,0,.06);
  transition: box-shadow .18s, transform .18s;
  cursor: default;
}
.coin-card:hover {
  box-shadow: 0 4px 16px rgba(0,0,0,.10);
  transform: translateY(-2px);
}

/* header row */
.card-header {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
}
.coin-icon {
  font-size: 22px;
  width: 32px;
  text-align: center;
  flex-shrink: 0;
  line-height: 1;
}
.coin-meta {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 1px;
}
.coin-name {
  font-size: 13.5px;
  font-weight: 600;
  color: #1a1a2e;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.coin-symbol {
  font-size: 11px;
  color: #999;
  font-weight: 400;
}

/* change pct badge */
.pct-badge {
  font-size: 12px;
  font-weight: 700;
  padding: 2px 6px;
  border-radius: 6px;
  flex-shrink: 0;
}
.pct-up   { color: #e8534a; background: #fff0ef; }
.pct-down { color: #4caf82; background: #edf9f3; }

/* price */
.card-price {
  display: flex;
  align-items: baseline;
  gap: 5px;
  margin-bottom: 12px;
}
.price-value {
  font-size: 20px;
  font-weight: 700;
  color: #1a1a2e;
  letter-spacing: -0.5px;
}
.price-unit {
  font-size: 12px;
  color: #aaa;
}

/* stats */
.card-stats {
  display: flex;
  gap: 0;
  border-top: 1px solid #f5f5f5;
  padding-top: 10px;
  margin-bottom: 8px;
}
.stat {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 2px;
  text-align: center;
}
.stat:not(:last-child) {
  border-right: 1px solid #f5f5f5;
}
.stat-label {
  font-size: 10px;
  color: #bbb;
  text-transform: uppercase;
  letter-spacing: 0.4px;
}
.stat-value {
  font-size: 12px;
  color: #555;
  font-weight: 500;
}

/* footer */
.card-footer {
  text-align: right;
}
.card-date {
  font-size: 11px;
  color: #ccc;
}
</style>
