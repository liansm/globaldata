<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { fetchFxPairs } from '@/api/fx'
import type { FxPair } from '@/types/fx'

const router = useRouter()

const pairs   = ref<FxPair[]>([])
const loading = ref(true)
const error   = ref<string | null>(null)

onMounted(async () => {
  try {
    pairs.value = await fetchFxPairs()
  } catch (e: any) {
    error.value = e?.message ?? '加载失败，请稍后重试'
  } finally {
    loading.value = false
  }
})

// 分组顺序由后端 sort_order 给定（主要货币在前），这里只做 slice，不重排
const CATEGORIES = ['主要货币', '其他货币']
const groups = computed(() =>
  CATEGORIES
    .map(category => ({ category, items: pairs.value.filter(p => p.category === category) }))
    .filter(g => g.items.length > 0),
)

// ── 展示口径（唯一出处）────────────────────────────────────────────────────
// `1 单位外币 = X 人民币`，日元按官方惯例以 100 为单位。
// 数值上行 = 人民币贬值（红）／下行 = 人民币升值（绿）。换算已在后端完成。
function unitLabel(p: FxPair): string {
  return `${p.quoteUnit} ${p.baseCode}`
}
function pairLabel(p: FxPair): string {
  return `${unitLabel(p)} / CNY`
}

/** 小数位按报价量级自适应：≥1 用 4 位，<1 用 6 位（韩元 0.004958 才看得见） */
function rateDigits(v: number | null): number {
  if (v == null) return 4
  return Math.abs(v) >= 1 ? 4 : 6
}
function fmtRate(v: number | null): string {
  if (v == null) return '—'
  return v.toFixed(rateDigits(v))
}
/**
 * 涨跌额的小数位**跟随报价本身**，而不是跟随涨跌额的大小。
 * 否则 6.7351（4 位）会配一个 -0.006000（6 位）的涨跌额，同一张卡上两个精度。
 */
function fmtChange(v: number | null, ref: number | null): string {
  if (v == null) return '—'
  return (v > 0 ? '+' : '') + v.toFixed(rateDigits(ref))
}
function fmtPct(v: number | null): string | null {
  if (v == null) return null
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`
}
function signClass(v: number | null): string {
  if (v == null || v === 0) return 'flat'
  return v > 0 ? 'up' : 'down'
}
function mmdd(d: string | null): string {
  return d ? d.slice(5) : '—'
}

function open(key: string) {
  router.push(`/fx/${key}`)
}
</script>

<template>
  <div class="fx-page">
    <header class="page-header">
      <h1 class="page-title">💱 人民币汇率</h1>
      <p class="page-sub">
        人民币汇率中间价（中国外汇交易中心） · 即期汇率（新浪财经）
      </p>
    </header>

    <!-- 口径说明：本页所有数值共用一套语义，必须显式写出来 -->
    <el-alert type="info" :closable="false" class="caliber-alert">
      <template #title>
        <span class="caliber-title">计价口径</span>
      </template>
      <div class="caliber-body">
        <p>
          全部报价统一为 <b>「1 单位外币 = X 人民币」</b>（日元按官方惯例用 100 单位）。
          因此
          <span class="up">数值上行 = 人民币贬值</span>、
          <span class="down">数值下行 = 人民币升值</span>。
        </p>
        <p>
          <b>中间价</b>：中国人民银行授权中国外汇交易中心发布的官方定价，<b>每个工作日 9:15</b> 发布
          （周末与法定节假日无数据）。覆盖 25 个货币对，历史起点 2006-01。
          <br />
          <b>即期</b>：市场汇率日线（新浪财经），覆盖 19 个货币对。<b>实时</b>：即期最新报价快照，
          <b>休市时停更</b>，请以报价时间为准。
        </p>
      </div>
    </el-alert>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" style="margin-bottom:20px" />

    <!-- Loading -->
    <div v-if="loading" class="fx-grid">
      <el-skeleton v-for="i in 10" :key="i" :rows="3" animated
                   style="padding:20px;background:#fff;border-radius:12px" />
    </div>

    <template v-else-if="pairs.length">
      <section v-for="g in groups" :key="g.category" class="section">
        <h2 class="section-title">{{ g.category }}（{{ g.items.length }}）</h2>
        <div class="fx-grid">
          <div
            v-for="p in g.items"
            :key="p.key"
            class="fx-card"
            @click="open(p.key)"
          >
            <!-- 头部：货币名 + 原始货币对代码 -->
            <div class="card-header">
              <div class="fx-meta">
                <span class="fx-name">{{ p.baseName }}</span>
                <span class="fx-code">{{ pairLabel(p) }}</span>
              </div>
              <span
                v-if="fmtPct(p.mid.changePct)"
                :class="['pct-badge', signClass(p.mid.changePct)]"
              >{{ fmtPct(p.mid.changePct) }}</span>
            </div>

            <!-- 主数值：中间价 -->
            <div class="card-price">
              <span class="price-label">中间价</span>
              <span class="price-value">{{ fmtRate(p.mid.close) }}</span>
            </div>
            <div class="card-sub">
              <span :class="['chg', signClass(p.mid.change)]">
                {{ fmtChange(p.mid.change, p.mid.close) }}
              </span>
              <span class="date">{{ mmdd(p.mid.date) }}</span>
              <span class="src">CFETS</span>
            </div>

            <!-- 副行：即期 / 实时 -->
            <div class="card-stats">
              <div class="stat">
                <span class="stat-label">即期</span>
                <span class="stat-value" :class="signClass(p.spot.changePct)">
                  {{ fmtRate(p.spot.close) }}
                </span>
                <span class="stat-note">{{ mmdd(p.spot.date) }}</span>
              </div>
              <div class="stat">
                <span class="stat-label">实时</span>
                <span class="stat-value" :class="signClass(p.live?.changePct ?? null)">
                  {{ fmtRate(p.live?.price ?? null) }}
                </span>
                <span class="stat-note">{{ p.live?.quoteTime ?? '—' }}</span>
              </div>
            </div>
          </div>
        </div>
      </section>
      <p class="footnote">
        点击卡片查看历史走势。两个口径可切换查看，但覆盖范围与历史深度不同，不可拼成同一条序列。
      </p>
    </template>

    <el-empty
      v-else
      description="暂无外汇数据，请先运行 python fetch_fx.py --full"
    />
  </div>
</template>

<style scoped>
.fx-page {
  padding: 28px 32px;
  max-width: 1280px;
}

.page-header { margin-bottom: 20px; }
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

/* ── 口径说明 ───────────────────────────────────────────────────────────── */
.caliber-alert { margin-bottom: 26px; }
.caliber-title { font-weight: 700; }
.caliber-body { font-size: 12.5px; line-height: 1.75; color: #5a6472; }
.caliber-body p { margin: 0 0 6px; }
.caliber-body p:last-child { margin-bottom: 0; }

/* ── Section ────────────────────────────────────────────────────────────── */
.section { margin-bottom: 32px; }
.section-title {
  font-size: 15px;
  font-weight: 600;
  color: #555;
  margin: 0 0 14px;
  padding-left: 10px;
  border-left: 3px solid #409eff;
}

/* ── Grid / Card ────────────────────────────────────────────────────────── */
.fx-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(232px, 1fr));
  gap: 16px;
}

.fx-card {
  background: #fff;
  border-radius: 12px;
  padding: 16px 16px 12px;
  box-shadow: 0 1px 4px rgba(0,0,0,.06);
  transition: box-shadow .18s, transform .18s;
  cursor: pointer;
}
.fx-card:hover {
  box-shadow: 0 4px 16px rgba(0,0,0,.10);
  transform: translateY(-2px);
}

.card-header {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin-bottom: 10px;
}
.fx-meta {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 1px;
}
.fx-name {
  font-size: 13.5px;
  font-weight: 600;
  color: #1a1a2e;
}
.fx-code {
  font-size: 11px;
  color: #a8b0bb;
  font-family: ui-monospace, Menlo, Consolas, monospace;
}

.pct-badge {
  font-size: 11.5px;
  font-weight: 700;
  padding: 2px 6px;
  border-radius: 6px;
  flex-shrink: 0;
  font-variant-numeric: tabular-nums;
}
.up    { color: #e8534a; background: #fff0ef; }
.down  { color: #4caf82; background: #edf9f3; }
.flat  { color: #8a94a0; background: #f3f5f7; }

.card-price {
  display: flex;
  align-items: baseline;
  gap: 6px;
}
.price-label {
  font-size: 11px;
  color: #b6bdc7;
  flex-shrink: 0;
}
.price-value {
  font-size: 21px;
  font-weight: 700;
  color: #1a1a2e;
  letter-spacing: -0.4px;
  font-variant-numeric: tabular-nums;
}

.card-sub {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 3px;
  margin-bottom: 10px;
  font-size: 11.5px;
  font-variant-numeric: tabular-nums;
}
.chg { font-weight: 600; }
.card-sub .date { color: #b6bdc7; }
.src {
  margin-left: auto;
  font-size: 10px;
  color: #c3c9d2;
  letter-spacing: 0.4px;
}

.card-stats {
  display: flex;
  border-top: 1px solid #f4f6f8;
  padding-top: 9px;
}
.stat {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 1px;
}
.stat:not(:last-child) { border-right: 1px solid #f4f6f8; }
.stat + .stat { padding-left: 10px; }
.stat-label {
  font-size: 10px;
  color: #c3c9d2;
  letter-spacing: 0.4px;
}
.stat-value {
  font-size: 12.5px;
  font-weight: 600;
  color: #444c57;
  font-variant-numeric: tabular-nums;
}
.stat-note {
  font-size: 10px;
  color: #c3c9d2;
  font-variant-numeric: tabular-nums;
}

.footnote {
  font-size: 12px;
  color: #b0b8c2;
  margin: 4px 0 0;
}
</style>
