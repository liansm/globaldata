<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { fetchFundDetail } from '@/api/funds'
import type { FundDetailResp, FundHolding } from '@/types/fund'

const route  = useRoute()
const router = useRouter()

const loading   = ref(true)
const error     = ref('')
const detail    = ref<FundDetailResp | null>(null)
const reportDate = ref<string>('')

async function load(code: string) {
  loading.value = true
  error.value = ''
  try {
    detail.value = await fetchFundDetail(code, reportDate.value || undefined)
    // 报告期不在列表中（如 URL 带了无效 date）时回落到最新
    if (detail.value.reportDates.length && !detail.value.reportDates.includes(reportDate.value)) {
      reportDate.value = detail.value.reportDates[0]
    }
  } catch (e: any) {
    const msg = e?.response?.data?.error
    error.value = msg ?? '加载失败，请检查后端服务是否启动'
    detail.value = null
  } finally {
    loading.value = false
  }
}

watch(
  () => route.params.code as string,
  (code) => { if (code) load(code) },
  { immediate: true },
)

watch(reportDate, () => {
  const code = route.params.code as string
  if (code) load(code)
})

const stockHoldings = computed<FundHolding[]>(() =>
  (detail.value?.holdings ?? []).filter(h => h.holdingType === 'stock'))
const bondHoldings = computed<FundHolding[]>(() =>
  (detail.value?.holdings ?? []).filter(h => h.holdingType === 'bond'))

/** 重仓合计占净值比例（股票） */
const stockRatioSum = computed(() =>
  stockHoldings.value.reduce((s, h) => s + (h.ratio ?? 0), 0))

/** 比例条最大值（用于归一化宽度） */
const maxRatio = computed(() => {
  const all = [...stockHoldings.value, ...bondHoldings.value]
  return Math.max(1, ...all.map(h => h.ratio ?? 0))
})

function goBack() {
  router.push('/funds')
}

function goCompany(key: string) {
  router.push(`/company/${encodeURIComponent(key)}`)
}

// ── Formatters ──────────────────────────────────────────────────────────────
function fmtScale(v: number | null) {
  if (v == null) return '—'
  if (v >= 100) return v.toFixed(0)
  if (v >= 1)   return v.toFixed(2)
  return v.toFixed(4)
}

function fmtRatio(v: number | null) {
  return v == null ? '—' : v.toFixed(2) + '%'
}

function fmtShares(v: number | null) {
  if (v == null) return '—'
  if (v >= 10_000) return (v / 10_000).toFixed(2) + ' 亿股'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }) + ' 万'
}

function fmtMv(v: number | null) {
  if (v == null) return '—'
  if (v >= 10_000) return (v / 10_000).toFixed(2) + ' 亿'
  return v.toLocaleString('zh-CN', { maximumFractionDigits: 2 }) + ' 万'
}

function fmtDate(d: string | null | undefined) {
  return d ? d.slice(0, 10) : '—'
}
</script>

<template>
  <div class="fund-detail-page">
    <div class="page-header">
      <el-button text @click="goBack" class="back-btn">
        <el-icon style="margin-right: 4px"><ArrowLeft /></el-icon>返回列表
      </el-button>
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
      <el-skeleton :rows="8" animated />
    </div>

    <template v-else-if="detail">
      <!-- 基金信息头 -->
      <section class="meta-card">
        <div class="meta-top">
          <h1 class="fund-title">{{ detail.fundName }}</h1>
          <el-tag size="small" type="info" class="fund-code">{{ detail.fundCode }}</el-tag>
        </div>

        <div class="meta-stats">
          <div class="stat">
            <span class="stat-label">最新规模</span>
            <span class="stat-value">{{ fmtScale(detail.scale) }}<span class="stat-unit">亿元</span></span>
          </div>
          <div class="stat">
            <span class="stat-label">类型</span>
            <span class="stat-value text">{{ detail.fundType ?? '—' }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">基金经理</span>
            <span class="stat-value text">{{ detail.fundManager ?? '—' }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">基金公司</span>
            <span
              v-if="detail.companyKey"
              class="stat-value text link"
              :title="`查看 ${detail.fundCompany} 的全部产品`"
              @click="goCompany(detail.companyKey)"
            >{{ detail.companyKey }}</span>
            <span v-else class="stat-value text">{{ detail.fundCompany ?? '—' }}</span>
          </div>
          <div class="stat">
            <span class="stat-label">成立日期</span>
            <span class="stat-value">{{ fmtDate(detail.inceptionDate) }}</span>
          </div>
          <div class="stat" v-if="detail.reportDates.length">
            <span class="stat-label">前十大重仓合计</span>
            <span class="stat-value">{{ stockRatioSum.toFixed(2) }}%</span>
          </div>
        </div>
      </section>

      <!-- 持仓 -->
      <section class="section">
        <div class="section-header">
          <h2 class="section-title">重仓持仓</h2>
          <el-select
            v-if="detail.reportDates.length"
            v-model="reportDate"
            size="small"
            style="width: 150px"
          >
            <el-option
              v-for="d in detail.reportDates"
              :key="d"
              :label="d"
              :value="d"
            />
          </el-select>
        </div>

        <el-empty
          v-if="!detail.reportDates.length"
          description="暂无持仓数据（季报未披露或未抓取）"
        />

        <template v-else>
          <h3 v-if="stockHoldings.length" class="sub-title">股票持仓 · 前十大重仓</h3>
          <el-table
            v-if="stockHoldings.length"
            :data="stockHoldings"
            style="width: 100%"
            class="holding-table"
            :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
          >
            <el-table-column type="index" label="#" width="44" align="center" />
            <el-table-column prop="securityCode" label="股票代码" width="90">
              <template #default="{ row }">
                <span class="sec-code">{{ row.securityCode }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="securityName" label="股票名称" min-width="130">
              <template #default="{ row }">
                <span class="sec-name">{{ row.securityName ?? '—' }}</span>
              </template>
            </el-table-column>
            <el-table-column label="占净值比例" min-width="180" align="right">
              <template #default="{ row }">
                <div class="ratio-cell">
                  <span class="ratio-text">{{ fmtRatio(row.ratio) }}</span>
                  <div class="ratio-bar-track">
                    <div
                      class="ratio-bar"
                      :style="{ width: ((row.ratio ?? 0) / maxRatio * 100).toFixed(1) + '%' }"
                    />
                  </div>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="持股数 (万)" width="120" align="right">
              <template #default="{ row }">{{ fmtShares(row.shares) }}</template>
            </el-table-column>
            <el-table-column label="持仓市值 (万)" width="120" align="right">
              <template #default="{ row }">{{ fmtMv(row.marketValue) }}</template>
            </el-table-column>
          </el-table>

          <h3 v-if="bondHoldings.length" class="sub-title">债券持仓 · 前五大重仓</h3>
          <el-table
            v-if="bondHoldings.length"
            :data="bondHoldings"
            style="width: 100%"
            class="holding-table"
            :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
          >
            <el-table-column type="index" label="#" width="44" align="center" />
            <el-table-column prop="securityCode" label="债券代码" width="100">
              <template #default="{ row }">
                <span class="sec-code">{{ row.securityCode }}</span>
              </template>
            </el-table-column>
            <el-table-column prop="securityName" label="债券名称" min-width="220" show-overflow-tooltip>
              <template #default="{ row }">{{ row.securityName ?? '—' }}</template>
            </el-table-column>
            <el-table-column label="占净值比例" min-width="180" align="right">
              <template #default="{ row }">
                <div class="ratio-cell">
                  <span class="ratio-text">{{ fmtRatio(row.ratio) }}</span>
                  <div class="ratio-bar-track">
                    <div
                      class="ratio-bar bond"
                      :style="{ width: ((row.ratio ?? 0) / maxRatio * 100).toFixed(1) + '%' }"
                    />
                  </div>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="持仓市值 (万)" width="130" align="right">
              <template #default="{ row }">{{ fmtMv(row.marketValue) }}</template>
            </el-table-column>
          </el-table>
        </template>
      </section>

      <p class="data-note">
        持仓来源于基金季报披露（前十大重仓股 / 前五大重仓债券），完整持仓以半年报/年报为准。
      </p>
    </template>
  </div>
</template>

<style scoped>
.fund-detail-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 24px 20px 60px;
}

.page-header {
  margin-bottom: 12px;
}

.back-btn {
  font-size: 13px;
  color: #666;
  padding-left: 0;
}

/* ── 信息头 ─────────────────────────────────────────────────────────────── */
.meta-card {
  background: #fff;
  border: 1px solid #e8e8f0;
  border-radius: 12px;
  padding: 22px 26px;
  margin-bottom: 28px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}

.meta-top {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 18px;
}

.fund-title {
  margin: 0;
  font-size: 22px;
  font-weight: 700;
  color: #1a1a2e;
}

.fund-code {
  font-variant-numeric: tabular-nums;
}

.meta-stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 16px;
  border-top: 1px solid #f0f0f5;
  padding-top: 16px;
}

.stat {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.stat-label {
  font-size: 11px;
  color: #aaa;
  text-transform: uppercase;
  letter-spacing: 0.4px;
}

.stat-value {
  font-size: 16px;
  font-weight: 700;
  color: #1a1a2e;
  font-variant-numeric: tabular-nums;
}

.stat-value.text {
  font-size: 14px;
  font-weight: 500;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.stat-value.link {
  color: #409eff;
  cursor: pointer;
}

.stat-value.link:hover {
  text-decoration: underline;
}

.stat-unit {
  font-size: 12px;
  font-weight: 400;
  color: #999;
  margin-left: 3px;
}

/* ── 持仓区块 ───────────────────────────────────────────────────────────── */
.section {
  margin-bottom: 24px;
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 14px;
}

.section-title {
  margin: 0;
  font-size: 17px;
  font-weight: 600;
  color: #1a1a2e;
}

.sub-title {
  margin: 18px 0 10px;
  font-size: 14px;
  font-weight: 600;
  color: #555;
}

.holding-table {
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}

.sec-code {
  color: #555;
  font-variant-numeric: tabular-nums;
}

.sec-name {
  font-weight: 500;
  color: #1a1a2e;
}

/* 比例条 */
.ratio-cell {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 8px;
}

.ratio-text {
  font-variant-numeric: tabular-nums;
  font-weight: 600;
  color: #1a1a2e;
  white-space: nowrap;
}

.ratio-bar-track {
  width: 80px;
  height: 6px;
  background: #f0f2f5;
  border-radius: 3px;
  overflow: hidden;
  flex-shrink: 0;
}

.ratio-bar {
  height: 100%;
  border-radius: 3px;
  background: linear-gradient(90deg, #79bbff, #409eff);
}

.ratio-bar.bond {
  background: linear-gradient(90deg, #b3e19d, #67c23a);
}

.data-note {
  font-size: 12px;
  color: #bbb;
  margin-top: 20px;
}

.skeleton-wrap { padding: 16px 0; }
</style>
