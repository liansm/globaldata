<script setup lang="ts">
import { ref, computed, reactive, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { fetchCompanyDetail } from '@/api/companies'
import type { CompanyDetailResp } from '@/types/company'

const route  = useRoute()
const router = useRouter()

const loading = ref(true)
const error   = ref('')
const detail  = ref<CompanyDetailResp | null>(null)

const filter = reactive({
  type: '',
  q: '',
  order: 'scale' as 'scale' | 'code' | 'inception',
  page: 1,
  pageSize: 50,
})

let searchTimer: ReturnType<typeof setTimeout> | null = null

async function load(key: string) {
  loading.value = true
  error.value = ''
  try {
    detail.value = await fetchCompanyDetail(key, {
      type: filter.type,
      q: filter.q,
      order: filter.order,
      page: filter.page,
      pageSize: filter.pageSize,
    })
  } catch (e: any) {
    error.value = e?.response?.data?.error ?? '加载失败，请检查后端服务是否启动'
    detail.value = null
  } finally {
    loading.value = false
  }
}

// 切换公司时重置筛选条件
watch(
  () => route.params.key as string,
  (key) => {
    if (!key) return
    filter.type  = ''
    filter.q     = ''
    filter.page  = 1
    filter.order = 'scale'
    load(key)
  },
  { immediate: true },
)

function reload() {
  const key = route.params.key as string
  if (key) load(key)
}

function onTypeChange() {
  filter.page = 1
  reload()
}

function onOrderChange() {
  filter.page = 1
  reload()
}

function onSearch() {
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(() => {
    filter.page = 1
    reload()
  }, 350)
}

function onPageChange(page: number) {
  filter.page = page
  reload()
}

function onPageSizeChange(size: number) {
  filter.pageSize = size
  filter.page = 1
  reload()
}

function goBack() {
  router.push('/companies')
}

function goFund(code: string) {
  router.push(`/fund/${code}`)
}

function applyType(t: string) {
  filter.type = filter.type === t ? '' : t
  filter.page = 1
  reload()
}

// ── 类型分布条的归一化基准 ─────────────────────────────────────────────────
const maxTypeCount = computed(() =>
  Math.max(1, ...(detail.value?.types ?? []).map(t => t.count)))

// ── Formatters ──────────────────────────────────────────────────────────────
function fmtScale(v: number | null | undefined) {
  if (v == null) return '—'
  if (v >= 1000) return v.toFixed(0)
  if (v >= 100)  return v.toFixed(1)
  if (v >= 1)    return v.toFixed(2)
  return v.toFixed(4)
}

function fmtDate(d: string | null | undefined) {
  return d ? d.slice(0, 10) : '—'
}

function typeTagColor(t: string | null) {
  if (!t) return 'info'
  if (t.includes('股票')) return 'danger'
  if (t.includes('混合')) return 'warning'
  if (t.includes('指数')) return 'primary'
  if (t.includes('债券')) return 'success'
  return 'info'
}

/** 规模覆盖率的展示文案 */
const coverage = computed(() => {
  const d = detail.value
  if (!d) return { scale: '—', holdings: '—' }
  return {
    scale:    `${d.scaleCovered}/${d.fundCount}`,
    holdings: `${d.holdingsCovered}/${d.fundCount}`,
  }
})
</script>

<template>
  <div class="company-detail-page">
    <div class="page-header">
      <el-button text @click="goBack" class="back-btn">
        <el-icon style="margin-right: 4px"><ArrowLeft /></el-icon>返回公司列表
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

    <div v-if="loading && !detail" class="skeleton-wrap">
      <el-skeleton :rows="8" animated />
    </div>

    <template v-else-if="detail">
      <!-- 公司信息头 -->
      <section class="meta-card">
        <div class="meta-top">
          <h1 class="company-title">{{ detail.name }}</h1>
          <span v-if="detail.fundCount" class="total-badge">
            {{ detail.fundCount }} 只产品
          </span>
        </div>
        <p class="company-full">{{ detail.fullName }}</p>

        <p v-if="detail.aliases.length > 1" class="alias-hint">
          本页已合并该公司在数据源中的 {{ detail.aliases.length }} 种名称写法：
          <el-tooltip placement="top" :content="detail.aliases.join('、')">
            <span class="alias-more">{{ detail.aliases.join(' / ') }}</span>
          </el-tooltip>
        </p>

        <div class="meta-stats">
          <div class="stat">
            <span class="stat-label">产品数</span>
            <span class="stat-value">{{ detail.fundCount }}<span class="stat-unit">只</span></span>
          </div>
          <div class="stat">
            <span class="stat-label">合计规模</span>
            <span class="stat-value">{{ fmtScale(detail.totalScale) }}<span class="stat-unit">亿元</span></span>
          </div>
          <div class="stat">
            <span class="stat-label">规模覆盖</span>
            <span class="stat-value small" title="已采集到规模数据的产品数 / 总产品数">
              {{ coverage.scale }}
            </span>
          </div>
          <div class="stat">
            <span class="stat-label">股票持仓覆盖</span>
            <span class="stat-value small" title="有季度重仓明细的产品数 / 总产品数">
              {{ coverage.holdings }}
            </span>
          </div>
          <div class="stat">
            <span class="stat-label">基金经理</span>
            <span class="stat-value">{{ detail.managerCount }}<span class="stat-unit">人次</span></span>
          </div>
          <div class="stat">
            <span class="stat-label">最早成立</span>
            <span class="stat-value small">{{ fmtDate(detail.earliestInception) }}</span>
          </div>
        </div>
      </section>

      <!-- 类型分布 -->
      <section v-if="detail.types.length" class="section">
        <h2 class="section-title">产品类型分布</h2>
        <div class="type-bars">
          <div
            v-for="t in detail.types.slice(0, 10)"
            :key="t.fundType"
            class="type-row"
            :class="{ active: filter.type === t.fundType }"
            @click="applyType(t.fundType)"
          >
            <span class="type-name">{{ t.fundType }}</span>
            <div class="type-bar-track">
              <div
                class="type-bar"
                :style="{ width: (t.count / maxTypeCount * 100).toFixed(1) + '%' }"
              />
            </div>
            <span class="type-count">{{ t.count }} 只</span>
            <span class="type-scale">{{ fmtScale(t.scale) }} 亿</span>
          </div>
        </div>
        <p class="section-hint">点击某一类可筛选下方产品列表</p>
      </section>

      <!-- 基金经理 -->
      <section v-if="detail.managers.length" class="section">
        <h2 class="section-title">
          基金经理
          <span class="section-sub">按管理产品数排序，共 {{ detail.managerCount }} 人次</span>
        </h2>
        <div class="manager-list">
          <el-tag
            v-for="m in detail.managers"
            :key="m.name"
            size="small"
            effect="plain"
            class="manager-tag"
          >
            {{ m.name }}
            <span class="manager-count">{{ m.count }}</span>
          </el-tag>
        </div>
      </section>

      <!-- 产品列表 -->
      <section class="section">
        <h2 class="section-title">
          旗下产品
          <span class="section-sub">
            共 {{ detail.total }} 只{{ filter.type ? ` · 已筛选「${filter.type}」` : '' }}
          </span>
        </h2>

        <div class="filter-bar">
          <el-select
            v-model="filter.type"
            placeholder="全部类型"
            clearable
            filterable
            style="width: 200px"
            @change="onTypeChange"
          >
            <el-option
              v-for="t in detail.types"
              :key="t.fundType"
              :label="`${t.fundType} (${t.count})`"
              :value="t.fundType"
            />
          </el-select>

          <el-input
            v-model="filter.q"
            placeholder="搜索产品名称 / 代码"
            clearable
            style="width: 230px"
            @input="onSearch"
            @clear="onSearch"
          >
            <template #prefix><el-icon><Search /></el-icon></template>
          </el-input>

          <el-radio-group v-model="filter.order" @change="onOrderChange">
            <el-radio-button value="scale">按规模</el-radio-button>
            <el-radio-button value="inception">按成立日期</el-radio-button>
            <el-radio-button value="code">按代码</el-radio-button>
          </el-radio-group>
        </div>

        <el-table
          :data="detail.items"
          v-loading="loading"
          style="width: 100%"
          class="fund-table"
          @row-click="(row: any) => goFund(row.fundCode)"
          :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
        >
          <el-table-column prop="fundCode" label="代码" width="90">
            <template #default="{ row }">
              <span class="code-link">{{ row.fundCode }}</span>
            </template>
          </el-table-column>

          <el-table-column prop="fundName" label="基金名称" min-width="220" show-overflow-tooltip>
            <template #default="{ row }">
              <span class="fund-name">{{ row.fundName }}</span>
            </template>
          </el-table-column>

          <el-table-column prop="fundType" label="类型" width="132">
            <template #default="{ row }">
              <el-tag v-if="row.fundType" size="small" :type="typeTagColor(row.fundType)" effect="light">
                {{ row.fundType }}
              </el-tag>
              <span v-else>—</span>
            </template>
          </el-table-column>

          <el-table-column prop="fundManager" label="基金经理" min-width="120" show-overflow-tooltip>
            <template #default="{ row }">{{ row.fundManager ?? '—' }}</template>
          </el-table-column>

          <el-table-column label="规模 (亿元)" width="112" align="right">
            <template #default="{ row }">
              <span class="scale-value">{{ fmtScale(row.scale) }}</span>
            </template>
          </el-table-column>

          <el-table-column label="成立日期" width="110" align="center">
            <template #default="{ row }">{{ fmtDate(row.inceptionDate) }}</template>
          </el-table-column>

          <el-table-column label="最新持仓期" width="114" align="center">
            <template #default="{ row }">
              <span v-if="row.latestReportDate" class="report-date">
                {{ fmtDate(row.latestReportDate) }}
              </span>
              <el-tooltip
                v-else
                content="该产品没有股票持仓明细（ETF 联接、纯债、QDII 债等结构性无股票投资明细）"
                placement="top"
              >
                <span class="no-holding">无</span>
              </el-tooltip>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination-wrap">
          <el-pagination
            v-model:current-page="filter.page"
            :page-size="filter.pageSize"
            :page-sizes="[20, 50, 100, 200]"
            :total="detail.total"
            layout="total, sizes, prev, pager, next, jumper"
            @current-change="onPageChange"
            @size-change="onPageSizeChange"
          />
        </div>
      </section>
    </template>
  </div>
</template>

<style scoped>
.company-detail-page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 28px 20px 60px;
}

.page-header {
  margin-bottom: 8px;
}

.back-btn {
  color: #666;
  padding-left: 0;
}

/* ── 信息头 ─────────────────────────────────────────────────────────────── */
.meta-card {
  background: #fff;
  border-radius: 14px;
  padding: 24px 26px 22px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
  margin-bottom: 26px;
}

.meta-top {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.company-title {
  margin: 0;
  font-size: 28px;
  font-weight: 700;
  color: #1a1a2e;
  letter-spacing: -0.5px;
}

.total-badge {
  font-size: 13px;
  color: #409eff;
  background: #e8f3ff;
  padding: 4px 12px;
  border-radius: 12px;
  font-weight: 600;
  white-space: nowrap;
}

.company-full {
  margin: 6px 0 0;
  font-size: 13.5px;
  color: #999;
}

.alias-hint {
  margin: 10px 0 0;
  font-size: 12.5px;
  color: #8c8c8c;
  line-height: 1.7;
}

.alias-more {
  color: #409eff;
  border-bottom: 1px dashed #a9d0fb;
  cursor: help;
}

.meta-stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 16px 8px;
  margin-top: 22px;
  padding-top: 20px;
  border-top: 1px solid #f2f2f2;
}

.stat {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

.stat-label {
  font-size: 12.5px;
  color: #999;
}

.stat-value {
  font-size: 21px;
  font-weight: 700;
  color: #1a1a2e;
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.3px;
}

.stat-value.small {
  font-size: 16px;
  font-weight: 600;
}

.stat-unit {
  font-size: 12px;
  font-weight: 500;
  color: #aaa;
  margin-left: 3px;
}

/* ── 通用区块 ───────────────────────────────────────────────────────────── */
.section {
  margin-bottom: 30px;
}

.section-title {
  margin: 0 0 14px;
  font-size: 17px;
  font-weight: 700;
  color: #1a1a2e;
  display: flex;
  align-items: baseline;
  gap: 10px;
}

.section-sub {
  font-size: 12.5px;
  font-weight: 400;
  color: #999;
}

.section-hint {
  margin: 10px 0 0;
  font-size: 12.5px;
  color: #bbb;
}

/* ── 类型分布 ───────────────────────────────────────────────────────────── */
.type-bars {
  background: #fff;
  border-radius: 12px;
  padding: 18px 22px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.type-row {
  display: grid;
  grid-template-columns: 148px 1fr 62px 92px;
  align-items: center;
  gap: 12px;
  padding: 3px 6px;
  border-radius: 8px;
  cursor: pointer;
  transition: background 0.15s;
}

.type-row:hover {
  background: #f6f9ff;
}

.type-row.active {
  background: #e8f3ff;
}

.type-name {
  font-size: 13px;
  color: #555;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.type-bar-track {
  height: 9px;
  background: #f1f3f7;
  border-radius: 5px;
  overflow: hidden;
}

.type-bar {
  height: 100%;
  background: linear-gradient(90deg, #79bbff, #409eff);
  border-radius: 5px;
  transition: width 0.3s;
}

.type-count {
  font-size: 12.5px;
  color: #666;
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.type-scale {
  font-size: 12.5px;
  color: #999;
  text-align: right;
  font-variant-numeric: tabular-nums;
}

/* ── 基金经理 ───────────────────────────────────────────────────────────── */
.manager-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  background: #fff;
  border-radius: 12px;
  padding: 16px 20px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}

.manager-tag {
  font-size: 12.5px;
}

.manager-count {
  margin-left: 4px;
  color: #409eff;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}

/* ── 产品表 ─────────────────────────────────────────────────────────────── */
.filter-bar {
  display: flex;
  align-items: center;
  gap: 14px;
  margin-bottom: 14px;
  flex-wrap: wrap;
}

.fund-table {
  background: #fff;
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}

:deep(.el-table__row) {
  cursor: pointer;
}

:deep(.el-table__row:hover) .code-link {
  text-decoration: underline;
}

.code-link {
  color: #409eff;
  font-variant-numeric: tabular-nums;
  font-weight: 500;
}

.fund-name {
  font-weight: 500;
  color: #1a1a2e;
}

.scale-value {
  font-variant-numeric: tabular-nums;
  font-weight: 600;
  color: #1a1a2e;
}

.report-date {
  font-size: 12.5px;
  color: #666;
  font-variant-numeric: tabular-nums;
}

.no-holding {
  font-size: 12px;
  color: #c0c4cc;
  border-bottom: 1px dashed #e0e0e0;
  cursor: help;
}

.pagination-wrap {
  display: flex;
  justify-content: flex-end;
  margin-top: 16px;
}

.skeleton-wrap {
  background: #fff;
  border-radius: 12px;
  padding: 24px;
}
</style>
