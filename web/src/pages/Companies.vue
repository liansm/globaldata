<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { fetchCompanies } from '@/api/companies'
import type { CompanySummary } from '@/types/company'

const router = useRouter()

const loading = ref(false)
const error   = ref('')
const list    = ref<CompanySummary[]>([])
const total   = ref(0)
const unassigned = ref<{ fundCount: number; totalScale: number } | null>(null)

const filter = reactive({
  q: '',
  order: 'scale' as 'scale' | 'count' | 'name',
  page: 1,
  pageSize: 50,
})

let searchTimer: ReturnType<typeof setTimeout> | null = null

async function load() {
  loading.value = true
  error.value = ''
  try {
    const resp = await fetchCompanies({
      q: filter.q,
      order: filter.order,
      page: filter.page,
      pageSize: filter.pageSize,
    })
    list.value  = resp.items
    total.value = resp.total
    unassigned.value = resp.unassigned
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
    list.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

onMounted(load)

function onSearch() {
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(() => {
    filter.page = 1
    load()
  }, 350)
}

function onOrderChange() {
  filter.page = 1
  load()
}

function onPageChange(page: number) {
  filter.page = page
  load()
}

function onPageSizeChange(size: number) {
  filter.pageSize = size
  filter.page = 1
  load()
}

function goCompany(row: CompanySummary) {
  router.push(`/company/${encodeURIComponent(row.key)}`)
}

function goFund(code: string) {
  router.push(`/fund/${code}`)
}

/** 排名：翻页后要接上前面的序号 */
function rankOf(index: number) {
  return (filter.page - 1) * filter.pageSize + index + 1
}

// ── Formatters ──────────────────────────────────────────────────────────────
function fmtScale(v: number | null | undefined, unit = '') {
  if (v == null) return '—'
  const s = v >= 1000 ? v.toFixed(0)
    : v >= 100 ? v.toFixed(1)
      : v >= 1 ? v.toFixed(2)
        : v.toFixed(4)
  return s + unit
}

function fmtDate(d: string | null) {
  return d ? d.slice(0, 10) : '—'
}
</script>

<template>
  <div class="companies-page">
    <div class="page-header">
      <div>
        <h1>基金公司</h1>
        <p class="subtitle">
          {{ total }} 家管理人 · 按旗下产品数 / 合计规模查看 · 点击公司进入产品列表
        </p>
      </div>
      <span v-if="total" class="total-badge">共 {{ total }} 家</span>
    </div>

    <el-alert
      v-if="error"
      :title="error"
      type="error"
      show-icon
      :closable="false"
      style="margin-bottom: 20px"
    />

    <!-- 筛选栏 -->
    <div class="filter-bar">
      <el-input
        v-model="filter.q"
        placeholder="搜索基金公司名称"
        clearable
        style="width: 260px"
        @input="onSearch"
        @clear="onSearch"
      >
        <template #prefix><el-icon><Search /></el-icon></template>
      </el-input>

      <el-radio-group v-model="filter.order" @change="onOrderChange">
        <el-radio-button value="scale">按合计规模</el-radio-button>
        <el-radio-button value="count">按产品数</el-radio-button>
        <el-radio-button value="name">按名称</el-radio-button>
      </el-radio-group>
    </div>

    <!-- 列表 -->
    <el-table
      :data="list"
      v-loading="loading"
      style="width: 100%"
      class="company-table"
      @row-click="goCompany"
      :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }"
    >
      <el-table-column label="#" width="56" align="center">
        <template #default="{ $index }">
          <span class="rank">{{ rankOf($index) }}</span>
        </template>
      </el-table-column>

      <el-table-column label="基金公司" min-width="210">
        <template #default="{ row }">
          <div class="company-cell">
            <span class="company-name">{{ row.name }}</span>
            <span v-if="row.fullName !== row.name" class="company-full">{{ row.fullName }}</span>
          </div>
        </template>
      </el-table-column>

      <el-table-column label="产品数" width="92" align="right" sortable :sort-by="'fundCount'">
        <template #default="{ row }">
          <span class="num">{{ row.fundCount }}</span>
        </template>
      </el-table-column>

      <el-table-column label="合计规模 (亿元)" width="136" align="right" sortable :sort-by="'totalScale'">
        <template #default="{ row }">
          <span class="num strong">{{ fmtScale(row.totalScale) }}</span>
        </template>
      </el-table-column>

      <el-table-column label="规模覆盖" width="110" align="center">
        <template #default="{ row }">
          <el-tooltip
            :content="`${row.scaleCovered} / ${row.fundCount} 只产品已采集到规模数据`"
            placement="top"
          >
            <span class="coverage">{{ row.scaleCovered }}/{{ row.fundCount }}</span>
          </el-tooltip>
        </template>
      </el-table-column>

      <el-table-column label="有股票持仓" width="106" align="center">
        <template #default="{ row }">
          <el-tooltip
            :content="`${row.holdingsCovered} 只产品有季度重仓明细`"
            placement="top"
          >
            <span class="coverage">{{ row.holdingsCovered }}</span>
          </el-tooltip>
        </template>
      </el-table-column>

      <el-table-column label="最早成立" width="106" align="center">
        <template #default="{ row }">{{ fmtDate(row.earliestInception) }}</template>
      </el-table-column>

      <el-table-column label="代表产品" min-width="260">
        <template #header>
          <el-tooltip
            content="按规模排序的前三只产品（货币型排在最后，仅在纯货币公司出现）"
            placement="top"
          >
            <span class="th-hint">代表产品</span>
          </el-tooltip>
        </template>
        <template #default="{ row }">
          <div class="topfunds">
            <el-tag
              v-for="f in row.topFunds"
              :key="f.fundCode"
              size="small"
              type="info"
              effect="plain"
              class="topfund-tag"
              @click.stop="goFund(f.fundCode)"
            >
              {{ f.fundName }}
            </el-tag>
            <span v-if="!row.topFunds.length" class="muted">—</span>
          </div>
        </template>
      </el-table-column>
    </el-table>

    <!-- 分页 -->
    <div class="pagination-wrap">
      <el-pagination
        v-model:current-page="filter.page"
        :page-size="filter.pageSize"
        :page-sizes="[20, 50, 100, 200]"
        :total="total"
        layout="total, sizes, prev, pager, next, jumper"
        @current-change="onPageChange"
        @size-change="onPageSizeChange"
      />
    </div>

    <!-- 数据覆盖说明 -->
    <p v-if="unassigned && unassigned.fundCount" class="footnote">
      另有 <b>{{ unassigned.fundCount.toLocaleString() }}</b> 只产品未采集到管理人信息，
      未出现在上方列表（多为场内 / 暂不销售的份额，雪球接口不返回其管理人）。
    </p>
  </div>
</template>

<style scoped>
.companies-page {
  max-width: 1320px;
  margin: 0 auto;
  padding: 36px 20px 60px;
}

.page-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 24px;
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

.total-badge {
  font-size: 13px;
  color: #409eff;
  background: #e8f3ff;
  padding: 4px 12px;
  border-radius: 12px;
  font-weight: 600;
  white-space: nowrap;
}

/* ── 筛选栏 ─────────────────────────────────────────────────────────────── */
.filter-bar {
  display: flex;
  align-items: center;
  gap: 14px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}

/* ── 表格 ───────────────────────────────────────────────────────────────── */
.company-table {
  background: #fff;
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05);
}

:deep(.el-table__row) {
  cursor: pointer;
}

.rank {
  color: #aaa;
  font-variant-numeric: tabular-nums;
  font-size: 13px;
}

.company-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
  line-height: 1.35;
}

.company-name {
  font-weight: 600;
  color: #409eff;
}

:deep(.el-table__row:hover) .company-name {
  text-decoration: underline;
}

.company-full {
  font-size: 12px;
  color: #aaa;
}

.num {
  font-variant-numeric: tabular-nums;
  color: #1a1a2e;
}

.num.strong {
  font-weight: 600;
}

.coverage {
  font-variant-numeric: tabular-nums;
  font-size: 13px;
  color: #666;
  border-bottom: 1px dashed #ddd;
  cursor: help;
}

.th-hint {
  border-bottom: 1px dashed #ccc;
  cursor: help;
}

.topfunds {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.topfund-tag {
  cursor: pointer;
  max-width: 100%;
}

.topfund-tag:hover {
  border-color: #409eff;
  color: #409eff;
}

.muted {
  color: #bbb;
}

/* ── 分页 / 脚注 ────────────────────────────────────────────────────────── */
.pagination-wrap {
  display: flex;
  justify-content: flex-end;
  margin-top: 16px;
}

.footnote {
  margin: 18px 0 0;
  font-size: 12.5px;
  color: #999;
  line-height: 1.7;
}

.footnote b {
  color: #e6a23c;
  font-variant-numeric: tabular-nums;
}
</style>
