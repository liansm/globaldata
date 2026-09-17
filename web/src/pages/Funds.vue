<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { fetchFunds, fetchFundTypes } from '@/api/funds'
import type { FundSummary, FundTypeCount } from '@/types/fund'

const router = useRouter()

const loading  = ref(false)
const error    = ref('')
const list     = ref<FundSummary[]>([])
const total    = ref(0)
const types    = ref<FundTypeCount[]>([])

const filter = reactive({
  type: '' as string,
  q: '' as string,
  withScale: true,
  page: 1,
  pageSize: 50,
})

let searchTimer: ReturnType<typeof setTimeout> | null = null

async function load() {
  loading.value = true
  error.value = ''
  try {
    const resp = await fetchFunds({
      type: filter.type,
      q: filter.q,
      withScale: filter.withScale,
      page: filter.page,
      pageSize: filter.pageSize,
    })
    list.value  = resp.items
    total.value = resp.total
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  load()
  try {
    types.value = await fetchFundTypes()
  } catch { /* 类型下拉加载失败不阻塞 */ }
})

function onTypeChange() {
  filter.page = 1
  load()
}

/** 关键词防抖搜索 */
function onSearch() {
  if (searchTimer) clearTimeout(searchTimer)
  searchTimer = setTimeout(() => {
    filter.page = 1
    load()
  }, 350)
}

function onWithScaleChange() {
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

function goDetail(row: FundSummary) {
  router.push(`/fund/${row.fundCode}`)
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

function fmtDate(d: string | null) {
  return d ? d.slice(0, 10) : '—'
}

function fmtNav(v: number | null) {
  return v == null
    ? '—'
    : v.toLocaleString('zh-CN', { minimumFractionDigits: 4, maximumFractionDigits: 4 })
}

function fmtPct(v: number | null) {
  if (v == null) return '—'
  return (v > 0 ? '+' : '') + v.toFixed(2) + '%'
}

function typeTagColor(t: string | null): 'info' | 'warning' | 'danger' | 'primary' | 'success' {
  if (!t) return 'info'
  if (t.includes('股票')) return 'danger'
  if (t.includes('混合')) return 'warning'
  if (t.includes('指数')) return 'primary'
  if (t.includes('债券')) return 'success'
  return 'info'
}
</script>

<template>
  <div class="funds-page">
    <div class="page-header">
      <div>
        <h1>公募基金</h1>
        <p class="subtitle">基金名录 · 最新规模 · 季度重仓持仓 · 数据来源 天天基金 / 雪球</p>
      </div>
      <span v-if="total" class="total-badge">共 {{ total.toLocaleString() }} 只</span>
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
      <el-select
        v-model="filter.type"
        placeholder="全部类型"
        clearable
        filterable
        style="width: 180px"
        @change="onTypeChange"
      >
        <el-option
          v-for="t in types"
          :key="t.fundType ?? ''"
          :label="`${t.fundType ?? '未分类'} (${t.count})`"
          :value="t.fundType ?? ''"
        />
      </el-select>

      <el-input
        v-model="filter.q"
        placeholder="搜索基金名称 / 代码"
        clearable
        style="width: 240px"
        @input="onSearch"
        @clear="onSearch"
      >
        <template #prefix><el-icon><Search /></el-icon></template>
      </el-input>

      <el-checkbox v-model="filter.withScale" @change="onWithScaleChange">
        仅有规模数据
      </el-checkbox>
    </div>

    <!-- 列表 -->
    <el-table
      :data="list"
      v-loading="loading"
      style="width: 100%"
      class="fund-table"
      @row-click="goDetail"
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
      <el-table-column prop="fundType" label="类型" width="130">
        <template #default="{ row }">
          <el-tag v-if="row.fundType" size="small" :type="typeTagColor(row.fundType)" effect="light">
            {{ row.fundType }}
          </el-tag>
          <span v-else>—</span>
        </template>
      </el-table-column>
      <el-table-column prop="fundCompany" label="基金公司" min-width="150" show-overflow-tooltip>
        <template #default="{ row }">
          <!-- 显示规范化短名而非库里的原始写法：原始值同一家公司有
               「鹏华基金公司 / 鹏华基金管理公司」等多种拼法，列表里看起来像脏数据 -->
          <span
            v-if="row.companyKey"
            class="company-link"
            :title="row.fundCompany ? `原始名称：${row.fundCompany}` : ''"
            @click.stop="goCompany(row.companyKey)"
          >{{ row.companyKey }}</span>
          <span v-else>{{ row.fundCompany ?? '—' }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="fundManager" label="基金经理" min-width="110" show-overflow-tooltip>
        <template #default="{ row }">{{ row.fundManager ?? '—' }}</template>
      </el-table-column>
      <el-table-column label="规模 (亿元)" width="110" align="right" sortable :sort-by="'scale'">
        <template #default="{ row }">
          <span class="scale-value">{{ fmtScale(row.scale) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="单位净值" width="100" align="right">
        <template #default="{ row }">
          <!-- 货币基金没有单位净值，这一列放万份收益(元) -->
          <span class="nav-value" :title="row.navKind === 'money' ? '万份收益(元)' : '单位净值'">
            {{ row.latestNav != null ? fmtNav(row.latestNav) : '—' }}
          </span>
        </template>
      </el-table-column>
      <el-table-column label="日涨跌" width="90" align="right">
        <template #default="{ row }">
          <span
            v-if="row.latestDailyReturn != null"
            :class="row.latestDailyReturn > 0 ? 'up' : row.latestDailyReturn < 0 ? 'down' : ''"
          >{{ fmtPct(row.latestDailyReturn) }}</span>
          <span v-else class="nav-dash">—</span>
        </template>
      </el-table-column>
      <el-table-column label="成立日期" width="110" align="center">
        <template #default="{ row }">{{ fmtDate(row.inceptionDate) }}</template>
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
  </div>
</template>

<style scoped>
.funds-page {
  max-width: 1200px;
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

.company-link {
  color: #409eff;
  cursor: pointer;
}

.company-link:hover {
  text-decoration: underline;
}

.scale-value {
  font-variant-numeric: tabular-nums;
  font-weight: 600;
  color: #1a1a2e;
}

.nav-value {
  font-variant-numeric: tabular-nums;
  font-weight: 600;
  color: #1a1a2e;
}

.nav-dash { color: #ccc; }

/* 涨红跌绿（中式约定） */
.up   { color: #e8534a; font-variant-numeric: tabular-nums; }
.down { color: #26a17b; font-variant-numeric: tabular-nums; }

/* ── 分页 ───────────────────────────────────────────────────────────────── */
.pagination-wrap {
  display: flex;
  justify-content: flex-end;
  margin-top: 16px;
}
</style>
