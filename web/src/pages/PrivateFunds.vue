<script setup lang="ts">
import { ref, reactive, onMounted, computed } from 'vue'
import { useRouter } from 'vue-router'
import {
  fetchPrivateFunds,
  fetchPrivateManagers,
  fetchPrivateStats,
  fetchPrivateStates,
  fetchPrivateManagerTypes,
} from '@/api/privateFunds'
import type {
  PrivateFundSummary,
  PrivateManagerRow,
  PrivateFundStats,
  PrivateStateCount,
  PrivateTypeCount,
} from '@/types/privateFund'

const router = useRouter()

const stats  = ref<PrivateFundStats | null>(null)
const states = ref<PrivateStateCount[]>([])
const types  = ref<PrivateTypeCount[]>([])
const tab    = ref<'funds' | 'managers'>('funds')

// ── 产品列表 ────────────────────────────────────────────────────────────────
const fundLoading = ref(false)
const fundError   = ref('')
const fundList    = ref<PrivateFundSummary[]>([])
const fundTotal   = ref(0)

const fundFilter = reactive({
  q: '',
  state: '',
  hasNav: false,
  order: 'record',
  page: 1,
  pageSize: 50,
})

// ── 管理人列表 ──────────────────────────────────────────────────────────────
const mgrLoading = ref(false)
const mgrError   = ref('')
const mgrList    = ref<PrivateManagerRow[]>([])
const mgrTotal   = ref(0)

const mgrFilter = reactive({
  q: '',
  investType: '',
  page: 1,
  pageSize: 50,
})

let fundTimer: ReturnType<typeof setTimeout> | null = null
let mgrTimer: ReturnType<typeof setTimeout> | null = null

async function loadFunds() {
  fundLoading.value = true
  fundError.value = ''
  try {
    const resp = await fetchPrivateFunds({
      q: fundFilter.q,
      state: fundFilter.state,
      hasNav: fundFilter.hasNav,
      order: fundFilter.order,
      page: fundFilter.page,
      pageSize: fundFilter.pageSize,
    })
    fundList.value  = resp.rows
    fundTotal.value = resp.total
  } catch {
    fundError.value = '加载失败，请检查后端服务是否启动'
  } finally {
    fundLoading.value = false
  }
}

async function loadManagers() {
  mgrLoading.value = true
  mgrError.value = ''
  try {
    const resp = await fetchPrivateManagers({
      q: mgrFilter.q,
      investType: mgrFilter.investType,
      page: mgrFilter.page,
      pageSize: mgrFilter.pageSize,
    })
    mgrList.value  = resp.rows
    mgrTotal.value = resp.total
  } catch {
    mgrError.value = '加载失败，请检查后端服务是否启动'
  } finally {
    mgrLoading.value = false
  }
}

onMounted(async () => {
  loadFunds()
  try { stats.value = await fetchPrivateStats() } catch { /* 非阻塞 */ }
  try { states.value = await fetchPrivateStates() } catch { /* 非阻塞 */ }
})

// el-tabs 的 tab-change 回调参数类型是 TabPaneName（string | number），
// 直接标 string 会让 vue-tsc 报 TS2322。转成字符串再比。
async function onTabChange(name: string | number) {
  if (String(name) === 'managers') {
    if (!types.value.length) {
      try { types.value = await fetchPrivateManagerTypes() } catch { /* 非阻塞 */ }
    }
    if (!mgrList.value.length) loadManagers()
  }
}

function onFundFilterChange() { fundFilter.page = 1; loadFunds() }
function onFundSearch() {
  if (fundTimer) clearTimeout(fundTimer)
  fundTimer = setTimeout(() => { fundFilter.page = 1; loadFunds() }, 350)
}
function onFundPage(p: number) { fundFilter.page = p; loadFunds() }
function onFundPageSize(s: number) { fundFilter.pageSize = s; fundFilter.page = 1; loadFunds() }

function onMgrFilterChange() { mgrFilter.page = 1; loadManagers() }
function onMgrSearch() {
  if (mgrTimer) clearTimeout(mgrTimer)
  mgrTimer = setTimeout(() => { mgrFilter.page = 1; loadManagers() }, 350)
}
function onMgrPage(p: number) { mgrFilter.page = p; loadManagers() }
function onMgrPageSize(s: number) { mgrFilter.pageSize = s; mgrFilter.page = 1; loadManagers() }

function goDetail(row: { fundNo: string }) { router.push(`/private-fund/${row.fundNo}`) }

// ── Formatters ──────────────────────────────────────────────────────────────
function fmtDate(d: string | null) { return d ? d.slice(0, 10) : '—' }

function fmtNav(v: number | null) {
  return v == null ? '—'
    : v.toLocaleString('zh-CN', { minimumFractionDigits: 4, maximumFractionDigits: 4 })
}

function fmtPct(v: number | null) {
  if (v == null) return '—'
  return (v > 0 ? '+' : '') + v.toFixed(2) + '%'
}

/** 规模：源侧给的是元；量级小的一律按原值展示，不四舍五入成 0 */
function fmtSize(v: number | null) {
  if (v == null) return '—'
  const yi = v / 1e8
  if (yi >= 1) return yi.toFixed(2) + ' 亿'
  if (yi >= 0.01) return yi.toFixed(3) + ' 亿'
  return Math.round(v).toLocaleString('zh-CN') + ' 元'
}

function stateTagType(s: string | null): 'success' | 'warning' | 'info' | 'danger' {
  if (!s) return 'info'
  if (s.includes('正在运作')) return 'success'
  if (s.includes('清算'))    return 'warning'
  return 'info'
}

const coverageText = computed(() => {
  const c = stats.value?.navCoverage
  return c == null ? '—' : c.toFixed(2) + '%'
})

/**
 * 备案名录覆盖率 = 库内唯一备案编码 / 中基协自报总量。
 * ⚠ 必须展示：中基协分页无稳定排序（偏移量在相邻请求间漂移），多遍扫描也无法收敛到 100%；
 *   且分母里约 600 条是 2013~2016 年通道类资管计划（中基协未赋备案编码，主键缺失、抓了也写不进）。
 */
const regCoverageText = computed(() => {
  const s = stats.value
  if (!s || !s.registryCoverage) return '—'
  return s.registryCoverage.toFixed(2) + '%'
})
const regCoverageTip = computed(() => {
  const s = stats.value
  if (!s) return ''
  return `库内 ${s.registry.toLocaleString()} / 中基协自报 ${s.registrySource.toLocaleString()}`
})
</script>

<template>
  <div class="pf-page">
    <div class="page-header">
      <div>
        <h1>私募基金</h1>
        <p class="subtitle">
          备案名录 · 管理人 · 公开净值（天天基金代销池 + 私募排排网） —— 数据来源 中国证券投资基金业协会 / 天天基金 / 私募排排网
        </p>
      </div>
    </div>

    <!-- ⚠ 口径提示必须显著：私募与公募不是一个物种 -->
    <div class="scope-note">
      <div class="scope-row">
        <span class="scope-badge">口径</span>
        <span>
          私募<strong>不得公开宣传推介与披露业绩</strong>（《私募投资基金募集行为管理办法》），
          所以公开渠道只有 <strong>备案名录</strong> 能大规模获取；
          <strong>规模 / 持仓 / 净值没有全量数据</strong>。
          本页净值来自两个公开源，合计覆盖 <strong>{{ coverageText }}</strong>：
          <strong>天天基金「高端理财」代销池</strong>（单位 / 累计净值，池内多为券商资管产品）
          与 <strong>私募排排网可见名录</strong>（<strong>复权净值（分红再投）</strong>口径，
          该名录仅约 5,565 只 = 公开可见净值的全部天花板，占备案名录 2% 量级）——
          <strong>不代表全市场私募业绩</strong>。
          ⚠ 两源口径不同（累计净值 vs 复权净值），在「有分红」标的上终身收益最多差数倍，
          故<strong>同一只产品的净值为单源、不混拼</strong>；与代销池重叠的标的保持代销池原样。
          备案名录本身也<strong>不是全量</strong>：覆盖
          <strong>{{ regCoverageText }}</strong>（{{ regCoverageTip }}）。
          缺口来自中基协分页无稳定排序造成的偏移漂移，多遍扫描仍无法收敛到 100%；
          另有约 600 条 2013~2016 年通道类资管计划中基协未赋备案编码（无主键，抓了也入不了库）。
        </span>
      </div>
    </div>

    <!-- 统计卡 -->
    <div v-if="stats" class="stat-grid">
      <div class="stat-card">
        <div class="stat-label">备案产品</div>
        <div class="stat-value">{{ stats.registry.toLocaleString() }}</div>
        <div class="stat-sub" :title="regCoverageTip">
          名录覆盖率 {{ regCoverageText }}
        </div>
      </div>
      <div class="stat-card">
        <div class="stat-label">备案管理人</div>
        <div class="stat-value">{{ stats.managers.toLocaleString() }}</div>
        <div class="stat-sub">家</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">有净值的</div>
        <div class="stat-value">{{ stats.withNav.toLocaleString() }}</div>
        <div class="stat-sub">覆盖率 {{ coverageText }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">净值行数</div>
        <div class="stat-value">{{ stats.navRows.toLocaleString() }}</div>
        <div class="stat-sub">至 {{ fmtDate(stats.latestNavDate) }}</div>
      </div>
    </div>

    <el-tabs v-model="tab" @tab-change="onTabChange">
      <!-- ── 产品 ─────────────────────────────────────────────────────────── -->
      <el-tab-pane label="备案产品" name="funds">
        <el-alert v-if="fundError" :title="fundError" type="error" show-icon
                  :closable="false" style="margin-bottom: 16px" />

        <div class="filter-bar">
          <el-input v-model="fundFilter.q" placeholder="搜索产品名称 / 备案编码 / 管理人"
                    clearable style="width: 280px" @input="onFundSearch" @clear="onFundSearch">
            <template #prefix><el-icon><Search /></el-icon></template>
          </el-input>

          <el-select v-model="fundFilter.state" placeholder="全部运行状态" clearable
                     style="width: 170px" @change="onFundFilterChange">
            <el-option v-for="s in states" :key="s.state ?? ''"
                       :label="`${s.state ?? '未知'} (${s.count})`" :value="s.state ?? ''" />
          </el-select>

          <el-select v-model="fundFilter.order" style="width: 150px" @change="onFundFilterChange">
            <el-option label="按备案时间" value="record" />
            <el-option label="按成立时间" value="established" />
            <el-option label="按净值日期" value="nav" />
          </el-select>

          <el-checkbox v-model="fundFilter.hasNav" @change="onFundFilterChange">
            仅有净值
          </el-checkbox>

          <span v-if="fundTotal" class="total-badge">共 {{ fundTotal.toLocaleString() }} 只</span>
        </div>

        <el-table :data="fundList" v-loading="fundLoading" style="width: 100%"
                  class="pf-table" @row-click="goDetail"
                  :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }">
          <el-table-column prop="fundNo" label="备案编码" width="100">
            <template #default="{ row }"><span class="code-link">{{ row.fundNo }}</span></template>
          </el-table-column>
          <el-table-column prop="fundName" label="产品名称" min-width="230" show-overflow-tooltip>
            <template #default="{ row }"><span class="name-cell">{{ row.fundName }}</span></template>
          </el-table-column>
          <el-table-column prop="managerName" label="管理人" min-width="180" show-overflow-tooltip>
            <template #default="{ row }">{{ row.managerName ?? '—' }}</template>
          </el-table-column>
          <el-table-column prop="workingState" label="运行状态" width="110">
            <template #default="{ row }">
              <el-tag v-if="row.workingState" size="small" :type="stateTagType(row.workingState)" effect="light">
                {{ row.workingState }}
              </el-tag>
              <span v-else>—</span>
            </template>
          </el-table-column>
          <!-- 净值单源、不混拼：代销池是「累计净值」，排排网是「复权净值」，故表头不写死单位净值 -->
          <el-table-column label="最新净值" width="110" align="right">
            <template #header>
              <el-tooltip placement="top" content="代销池源=累计净值；排排网源=复权净值(分红再投)。同一产品只取一个源，不混拼">
                <span style="cursor: help">最新净值</span>
              </el-tooltip>
            </template>
            <template #default="{ row }">
              <span v-if="row.latestNav != null" class="num">{{ fmtNav(row.latestNav) }}</span>
              <span v-else class="dash">—</span>
            </template>
          </el-table-column>
          <el-table-column label="日涨跌" width="90" align="right">
            <template #default="{ row }">
              <span v-if="row.latestDailyReturn != null"
                    :class="row.latestDailyReturn > 0 ? 'up' : row.latestDailyReturn < 0 ? 'down' : ''">
                {{ fmtPct(row.latestDailyReturn) }}
              </span>
              <span v-else class="dash">—</span>
            </template>
          </el-table-column>
          <el-table-column label="净值日期" width="110" align="center">
            <template #default="{ row }">{{ fmtDate(row.latestNavDate) }}</template>
          </el-table-column>
          <el-table-column prop="mandatorName" label="托管人" min-width="150" show-overflow-tooltip>
            <template #default="{ row }">{{ row.mandatorName ?? '—' }}</template>
          </el-table-column>
          <el-table-column label="备案时间" width="110" align="center">
            <template #default="{ row }">{{ fmtDate(row.recordDate) }}</template>
          </el-table-column>
        </el-table>

        <div class="pagination-wrap">
          <el-pagination v-model:current-page="fundFilter.page" :page-size="fundFilter.pageSize"
                         :page-sizes="[20, 50, 100, 200]" :total="fundTotal"
                         layout="total, sizes, prev, pager, next, jumper"
                         @current-change="onFundPage" @size-change="onFundPageSize" />
        </div>
      </el-tab-pane>

      <!-- ── 管理人 ───────────────────────────────────────────────────────── -->
      <el-tab-pane label="管理人" name="managers">
        <el-alert v-if="mgrError" :title="mgrError" type="error" show-icon
                  :closable="false" style="margin-bottom: 16px" />

        <div class="filter-bar">
          <el-input v-model="mgrFilter.q" placeholder="搜索管理人名称" clearable
                    style="width: 260px" @input="onMgrSearch" @clear="onMgrSearch">
            <template #prefix><el-icon><Search /></el-icon></template>
          </el-input>

          <el-select v-model="mgrFilter.investType" placeholder="全部机构类型" clearable
                     style="width: 220px" @change="onMgrFilterChange">
            <el-option v-for="t in types" :key="t.investType ?? ''"
                       :label="`${t.investType ?? '未分类'} (${t.count})`" :value="t.investType ?? ''" />
          </el-select>

          <span v-if="mgrTotal" class="total-badge">共 {{ mgrTotal.toLocaleString() }} 家</span>
        </div>

        <el-table :data="mgrList" v-loading="mgrLoading" style="width: 100%"
                  class="pf-table"
                  :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }">
          <el-table-column prop="managerName" label="管理人名称" min-width="230" show-overflow-tooltip>
            <template #default="{ row }"><span class="name-cell">{{ row.managerName }}</span></template>
          </el-table-column>
          <el-table-column prop="registerNo" label="登记编号" width="110">
            <template #default="{ row }"><span class="mono">{{ row.registerNo }}</span></template>
          </el-table-column>
          <el-table-column prop="artificialPerson" label="法定代表人" width="110" show-overflow-tooltip>
            <template #default="{ row }">{{ row.artificialPerson ?? '—' }}</template>
          </el-table-column>
          <el-table-column prop="investType" label="机构类型" min-width="150" show-overflow-tooltip>
            <template #default="{ row }">
              <el-tag v-if="row.investType" size="small" effect="light">{{ row.investType }}</el-tag>
              <span v-else>—</span>
            </template>
          </el-table-column>
          <el-table-column label="在管基金" width="100" align="right">
            <template #default="{ row }">
              <span v-if="row.fundCount != null" class="num">{{ row.fundCount }}</span>
              <span v-else class="dash">—</span>
            </template>
          </el-table-column>
          <el-table-column prop="registerProvince" label="注册地" width="100">
            <template #default="{ row }">{{ row.registerProvince ?? '—' }}</template>
          </el-table-column>
          <el-table-column label="登记时间" width="110" align="center">
            <template #default="{ row }">{{ fmtDate(row.registerDate) }}</template>
          </el-table-column>
          <el-table-column label="诚信提示" width="90" align="center">
            <template #default="{ row }">
              <el-tag v-if="row.hasCreditTips" size="small" type="danger" effect="light">有</el-tag>
              <span v-else class="dash">—</span>
            </template>
          </el-table-column>
        </el-table>

        <div class="pagination-wrap">
          <el-pagination v-model:current-page="mgrFilter.page" :page-size="mgrFilter.pageSize"
                         :page-sizes="[20, 50, 100, 200]" :total="mgrTotal"
                         layout="total, sizes, prev, pager, next, jumper"
                         @current-change="onMgrPage" @size-change="onMgrPageSize" />
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<style scoped>
.pf-page { max-width: 1280px; margin: 0 auto; padding: 36px 20px 60px; }

.page-header { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 20px; }

h1 { margin: 0 0 4px; font-size: 26px; font-weight: 700; color: #1a1a2e; }
.subtitle { margin: 0; color: #888; font-size: 14px; }

/* ── 口径提示条 ─────────────────────────────────────────────────────────── */
.scope-note {
  background: #fff8e6;
  border: 1px solid #f5d99b;
  border-radius: 10px;
  padding: 12px 16px;
  margin-bottom: 20px;
}
.scope-row { display: flex; gap: 10px; align-items: flex-start; font-size: 13px; color: #6b5316; line-height: 1.75; }
.scope-badge {
  flex: none;
  background: #f0b429;
  color: #fff;
  border-radius: 4px;
  padding: 1px 7px;
  font-size: 12px;
  font-weight: 600;
  margin-top: 2px;
}
.scope-note strong { color: #8a6100; }

/* ── 统计卡 ─────────────────────────────────────────────────────────────── */
.stat-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  gap: 12px;
  margin-bottom: 20px;
}
.stat-card { background: #f7f8fa; border-radius: 10px; padding: 14px 16px; }
.stat-label { font-size: 13px; color: #888; margin-bottom: 6px; }
.stat-value { font-size: 24px; font-weight: 500; color: #1a1a2e; font-variant-numeric: tabular-nums; }
.stat-sub { font-size: 12px; color: #aaa; margin-top: 2px; }

/* ── 筛选栏 ─────────────────────────────────────────────────────────────── */
.filter-bar { display: flex; align-items: center; gap: 14px; margin-bottom: 16px; flex-wrap: wrap; }

.total-badge {
  font-size: 13px; color: #409eff; background: #e8f3ff;
  padding: 4px 12px; border-radius: 12px; font-weight: 600; white-space: nowrap;
}

/* ── 表格 ───────────────────────────────────────────────────────────────── */
.pf-table { background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 4px rgba(0, 0, 0, 0.05); }

:deep(.el-table__row) { cursor: pointer; }
:deep(.el-table__row:hover) .code-link { text-decoration: underline; }

.code-link { color: #409eff; font-variant-numeric: tabular-nums; font-weight: 500; }
.name-cell { font-weight: 500; color: #1a1a2e; }
.mono { font-variant-numeric: tabular-nums; color: #666; }
.num { font-variant-numeric: tabular-nums; font-weight: 600; color: #1a1a2e; }
.dash { color: #ccc; }

/* 涨红跌绿（中式约定） */
.up   { color: #e8534a; font-variant-numeric: tabular-nums; }
.down { color: #26a17b; font-variant-numeric: tabular-nums; }

.pagination-wrap { display: flex; justify-content: flex-end; margin-top: 16px; }
</style>
