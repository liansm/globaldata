<script setup lang="ts">
// 「私募基金管理人」详情页：管理人备案信息 + 旗下基金列表（通过 manager_id 关联）。
// 入口来自 PrivateFundsPanel.vue 管理人 tab 的行点击，URL = /private-manager/:registerNo。
// ⚠ 口径与私募总页一致：净值仅覆盖三个公开源（见 PrivateFundsPanel 的 scope-note），
//   本页只补一条精简提示，不重复整段。
import { ref, reactive, onMounted, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  fetchPrivateManagerDetail,
  fetchPrivateStates,
} from '@/api/privateFunds'
import type {
  PrivateFundSummary,
  PrivateManagerInfo,
  PrivateManagerStats,
  PrivateStateCount,
} from '@/types/privateFund'

const route = useRoute()
const router = useRouter()
const registerNo = computed(() => String(route.params.registerNo ?? ''))

const loading = ref(false)
const error   = ref('')
const notFound = ref(false)
const manager = ref<PrivateManagerInfo | null>(null)
const stats   = ref<PrivateManagerStats | null>(null)
const fundList = ref<PrivateFundSummary[]>([])
const fundTotal = ref(0)

const states = ref<PrivateStateCount[]>([])

const filter = reactive({
  q: '',
  state: '',
  hasNav: false,
  order: 'record',
  page: 1,
  pageSize: 50,
})

let timer: ReturnType<typeof setTimeout> | null = null

async function load() {
  loading.value = true
  error.value = ''
  notFound.value = false
  try {
    const resp = await fetchPrivateManagerDetail(registerNo.value, {
      q: filter.q,
      state: filter.state,
      hasNav: filter.hasNav,
      order: filter.order,
      page: filter.page,
      pageSize: filter.pageSize,
    })
    if (!resp.found) {
      notFound.value = true
      // 切换管理人（A → 不存在的 B）时清掉上一家的数据，否则头部/统计会残留旧值
      manager.value = null
      stats.value = null
      fundList.value = []
      fundTotal.value = 0
      return
    }
    manager.value = resp.manager ?? null
    stats.value   = resp.stats ?? null
    fundList.value = resp.rows
    fundTotal.value = resp.total
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  load()
  try { states.value = await fetchPrivateStates() } catch { /* 非阻塞 */ }
})

// 切换不同管理人（路由参数变化）时重新加载
watch(registerNo, () => {
  filter.page = 1
  load()
})

function onFilterChange() { filter.page = 1; load() }
function onSearch() {
  if (timer) clearTimeout(timer)
  timer = setTimeout(() => { filter.page = 1; load() }, 350)
}
function onPage(p: number) { filter.page = p; load() }
function onPageSize(s: number) { filter.pageSize = s; filter.page = 1; load() }
function goFund(row: { fundNo: string }) { router.push(`/private-fund/${row.fundNo}`) }
function goBack() {
  // 从「管理人」tab 点进来的要落回管理人 tab（?sub=managers），
  // 从产品列表/产品详情点进来的落回备案产品 tab，别一律丢回第一页。
  const query: Record<string, string> = { tab: 'private' }
  if (route.query.from === 'managers') query.sub = 'managers'
  router.push({ name: 'funds-info', query })
}

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
function stateTagType(s: string | null): 'success' | 'warning' | 'info' | 'danger' {
  if (!s) return 'info'
  if (s.includes('正在运作')) return 'success'
  if (s.includes('清算'))    return 'warning'
  return 'info'
}
</script>

<template>
  <div class="pmd-page">
    <div class="page-header">
      <div>
        <el-button text @click="goBack" class="back-btn"><el-icon><ArrowLeft /></el-icon>返回私募基金</el-button>
        <h1 v-if="manager">{{ manager.managerName }}</h1>
        <h1 v-else-if="notFound">管理人不存在</h1>
        <h1 v-else>私募基金管理人</h1>
        <p class="subtitle">登记编号 {{ manager?.registerNo ?? registerNo }}</p>
      </div>
    </div>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" style="margin-bottom: 16px" />
    <el-alert v-else-if="notFound" type="info" :closable="false"
              :title="`未找到登记编号为 ${registerNo} 的管理人`" style="margin-bottom: 16px" />

    <!-- 管理人信息卡 -->
    <div v-if="manager" class="mgr-card">
      <div class="mgr-grid">
        <div class="mgr-item"><span class="lbl">法定代表人</span><span class="val">{{ manager.artificialPerson ?? '—' }}</span></div>
        <div class="mgr-item"><span class="lbl">机构类型</span><span class="val">{{ manager.investType ?? '—' }}</span></div>
        <div class="mgr-item"><span class="lbl">会员类型</span><span class="val">{{ manager.memberType ?? '—' }}</span></div>
        <div class="mgr-item"><span class="lbl">注册地</span><span class="val">{{ manager.registerProvince ?? '—' }}</span></div>
        <div class="mgr-item"><span class="lbl">登记时间</span><span class="val">{{ fmtDate(manager.registerDate) }}</span></div>
        <div class="mgr-item"><span class="lbl">成立时间</span><span class="val">{{ fmtDate(manager.establishDate) }}</span></div>
      </div>
      <div class="mgr-extra">
        <div class="kpi">
          <div class="kpi-val">{{ manager.fundCount != null ? manager.fundCount.toLocaleString() : '—' }}</div>
          <div class="kpi-lbl">在管产品数<span class="kpi-note">（中基协公示）</span></div>
        </div>
        <div class="kpi">
          <div class="kpi-val">{{ stats ? stats.totalFunds.toLocaleString() : '—' }}</div>
          <div class="kpi-lbl">库内旗下产品</div>
        </div>
        <div class="kpi">
          <div class="kpi-val">{{ stats ? stats.withNav.toLocaleString() : '—' }}</div>
          <div class="kpi-lbl">其中有净值</div>
        </div>
        <el-tag v-if="manager.hasCreditTips" type="danger" effect="light" size="small">诚信提示</el-tag>
      </div>
    </div>

    <!-- 旗下基金（管理人不存在时不展示空表） -->
    <template v-if="!notFound">
    <div class="section-title">
      <span>旗下基金</span>
      <span v-if="fundTotal" class="total-badge">共 {{ fundTotal.toLocaleString() }} 只</span>
    </div>

    <div class="scope-mini">
      净值仅覆盖三个公开源（天天基金高端理财代销池 / 私募排排网 / 格上理财），合计约 2.8%，不代表全市场业绩。
    </div>

    <div class="filter-bar">
      <el-input v-model="filter.q" placeholder="搜索产品名称 / 备案编码" clearable
                style="width: 280px" @input="onSearch" @clear="onSearch">
        <template #prefix><el-icon><Search /></el-icon></template>
      </el-input>

      <el-select v-model="filter.state" placeholder="全部运行状态" clearable
                 style="width: 170px" @change="onFilterChange">
        <el-option v-for="s in states" :key="s.state ?? ''"
                   :label="`${s.state ?? '未知'} (${s.count})`" :value="s.state ?? ''" />
      </el-select>

      <el-select v-model="filter.order" style="width: 150px" @change="onFilterChange">
        <el-option label="按备案时间" value="record" />
        <el-option label="按成立时间" value="established" />
        <el-option label="按净值日期" value="nav" />
      </el-select>

      <el-checkbox v-model="filter.hasNav" @change="onFilterChange">仅有净值</el-checkbox>
    </div>

    <el-table :data="fundList" v-loading="loading" style="width: 100%"
              class="pf-table" @row-click="goFund"
              :header-cell-style="{ background: '#fafbfc', color: '#555', fontWeight: 600 }">
      <el-table-column prop="fundNo" label="备案编码" width="100">
        <template #default="{ row }"><span class="code-link">{{ row.fundNo }}</span></template>
      </el-table-column>
      <el-table-column prop="fundName" label="产品名称" min-width="230" show-overflow-tooltip>
        <template #default="{ row }"><span class="name-cell">{{ row.fundName }}</span></template>
      </el-table-column>
      <el-table-column prop="workingState" label="运行状态" width="110">
        <template #default="{ row }">
          <el-tag v-if="row.workingState" size="small" :type="stateTagType(row.workingState)" effect="light">
            {{ row.workingState }}
          </el-tag>
          <span v-else>—</span>
        </template>
      </el-table-column>
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
      <el-pagination v-model:current-page="filter.page" :page-size="filter.pageSize"
                     :page-sizes="[20, 50, 100, 200]" :total="fundTotal"
                     layout="total, sizes, prev, pager, next, jumper"
                     @current-change="onPage" @size-change="onPageSize" />
    </div>
    </template>
  </div>
</template>

<style scoped>
.pmd-page { max-width: 1320px; margin: 0 auto; padding: 28px 20px 60px; }

.page-header { margin-bottom: 16px; }
.back-btn { margin: 0 0 6px -8px; color: #888; }
h1 { margin: 0 0 4px; font-size: 24px; font-weight: 700; color: #1a1a2e; }
.subtitle { margin: 0; color: #999; font-size: 13px; font-variant-numeric: tabular-nums; }

/* 管理人卡 */
.mgr-card {
  background: #f7f8fa; border-radius: 12px; padding: 18px 20px; margin-bottom: 22px;
  display: flex; flex-wrap: wrap; gap: 18px 40px; align-items: center; justify-content: space-between;
}
.mgr-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px 28px; flex: 1; min-width: 320px; }
.mgr-item { display: flex; flex-direction: column; gap: 3px; }
.mgr-item .lbl { font-size: 12px; color: #999; }
.mgr-item .val { font-size: 14px; color: #1a1a2e; font-weight: 500; }
.mgr-extra { display: flex; align-items: center; gap: 22px; }
.kpi { text-align: center; }
.kpi-val { font-size: 22px; font-weight: 600; color: #1a1a2e; font-variant-numeric: tabular-nums; }
.kpi-lbl { font-size: 12px; color: #999; margin-top: 2px; }
.kpi-note { color: #bbb; }

.section-title { display: flex; align-items: center; gap: 12px; font-size: 17px; font-weight: 600; color: #1a1a2e; margin-bottom: 10px; }
.total-badge { font-size: 13px; color: #409eff; background: #e8f3ff; padding: 4px 12px; border-radius: 12px; font-weight: 600; }

.scope-mini { font-size: 12px; color: #b08900; background: #fff8e6; border: 1px solid #f5d99b; border-radius: 8px; padding: 8px 12px; margin-bottom: 14px; }

.filter-bar { display: flex; align-items: center; gap: 14px; margin-bottom: 16px; flex-wrap: wrap; }
.pf-table { background: #fff; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 4px rgba(0,0,0,0.05); }
:deep(.el-table__row) { cursor: pointer; }
:deep(.el-table__row:hover) .code-link { text-decoration: underline; }
.code-link { color: #409eff; font-variant-numeric: tabular-nums; font-weight: 500; }
.name-cell { font-weight: 500; color: #1a1a2e; }
.num { font-variant-numeric: tabular-nums; font-weight: 600; color: #1a1a2e; }
.dash { color: #ccc; }
.up   { color: #e8534a; font-variant-numeric: tabular-nums; }
.down { color: #26a17b; font-variant-numeric: tabular-nums; }
.pagination-wrap { display: flex; justify-content: flex-end; margin-top: 16px; }
</style>
