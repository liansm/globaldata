<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { fetchStocks, fetchStockFilters } from '@/api/stocks'
import type { StockItem, StockFilters } from '@/types/stocks'

const router = useRouter()

const loading = ref(false)
const error = ref('')
const rows = ref<StockItem[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(20)

const q = ref('')
const exchange = ref('')
const board = ref('')
const region = ref('')
const industry = ref('')
const tag = ref('')
const sort = ref('code')

const filters = ref<StockFilters>({
  exchanges: [], boards: [], regions: [], industries: [], tags: [], total: 0,
})

const SORT_OPTIONS = [
  { label: '按代码', value: 'code' },
  { label: '按总市值', value: 'marketCap' },
  { label: '按涨跌幅', value: 'changePct' },
]

let debounce: number | undefined
function onSearchInput() {
  if (debounce) window.clearTimeout(debounce)
  debounce = window.setTimeout(() => { page.value = 1; load() }, 300)
}
function immediateSearch() {
  if (debounce) window.clearTimeout(debounce)
  page.value = 1
  load()
}
function reset() {
  q.value = ''; exchange.value = ''; board.value = ''
  region.value = ''; industry.value = ''; tag.value = ''; sort.value = 'code'
  page.value = 1
  load()
}
function onPage(p: number) { page.value = p; load() }

async function load() {
  loading.value = true
  error.value = ''
  try {
    const r = await fetchStocks({
      q: q.value.trim() || undefined,
      exchange: exchange.value || undefined,
      board: board.value || undefined,
      region: region.value || undefined,
      industry: industry.value || undefined,
      tag: tag.value || undefined,
      sort: sort.value,
      page: page.value,
      pageSize: pageSize.value,
    })
    rows.value = r.rows
    total.value = r.total
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
    rows.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

onMounted(async () => {
  try { filters.value = await fetchStockFilters() } catch { /* 下拉为空也能用搜索 */ }
  load()
})
watch([exchange, board, region, industry, tag, sort], () => { page.value = 1; load() })

function openDetail(row: StockItem) {
  router.push({ name: 'stock-detail', params: { code: row.code } })
}

// ── 展示格式化（口径唯一出处）────────────────────────────────────────────────
function fmtShares(v: number | null): string {
  if (v == null) return '—'
  if (v >= 1e8) return `${(v / 1e8).toFixed(2)} 亿股`
  if (v >= 1e4) return `${(v / 1e4).toFixed(2)} 万股`
  return `${v.toLocaleString('zh-CN')} 股`
}
function fmtCap(v: number | null): string {
  if (v == null) return '—'
  if (v >= 1e12) return `¥${(v / 1e12).toFixed(2)} 万亿`
  if (v >= 1e8) return `¥${(v / 1e8).toFixed(2)} 亿`
  return `¥${v.toLocaleString('zh-CN')}`
}
function fmtPrice(v: number | null): string {
  return v == null ? '—' : v.toFixed(2)
}
/** 财务列：空值显示「—」，不用 0 充数 */
function fmtNum(v: number | null | undefined): string {
  return v == null ? '—' : v.toFixed(2)
}
function fmtPct(v: number | null): string {
  if (v == null) return '—'
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`
}
// A 股习惯：涨红跌绿
function pctClass(v: number | null): string {
  if (v == null || v === 0) return 'flat'
  return v > 0 ? 'up' : 'down'
}
const activeCount = computed(() =>
  [q.value, exchange.value, board.value, region.value, industry.value, tag.value]
    .filter(Boolean).length)
</script>

<template>
  <div class="stocks-page">
    <div class="page-head">
      <div>
        <h1>股票信息</h1>
        <p class="sub">A 股 · 沪市主板 / 深市主板 / 创业板 / 科创板 · 收录 {{ filters.total.toLocaleString('zh-CN') }} 只</p>
      </div>
      <div class="head-note">数据源：东方财富（免费）<br />日线为不复权价</div>
    </div>

    <div class="filter-bar">
      <el-input
        v-model="q" placeholder="搜索代码 / 名称 / 拼音首字母，如 600519、茅台、ZSLC" clearable
        style="width: 330px" @input="onSearchInput" @keyup.enter="immediateSearch"
        @clear="immediateSearch"
      />
      <el-select v-model="exchange" placeholder="交易所" clearable style="width: 120px">
        <el-option v-for="x in filters.exchanges" :key="x" :label="x" :value="x" />
      </el-select>
      <el-select v-model="board" placeholder="板块" clearable style="width: 130px">
        <el-option v-for="x in filters.boards" :key="x" :label="x" :value="x" />
      </el-select>
      <el-select v-model="region" placeholder="地域" clearable filterable style="width: 130px">
        <el-option v-for="x in filters.regions" :key="x" :label="x" :value="x" />
      </el-select>
      <el-select v-model="industry" placeholder="证监会行业" clearable filterable style="width: 190px">
        <el-option v-for="x in filters.industries" :key="x" :label="x" :value="x" />
      </el-select>
      <el-select v-model="tag" placeholder="细分行业 / 概念" clearable filterable style="width: 190px">
        <el-option v-for="x in filters.tags" :key="x.name" :label="`${x.name} (${x.count})`" :value="x.name" />
      </el-select>
      <el-select v-model="sort" style="width: 125px">
        <el-option v-for="x in SORT_OPTIONS" :key="x.value" :label="x.label" :value="x.value" />
      </el-select>
      <el-button @click="reset">清空</el-button>
    </div>

    <div class="result-bar">
      <span>共 <b>{{ total.toLocaleString('zh-CN') }}</b> 条结果</span>
      <span v-if="activeCount" class="hint">已启用 {{ activeCount }} 个筛选条件</span>
      <span v-if="rows.length" class="hint">最新行情日 {{ rows[0]?.quoteDate || '—' }}</span>
    </div>

    <div v-if="error" class="error-box">{{ error }}</div>

    <el-table
      :data="rows" v-loading="loading" class="stock-table"
      row-class-name="clickable-row" @row-click="openDetail"
      empty-text="没有符合条件的股票"
    >
      <el-table-column prop="code" label="代码" width="100">
        <template #default="{ row }"><span class="code-cell">{{ row.code }}</span></template>
      </el-table-column>
      <el-table-column prop="name" label="名称" min-width="110">
        <template #default="{ row }"><span class="name-cell">{{ row.name }}</span></template>
      </el-table-column>
      <el-table-column label="交易所 / 板块" width="170">
        <template #default="{ row }">
          <span class="muted">{{ row.exchange }}</span>
          <el-tag size="small" class="board-tag">{{ row.board }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column prop="region" label="地域" width="90">
        <template #default="{ row }">{{ row.region || '—' }}</template>
      </el-table-column>
      <el-table-column label="所属行业" min-width="180">
        <template #default="{ row }">
          <span :title="row.industryCsrc || ''">{{ row.industry || row.industryCsrc || '—' }}</span>
        </template>
      </el-table-column>
      <el-table-column label="总股本" width="120" align="right">
        <template #default="{ row }">{{ fmtShares(row.totalShares) }}</template>
      </el-table-column>
      <el-table-column label="总市值" width="120" align="right">
        <template #default="{ row }">{{ fmtCap(row.marketCap) }}</template>
      </el-table-column>
      <el-table-column label="市盈率" width="100" align="right">
        <template #default="{ row }">
          <span :title="row.peTtm == null ? '源侧无数据' : 'TTM 口径'">{{ fmtNum(row.peTtm) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="股息率" width="100" align="right">
        <template #default="{ row }">
          <span :title="row.dividendYield == null ? '近 12 个月无已实施分红' : '近12月已实施分红 ÷ 最新价'">
            {{ fmtNum(row.dividendYield) }}<template v-if="row.dividendYield != null">%</template>
          </span>
        </template>
      </el-table-column>
      <el-table-column label="最新价" width="100" align="right">
        <template #default="{ row }">{{ fmtPrice(row.latestPrice) }}</template>
      </el-table-column>
      <el-table-column label="涨跌幅" width="100" align="right">
        <template #default="{ row }">
          <span :class="pctClass(row.changePct)">{{ fmtPct(row.changePct) }}</span>
        </template>
      </el-table-column>
    </el-table>

    <div class="pager">
      <el-pagination
        background layout="total, prev, pager, next, jumper"
        :total="total" :page-size="pageSize" :current-page="page"
        @current-change="onPage"
      />
    </div>
  </div>
</template>

<style scoped>
.stocks-page { padding: 24px 28px 40px; max-width: 1500px; }
.page-head { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 18px; }
h1 { margin: 0 0 6px; font-size: 22px; font-weight: 650; color: #1a1a2e; }
.sub { margin: 0; font-size: 13px; color: #888; }
.head-note { font-size: 12px; color: #9aa0ac; text-align: right; line-height: 1.7; }

.filter-bar {
  display: flex; flex-wrap: wrap; gap: 10px; align-items: center;
  background: #fff; border: 1px solid #eceef3; border-radius: 10px;
  padding: 14px 16px; margin-bottom: 12px;
}
.result-bar {
  display: flex; gap: 18px; align-items: center; font-size: 13px; color: #666;
  padding: 2px 4px 10px;
}
.result-bar b { color: #409eff; }
.hint { color: #9aa0ac; }

.stock-table { background: #fff; border-radius: 10px; overflow: hidden; }
:deep(.clickable-row) { cursor: pointer; }
:deep(.el-table__row:hover > td) { background: #f4f9ff !important; }
.code-cell { font-family: ui-monospace, Consolas, monospace; font-weight: 600; color: #409eff; }
.name-cell { font-weight: 600; color: #1a1a2e; }
.muted { color: #888; margin-right: 6px; }
.board-tag { margin-left: 2px; }
.up { color: #e04b4b; font-weight: 600; }
.down { color: #17a673; font-weight: 600; }
.flat { color: #888; }
.error-box { padding: 12px 16px; background: #fff2f0; color: #c0392b; border-radius: 8px; margin-bottom: 12px; }
.pager { display: flex; justify-content: flex-end; margin-top: 16px; }
</style>
