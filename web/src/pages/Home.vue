<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Search } from '@element-plus/icons-vue'
import { fetchCommodities } from '@/api/commodities'
import type { Commodity } from '@/types/commodity'

const router = useRouter()
const loading = ref(false)
const error = ref('')
const list = ref<Commodity[]>([])
const search = ref('')

onMounted(async () => {
  loading.value = true
  try {
    list.value = await fetchCommodities()
  } catch (e) {
    error.value = '加载失败，请检查后端服务是否启动'
  } finally {
    loading.value = false
  }
})

const filtered = computed(() => {
  const q = search.value.trim().toLowerCase()
  if (!q) return list.value
  return list.value.filter(
    r => r.commodity.toLowerCase().includes(q) || r.key.toLowerCase().includes(q),
  )
})

function formatPrice(price: number | null, unit: string | null) {
  if (price == null) return '—'
  return `${price.toLocaleString('zh-CN', { maximumFractionDigits: 2 })}${unit ? ' ' + unit : ''}`
}

function formatDate(d: string | null) {
  if (!d) return '—'
  return d.slice(0, 10)
}

function goDetail(row: Commodity) {
  router.push({ name: 'detail', params: { key: row.key } })
}
</script>

<template>
  <div class="home-page">
    <!-- 页头 -->
    <div class="page-header">
      <div class="title-block">
        <h1>全球大宗商品价格</h1>
        <p class="subtitle">实时追踪全球主要商品现货 &amp; 期货价格</p>
      </div>
      <el-input
        v-model="search"
        placeholder="搜索商品名称 / Key"
        :prefix-icon="Search"
        clearable
        class="search-input"
      />
    </div>

    <!-- 错误提示 -->
    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false" style="margin-bottom:16px" />

    <!-- 数据表格 -->
    <el-table
      v-loading="loading"
      :data="filtered"
      stripe
      highlight-current-row
      @row-click="goDetail"
      style="width:100%;cursor:pointer"
    >
      <el-table-column label="商品" prop="commodity" min-width="160" />
      <el-table-column label="Key" prop="key" min-width="120">
        <template #default="{ row }">
          <el-tag size="small" type="info">{{ row.key }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="价格类型" prop="priceType" min-width="100">
        <template #default="{ row }">
          <el-tag
            v-if="row.priceType"
            size="small"
            :type="row.priceType === 'futures' ? 'warning' : 'success'"
          >
            {{ row.priceType === 'futures' ? '期货' : '现货' }}
          </el-tag>
          <span v-else>—</span>
        </template>
      </el-table-column>
      <el-table-column label="最新价格" min-width="150" align="right">
        <template #default="{ row }">
          <span class="price-cell">{{ formatPrice(row.latestPrice, row.unit) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="最新日期" min-width="110" align="center">
        <template #default="{ row }">
          {{ formatDate(row.latestDate) }}
        </template>
      </el-table-column>
      <el-table-column label="" width="80" align="center">
        <template #default>
          <el-text type="primary" size="small">查看 →</el-text>
        </template>
      </el-table-column>
    </el-table>

    <div v-if="!loading && filtered.length === 0 && !error" class="empty-hint">
      暂无匹配商品
    </div>
  </div>
</template>

<style scoped>
.home-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 32px 20px;
}

.page-header {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 24px;
  flex-wrap: wrap;
}

h1 {
  margin: 0 0 4px;
  font-size: 26px;
  font-weight: 700;
  color: #1a1a2e;
}

.subtitle {
  margin: 0;
  color: #666;
  font-size: 14px;
}

.search-input {
  width: 260px;
}

.price-cell {
  font-variant-numeric: tabular-nums;
  font-weight: 600;
  color: #1a1a2e;
}

.empty-hint {
  text-align: center;
  padding: 40px;
  color: #aaa;
  font-size: 14px;
}
</style>
