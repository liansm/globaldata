<script setup lang="ts">
// ── 基金信息 ─────────────────────────────────────────────────────────────────
// 原先 sidebar 上是三个平行项：公募基金 / 基金公司 / 私募基金。三者其实是同一个
// 提问角度下的三层（产品 → 管理人 → 另一类产品），分成三个导航项之后：
//   · sidebar 被基金占掉三行，比「大宗商品」「全球股市」这类一级域还多；
//   · 从「公募基金」跳到「基金公司」要回 sidebar 再点，看不出两者是关联的。
// 现在合并成一页三个 tab，sidebar 只留一项。各 tab 的内容整体搬到
// components/{Funds,Companies,PrivateFunds}Panel.vue，**只去掉页面级容器和 h1**，
// 表格/筛选/分页逻辑一行未改（搬迁不改行为）。
//
// ⚠ tab 必须与 URL query 双向绑定：详情页（/fund/:code 等）返回时要能落回对的 tab，
//   旧链接 /funds /companies /private-funds 也靠 redirect + ?tab= 落到这里，
//   见 router/index.ts。默认不写 ?tab=（公开基金是最常用入口，URL 干净些）。
// ⚠ el-tab-pane 默认 lazy：只有被激活过的 tab 才会挂载并请求接口。三个 tab 的数据量
//   都很大（公募 2.7 万只、私募名录 20 万+），一次性全拉会白等三份请求。
import { ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import FundsPanel from '@/components/FundsPanel.vue'
import CompaniesPanel from '@/components/CompaniesPanel.vue'
import PrivateFundsPanel from '@/components/PrivateFundsPanel.vue'

type TabName = 'public' | 'companies' | 'private'
const TABS: TabName[] = ['public', 'companies', 'private']

const route = useRoute()
const router = useRouter()

/** 未知/缺失的 ?tab= 一律落到公募基金（宁可回默认，也不要白屏） */
function normalize(v: unknown): TabName {
  return TABS.includes(v as TabName) ? (v as TabName) : 'public'
}

const tab = ref<TabName>(normalize(route.query.tab))
watch(() => route.query.tab, t => { tab.value = normalize(t) })

function onTabChange(name: string | number) {
  // el-tabs 先改 modelValue 再抛 tab-change，这里只对齐 URL，避免多余导航。
  const want = normalize(name)
  if (want === normalize(route.query.tab)) return
  router.replace(want === 'public' ? { name: 'funds-info' } : { name: 'funds-info', query: { tab: want } })
}
</script>

<template>
  <div class="funds-info-page">
    <div class="page-header">
      <div>
        <h1>基金信息</h1>
        <p class="subtitle">
          公募基金名录与净值 · 基金公司（管理人）· 私募备案与公开净值
        </p>
      </div>
    </div>

    <el-tabs v-model="tab" class="fi-tabs" @tab-change="onTabChange">
      <el-tab-pane name="public">
        <template #label><span class="tab-label">💰 公募基金</span></template>
        <FundsPanel />
      </el-tab-pane>

      <el-tab-pane name="companies">
        <template #label><span class="tab-label">🏢 基金公司</span></template>
        <CompaniesPanel />
      </el-tab-pane>

      <el-tab-pane name="private">
        <template #label><span class="tab-label">🏦 私募基金</span></template>
        <PrivateFundsPanel />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<style scoped>
/* 宽度取三个子页原来的最大值（基金公司表最宽，1320），窄的两个由 el-table
   的 width:100% 自适应撑开，不会留空白。 */
.funds-info-page {
  max-width: 1320px;
  margin: 0 auto;
  padding: 36px 20px 60px;
}

.page-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 18px;
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

.tab-label { font-size: 14px; font-weight: 500; }

/* el-tabs 头部与内容之间留点距离；私募 tab 内部还有一层 card 型 tab，
   这里只保证外层是默认下划线型，两层不会混在一起。 */
:deep(.el-tabs__header) { margin-bottom: 18px; }
</style>
