<script setup lang="ts">
import { RouterView, useRoute } from 'vue-router'
import { computed } from 'vue'
import { NAV_TO, detailNav, navForPath } from '@/lib/nav'

const route = useRoute()

// ⚠ 「全球股市」的 names 里**不能**再放 `market-detail`：/market/:key 也承载航运运价
//   和建材 CEMPI（route.name 全是 market-detail），放进去就会出现
//   「点 CCFI 综合指数，左侧亮全球股市」。详情页归属统一由 activeTo 解析。
const navItems = [
  { label: '大宗商品', icon: '📦', to: '/',        names: ['home', 'detail'] },
  // 航运信息挂在「大宗商品」下面：运价指数（原大宗商品页底部那几组）+ 油船信息（原「油运信息」页）
  { label: '航运信息', icon: '🚢', to: '/shipping', names: ['shipping'] },
  { label: '全球股市', icon: '📈', to: '/markets',  names: ['markets'] },
  { label: '加密货币', icon: '₿',  to: '/crypto',   names: ['crypto'] },
  { label: '外汇',     icon: '💱', to: '/fx',       names: ['fx', 'fx-detail'] },
  { label: '公募基金', icon: '💰', to: '/funds',    names: ['funds', 'fund-detail'] },
  { label: '基金公司', icon: '🏢', to: '/companies', names: ['companies', 'company-detail'] },
  { label: '私募基金', icon: '🏦', to: '/private-funds', names: ['private-funds', 'private-fund-detail'] },
  { label: '新股日历', icon: '🆕', to: '/ipo-calendar', names: ['ipo-calendar'] },
]

// 当前该高亮哪一项（返回路由地址）。多数页面靠 route.name 直接命中；
// market-detail 要先知道「这条指数属于哪个市场」，见 lib/nav.ts 的说明。
const activeTo = computed<string | null>(() => {
  const name = route.name as string
  if (name === 'market-detail') {
    const grp = detailNav.value ?? navForPath(window.history.state?.back)
    return grp ? NAV_TO[grp] : null       // 解析不出来就都不亮，宁可空着也别亮错
  }
  return navItems.find(i => i.names.includes(name))?.to ?? null
})
</script>

<template>
  <div class="app-shell">
    <!-- Sidebar -->
    <aside class="sidebar">
      <div class="brand">📊 GlobalData</div>
      <nav class="nav">
        <router-link
          v-for="item in navItems"
          :key="item.to"
          :to="item.to"
          class="nav-item"
          :class="{ active: item.to === activeTo }"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span class="nav-label">{{ item.label }}</span>
        </router-link>
      </nav>
    </aside>

    <!-- Main content -->
    <main class="app-main">
      <RouterView />
    </main>
  </div>
</template>

<style>
*, *::before, *::after { box-sizing: border-box; }
body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Microsoft YaHei', sans-serif;
  background: #f5f6fa;
  color: #1a1a2e;
}
a { text-decoration: none; color: inherit; }
</style>

<style scoped>
.app-shell {
  display: flex;
  min-height: 100vh;
}

/* ── Sidebar ──────────────────────────────────────────────────────────── */
.sidebar {
  width: 160px;
  flex-shrink: 0;
  background: #fff;
  border-right: 1px solid #eee;
  display: flex;
  flex-direction: column;
  position: sticky;
  top: 0;
  height: 100vh;
  overflow-y: auto;
}

.brand {
  height: 56px;
  display: flex;
  align-items: center;
  padding: 0 18px;
  font-size: 16px;
  font-weight: 700;
  color: #409eff;
  border-bottom: 1px solid #f0f0f0;
  letter-spacing: -0.3px;
  flex-shrink: 0;
}

.nav {
  padding: 12px 10px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 9px 12px;
  border-radius: 8px;
  font-size: 13.5px;
  color: #555;
  transition: background 0.15s, color 0.15s;
  cursor: pointer;
}

.nav-item:hover {
  background: #f0f6ff;
  color: #409eff;
}

.nav-item.active {
  background: #e8f3ff;
  color: #409eff;
  font-weight: 600;
}

.nav-icon {
  font-size: 15px;
  line-height: 1;
}

/* ── Main ─────────────────────────────────────────────────────────────── */
.app-main {
  flex: 1;
  min-width: 0;
  overflow-y: auto;
}
</style>
