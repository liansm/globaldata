<script setup lang="ts">
import { RouterView, useRoute } from 'vue-router'
import { computed } from 'vue'

const route = useRoute()

const navItems = [
  { label: '大宗商品', icon: '📦', to: '/',        names: ['home', 'detail'] },
  { label: '全球股市', icon: '📈', to: '/markets',  names: ['markets', 'market-detail'] },
  { label: '加密货币', icon: '₿',  to: '/crypto',   names: ['crypto'] },
]

function isActive(item: typeof navItems[0]) {
  return item.names.includes(route.name as string)
}
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
          :class="{ active: isActive(item) }"
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
