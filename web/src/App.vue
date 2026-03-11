<script setup lang="ts">
import { RouterView } from 'vue-router'
</script>

<template>
  <div class="app-shell">
    <header class="app-nav">
      <router-link to="/" class="brand">📊 GlobalData</router-link>
    </header>
    <main class="app-main">
      <RouterView />
    </main>
  </div>
</template>

<style>
/* 全局重置 */
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
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

.app-nav {
  background: #fff;
  border-bottom: 1px solid #eee;
  padding: 0 24px;
  height: 52px;
  display: flex;
  align-items: center;
  position: sticky;
  top: 0;
  z-index: 100;
}

.brand {
  font-size: 18px;
  font-weight: 700;
  color: #409eff;
  letter-spacing: -0.5px;
}

.app-main {
  flex: 1;
  padding: 0;
}
</style>
