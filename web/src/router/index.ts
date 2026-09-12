import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      redirect: '/commodities',
    },
    {
      path: '/commodities',
      name: 'home',
      component: () => import('@/pages/Home.vue'),
    },
    {
      path: '/commodity/:key',
      name: 'detail',
      component: () => import('@/pages/Detail.vue'),
    },
    {
      path: '/markets',
      name: 'markets',
      component: () => import('@/pages/Markets.vue'),
    },
    {
      path: '/market/:key',
      name: 'market-detail',
      component: () => import('@/pages/MarketDetail.vue'),
    },
    {
      path: '/crypto',
      name: 'crypto',
      component: () => import('@/pages/Crypto.vue'),
    },
    {
      path: '/funds',
      name: 'funds',
      component: () => import('@/pages/Funds.vue'),
    },
    {
      path: '/fund/:code',
      name: 'fund-detail',
      component: () => import('@/pages/FundDetail.vue'),
    },
  ],
})

export default router
