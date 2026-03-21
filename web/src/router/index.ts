import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
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
  ],
})

export default router
