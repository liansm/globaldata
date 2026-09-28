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
    {
      path: '/companies',
      name: 'companies',
      component: () => import('@/pages/Companies.vue'),
    },
    {
      // :key 是规范化后的公司短名（如「中欧基金」），由后端 canonicalCompany 生成
      path: '/company/:key',
      name: 'company-detail',
      component: () => import('@/pages/CompanyDetail.vue'),
    },
    {
      path: '/private-funds',
      name: 'private-funds',
      component: () => import('@/pages/PrivateFunds.vue'),
    },
    {
      // :code 是备案编码（如 SB595C），与中基协 fundNo 同码
      path: '/private-fund/:code',
      name: 'private-fund-detail',
      component: () => import('@/pages/PrivateFundDetail.vue'),
    },
    {
      // 新股日历：A股 / 北交所 / 港股，日历 + 列表双视图
      path: '/ipo-calendar',
      name: 'ipo-calendar',
      component: () => import('@/pages/IpoCalendar.vue'),
    },
  ],
})

export default router
