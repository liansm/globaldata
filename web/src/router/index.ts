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
      // 单币走势：日收盘折线（非 K 线），断档处显式断开
      path: '/crypto/:key',
      name: 'crypto-detail',
      component: () => import('@/pages/CryptoDetail.vue'),
    },
    {
      // 外汇：人民币汇率中间价（CFETS）+ 即期汇率（新浪），两个口径可切换
      path: '/fx',
      name: 'fx',
      component: () => import('@/pages/Fx.vue'),
    },
    {
      path: '/fx/:key',
      name: 'fx-detail',
      component: () => import('@/pages/FxDetail.vue'),
    },
    {
      // 基金信息：公募基金 / 基金公司 / 私募基金 三个 tab 合并成一页（原为三个平行导航项）。
      // 各 tab 仍可直达：/funds-info?tab=companies、?tab=private。
      path: '/funds-info',
      name: 'funds-info',
      component: () => import('@/pages/FundsInfo.vue'),
    },
    {
      // 兼容旧链接：原「公募基金」独立页 → 基金信息的公募 tab（默认 tab，不带 query）
      path: '/funds',
      redirect: { name: 'funds-info' },
    },
    {
      // 兼容旧链接：原「基金公司」独立页
      path: '/companies',
      redirect: { name: 'funds-info', query: { tab: 'companies' } },
    },
    {
      // 兼容旧链接：原「私募基金」独立页
      path: '/private-funds',
      redirect: { name: 'funds-info', query: { tab: 'private' } },
    },
    {
      path: '/fund/:code',
      name: 'fund-detail',
      component: () => import('@/pages/FundDetail.vue'),
    },
    {
      // :key 是规范化后的公司短名（如「中欧基金」），由后端 canonicalCompany 生成
      path: '/company/:key',
      name: 'company-detail',
      component: () => import('@/pages/CompanyDetail.vue'),
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
    {
      // 航运信息：运价指数（CCFI/BDI/油运）+ 油船信息（VLCC 船位）两个 tab。
      // 原先「航运运价」挂在「大宗商品」页底部、「油运信息」是独立页，现已合并到这。
      path: '/shipping',
      name: 'shipping',
      component: () => import('@/pages/Shipping.vue'),
    },
    {
      // 兼容旧链接：原「油运信息」独立页 → 航运信息的油船 tab
      path: '/oil-shipping',
      redirect: { name: 'shipping', query: { tab: 'vessels' } },
    },
    {
      // AI 硬件：存储颗粒现货（dram.com.cn，DDR5/DDR4）+ GPU 云租价（aimultiple，含单卡买断参考价）
      path: '/ai-hardware',
      name: 'ai-hardware',
      component: () => import('@/pages/AiHardware.vue'),
    },
  ],
})

export default router
