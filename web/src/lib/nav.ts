import { ref } from 'vue'

/**
 * 侧边栏高亮的「归属」计算 —— 专治 `/market/:key` 一类「一条路由装多种东西」的页面。
 *
 * 背景：`/market/:key` 同时承载股市指数（上证指数…）、航运运价（ccfi_total / bdi…）
 * 和建材 CEMPI（cement_cempi），**route.name 一律是 `market-detail`**，路由层根本
 * 分不出它属于哪个导航项。之前把 `market-detail` 直接写进「全球股市」的 names 里，
 * 结果点 CCFI 综合指数时左侧也亮「全球股市」——就是那个 bug。
 *
 * 现在改成：详情接口回来后再按 market 归属（`detailNav` 回填），
 * 接口回来之前用「上一页路径」猜一个占位（`navForPath`），避免高亮空一帧。
 */
export const STOCK_MARKETS = new Set(['A股', '港股', '美股', '欧洲', '亚太', '资金流向'])

export type NavGroup = 'home' | 'markets' | 'shipping'

/** navGroup → 导航项的路由地址（App.vue 用它比对 `item.to`） */
export const NAV_TO: Record<NavGroup, string> = {
  home: '/',
  markets: '/markets',
  shipping: '/shipping',
}

/** market 字段 → 所属导航项；未知 market 归「大宗商品」（水泥 CEMPI 走的就是这条） */
export function navForMarket(market?: string | null): NavGroup | null {
  if (!market) return null
  if (STOCK_MARKETS.has(market)) return 'markets'
  if (market === '航运') return 'shipping'
  return 'home'
}

/** 详情页数据到达后回填；离开详情页时置空（由 MarketDetail 负责），避免串到下一个页面 */
export const detailNav = ref<NavGroup | null>(null)

/**
 * 从「上一页路径」做即时猜测 —— 仅用于接口返回前的占位，不是最终答案。
 * 例：从 /shipping 点进 CCFI → 立刻先亮「航运信息」，数据回来后由 detailNav 校正。
 */
export function navForPath(path?: string | null): NavGroup | null {
  if (!path) return null
  if (path.startsWith('/shipping')) return 'shipping'
  if (path.startsWith('/markets')) return 'markets'
  if (path === '/' || path.startsWith('/commodities')) return 'home'
  return null
}
