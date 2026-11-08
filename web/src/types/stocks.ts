// A 股股票信息 —— 与后端 /api/stocks/* 的返回结构一一对应
// numeric 列已在 API 层转成 number（后端 src/routes/stocks.ts::toNum）

export interface StockItem {
  code: string
  name: string
  exchange: string
  board: string
  region: string | null
  industry: string | null
  industryCsrc: string | null
  totalShares: number | null
  floatShares: number | null
  marketCap: number | null
  floatMarketCap: number | null
  peTtm: number | null           // 列表页财务列，详情见 StockDetailResult.fundamentals
  dividendYield: number | null   // %
  latestPrice: number | null
  changePct: number | null
  quoteDate: string | null
  latestPriceDate: string | null
  listingDate: string | null
}

export interface StockListResult {
  rows: StockItem[]
  total: number
  page: number
  pageSize: number
}

export interface StockFilters {
  exchanges: string[]
  boards: string[]
  regions: string[]
  industries: string[]
  tags: { name: string; count: number }[]
  total: number
}

export interface StockDetail extends StockItem {
  companyName: string | null
  nameEn: string | null
  formerName: string | null
  introduction: string | null
  mainBusiness: string | null
  businessScope: string | null
  registeredCapital: number | null
  foundedDate: string | null
  legalPerson: string | null
  registeredAddress: string | null
  officeAddress: string | null
  website: string | null
  email: string | null
  phone: string | null
  updatedAt: string | null
}

export interface StockPriceStats {
  rows: number
  earliest: string | null
  latest: string | null
}

/** 每股财务指标快照（口径见 StockDetail.vue 的「财务指标」卡与 DATASOURCES.md） */
export interface StockFundamentals {
  epsTtm: number | null           // 滚动 12 个月，= 最新价 / PE(TTM)
  epsReport: number | null        // 最新报告期累计、未年化
  reportDate: string | null
  peTtm: number | null
  peDynamic: number | null
  peStatic: number | null
  bvps: number | null
  pb: number | null               // 东财源侧值，未必 = 最新价 / bvps
  dividend: number | null         // 近 12 个月已实施分红合计，税前 元/股
  dividendYield: number | null    // %
  dividendPlans: number | null
  dividendLastEx: string | null
  price: number | null
  quoteDate: string | null
  source: string | null
  updatedAt: string | null
}

// ⚠ 详情接口不存在时是 200 + found:false，不是 404（axios 对 404 会抛异常）
export interface StockDetailResult {
  found: boolean
  stock: StockDetail | null
  industries: { industry: string[]; concept: string[]; region: string[] }
  priceStats: StockPriceStats | null
  fundamentals: StockFundamentals | null
  rows: unknown[]
  total: number
  page: number
  pageSize: number
}

export type PricePeriod = 'day' | 'week' | 'month' | 'year'

export interface PriceBar {
  date: string
  open: number | null
  high: number | null
  low: number | null
  close: number | null
  volume: number | null
  turnover: number | null
  amplitude: number | null
  changePct: number | null
  changeAmt: number | null
  turnoverRate: number | null
}

export interface PriceResult {
  period: PricePeriod
  code: string
  rows: PriceBar[]
  total: number
  page: number
  pageSize: number
}
