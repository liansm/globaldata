// 与后端 API 响应保持一致的类型定义

export interface Commodity {
  key: string
  symbol: string | null
  commodity: string
  unit: string | null
  priceType: string | null
  kcal: number | null
  gradeType: string | null
  updatedAt: string | null
  latestDate: string | null
  latestPrice: number | null
  // Real-time spot fields (non-null when commodity_spot has a record)
  spotChangePct: number | null   // e.g. 1.23 for +1.23%
  spotUpdatedAt: string | null   // "YYYY-MM-DD HH:MM" (Asia/Shanghai)
  hasMinutes:    boolean         // true when intraday minute bars exist for today
}

export interface SpotSnapshot {
  price:     number | null
  changePct: number | null
  changeAmt: number | null
  prevClose: number | null
  volume:    number | null
  turnover:  number | null
  spotDate:  string | null
  updatedAt: string | null
}

export interface PricePoint {
  date: string
  price: number | null
}

export interface MinuteBar {
  time:     string        // "YYYY-MM-DD HH:MM"
  open:     number | null
  high:     number | null
  low:      number | null
  close:    number | null
  volume:   number | null
  turnover: number | null
}

export interface CommodityDetail {
  key: string
  symbol: string | null
  commodity: string
  unit: string | null
  priceType: string | null
  kcal: number | null
  gradeType: string | null
  updatedAt: string | null
  spot: SpotSnapshot | null
  history: PricePoint[]
}

export interface CommodityMinutes {
  key:       string
  commodity: string
  unit:      string | null
  date:      string | null
  minutes:   MinuteBar[]
}

export interface LatestPrice {
  key: string
  commodity: string
  unit: string | null
  date: string
  price: number | null
}

// 时间范围选项
export type DaysOption = 'ytd' | 7 | 30 | 90 | 180 | 365 | 730 | 1825 | 3650 | 7300
