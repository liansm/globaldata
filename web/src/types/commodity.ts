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
  // Spot snapshot fields (non-null when a usable commodity_spot row exists)
  spotChangePct: number | null   // e.g. 1.23 for +1.23%
  spotUpdatedAt: string | null   // "YYYY-MM-DD HH:MM" (Asia/Shanghai)；仅盘中快照有值
  //   'intraday' → 盘中/夜盘实时价，显示「HH:MM · 实时」
  //   'close'    → 收市静态价（日期已校正成最近交易日），显示「收盘」
  spotSnapshotKind: 'intraday' | 'close' | null
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
  snapshotKind: 'intraday' | 'close' | null
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
// 'ytd' / 'wtd' 是**自然周期**（今年来 = 1 月 1 日起、本周 = 本周一起），不是滚动 N 天；
// 数字才是滚动天数。两者不能混：本周六选「本周」应看到周一至今 5 个交易日，不是近 7 天。
export type DaysOption = 'wtd' | 'ytd' | 7 | 30 | 90 | 180 | 365 | 730 | 1825 | 3650 | 7300
