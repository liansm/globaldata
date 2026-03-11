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
}

export interface PricePoint {
  date: string
  price: number | null
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
  history: PricePoint[]
}

export interface LatestPrice {
  key: string
  commodity: string
  unit: string | null
  date: string
  price: number | null
}

// 时间范围选项
export type DaysOption = 7 | 30 | 90 | 180 | 365
