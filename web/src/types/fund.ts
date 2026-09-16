/** 净值口径：'money' = 货币基金，latestNav 是万份收益(元)、latestAccNav 是七日年化(%) */
export type FundNavKind = 'unit' | 'money' | null

export interface FundSummary {
  fundCode: string
  fundName: string
  fundType: string | null
  fundCompany: string | null
  /** 规范化公司短名，用于跳转 /company/:key */
  companyKey: string | null
  fundManager: string | null
  scale: number | null
  scaleRaw: string | null
  inceptionDate: string | null
  latestNav: number | null
  latestAccNav: number | null
  latestNavDate: string | null
  latestDailyReturn: number | null
  navKind: FundNavKind
}

export interface FundListResp {
  total: number
  page: number
  pageSize: number
  items: FundSummary[]
}

export interface FundNavPoint {
  date: string
  nav: number | null
  accNav: number | null
  dailyReturn: number | null
}

export interface FundNavResp {
  fundCode: string
  navKind: FundNavKind
  latestNavDate: string | null
  count: number
  items: FundNavPoint[]
}

export interface FundTypeCount {
  fundType: string | null
  count: number
}

export interface FundHolding {
  reportDate: string
  holdingType: string
  securityCode: string
  securityName: string | null
  ratio: number | null
  shares: number | null
  marketValue: number | null
}

export interface FundDetailResp {
  fundCode: string
  fundName: string
  fundType: string | null
  fundCompany: string | null
  /** 规范化公司短名，用于跳转 /company/:key */
  companyKey: string | null
  fundManager: string | null
  scale: number | null
  scaleRaw: string | null
  inceptionDate: string | null
  latestNav: number | null
  latestAccNav: number | null
  latestNavDate: string | null
  latestDailyReturn: number | null
  navKind: FundNavKind
  updatedAt: string
  reportDates: string[]
  holdings: FundHolding[]
}
