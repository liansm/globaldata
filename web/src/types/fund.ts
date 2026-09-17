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
  /** 命中总行数（不受 limit/offset 影响），供分页表格用 */
  total: number
  /** 本次返回行数 */
  count: number
  items: FundNavPoint[]
}

/** 历史业绩：单个自然年的涨跌幅（%） */
export interface FundYearlyItem {
  year: number
  /** 年度涨跌幅（%）。按累计净值算；基数缺失时为 null */
  ret: number | null
}

export interface FundYearlyResp {
  fundCode: string
  navKind: FundNavKind
  total: number
  /** 按年份降序 */
  items: FundYearlyItem[]
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
  /** 今年来涨跌幅（%），按累计净值算；货币口径为 null */
  ytdReturn: number | null
  /** 成立以来累计收益（%），按累计净值算；货币口径为 null */
  totalReturn: number | null
  updatedAt: string
  reportDates: string[]
  holdings: FundHolding[]
}
