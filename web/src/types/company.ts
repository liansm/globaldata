export interface CompanyTypeBreakdown {
  fundType: string
  count: number
  scale: number
}

export interface CompanyTopFund {
  fundCode: string
  fundName: string
  scale: number | null
}

/** 公司列表项（来自 /api/fund-companies） */
export interface CompanySummary {
  /** 规范化短名，同时是 URL key，如「中欧基金」 */
  key: string
  name: string
  /** 库里最完整的法定名 */
  fullName: string
  /** 归并到该公司的全部原始写法 */
  aliases: string[]
  fundCount: number
  scaleCovered: number
  totalScale: number
  holdingsCovered: number
  managerCount: number
  earliestInception: string | null
  topFunds: CompanyTopFund[]
  /** 类型数量（列表接口不下发 types 明细） */
  typeCount: number
}

export interface CompanyListResp {
  total: number
  page: number
  pageSize: number
  /** 未采集到基金公司的产品统计 */
  unassigned: { fundCount: number; totalScale: number }
  items: CompanySummary[]
}

export interface CompanyManager {
  name: string
  count: number
  scale: number
}

/** 公司旗下产品（字段与基金列表页一致，额外带最新持仓报告期） */
export interface CompanyFund {
  fundCode: string
  fundName: string
  fundType: string | null
  fundManager: string | null
  scale: number | null
  scaleRaw: string | null
  inceptionDate: string | null
  latestReportDate: string | null
  companyKey: string
}

export interface CompanyDetailResp {
  key: string
  name: string
  fullName: string
  aliases: string[]
  fundCount: number
  scaleCovered: number
  totalScale: number
  holdingsCovered: number
  managerCount: number
  earliestInception: string | null
  types: CompanyTypeBreakdown[]
  topFunds: CompanyTopFund[]
  managers: CompanyManager[]
  total: number
  page: number
  pageSize: number
  items: CompanyFund[]
}
