export interface FundSummary {
  fundCode: string
  fundName: string
  fundType: string | null
  fundCompany: string | null
  fundManager: string | null
  scale: number | null
  scaleRaw: string | null
  inceptionDate: string | null
}

export interface FundListResp {
  total: number
  page: number
  pageSize: number
  items: FundSummary[]
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
  fundManager: string | null
  scale: number | null
  scaleRaw: string | null
  inceptionDate: string | null
  updatedAt: string
  reportDates: string[]
  holdings: FundHolding[]
}
