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

/**
 * 走势图的区间摘要。
 * 复权因子由后端在**全序列**上估计份额折算系数后按区间连乘 —— 前端不要自己用
 * dailyReturn 复刻，源侧日增长率有错值（010500 有 3 天与净值对不上）。
 */
export interface FundNavSummaryResp {
  fundCode: string
  navKind: FundNavKind
  /** 区间实际首日（无数据时为 null） */
  from: string | null
  /** 区间实际末日 */
  to: string | null
  /** 涨跌幅的基数日 = 区间首日的**前一个**披露日（区间首日无前一日时等于 from） */
  baseDate: string | null
  /** 区间内的披露日数 */
  days: number
  /** 区间涨跌幅（%），**复权**（分红再投）；货币口径为 null */
  adjReturn: number | null
  /** 区间涨跌幅（%），**分红不复投**（累计净值口径）；货币口径为 null */
  accReturn: number | null
  /** 区间内单位净值的最高点与日期 */
  navMax: { v: number; d: string } | null
  /** 区间内单位净值的最低点与日期 */
  navMin: { v: number; d: string } | null
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
  /** 今年来涨跌幅（%），**复权**口径（分红再投），与雪球/天天基金一致；货币口径为 null */
  ytdReturn: number | null
  /** 成立以来累计收益（%），**复权**口径（分红再投）；货币口径为 null */
  totalReturn: number | null
  /** 今年来涨跌幅（%）的副口径：累计净值（现金分红不再投），分红多的基金会明显偏低 */
  ytdReturnAcc: number | null
  /** 成立以来累计收益（%）的副口径：累计净值（现金分红不再投） */
  totalReturnAcc: number | null
  updatedAt: string
  reportDates: string[]
  holdings: FundHolding[]
}
