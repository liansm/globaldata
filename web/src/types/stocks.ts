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
  updatedAt: string | null   // 库内快照写入时刻，北京时间 'YYYY-MM-DD HH:mm:ss'
  quoteTime: string | null   // 源侧行情时刻（东财 f124）；停牌/退市股可能是当日 08:00 占位
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

/** 季度财报 PDF 链接（只存链接，不下载正文） */
export interface StockReport {
  code: string
  exchange: string
  reportYear: number | null      // 报告期年份，不是披露年份
  quarter: string                // Q1 | H1 | Q3 | FY
  quarterNum: number | null      // 1 | 2 | 3 | 4
  title: string | null
  pdfUrl: string
  publishDate: string | null
  source: string                 // sse | szse
}

export interface StockReportsResult {
  code: string
  rows: StockReport[]
  total: number
  page: number
  pageSize: number
}

/** 持有该股的公募基金份额（由 fund_holdings 反查，按持股数降序）*/
export interface StockFundHolding {
  fundCode: string
  fundName: string | null
  fundType: string | null
  fundCompany: string | null
  ratio: number | null          // 占净值比例 %
  shares: number | null         // 万股
  marketValue: number | null    // 万元
}

export interface HoldingReportDate {
  date: string
  count: number
}

export interface StockHoldingsResult {
  found: boolean
  code: string
  reportDate: string | null
  reportDates: HoldingReportDate[]
  securityName: string | null
  /** 同 security_code 下被名称闸门排除的其他证券 / 旧名记录（透明展示，不静默吞） */
  excluded: { name: string; count: number }[]
  /** 被「只统计季度末」规则忽略的非季末披露行数（新建 ETF 建仓 / 临时组合快照） */
  ignoredNonQuarter: number
  /** 命中时使用的「去后缀别名」（九号公司-WD → 九号公司），未使用则为 null */
  matchedAlias: string | null
  summary: {
    funds: number               // 基金份额只数（A/C 类分开计），不是产品只数
    shares: number | null       // 合计持股，万股
    marketValue: number | null  // 合计持仓市值，万元
    /** 最新总股本（股），stocks.total_shares；缺失为 null */
    totalShares: number | null
    /** 最新流通股本（股），stocks.float_shares；缺失为 null */
    floatShares: number | null
    /** 合计持股占总股本 %（已 ×100，如 6.6411 表示 6.6411%）；股本缺失时为 null */
    shareRatio: number | null
    /** 合计持股占流通股 %；缺失时为 null */
    floatShareRatio: number | null
  }
  rows: StockFundHolding[]
  total: number
  page: number
  pageSize: number
}

/** 财报三表逐季度对比：一行 = 一个报告期（三表合并）*/
export interface StockFinancialRow {
  reportDate: string | null
  reportYear: number | null
  quarterNum: number | null
  reportType: string | null
  // 年初至今累计值
  revenue: number | null
  operateIncome: number | null
  cost: number | null
  parentNetprofit: number | null
  netprofit: number | null
  netcashOperate: number | null
  // 单季（年内差分，缺上一期时为 null）
  qRevenue: number | null
  qParentNetprofit: number | null
  qNetcashOperate: number | null
  /** 单季毛利润 = 单季营业收入 − 单季营业成本 */
  qGrossProfit: number | null
  /** 单季毛利率 %（分子分母都是单季值；金融业为 null） */
  qGrossMargin: number | null
  qSaleExpense: number | null
  qManageExpense: number | null
  qResearchExpense: number | null
  /** 单季财务费用；为负表示利息收入大于支出 */
  qFinanceExpense: number | null
  /** 单季其他收益；为负表示净损失 */
  qOtherIncome: number | null
  qOperateProfit: number | null
  qIncomeTax: number | null
  /** 单季净利润，**合并口径（含少数股东损益）**，不是归母 */
  qNetprofit: number | null
  /** 单季净利率 % = 单季净利润 ÷ 单季营业总收入（分母 ≤0 或缺上一期时为 null） */
  qNetMargin: number | null
  // 资产负债表季末时点值（不差分）
  totalAssets: number | null
  totalCurrentAssets: number | null
  /** 负债合计 */
  totalLiabilities: number | null
  inventory: number | null
  /** 货币资金 */
  monetaryFunds: number | null
  /** 应收账款 */
  accountsRece: number | null
  /** 应付账款 */
  accountsPayable: number | null
  /** 短期借款；源侧不披露（无有息负债）时为 null */
  shortLoan: number | null
  /** 长期借款；源侧不披露时为 null */
  longLoan: number | null
  totalCurrentLiab: number | null
  totalEquity: number | null
  /** 归母所有者权益（不含少数股东权益） */
  totalParentEquity: number | null
  /** 期末现金及现金等价物余额（时点值） */
  endCce: number | null
  /** 员工数量：年度快照，按 reportYear 匹配；没有该年快照时为 null */
  employeeNum: number | null
  // 派生指标（金融业为 null）
  grossMargin: number | null
  netMargin: number | null
  quickRatio: number | null
  currentRatio: number | null
  debtRatio: number | null
  // 股东户数（季度内最后一个统计截止日）
  holderNum: number | null
  holderEndDate: string | null
  hasIncome: boolean
  hasBalance: boolean
  hasCashflow: boolean
  noticeDate: string | null
  updateDate: string | null
}

export interface StockFinancialsResult {
  found: boolean
  code: string
  orgType: string | null
  /** 银行/证券/保险：毛利率、速动比、流动比率口径不适用 */
  isFinancial: boolean
  latestReportDate: string | null
  /** 员工数只有当前值 + 年度快照，不伪装成逐季度数据 */
  employee: { year: number | null; empNum: number | null; collectedAt: string | null } | null
  rows: StockFinancialRow[]
  total: number
  page: number
  pageSize: number
}

// ── 逐科目三表（雪球式） ──────────────────────────────────────────────
export type StmtType = 'BALANCE' | 'INCOME' | 'CASHFLOW'
export type StmtFilter = 'all' | 'annual' | 'h1' | 'q1' | 'q3'

export interface StmtLineRow {
  key: string
  label: string
  /** section 为前端合成的分组标题行，item/sub/total 来自后端字典 */
  kind: 'item' | 'sub' | 'total' | 'section'
  /** plain = 每股收益这类，不做亿/万换算 */
  fmt: 'money' | 'plain'
  /** 与 periods 一一对应，单位元；null = 该期未披露 */
  values: (number | null)[]
}

export interface StockStatementResult {
  found: boolean
  code: string
  type: StmtType
  typeLabel: string
  filter: StmtFilter
  page: number
  pageSize: number
  totalPeriods: number
  orgType: string | null
  currency: string | null
  periods: {
    reportDate: string
    reportYear: number | null
    quarterNum: number | null
    reportType: string | null
    label: string
  }[]
  sections: { title: string | null; rows: StmtLineRow[] }[]
  unit: string
}

// ── 十大股东 / 十大流通股东（2026-11-10）────────────────────────────────
export interface StockShareholderRow {
  rank: number | null
  name: string | null
  holderCode: string | null
  /** 持股数（股） */
  holdNum: number | null
  /** 占总股本 %（两类都有） */
  totalRatio: number | null
  /** 占流通股 %（仅十大流通股东有） */
  floatRatio: number | null
  /** 增减股数（源侧是数字时） */
  holdChange: number | null
  /** 增减文字：不变 / 新进 / 加仓 … */
  changeText: string | null
  sharesType: string | null
  /** 股东类型（仅流通股东：其它 / 投资公司 / 基金 …） */
  holderType: string | null
  isOrg: number | null
  isDg: number | null
  isSjkzr: number | null
  marketCap: number | null
  totalShares: number | null
  noticeDate: string | null
  reportName: string | null
}

export interface StockShareholdersResult {
  found: boolean
  code: string
  /** 实际返回的报告期（请求的期库里没有时会回退到最新一期） */
  period: string | null
  periods: { date: string; label: string }[]
  top: StockShareholderRow[]
  free: StockShareholderRow[]
  source?: string
}
