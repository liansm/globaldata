// 私募基金相关类型
// ⚠ 口径提醒：私募净值**仅覆盖天天基金高端理财代销池**（约 0.48%），
//    不是全市场私募业绩；备案名录来自中基协公示，只有备案级字段。

export interface PrivateFundSummary {
  fundNo: string
  fundName: string
  managerName: string | null
  managerType: string | null
  workingState: string | null      // 正在运作 / 延期清算 / 提前清算 / 正常清算
  recordDate: string | null        // 备案时间
  establishDate: string | null
  mandatorName: string | null      // 托管人
  inRegistry: boolean              // 在中基协备案库
  hasNav: boolean                  // 有代销池净值
  latestNav: number | null
  latestAccNav: number | null
  latestNavDate: string | null
  latestDailyReturn: number | null
  latestFundSize: number | null    // 元（多数产品源侧不给）
}

export interface PrivateFundListResp {
  total: number
  page: number
  pageSize: number
  rows: PrivateFundSummary[]
}

export interface PrivateManagerRow {
  registerNo: string               // 登记编号
  managerName: string
  artificialPerson: string | null
  investType: string | null        // 机构类型
  registerProvince: string | null
  officeAddress: string | null
  establishDate: string | null
  registerDate: string | null
  fundCount: number | null         // 在管基金数量
  memberType: string | null
  hasCreditTips: boolean | null
}

export interface PrivateManagerListResp {
  total: number
  page: number
  pageSize: number
  rows: PrivateManagerRow[]
}

export interface PrivateFundStats {
  managers: number
  funds: number
  registry: number
  registrySource: number          // 中基协自报的备案总量（分母，会随快照波动）
  registryCoverage: number        // 备案名录覆盖率 %
  withNav: number
  navRows: number
  latestNavDate: string | null
  navCoverage: number | null       // 代销池覆盖率 %
}

export interface PrivateStateCount {
  state: string | null
  count: number
}

export interface PrivateTypeCount {
  investType: string | null
  count: number
}

export interface PrivateNavPoint {
  navDate: string
  unitNav: number | null
  accNav: number | null
  dailyReturn: number | null
  /** 净值出处：em_gaoduan=天天基金代销池（accNav=累计净值）；sppw=私募排排网（accNav=复权净值） */
  source?: 'em_gaoduan' | 'sppw' | string | null
}

export interface PrivateNavResp {
  total: number
  rows: PrivateNavPoint[]
}

export interface PrivateFundDetail extends PrivateFundSummary {
  found: boolean
  code?: string
  managerId: number | null
  isDeputeManage: boolean | null
  navPointCount: number
  navEarliest: string | null
  navLatest: string | null
  /** 净值出处：em_gaoduan=天天基金代销池；sppw=私募排排网；null=无净值 */
  navSource: string | null
  ytdReturn: number | null
  totalReturn: number | null
}
