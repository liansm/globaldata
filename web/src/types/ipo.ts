/** 新股日历条目。一只新股 = 一行，多个关键日期分散在不同列上 */
export interface IpoItem {
  market:         string          // 'A股' | '北交所' | '港股'
  code:           string
  name:           string
  exchange:       string | null
  board:          string | null   // A股板块
  industry:       string | null   // 港股行业
  issuePrice:     number | null   // 发行价 / 招股价下限
  issuePriceHigh: number | null   // 招股价区间上限（港股区间报价）
  currency:       string | null   // CNY | HKD
  issueShares:    number | null   // 发行总数（股）
  raiseAmount:    number | null   // 募集资金（亿）
  lotSize:        number | null   // 每手股数（港股）
  entryFee:       number | null   // 入场费（港元）
  applyDate:      string | null   // A股申购日 / 港股招股起始日
  applyEndDate:   string | null   // 港股招股截止日
  pricingDate:    string | null
  allotmentDate:  string | null   // 中签号公布日 / 公布售股结果日
  payDate:        string | null   // 中签缴款日
  refundDate:     string | null
  greyDate:       string | null   // 暗盘日（港股独有）
  listingDate:    string | null
  peIssue:        number | null
  peIndustry:     number | null
  winRate:        number | null   // 中签率 %
  source:         string
  updatedAt:      string | null
}

export interface IpoStats {
  market:        string
  upcomingApply: number
  upcomingList:  number
  total:         number
}

/**
 * 日历 / 列表里画的一个事件。
 *
 * ⚠ kind 只含日历会绘制的三类：**中签公布、中签缴款不画**（只放进详情弹窗，
 *   否则一天三四个标记会把格子挤爆）。
 *   不要在类型里预留 allotment —— 组件的颜色表 / 排序表都是按三类定义的，
 *   多一个 key 会让 KIND_ORDER 取到 undefined，排序变成 NaN，静默错乱且很难查。
 */
export interface IpoEvent {
  date:  string
  kind:  'apply' | 'grey' | 'listing'
  item:  IpoItem
}
