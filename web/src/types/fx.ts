// 人民币汇率（外汇）
//
// ⚠ 数值口径：所有 `close` / `price` 都是「1 单位外币 兑 人民币」——
//   日元已按 quoteUnit=100 换算，即 `100 JPY = 4.2727 CNY`。
//   换算在**后端**完成（src/routes/fx.ts::toDisplay），前端不要再乘一遍。
//   ⇒ 全站唯一语义：**数值上行 = 人民币贬值（红）**，数值下行 = 人民币升值（绿）。

export type FxRateType = 'mid' | 'spot'

export interface FxQuote {
  date:      string | null
  close:     number | null
  prevClose: number | null
  change:    number | null
  changePct: number | null
  points:    number
}

export interface FxSpotQuote extends FxQuote {
  /** 即期序列最早日期（中间价是 2006 起，即期各对不一，前端要用它做「全部」区间起点） */
  earliest: string | null
}

export interface FxLiveQuote {
  price:     number | null
  prevClose: number | null
  change:    number | null
  changePct: number | null
  quoteTime: string | null      // HH:MM:SS，源侧报价时间
  date:      string | null
}

export interface FxPair {
  key:          string
  baseCode:     string          // 'USD'
  baseName:     string          // '美元'
  quoteUnit:    number          // 1 | 100
  category:     string          // '主要货币' | '其他货币'
  officialCode: string | null   // CFETS 原始代码 '100JPY/CNY'
  spotCode:     string | null   // 新浪即期代码 'fx_sjpycny'
  updatedAt:    string | null
  mid:          FxQuote
  spot:         FxSpotQuote
  live:         FxLiveQuote | null
}

export interface FxPoint {
  date:  string
  open:  number | null    // mid 口径源侧只有中间价一个值 → open/high/low 恒为 null
  high:  number | null
  low:   number | null
  close: number | null
}

export interface FxTypeInfo {
  points:   number
  earliest: string | null
  latest:   string | null
}

export interface FxDetail {
  key:            string
  baseCode:       string
  baseName:       string
  quoteUnit:      number
  category:       string
  officialCode:   string | null
  spotCode:       string | null
  rateType:       FxRateType
  source:         string
  updatedAt:      string | null
  availableTypes: Partial<Record<FxRateType, FxTypeInfo>>
  range:          { from: string; to: string }
  stats: {
    points:         number
    latest:         FxPoint | null
    earliest:       FxPoint | null
    rangeChangePct: number | null
  }
  history: FxPoint[]
}
