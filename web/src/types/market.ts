export interface MarketIndex {
  key:            string
  symbol:         string | null
  name:           string
  market:         string          // 'A股' | '港股' | '资金流向'
  unit:           string | null
  updatedAt:      string | null
  latestDate:     string | null
  latestClose:    number | null   // 指数点位 or 净流入亿元
  latestTurnover: number | null   // 成交额（仅指数）
  changePct:      number | null   // 涨跌幅 %（与前一交易日比较）
  // Non-null when displayed price comes from real-time spot data (index_spot)
  // Format: "YYYY-MM-DD HH:MM" (China local time)
  latestSpotUpdatedAt: string | null
}

export interface IndexPoint {
  date:     string
  close:    number | null
  volume:   number | null
  turnover: number | null
}

export interface MarketDetail {
  key:       string
  symbol:    string | null
  name:      string
  market:    string
  unit:      string | null
  updatedAt: string | null
  history:   IndexPoint[]
}

export interface MinutePoint {
  time:     string        // "YYYY-MM-DD HH:MM"
  open:     number | null
  high:     number | null
  low:      number | null
  close:    number | null
  volume:   number | null
  turnover: number | null
}

export interface MarketMinutes {
  key:     string
  name:    string
  market:  string
  date:    string | null
  minutes: MinutePoint[]
}
