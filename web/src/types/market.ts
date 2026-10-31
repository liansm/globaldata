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
  latestVolume:   number | null   // 成交量（股）；成交额缺失时卡片回退显示此项
  changePct:      number | null   // 涨跌幅 %（与前一交易日比较）
  // Non-null when displayed price comes from a spot snapshot (index_spot)
  //   'intraday' → 盘中的实时价，配合 latestSpotUpdatedAt 显示「HH:MM · 实时」
  //   'close'    → 收盘快照（收盘后写入、带成交额），显示「收盘」
  //   null       → 当前展示的是日线，不该挂任何快照标记
  spotSnapshotKind: 'intraday' | 'close' | null
  // Non-null only for intraday snapshots — 收盘快照的 updated_at 是抓取时刻，
  // 拿它当"收盘时间"会误导（周末 11:34 抓的也是周五收盘）。
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
  // 区间涨跌幅的基期：区间起点**前**最后一个交易日收盘（后端按本次请求的 from 算）；
  // 序列在区间内才有数据（无更早历史）时为 null。
  baseline:  { date: string; close: number | null } | null
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
  key:       string
  name:      string
  market:    string
  date:      string | null
  prevClose: number | null
  minutes:   MinutePoint[]
}
