import {
  pgTable,
  varchar,
  integer,
  numeric,
  date,
  timestamp,
  serial,
  bigserial,
  unique,
  index,
} from 'drizzle-orm/pg-core'
import { sql } from 'drizzle-orm'

// --------------------------------------------------------------------------
// commodities — one row per commodity, updated on every fetch run
// --------------------------------------------------------------------------
export const commodities = pgTable('commodities', {
  key:       varchar('key',        { length: 60  }).primaryKey(),
  symbol:    varchar('symbol',     { length: 60  }).notNull(),
  commodity: varchar('commodity',  { length: 200 }).notNull(),
  unit:      varchar('unit',       { length: 50  }).notNull(),
  sourceApi: varchar('source_api', { length: 200 }),
  priceType: varchar('price_type', { length: 100 }),  // e.g. "环渤海现货" for CCTD
  kcal:      integer('kcal'),                          // coal calorific value
  gradeType: varchar('grade_type', { length: 50  }),   // "港口" / "进口" / "产地"
  updatedAt: timestamp('updated_at', { withTimezone: true })
               .default(sql`NOW()`).notNull(),
})

// --------------------------------------------------------------------------
// prices — daily price history, unique per (commodity_key, price_date)
// --------------------------------------------------------------------------
export const prices = pgTable('prices', {
  id:           bigserial('id', { mode: 'number' }).primaryKey(),
  commodityKey: varchar('commodity_key', { length: 60 }).notNull(),
  priceDate:    date('price_date').notNull(),
  price:        numeric('price', { precision: 14, scale: 4 }).notNull(),
})

// --------------------------------------------------------------------------
// market_indices — one row per index / capital-flow series
// --------------------------------------------------------------------------
export const marketIndices = pgTable('market_indices', {
  key:       varchar('key',    { length: 60  }).primaryKey(),
  symbol:    varchar('symbol', { length: 60  }).notNull(),
  name:      varchar('name',   { length: 200 }).notNull(),
  market:    varchar('market', { length: 50  }).notNull(),   // 'A股' | '港股' | '资金流向'
  unit:      varchar('unit',   { length: 50  }),
  updatedAt: timestamp('updated_at', { withTimezone: true })
               .default(sql`NOW()`).notNull(),
})

// --------------------------------------------------------------------------
// index_prices — daily close / volume per index, unique per (index_key, date)
// --------------------------------------------------------------------------
export const indexPrices = pgTable('index_prices', {
  id:        bigserial('id', { mode: 'number' }).primaryKey(),
  indexKey:  varchar('index_key',  { length: 60 }).notNull(),
  priceDate: date('price_date').notNull(),
  open:      numeric('open',     { precision: 16, scale: 4 }),
  high:      numeric('high',     { precision: 16, scale: 4 }),
  low:       numeric('low',      { precision: 16, scale: 4 }),
  close:     numeric('close',    { precision: 16, scale: 4 }),
  volume:    numeric('volume',   { precision: 24, scale: 4 }),
  turnover:  numeric('turnover', { precision: 24, scale: 4 }),
})

// --------------------------------------------------------------------------
// crypto_coins — one row per cryptocurrency
// --------------------------------------------------------------------------
export const cryptoCoins = pgTable('crypto_coins', {
  key:       varchar('key',    { length: 60  }).primaryKey(),
  symbol:    varchar('symbol', { length: 30  }),
  name:      varchar('name',   { length: 100 }),
  unit:      varchar('unit',   { length: 20  }).default('USD'),
  updatedAt: timestamp('updated_at', { withTimezone: true })
               .default(sql`NOW()`).notNull(),
})

// --------------------------------------------------------------------------
// crypto_prices — daily snapshot per coin (upserted on each run)
// --------------------------------------------------------------------------
export const cryptoPrices = pgTable('crypto_prices', {
  id:        bigserial('id', { mode: 'number' }).primaryKey(),
  coinKey:   varchar('coin_key',   { length: 60 }).notNull(),
  priceDate: date('price_date').notNull(),
  close:     numeric('close',      { precision: 20, scale: 6 }),
  changePct: numeric('change_pct', { precision: 8,  scale: 4 }),
  volume24h: numeric('volume_24h', { precision: 24, scale: 2 }),
  high24h:   numeric('high_24h',   { precision: 20, scale: 6 }),
  low24h:    numeric('low_24h',    { precision: 20, scale: 6 }),
}, (t) => [
  unique('crypto_prices_uniq').on(t.coinKey, t.priceDate),
  index('crypto_prices_key_date').on(t.coinKey, t.priceDate),
])

// --------------------------------------------------------------------------
// index_minutes — 1-minute intraday bars for A-share indices
// --------------------------------------------------------------------------
export const indexMinutes = pgTable('index_minutes', {
  id:       bigserial('id', { mode: 'number' }).primaryKey(),
  indexKey: varchar('index_key', { length: 60 }).notNull(),
  dt:       timestamp('dt').notNull(),            // China local time (no TZ)
  open:     numeric('open',     { precision: 16, scale: 4 }),
  high:     numeric('high',     { precision: 16, scale: 4 }),
  low:      numeric('low',      { precision: 16, scale: 4 }),
  close:    numeric('close',    { precision: 16, scale: 4 }),
  volume:   numeric('volume',   { precision: 24, scale: 4 }),
  turnover: numeric('turnover', { precision: 24, scale: 4 }),
}, (t) => [
  unique('index_minutes_uniq').on(t.indexKey, t.dt),
  index('idx_index_minutes_key_dt').on(t.indexKey, t.dt),
])

// --------------------------------------------------------------------------
// index_spot — latest real-time snapshot for A-share indices (one row per index)
// Populated by fetch_index_spot.py via stock_zh_index_spot_sina
// --------------------------------------------------------------------------
export const indexSpot = pgTable('index_spot', {
  indexKey:  varchar('index_key',  { length: 60 }).primaryKey(),
  price:     numeric('price',      { precision: 16, scale: 4 }),
  changePct: numeric('change_pct', { precision: 8,  scale: 4 }),  // e.g. 1.23 for +1.23%
  turnover:  numeric('turnover',   { precision: 24, scale: 4 }),  // 元
  prevClose: numeric('prev_close', { precision: 16, scale: 4 }),
  spotDate:  date('spot_date'),
  updatedAt: timestamp('updated_at', { withTimezone: true })
               .default(sql`NOW()`).notNull(),
})

// --------------------------------------------------------------------------
// commodity_spot — real-time snapshot per commodity (one row, upserted each run)
// Populated by fetch_commodity_spot.py via futures_zh_spot / futures_foreign_commodity_realtime
// --------------------------------------------------------------------------
export const commoditySpot = pgTable('commodity_spot', {
  commodityKey: varchar('commodity_key', { length: 60 }).primaryKey(),
  price:        numeric('price',      { precision: 20, scale: 6 }),
  changePct:    numeric('change_pct', { precision: 8,  scale: 4 }),  // e.g. 1.23 for +1.23%
  changeAmt:    numeric('change_amt', { precision: 20, scale: 6 }),
  prevClose:    numeric('prev_close', { precision: 20, scale: 6 }),
  volume:       numeric('volume',     { precision: 24, scale: 4 }),
  turnover:     numeric('turnover',   { precision: 24, scale: 4 }),
  spotDate:     date('spot_date'),
  updatedAt:    timestamp('updated_at', { withTimezone: true })
                  .default(sql`NOW()`).notNull(),
})

// --------------------------------------------------------------------------
// commodity_minutes — 1-minute intraday OHLCV bars for domestic futures
// Populated by fetch_commodity_minutes.py via futures_zh_minute_sina
// --------------------------------------------------------------------------
export const commodityMinutes = pgTable('commodity_minutes', {
  id:           bigserial('id', { mode: 'number' }).primaryKey(),
  commodityKey: varchar('commodity_key', { length: 60 }).notNull(),
  dt:           timestamp('dt').notNull(),   // China local time (no TZ)
  open:         numeric('open',     { precision: 20, scale: 6 }),
  high:         numeric('high',     { precision: 20, scale: 6 }),
  low:          numeric('low',      { precision: 20, scale: 6 }),
  close:        numeric('close',    { precision: 20, scale: 6 }),
  volume:       numeric('volume',   { precision: 24, scale: 4 }),
  turnover:     numeric('turnover', { precision: 24, scale: 4 }),
}, (t) => [
  unique('commodity_minutes_uniq').on(t.commodityKey, t.dt),
  index('idx_commodity_minutes_key_dt').on(t.commodityKey, t.dt),
])

// --------------------------------------------------------------------------
// funds — one row per public fund (份额口径，A/C 类分开)
// Populated by fetch_funds.py: 名录来自天天基金 fund_name_em，
// 规模/公司/经理来自雪球 fund_individual_basic_info_xq
// --------------------------------------------------------------------------
export const funds = pgTable('funds', {
  fundCode:        varchar('fund_code',        { length: 12  }).primaryKey(),
  fundName:        varchar('fund_name',        { length: 200 }).notNull(),
  fundType:        varchar('fund_type',        { length: 60  }),   // '混合型-偏股' | '股票型' | ...
  fundCompany:     varchar('fund_company',     { length: 200 }),
  fundManager:     varchar('fund_manager',     { length: 200 }),
  scale:           numeric('scale',            { precision: 16, scale: 4 }),  // 最新规模（亿元，含"万"级小基金）
  scaleRaw:        varchar('scale_raw',        { length: 50  }),   // 原文，如 '39.38亿'
  inceptionDate:   date('inception_date'),
  scaleUpdatedAt:  timestamp('scale_updated_at', { withTimezone: true }),
  // 最新净值快照（由 fetch_fund_nav.py 维护）
  latestNav:        numeric('latest_nav',         { precision: 14, scale: 4 }),
  latestAccNav:     numeric('latest_acc_nav',     { precision: 14, scale: 4 }),
  latestNavDate:    date('latest_nav_date'),
  latestDailyReturn:numeric('latest_daily_return',{ precision: 10, scale: 4 }),
  // 净值口径：'unit' = 单位净值/累计净值；'money' = 货币基金（万份收益 元 / 七日年化 %）
  navKind:          varchar('nav_kind',           { length: 10  }),
  navUpdatedAt:     timestamp('nav_updated_at',   { withTimezone: true }),
  updatedAt:       timestamp('updated_at',       { withTimezone: true })
                     .default(sql`NOW()`).notNull(),
})

// --------------------------------------------------------------------------
// fund_nav — 基金净值日线序列（全历史，份额口径）
// Populated by fetch_fund_nav.py:
//   回填 = pingzhongdata/<code>.js（1 请求拿全历史）
//   增量 = 天天基金排行榜批量接口（4 请求拿全市场当日净值）
// ⚠ nav_kind='money' 时列含义不同：unit_nav = 万份收益(元)、acc_nav = 七日年化(%)
//   —— 货币基金没有「单位净值」，别和普通基金混算
// --------------------------------------------------------------------------
export const fundNav = pgTable('fund_nav', {
  id:          bigserial('id', { mode: 'number' }).primaryKey(),
  fundCode:    varchar('fund_code',   { length: 12 }).notNull(),
  navDate:     date('nav_date').notNull(),
  unitNav:     numeric('unit_nav',     { precision: 14, scale: 4 }),  // 单位净值 / 万份收益
  accNav:      numeric('acc_nav',      { precision: 14, scale: 4 }),  // 累计净值 / 七日年化
  dailyReturn: numeric('daily_return', { precision: 10, scale: 4 }),  // 日增长率 %
  navKind:     varchar('nav_kind',     { length: 10 }).notNull().default('unit'),
}, (t) => [
  unique('fund_nav_uniq').on(t.fundCode, t.navDate),
  index('idx_fund_nav_fund_date').on(t.fundCode, t.navDate),
  index('idx_fund_nav_date').on(t.navDate),
])

// --------------------------------------------------------------------------
// fund_holdings — quarterly portfolio holdings (季报前十大重仓)
// 股票来自 FundArchivesDatas.aspx?type=jjcc，债券 type=zqcc（--bonds 开启）
// 注意：完整持仓仅半年报/年报披露，季报只有前十大
// --------------------------------------------------------------------------
export const fundHoldings = pgTable('fund_holdings', {
  id:           bigserial('id', { mode: 'number' }).primaryKey(),
  fundCode:     varchar('fund_code',     { length: 12  }).notNull(),
  reportDate:   date('report_date').notNull(),            // 报告期截止日，如 2026-06-30
  holdingType:  varchar('holding_type', { length: 10  }).notNull(),  // 'stock' | 'bond'
  securityCode: varchar('security_code', { length: 12  }).notNull(),
  securityName: varchar('security_name', { length: 100 }),
  ratio:        numeric('ratio',        { precision: 10, scale: 4 }),  // 占净值比例 %
  shares:       numeric('shares',       { precision: 20, scale: 4 }),  // 持股数（万股，股票）
  marketValue:  numeric('market_value', { precision: 20, scale: 4 }),  // 持仓市值（万元）
}, (t) => [
  unique('fund_holdings_uniq').on(t.fundCode, t.reportDate, t.holdingType, t.securityCode),
  index('idx_fund_holdings_fund_date').on(t.fundCode, t.reportDate),
])

// --------------------------------------------------------------------------
// fetch_log — one row per commodity per run (audit trail)
// --------------------------------------------------------------------------
export const fetchLog = pgTable('fetch_log', {
  id:           serial('id').primaryKey(),
  fetchedAt:    timestamp('fetched_at', { withTimezone: true })
                  .default(sql`NOW()`).notNull(),
  commodityKey: varchar('commodity_key', { length: 60  }).notNull(),
  latestDate:   date('latest_date'),
  latestPrice:  numeric('latest_price', { precision: 14, scale: 4 }),
  changeDay:    numeric('change_day',   { precision: 10, scale: 4 }),
})

// --------------------------------------------------------------------------
// ipo_calendar — 新股日历（A股 / 北交所 / 港股，2026-09-28 落地）
// Populated by fetch_ipo_calendar.py
//
// ⚠ 三个市场的字段重合度很低，**一张表统管、市场特有列一律可空**
//   （不按市场分表，与本项目「绝对价格 / 指数」的分层习惯一致）：
//     A股独有 → allotment_date(中签号公布) / pay_date(中签缴款) / pe_industry / win_rate
//     港股独有 → apply_end_date(招股截止) / pricing_date(定价) / refund_date(退票)
//                / grey_date(暗盘) / lot_size(每手) / entry_fee(入场费)
//
// ⚠ raise_amount 单位统一「亿」，币种看 currency（CNY / HKD）。
//   A股募资额 = 发行总数(万股) × 发行价 / 1e4，由脚本自算；
//   港股募资额只有**已上市**标的能在东财拿到，未上市新股源侧不披露
//   → 留空，前端显示「—」。**不要用 0 充数。**
//
// ⚠ 招股节点两套叫法：A股是「申购日 → 中签号公布 → 中签缴款 → 上市」，
//   港股是「招股起止 → 定价 → 公布售股结果 → 退票 → 暗盘 → 上市」。
//   前端的「申购/招股」统一映射到 apply_date。
// --------------------------------------------------------------------------
export const ipoCalendar = pgTable('ipo_calendar', {
  id:             bigserial('id', { mode: 'number' }).primaryKey(),
  market:         varchar('market',   { length: 10  }).notNull(),   // 'A股' | '北交所' | '港股'
  code:           varchar('code',     { length: 16  }).notNull(),
  name:           varchar('name',     { length: 120 }).notNull(),
  exchange:       varchar('exchange', { length: 30  }),
  board:          varchar('board',    { length: 20  }),   // A股板块：非科创板 / 科创板 / 北交所
  industry:       varchar('industry', { length: 60  }),   // 港股行业分类
  issuePrice:     numeric('issue_price',      { precision: 14, scale: 4 }),  // 发行价 / 招股价下限
  issuePriceHigh: numeric('issue_price_high', { precision: 14, scale: 4 }),  // 招股价区间上限（港股）
  currency:       varchar('currency', { length: 6  }),    // CNY | HKD
  issueShares:    numeric('issue_shares', { precision: 24, scale: 4 }),      // 发行总数（股）
  raiseAmount:    numeric('raise_amount', { precision: 20, scale: 4 }),      // 募集资金（亿）
  lotSize:        numeric('lot_size',  { precision: 14, scale: 2 }),         // 每手股数（港股）
  entryFee:       numeric('entry_fee', { precision: 14, scale: 2 }),         // 入场费（港元，港股）
  applyDate:      date('apply_date'),        // A股申购日 / 港股招股起始日
  applyEndDate:   date('apply_end_date'),    // 港股招股截止日
  pricingDate:    date('pricing_date'),      // 定价日（港股）
  allotmentDate:  date('allotment_date'),    // 中签号公布日 / 公布售股结果日
  payDate:        date('pay_date'),          // 中签缴款日（A股）
  refundDate:     date('refund_date'),       // 退票寄发日（港股）
  greyDate:       date('grey_date'),         // 暗盘日（港股独有节点）
  listingDate:    date('listing_date'),
  peIssue:        numeric('pe_issue',    { precision: 14, scale: 4 }),       // 发行市盈率
  peIndustry:     numeric('pe_industry', { precision: 14, scale: 4 }),       // 行业市盈率
  winRate:        numeric('win_rate',    { precision: 14, scale: 6 }),       // 中签率 %
  source:         varchar('source', { length: 120 }).notNull(),  // 多源时用 + 连接，便于追溯
  updatedAt:      timestamp('updated_at', { withTimezone: true })
                    .default(sql`NOW()`).notNull(),
}, (t) => [
  unique('ipo_calendar_market_code_uniq').on(t.market, t.code),
  index('idx_ipo_calendar_listing').on(t.listingDate),
  index('idx_ipo_calendar_apply').on(t.applyDate),
  index('idx_ipo_calendar_market').on(t.market),
])
