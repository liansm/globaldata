import {
  pgTable,
  varchar,
  integer,
  numeric,
  date,
  timestamp,
  serial,
  bigserial,
  bigint,
  boolean,
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
//
// 数据源分层（market 取值 → 抓取脚本）：
//   'A股'|'港股'|'美股'|'欧洲'|'亚太' → fetch_markets.py + fetch_index_spot.py
//   '资金流向'                       → fetch_markets.py（沪深港通）
//   '航运'                           → fetch_bdi.py  BDI/BCI/BSI 干散货 + BDTI/BCTI 油运
//                                                 （2006-07 起日频，**含全历史**）
//                                       fetch_ccfi.py / fetch_ccfi_history.py  出口集装箱
//                                                 （周频 / 2023-04 起）
//                                       fetch_ctfi.py  **中国进口原油运价指数 + VLCC 各航线
//                                                 WS / 美元每吨 / TCE**（日频，但页面上只给
//                                                 当期值、历史是付费墙 → **只能滚存**）
//   '建材'                           → fetch_cement.py（CEMPI）
// ⚠ 同表内各序列的「可回补性」差别很大：BDI 系列含全历史，
//   **CCFI / CTFI 只能靠定期运行累积，跑漏的期次永久缺失。**
// --------------------------------------------------------------------------
export const marketIndices = pgTable('market_indices', {
  key:       varchar('key',    { length: 60  }).primaryKey(),
  symbol:    varchar('symbol', { length: 60  }).notNull(),
  name:      varchar('name',   { length: 200 }).notNull(),
  market:    varchar('market', { length: 50  }).notNull(),   // 'A股'|'港股'|'资金流向'|'航运'|'建材'|'美股'|'欧洲'|'亚太'
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
// 私募基金（2026-09-17 落地，fetch_private_funds.py）
//
// ⚠ 私募和公募不是一个物种：公募的「名录 / 规模 / 持仓 / 净值」四层，私募
//   **只有名录能全量对标**。《私募投资基金募集行为管理办法》禁止公开宣传推介
//   与披露业绩，规模/持仓/净值在公开渠道拿不到 —— 监管口径，不是技术问题。
//
// private_managers — 管理人（中基协公示，~1.84 万家）
// private_funds    — 备案产品（中基协公示，~25 万只）+ 代销池净值快照
// private_fund_nav — 净值日线。**两个来源、两种口径**（2026-09-20 起）：
//     source=em_gaoduan 天天基金高端理财代销池：unit_nav=单位净值、acc_nav=累计净值（现金分红累加）
//     source=sppw       私募排排网 fundNavTrend： acc_nav=**复权净值（分红再投）**、unit_nav 留空
//   ⚠ 两源在「有分红」标的上终身收益最多差 2.5 倍，**禁止跨源拼同一条序列**。
//     写库时对与代销池重叠的 fund_no 一律跳过（见 .workbuddy/load_simu_nav.py）。
// --------------------------------------------------------------------------
export const privateManagers = pgTable('private_managers', {
  registerNo:       varchar('register_no',       { length: 40 }).primaryKey(),  // 登记编号
  managerId:        bigint('manager_id', { mode: 'number' }),  // 中基协内部 id（大数，超出 int4）
  managerName:      varchar('manager_name',      { length: 200 }).notNull(),
  artificialPerson: varchar('artificial_person', { length: 120 }),  // 法定代表人
  investType:       varchar('invest_type',       { length: 80  }),  // 机构类型
  registerProvince: varchar('register_province', { length: 80  }),
  officeAddress:    varchar('office_address',    { length: 300 }),
  establishDate:    date('establish_date'),
  registerDate:     date('register_date'),
  fundCount:        integer('fund_count'),        // 在管基金数量
  memberType:       varchar('member_type',       { length: 80  }),
  hasSpecialTips:   boolean('has_special_tips'),
  hasCreditTips:    boolean('has_credit_tips'),
  updatedAt:        timestamp('updated_at', { withTimezone: true })
                      .default(sql`NOW()`).notNull(),
})

export const privateFunds = pgTable('private_funds', {
  fundNo:           varchar('fund_no',   { length: 40 }).primaryKey(),  // 备案编码，与代销池同码
  fundName:         varchar('fund_name', { length: 300 }).notNull(),
  managerName:      varchar('manager_name', { length: 200 }),
  managerId:        bigint('manager_id', { mode: 'number' }),
  managerType:      varchar('manager_type', { length: 40  }),   // 受托管理 / ...
  workingState:     varchar('working_state',{ length: 40  }),   // 正在运作 / 延期清算 / ...
  recordDate:       date('record_date'),                        // 备案时间
  establishDate:    date('establish_date'),
  mandatorName:     varchar('mandator_name', { length: 200 }),  // 托管人
  isDeputeManage:   boolean('is_depute_manage'),
  inRegistry:       boolean('in_registry').notNull().default(false),  // 在中基协备案库
  hasNav:           boolean('has_nav').notNull().default(false),      // 有代销池净值
  latestNav:        numeric('latest_nav',          { precision: 14, scale: 4 }),
  latestAccNav:     numeric('latest_acc_nav',      { precision: 14, scale: 4 }),
  latestNavDate:    date('latest_nav_date'),
  latestDailyReturn:numeric('latest_daily_return', { precision: 10, scale: 4 }),
  latestFundSize:   numeric('latest_fund_size',    { precision: 20, scale: 2 }),  // 元；多数源侧不给
  navUpdatedAt:     timestamp('nav_updated_at', { withTimezone: true }),
  updatedAt:        timestamp('updated_at', { withTimezone: true })
                      .default(sql`NOW()`).notNull(),
})

export const privateFundNav = pgTable('private_fund_nav', {
  id:          bigserial('id', { mode: 'number' }).primaryKey(),
  fundNo:      varchar('fund_no',  { length: 40 }).notNull(),
  navDate:     date('nav_date').notNull(),
  unitNav:     numeric('unit_nav',     { precision: 14, scale: 4 }),
  accNav:      numeric('acc_nav',      { precision: 14, scale: 4 }),  // 含分红口径：累计净值(em) / 复权净值(sppw)
  dailyReturn: numeric('daily_return', { precision: 10, scale: 4 }),  // 日涨跌 %
  source:      varchar('source',       { length: 16 }).notNull().default('em_gaoduan'),
}, (t) => [
  unique('private_fund_nav_uniq').on(t.fundNo, t.navDate),
  index('idx_pf_nav_fund_date').on(t.fundNo, t.navDate),
  index('idx_pf_nav_source').on(t.source),
])

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

// --------------------------------------------------------------------------
// VLCC 船队（2026-09-30 落地，「油运信息」地图页）
//   vlcc_vessels   ← fetch_vlcc_fleet.py            （名录：船东→船名，静态）
//   vlcc_positions ← fetch_vlcc_position_hifleet.py （船位主源：HiFleet REST，付费）
//                  ← fetch_vlcc_position.py         （船位副源：aisstream.io 流，免费但覆盖塌陷）
//   source 列区分两条来源：'hifleet' / 'aisstream'。同一 (name_ais, ts) 唯一，两源可共存。
//
// 为什么单开表，不塞进 market_indices / index_prices
// -------------------------------------------------
// 这里是「**实体（船）+ 时空点（船位）**」，不是时间序列。船有船东/吨位/建造年等
// 静态属性，船位是 (lat,lon,sog,cog…) —— 塞进 index_prices 的 `close` 会丢语义，
// 而且一艘船位数据点没有「开高低收」。
//
// ⚠ 三条口径，别搞错
// -------------------------------------------------
// ① 名录口径 = **中国船东**（招商轮船 / 中远海能），**不是挂旗口径**。
//    这两家的 VLCC 大量挂中国香港旗 / 新加坡旗 / 巴拿马旗 / 利比里亚旗，
//    按「挂中国旗」筛会漏掉绝大多数。船旗只在 `flag` 列做参考，不参与筛选。
// ② `name_ais` = 英文船名规范化（大写 + 折叠空白）→ **AIS 匹配键**。
//    AIS 只认船名 / MMSI，**不认船东**；没有这份名录就没法从全球 AIS 流里
//    挑出「中国的 VLCC」。所以名录不是装饰，是过滤器本身。
// ③ 名录源侧**都不给 IMO/MMSI**（中远海能官方 PDF、chinashipbuild 都没有），
//    由船位脚本 `--learn` 从 AIS 的 ShipStaticData 反推回写，并把 verified 置真。
//    `verified=false` 只代表「还没被 AIS 实见过」，**不代表这船不存在**。
//
// ⚠ 船位时间语义：`ts` 是 **AIS 报文时间（UTC）**，不是我们收到的时间。
//   远洋船没有岸基 AIS 覆盖时，最新报文可能已过去几小时甚至几天 ——
//   这是 AIS 的固有限制，**不是数据坏了**。前端必须显示「更新于 X 小时前」，
//   不能默认所有点都是实时的。
//
// ⚠ 名录口径（2026-10-01 定案，别再问「要不要换成在役源」）
// -------------------------------------------------
// 两个船东的**名录时效性根本不同**，所以必须各记各的，不能混着当「当前船队」：
//   * 招商轮船：`chinashipbuild` **在役**船队库 → `roster_asof` = 抓取当日。
//   * 中远海能：官网《本集团自有油轮运力》**2021-06-30 官方 PDF 快照**
//     → `roster_asof` = 2021-06-30，语义是「2021-06-30 在册」，**不是「当前在役」**。
// 为什么不把中远也换成「当前在役」源：**公开渠道根本不存在逐船名的中远在役清单**。
//   2025 年报（2026-03-26）只给「油轮 155 艘 / 2257.6 万载重吨」这类**船型汇总**；
//   券商研报（东方证券 2026-07-09）给到「VLCC 41 自有 + 7 租入 / 1472 万载重吨」，
//   同样**不给船名**；船队库（chinashipbuild）只覆盖招商；逐船名的在役库
//   （Equasis / Miramar / Clarksons）都是付费产品，不在本项目口径内。
//   ⇒ 硬凑一个「当前口径」只会引入来源不明的数据，**宁缺勿错**：保留有出处的快照，
//     把「它是什么口径」写进 `roster_asof`，把「哪些已经不属于它了」写进 `roster_status`。
// `roster_status = 'retired'` = 已核实转手/改名（内容见 `status_note`），**唯一出处是
//   `fetch_vlcc_position_hifleet.py` 顶部的 `RETIRED` 表**，每次运行投影到本列。
//   前端必须把 retired 显示成「已转手」，**不能和「AIS 静默期无船位」混为一谈**。
// --------------------------------------------------------------------------
export const vlccVessels = pgTable('vlcc_vessels', {
  id:        bigserial('id', { mode: 'number' }).primaryKey(),
  nameAis:   varchar('name_ais',   { length: 120 }).notNull(),
  nameEn:    varchar('name_en',    { length: 120 }).notNull(),
  nameCn:    varchar('name_cn',    { length: 120 }),   // 拿不到就留空，不猜
  owner:     varchar('owner',      { length: 60  }).notNull(),   // '招商轮船' | '中远海能'
  ownerFull: varchar('owner_full', { length: 200 }),
  dwt:       numeric('dwt',        { precision: 14, scale: 2 }),
  builtYear: integer('built_year'),
  flag:      varchar('flag',       { length: 12 }),    // CN/HK/SG/PA/LR/MH/MT
  source:    varchar('source',     { length: 200 }).notNull(),
  imo:       varchar('imo',        { length: 16 }),
  mmsi:      varchar('mmsi',       { length: 16 }),
  verified:  boolean('verified').notNull().default(false),
  // ── 名录口径（见上方长注释）──────────────────────────────────────────────
  rosterAsof:   date('roster_asof'),                    // 该条名录的口径截止日；招商=抓取日，中远=2021-06-30
  rosterStatus: varchar('roster_status', { length: 12 }).notNull().default('active'), // active | retired
  statusNote:   varchar('status_note',   { length: 200 }),  // retired 时：现名 / 转手时间 / 依据
  updatedAt: timestamp('updated_at', { withTimezone: true })
               .default(sql`NOW()`).notNull(),
}, (t) => [
  unique('vlcc_vessels_name_ais_uniq').on(t.nameAis),
  index('idx_vlcc_vessels_owner').on(t.owner),
])

export const vlccPositions = pgTable('vlcc_positions', {
  id:        bigserial('id', { mode: 'number' }).primaryKey(),
  nameAis:   varchar('name_ais', { length: 120 }).notNull(),
  mmsi:      varchar('mmsi',     { length: 16  }),
  ts:        timestamp('ts', { withTimezone: true }).notNull(),
  lat:       numeric('lat',     { precision: 10, scale: 6 }).notNull(),
  lon:       numeric('lon',     { precision: 10, scale: 6 }).notNull(),
  sog:       numeric('sog',     { precision: 8, scale: 2 }),
  cog:       numeric('cog',     { precision: 8, scale: 2 }),
  heading:   numeric('heading', { precision: 8, scale: 2 }),
  navStatus: varchar('nav_status', { length: 40 }),
  dest:      varchar('dest',    { length: 120 }),
  draught:   numeric('draught', { precision: 8, scale: 2 }),
  source:    varchar('source',  { length: 40 }).notNull().default('aisstream'),
  createdAt: timestamp('created_at', { withTimezone: true })
               .default(sql`NOW()`).notNull(),
}, (t) => [
  unique('vlcc_positions_name_ts_uniq').on(t.nameAis, t.ts),
  index('idx_vlcc_positions_name_ts').on(t.nameAis, t.ts),
])

// --------------------------------------------------------------------------
// 外汇 / 人民币汇率（2026-10-01 落地，fetch_fx.py）
//   fx_pairs ← 货币对元数据（25 对，手工维护在脚本的 PAIRS 里）
//   fx_rates ← 日线序列。**两个口径分开存**：
//              rate_type='mid' 中间价(cfets) / 'spot' 即期(sina)
//   fx_spot  ← 即期实时快照（新浪 hq.sinajs，一对一行，与 index_spot 同模式）
//
// ⚠ 唯一数值口径：`fx_rates.close` = **1 单位外币 兑 人民币**（CNY per 1 unit foreign）
//   CFETS 官方对 10 对报「外币/人民币」（USD/CNY…），对另 15 对报「人民币/外币」
//   （CNY/KRW 201.70 → 入库 0.0049579，取倒数）。统一方向是刻意的：
//   否则同一页会出现「USD/CNY 上行=人民币贬值」与「CNY/KRW 上行=人民币升值」
//   两种互斥语义，涨跌红绿互相矛盾。
//   ⇒ 本表内 **数值上行 = 人民币贬值**，全表唯一。
//   `fx_pairs.quote_unit`（日元 100，其余 1）只管**展示倍数**，不改语义。
//
// ⚠ 两个口径的覆盖与深度不同，**不能当它们可以互换，更禁止拼成一条序列**：
//   中间价(cfets) 25 对，官方定价，官网历史起点 2006-01；每工作日 9:15 发布，
//     周末/法定节假日**没有数据**（是「当天就没发」，不是「没抓到」）。
//   即期(sina)   19 对（缺 KRW/SAR/HUF/PLN/TRY/MXN），市场价；
//     USDCNY 1994-08 起、EURCNY 2008-09 起，其余多数 **2023-07 起且被截断在 1000 根**。
//
// ⚠ `mid` 只有 close（官方只给单一中间价，没有开高低）；`spot` 才有 OHLC。
//   UI 上 mid 的 OHLC 一律显示「—」，**不要用 close 充数**。
// --------------------------------------------------------------------------
export const fxPairs = pgTable('fx_pairs', {
  key:          varchar('key',         { length: 40 }).primaryKey(),  // 'usd_cny'
  baseCode:     varchar('base_code',   { length: 8  }).notNull(),     // 'USD'
  baseName:     varchar('base_name',   { length: 40 }).notNull(),     // '美元'
  quoteUnit:    integer('quote_unit').notNull().default(1),           // 展示倍数：JPY=100
  category:     varchar('category',    { length: 20 }).notNull(),     // '主要货币' | '其他货币'
  officialCode: varchar('official_code', { length: 24 }),             // CFETS 原始代码 '100JPY/CNY'
  spotCode:     varchar('spot_code',     { length: 24 }),             // 新浪即期代码 'fx_sjpycny'
  sortOrder:    integer('sort_order').notNull().default(100),
  updatedAt:    timestamp('updated_at', { withTimezone: true })
                  .default(sql`NOW()`).notNull(),
}, (t) => [
  index('idx_fx_pairs_category').on(t.category),
])

export const fxRates = pgTable('fx_rates', {
  id:       bigserial('id', { mode: 'number' }).primaryKey(),
  pairKey:  varchar('pair_key',  { length: 40 }).notNull(),
  rateDate: date('rate_date').notNull(),
  rateType: varchar('rate_type', { length: 8 }).notNull(),   // 'mid' 中间价 | 'spot' 即期
  open:     numeric('open',  { precision: 20, scale: 10 }),
  high:     numeric('high',  { precision: 20, scale: 10 }),
  low:      numeric('low',   { precision: 20, scale: 10 }),
  close:    numeric('close', { precision: 20, scale: 10 }),
  source:   varchar('source', { length: 16 }).notNull(),     // 'cfets' | 'sina'
}, (t) => [
  unique('fx_rates_uniq').on(t.pairKey, t.rateDate, t.rateType),
  index('idx_fx_rates_pair_date').on(t.pairKey, t.rateDate),
  index('idx_fx_rates_type').on(t.rateType),
])

export const fxSpot = pgTable('fx_spot', {
  pairKey:   varchar('pair_key',   { length: 40 }).primaryKey(),
  price:     numeric('price',      { precision: 20, scale: 10 }),
  prevClose: numeric('prev_close', { precision: 20, scale: 10 }),
  open:      numeric('open',       { precision: 20, scale: 10 }),
  high:      numeric('high',       { precision: 20, scale: 10 }),
  low:       numeric('low',        { precision: 20, scale: 10 }),
  changePct: numeric('change_pct', { precision: 10, scale: 4 }),
  changeAmt: numeric('change_amt', { precision: 20, scale: 10 }),
  quoteTime: varchar('quote_time', { length: 16 }),   // 源侧报价时间 HH:MM:SS
  spotDate:  date('spot_date'),
  updatedAt: timestamp('updated_at', { withTimezone: true })
               .default(sql`NOW()`).notNull(),
})
