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
