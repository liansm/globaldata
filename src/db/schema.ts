import {
  pgTable,
  varchar,
  integer,
  numeric,
  date,
  timestamp,
  serial,
  bigserial,
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
