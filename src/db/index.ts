import { drizzle } from 'drizzle-orm/node-postgres'
import { Pool } from 'pg'
import * as schema from './schema'

if (!process.env.DATABASE_URL) {
  throw new Error('DATABASE_URL is not set. Check your .env file.')
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,  // connection pool size
})

export const db = drizzle(pool, { schema })
