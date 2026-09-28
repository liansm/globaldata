import 'dotenv/config'
import Fastify from 'fastify'
import cors from '@fastify/cors'
import { commoditiesRoutes } from './routes/commodities'
import { marketsRoutes } from './routes/markets'
import { cryptoRoutes } from './routes/crypto'
import { fundsRoutes } from './routes/funds'
import { fundCompaniesRoutes } from './routes/fundCompanies'
import { privateFundsRoutes } from './routes/privateFunds'
import { ipoRoutes } from './routes/ipo'

const app = Fastify({
  logger: {
    transport: {
      target: 'pino-pretty',
      options: { colorize: true },
    },
  },
})

// ── Plugins ─────────────────────────────────────────────────────────────────
app.register(cors, {
  origin: true,  // allow all origins in dev; restrict in production
})

// ── Routes ───────────────────────────────────────────────────────────────────
app.register(commoditiesRoutes)
app.register(marketsRoutes)
app.register(cryptoRoutes)
app.register(fundsRoutes)
app.register(fundCompaniesRoutes)
app.register(privateFundsRoutes)
app.register(ipoRoutes)

// Health check
app.get('/health', async () => ({
  status: 'ok',
  time:   new Date().toISOString(),
}))

export default app
