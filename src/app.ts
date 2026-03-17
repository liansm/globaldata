import 'dotenv/config'
import Fastify from 'fastify'
import cors from '@fastify/cors'
import { commoditiesRoutes } from './routes/commodities'
import { marketsRoutes } from './routes/markets'

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

// Health check
app.get('/health', async () => ({
  status: 'ok',
  time:   new Date().toISOString(),
}))

export default app
