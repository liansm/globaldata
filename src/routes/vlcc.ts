import { FastifyInstance } from 'fastify'
import { db } from '../db'
import { vlccVessels, vlccPositions } from '../db/schema'
import { eq, and, gte, desc, sql } from 'drizzle-orm'

// numeric 列在 pg 驱动里返回**字符串**，必须在 API 层转 number，
// 否则前端 toFixed / 算术直接崩（本项目踩过多次）
const toNum = (v: string | null) => (v == null ? null : parseFloat(v))

const staleHours = (ts: Date | null): number | null =>
  ts == null ? null : (Date.now() - new Date(ts).getTime()) / 36e5

export async function vlccRoutes(app: FastifyInstance) {

  // ── GET /api/vlcc/fleet ──────────────────────────────────────────────────
  // 「油运信息」地图主接口：中国船东 VLCC 名录 + 每艘的**最新船位**。
  //
  // Query:
  //   owner       = 全部(默认) | 招商轮船 | 中远海能
  //   onlyWithPos = 1 时只返回有船位的（地图打点用；名录列表用全量）
  //
  // ⚠ 船位可能很旧：`ts` 是 AIS 报文时间，远洋无岸基覆盖时几小时到几天都正常。
  //   返回 `staleHours` 让前端显示「更新于 X 小时前」，**不要默认实时**。
  app.get<{
    Querystring: { owner?: string; onlyWithPos?: string }
  }>('/api/vlcc/fleet', async (req) => {
    const owner = req.query.owner && req.query.owner !== '全部' ? req.query.owner : null
    const onlyWithPos = req.query.onlyWithPos === '1' || req.query.onlyWithPos === 'true'

    // 1) 名录
    const vessels = await db
      .select()
      .from(vlccVessels)
      .where(owner ? eq(vlccVessels.owner, owner) : undefined)
      .orderBy(vlccVessels.owner, vlccVessels.nameAis)

    // 2) 每艘船的最新船位（DISTINCT ON 走 (name_ais, ts DESC) 索引）
    const latest = await db
      .selectDistinctOn([vlccPositions.nameAis], {
        nameAis:   vlccPositions.nameAis,
        ts:        vlccPositions.ts,
        lat:       vlccPositions.lat,
        lon:       vlccPositions.lon,
        sog:       vlccPositions.sog,
        cog:       vlccPositions.cog,
        heading:   vlccPositions.heading,
        navStatus: vlccPositions.navStatus,
        dest:      vlccPositions.dest,
        mmsi:      vlccPositions.mmsi,
      })
      .from(vlccPositions)
      .orderBy(vlccPositions.nameAis, desc(vlccPositions.ts))

    const posOf = new Map(latest.map(p => [p.nameAis, p]))

    // 3) 合并
    const items = vessels.map(v => {
      const p = posOf.get(v.nameAis) ?? null
      return {
        nameAis:   v.nameAis,
        nameEn:    v.nameEn,
        nameCn:    v.nameCn,
        owner:     v.owner,
        ownerFull: v.ownerFull,
        dwt:       toNum(v.dwt),
        builtYear: v.builtYear,
        flag:      v.flag,
        imo:       v.imo,
        mmsi:      v.mmsi ?? p?.mmsi ?? null,
        verified:  v.verified,
        source:    v.source,
        pos: p ? {
          ts:        p.ts,
          lat:       toNum(p.lat),
          lon:       toNum(p.lon),
          sog:       toNum(p.sog),
          cog:       toNum(p.cog),
          heading:   toNum(p.heading),
          navStatus: p.navStatus,
          dest:      p.dest,
          staleHours: staleHours(p.ts),
        } : null,
      }
    })

    const filtered = onlyWithPos ? items.filter(i => i.pos) : items

    // 4) 概况（**用过滤前的全量**统计，否则前端会以为名录只有几艘船）
    const withPos = items.filter(i => i.pos).length
    const tsList = items.map(i => i.pos?.ts).filter(Boolean) as Date[]
    const byOwner: Record<string, { total: number; withPos: number }> = {}
    for (const i of items) {
      const b = (byOwner[i.owner] ??= { total: 0, withPos: 0 })
      b.total++
      if (i.pos) b.withPos++
    }

    return {
      stats: {
        total: items.length,
        withPos,
        byOwner,
        latestTs: tsList.length ? new Date(Math.max(...tsList.map(t => +new Date(t)))) : null,
        // 名录覆盖口径，直接回给前端展示，避免「以为这就是全部中国 VLCC」
        coverageNote: '口径 = 招商轮船 + 中远海能（中国船东）。中远海能名录为 2021-06 官方快照，'
                    + '此后交付的新船与其它中国船东（中石油/中石化/山东海运等）尚未纳入。'
                    + '船位源 = HiFleet（岸基 + 卫星）；个别船处于 AIS 静默期时无位置，属正常。',
      },
      vessels: filtered,
    }
  })

  // ── GET /api/vlcc/vessel/:name ───────────────────────────────────────────
  // 单船详情 + 轨迹。`:name` 用 name_ais（英文船名规范化，大写）。
  // Query: days = 回看天数（默认 30，上限 365）
  //
  // ⚠ 本项目约定：详情接口对不存在的键若返 200 + {found:false}，前端**必须显式分支**，
  //   否则会渲染成全 `—` 的空白页。
  app.get<{
    Params: { name: string }
    Querystring: { days?: string }
  }>('/api/vlcc/vessel/:name', async (req) => {
    const name = decodeURIComponent(req.params.name).trim().toUpperCase()
    const days = Math.min(Math.max(parseInt(req.query.days ?? '30', 10) || 30, 1), 365)

    const [v] = await db
      .select()
      .from(vlccVessels)
      .where(eq(vlccVessels.nameAis, name))
      .limit(1)

    if (!v) return { found: false as const, name }

    const since = new Date(Date.now() - days * 864e5)
    const track = await db
      .select({
        ts:        vlccPositions.ts,
        lat:       vlccPositions.lat,
        lon:       vlccPositions.lon,
        sog:       vlccPositions.sog,
        cog:       vlccPositions.cog,
        navStatus: vlccPositions.navStatus,
        dest:      vlccPositions.dest,
      })
      .from(vlccPositions)
      .where(and(eq(vlccPositions.nameAis, name), gte(vlccPositions.ts, since)))
      .orderBy(vlccPositions.ts)

    return {
      found: true as const,
      vessel: {
        nameAis:   v.nameAis,
        nameEn:    v.nameEn,
        nameCn:    v.nameCn,
        owner:     v.owner,
        ownerFull: v.ownerFull,
        dwt:       toNum(v.dwt),
        builtYear: v.builtYear,
        flag:      v.flag,
        imo:       v.imo,
        mmsi:      v.mmsi,
        verified:  v.verified,
        source:    v.source,
      },
      days,
      track: track.map(p => ({
        ts:        p.ts,
        lat:       toNum(p.lat),
        lon:       toNum(p.lon),
        sog:       toNum(p.sog),
        cog:       toNum(p.cog),
        navStatus: p.navStatus,
        dest:      p.dest,
      })),
    }
  })

  // ── GET /api/vlcc/owners ─────────────────────────────────────────────────
  // 船东清单（含各自艘数），给前端筛选器用 —— 别在前端硬编码船东名
  app.get('/api/vlcc/owners', async () => {
    const rows = await db
      .select({
        owner: vlccVessels.owner,
        total: sql<number>`COUNT(*)::int`,
      })
      .from(vlccVessels)
      .groupBy(vlccVessels.owner)
      .orderBy(desc(sql`COUNT(*)`))
    return rows
  })
}
