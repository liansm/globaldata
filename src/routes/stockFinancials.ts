import { FastifyInstance } from 'fastify'
import { sql } from 'drizzle-orm'
import { db } from '../db'

// ⚠ numeric 列在 pg 驱动里返回字符串，必须转 number（本项目踩过多次）
const toNum = (v: unknown): number | null => (v == null ? null : Number(v))
const day = (v: unknown): string | null => (v == null ? null : String(v).slice(0, 10))
const toInt = (v: unknown): number | null => (v == null ? null : Number(v))

type Row = Record<string, unknown>

/**
 * GET /api/stocks/:code/financials
 *
 * 逐季度对比：一行 = 一个报告期，同时给「累计值」与「单季差分值」。
 * 单季由视图 v_stock_financials_quarterly 用 LAG 年内差分，**不落派生表**
 * （照 stock_prices 周/月线由 date_trunc 聚合的先例）。
 *
 * ⚠ 口径（改这里前读 fetch_stock_financials.py docstring）：
 *   ① 利润表 / 现金流量表 = 年初至今累计；资产负债表 = 季末时点值，不差分。
 *   ② 毛利率分母是 operate_income（营业收入），不是 total_operate_income。
 *   ③ 金融业（银行/证券/保险）毛利率、速动比、流动比率在视图里直接置 NULL，
 *      UI 显示「—」，不用 0 充数。
 *   ④ 股东户数 END_DATE 是统计截止日，按「该报告期所在季度内最后一个 END_DATE」
 *      映射，并把真实日期一并返回，不硬凑成季末。
 *   ⑤ 员工数只有「当前值 + 年度快照」，单独返回 employee，不伪装成每季度的列。
 *   ⑥ 详情类接口一律 200 + {found:false}（axios 对 404 直接抛异常），并补齐
 *      rows/total/page/pageSize，前端必须显式分支。
 */
export function stockFinancialsRoutes(app: FastifyInstance) {
  app.get<{
    Params: { code: string }
    Querystring: { limit?: string }
  }>('/api/stocks/:code/financials', async (req) => {
    const code = req.params.code.trim()
    const limit = Math.min(120, Math.max(1, parseInt(req.query.limit ?? '40', 10) || 40))
    const empty = {
      found: false, code, orgType: null as string | null, isFinancial: false,
      latestReportDate: null as string | null, employee: null as unknown,
      rows: [] as unknown[], total: 0, page: 1, pageSize: limit,
    }

    // 股东户数：严格取该报告期所在季度内的最后一个统计截止日。
    // 用 LATERAL 而不是 LEFT JOIN 全表，避免 25 万行股东户数把查询拖垮。
    const [listRes, metaRes, empRes] = await Promise.all([
      db.execute(sql`
        SELECT v.*,
               h.holder_num, h.end_date AS holder_end_date
        FROM v_stock_financials_quarterly v
        LEFT JOIN LATERAL (
          SELECT hc.holder_num, hc.end_date
          FROM stock_holder_counts hc
          WHERE hc.code = v.code
            AND hc.end_date <= v.report_date
            AND hc.end_date >= date_trunc('quarter', v.report_date::timestamp)::date
          ORDER BY hc.end_date DESC
          LIMIT 1
        ) h ON true
        WHERE v.code = ${code}
        ORDER BY v.report_date DESC
        LIMIT ${limit}
      `),
      db.execute(sql`
        SELECT COUNT(*)::int AS n, MAX(report_date)::text AS latest,
               (SELECT org_type FROM stock_financials
                 WHERE code = ${code} ORDER BY report_date DESC LIMIT 1) AS org_type
        FROM stock_financials WHERE code = ${code}
      `),
      db.execute(sql`
        SELECT snapshot_year, emp_num, collected_at::text AS collected_at
        FROM stock_employee_snapshots
        WHERE code = ${code} ORDER BY snapshot_year DESC LIMIT 1
      `),
    ])

    const meta = (metaRes.rows as Row[])[0] ?? {}
    const total = Number(meta.n ?? 0)
    if (!total) return empty

    const orgType = meta.org_type == null ? null : String(meta.org_type)
    const emp = (empRes.rows as Row[])[0]

    return {
      found: true,
      code,
      orgType,
      isFinancial: orgType != null && orgType !== '通用',
      latestReportDate: day(meta.latest),
      employee: emp ? {
        year: toInt(emp.snapshot_year),
        empNum: toInt(emp.emp_num),
        collectedAt: day(emp.collected_at),
      } : null,
      rows: (listRes.rows as Row[]).map(r => ({
        reportDate:        day(r.report_date),
        reportYear:        toInt(r.report_year),
        quarterNum:        toInt(r.quarter_num),
        reportType:        r.report_type ?? null,
        // 累计（年初至今）
        revenue:           toNum(r.total_operate_income),
        operateIncome:     toNum(r.operate_income),
        cost:              toNum(r.operate_cost),
        parentNetprofit:   toNum(r.parent_netprofit),
        netprofit:         toNum(r.netprofit),
        netcashOperate:    toNum(r.netcash_operate),
        // 单季（差分；缺上一期时为 null）
        qRevenue:          toNum(r.q_revenue),
        qParentNetprofit:  toNum(r.q_parent_netprofit),
        qNetcashOperate:   toNum(r.q_netcash_operate),
        // 资产负债表（季末时点值，禁止差分）
        totalAssets:       toNum(r.total_assets),
        totalCurrentAssets:toNum(r.total_current_assets),
        inventory:         toNum(r.inventory),
        totalCurrentLiab:  toNum(r.total_current_liab),
        totalEquity:       toNum(r.total_equity),
        // 派生指标（金融业为 null）
        grossMargin:       toNum(r.gross_margin),
        netMargin:         toNum(r.net_margin),
        quickRatio:        toNum(r.quick_ratio),
        currentRatio:      toNum(r.current_ratio),
        debtRatio:         toNum(r.debt_ratio),
        // 股东户数
        holderNum:         toInt(r.holder_num),
        holderEndDate:     day(r.holder_end_date),
        // 数据完整性 & 溯源
        hasIncome:         r.parent_netprofit != null || r.total_operate_income != null,
        hasBalance:        r.total_assets != null,
        hasCashflow:       r.netcash_operate != null,
        noticeDate:        day(r.notice_date),
        updateDate:        day(r.update_date),
      })),
      total,
      page: 1,
      pageSize: limit,
    }
  })
}
