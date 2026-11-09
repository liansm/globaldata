import { FastifyInstance } from 'fastify'
import { sql } from 'drizzle-orm'
import { db } from '../db'
import { LAYOUT, STMT_LABEL, IS_STMT, type Stmt } from '../lib/statementLayout'

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
        // 单季利润表明细（季度对比表前 9 列：总收入→净利润）
        qGrossProfit:      toNum(r.q_gross_profit),
        // 单季毛利率：分子分母都用单季值，金融业不适用（与累计口径的 grossMargin 分开）
        qGrossMargin:      r.org_type != null && String(r.org_type) !== '通用' ? null
                           : (toNum(r.q_operate_income) != null && toNum(r.q_operate_income)! > 0
                              && toNum(r.q_gross_profit) != null
                                ? (toNum(r.q_gross_profit)! / toNum(r.q_operate_income)! * 100)
                                : null),
        qSaleExpense:      toNum(r.q_sale_expense),
        qManageExpense:    toNum(r.q_manage_expense),
        qResearchExpense:  toNum(r.q_research_expense),
        qOperateProfit:    toNum(r.q_operate_profit),
        qIncomeTax:        toNum(r.q_income_tax),
        // 「净利润」= 合并口径（含少数股东损益），不是归母；归母看 parentNetprofit / qParentNetprofit
        qNetprofit:        toNum(r.q_netprofit),
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

  /**
   * GET /api/stocks/:code/statement
   *
   * 雪球式的**逐科目**三表：行 = 报表科目（带层级/顺序），列 = 报告期。
   * 数据直接来自 `raw`（三张表各 200~319 个字段全在库里），科目名与顺序由
   * `src/lib/statementLayout.ts` 的字典给出，**不需要重抓**。
   *
   * 口径：
   *   ① 三表都是**累计值**（一季报 = Q1、中报 = H1、三季报 = Q1~Q3、年报 = FY），
   *      与雪球/东财网页一致，这里不做单季差分（单季看另一个接口的 q* 字段）。
   *   ② 金融业（银行/证券/保险）用各自模板，字典里通用科目为空 → 整行隐藏，
   *      所以同一个字典能同时喂四类主体，前端不用分支。
   *   ③ 资产减值损失/信用减值损失源侧存「带符号的收益」，字典 sign=-1 转成报表习惯的正损失额。
   *   ④ 单位统一返回**元**，亿/万换算交给前端（每股收益是元/股，标 fmt='plain'）。
   */
  app.get<{
    Params: { code: string }
    Querystring: {
      type?: string; filter?: string; page?: string; pageSize?: string
    }
  }>('/api/stocks/:code/statement', async (req) => {
    const code = req.params.code.trim()
    const q = req.query
    const type: Stmt = q.type && IS_STMT(q.type) ? q.type : 'BALANCE'
    // 全部 / 年报 / 中报 / 一季报 / 三季报 —— 按 quarter_num 过滤
    const FILTERS: Record<string, number[] | null> = {
      all: null, annual: [4], h1: [2], q1: [1], q3: [3],
    }
    const filter = q.filter && q.filter in FILTERS ? q.filter : 'all'
    const quarters = FILTERS[filter]
    const pageSize = Math.min(8, Math.max(1, parseInt(q.pageSize ?? '5', 10) || 5))
    const page = Math.max(1, parseInt(q.page ?? '1', 10) || 1)

    const empty = {
      found: false, code, type, typeLabel: STMT_LABEL[type], filter,
      page, pageSize, totalPeriods: 0, orgType: null as string | null,
      currency: null as string | null, periods: [] as unknown[],
      sections: [] as unknown[], unit: '元',
    }

    // 过滤条件只有 1/2/3/4 这几个白名单整数，用 sql.raw 拼安全
    const cond = quarters
      ? sql`AND quarter_num IN (${sql.raw(quarters.map(n => Number(n)).join(','))})`
      : sql``

    const [cntRes, listRes] = await Promise.all([
      db.execute(sql`
        SELECT COUNT(*)::int AS n FROM stock_financials
        WHERE code = ${code} ${cond}
      `),
      db.execute(sql`
        SELECT report_date::text AS report_date, report_year, quarter_num, report_type,
               org_type, currency, raw
        FROM stock_financials
        WHERE code = ${code} ${cond}
        ORDER BY report_date DESC
        LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}
      `),
    ])

    const totalPeriods = Number((cntRes.rows as Row[])[0]?.n ?? 0)
    const list = listRes.rows as Row[]
    if (!totalPeriods || !list.length) return { ...empty, totalPeriods }

    const periods = list.map(r => ({
      reportDate: day(r.report_date),
      reportYear: toInt(r.report_year),
      quarterNum: toInt(r.quarter_num),
      reportType: r.report_type ?? null,
      // 雪球式列头：2026三季报 / 2025年报
      label: `${r.report_year ?? ''}${r.report_type ?? ''}`,
    }))

    // 逐科目取数：直接读 raw；整行为空（该模板没有这个科目）就不展示
    const built = LAYOUT[type]
      .map(sec => ({
        title: sec.title,
        rows: sec.lines
          .map(line => {
            const values = list.map(r => {
              const d = ((r.raw as Record<string, Record<string, unknown>>) ?? {})[type] ?? {}
              let v = d[line.key]
              if (v == null || v === '' || v === '-') v = line.alt ? d[line.alt] : null
              if (v == null || v === '' || v === '-') return null
              const n = Number(v)
              if (!Number.isFinite(n)) return null
              // 减值损失：源侧是带符号收益，报表显示正的损失额
              return line.sign === -1 ? -n : n
            })
            return {
              key: line.key,
              label: line.label,
              kind: line.kind ?? 'item',
              fmt: line.fmt ?? 'money',
              values,
            }
          })
          .filter(r => r.values.some(v => v != null)),
      }))
      .filter(s => s.rows.length)

    // 银行 / 证券 / 保险不按「流动 / 非流动」划分（源侧那两组合计全是空），
    // 把同名分组标题合并成「资产」「负债」，否则会留下几个只剩一行的小标题。
    const orgTypeRaw = list[0].org_type == null ? '' : String(list[0].org_type)
    const sections = (() => {
      if (!orgTypeRaw || orgTypeRaw === '通用') return built
      const merged: typeof built = []
      for (const s of built) {
        const t = s.title === '流动资产' || s.title === '非流动资产' ? '资产'
          : s.title === '流动负债' || s.title === '非流动负债' ? '负债'
          : s.title
        const last = merged[merged.length - 1]
        if (last && last.title === t) last.rows.push(...s.rows)
        else merged.push({ title: t, rows: [...s.rows] })
      }
      return merged
    })()

    const head = list[0]
    return {
      found: sections.length > 0,
      code,
      type,
      typeLabel: STMT_LABEL[type],
      filter,
      page,
      pageSize,
      totalPeriods,
      orgType: head.org_type == null ? null : String(head.org_type),
      currency: head.currency == null ? null : String(head.currency),
      periods,
      sections,
      unit: '元',
    }
  })
}
