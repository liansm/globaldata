import axios from 'axios'
import type {
  FundListResp,
  FundTypeCount,
  FundDetailResp,
  FundNavResp,
  FundYearlyResp,
} from '@/types/fund'

const http = axios.create({
  baseURL: '/api',
  timeout: 15_000,
})

export function fetchFunds(options: {
  type?: string
  q?: string
  withScale?: boolean
  page?: number
  pageSize?: number
} = {}): Promise<FundListResp> {
  return http
    .get<FundListResp>('/funds', {
      params: {
        type: options.type || undefined,
        q: options.q || undefined,
        withScale: options.withScale ? 1 : undefined,
        page: options.page ?? 1,
        pageSize: options.pageSize ?? 50,
      },
    })
    .then(r => r.data)
}

export function fetchFundTypes(): Promise<FundTypeCount[]> {
  return http.get<FundTypeCount[]>('/funds/types').then(r => r.data)
}

export function fetchFundDetail(code: string, date?: string): Promise<FundDetailResp> {
  return http
    .get<FundDetailResp>(`/funds/${code}`, { params: date ? { date } : {} })
    .then(r => r.data)
}

/** 净值序列（日线）。days=0 表示取全部历史；order='desc' + offset 供分页表格用 */
export function fetchFundNav(
  code: string,
  options: {
    days?: number
    from?: string
    to?: string
    limit?: number
    offset?: number
    order?: 'asc' | 'desc'
  } = {},
): Promise<FundNavResp> {
  return http
    .get<FundNavResp>(`/funds/${code}/nav`, {
      params: {
        days: options.days ?? 365,
        from: options.from || undefined,
        to: options.to || undefined,
        limit: options.limit,
        offset: options.offset,
        order: options.order,
      },
    })
    .then(r => r.data)
}

/** 历史业绩：各自然年涨跌幅，按年份降序 */
export function fetchFundYearly(code: string): Promise<FundYearlyResp> {
  return http.get<FundYearlyResp>(`/funds/${code}/yearly`).then(r => r.data)
}
