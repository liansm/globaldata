import axios from 'axios'
import type {
  FundListResp,
  FundTypeCount,
  FundDetailResp,
  FundNavResp,
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

/** 净值序列（日线，升序）。days=0 表示取全部历史 */
export function fetchFundNav(
  code: string,
  options: { days?: number; from?: string; to?: string } = {},
): Promise<FundNavResp> {
  return http
    .get<FundNavResp>(`/funds/${code}/nav`, {
      params: {
        days: options.days ?? 365,
        from: options.from || undefined,
        to: options.to || undefined,
      },
    })
    .then(r => r.data)
}
