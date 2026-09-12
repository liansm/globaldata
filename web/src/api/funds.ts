import axios from 'axios'
import type { FundListResp, FundTypeCount, FundDetailResp } from '@/types/fund'

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
