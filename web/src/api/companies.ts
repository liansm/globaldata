import axios from 'axios'
import type { CompanyListResp, CompanyDetailResp } from '@/types/company'

const http = axios.create({
  baseURL: '/api',
  timeout: 20_000,
})

export function fetchCompanies(options: {
  q?: string
  order?: 'scale' | 'count' | 'name'
  page?: number
  pageSize?: number
} = {}): Promise<CompanyListResp> {
  return http
    .get<CompanyListResp>('/fund-companies', {
      params: {
        q: options.q || undefined,
        order: options.order ?? 'scale',
        page: options.page ?? 1,
        pageSize: options.pageSize ?? 50,
      },
    })
    .then(r => r.data)
}

export function fetchCompanyDetail(
  key: string,
  options: {
    type?: string
    q?: string
    order?: 'scale' | 'code' | 'inception'
    page?: number
    pageSize?: number
  } = {},
): Promise<CompanyDetailResp> {
  return http
    .get<CompanyDetailResp>(`/fund-companies/${encodeURIComponent(key)}`, {
      params: {
        type: options.type || undefined,
        q: options.q || undefined,
        order: options.order ?? 'scale',
        page: options.page ?? 1,
        pageSize: options.pageSize ?? 50,
      },
    })
    .then(r => r.data)
}
