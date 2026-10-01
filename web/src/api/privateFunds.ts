import axios from 'axios'
import type {
  PrivateFundListResp,
  PrivateManagerListResp,
  PrivateFundStats,
  PrivateStateCount,
  PrivateTypeCount,
  PrivateNavResp,
  PrivateFundDetail,
} from '@/types/privateFund'

const http = axios.create({
  baseURL: '/api',
  timeout: 20_000,
})

export function fetchPrivateStats(): Promise<PrivateFundStats> {
  return http.get<PrivateFundStats>('/private-funds/stats').then(r => r.data)
}

export function fetchPrivateStates(): Promise<PrivateStateCount[]> {
  return http.get<PrivateStateCount[]>('/private-funds/states').then(r => r.data)
}

export function fetchPrivateFunds(options: {
  q?: string
  state?: string
  hasNav?: boolean
  order?: string
  page?: number
  pageSize?: number
} = {}): Promise<PrivateFundListResp> {
  return http
    .get<PrivateFundListResp>('/private-funds', {
      params: {
        q: options.q || undefined,
        state: options.state || undefined,
        hasNav: options.hasNav ? 1 : undefined,
        order: options.order || undefined,
        page: options.page ?? 1,
        pageSize: options.pageSize ?? 50,
      },
    })
    .then(r => r.data)
}

export function fetchPrivateManagers(options: {
  q?: string
  investType?: string
  province?: string
  page?: number
  pageSize?: number
} = {}): Promise<PrivateManagerListResp> {
  return http
    .get<PrivateManagerListResp>('/private-funds/managers', {
      params: {
        q: options.q || undefined,
        investType: options.investType || undefined,
        province: options.province || undefined,
        page: options.page ?? 1,
        pageSize: options.pageSize ?? 50,
      },
    })
    .then(r => r.data)
}

export function fetchPrivateManagerTypes(): Promise<PrivateTypeCount[]> {
  return http.get<PrivateTypeCount[]>('/private-funds/manager-types').then(r => r.data)
}

export function fetchPrivateFundDetail(code: string): Promise<PrivateFundDetail> {
  return http.get<PrivateFundDetail>(`/private-funds/${code}`).then(r => r.data)
}

/** 净值序列。days=0 全部历史；from='YYYY-MM-DD' 用于「今年来」；limit 供表格分页 */
export function fetchPrivateNav(
  code: string,
  options: { days?: number; from?: string; order?: 'asc' | 'desc'; offset?: number; limit?: number } = {},
): Promise<PrivateNavResp> {
  return http
    .get<PrivateNavResp>(`/private-funds/${code}/nav`, {
      params: {
        days: options.days ?? 365,
        from: options.from || undefined,
        order: options.order ?? 'desc',
        offset: options.offset ?? 0,
        limit: options.limit,
      },
    })
    .then(r => r.data)
}
