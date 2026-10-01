import axios from 'axios'
import type { FxPair, FxDetail, FxRateType } from '@/types/fx'

const http = axios.create({
  baseURL: '/api',
  timeout: 10_000,
})

export function fetchFxPairs(): Promise<FxPair[]> {
  return http.get<FxPair[]>('/fx').then(r => r.data)
}

export function fetchFxDetail(
  key: string,
  options: { rateType?: FxRateType; days?: number; from?: string; to?: string } = {},
): Promise<FxDetail> {
  return http.get<FxDetail>(`/fx/${key}`, { params: options }).then(r => r.data)
}
