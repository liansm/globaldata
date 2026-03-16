import axios from 'axios'
import type { MarketIndex, MarketDetail } from '@/types/market'

const http = axios.create({
  baseURL: '/api',
  timeout: 10_000,
})

export function fetchMarkets(): Promise<MarketIndex[]> {
  return http.get<MarketIndex[]>('/markets').then(r => r.data)
}

export function fetchMarketDetail(
  key: string,
  options: { days?: number; from?: string; to?: string } = {},
): Promise<MarketDetail> {
  return http.get<MarketDetail>(`/markets/${key}`, { params: options }).then(r => r.data)
}
