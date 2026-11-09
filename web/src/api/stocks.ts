import axios from 'axios'
import type {
  StockListResult, StockFilters, StockDetailResult,
  PricePeriod, PriceResult, StockReportsResult, StockHoldingsResult,
  StockFinancialsResult, StockStatementResult,
} from '@/types/stocks'

const http = axios.create({ baseURL: '/api', timeout: 20_000 })

export interface StockQuery {
  q?: string
  exchange?: string
  board?: string
  region?: string
  industry?: string
  tag?: string
  sort?: string
  page?: number
  pageSize?: number
}

export function fetchStocks(params: StockQuery = {}): Promise<StockListResult> {
  return http.get<StockListResult>('/stocks', { params }).then(r => r.data)
}

export function fetchStockFilters(): Promise<StockFilters> {
  return http.get<StockFilters>('/stocks/filters').then(r => r.data)
}

export function fetchStockDetail(code: string): Promise<StockDetailResult> {
  return http.get<StockDetailResult>(`/stocks/${encodeURIComponent(code)}`).then(r => r.data)
}

export function fetchStockPrices(
  code: string,
  params: { period?: PricePeriod; from?: string; to?: string; limit?: number } = {},
): Promise<PriceResult> {
  return http.get<PriceResult>(`/stocks/${encodeURIComponent(code)}/prices`, { params })
    .then(r => r.data)
}

export function fetchStockReports(
  code: string,
  params: { quarter?: string; limit?: number } = {},
): Promise<StockReportsResult> {
  return http.get<StockReportsResult>(`/stocks/${encodeURIComponent(code)}/reports`, { params })
    .then(r => r.data)
}

export function fetchStockHoldings(
  code: string,
  params: { reportDate?: string; page?: number; pageSize?: number } = {},
): Promise<StockHoldingsResult> {
  return http.get<StockHoldingsResult>(`/stocks/${encodeURIComponent(code)}/holdings`, { params })
    .then(r => r.data)
}

export function fetchStockFinancials(
  code: string,
  params: { limit?: number } = {},
): Promise<StockFinancialsResult> {
  return http.get<StockFinancialsResult>(`/stocks/${encodeURIComponent(code)}/financials`, { params })
    .then(r => r.data)
}

export function fetchStockStatement(
  code: string,
  params: { type?: string; filter?: string; page?: number; pageSize?: number } = {},
): Promise<StockStatementResult> {
  return http.get<StockStatementResult>(`/stocks/${encodeURIComponent(code)}/statement`, { params })
    .then(r => r.data)
}
