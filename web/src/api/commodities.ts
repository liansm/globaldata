import axios from 'axios'
import type { Commodity, CommodityDetail, LatestPrice } from '@/types/commodity'

const http = axios.create({
  baseURL: '/api',
  timeout: 10_000,
})

/** 获取所有商品及最新价格 */
export function fetchCommodities(): Promise<Commodity[]> {
  return http.get<Commodity[]>('/commodities').then(r => r.data)
}

/** 获取单个商品的元数据 + 历史价格 */
export function fetchCommodityDetail(
  key: string,
  options: { days?: number; from?: string; to?: string } = {},
): Promise<CommodityDetail> {
  return http
    .get<CommodityDetail>(`/commodities/${key}`, { params: options })
    .then(r => r.data)
}

/** 获取单个商品最新价格（轻量接口） */
export function fetchLatestPrice(key: string): Promise<LatestPrice> {
  return http.get<LatestPrice>(`/commodities/${key}/latest`).then(r => r.data)
}
