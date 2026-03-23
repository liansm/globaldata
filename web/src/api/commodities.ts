import axios from 'axios'
import type { Commodity, CommodityDetail, CommodityMinutes, LatestPrice } from '@/types/commodity'

const http = axios.create({
  baseURL: '/api',
  timeout: 10_000,
})

/** 获取所有商品及最新价格（含实时 spot 数据） */
export function fetchCommodities(): Promise<Commodity[]> {
  return http.get<Commodity[]>('/commodities').then(r => r.data)
}

/** 获取单个商品的元数据 + 历史价格 + spot 快照 */
export function fetchCommodityDetail(
  key: string,
  options: { days?: number; from?: string; to?: string } = {},
): Promise<CommodityDetail> {
  return http
    .get<CommodityDetail>(`/commodities/${key}`, { params: options })
    .then(r => r.data)
}

/** 获取单个商品今日分时 bars */
export function fetchCommodityMinutes(
  key: string,
  date?: string,
): Promise<CommodityMinutes> {
  return http
    .get<CommodityMinutes>(`/commodities/${key}/minutes`, { params: date ? { date } : {} })
    .then(r => r.data)
}

/** 获取单个商品最新价格（轻量接口） */
export function fetchLatestPrice(key: string): Promise<LatestPrice> {
  return http.get<LatestPrice>(`/commodities/${key}/latest`).then(r => r.data)
}
