import axios from 'axios'
import type { DramProduct, GpuProduct, StorageResponse } from '@/types/ai'

const http = axios.create({
  baseURL: '/api',
  timeout: 20_000,
})

/**
 * 存储颗粒现货（DDR5 / DDR4）—— 全部品种 + 最新价 + 走势序列。
 * 一次拿全（9 品种 × ~122 点），切品类不再发请求。
 * 源站只有滚动半年，所以不提供 days 之外的区间选择。
 */
export function fetchDram(days = 500): Promise<DramProduct[]> {
  return http.get<DramProduct[]>('/ai/dram', { params: { days } }).then(r => r.data)
}

/**
 * GPU 云租价 —— 型号 + 最新价 + 走势（market_min 序列）+ 各云厂商最新明细 + 买断参考价。
 * 一次拿全，切换型号不再发请求。
 */
export function fetchGpu(days = 365): Promise<GpuProduct[]> {
  return http.get<GpuProduct[]>('/ai/gpu', { params: { days } }).then(r => r.data)
}

/**
 * 存储成品盘 / NAND 颗粒 —— 115 个品种一次拿全（约 3500 点），切品类不再发请求。
 * 返回 `categories`（分组与更新节奏）和 `items`（含虚拟品种 `avg_ssd_street`）。
 */
export function fetchStorage(days = 500): Promise<StorageResponse> {
  return http.get<StorageResponse>('/ai/storage', { params: { days } }).then(r => r.data)
}
