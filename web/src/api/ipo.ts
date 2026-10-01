import axios from 'axios'
import type { IpoItem, IpoStats } from '@/types/ipo'

const http = axios.create({
  baseURL: '/api',
  timeout: 15_000,
})

export function fetchIpoCalendar(
  params: { market?: string; from?: string; to?: string; limit?: number } = {},
): Promise<IpoItem[]> {
  return http.get<IpoItem[]>('/ipo-calendar', { params }).then(r => r.data)
}

export function fetchIpoStats(
  params: { from?: string; to?: string } = {},
): Promise<IpoStats[]> {
  return http.get<IpoStats[]>('/ipo-calendar/stats', { params }).then(r => r.data)
}
