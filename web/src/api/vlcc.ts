import axios from 'axios'
import type { VlccFleet, VlccVesselResp, VlccOwner } from '@/types/vlcc'

const http = axios.create({
  baseURL: '/api',
  timeout: 15_000,
})

export function fetchVlccFleet(
  options: { owner?: string; onlyWithPos?: boolean } = {},
): Promise<VlccFleet> {
  return http
    .get<VlccFleet>('/vlcc/fleet', {
      params: {
        owner: options.owner && options.owner !== '全部' ? options.owner : undefined,
        onlyWithPos: options.onlyWithPos ? '1' : undefined,
      },
    })
    .then(r => r.data)
}

export function fetchVlccOwners(): Promise<VlccOwner[]> {
  return http.get<VlccOwner[]>('/vlcc/owners').then(r => r.data)
}

export function fetchVlccVessel(nameAis: string, days = 30): Promise<VlccVesselResp> {
  return http
    .get<VlccVesselResp>(`/vlcc/vessel/${encodeURIComponent(nameAis)}`, { params: { days } })
    .then(r => r.data)
}
