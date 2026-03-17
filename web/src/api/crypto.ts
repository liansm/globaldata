import axios from 'axios'
import type { CryptoCoin, CryptoDetail } from '@/types/crypto'

const http = axios.create({ baseURL: '/api', timeout: 10_000 })

export function fetchCrypto(): Promise<CryptoCoin[]> {
  return http.get<CryptoCoin[]>('/crypto').then(r => r.data)
}

export function fetchCryptoDetail(
  key: string,
  opts: { days?: number; from?: string; to?: string } = {}
): Promise<CryptoDetail> {
  return http.get<CryptoDetail>(`/crypto/${key}`, { params: opts }).then(r => r.data)
}
