export interface CryptoCoin {
  key: string
  symbol: string
  name: string
  unit: string
  updatedAt: string
  latestDate: string | null
  latestClose: number | null
  changePct: number | null
  volume24h: number | null
  high24h: number | null
  low24h: number | null
}

export interface CryptoPoint {
  date: string
  close: number | null
  changePct: number | null
  volume24h: number | null
  high24h: number | null
  low24h: number | null
}

export interface CryptoDetail {
  key: string
  symbol: string
  name: string
  unit: string
  updatedAt: string
  history: CryptoPoint[]
}
