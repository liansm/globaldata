// 「油运信息」——中国船东 VLCC 船队 / 船位
// 口径：船东 = 招商轮船 + 中远海能（**中国船东**，不是挂旗口径）
// ⚠ 船位 `ts` 是 AIS **报文时间**，远洋无岸基覆盖时几小时到几天都正常，
//   用 staleHours 显示「更新于 X 小时前」，别当实时。

export interface VlccPosition {
  ts: string
  lat: number
  lon: number
  sog: number | null
  cog: number | null
  heading: number | null
  navStatus: string | null
  dest: string | null
  staleHours: number | null
}

export interface VlccVessel {
  nameAis: string
  nameEn: string
  nameCn: string | null
  owner: string
  ownerFull: string | null
  dwt: number | null
  builtYear: number | null
  flag: string | null
  imo: string | null
  mmsi: string | null
  verified: boolean
  source: string
  pos: VlccPosition | null
}

export interface VlccFleetStats {
  total: number
  withPos: number
  byOwner: Record<string, { total: number; withPos: number }>
  latestTs: string | null
  coverageNote: string
}

export interface VlccFleet {
  stats: VlccFleetStats
  vessels: VlccVessel[]
}

export interface VlccTrackPoint {
  ts: string
  lat: number
  lon: number
  sog: number | null
  cog: number | null
  navStatus: string | null
  dest: string | null
}

export interface VlccVesselDetail {
  found: true
  vessel: Omit<VlccVessel, 'pos'>
  days: number
  track: VlccTrackPoint[]
}

// ⚠ 详情接口对不存在的键返 200 + {found:false}，前端**必须显式分支**，
//   否则会渲染成全 `—` 的空白页（本项目踩过）
export interface VlccVesselMissing {
  found: false
  name: string
}

export type VlccVesselResp = VlccVesselDetail | VlccVesselMissing

export interface VlccOwner {
  owner: string
  total: number
}
