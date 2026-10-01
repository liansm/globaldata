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
  /** 名录口径截止日（招商=抓取日，中远=2021-06-30 官方快照）。两者不同是事实，不是 bug。 */
  rosterAsof: string | null
  /** 'active' | 'retired'。retired = 已核实转手/改名，**没有船位属正常**，不是待补缺口。 */
  rosterStatus: string
  /** retired 时：现名 / 转手时间 / 依据 */
  statusNote: string | null
  pos: VlccPosition | null
}

export interface VlccFleetStats {
  total: number
  withPos: number
  /** 名录内已核实转手的艘数（**不是**「无船位」，别混） */
  retired: number
  byOwner: Record<string, { total: number; withPos: number; retired: number }>
  /** 各船东的名录口径日；min≠max 说明同船东内口径不一致（应报警） */
  asofByOwner: Record<string, { min: string; max: string }>
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
