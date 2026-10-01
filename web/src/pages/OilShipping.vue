<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount, nextTick, watch } from 'vue'
import { fetchVlccFleet, fetchVlccOwners, fetchVlccVessel } from '@/api/vlcc'
import type { VlccVessel, VlccFleetStats, VlccOwner, VlccVesselDetail } from '@/types/vlcc'

// ── 地图：按合规要求**只能**用白名单厂商（腾讯 / 高德 / 百度 / 天地图），
//    默认走腾讯地图 GL JS。**不用 OpenStreetMap / Google / Mapbox**。
//
// key 从环境变量读，**绝不写进代码**：web/.env 里加 VITE_TMAP_KEY=你的key
// ⚠ 必须是 web/.env（vite 的项目根），放仓库根 .env 前端读不到
// 申请：https://lbs.qq.com/ → 控制台 → 应用管理 → 添加 Key（Web端 JavaScript API GL）
//
// 实测（2026-09-30）：腾讯 GL JS 在 **localhost 不带 key** 也能出图，
// 只是地图上会浮一条「当前产品鉴权失败，请检查KEY」的提示条。
// 所以这里**不做无 key 降级**，一律初始化地图；key 只决定是否出现那条提示。
// 上线/部署到非 localhost 域时必须配 key，否则会一直挂着鉴权提示。
const TMAP_KEY = ((import.meta as any).env?.VITE_TMAP_KEY as string | undefined)?.trim() || ''
const hasKey = computed(() => !!TMAP_KEY)

const OWNER_COLOR: Record<string, string> = {
  招商轮船: '#2f6fed',
  中远海能: '#f2913d',
}
const FALLBACK_COLOR = '#8a94a6'

const ownerColor = (o: string) => OWNER_COLOR[o] ?? FALLBACK_COLOR

const FLAG_LABEL: Record<string, string> = {
  CN: '中国', HK: '中国香港', SG: '新加坡', PA: '巴拿马',
  LR: '利比里亚', MH: '马绍尔群岛', MT: '马耳他',
}

// ── 自绘地理标注 ─────────────────────────────────────────────────────────────
// 为什么自己画：腾讯底图**境外没有地名注记** ——「海外图」是它单独的付费产品
// （「海外位置服务」，官方 FAQ：不支持个人申请、需公司主体、是付费服务），
// 默认的 JavaScript API GL key 不含这项能力。实测：z6 法国/德国上空**零注记**，
// z3 全球只有「中华人民共和国」一个国名；境外瓦片字节数只有境内的 50%
// （是 200 有数据的，只是内容就是光秃秃的陆地 + 海洋轮廓 + 大洲/大洋名）。
// 详见 .workbuddy/memory/DATASOURCES.md「🌍 底图境外不显示地名」。
//
// 对油运图来说，真正要回答的是「这条 VLCC 现在堵在哪个咽喉」——
// 标「法国」「德国」帮不上忙。所以这里画两层：
//   choke 咽喉点：海峡 / 运河（霍尔木兹、马六甲、苏伊士、好望角…）
//   sea   海域名：**境外**主要海域（境内东海/黄海/南海腾讯自己的注记里有，不重复画）
//
// ⚠ 坐标一律按**真实经纬度**填，不做 GCJ-02 偏移转换。三条理由：
//   ① 咽喉点绝大多数在境外，而腾讯境外图本身就是 WGS84；
//   ② 境内那几个（台湾/琼州/渤海海峡、长江口、珠江口）GCJ-02 偏移约 500m，
//      在海峡尺度（几十~几百公里）下连 1px 都不到；
//   ③ 最要紧：本仓库船位也是**原样 WGS84** 打上去的，地名跟着同一套坐标走才与
//      船位自洽 —— 只给地名做转换，反而会让地名和船位互相错开。
//      （船位本身的坐标系隐患是另一件事，见 DATASOURCES.md，与本图层无关。）
type GeoKind = 'choke' | 'sea'
interface GeoLabel { id: string; name: string; lat: number; lon: number; kind: GeoKind; minZoom: number }

// minZoom = 低于该缩放级别不显示，做语义分层：全球视图只留干线咽喉，
// 放大后才补出区域航线与国内口门 —— 否则 z3 上几十个标签会糊成一片。
const GEO_LABELS: GeoLabel[] = [
  // ── 一级咽喉：全球油运干线 ────────────────────────────────────────────
  { id: 'hormuz',       name: '霍尔木兹海峡',   lat: 26.57,  lon: 56.25,   kind: 'choke', minZoom: 3 },
  { id: 'malacca',      name: '马六甲海峡',     lat: 2.20,   lon: 102.20,  kind: 'choke', minZoom: 3 },
  { id: 'suez',         name: '苏伊士运河',     lat: 30.60,  lon: 32.30,   kind: 'choke', minZoom: 3 },
  { id: 'bab_mandeb',   name: '曼德海峡',       lat: 12.58,  lon: 43.40,   kind: 'choke', minZoom: 3 },
  { id: 'gibraltar',    name: '直布罗陀海峡',   lat: 35.95,  lon: -5.60,   kind: 'choke', minZoom: 3 },
  { id: 'good_hope',    name: '好望角',         lat: -34.36, lon: 18.47,   kind: 'choke', minZoom: 3 },
  { id: 'panama',       name: '巴拿马运河',     lat: 9.08,   lon: -79.68,  kind: 'choke', minZoom: 3 },
  { id: 'bosphorus',    name: '博斯普鲁斯海峡', lat: 41.12,  lon: 29.06,   kind: 'choke', minZoom: 3 },
  // ── 二级咽喉：替代航线与区域口门 ──────────────────────────────────────
  { id: 'dardanelles',  name: '达达尼尔海峡',   lat: 40.05,  lon: 26.20,   kind: 'choke', minZoom: 5 },
  { id: 'singapore',    name: '新加坡海峡',     lat: 1.22,   lon: 103.85,  kind: 'choke', minZoom: 5 },
  { id: 'sunda',        name: '巽他海峡',       lat: -5.90,  lon: 105.50,  kind: 'choke', minZoom: 5 },
  { id: 'lombok',       name: '龙目海峡',       lat: -8.50,  lon: 115.80,  kind: 'choke', minZoom: 5 },
  { id: 'mozambique',   name: '莫桑比克海峡',   lat: -18.00, lon: 41.00,   kind: 'choke', minZoom: 5 },
  { id: 'dover',        name: '多佛海峡',       lat: 51.02,  lon: 1.50,    kind: 'choke', minZoom: 5 },
  { id: 'danish',       name: '丹麦海峡',       lat: 57.00,  lon: 10.50,   kind: 'choke', minZoom: 5 },
  { id: 'korea',        name: '朝鲜海峡',       lat: 34.50,  lon: 129.00,  kind: 'choke', minZoom: 5 },
  { id: 'tsugaru',      name: '津轻海峡',       lat: 41.50,  lon: 140.70,  kind: 'choke', minZoom: 5 },
  { id: 'bering',       name: '白令海峡',       lat: 65.90,  lon: -169.00, kind: 'choke', minZoom: 5 },
  { id: 'magellan',     name: '麦哲伦海峡',     lat: -53.50, lon: -70.50,  kind: 'choke', minZoom: 5 },
  { id: 'taiwan_str',   name: '台湾海峡',       lat: 24.30,  lon: 119.30,  kind: 'choke', minZoom: 5 },
  { id: 'qiongzhou',    name: '琼州海峡',       lat: 20.15,  lon: 110.30,  kind: 'choke', minZoom: 6 },
  { id: 'bohai',        name: '渤海海峡',       lat: 38.55,  lon: 121.10,  kind: 'choke', minZoom: 6 },
  { id: 'yangtze',      name: '长江口',         lat: 31.20,  lon: 122.20,  kind: 'choke', minZoom: 6 },
  { id: 'pearl_river',  name: '珠江口',         lat: 21.90,  lon: 113.90,  kind: 'choke', minZoom: 6 },
  // ── 境外海域名 ────────────────────────────────────────────────────────
  { id: 'persian_gulf',   name: '波斯湾',       lat: 26.50,  lon: 51.50,   kind: 'sea', minZoom: 3 },
  { id: 'red_sea',        name: '红海',         lat: 20.00,  lon: 38.00,   kind: 'sea', minZoom: 3 },
  { id: 'gulf_aden',      name: '亚丁湾',       lat: 12.50,  lon: 48.00,   kind: 'sea', minZoom: 4 },
  { id: 'arabian_sea',    name: '阿拉伯海',     lat: 15.00,  lon: 63.00,   kind: 'sea', minZoom: 3 },
  { id: 'bengal',         name: '孟加拉湾',     lat: 15.00,  lon: 88.00,   kind: 'sea', minZoom: 3 },
  { id: 'mediterranean',  name: '地中海',       lat: 35.00,  lon: 18.00,   kind: 'sea', minZoom: 3 },
  { id: 'black_sea',      name: '黑海',         lat: 43.00,  lon: 34.00,   kind: 'sea', minZoom: 4 },
  { id: 'north_sea',      name: '北海',         lat: 56.00,  lon: 3.00,    kind: 'sea', minZoom: 4 },
  { id: 'baltic',         name: '波罗的海',     lat: 58.00,  lon: 20.00,   kind: 'sea', minZoom: 4 },
  { id: 'japan_sea',      name: '日本海',       lat: 39.00,  lon: 135.00,  kind: 'sea', minZoom: 3 },
  { id: 'philippine_sea', name: '菲律宾海',     lat: 20.00,  lon: 130.00,  kind: 'sea', minZoom: 4 },
  { id: 'coral_sea',      name: '珊瑚海',       lat: -18.00, lon: 155.00,  kind: 'sea', minZoom: 3 },
  { id: 'gulf_mexico',    name: '墨西哥湾',     lat: 25.00,  lon: -90.00,  kind: 'sea', minZoom: 3 },
  { id: 'caribbean',      name: '加勒比海',     lat: 15.00,  lon: -75.00,  kind: 'sea', minZoom: 3 },
  { id: 'great_bight',    name: '大澳大利亚湾', lat: -35.00, lon: 132.00,  kind: 'sea', minZoom: 3 },
  { id: 'south_ocean',    name: '南大洋',       lat: -58.00, lon: 70.00,   kind: 'sea', minZoom: 3 },
]

// ── state ────────────────────────────────────────────────────────────────────
const loading   = ref(false)
const error     = ref('')
const stats     = ref<VlccFleetStats | null>(null)
const vessels   = ref<VlccVessel[]>([])
const owners    = ref<VlccOwner[]>([])
const ownerSel  = ref('全部')
const keyword   = ref('')
const onlyPos   = ref(false)
const selected  = ref<string | null>(null)
const detail    = ref<VlccVesselDetail | null>(null)
const detailLoading = ref(false)

const mapEl     = ref<HTMLElement | null>(null)
const mapReady  = ref(false)
const mapError  = ref('')
const showGeo   = ref(true)      // 自绘咽喉/海域标注开关（见 GEO_LABELS 注释）
let map: any = null
let markerLayer: any = null
let labelLayer: any = null
let trackLayer: any = null
let infoWindow: any = null
let TMapRef: any = null
let lastGeoKey = ''   // 上一次实际渲染的标注 id 集合，用于跳过无谓重绘

const withPos    = computed(() => vessels.value.filter(v => v.pos))
const shownList  = computed(() => {
  const kw = keyword.value.trim().toUpperCase()
  return vessels.value.filter(v => {
    if (onlyPos.value && !v.pos) return false
    if (!kw) return true
    return (v.nameAis.includes(kw) || (v.nameCn ?? '').includes(keyword.value.trim()))
  })
})

// ── 地图 ─────────────────────────────────────────────────────────────────────
function markerSvg(color: string) {
  const svg =
    `<svg xmlns="http://www.w3.org/2000/svg" width="26" height="34" viewBox="0 0 26 34">` +
    `<path d="M13 0C5.8 0 0 5.8 0 13c0 9.8 13 21 13 21s13-11.2 13-21c0-7.2-5.8-13-13-13z" ` +
    `fill="${color}" stroke="#ffffff" stroke-width="1.5"/>` +
    `<circle cx="13" cy="13" r="4.6" fill="#ffffff"/></svg>`
  return 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg)
}

function loadTMapSdk(key: string): Promise<any> {
  const w = window as any
  if (w.TMap) return Promise.resolve(w.TMap)
  return new Promise((resolve, reject) => {
    const s = document.createElement('script')
    // 不带 key 也加载（localhost 能出图）；带了就带 ?key=
    s.src = 'https://map.qq.com/api/gljs?v=1.exp' + (key ? `&key=${encodeURIComponent(key)}` : '')
    s.async = true
    s.onload = () => (w.TMap ? resolve(w.TMap) : reject(new Error('腾讯地图 SDK 已加载但未挂载 TMap')))
    s.onerror = () => reject(new Error('腾讯地图 SDK 加载失败（网络受限？）'))
    document.head.appendChild(s)
  })
}

// 「全部装下」。
// 早先手算 center+zoom，右边缘（中国/日本）被切掉；改用 SDK 自带 fitBounds，
// 已对着真 SDK 验过签名：map.fitBounds(LatLngBounds, { padding }) 可用。
// 无船位时退回「西太—印度洋」默认视野（不是全球居中，因为船就在这一带）。
// ── 默认视野 ────────────────────────────────────────────────────────────────
// ⚠ 一屏装下**全部**船位在腾讯 GL 上做不到：SDK 把 zoom 下限硬编码成 3
//    实测 `setMinZoom()` 无效，`setZoom(2 / 1 / 0)` 一律被钳回 3；
//    而 zoom 3 时本页地图容器只能覆盖约 152 度经度。中国船东的 VLCC 跑的是全球
//    航线，船位经度跨度实测 248 度 → 必然有一批落在视野外，这是 API 层的物理限制。
// 所以默认视野**不取经纬极值的中点** —— 那会把视野放在船最少的大洋中央，
// 实测只能看见 49/95 艘；改为取**覆盖船数最多的那段经度窗口**，实测 81/95 艘。
// 没进视野的船随时可点右侧列表飞过去（selectVessel 会 setCenter）。
function lonSpanAtFloorZoom() {
  const w = mapEl.value?.clientWidth || 800
  // zoom 3 = 全球 256*2^3 像素宽。0.96 是边距余量，不是随手取的：
  // fitBounds 的 zoom 一被钳到下限 3，视野就固定成「容器全宽对应的经度」，
  // 窗口取太满会让两端的船点正好贴在容器边缘上（padding 此时已不起作用）。
  return (w / (256 * 8)) * 360 * 0.96
}

// 选「装船最多」的经度窗口。窗口左沿只需试**正好落在某艘船上**的位置 ——
// 覆盖数最大的窗口总可以平移到某个点处（经典区间覆盖结论），不必按步长穷举。
// ⚠ 不处理跨日界线的窗口（本项目船位在 −106~143 度，不跨）；将来若跨了要另写。
function pickDensestWindow<T extends { lon: number }>(pts: T[], span: number): T[] {
  const lons = pts.map(p => p.lon).sort((a, b) => a - b)
  let bestLo = lons[0], bestN = -1
  for (const s of lons) {
    let n = 0
    for (const x of lons) if (x >= s && x <= s + span) n++
    if (n > bestN) { bestN = n; bestLo = s }
  }
  return pts.filter(p => p.lon >= bestLo && p.lon <= bestLo + span)
}

function fitAll() {
  if (!map || !TMapRef) return
  const uniq = new Map<string, { lat: number; lon: number }>()
  for (const v of withPos.value) {
    const p = v.pos
    if (!p) continue
    // 同名船多点时只取一个，避免视野被单船的航迹撑开
    if (!uniq.has(v.nameAis)) uniq.set(v.nameAis, { lat: p.lat, lon: p.lon })
  }
  const pts = [...uniq.values()]
  if (!pts.length) {
    map.setCenter(new TMapRef.LatLng(18, 88))
    map.setZoom(3)
    return
  }
  if (pts.length === 1) {
    map.setCenter(new TMapRef.LatLng(pts[0].lat, pts[0].lon))
    map.setZoom(7)
    return
  }

  const los = pts.map(p => p.lon)
  const span = lonSpanAtFloorZoom()
  // 经度装得下就全部上；装不下才丢卒保车，只留船最多的那一段
  const use = (Math.max(...los) - Math.min(...los) <= span)
    ? pts
    : pickDensestWindow(pts, span)

  // 交给 SDK 定 zoom（窗口内船密集时会自动放大，比写死 3 好）
  map.fitBounds(
    new TMapRef.LatLngBounds(
      new TMapRef.LatLng(Math.min(...use.map(p => p.lat)), Math.min(...use.map(p => p.lon))),
      new TMapRef.LatLng(Math.max(...use.map(p => p.lat)), Math.max(...use.map(p => p.lon))),
    ),
    { padding: 60 },
  )
}

function renderMarkers() {
  if (!map || !TMapRef || !markerLayer) return
  const geos = withPos.value
    .filter(v => v.pos)
    .map(v => ({
      id: v.nameAis,
      styleId: v.owner === '招商轮船' ? 'cmes' : v.owner === '中远海能' ? 'cosco' : 'other',
      position: new TMapRef.LatLng(v.pos!.lat, v.pos!.lon),
      properties: { nameAis: v.nameAis },
    }))
  markerLayer.setGeometries(geos)
}

function infoHtml(v: VlccVessel) {
  const p = v.pos!
  const stale = p.staleHours == null ? '—'
    : p.staleHours < 1 ? '刚刚'
    : p.staleHours < 24 ? `${p.staleHours.toFixed(1)} 小时前`
    : `${(p.staleHours / 24).toFixed(1)} 天前`
  const row = (k: string, val: string) =>
    `<div style="display:flex;gap:10px;line-height:1.7"><span style="color:#8a94a6;min-width:56px">${k}</span><span style="color:#1a1a2e">${val}</span></div>`
  return `<div style="font-size:12.5px;font-family:-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;min-width:212px">
    <div style="font-weight:700;font-size:14px;margin-bottom:2px;color:#1a1a2e">
      ${v.nameCn ? v.nameCn + ' ' : ''}${v.nameEn}
    </div>
    <div style="color:${ownerColor(v.owner)};font-size:12px;margin-bottom:6px">
      ${v.owner}${v.dwt ? ' · ' + (v.dwt / 10000).toFixed(1) + ' 万吨' : ''}
    </div>
    ${row('船位', `${p.lat.toFixed(3)}°, ${p.lon.toFixed(3)}°`)}
    ${row('航速', p.sog == null ? '—' : p.sog.toFixed(1) + ' 节')}
    ${row('状态', p.navStatus ?? '—')}
    ${row('目的港', p.dest || '—')}
    ${row('更新', stale)}
    <div style="margin-top:6px;padding-top:6px;border-top:1px solid #f0f0f0;color:#a0a6b5;font-size:11px">
      AIS 报文时间；远洋无岸基覆盖时可能较旧，属正常
    </div>
  </div>`
}

function focusVessel(nameAis: string, openPopup = true) {
  selected.value = nameAis
  const v = vessels.value.find(x => x.nameAis === nameAis)
  if (!v?.pos || !map || !TMapRef) return
  map.setCenter(new TMapRef.LatLng(v.pos.lat, v.pos.lon))
  if (map.getZoom() < 5) map.setZoom(6)
  if (openPopup && infoWindow) {
    infoWindow.setPosition(new TMapRef.LatLng(v.pos.lat, v.pos.lon))
    infoWindow.setContent(infoHtml(v))
    infoWindow.open()
  }
}

async function selectVessel(nameAis: string) {
  focusVessel(nameAis)
  detailLoading.value = true
  detail.value = null
  try {
    const r = await fetchVlccVessel(nameAis, 30)
    detail.value = r.found ? r : null
    if (r.found) drawTrack(r)
  } catch {
    detail.value = null
  } finally {
    detailLoading.value = false
  }
}

function drawTrack(d: VlccVesselDetail) {
  if (!map || !TMapRef || !trackLayer) return
  if (d.track.length < 2) { trackLayer.setGeometries([]); return }
  trackLayer.setGeometries([{
    id: d.vessel.nameAis,
    styleId: 'track',
    paths: d.track.map(p => new TMapRef.LatLng(p.lat, p.lon)),
  }])
}

// 按当前缩放级别挑出该显示的标注。拖动/滚轮缩放时 zoom_changed 会连续触发，
// 故用「可见 id 集合」做指纹，集合没变就整个跳过（不重建 LatLng、不重设图层）。
function renderGeoLabels() {
  if (!map || !TMapRef || !labelLayer) return
  const on = showGeo.value
    ? GEO_LABELS.filter(g => map.getZoom() >= g.minZoom)
    : []
  const key = on.map(g => g.id).join(',')
  if (key === lastGeoKey) return
  lastGeoKey = key
  labelLayer.setGeometries(on.map(g => ({
    id: g.id,
    styleId: g.kind,
    content: g.name,
    position: new TMapRef.LatLng(g.lat, g.lon),
    // rank 只在开启同层碰撞后生效：咽喉点(20) 压过海域名(5)，
    // 两者挤在一起时海域名主动让位，不会字叠字
    rank: g.kind === 'choke' ? 20 : 5,
  })))
}

async function initMap() {
  if (!mapEl.value) return
  try {
    TMapRef = await loadTMapSdk(TMAP_KEY)
    // ⚠ 境外没有地名注记是**腾讯底图本身如此**，不是这里的配置问题，别去调 baseMap/features。
    // 腾讯把境外地图做成了独立付费产品「海外位置服务」，默认的 JS API GL key 不含这项能力：
    // 境内走完整矢量数据（城市/省界/海域名齐全），境外只落到「陆地+海洋轮廓 + 大洲/大洋名」。
    // 实测：z6 法国上空零注记；z3 全图只有「中华人民共和国」一个国名；境外瓦片字节数
    // 只有境内的 50%，但确实 200 有数据。getOverseaEnabled()=true / getOverseaMapStyle()='auto'
    // 都不代表 key 有权限。官方 FAQ：海外服务不支持个人申请、需公司主体、是付费服务。
    // 详见 .workbuddy/memory/DATASOURCES.md「🌍 底图境外不显示地名」。
    map = new TMapRef.Map(mapEl.value, {
      zoom: 3,
      center: new TMapRef.LatLng(18, 88),
    })
    markerLayer = new TMapRef.MultiMarker({
      map,
      styles: {
        cmes:  new TMapRef.MarkerStyle({ width: 26, height: 34, anchor: { x: 13, y: 34 }, src: markerSvg(OWNER_COLOR['招商轮船']) }),
        cosco: new TMapRef.MarkerStyle({ width: 26, height: 34, anchor: { x: 13, y: 34 }, src: markerSvg(OWNER_COLOR['中远海能']) }),
        other: new TMapRef.MarkerStyle({ width: 26, height: 34, anchor: { x: 13, y: 34 }, src: markerSvg(FALLBACK_COLOR) }),
      },
      geometries: [],
    })
    trackLayer = new TMapRef.MultiPolyline({
      map,
      styles: { track: new TMapRef.PolylineStyle({ color: '#2f6fed', width: 3, borderWidth: 1, borderColor: '#ffffff', lineCap: 'round' }) },
      geometries: [],
    })
    // 地理标注层**最后建** —— 腾讯 GL 的覆盖物按创建顺序叠放，晚建的在上。
    // 实测两种次序都跑过：建在船位层之前时，船密集处（霍尔木兹/马六甲/中国沿海）
    // 地名被 marker 压得只剩半个字；建在之后才读得全。
    // 反过来的代价（地名压住某艘船的 marker 一角）可接受：标签是半透明的，
    // 而且 disableInteractive 让鼠标事件穿透下去，那艘船照样点得到。
    labelLayer = new TMapRef.MultiLabel({
      map,
      styles: {
        // 咽喉点：深色气泡 + 白字，压得住底图的浅色海洋/陆地块
        // ⚠ verticalAlignment 必须是 top：船位 marker 的锚点在底部尖角、本体向**上**
        //   伸 34px，而咽喉点恰恰是船最密的地方 —— 文字若居中就会和 marker 正面撞上。
        //   整体落到位置点下方就和 marker 错开了。
        choke: new TMapRef.LabelStyle({
          color: '#ffffff', size: 12,
          backgroundColor: 'rgba(31,42,64,0.86)',
          padding: '4px 8px', borderRadius: 4,
          alignment: 'center', verticalAlignment: 'top', offset: { x: 0, y: 5 },
        }),
        // 海域名：不加底色（叫「海」的东西本来就该轻），改用白色描边
        // 让灰字在深海底色上也读得出来
        sea: new TMapRef.LabelStyle({
          color: '#6b7f9e', size: 12,
          strokeColor: 'rgba(255,255,255,0.92)', strokeWidth: 3,
          alignment: 'center', verticalAlignment: 'middle',
        }),
      },
      geometries: [],
      // sameSource: 层内碰撞（靠 rank 决定谁让位）；vectorBaseMapSource 保持关，
      // 不跟腾讯底图自己的注记抢位置，否则境内注记会被我们挤掉
      collisionOptions: { sameSource: true, vectorBaseMapSource: false },
      // 必须禁用交互：否则标签会吞掉落在地图上的鼠标事件，拖着拖着就卡住
      disableInteractive: true,
    })
    infoWindow = new TMapRef.InfoWindow({ map, position: new TMapRef.LatLng(0, 0), content: '', offset: { x: 0, y: -36 } })
    infoWindow.close()

    markerLayer.on('click', (evt: any) => {
      const name = evt?.geometry?.properties?.nameAis
      if (name) selectVessel(name)
    })
    map.on('click', () => infoWindow?.close())
    // 缩放事件名是**实测**确认的（腾讯官方文档只给事件参数规范、不给名字清单）：
    // 在真 SDK 上把 zoom_changed / zoomend / zooming / idle / bounds_changed /
    // center_changed / tiles_loaded / loading / render 全注册一遍再 setZoom，
    // 实际触发的只有 zoom_changed、zoomend、bounds_changed、idle。
    // 用 zoom_changed：缩放过程中即时响应，且拖动地图时不会像 bounds_changed 那样被
    // 别的操作带出来。
    map.on('zoom_changed', renderGeoLabels)

    mapReady.value = true
    renderMarkers()
    fitAll()
    renderGeoLabels()   // 初次进入：fitAll 改过 zoom，那一刻事件可能早于图层就绪
  } catch (e: any) {
    mapError.value = e?.message || '地图初始化失败'
  }
}

// ── 数据 ─────────────────────────────────────────────────────────────────────
async function load() {
  loading.value = true
  error.value = ''
  try {
    const [fleet, os] = await Promise.all([fetchVlccFleet(), fetchVlccOwners()])
    stats.value = fleet.stats
    vessels.value = fleet.vessels
    owners.value = os
    if (mapReady.value) { renderMarkers(); fitAll() }
  } catch {
    error.value = '加载失败，请检查后端服务（localhost:3000）是否启动'
  } finally {
    loading.value = false
  }
}

watch(ownerSel, async () => {
  loading.value = true
  try {
    const fleet = await fetchVlccFleet({ owner: ownerSel.value })
    vessels.value = fleet.vessels
    stats.value = fleet.stats
    selected.value = null
    detail.value = null
    trackLayer?.setGeometries([])
    infoWindow?.close()
    if (mapReady.value) { renderMarkers(); fitAll() }
  } catch { error.value = '加载失败' } finally { loading.value = false }
})

// 开关只改「显示哪些」，切换时把指纹清掉强制重绘一次
watch(showGeo, () => { lastGeoKey = '\u0000'; renderGeoLabels() })

onMounted(async () => {
  await nextTick()
  await initMap()
  await load()
})

onBeforeUnmount(() => {
  try { map?.destroy?.() } catch { /* noop */ }
  map = markerLayer = labelLayer = trackLayer = infoWindow = TMapRef = null
  lastGeoKey = ''
})

function fmtStale(h: number | null | undefined) {
  if (h == null) return '—'
  if (h < 1) return '刚刚'
  if (h < 24) return h.toFixed(1) + 'h 前'
  return (h / 24).toFixed(1) + 'd 前'
}
</script>

<template>
  <div class="oil-page">
    <header class="page-head">
      <div class="title-block">
        <h1>油运信息</h1>
        <p class="sub">中国船东 VLCC 船队 · 最新 AIS 船位</p>
      </div>
      <div class="stats">
        <div class="stat"><b>{{ stats?.total ?? '—' }}</b><span>艘 VLCC</span></div>
        <div class="stat"><b>{{ stats?.withPos ?? '—' }}</b><span>艘有船位</span></div>
        <div class="stat" v-for="(v, k) in (stats?.byOwner ?? {})" :key="k">
          <b :style="{ color: ownerColor(k) }">{{ v.withPos }}/{{ v.total }}</b>
          <span>{{ k }}</span>
        </div>
        <div class="stat">
          <b>{{ stats?.latestTs ? new Date(stats.latestTs).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' }) : '—' }}</b>
          <span>最新报文</span>
        </div>
      </div>
    </header>

    <div class="guide" v-if="mapError">
      <div class="guide-title">⚠ 地图初始化失败：{{ mapError }}</div>
      <div class="guide-body">
        按合规要求，本项目地图只使用白名单厂商（腾讯 / 高德 / 百度 / 天地图），<b>不使用 OpenStreetMap、Google、Mapbox</b>。
        可到 <a href="https://lbs.qq.com/" target="_blank" rel="noreferrer">腾讯位置服务</a> 申请 Key（Web端 JavaScript API GL），
        写入 <code>web/.env</code> 的 <code>VITE_TMAP_KEY</code> 后重启 dev server。
        <b>注意是 web/.env，不是仓库根目录的 .env</b>（vite 只读前端项目根下的 .env）。
        下方<b>名录视图照常可用</b>。
      </div>
    </div>

    <div class="guide soft" v-else-if="!hasKey">
      <div class="guide-title">ℹ 未配置腾讯地图 Key（不影响使用）</div>
      <div class="guide-body">
        实测腾讯 GL JS 在 <code>localhost</code> 不带 Key 也能出图，只是地图上会浮一条「鉴权失败」提示条。
        要消掉它：到 <a href="https://lbs.qq.com/" target="_blank" rel="noreferrer">腾讯位置服务</a> 申请 Key，
        写进 <code>web/.env</code> 的 <code>VITE_TMAP_KEY</code>，重启 <code>npm run dev</code>。
        <b>部署到非 localhost 域名时必须配 Key。</b>
      </div>
    </div>

    <div class="body">
      <div class="map-wrap">
        <div ref="mapEl" class="map" :class="{ hidden: !!mapError }"></div>
        <div class="map-empty" v-if="mapError">
          <div class="map-empty-icon">🗺️</div>
          <div>地图加载失败（见上方说明）</div>
        </div>
        <div class="map-empty soft" v-else-if="mapReady && !withPos.length">
          <div class="map-empty-icon">🛰️</div>
          <div>名录 {{ stats?.total ?? 0 }} 艘，但库里还没有船位</div>
          <div class="hint">
            船位来自 <b>HiFleet（船队在线）</b>，需先取 Key：<br>
            <a href="https://skills.hifleet.com/openclaw/console.html#/plans" target="_blank" rel="noreferrer">打开控制台</a>
            　→　写进 <code>.env</code> 的 <code>HIFLEET_API_KEY</code>，再跑<br>
            <code>python fetch_vlcc_position_hifleet.py</code>
          </div>
        </div>
        <div class="legend" v-if="mapReady && withPos.length">
          <span v-for="(c, o) in OWNER_COLOR" :key="o">
            <i :style="{ background: c }"></i>{{ o }}
          </span>
        </div>
      </div>

      <aside class="side">
        <div class="filters">
          <el-select v-model="ownerSel" size="small" style="width:100%">
            <el-option label="全部船东" value="全部" />
            <el-option v-for="o in owners" :key="o.owner" :label="`${o.owner} (${o.total})`" :value="o.owner" />
          </el-select>
          <el-input v-model="keyword" size="small" placeholder="搜索船名（中/英）" clearable />
          <div class="filter-row">
            <el-checkbox v-model="onlyPos" size="small">只看有船位的</el-checkbox>
            <el-checkbox
              v-model="showGeo" size="small"
              title="腾讯底图境外不提供地名注记（海外图是其单独的付费产品），此图层为自绘的咽喉 / 海域标注"
            >咽喉 / 海域标注</el-checkbox>
          </div>
        </div>

        <div class="list-head">
          名录 <b>{{ shownList.length }}</b> 艘
          <span class="list-sub">（有船位 {{ shownList.filter(v => v.pos).length }}）</span>
        </div>

        <div class="list" v-loading="loading">
          <div v-if="error" class="err">{{ error }}</div>
          <div
            v-for="v in shownList"
            :key="v.nameAis"
            class="item"
            :class="{ active: selected === v.nameAis, nopos: !v.pos }"
            @click="selectVessel(v.nameAis)"
          >
            <div class="item-main">
              <div class="item-name">
                <span class="dot" :style="{ background: v.pos ? ownerColor(v.owner) : '#c9cfda' }"></span>
                <span v-if="v.nameCn" class="cn">{{ v.nameCn }}</span>
                <span class="en">{{ v.nameEn }}</span>
              </div>
              <div class="item-meta">
                {{ v.owner }}
                <template v-if="v.dwt"> · {{ (v.dwt / 10000).toFixed(1) }}万吨</template>
                <template v-if="v.builtYear"> · {{ v.builtYear }}</template>
              </div>
            </div>
            <div class="item-right">
              <template v-if="v.pos">
                <span class="stale">{{ fmtStale(v.pos.staleHours) }}</span>
                <span class="dest" v-if="v.pos.dest">{{ v.pos.dest }}</span>
              </template>
              <span v-else class="nopos-tag">无船位</span>
            </div>
          </div>
          <div v-if="!loading && !shownList.length" class="empty">没有匹配的船</div>
        </div>

        <div class="detail" v-if="selected">
          <div class="detail-head">
            <span>单船轨迹（近 30 天）</span>
            <span class="close" @click="selected = null; detail = null; trackLayer?.setGeometries([])">✕</span>
          </div>
          <div v-loading="detailLoading" class="detail-body">
            <template v-if="detail">
              <div v-if="detail.track.length >= 2" class="ok">
                轨迹 {{ detail.track.length }} 点，
                {{ new Date(detail.track[0].ts).toLocaleDateString('zh-CN') }} ~
                {{ new Date(detail.track[detail.track.length - 1].ts).toLocaleDateString('zh-CN') }}
              </div>
              <div v-else class="warn">近 30 天只有 {{ detail.track.length }} 个船位点，画不出轨迹。
                <br /><span class="hint">船处于 AIS 静默期（远洋 / 未进接收范围）时会这样，属正常。</span>
              </div>
            </template>
            <template v-else-if="!detailLoading">
              <div class="warn">该船近 30 天无船位记录。</div>
            </template>
          </div>
        </div>

        <div class="coverage" v-if="stats?.coverageNote">{{ stats.coverageNote }}</div>
      </aside>
    </div>
  </div>
</template>

<style scoped>
/* 整屏布局：页面自身占满一屏，内部用 flex 纵向分配 —— 标题栏/提示条占自然高度，
   地图区 flex:1 吃掉剩余全部空间。原来地图写死 620px，在大屏上只有小半屏。
   为什么用 vh 而不是 100%：.app-main 是 flex:1 的滚动容器，高度来自
   .app-shell 的 min-height:100vh，对它写 height:100% 解析不出确定值。
   高度值要和 App.vue 的 .sidebar(100vh)/.app-shell(min-height:100vh) 保持一致，
   否则会出现「页面比一屏矮一点」或「多出一条滚动条」。 */
.oil-page {
  padding: 18px 22px 28px;
  height: 100vh; display: flex; flex-direction: column;
}

.page-head {
  display: flex; align-items: flex-end; justify-content: space-between;
  gap: 16px; flex-wrap: wrap; margin-bottom: 14px;
}
.title-block h1 { margin: 0; font-size: 22px; font-weight: 700; letter-spacing: -0.4px; }
.sub { margin: 4px 0 0; font-size: 12.5px; color: #8a94a6; }

.stats { display: flex; gap: 10px; flex-wrap: wrap; }
.stat {
  background: #fff; border: 1px solid #eef0f5; border-radius: 10px;
  padding: 7px 13px; display: flex; flex-direction: column; align-items: flex-start; min-width: 76px;
}
.stat b { font-size: 16px; color: #1a1a2e; line-height: 1.2; }
.stat span { font-size: 11px; color: #98a0b0; margin-top: 1px; }

.guide {
  background: #fff8e6; border: 1px solid #ffe2a8; border-radius: 10px;
  padding: 12px 16px; margin-bottom: 14px; font-size: 12.5px; color: #7a5b12; line-height: 1.75;
}
.guide-title { font-weight: 700; margin-bottom: 4px; }
.guide.soft { background: #f2f7ff; border-color: #d5e4ff; color: #35558c; }
.guide.soft code { border-color: #d5e4ff; }
.guide code { background: #fff; padding: 1px 5px; border-radius: 4px; border: 1px solid #f0dca8; font-size: 12px; }
.guide ol { margin: 4px 0 0; padding-left: 20px; }
.guide a { color: #2f6fed; }

/* align-items 必须是 stretch（不能 flex-start）：地图与右侧面板要等高撑满。
   flex-start 会让两者各按内容高度，地图就退回写死高度。
   min-height:0 是 flex 纵向伸缩的必要条件（子项默认 min-height:auto 会被内容顶开）。 */
.body { display: flex; gap: 14px; align-items: stretch; flex: 1; min-height: 0; }

.map-wrap {
  position: relative; flex: 1; min-width: 0;
  /* min-height 是可用性下限：出现「地图初始化失败/未配 Key」提示条时纵向空间会被挤，
     再矮就该让整页滚动，而不是把地图压成一条缝。 */
  min-height: 320px;
  background: #eef1f6; border: 1px solid #e6e9f0; border-radius: 12px; overflow: hidden;
}
/* 100% 而非固定 px：高度由 .map-wrap 给出（flex 拉伸成确定高度）。
   腾讯 GL JS 初始化时读的是布局后的实际高度，所以不再写死像素。 */
.map { width: 100%; height: 100%; }
.map.hidden { display: none; }

/* 空态/失败态是「盖在地图上的浮层」，必须 absolute —— 早先写成普通流式块，
   被 .map-wrap 的 overflow:hidden 连同那点高度一起裁掉：DOM 里在、屏幕上没有。
   改 absolute 后自身不再撑高，故 .map-wrap 要补 min-height。 */
.map-empty {
  /* z-index 必须 > 1000：腾讯 GL JS 的地图容器自带 z-index:1000，
     低于它会被地图连同它那条「鉴权失败」提示条盖住。 */
  position: absolute; inset: 0; z-index: 1100;
  display: flex; flex-direction: column; align-items: center;
  justify-content: center; gap: 8px; padding: 0 24px; text-align: center;
  color: #8a94a6; font-size: 13.5px;
  background: rgba(238,241,246,.96);
}
.map-empty-actions { margin-top: 4px; }
.map-empty.soft { color: #8a94a6; }
.map-empty-icon { font-size: 34px; }
.map-empty .hint { font-size: 12px; color: #a8b0be; line-height: 1.9; }
.map-empty code { background: #fff; padding: 2px 6px; border-radius: 4px; border: 1px solid #e3e7ee; }

.legend {
  /* 只放左上角。左下角是腾讯 SDK 自己的地盘：比例尺 + logo + 「腾讯地图 ©… GS(…)号」
     版权行，位置不受我们控制；原先放 bottom:12px 时实测与版权行重叠 88x7px（字叠字）。
     右上角同样被 SDK 的缩放/旋转控件占着。左上角是唯一干净的角。 */
  position: absolute; left: 12px; top: 12px; z-index: 30;
  background: rgba(255,255,255,.94);
  border: 1px solid #e6e9f0; border-radius: 8px; padding: 6px 10px;
  display: flex; gap: 14px; font-size: 12px; color: #5a6272;
}
.legend i { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 5px; }

.side {
  width: 356px; flex-shrink: 0; display: flex; flex-direction: column; gap: 10px;
  min-height: 0;
  background: #fff; border: 1px solid #eef0f5; border-radius: 12px; padding: 12px;
}
.filters { display: flex; flex-direction: column; gap: 8px; }
.filter-row { display: flex; gap: 12px; flex-wrap: wrap; }
.list-head { font-size: 12.5px; color: #6b7280; border-bottom: 1px solid #f2f4f8; padding-bottom: 6px; }
.list-head b { color: #1a1a2e; }
.list-sub { color: #a3aab8; }

/* 列表吃掉面板的剩余高度（原来是 max-height:430px 死值，整屏后只剩一半高）。
   min-height:0 必须有：flex 子项默认 min-height:auto，会被内容顶开，
   那样 overflow-y 永远不会触发、面板被撑出可视区。 */
.list { flex: 1; min-height: 0; overflow-y: auto; display: flex; flex-direction: column; gap: 2px; }
.item {
  display: flex; justify-content: space-between; gap: 8px; align-items: center;
  padding: 7px 8px; border-radius: 8px; cursor: pointer; transition: background .12s;
}
.item:hover { background: #f5f8ff; }
.item.active { background: #e8f1ff; }
.item.nopos { opacity: .62; }
.item-name { display: flex; align-items: center; gap: 6px; font-size: 13px; }
.item-name .cn { font-weight: 600; color: #1a1a2e; }
.item-name .en { color: #8a94a6; font-size: 11.5px; }
.dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
.item-meta { font-size: 11px; color: #98a0b0; margin-top: 2px; }
.item-right { display: flex; flex-direction: column; align-items: flex-end; gap: 2px; flex-shrink: 0; }
.stale { font-size: 11px; color: #6b7280; }
.dest { font-size: 10.5px; color: #a8b0be; max-width: 96px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nopos-tag { font-size: 10.5px; color: #b6bdc9; }
.empty, .err { font-size: 12.5px; color: #98a0b0; padding: 14px 4px; text-align: center; }
.err { color: #e56b6b; }

.detail { border-top: 1px solid #f2f4f8; padding-top: 8px; }
.detail-head {
  display: flex; justify-content: space-between; font-size: 12.5px; font-weight: 600;
  color: #1a1a2e; margin-bottom: 6px;
}
.detail-head .close { cursor: pointer; color: #b6bdc9; font-weight: 400; }
.detail-body { min-height: 34px; font-size: 12px; line-height: 1.7; }
.detail-body .ok { color: #4b5563; }
.detail-body .warn { color: #b07d20; }
.detail-body .hint { color: #a8b0be; font-size: 11px; }

.coverage { font-size: 11px; color: #a8b0be; line-height: 1.65; border-top: 1px solid #f2f4f8; padding-top: 8px; }
</style>
