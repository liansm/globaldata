<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { fetchMarkets } from '@/api/markets'
import type { MarketIndex } from '@/types/market'
import ShippingVesselPanel from '@/components/ShippingVesselPanel.vue'

// ── 航运信息 ─────────────────────────────────────────────────────────────────
// 由来：原先「航运运价」这一大块散落在「大宗商品」页底部（Home.vue 的
// shippingSections），和「油运信息」（VLCC 船位地图）分在两处，看运价的和看船的是
// 两拨人却找不到彼此。现在合成一页、两个 tab：
//   ① 运价指数 —— market_indices 里 market='航运' 的全部序列（CCFI / BDI / 油运 / CTFI 均值）
//   ② 油船信息 —— 中国船东 VLCC 船队 + AIS 船位（组件 ShippingVesselPanel）
// 运价部分与 Home.vue 的板块定义**不再重复**：Home 已移除航运 section，这里成为唯一出处。

const route = useRoute()
const router = useRouter()

const loading = ref(false)
const error = ref('')
const shipping = ref<MarketIndex[]>([])

onMounted(async () => {
  loading.value = true
  try {
    const rows = await fetchMarkets()
    // market='航运' 是目前运价指数的唯一归类；建材（水泥 CEMPI）留在「大宗商品」
    shipping.value = rows.filter(r => r.market === '航运')
  } catch {
    error.value = '加载失败，请检查后端服务（localhost:3000）是否启动'
  } finally {
    loading.value = false
  }
})

// ── tab ──────────────────────────────────────────────────────────────────────
// tab 状态与 URL query 双向绑定（?tab=vessels），这样刷新/分享链接能直达油船页，
// 旧的 /oil-shipping 路由也重定向到这里并带上 ?tab=vessels（见 router/index.ts）。
type TabName = 'prices' | 'vessels'
const tab = ref<TabName>(route.query.tab === 'vessels' ? 'vessels' : 'prices')
watch(() => route.query.tab, t => { tab.value = t === 'vessels' ? 'vessels' : 'prices' })
function onTabChange(name: string | number) {
  // el-tabs 先改 modelValue 再抛 tab-change，这里只负责把 URL 对齐（避免多余导航）。
  const wantVessels = name === 'vessels'
  if (wantVessels === (route.query.tab === 'vessels')) return
  router.replace(wantVessels ? { name: 'shipping', query: { tab: 'vessels' } } : { name: 'shipping' })
}

// ── 运价分组 ─────────────────────────────────────────────────────────────────
// 分组顺序：BDI 干散货 → CCFI 集装箱 → 油运（点值）→ 油运（期间均值）。
// 2026-10-30 按需求调整：**油运整体下移到集装箱之后**（原先油运在最前）。
// 干散货与集装箱的相对顺序不变，只把油运这两组挪到最后 —— 与顶部概览卡片的
// 顺序（BDI → CCFI → BDTI → BCTI）保持一致，上下对得上。
const CCFI_ORDER = [
  'ccfi_total', 'ccfi_europe', 'ccfi_med', 'ccfi_wc_america', 'ccfi_ec_america',
  'ccfi_japan', 'ccfi_korea', 'ccfi_se_asia', 'ccfi_anz',
  'ccfi_south_africa', 'ccfi_south_america', 'ccfi_we_africa', 'ccfi_persian_gulf',
]
// 干散货（BDTI/BCTI 是油运指数，不在这一组 —— 见 OIL_ORDER）
const BDI_ORDER = ['bdi', 'bci', 'bsi']
// 油运：VLCC 原油 / 成品油运价 + TCE（等价期租租金，船东盈利的直接代理变量）
// 顺序：BDTI/BCTI 国际油运指数 → CTFI 中国进口原油综合 → CT1 中东湾 → CT2 西非 → CT4 美湾
// ctfi_month_close = 月报口径的「CTFI 月末值」，是**点值**（与 ctfi_total 同口径），
// 只是日期用月末标签（源报告未写具体日，宁缺勿错）→ 归在本组，能把 ctfi_total 补到 2021-09
const OIL_ORDER = [
  'bdti', 'bcti',
  'ctfi_total', 'ctfi_month_close',
  'ctfi_ct1', 'ctfi_ct1_ws', 'ctfi_ct1_usd_ton', 'ctfi_ct1_tce_std', 'ctfi_ct1_tce_eco',
  'ctfi_ct2', 'ctfi_ct2_ws', 'ctfi_ct2_usd_ton', 'ctfi_ct2_tce_std', 'ctfi_ct2_tce_eco',
  'ctfi_ct4', 'ctfi_ct4_usd_ton', 'ctfi_ct4_tce_std', 'ctfi_ct4_tce_eco',
]
// ⚠ 期间均值（月均 / 周均）**口径不同于上面点值**，必须单独成组：
// 均值把区间波动抹平，和当日点值并排比大小会失真。标题里写明口径是硬要求。
const CTFI_AVG_ORDER = [
  'ctfi_month_avg',
  'ctfi_ct1_ws_month_avg', 'ctfi_ct1_ws_week_avg',
  'ctfi_ct1_tce_std_month_avg', 'ctfi_ct1_tce_std_week_avg',
  'ctfi_ct2_ws_month_avg', 'ctfi_ct2_ws_week_avg',
  'ctfi_ct2_tce_std_month_avg', 'ctfi_ct2_tce_std_week_avg',
  'ctfi_ct4_usd_month_avg', 'ctfi_ct4_tce_std_month_avg',
]

const groups = computed(() => {
  const map = new Map(shipping.value.map(s => [s.key, s]))
  return [
    { key: 'dry',     title: 'BDI 波罗的海干散货运价', icon: '⚓', keys: BDI_ORDER },
    { key: 'ccfi',    title: 'CCFI 出口集装箱运价', icon: '🚢', keys: CCFI_ORDER },
    { key: 'oil',     title: '油运：原油 / 成品油运价与 VLCC TCE', icon: '🛢️', keys: OIL_ORDER },
    { key: 'oil-avg', title: '油运 · CTFI 期间均值（月均 / 周均，均值口径，勿与上面当日点值直接比大小）',
      icon: '📉', keys: CTFI_AVG_ORDER },
  ].map(g => ({
    ...g,
    items: g.keys.map(k => map.get(k)).filter(Boolean) as MarketIndex[],
  })).filter(g => g.items.length > 0)
})

const totalCount = computed(() => groups.value.reduce((n, g) => n + g.items.length, 0))

// ── 概览 ─────────────────────────────────────────────────────────────────────
// 四个「看一眼就够」的代表序列：干散货 → 集装箱 → 油运双指数。
// 顺序跟下面的分组一致（油运放最后）；只取点值口径（ctfi_total 而非月均），
// 别把均值混进来。
const OVERVIEW_KEYS = ['bdi', 'ccfi_total', 'bdti', 'bcti']
const overview = computed(() => {
  const map = new Map(shipping.value.map(s => [s.key, s]))
  return OVERVIEW_KEYS.map(k => map.get(k)).filter(Boolean) as MarketIndex[]
})

// ── 展示工具 ─────────────────────────────────────────────────────────────────
function fmt(price: number | null | undefined) {
  if (price == null) return '—'
  return price.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
}
function fmtDate(d: string | null) {
  return d ? d.slice(0, 10) : '—'
}
function pctClass(v: number | null | undefined) {
  if (v == null) return ''
  return v >= 0 ? 'up' : 'down'
}
function fmtPct(v: number | null | undefined) {
  if (v == null) return '—'
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
}
function goDetail(row: MarketIndex) {
  router.push({ name: 'market-detail', params: { key: row.key } })
}
</script>

<template>
  <div class="shipping-page">
    <header class="page-head">
      <div>
        <h1>航运信息</h1>
        <p class="subtitle">
          航运与油运运价（BDI 干散货 / CCFI 集装箱 / BDTI·BCTI·CTFI 油运）
          <span class="dot">·</span> 中国船东 VLCC 船队与 AIS 船位
        </p>
      </div>
      <div class="head-stats" v-if="shipping.length">
        <div class="hstat"><b>{{ groups.length }}</b><span>组运价</span></div>
        <div class="hstat"><b>{{ totalCount }}</b><span>条序列</span></div>
      </div>
    </header>

    <div class="tabs-wrap">
      <el-tabs v-model="tab" class="ship-tabs" @tab-change="onTabChange">
        <!-- ── ① 运价指数 ─────────────────────────────────────────────── -->
        <el-tab-pane name="prices">
          <template #label>
            <span class="tab-label">📈 运价指数</span>
          </template>

          <div class="pane scroll">
            <el-alert
              v-if="error" :title="error" type="error" show-icon :closable="false"
              style="margin-bottom: 16px"
            />

            <div v-if="loading" class="skeleton-wrap">
              <el-skeleton :rows="8" animated />
            </div>

            <template v-else-if="shipping.length">
              <!-- 概览：代表序列一眼看 -->
              <div class="overview">
                <div
                  v-for="o in overview" :key="o.key"
                  class="ov-card" @click="goDetail(o)"
                >
                  <div class="ov-name">{{ o.name }}</div>
                  <div class="ov-price">
                    {{ fmt(o.latestClose) }}<span class="ov-unit">{{ o.unit ?? '点' }}</span>
                  </div>
                  <div class="ov-foot">
                    <span class="ov-pct" :class="pctClass(o.changePct)">{{ fmtPct(o.changePct) }}</span>
                    <span class="ov-date">{{ fmtDate(o.latestDate) }}</span>
                  </div>
                </div>
              </div>

              <!-- 分组明细 -->
              <section v-for="g in groups" :key="g.key" class="group">
                <div class="group-head">
                  <span class="group-icon">{{ g.icon }}</span>
                  <h2 class="group-title">{{ g.title }}</h2>
                  <span class="group-count">{{ g.items.length }} 条</span>
                </div>

                <div class="card-grid">
                  <div
                    v-for="item in g.items" :key="item.key"
                    class="card" @click="goDetail(item)"
                  >
                    <div class="card-top">
                      <span class="card-name" :title="item.name">{{ item.name }}</span>
                    </div>
                    <div class="card-price">
                      {{ fmt(item.latestClose) }}
                      <span class="card-unit">{{ item.unit ?? '点' }}</span>
                    </div>
                    <div class="card-pct" :class="pctClass(item.changePct)">
                      {{ fmtPct(item.changePct) }}
                    </div>
                    <div class="card-footer">
                      <span class="card-date">{{ fmtDate(item.latestDate) }}</span>
                      <span class="card-key" :title="item.key">{{ item.key }}</span>
                      <span class="card-arrow">→</span>
                    </div>
                  </div>
                </div>
              </section>

              <p class="foot-note">
                数据来自 <code>market_indices</code>（market='航运'）。
                点值（当日）与期间均值（月均 / 周均）口径不同，请勿直接比大小；点击卡片查看历史走势。
              </p>
            </template>

            <div v-else-if="!error" class="empty">暂无航运运价数据</div>
          </div>
        </el-tab-pane>

        <!-- ── ② 油船信息 ─────────────────────────────────────────────── -->
        <el-tab-pane name="vessels">
          <template #label>
            <span class="tab-label">🛢️ 油船信息</span>
          </template>
          <!-- ⚠ 必须 v-if 挂载，不能 v-show：地图容器在 display:none 时尺寸为 0，
               腾讯 GL 初始化读到的就是 0×0，切回来是空白；而本 SDK 没有可靠的 resize 入口。
               每次切进 tab 重新挂载 → onMounted 里 nextTick 后量到的就是真实尺寸。 -->
          <div class="pane" v-if="tab === 'vessels'">
            <ShippingVesselPanel />
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
</template>

<style scoped>
/* 与 OilShipping 原先的整屏布局同源：页面占满一屏，内部 flex 纵向分配，
   地图区吃剩余高度。高度用 vh 而不是 100%，因为 .app-main 是 flex:1 的滚动容器，
   对它 height:100% 解析不出确定值（详见 App.vue 的 skeleton 注释）。 */
.shipping-page {
  padding: 16px 22px 18px;
  height: 100vh;
  display: flex;
  flex-direction: column;
}

.page-head {
  display: flex; align-items: flex-end; justify-content: space-between;
  gap: 16px; flex-wrap: wrap; margin-bottom: 8px; flex-shrink: 0;
}
.page-head h1 { margin: 0; font-size: 22px; font-weight: 700; letter-spacing: -0.4px; }
.subtitle { margin: 4px 0 0; font-size: 12.5px; color: #8a94a6; }
.subtitle .dot { margin: 0 4px; color: #c9cfda; }

.head-stats { display: flex; gap: 8px; }
.hstat {
  background: #fff; border: 1px solid #eef0f5; border-radius: 10px;
  padding: 6px 12px; display: flex; flex-direction: column; min-width: 70px;
}
.hstat b { font-size: 15px; line-height: 1.2; color: #1a1a2e; }
.hstat span { font-size: 11px; color: #98a0b0; }

/* ── tabs 撑满剩余高度 ──────────────────────────────────────────────────────
   Element Plus 的 .el-tabs / .el-tabs__content 默认按内容高度排，不给 flex，
   所以整条链都要显式 flex。少一环（比如漏了 __content）就会出现「地图 0 高」。 */
.tabs-wrap { flex: 1; min-height: 0; display: flex; flex-direction: column; }
.ship-tabs { flex: 1; min-height: 0; display: flex; flex-direction: column; }
:deep(.el-tabs__header) { margin-bottom: 12px; flex-shrink: 0; }
:deep(.el-tabs__content) { flex: 1; min-height: 0; overflow: hidden; }
:deep(.el-tab-pane) { height: 100%; }
.tab-label { font-size: 13.5px; font-weight: 500; }

.pane { height: 100%; min-height: 0; display: flex; flex-direction: column; }
.pane.scroll { overflow-y: auto; display: block; padding-right: 4px; }

/* ── 概览 ─────────────────────────────────────────────────────────────── */
/* 4 列与下面的卡片网格一致（原来 auto-fit + minmax(180px) 是按可用宽度自己排，
   和网格的列宽对不上，上下左边线是歪的）。概览固定 4 条序列，正好四列；
   万一某条缺数据，第四格留空也不影响阅读。 */
.overview {
  display: grid; grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px; margin-bottom: 22px;
}
.ov-card {
  background: linear-gradient(180deg, #fff, #fbfcff);
  border: 1px solid #e8eaf2; border-radius: 12px;
  padding: 12px 14px; cursor: pointer;
  transition: box-shadow .18s, transform .18s, border-color .18s;
}
.ov-card:hover { box-shadow: 0 6px 18px rgba(0,0,0,.08); transform: translateY(-2px); border-color: #409eff; }
.ov-name { font-size: 12.5px; color: #6b7280; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.ov-price {
  font-size: 22px; font-weight: 700; color: #1a1a2e;
  font-variant-numeric: tabular-nums; line-height: 1.25; margin-top: 2px;
}
.ov-unit { font-size: 12px; font-weight: 400; color: #98a0b0; margin-left: 3px; }
.ov-foot { display: flex; align-items: baseline; justify-content: space-between; margin-top: 2px; }
.ov-pct { font-size: 13px; font-weight: 600; font-variant-numeric: tabular-nums; }
.ov-date { font-size: 11px; color: #b6bdc9; }

/* ── 分组 ─────────────────────────────────────────────────────────────── */
.group { margin-bottom: 24px; }
.group-head { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
.group-icon { font-size: 16px; line-height: 1; }
.group-title { margin: 0; font-size: 15px; font-weight: 600; color: #1a1a2e; }
.group-count { margin-left: auto; font-size: 12px; color: #aaa; }

/* ── 卡片网格：固定 4 列 ───────────────────────────────────────────────────
   原来 auto-fill + minmax(212px, 1fr)：1500 宽的窗口会排出 5 列，单卡只有 ~210px，
   名称两行被挤、页脚 key 也糊成一团。改成显式 4 列（窄屏 3/2/1），
   单卡 ~340px，名称、数值、日期都松得开。
   ⚠ 窗口再宽也是 4 列，不再「越宽越多」——这是刻意的密度上限。 */
.card-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 14px;
}
@media (max-width: 1200px) { .card-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
@media (max-width: 900px)  { .card-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 620px)  { .card-grid { grid-template-columns: 1fr; } }
@media (max-width: 1200px) { .overview { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 620px)  { .overview { grid-template-columns: 1fr; } }
.card {
  background: #fff; border: 1px solid #e8e8f0; border-radius: 12px;
  padding: 13px 15px 11px; cursor: pointer;
  transition: box-shadow .18s, transform .18s, border-color .18s;
  display: flex; flex-direction: column; gap: 6px;
}
.card:hover { box-shadow: 0 6px 20px rgba(0,0,0,.09); transform: translateY(-2px); border-color: #409eff; }
.card-top { display: flex; align-items: flex-start; justify-content: space-between; gap: 6px; }
/* ⚠ 名称必须占满整行、放两行：CTFI 那批序列名只差尾巴（WS / 美元/吨 / 美元/天(标准航速)），
   一截断就变成一堆同名卡片，分不出哪条是哪条（实测 5 张卡全显示「CTFI 中东湾—中国宁...」）。
   所以右上角**不再放 key 标签**（它会吃掉 40~60px，正好把差异段挤掉），key 移到页脚。
   超出两行仍然省略 —— 那种情况下面页脚的 key 就是兜底。 */
.card-name {
  font-size: 13px; font-weight: 600; color: #1a1a2e; line-height: 1.35;
  display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
  overflow: hidden; word-break: break-word;
  /* 强制占两行的高度：同一排里有的名字一行、有的两行，价格会跟着上下错位
     （截图实测差 17px，很显眼）。统一占满两行后价格基线齐平。 */
  min-height: 2.7em;
}
.card-price {
  font-size: 20px; font-weight: 700; color: #1a1a2e;
  font-variant-numeric: tabular-nums; line-height: 1.15;
}
.card-unit { font-size: 11.5px; font-weight: 400; color: #98a0b0; margin-left: 3px; }
/* 涨跌幅默认色 = 浅灰：changePct 为 null 时渲染的是「—」，不能让它跟真涨跌一样显眼 */
.card-pct { font-size: 13px; font-weight: 600; font-variant-numeric: tabular-nums; color: #c9cfda; }
.ov-pct { color: #c9cfda; }

/* 红涨绿跌（中国习惯） */
.up { color: #f56c6c; }
.down { color: #67c23a; }

.card-footer { display: flex; align-items: center; gap: 6px; margin-top: auto; }
.card-date { font-size: 11.5px; color: #aaa; flex-shrink: 0; }
/* key 是内部标识（也是详情页 URL 的最后一段），放页脚当小字更好：既不挤名称，
   也还能对着 URL / 日志核得上。 */
.card-key {
  margin-left: auto; font-size: 10px; color: #c2c8d4; letter-spacing: -0.2px;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 118px;
}
.card-arrow { font-size: 13px; color: #409eff; flex-shrink: 0; }

.foot-note { font-size: 11.5px; color: #a8b0be; line-height: 1.7; margin: 6px 0 10px; }
.foot-note code { background: #f5f7fb; padding: 1px 5px; border-radius: 4px; }

.skeleton-wrap { padding: 16px 0; }
.empty { font-size: 13px; color: #98a0b0; padding: 40px 0; text-align: center; }
</style>
