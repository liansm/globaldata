<script setup lang="ts">
import { ref, computed, onMounted, nextTick, watch } from 'vue'
import { fetchIpoCalendar, fetchIpoStats } from '@/api/ipo'
import type { IpoItem, IpoStats, IpoEvent } from '@/types/ipo'

const loading = ref(false)
const error   = ref('')
const items   = ref<IpoItem[]>([])
const stats   = ref<IpoStats[]>([])

const view   = ref<'calendar' | 'list'>('calendar')
const market = ref('全部')
const MARKET_TABS = ['全部', 'A股', '北交所', '港股']

const today = new Date()
const todayStr = ymd(today)
const cursor = ref(new Date(today.getFullYear(), today.getMonth(), 1))

// 详情弹窗
const detailVisible = ref(false)
const detailItem    = ref<IpoItem | null>(null)

// 列表容器 —— 切到列表视图时用来定位到今天
const listWrap = ref<HTMLElement | null>(null)

// ── 日期小工具（不引第三方库）──────────────────────────────────────────────
function ymd(d: Date) {
  const p = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`
}
function parseYmd(s: string) {
  const [y, m, d] = s.split('-').map(Number)
  return new Date(y, m - 1, d)
}
function fmtDate(s: string | null) {
  if (!s) return '—'
  const d = parseYmd(s.slice(0, 10))
  return `${d.getMonth() + 1}月${d.getDate()}日`
}
function fmtDateFull(s: string | null) {
  return s ? s.slice(0, 10) : '—'
}

// ── 数据加载：按当前显示月取数（含日历跨月格余量）────────────────────────
async function load() {
  loading.value = true
  error.value = ''
  try {
    const y = cursor.value.getFullYear()
    const m = cursor.value.getMonth()
    // 日历固定 42 格，第一格可能落在上月末 6 天内，最后一格最远到下月 12 日。
    // 前后各留 7 天，足够覆盖全部跨月格；列表只取当月（见 listGroups），多拉的余量不会流到列表。
    const from = ymd(new Date(y, m, 1 - 7))
    const to   = ymd(new Date(y, m, 1 + 42))
    const mk = market.value === '全部' ? undefined : market.value
    const [rows, st] = await Promise.all([
      fetchIpoCalendar({ market: mk, from, to, limit: 2000 }),
      fetchIpoStats({ from: todayStr, to: ymd(new Date(today.getFullYear(), today.getMonth() + 4, 0)) }).catch(() => []),
    ])
    items.value = rows
    stats.value = st
  } catch {
    error.value = '加载失败，请检查后端服务是否启动'
  } finally {
    loading.value = false
  }
  // 换了一批数据（切月 / 切市场）后列表整体重排，滚动位置要跟着重置，
  // 否则会停在上一批数据的偏移上。必须在 loading 落地、DOM 重渲染之后调。
  await scrollListToAnchor()
}

onMounted(load)

// 列表按日期升序，今天通常落在中间 —— 不定位一下，切过去看到的是月初的
// 「已上市」，会误以为最近没有新股。优先滚到今天那一组，当月没有今天则回到顶部。
async function scrollListToAnchor() {
  if (view.value !== 'list') return
  await nextTick()
  const anchor = (listWrap.value?.querySelector('.list-date.on')
    ?? listWrap.value?.querySelector('.list-date')) as HTMLElement | null
  if (anchor) anchor.scrollIntoView({ block: 'start' })
}
watch(view, v => { if (v === 'list') scrollListToAnchor() })

function shiftMonth(delta: number) {
  cursor.value = new Date(cursor.value.getFullYear(), cursor.value.getMonth() + delta, 1)
  load()
}
function shiftToToday() {
  cursor.value = new Date(today.getFullYear(), today.getMonth(), 1)
  load()
}
function changeMarket(m: string) {
  market.value = m
  load()
}

const monthLabel = computed(
  () => `${cursor.value.getFullYear()}年${cursor.value.getMonth() + 1}月`,
)

// ── 按日期归组事件 ─────────────────────────────────────────────────────────
// 一只新股最多贡献 4 个关键日期：申购/招股、暗盘、中签公布、上市。
// 日历/列表只画前三个里的 apply / grey / listing（中签、缴款放进详情，避免格子过载），
// 这个约束落在 IpoEvent['kind'] 的类型定义上，所以这里直接用即可。
const eventsByDate = computed(() => {
  const map: Record<string, IpoEvent[]> = {}
  const push = (date: string | null, kind: IpoEvent['kind'], item: IpoItem) => {
    if (!date) return
    const key = date.slice(0, 10)
    ;(map[key] ??= []).push({ date: key, kind, item })
  }
  for (const it of items.value) {
    push(it.applyDate,    'apply',   it)
    push(it.greyDate,     'grey',    it)
    push(it.listingDate,  'listing', it)
  }
  // 同一天常常挤着「申购 + 暗盘 + 上市」，申购是用户要动手的那件事，必须排最前。
  // 不能依赖 items 的顺序 —— 那是后端按 COALESCE(apply_date, listing_date) 排的，
  // 会把「某只 9/21 申购的股票在 9/28 的暗盘」插到「9/28 申购的新股」前面。
  const KIND_ORDER: Record<IpoEvent['kind'], number> = { apply: 0, grey: 1, listing: 2 }
  for (const key of Object.keys(map)) {
    map[key].sort((a, b) => KIND_ORDER[a.kind] - KIND_ORDER[b.kind])
  }
  return map
})

const calendarCells = computed(() => {
  const y = cursor.value.getFullYear()
  const m = cursor.value.getMonth()
  const first = new Date(y, m, 1)
  const offset = (first.getDay() + 6) % 7      // 周一为一周起点
  const cells = []
  for (let i = 0; i < 42; i++) {
    const d = new Date(y, m, 1 - offset + i)
    const key = ymd(d)
    cells.push({
      date:     key,
      day:      d.getDate(),
      inMonth:  d.getMonth() === m,
      isToday:  key === todayStr,
      events:   eventsByDate.value[key] ?? [],
    })
  }
  return cells
})

// 列表视图：按日期升序分组，**只保留 cursor 所在的那一个月**。
// ⚠ 曾经写成「±2 个月」，本意是怕数据不够；但用户点了 9 月却看到 7/8/10 月的行，
//   等于月份切换失效。列表的语义就是「选中月的明细」，必须严格当月。
//   数据侧多拉的余量只服务于日历的跨月格，不该流到列表来。
const listGroups = computed(() => {
  const y = cursor.value.getFullYear()
  const m = cursor.value.getMonth()
  const groups: { date: string; events: IpoEvent[] }[] = []
  for (const [date, evs] of Object.entries(eventsByDate.value)) {
    const d = parseYmd(date)
    if (d.getFullYear() === y && d.getMonth() === m) groups.push({ date, events: evs })
  }
  return groups.sort((a, b) => a.date.localeCompare(b.date))
})

// ── 展示辅助 ───────────────────────────────────────────────────────────────
/**
 * 打新动作的市场术语。**只此一份** —— 事件标签、状态列、顶部统计卡都从这里取，
 * 散落硬编码中文的话，改一处漏一处，必然漂移成「招股」标签 +「今日申购」状态。
 * 港股没有「申购」，只有招股/认购期；A股/北交所才叫申购。
 */
function applyTerms(market: string) {
  return market === '港股'
    ? { verb: '招股', wait: '待招股', today: '今日招股', open: '招股中', done: '已截止' }
    : { verb: '申购', wait: '待申购', today: '今日申购', open: '申购中', done: '已申购' }
}

// apply 的措辞按市场走（见 applyTerms），这里只列两个市场共用的
const KIND_LABEL: Record<Exclude<IpoEvent['kind'], 'apply'>, string> = {
  grey:    '暗盘',
  listing: '上市',
}

function eventLabel(ev: IpoEvent) {
  return ev.kind === 'apply' ? applyTerms(ev.item.market).verb : KIND_LABEL[ev.kind]
}

/**
 * 状态口径。传 kind 时按**这一行代表的事件**判定（列表视图），
 * 不传时按股票的当前所处阶段判定（详情弹窗）。
 *
 * ⚠ 不能只看股票：同一只股票在列表里会出现两次（申购行 + 上市行），
 *   若状态只看股票，9/29 的「上市」行会显示成「申购期」，暗盘行也是。
 * ⚠ A股只有申购一天，港股是一段招股期 [applyDate, applyEndDate]，要分别处理。
 * ⚠ **术语必须跟市场走**：港股叫「招股/认购」，A股/北交所叫「申购」。
 *   否则港股行会出现左边标签写「招股」、右边状态写「今日申购」的自相矛盾。
 *   措辞集中在这里定义 —— 别在模板里硬编码中文，两处各写一遍必然漂移。
 */
type Status = { text: string; cls: string }

function statusOf(it: IpoItem, kind?: IpoEvent['kind']): Status {
  const listed = !!it.listingDate && it.listingDate <= todayStr

  if (kind === 'listing') {
    return listed
      ? { text: '已上市', cls: 'st-listed' }
      : { text: '待上市', cls: 'st-wait' }
  }
  if (kind === 'grey') {
    return { text: it.greyDate === todayStr ? '今日暗盘' : '暗盘', cls: 'st-grey' }
  }

  if (listed) return { text: '已上市', cls: 'st-listed' }
  if (it.greyDate === todayStr) return { text: '暗盘中', cls: 'st-grey' }

  const W = applyTerms(it.market)

  const start = it.applyDate
  const end   = it.applyEndDate ?? it.applyDate
  if (start && end) {
    if (todayStr < start) return { text: W.wait,  cls: 'st-wait' }
    if (todayStr <= end)  return { text: start === todayStr ? W.today : W.open, cls: 'st-today' }
  }
  if (it.listingDate) return { text: '待上市', cls: 'st-open' }
  if (it.applyDate)   return { text: W.done,   cls: 'st-open' }
  return { text: '—', cls: '' }
}

/** 价格展示：港股区间报价 → "1.48 ~ 1.59" */
function priceText(it: IpoItem) {
  if (it.issuePrice == null) return '待定'
  const sym = it.currency === 'HKD' ? 'HK$' : '¥'
  if (it.issuePriceHigh != null && it.issuePriceHigh !== it.issuePrice) {
    return `${sym}${it.issuePrice} ~ ${it.issuePriceHigh}`
  }
  return `${sym}${it.issuePrice}`
}

function raiseText(it: IpoItem) {
  if (it.raiseAmount == null) return '—'
  const unit = it.currency === 'HKD' ? '亿港元' : '亿'
  return `${it.raiseAmount.toFixed(2)} ${unit}`
}

/** 发行股数：量级横跨 120 万 ~ 8.1 亿股，统一按「亿/万」两档显示，避免一列里挤七八位数字。
 *  港股招股中的新股源侧不披露总发行股数 → 留空显示「—」，不用 0 充数。 */
function sharesText(it: IpoItem) {
  if (it.issueShares == null) return '—'
  const v = it.issueShares
  return v >= 1e8
    ? `${(v / 1e8).toFixed(2)} 亿股`
    : `${Math.round(v / 1e4).toLocaleString('zh-CN')} 万股`
}

function boardText(it: IpoItem) {
  return it.board || it.industry || '—'
}

function openDetail(it: IpoItem) {
  detailItem.value = it
  detailVisible.value = true
}

const DETAIL_FIELDS: { label: string; key: keyof IpoItem; fmt?: 'date' | 'num' | 'pct' }[] = [
  { label: '市场',       key: 'market' },
  { label: '代码',       key: 'code' },
  { label: '交易所',     key: 'exchange' },
  { label: '板块/行业',  key: 'board' },
  { label: '发行价',     key: 'issuePrice' },
  { label: '发行总数（股）', key: 'issueShares', fmt: 'num' },
  { label: '募集资金',   key: 'raiseAmount' },
  { label: '每手股数',   key: 'lotSize', fmt: 'num' },
  { label: '入场费',     key: 'entryFee', fmt: 'num' },
  { label: '申购/招股日', key: 'applyDate', fmt: 'date' },
  { label: '招股截止日', key: 'applyEndDate', fmt: 'date' },
  { label: '定价日',     key: 'pricingDate', fmt: 'date' },
  { label: '中签公布日', key: 'allotmentDate', fmt: 'date' },
  { label: '中签缴款日', key: 'payDate', fmt: 'date' },
  { label: '退票寄发日', key: 'refundDate', fmt: 'date' },
  { label: '暗盘日',     key: 'greyDate', fmt: 'date' },
  { label: '上市日',     key: 'listingDate', fmt: 'date' },
  { label: '发行市盈率', key: 'peIssue' },
  { label: '行业市盈率', key: 'peIndustry' },
  { label: '中签率',     key: 'winRate', fmt: 'pct' },
]

function detailValue(key: keyof IpoItem, fmt?: string) {
  const it = detailItem.value
  if (!it) return '—'
  const v = it[key]
  if (v == null || v === '') return '—'
  if (fmt === 'date') return fmtDateFull(String(v))
  if (fmt === 'num')  return Number(v).toLocaleString('zh-CN')
  if (fmt === 'pct')  return `${v}%`
  return String(v)
}
</script>

<template>
  <div class="ipo-page">
    <div class="page-header">
      <div>
        <h1>新股日历</h1>
        <p class="subtitle">A股 · 北交所 · 港股　|　申购日 / 招股日 / 暗盘 / 上市日</p>
      </div>
      <div class="stat-row">
        <div v-for="s in stats" :key="s.market" class="stat">
          <span class="stat-label">{{ s.market }}</span>
          <span class="stat-value">{{ s.upcomingApply }}<em>{{ applyTerms(s.market).wait }}</em></span>
          <span class="stat-value">{{ s.upcomingList }}<em>待上市</em></span>
        </div>
      </div>
    </div>

    <div class="toolbar">
      <div class="chips">
        <button
          v-for="m in MARKET_TABS"
          :key="m"
          class="chip"
          :class="{ on: market === m }"
          @click="changeMarket(m)"
        >{{ m }}</button>
      </div>
      <div class="spacer" />
      <button class="chip" @click="shiftMonth(-1)">‹</button>
      <span class="month-label" @click="shiftToToday" title="回到本月">{{ monthLabel }}</span>
      <button class="chip" @click="shiftMonth(1)">›</button>
      <div class="view-switch">
        <button class="chip" :class="{ on: view === 'calendar' }" @click="view = 'calendar'">日历</button>
        <button class="chip" :class="{ on: view === 'list' }" @click="view = 'list'">列表</button>
      </div>
    </div>

    <el-alert
      v-if="error"
      :title="error"
      type="error"
      show-icon
      :closable="false"
      style="margin-bottom: 16px"
    />

    <div v-if="loading" class="skeleton-wrap"><el-skeleton :rows="8" animated /></div>

    <template v-else>
      <!-- 日历视图 -->
      <div v-if="view === 'calendar'" class="cal-card">
        <div class="cal-week">
          <div v-for="w in ['一', '二', '三', '四', '五', '六', '日']" :key="w">{{ w }}</div>
        </div>
        <div class="cal-grid">
          <div
            v-for="cell in calendarCells"
            :key="cell.date"
            class="cal-cell"
            :class="{ out: !cell.inMonth, today: cell.isToday }"
          >
            <div class="cal-day" :class="{ on: cell.isToday }">
              {{ cell.day }}<span v-if="cell.isToday" class="today-tag">今日</span>
            </div>
            <div
              v-for="ev in cell.events.slice(0, 2)"
              :key="ev.kind + ev.item.code"
              class="cal-ev"
              :class="'ev-' + ev.kind"
              :title="`${ev.item.name} · ${eventLabel(ev)}`"
              @click="openDetail(ev.item)"
            >
              <span class="ev-dot" /><span class="ev-name">{{ ev.item.name }}</span>
            </div>
            <div v-if="cell.events.length > 2" class="cal-more">
              +{{ cell.events.length - 2 }} 只
            </div>
          </div>
        </div>
        <div class="legend">
          <span><i class="lg ev-apply" />申购 / 招股</span>
          <span><i class="lg ev-grey" />暗盘（港股）</span>
          <span><i class="lg ev-listing" />上市</span>
        </div>
      </div>

      <!-- 列表视图 -->
      <div v-else ref="listWrap" class="list-card">
        <div v-if="listGroups.length === 0" class="empty"><el-empty description="本月区间内暂无新股" /></div>
        <div v-for="g in listGroups" :key="g.date" class="list-group">
          <div class="list-date" :class="{ on: g.date === todayStr }">
            {{ fmtDate(g.date) }}
            <span class="list-weekday">周{{ '日一二三四五六'[parseYmd(g.date).getDay()] }}</span>
            <span v-if="g.date === todayStr" class="today-tag">今日</span>
          </div>
          <div class="list-body">
            <div
              v-for="ev in g.events"
              :key="ev.kind + ev.item.code"
              class="list-row"
              @click="openDetail(ev.item)"
            >
              <span class="ev-tag" :class="'ev-' + ev.kind">{{ eventLabel(ev) }}</span>
              <span class="row-name">{{ ev.item.name }}</span>
              <span class="row-code">{{ ev.item.code }}</span>
              <span class="row-market">{{ ev.item.market }}</span>
              <span class="row-price">{{ priceText(ev.item) }}</span>
              <span class="row-shares">{{ sharesText(ev.item) }}</span>
              <span class="row-raise">{{ raiseText(ev.item) }}</span>
              <span class="row-status" :class="statusOf(ev.item, ev.kind).cls">
                {{ statusOf(ev.item, ev.kind).text }}
              </span>
            </div>
          </div>
        </div>
      </div>
    </template>

    <!-- 详情 -->
    <el-dialog v-model="detailVisible" :title="detailItem?.name ?? ''" width="560px">
      <div v-if="detailItem" class="detail-head">
        <span class="detail-code">{{ detailItem.code }}</span>
        <span class="detail-market">{{ detailItem.market }}</span>
        <span class="detail-status" :class="statusOf(detailItem).cls">
          {{ statusOf(detailItem).text }}
        </span>
        <span class="detail-price">{{ priceText(detailItem) }}</span>
      </div>
      <div v-if="detailItem" class="detail-grid">
        <div v-for="f in DETAIL_FIELDS" :key="f.label" class="detail-cell">
          <span class="d-label">{{ f.label }}</span>
          <span class="d-value">{{ detailValue(f.key, f.fmt) }}</span>
        </div>
      </div>
      <div v-if="detailItem" class="detail-foot">
        数据来源：{{ detailItem.source }}　·　更新 {{ fmtDateFull(detailItem.updatedAt?.slice(0, 10) ?? null) }}
      </div>
    </el-dialog>
  </div>
</template>

<style scoped>
.ipo-page {
  max-width: 1180px;
  margin: 0 auto;
  padding: 36px 20px 60px;
}

.page-header {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 20px;
  margin-bottom: 24px;
  flex-wrap: wrap;
}

h1 { margin: 0 0 4px; font-size: 28px; font-weight: 700; color: #1a1a2e; }
.subtitle { margin: 0; color: #888; font-size: 16px; }

/* ── 顶部统计 ─────────────────────────────────────────────────────────── */
.stat-row { display: flex; gap: 18px; flex-wrap: wrap; }
.stat {
  display: flex; align-items: center; gap: 8px;
  padding: 8px 14px; background: #fff; border: 1px solid #e8e8f0; border-radius: 10px;
}
.stat-label { font-size: 14px; color: #888; }
.stat-value { font-size: 17px; font-weight: 700; color: #1a1a2e; font-variant-numeric: tabular-nums; }
.stat-value em { font-size: 13px; font-weight: 400; font-style: normal; color: #aaa; margin-left: 2px; }

/* ── 工具条 ───────────────────────────────────────────────────────────── */
.toolbar {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 16px; flex-wrap: wrap;
}
.spacer { flex: 1; }
.chips, .view-switch { display: flex; gap: 6px; }
.chip {
  font: inherit; font-size: 15px;
  padding: 5px 13px; border-radius: 7px;
  border: 1px solid #dcdfe6; background: #fff; color: #555;
  cursor: pointer; transition: all .15s;
}
.chip:hover { border-color: #409eff; color: #409eff; }
.chip.on { background: #ecf5ff; border-color: #409eff; color: #409eff; font-weight: 600; }
.month-label {
  font-size: 16px; font-weight: 600; color: #1a1a2e;
  padding: 0 6px; cursor: pointer; min-width: 92px; text-align: center;
}
.month-label:hover { color: #409eff; }

/* ── 日历 ─────────────────────────────────────────────────────────────── */
.cal-card {
  background: #fff; border: 1px solid #e8e8f0; border-radius: 12px;
  overflow: hidden;
}
.cal-week {
  display: grid; grid-template-columns: repeat(7, minmax(0, 1fr));
  background: #fafbfc;
}
.cal-week > div {
  padding: 8px 0; text-align: center; font-size: 14px; color: #888;
}
.cal-grid { display: grid; grid-template-columns: repeat(7, minmax(0, 1fr)); }
.cal-cell {
  min-height: 100px; padding: 6px 7px;
  border-right: 1px solid #f0f0f5; border-bottom: 1px solid #f0f0f5;
}
.cal-cell:nth-child(7n) { border-right: none; }
.cal-cell:nth-last-child(-n+7) { border-bottom: none; }
.cal-cell.out { background: #fbfbfd; }
.cal-cell.out .cal-day { color: #ccc; }
.cal-cell.today { background: #f4f9ff; box-shadow: inset 0 0 0 1px #b9dbff; }
.cal-day { font-size: 14px; color: #888; margin-bottom: 3px; }
.cal-day.on { color: #409eff; font-weight: 700; }
.today-tag {
  display: inline-block; margin-left: 4px; padding: 0 4px;
  font-size: 12px; line-height: 17px; border-radius: 3px;
  background: #409eff; color: #fff; font-weight: 400;
}
.cal-ev {
  display: flex; align-items: center; gap: 4px;
  font-size: 13px; padding: 1px 4px; border-radius: 3px;
  margin-bottom: 2px; cursor: pointer;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.cal-ev:hover { filter: brightness(0.96); }
.ev-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
/* flex 容器里 text-overflow 对裸文本节点不生效，名字必须包一层 */
.ev-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.cal-more { font-size: 12px; color: #999; padding-left: 2px; }

/* 事件配色：申购/招股 = 蓝，暗盘 = 紫，上市 = 琥珀 */
.ev-apply   { background: #ecf5ff; color: #1d6fbb; }
.ev-apply   .ev-dot { background: #409eff; }
.ev-grey    { background: #f5f0ff; color: #7a52c0; }
.ev-grey    .ev-dot { background: #9a7ade; }
.ev-listing { background: #fdf6ec; color: #b57e21; }
.ev-listing .ev-dot { background: #e6a23c; }

.legend {
  display: flex; gap: 20px; padding: 10px 14px;
  border-top: 1px solid #f0f0f5; font-size: 14px; color: #888;
}
.legend span { display: flex; align-items: center; gap: 6px; }
.lg { display: inline-block; width: 10px; height: 10px; border-radius: 3px; }

/* ── 列表 ─────────────────────────────────────────────────────────────── */
.list-card {
  background: #fff; border: 1px solid #e8e8f0; border-radius: 12px;
  overflow: hidden;
}
.empty { padding: 40px 0; }
.list-group { display: flex; border-bottom: 1px solid #f0f0f5; }
.list-group:last-child { border-bottom: none; }
.list-date {
  width: 108px; flex-shrink: 0; padding: 12px 14px;
  font-size: 15px; font-weight: 600; color: #1a1a2e;
  background: #fafbfc; border-right: 1px solid #f0f0f5;
}
.list-date.on { background: #f4f9ff; color: #409eff; }
.list-weekday { display: block; font-size: 13px; font-weight: 400; color: #aaa; margin-top: 2px; }
.list-body { flex: 1; min-width: 0; }
.list-row {
  display: grid;
  grid-template-columns: 54px 1.5fr 72px 62px 1fr 1fr 1fr 76px;
  align-items: center; gap: 8px;
  padding: 10px 14px; border-bottom: 1px solid #f7f7fa;
  font-size: 15px; cursor: pointer; transition: background .12s;
}
.list-row:last-child { border-bottom: none; }
.list-row:hover { background: #fafcff; }
.ev-tag {
  font-size: 13px; padding: 1px 7px; border-radius: 4px; text-align: center;
}
.row-name { font-weight: 600; color: #1a1a2e; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.row-code { font-size: 14px; color: #aaa; font-variant-numeric: tabular-nums; }
.row-market { font-size: 14px; color: #666; }
.row-price { font-variant-numeric: tabular-nums; color: #1a1a2e; }
.row-shares { font-variant-numeric: tabular-nums; color: #666; }
.row-raise { font-variant-numeric: tabular-nums; color: #666; }
.row-status { font-size: 14px; text-align: right; font-weight: 600; }
.st-today  { color: #409eff; }
.st-wait   { color: #e6a23c; }
.st-open   { color: #7a52c0; }
.st-grey   { color: #9a7ade; }
.st-listed { color: #999; }

/* ── 详情 ─────────────────────────────────────────────────────────────── */
.detail-head {
  display: flex; align-items: center; gap: 10px;
  padding-bottom: 14px; border-bottom: 1px solid #f0f0f5; margin-bottom: 14px;
}
.detail-code { font-size: 15px; color: #888; font-variant-numeric: tabular-nums; }
.detail-market {
  font-size: 14px; padding: 1px 8px; border-radius: 4px;
  background: #f0f6ff; color: #409eff;
}
.detail-status { font-size: 14px; font-weight: 600; }
.detail-price {
  margin-left: auto; font-size: 20px; font-weight: 700;
  color: #1a1a2e; font-variant-numeric: tabular-nums;
}
.detail-grid {
  display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1px; background: #f0f0f5; border: 1px solid #f0f0f5; border-radius: 8px;
  overflow: hidden;
}
.detail-cell {
  display: flex; justify-content: space-between; gap: 10px;
  padding: 9px 13px; background: #fff; font-size: 15px;
}
.d-label { color: #888; flex-shrink: 0; }
.d-value { color: #1a1a2e; font-variant-numeric: tabular-nums; text-align: right; }
.detail-foot {
  margin-top: 12px; font-size: 14px; color: #bbb; text-align: right;
}

.skeleton-wrap { padding: 16px 0; }
</style>
