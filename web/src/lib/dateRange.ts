/**
 * 时间区间起点（**唯一出处**）。
 *
 * 「今年来」「本周」都是**自然周期**：今年来 = 本自然年 1 月 1 日起，本周 = 本周一 0 点起。
 * 它们和「近 N 天 / 30 天」那种**滚动窗口**是两回事，不能混 —— 周六选「本周」应看到
 * 周一至周五 5 个交易日（加密货币 24/7 则 6 天），而不是最近 7 天。要滚动窗口就传 days。
 *
 * 刻意**不用数据最新日期的年月/周**：库里有已终止/停更的序列（公募基金、私募产品、
 * 部分商品/指数），快照停在往年或往周。按数据日期算会把 2024 年的曲线标成「今年来」、
 * 把上个月的走势标成「本周」—— 那是粉饰。按系统日期算，这类序列只会得到一张空图，
 * 那是真实的「这段时间没数据」。（同理，1 月初 / 周一早盘数据尚未更新时短暂为空，属实。）
 */

/** 今年来：本自然年 1 月 1 日，'YYYY-MM-DD' */
export function ytdFrom(): string {
  return `${new Date().getFullYear()}-01-01`
}

/**
 * 今天的日期 'YYYY-MM-DD'（Asia/Shanghai）。
 * 口径与后端 `(NOW() AT TIME ZONE 'Asia/Shanghai')::date` 一致 —— 用来判断
 * 「这条快照是不是今天的」，所以不能随手用 new Date().toISOString()（那是 UTC）。
 */
export function todayCn(): string {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(new Date())
}

/**
 * 本周：本周一，'YYYY-MM-DD'（本地时区）。
 * ⚠ getDay() 里**周日返回 0**，直接减 (dow-1) 会把周日算成本周日-…→ 变成下周一，
 *   所以先把 0 映射成 7。
 */
export function wtdFrom(): string {
  const now = new Date()
  const dow = now.getDay() === 0 ? 7 : now.getDay()   // 周一=1 … 周日=7
  const monday = new Date(now.getFullYear(), now.getMonth(), now.getDate() - (dow - 1))
  const mm = String(monday.getMonth() + 1).padStart(2, '0')
  const dd = String(monday.getDate()).padStart(2, '0')
  return `${monday.getFullYear()}-${mm}-${dd}`
}
