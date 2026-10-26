/**
 * AI 硬件 —— 存储颗粒现货（DDR5 / DDR4）+ GPU 云租价
 *
 * ⚠ `densityGb` 是**颗粒容量（Gb，Gigabit）**，不是内存条容量（GB）：
 *   DDR5 16Gb 颗粒 → 做成内存条是 2GB 一颗 × 8 颗 = 16GB 一条。
 *   `unit` 恒为 'USD/chip'（美元/颗）。
 *   `perGb` / `latestPerGb` 是接口折算好的**美元/GB**（= close ÷ (densityGb / 8)），
 *   横向比「哪一代更贵」要用它，用「美元/颗」比会被容量差异带偏。
 * ⚠ `grade`: 'prime' 原厂正片 / 'ett' 白牌有效测试。同规格两者差 5~6 倍，不可混算。
 */
export interface DramPoint {
  date:  string
  close: number
  perGb: number | null
  low:   number | null
  open:  number | null
}

export interface DramProduct {
  key:         string          // 'ddr5_24gb_5200'
  name:        string          // 'DDR5 24Gb 5200/6400 正片'
  generation:  'DDR5' | 'DDR4'
  densityGb:   number | null
  speed:       string | null   // '5200/6400'
  grade:       'prime' | 'ett'
  unit:        string          // 'USD/chip'
  source:      string          // 'dram.com.cn'
  series:      DramPoint[]
  latestDate:  string | null
  latestClose: number | null
  latestPerGb: number | null
  latestLow:   number | null
  latestOpen:  number | null
  prevClose:   number | null
  change:      number | null
  changePct:   number | null
  perGbMin:    number | null
  perGbMax:    number | null
  points:      number
  firstDate:   string | null
}

/**
 * GPU 云租价
 *
 * ⚠ 三重口径，页面上必须区分，别混着讲：
 *   ① `latestHourly` / `series` 是**租价**（美元/卡·小时），来源是各家云厂商挂牌价。
 *   ② `unitPriceLow/High` 是**单卡买断参考价**（美元/卡），静态、非日更，
 *      来自媒体转述的市场区间，不是 NVIDIA 官方 MSRP。两者差三个数量级。
 *   ③ `providers` 里的 `priceHourly` 已折算成**每卡**价（整节点报价 ÷ 节点卡数），
 *      `gpusPerNode` 标明是几卡打包 —— 8 卡节点摊下来的每卡价通常更低。
 */
export interface GpuProviderQuote {
  provider:    string
  region:      string
  priceHourly: number
  gpusPerNode: number | null
}

export interface GpuPoint {
  date:  string
  price: number
}

export interface GpuProduct {
  key:          string        // 'h100_sxm'
  name:         string        // 'H100 SXM'
  vendor:       string
  vramGb:       number | null
  bandwidthTbs: number | null
  unitPriceLow:  number | null   // NULL = 尚无公开报价（Rubin）
  unitPriceHigh: number | null
  unitPriceNote: string | null
  unitPriceAsof: string | null
  unitPriceSrc:  string | null
  latestDate:   string | null
  latestHourly: number | null
  prevHourly:   number | null
  changePct:    number | null
  minHourly:    number | null
  maxHourly:    number | null
  points:       number
  firstDate:    string | null
  series:       GpuPoint[]
  providers:    GpuProviderQuote[]
}
