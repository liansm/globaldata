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

/**
 * 存储成品盘 / NAND 颗粒（源 dram.com.cn，与 DRAM 同站不同 sortid）
 *
 * ⚠ `capacityGb` 是**归一化后的 GB**（NAND 的 1Tb 已 ÷8 折成 128GB），
 *   每 GB 价 = `close / capacityGb`。这与 `DramProduct.densityGb`（存 Gb，要 ÷8）
 *   **语义相反**，两个 tab 之间不要互相套公式。
 * ⚠ `currency` 是**按品类校正过**的（站方标记有错），显示要跟着它走：
 *   NAND / SSD OEM 是美元，SSD 零售 / HDD 是人民币。
 * ⚠ `virtual: true` 的 `avg_ssd_street` 是**接口合成的加权均价**，库里没有这一行；
 *   它的 `series[].samples` 是当日参与加权的品种数（浮动篮子，不是固定 66 个）。
 */
export interface StoragePoint {
  date:   string
  close:  number
  perGb:  number | null
  low:    number | null
  open:   number | null
  samples?: number        // 仅均价序列有：当日参与加权的品种数
}

export type StorageCategory = 'nand_wafer' | 'ssd_oem' | 'ssd_street' | 'hdd'

export interface StorageItem {
  key:          string
  name:         string
  displayName:  string
  category:     StorageCategory
  brand:        string | null
  capacityGb:   number | null
  capacityRaw:  string | null
  formFactor:   string | null
  interface:    string | null
  dtype:        string | null
  usage:        string | null
  currency:     'CNY' | 'USD'
  unit:         string          // 'USD/chip' / 'CNY/disk' / 'CNY/GB'
  currencyNote: string | null
  virtual:      boolean
  latestDate:   string | null
  latestClose:  number | null
  latestPerGb:  number | null
  prevClose:    number | null
  changePct:    number | null
  points:       number
  firstDate:    string | null
  series:       StoragePoint[]
}

export interface StorageCategoryMeta {
  key:        StorageCategory
  label:      string
  cadence:    string          // 更新节奏：周级 / 日更 / 季更
  count:      number
  currency:   'CNY' | 'USD' | null
  latestDate: string | null
}

export interface StorageResponse {
  categories: StorageCategoryMeta[]
  items:      StorageItem[]
}
