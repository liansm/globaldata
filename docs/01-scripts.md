# 抓取脚本使用说明

> 一共 32 个抓取入口（30 个 `fetch_*.py` + `refresh_all.py` + `refresh_private.py`），
> 覆盖 38 张表。本文按「怎么用」写；每个源的覆盖范围、口径、死路见
> [`02-datasources.md`](./02-datasources.md)；字段含义见 [`03-database.md`](./03-database.md)。
>
> 核对时间：2026-11-10。**脚本改动后请同步本文**，权威实现永远以各脚本 docstring 为准。

---

## 一、先看这五条（新手最容易踩的）

1. **必须用系统 Python**：`%LOCALAPPDATA%\Programs\Python\Python313\python.exe`。
   managed venv 里没有 `akshare` / `psycopg2`，用它会直接 `ModuleNotFoundError`。
   下面所有示例里的 `python` 都指这个解释器，为简洁起见不再写全路径。
2. **数据库连接读根目录 `.env` 的 `DATABASE_URL`**（默认 `postgresql://localhost/globaldata`）。
   前端的环境变量在 `web/.env`，两者不要混。
3. **只抓数据源 → 落库**，不做前端渲染。本仓库**没有 drizzle migrations**，
   建表 DDL 内联在各脚本的 `SQL_ENSURE` / `SCHEMA_SQL` 里，用幂等 `CREATE TABLE IF NOT EXISTS`。
4. **绝大多数脚本支持 `--dry-run`**（只抓不写库）。改解析逻辑前先用它验证，别直接打库。
5. **改了脚本必须整段重跑**。进程启动之后再改脚本，该进程跑的还是旧版本 ——
   2026-09-17 实测：进程 14:10 启动、脚本 14:24 被改，21 分钟抓取一行未入库。
   排查「跑了但没数据」时，第一件事是比对**进程启动时间 vs 脚本 mtime**。

---

## 二、一键刷新：`refresh_all.py`

**它自己一行数据都不抓**，只做三件事：打状态（`STATUS_QUERIES`）、串行调度
`FETCH_SCRIPTS`、清库（`CLEAR_TABLES`）。完整说明另见仓库根目录
[`REFRESH_ALL.md`](../REFRESH_ALL.md)（406 行，含耗时表、状态读法、排错表）。

```bash
python refresh_all.py                          # 先看状态 → 跑默认脚本 → 再看状态
python refresh_all.py --status                 # 只看库内现状，纯查询，安全
python refresh_all.py --fetch                  # 只刷新（适合 cron）
python refresh_all.py --fetch --with-vlcc      # 连带 VLCC 船位（+90 秒）
python refresh_all.py --fetch --with-private   # 私募整轮（+35~45 分钟）
python refresh_all.py --fetch --with-financials    # 财报三表（+1~2 分钟）
python refresh_all.py --fetch --with-shareholders  # 十大股东（+40 秒）
python refresh_all.py --clear                  # 交互式清库，需手输 YES
python refresh_all.py --force_clear            # 无确认清库，慎用
```

### 脚本的三种接入状态（别搞混）

| 方式 | 代码位置 | 判据 | 例子 |
|---|---|---|---|
| 默认跑 | `FETCH_SCRIPTS` | 日常必要，耗时可控 | `fetch_markets.py` |
| `--with-<键>` 按需 | `OPT_IN_SCRIPTS` | **单条 ≥60 秒 且 漏跑不丢历史**（快照/覆盖写） | `--with-vlcc` / `--with-licai` / `--with-private` / `--with-financials` / `--with-shareholders` |
| 注释掉 | `FETCH_SCRIPTS` 里被 `#` 掉 | 功能已下线 | `fetch_crypto.py`（2026-10-26 暂时下线） |
| 根本不提 | 只在注释里 | 必须人工盯 / 一次性回填 | `fetch_fund_nav.py --history` |

> 判据的核心是**「漏跑会不会永久缺数据」**。日频滚存类（CTFI / CCFI / 煤炭快讯）漏一天就永久少一天，
> 再慢也得天天跑；快照类漏跑只是位置旧一点，才允许挪进 `OPT_IN_SCRIPTS`。

### ⚠ 两个必须知道的坑

- **退出码恒为 0**，即使所有脚本都失败。挂 cron 时**别用退出码判成败**，
  要 grep 汇总行里的「失败: N」。
- **`--status` 会把状态打印两遍**（`main()` 里少排除了 `args.status`），
  第二遍顶着「刷新后数据库状态」的标题但其实没刷新。不影响数据。

---

## 三、增量策略：四类，没有统一开关

各脚本自己实现。「二次跑增量应为 0 行」**只对 A 类成立**，B/C/D 类行数不变但请求照发。

| 类别 | 机制 | 脚本 |
|---|---|---|
| **A. 真增量** | 查库里 `MAX(date)` / 进度文件，只抓更新的期次 | `bdi` `cement` `ctfi_history` `milk_price` `stock_info` `stock_prices` `stock_reports` |
| **B. 窗口重拉** | 每次重拉一个固定窗口，靠 `ON CONFLICT DO UPDATE` 去重 | `markets` `commodities` `ccfi_history` `ai_hardware` `fx` `vlcc_fleet` |
| **C. 当日滚存** | 源只给「当期」，历史只能靠每天跑一攒，**漏一天永久少一天** | `ccfi` `ctfi` `market_minutes` `commodity_minutes` `fund_nav`(默认) `coal_flash` `coal_intl` |
| **D. 快照覆盖** | 每键一行，每次直接覆盖，无历史 | `index_spot` `commodity_spot` `fx_spot` `stock_fundamentals` |

---

## 四、按域的脚本清单

### 4.1 行情 / 指数

#### `fetch_markets.py` — 全球市场指数 + 沪深港通资金
- **抓什么**：A股 6 指数（上证/深证/沪深300/创业板/科创50/北证50）+ 港股 3 指数
  （恒生/国企/科技）+ 海外 9 指数（道指/纳指/标普/日经/KOSPI/SENSEX/富时100/DAX/CAC40）
  + 沪深港通北向/南向资金。
- **落表**：`market_indices` + `index_prices`（`market` 列区分 `A股`/`港股`/`美股`/`欧洲`/`亚太`/`资金流向`）。
- **增量**：B 类，指数全历史（上限 20 年）每次重拉。
- **耗时**：约 26 秒（20 年全量，一指数一请求，akshare 砍不出增量）。
- **⚠ 资金流向已停抓**：2026-11-02 起 `refresh_all.py` 传 `--skip-flows`，
  因为前端「沪深港通资金流向」组 2026-10-31 下线了。**接口与历史数据全部保留**。
  补历史：`python fetch_markets.py`（不带参数即含资金流向）。

```bash
python fetch_markets.py                # 指数 + 资金流向
python fetch_markets.py --skip-flows   # 只抓指数（refresh_all 默认）
```

#### `fetch_index_spot.py` — A股/港股指数实时快照
- **落表**：`index_spot`（一键一行，9 行）。
- **增量**：D 类快照。`snapshot_kind` 区分 `intraday` / `close`。
- **耗时**：约 1.2 秒。
- **⚠ 盘前跑不写任何东西，这是对的**。三条判据命中任一就跳过写库：价格 ≤ 0 /
  价格等于某历史收盘 / 成交额 = 0。否则会把集合竞价参考价标成「今天实时」。
  **盘前页面看不到「实时」徽章是预期行为**，开盘后重跑即恢复。
- **北证50 走单只兜底**：批量接口（`stock_zh_index_spot_sina`）实测 562 行里 `bj*` 为 0 行，
  所以北证50 用 `hq.sinajs.cn/list=bj899050` 单独取（需 Referer，GBK）。

#### `fetch_market_minutes.py` — A股指数 1 分钟 K 线
- **落表**：`index_minutes`（141,006 行 / 6 个指数 / 2026-04-01 起）。
- **增量**：C 类滚存，只写当日。休市日跑出 0 行是正常的。
- **耗时**：约 13.8 秒。**必须收盘后（15:30 之后）跑**。

### 4.2 航运

#### `fetch_ccfi.py` — CCFI 中国出口集装箱运价指数（周频）
- **源**：上海航运交易所 `https://www.sse.net.cn/index/singleIndex?indexType=ccfi`，
  服务端渲染，plain requests 即可，**无需登录**。
- **落表**：`index_prices`（`market='航运'`，13 条线）。
- **增量**：C 类。每周五发布，**一次返回上期 + 本期两期**，所以漏一周还能补回一周，漏两周就断。
- **耗时**：约 2.9 秒。

```bash
python fetch_ccfi.py            # 抓取 + upsert
python fetch_ccfi.py --dry-run  # 只抓打印，不写库
```

#### `fetch_ccfi_history.py` — CCFI 历史周线回补
- **源**：GreenPacific（`greenpacific.org/ccfi-china-containerized-freight-index/`），
  全序列 JSON embed 在页面 HTML 里。
- **覆盖**：2023-04-21 起 13 条线（2,223 行）。**不是 2002 起全历史**（那部分仍付费墙）。
- **增量**：B 类重拉全序列。

#### `fetch_bdi.py` — BDI 系列干散货 + 油运指数（日频）
- **源**：`akshare.macro_china_freight_index()`，返回最近 5000 个交易日。
- **落表**：`index_prices`，5 条序列：`bdi` `bci` `bsi` `bdti` `bcti`。
- **覆盖**：2006-07 起全历史（22,648 行）。**这是本库少见的「历史能直接回补」的航运源**。
- **增量**：A 类真增量，按序列查 `MAX(price_date)` 只 upsert 更新的行。
- **主动放弃的序列**：`BHMI` 灵便型（只有 1 行 2009-01-28）、`HRCI` 集装箱租船（最后更新 2011-08-23）。

```bash
python fetch_bdi.py --full     # 忽略库内已有数据，全量重写
```

#### `fetch_ctfi.py` — CTFI 中国进口原油运价指数（日频滚存）
- **源**：上航所 `singleIndex?indexType=ctfi`，服务端渲染，无需登录。
- **抓什么**：综合指数 + CT1/CT2/CT4 三条 VLCC 航线，每条给
  **指数点 / WS / 美元每吨 / TCE 美元每天（标准+经济航速）**。
  TCE 是船东每天赚多少钱，是招商轮船/中远海能盈利的一阶代理。
- **落表**：`index_prices`（16 条当日口径序列 + 11 条期间均值序列，共 662 + 616 行）。
- **增量**：C 类，页面只给「本期」，**漏一天永久少一天**。
- **⚠ 口径分层**：当日值（`ctfi_*`）与期间均值（`ctfi_*_month_avg` / `_week_avg`）
  **绝不能拼成一条序列** —— 均值会把波动抹平后再接到点值上，图形失真。
- **⚠ CT4 的「点」列单位其实是「万美元」**（包干运费总额），权重 0% 不参与综合指数，
  上航所给了不同量纲。落库按源里单位原样记。

#### `fetch_ctfi_history.py` — CTFI 历史回补（免费第二路）
- **源**：中华航运网（上航所授权转载）周报 `/Tanker/CTFIWeek/` + 月报 `/Tanker/CTFIMon/`。
- **覆盖**：月报 2021-09 ~ 2026-08（约 5 年）、周报 2025-07 ~ 2026-09（约 14 个月）。
  更早的月报栏目翻页到 4 页为止，未覆盖。
- **增量**：A 类，默认只解析新报告。

```bash
python fetch_ctfi_history.py --status       # 看已入库期次
python fetch_ctfi_history.py --full         # 全重抓
python fetch_ctfi_history.py --save-raw     # 存原始 HTML 便于复核
python fetch_ctfi_history.py --verify       # 抽样核对
```

### 4.3 大宗商品 / 煤炭 / 水泥 / 乳品

#### `fetch_commodities.py` — 大宗商品日线（60+ 品种）
- **源**：上金所（金银现货）、新浪（沪铜/铝/原油、COMEX/LME/NYMEX 外盘）、
  sxcoal（已死，见下）、CCTD（煤炭历史 API，2018 起）。
- **落表**：`commodities`（69 个品种定义）+ `prices`（129,612 行，最早 1997-10-24）+ `fetch_log`。
- **增量**：B 类，主力连续 20 年重拉。
- **耗时**：约 33 秒（60+ 品种 × 20 年，逐品种请求）。
- **⚠ 煤炭段大部分已废**：sxcoal 汾渭 CCI 2026-10 起付费墙化（`result` 全部返回 `"*"`），
  9 条 `coal_*` 序列死在 2026-09-30。港口价改由 `fetch_coal_flash.py` 续接，
  国际 FOB 由 `fetch_coal_intl.py` 补（**都是新建 key，不覆盖原序列**）。
  本脚本现在只保留 CCTD 历史 API 那条线（`cctd_bohai_5500`，2018 起日更）。

#### `fetch_coal_flash.py` — 动力煤港口价（日频滚存）
- **源**：中国太原煤炭交易中心 `ctctc.cn/main/news/flashnewlist.jspx`「市场快讯」
  —— 免费转载 CCI 港口指数与 CCTD 环渤海参考价原文，服务端渲染。
- **抓什么**：CCI 5500/5000/4500（写回**原有** `coal_port_*` key，与 sxcoal 时期重叠 33 天零差异）
  + CCTD 环渤海 5000/4500（覆盖原来口径可疑的首页值）+ 广州港进口场地价 `coal_gz_*`
  + 神华巴图塔车板价 `coal_shenhua_*`。
- **增量**：C 类滚存，快讯只发当天；补历史靠翻页（`--pages N`，列表约 17 页 ≈ 11 个月）。
- **耗时**：约 13.5 秒（5 个关键词各翻自己的页）。

```bash
python fetch_coal_flash.py --status        # 看各序列最新日期
python fetch_coal_flash.py --only cci      # 只抓某一路（cci / cctd / gz / shenhua）
python fetch_coal_flash.py --pages 17      # 回补更多历史
```

#### `fetch_coal_intl.py` — 国际动力煤 FOB（周频，可回补 9 年）
- **源**：CCTD 中国煤炭市场网「国际煤炭」栏目（`list-464-{page}.html`，**GB18030 编码**）。
- **抓什么**：`coal_newc_fob` 澳洲纽卡斯尔 / `coal_richards_fob` 南非理查兹 / `coal_ara_fob` 欧洲三港
  （6000 大卡，美元/吨）。
- **覆盖**：`--pages 24` 可回补 475 期 ≈ 9 年。默认增量只下 3~4 篇（约 5.3 秒）。
- **⚠ 三个源侧坑**：
  1. 日期取正文「截止X月X日」的**数据日期**，不是发布日（周二发、数据截止上周五，用发布日会整体推后 3~5 天）；
  2. 句子里有「较截止…的旧值」第二三个数字，必须 `search` 只取第一个匹配；
  3. 2024-09-24 / 10-08 / 10-15 三期把「美元/吨」笔误写成「元/吨」，数值仍是美元 → 照收，靠量纲闸门挡真正的人民币报价。
- **⚠ `coal_ara_fob` 疑似停更**：自 2025-11-28 起一直 95.88（连续 40+ 期「持平」），
  是原文真实值，但**别再当有效变量用**。

#### `fetch_cement.py` — 水泥价格（CEMPI + P.O42.5）
- **源**：中国水泥网 `index.ccement.com`，两个 form POST，无登录、无 key。
- **落表**：
  - `cement_cempi`（CEMPI 指数，2011-09 起 3,638 行）→ `market_indices` + `index_prices`（只写 close）
  - `cement_po425`（P.O42.5 散装全国均价 元/吨，2013-08 起 3,247 行）→ `commodities` + `prices`
- **增量**：A 类，按序列查 `MAX(price_date)`。
- **⚠ `timeType` 是回看窗口不是粒度**：1=近1月 2=近3月 3=近1年 4=近3年 5=全历史。
  脚本恒传 5，靠 DB 去重。
- **为什么不走期货**：四大所都没有水泥合约，akshare 也没有可用接口
  （`get_qhkc_index` 的水泥指数后端 404，数字水泥网停在 2021-02-22）。

#### `fetch_milk_price.py` — 生鲜乳收购价（周频）
- **源**：农业农村部畜牧兽医局《畜产品和饲料集贸市场价格情况》周报，
  **10 主产省**（河北/山西/内蒙古/辽宁/黑龙江/山东/河南/陕西/宁夏/新疆）算术平均，元/公斤。
- **落表**：`prices.milk_raw`（463 期，2017-07 起）。
- **为什么是这个**：国内**没有牛奶期货**（四大所畜牧类只有鸡蛋/生猪），
  CME 乳品期货在 akshare 与新浪外盘两条免费路都是死的。这就是原奶定价锚。
- **增量**：A 类，按发布日窗口（库中最新采集日 − 20 天）裁期次 + 进度文件跳过已成功 URL。
  日常只 2~3 个请求、秒级。
- **⚠ 索引要两个源取并集**：主站 `sj/index.htm` 的 `message` 数组只保留最近 2500 条，
  且会漏期 → 必须叠加 `xmsyj.moa.gov.cn/jcyj/index_N.htm` 翻页到 24 页。
- **⚠ `200 + body 0 字节` 必须当失败重试**，不能当成「这期没发」。

```bash
python fetch_milk_price.py --full        # 全量 462 期，约 5~8 分钟，可续跑
python fetch_milk_price.py --retry-empty # 重试历史空响应
```

### 4.4 AI 硬件

#### `fetch_ai_hardware.py` — 一个脚本、两个站、三块口径
- **① 存储颗粒**（`dram.com.cn`，美元/颗）：DDR5/DDR4，11 个 key（1,438 行）。
  → `dram_products` + `dram_prices`
- **② 存储成品**（同站不同 `sortid`）：NAND 晶圆 / SSD OEM / SSD 零售 / HDD，
  107 个品种。→ `storage_products` + `storage_prices`（每 GB 价）
- **③ GPU 云租价**（`aimultiple.com/ai-gpus`，美元/卡·小时）：6 型号 × 17 家（90 条）。
  → `gpu_products` + `gpu_prices`
- **增量**：B 类，源站只给**滚动半年**，每次重拉整个半年窗口。
- **⚠ 存储段自带节流**：最多每 3 天跑一次（`STORAGE_INTERVAL_DAYS`，依据
  `storage_products.MAX(updated_at)`）。日常 21 秒（DDR+GPU），存储日约 170 秒。
  **不要改成「一次性不跑了」** —— 超过半年没跑就永久缺一段。
- **⚠ 三个口径陷阱**（详见 `03-database.md`）：颗粒是「美元/颗」不是内存条价；
  `density_gb` 存 **Gb** 要 ÷8，而 `storage.capacity_gb` 存**已归一化的 GB** 不能再 ÷8；
  GPU 是**每卡**时租，源站整机价已 ÷ 卡数。

```bash
python fetch_ai_hardware.py --dram            # 只抓颗粒
python fetch_ai_hardware.py --storage --force-storage   # 强制刷存储成品
python fetch_ai_hardware.py --gpu             # 只抓 GPU
python fetch_ai_hardware.py --status          # 看各段最新日期
python fetch_ai_hardware.py --purge-ddr3      # 清历史 DDR3 行
```

### 4.5 期货快照 / 分时

#### `fetch_commodity_spot.py` — 期货实时快照
- **落表**：`commodity_spot`（22 个品种，一键一行）。D 类快照，`snapshot_kind` 区分 intraday/close。
- **耗时**：约 14.8 秒。无命令行参数。
- **⚠ `futures_foreign_commodity_realtime()` 在当前 akshare 版本里是坏的**
  （`Length mismatch`），外盘品种直接跳过 —— 它们的日线由 `fetch_commodities.py` 提供。

#### `fetch_commodity_minutes.py` — 期货主力 1 分钟 K 线
- **落表**：`commodity_minutes`（143,504 行 / 20 个品种）。C 类滚存。
- **耗时**：约 27.4 秒。
- **⚠ 只写当日**：避免历史被换月合约的隔夜 tick 覆盖。换错品种后重建用 `--backfill`
  （把源侧给的最近约 3 个交易日一起写回）。
- **⚠ 手写 mark 会串种**：`futures_symbol_mark()` 表外的 mark 撞上的就是别的品种
  （甲醇 `mh_qh` 实际是棉花、锌 `xj_qh` 是橡胶）。两个脚本已按官方表逐个核对，
  并加了量级闸门（价格与该品种最新收盘比值须落在 [0.75, 1.25]，否则 `[REJECT]` 不入库）。

### 4.6 外汇

#### `fetch_fx.py` — 人民币汇率（两个口径分开存）
- **① 中间价** `rate_type='mid'` `source='cfets'`：中国货币网，25 对，2006-01 起（80,044 行）。
  每工作日 9:15 发布，周末/节假日无数据。
- **② 即期** `rate_type='spot'` `source='sina'`：19 对，USDCNY 1994-08 起（29,951 行）。
- **③ 即期实时快照**：`fx_spot`（19 行），**不写进日线序列**。
- **落表**：`fx_pairs` + `fx_rates` + `fx_spot`。
- **增量**：中间价近 30 天 + 即期近 7 天（B 类窗口重拉）；`--full` 全历史。
- **耗时**：约 16 秒。
- **⚠ 全表统一口径**：`close` = **1 单位外币兑人民币**。CFETS 对 15 对报「人民币/外币」，
  入库时取倒数。所以**数值上行 = 人民币贬值（红）**，全表唯一。
  `fx_pairs.quote_unit`（日元 100）只管展示倍数，不改语义。
- **⚠ `mid` 只有 close**（官方只给单一中间价），UI 上 OHLC 一律显示「—」，不要用 close 充数。

```bash
python fetch_fx.py --full        # 全历史回补（约 1.5 分钟）
python fetch_fx.py --days 60     # 增量回看 60 天
python fetch_fx.py --no-spot     # 只抓中间价
python fetch_fx.py --status
```

### 4.7 基金（公募）

#### `fetch_fund_nav.py` — 公募基金净值（快照 + 全历史）
- **落表**：`funds` 快照列（列表页用，避免每次扫 `fund_nav`）+ `fund_nav`
  （**3,462 万行 / 27,537 只 / 2001-09 起**）。
- **建表**：`SQL_ENSURE` 建 `fund_nav`；对 `funds` 它是**补列**（`latest_nav` 等 6 列），
  所以 2026-11-10 起脚本里先备了一个最小 `funds` 骨架，否则空库独立跑会 `relation does not exist`。
- **三个数据源，按耗时选**：
  - L1 批量快照（默认）：`rankhandler.aspx`，**4 个请求拿全市场当日净值**，19 秒；
  - L2 全历史（`--history`）：`pingzhongdata/<code>.js`，1 只 1 请求，约 2 小时；
  - L3 补缺口（`--gap`）：`f10/lsjz`，**只能补最近 20 个交易日**（pageSize 被夹到 20）。
- **⚠ 货币基金口径不同**：`nav_kind='money'` 时 `unit_nav` = 万份收益(元)、
  `acc_nav` = 七日年化(%)，且**不写 daily_return**（源侧 [6] 不是日增长率）。**别和普通基金混算**。
- **⚠ 东财 HTTP 514 限流**：非标准码、body 空、**随机失败**。实测串行 0s → 83%、0.5s → 100%、
  8 线程无节流 → 27%。脚本用全局 RateLimiter 限速，且**请求失败绝不写进度文件**
  （否则缺口永久跳过 —— 这个坑已经吃过两次）。
- **⚠ 不要用货币基金默认模式抓**：只看 `netWorthTrend` 会把 901 只货币基金全判成「无数据」。

#### `fetch_funds.py` — 公募名录 + 规模 + 季度持仓（手动）
- **落表**：`funds`（28,348 只）+ `fund_holdings`（**147.6 万行 / 23,041 只**，只前十大重仓）。
- **建表**：2026-11-10 起 `SQL_ENSURE` 里补了这两张表的 `CREATE TABLE IF NOT EXISTS`
  + 逐列 `ALTER ... IF NOT EXISTS`。此前全仓没有这两张表的 DDL，**新建空库会直接报
  `relation "fund_holdings" does not exist`**（在建索引那步就炸，早于任何写入）。
- **⚠ 东财 F10 持仓接口两个坑**：必须带 `ASP.NET_SessionId` cookie
  （裸请求 404，先 GET 一次 `ccmx_<code>.html` 拿 cookie 再带 Referer）；有 514 频控。
- **⚠ 季报只有前十大**，完整持仓仅半年报/年报披露 —— 行业规则，免费源都一样。
- 不进 refresh_all，手动跑：

```bash
python fetch_funds.py --types 股票型,混合型,指数型   # 权益类增量
python fetch_funds.py --years-back 5                # 回补近 5 年持仓
python fetch_funds.py --refresh-scale --full        # 强制全量重写
python fetch_funds.py --bonds                       # 一并抓债券持仓
```

### 4.8 基金（私募）

> 私募和公募**不是一个物种**：公募的「名录/规模/持仓/净值」四层，私募只有**名录**能全量对标。
> 规模/持仓/净值在公开渠道拿不到 —— 是监管口径（《私募投资基金募集行为管理办法》），不是技术问题。

#### `refresh_private.py` — 私募整轮更新（唯一推荐入口）
把「备案增量 + 代销池 / 格上 / 排排网三源净值」串起来，约 35~45 分钟。

```bash
python -u refresh_private.py --status          # 先看各源陈旧天数
python -u refresh_private.py >> .workbuddy/private_refresh.log 2>&1
python -u refresh_private.py --only licai      # 只补某一个源
python refresh_all.py --fetch --with-private   # 连整轮一起跑
```

**它修掉了两个「看起来在跑、实际不更新」的语义坑**：
① `--nav` 跳过所有已有净值产品（净值永久停在最后一次 `--full`）；
② 格上默认只补新进池产品（现加 `--refresh`）。

- **⚠ 排排网需登录态**（`.workbuddy/.cdp_session.json`）。会话失效时**自检后 SKIP 该源**，
  其余三源照常刷新 —— 绝不把匿名态「成功但 0 点」的假数据写进库。
- **⚠ 格上 `netvalue` 自 2026-11-07 起被源站挡成 HTTP 403**（三种出口 + 完整浏览器头均无效），
  该步明确报 FAIL、退出码 2，不再静默 OK。
- **⚠ `--with-private` 已含格上，别和 `--with-licai` 同开**（会跑两遍）。

#### `fetch_private_funds.py` — 中基协备案名录 + 代销池净值（手动，全量重扫）
- **落表**：`private_managers`（18,432 家）+ `private_funds`（251,491 只）+
  `private_fund_nav`（仅天天基金代销池 1,240 只）。
- **耗时**：全量约 55 分钟/遍，**可续跑**。日常改用 `refresh_private.py` 的增量（约 4 分钟）。
- **⚠ 中基协频控**：连续十几次后持续 400/500，**等 30 秒也不恢复**（按 IP 限流）。
  脚本锁间隔 1.2 秒，失败绝不写进度文件。
- **⚠ `page` 是 0-based**。文档曾写「从 1 开始」是错的 —— 那是把频控的 400 误当语义限制，
  代价是系统性漏掉 page 0 那一整页（而 page 0 正是全表最新的 100 条）。
- **⚠ 用 `curl` 打 `gs.amac.org.cn` 一律 400/500，同一 URL 用 requests 就 200**（TLS 指纹）。
  复核探针**别用 curl 下结论**。

```bash
python fetch_private_funds.py                     # 备案全量（约 55 分钟）
python fetch_private_funds.py --nav-only --full   # 代销池净值刷新（约 13 分钟）
python fetch_private_funds.py --status
python fetch_private_funds.py --managers-only      # 只刷管理人
```

#### `fetch_private_nav_licai.py` — 格上理财净值（第三源，免登录）
- **落表**：`private_fund_nav`，`source='licai'`（431 只 / 150,057 行）。
- **覆盖天花板**：公开白名单 **927 只**（参数全被服务端忽略，`is_hotsell=true` 反而缩到 10）
  → 其中 `is_show_nv=1` 的 651 只，能映射到备案名录 830 只（89.5%）。
- **⚠ 响应 `code=1004 / 用户未登录` 但 `result` 里有完整数据** —— 别按失败处理。
- **⚠ 同一 `fund_no` 只允许一个 source**：命中已有净值的产品**整只跳过**，绝不覆盖、绝不拼接。

### 4.9 新股日历

#### `fetch_ipo_calendar.py` — A股 / 北交所 / 港股新股
- **落表**：`ipo_calendar`（4,109 条，`market` 区分三个市场）。
- **四个源合并**：东财 `RPTA_APP_IPOAPPLY`（A股+北交所，含未来排期）+
  财华社 AAStocks（港股招股起始日，全站唯一免费来源）+ AAStocks 新股频道（招股截止/暗盘）+
  东财港股 `ipolist_N.html`（已上市+募资额，52 页 ≈ 2600 只）。
- **增量**：C 类，新股日历自带过去+未来；港股「招股截止/暗盘」有时效性。
- **耗时**：约 6.5 秒。
- **⚠ `stock_xgsglb_em` 的合法 symbol 是「全部股票」**，传「全部A股」直接 KeyError。
- **⚠「发行总数」单位是万股不是股**（按股算会让募资额小 1 万倍）。
- **⚠ 港股募资额只有已上市标的拿得到**，未上市留空、前端显示「—」，**不要用 0 充数**。

```bash
python fetch_ipo_calendar.py --full    # 全量重抓
python fetch_ipo_calendar.py --status
```

### 4.10 VLCC 船队 / 船位

#### `fetch_vlcc_fleet.py` — 中国船东 VLCC 名录
- **落表**：`vlcc_vessels`（99 艘：招商 51 + 中远 48）。
- **两个源**：
  - 招商轮船：`chinashipbuild.com` 在役船队库（142 艘全船队，按 DWT ≥ 20 万剔除阿芙拉 → 51 艘）；
  - 中远海能：官网《本集团自有油轮运力》**2021-06-30 官方 PDF 快照**（48 艘）。
    ⚠ 站点是 F5 WAF，plain curl 拿不到；因为该 PDF 是历史存档件，脚本**内嵌为常量快照**
    `COSCO_VLCC_20210630`，不联网重抓。
- **增量**：B 类整表 upsert。
- **⚠ 口径 = 中国船东，不是挂旗**。这两家的 VLCC 大量挂香港旗/新加坡旗/巴拿马旗/利比里亚旗，
  按「挂中国旗」筛会漏掉绝大多数。`flag` 列只做参考，不参与筛选。
- **⚠ 名录源侧都不给 IMO/MMSI**，由船位脚本 `--learn` 从 AIS 反推回写并把 `verified` 置真。
  `verified=false` 只代表「还没被 AIS 实见过」，**不代表这船不存在**。

#### `fetch_vlcc_position_hifleet.py` — 船位主源（HiFleet 付费）
- **落表**：`vlcc_positions`（3,241 行 / 95 艘 / `source='hifleet'`）。
- **默认不跑**（约 90 秒、95 次逐船查位），用 `--with-vlcc` 或单独跑。
- **⚠ 两个致命单位陷阱**：
  1. `la` / `lo` 单位是**「分」不是「度」**，必须 ÷60（官方原文 纬度 = la/60）。
     不换算会把船放到 60 倍远的地方。
  2. `ti` 是 **UTC+8**，库里 `ts` 存 UTC，必须 −8h。两条船位来源写同一张表，
     **这里串味就静默错 8 小时**。
  3. 同一家 API 的 `gettraffic` 返回 `lat`/`lon` 却是**度** —— 以后换接口必须重看单位。
- **计费**：实测查位 97 次扣 0 点（按小时结算），贵的是时间是钱不是钱。

```bash
python fetch_vlcc_position_hifleet.py                 # 只刷船位，约 90 秒
python fetch_vlcc_position_hifleet.py --selftest      # 用官方样例报文自检单位（不花额度）
python fetch_vlcc_position_hifleet.py --search-only   # 只补 MMSI/IMO
python fetch_vlcc_position_hifleet.py --status
```

#### `fetch_vlcc_position.py` — 船位副源（aisstream.io 免费）
- **⚠ 已降级为副源：覆盖塌陷**。窄 bbox 逐区实测，波斯湾/红海/阿拉伯海/马六甲/
  南中国海/上海长江口**全部 0 条/秒**；全球订阅那 ~43 条/秒里**六成来自北海**。
  用已学到的 MMSI 精确订阅仍是 0 条 → **当地没有接收站，不是限速也不是 key 的问题**。
- 保留作海外段补充与调研存档。手动跑：`python fetch_vlcc_position.py --minutes 10`。

### 4.11 加密货币（2026-10-26 暂时下线）

#### `fetch_crypto.py` — CoinGecko 日线（近 365 天可回补）
- **落表**：`crypto_coins` + `crypto_prices`。
- **状态**：**已从前端导航与 `FETCH_SCRIPTS` 摘除**，但路由/页面/接口/脚本/两张表**全部保留**。
  恢复只需把 `refresh_all.py` 里被注释的那一行加回 + 补回 `STATUS_QUERIES`。
- **⚠ 三条硬约束**：免费档时间窗上限 365 天（`days=366` 直接 401）；
  限流 ~5 次/分钟（10 币约 3 分钟）；**粒度随窗口自动变**（<90 天返小时级）
  → 增量也一律拉满 365 天，靠 upsert 幂等。
- **⚠ 写入顺序必须「先历史、后快照」**，否则今天 00:00 UTC 那个点会覆盖实时价。

### 4.12 A 股股票

> **顺序不能反**：先 `fetch_stock_info.py` 建名录，股价脚本才知道抓哪些代码。
> 全部免费（东财/交易所公开接口），无需 key、无需登录。**不含北交所**（8/4/920 开头）。

#### `fetch_stock_info.py` — 名录 + 静态属性 + 多值标签
- **落表**：`stocks`（5,562 只）+ `stock_industries`（概念 76,664 / 行业 36,696 / 地域 5,213 行）。
- **四个免费源**：东财 `clist`（名录+行情快照，60 页）、`stock/get`（股本/行业/地域）、
  F10 `CompanySurvey`（简介/经营范围/注册资金）、`slist`（多值板块）。
- **增量**：A 类。名录每次全量 UPSERT，逐只详情只挑 `profile_updated_at` /
  `boards_updated_at` 为空的股票（日常约 10 秒）。
- **⚠ `pz` 被服务端静默夹到 100**（传 10000 只返 100 条），必须翻页。
- **⚠ 不用 `f13` 判交易所**：北交所也返回 0，会混进深市。一律按代码前缀分类。
- **⚠ `f124`（`quote_time`）不是成交时刻**：339 只停牌/退市/ST 股返回当日 08:00:00 占位，
  **不能用它判断「最后交易日」**。

```bash
python fetch_stock_info.py --full              # 全量重抓逐只详情（约 20~30 分钟，可续跑）
python fetch_stock_info.py --codes 600519,000001
python fetch_stock_info.py --pinyin-only       # 只补拼音
python fetch_stock_info.py --limit 50 --dry-run
```

#### `fetch_stock_prices.py` — 不复权日线（增量入口）
- **落表**：`stock_prices`（**607 万行 / 5,415 只 / 2021-11 起**，默认近 5 年）。
- **增量**：A 类，从各股 `latest_price_date` 开抓，已到源侧最新日**0 请求**。
- **⚠ 不复权**（`adjust=''`）：除权日有跳空，**禁止与 qfq/hfq 序列拼接**。
  成交量单位「手」、成交额「元」。
- **⚠ 周/月/年线不落库**，由 API 用 `date_trunc` 聚合日线。日线是唯一事实来源。
- **⚠ `price_checked_date` 这个列别删**：退市/长期停牌股的 `latest_price_date` 永远追不上源侧，
  没有这列会每天白抓 345 只（30 秒）。

```bash
python fetch_stock_prices.py --years 5     # 首次回填近 5 年（约 14 分钟）
python fetch_stock_prices.py --full        # 全历史（约 1 小时 / 1600 万行）
python fetch_stock_prices.py --verify      # 与东财快照端点逐字段对照
```

#### `fetch_stock_fundamentals.py` — 每股财务快照
- **落表**：`stock_fundamentals`（5,562 行，一键一行快照）。
- **增量**：D 类覆盖写。**刻意不做「同一行情日就跳过」的守卫** ——
  分红方案会在同一天内新增，跳过就静默漏数据。宁可每次 15 秒。
- **⚠ 股息率自算**：`dividend / price × 100`（近 12 个月**已实施**分红，税前元/股），可验算。
  **不采用东财 `f133`**（口径不统一：茅台只算最近一次 1.93%，平安银行却算近 12 个月 5.16%）。
- **⚠ `PB = price / bvps` 不恒成立**：前 200 只里 5 只（多为科创板，如 688809）偏差 7~28%，
  是源侧 f23 与 f113 的报告期/股本口径差异 → 保留源侧 PB，**不要在 UI 宣称两者可互相验算**。
- **⚠ `eps_ttm`（滚动12月，由 PE 反推）与 `eps_report`（报告期累计未年化）绝不能混用**。
  茅台 2026 三季报累计 51.57，年化后约 68.7。

#### `fetch_stock_reports.py` — 季度财报 PDF 链接（只存链接）
- **落表**：`stock_reports`（175,174 行 / 5,468 只；沪 75,324 + 深 99,850）。
- **增量**：A 类，默认只查最近 20 天新披露（约 70 秒），进度记
  `.workbuddy/stock_reports_progress.json`（**失败任务不记进度，下轮自动补**）。
- **⚠ 上交所翻页必须同时把 `beginPage` 和 `endPage` 设成 pageNo**。只改 `pageNo`
  而把 `beginPage` 固定成 1 时，**每页都返回第 1 页内容**，`total` 还是正确值、不报错 →
  表现是「沪市每年只落 100 行」。2026-11-08 首轮全量就栽在这（沪 932 行 vs 深 99,850 行）。
- **⚠ 深交所必须走 `http://`**：本机代理对 `www.szse.cn:443` 的 HTTPS 隧道会断连
  （`ProxyError` / `RemoteDisconnected`），换 http 立刻 200。别误判成「深交所封 IP」。
- **⚠ 沪市 PDF 直链只存不下载**：脚本 GET 会撞阿里云 WAF 的 JS 挑战页（200 text/html），
  **不是 404、不是链接坏了**，浏览器带 cookie 能正常打开。
- **⚠ `queryCompanyBulletinNew.do` 是死路**（所有参数返回 total=0），
  正确接口是不带 New 的 `queryCompanyBulletin.do`。

```bash
python fetch_stock_reports.py --no-recent --years 10   # 近十年全量（约 16 分钟）
python fetch_stock_reports.py --full                   # 2001 起全历史
python fetch_stock_reports.py --recent-days 30         # 改增量回看窗口
```

#### `fetch_stock_financials.py` — 财报三表 + 股东户数 + 员工数（季度频）
- **落表**：`stock_financials`（**209,413 行 / 5,515 只 / 2016-03 起 / 43 个报告期**）+
  `stock_holder_counts`（398,451 行）+ `stock_employee_snapshots`（5,423 行）。
  另建视图 `v_stock_financials_quarterly`（单季差分，**不落派生表**）。
- **默认增量约 1~2 分钟**（按 `UPDATE_DATE` 回看 14 天）。
  **⚠ 但不是「只在发季报时跑」**：源侧追溯更正全年发生（11-09 当天仍有 2024 年报的修订）
  → 建议挂**周频**，披露窗口（1~4 / 7~8 / 10 月）提到每日。
- **⚠ `pageSize` 硬上限 500**，必须翻页。
- **⚠ 必须按 `stocks` 名录过滤**：源侧混着 `.BJ` / `.NQ` / `A25xxx` 等非目标主体
  （2015 年报 count 高达 19,082）。
- **⚠ `HOLDER_NUM_RATIO` 是百分比数值**（-6.0863 = −6.09%），新股首期可达 1.6e8 →
  numeric(24,4)。用 numeric(12,4) 会在 34 万行处 overflow。
- **⚠ 股东户数写库必须 `with conn:`**，否则静默 rollback。
- **⚠ 不要加进 `CLEAR_TABLES`**：清空后不会自动补回，全量要重跑约 8 分钟。

```bash
python fetch_stock_financials.py                     # 增量（默认回看 14 天）
python fetch_stock_financials.py --full              # 全量 43 期（约 8 分钟）
python fetch_stock_financials.py --since 1990-01-01 --full   # 更早历史
python refresh_all.py --fetch --with-financials
```

#### `fetch_stock_shareholders.py` — 十大股东 / 十大流通股东（季度频）
- **落表**：`stock_shareholders`（**350 万行 / 5,237 只 / 43 期 / 2016-03 起**；
  top 175万 + free 175万）。
- **增量**：缺期 + 最近 2 期，约 40 秒。`--full` 全量 43 期约 30 分钟，可续跑
  （`.workbuddy/stock_shareholders_progress.json`）。
- **⚠ 深分页有硬上限**：只按 `END_DATE` 过滤、靠 pageNumber 翻，到 offset ~5 万行就返回
  「服务器繁忙」并连带限流 → **5.6 万行拿不全**。脚本改为**按代码分批**
  （每批 250 个 `SECURITY_CODE in (...)`）。
- **⚠ filter 语法很挑**：日期用**单引号**、字符串用**双引号**；
  `END_DATE="..."` 不报错但返回 0 行；只支持 `=` 和 `in`。
- **⚠ 两个比例必须分开存**：`total_ratio`（占总股本）vs `float_ratio`（占流通股）。
  中芯国际 2026H1 HKSCC 占流通股 58.56% 但占总股本只有 54.82%。
- **⚠ `hold_change`（数字）与 `change_text`（"不变"/"新进"）分开存** ——
  源侧同一字段混着数字和中文，直接 numeric 化会整批丢值。
- **⚠ 不要加进 `CLEAR_TABLES`**（清了要重跑半小时）。

```bash
python fetch_stock_shareholders.py                     # 增量
python fetch_stock_shareholders.py --full              # 全量（后台跑）
python fetch_stock_shareholders.py --periods 2026-09-30,2026-06-30
python refresh_all.py --fetch --with-shareholders
```

---

## 五、长任务铁律

跑全量回填（股票、基金、私募)时的硬约束，都是踩过坑换来的：

1. **`-u` 无缓冲 + 重定向落日志**，否则中断后什么都看不到：
   ```bash
   python -u fetch_stock_shareholders.py --full > .workbuddy/sh.log 2>&1 &
   ```
2. **进度文件绝不记失败**。记了失败 = 缺口被永久跳过，增量永远补不回来
   （2026-09-10 那次全量跑就静默丢了数千只基金的持仓）。
3. **可续跑**：所有回填脚本都有 `--reset-progress` 或进度文件，重跑从断点继续。
4. **产物行数 vs 进度文件行数的差额要能解释**。解释不了就是有 bug。
5. **进程启动时间 vs 脚本 mtime**：改了脚本必须整段重跑，否则跑的还是旧代码。

---

## 六、新增一个数据源的完整清单

1. 新建 `fetch_<name>.py`，docstring 对齐 `fetch_ccfi.py` 的写法
   （源 URL / 字段映射 / 深度 / 死路 / 合规），支持 `--dry-run`，**默认增量**。
2. 归表：绝对价格 → `commodities` + `prices`；指数 → `market_indices` + `index_prices`；
   **实体 + 时空流水不要硬塞进时间序列表**（参考 `vlcc_vessels` + `vlcc_positions`）。
3. `refresh_all.py` 加 `FETCH_SCRIPTS` + `STATUS_QUERIES`：
   - 耗时 ≥60 秒 **且** 漏跑不丢历史 → 放 `OPT_IN_SCRIPTS`（`--with-<键>` 打开）；
   - 需 API key 的加 `{"requires_env": "XXX"}` → 缺 key 走 **SKIP 不判失败**，
     但原因照样打印，**不静默**。
4. 前端硬编码要手加：`web/src/pages/Markets.vue::SECTIONS` / `Home.vue` /
   `App.vue::navItems` / `router/index.ts`。
5. 验证：实跑 → 二次跑增量 0 行 → `--status` → curl API（**必须加 `--noproxy '*'`**）→ 截图。
6. 写进 `02-datasources.md` 对应域的一节（状态 / 口径 / 死路 / 待拍板）。
