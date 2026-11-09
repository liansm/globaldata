# refresh_all.py 使用文档

> 最后核对：2026-11-08（以 `refresh_all.py` 源码 + 各 `fetch_*.py` docstring 为准；脚本改动后请同步本文）

`refresh_all.py` **本身不抓任何数据**，它只做三件事：

1. **看状态** —— 按表打印行数 / 最新日期 / 覆盖率（`STATUS_QUERIES`）
2. **串行调度** —— 按 `FETCH_SCRIPTS` 顺序 `subprocess` 调用 **24 个** `fetch_*.py`，逐个打印耗时与成败
   （另有 `OPT_IN_SCRIPTS` 里的按需脚本默认不跑，见第五节）
3. **清库** —— 按依赖顺序 `TRUNCATE`（`CLEAR_TABLES`）

---

## 一、快速上手

### 1. 前置条件

| 项 | 要求 |
|---|---|
| Python | **系统** `%LOCALAPPDATA%\Programs\Python\Python313\python.exe`（装了 akshare / psycopg2 / requests / python-dotenv） |
| 数据库 | `DATABASE_URL`（默认 `postgresql://localhost/globaldata`），读根目录 `.env` |
| API key | 只有船位需要 `HIFLEET_API_KEY`；缺了对应脚本走 **SKIP**（不判失败） |

> ⚠ 用 `python refresh_all.py` 直接报 `ModuleNotFoundError: psycopg2`，说明命中的是 managed venv（无 akshare / psycopg2）。
> 用全路径调系统解释器即可。

### 2. 命令

```bash
# 默认：先看状态 → 跑全部 24 个脚本 → 再看一次状态（日常约 6~10 分钟；
#   含 A 股股价增量与财务快照；首次回填须先手动跑股票脚本，见第二节）
python refresh_all.py

# 只看库内现状，不抓（纯查询，安全）
python refresh_all.py --status

# 只刷新，不打印状态（适合 cron）
python refresh_all.py --fetch

# 刷新，并额外跑 VLCC 船位（约 90 秒，默认跳过）
python refresh_all.py --fetch --with-vlcc

# 私募整轮更新：备案增量 + 代销池/格上/排排网三源净值（约 35~45 分钟）
python refresh_all.py --fetch --with-private
# 也可以单独跑（推荐，日志更好读）
python -u refresh_private.py --status                 # 先看各源陈旧天数
python -u refresh_private.py >> .workbuddy/private_refresh.log 2>&1
# ⚠ --with-private 已包含格上（--refresh 模式），别和 --with-licai 同开（会跑两遍）

# 清空数据（交互式，需手输 YES）
python refresh_all.py --clear

# 清空数据（无确认，慎用）
python refresh_all.py --force_clear
```

`--status` 与 `--fetch` 同时给 = 什么都不做时的相反面（两者都显式指定则只剩「空跑」），日常别这么用。

### 3. 典型用法

```bash
# 每日收盘后（A 股 15:30 之后、外盘/航运数据发布后）跑一轮
%LOCALAPPDATA%\Programs\Python\Python313\python.exe refresh_all.py --fetch

# 工作日盘中想更新指数/期货快照与分时，只想看状态
python refresh_all.py --status
```

---

## 二、会抓哪些数据（24 个脚本，按执行顺序）

### 行情 / 指数

| # | 脚本 | 抓什么 | 落表 |
|---|---|---|---|
| 1 | `fetch_markets.py` | A股 6 指数（上证/深证/沪深300/创业板/科创50/北证50）+ 港股 3 指数（恒生/国企/科技）+ 海外 9 指数（道指/纳指/标普/日经/KOSPI/SENSEX/富时100/DAX/CAC40）+ 沪深港通北向/南向资金 | `market_indices` `index_prices` |
| 2 | `fetch_index_spot.py` | A股 + 港股指数**实时快照**（新浪） | `index_spot` |
| 3 | `fetch_market_minutes.py` | A股 6 指数**当日 1 分钟 K 线** | `index_minutes` |

### 航运

| # | 脚本 | 抓什么 | 落表 |
|---|---|---|---|
| 4 | `fetch_ccfi.py` | CCFI 中国出口集装箱运价指数（上航所，每周五，**一次返回上期+本期两期**） | `index_prices` |
| 5 | `fetch_ccfi_history.py` | CCFI 13 条线历史周线（GreenPacific，2023-04 起） | `index_prices` |
| 6 | `fetch_bdi.py` | BDI / BCI / BSI / BDTI / BCTI（akshare，2006-07 起全历史） | `index_prices` |
| 7 | `fetch_ctfi.py` | CTFI 中国进口原油运价指数 + VLCC 各航线 指数点 / WS / 美元每吨 / **TCE 美元每天** | `index_prices` |
| 8 | `fetch_ctfi_history.py` | CTFI 历史回补（中华航运网 月报 2021-09 起 / 周报 2025-07 起） | `index_prices` |

### 大宗商品 / 建材 / AI 硬件

| # | 脚本 | 抓什么 | 落表 |
|---|---|---|---|
| 9 | `fetch_commodities.py` | 60 个品种日线：上金所 + 国内期货主力连续 + 外盘（煤炭段已废，见 9b） | `commodities` `prices` |
| 9b | `fetch_coal_flash.py` | **动力煤港口价**：汾渭 CCI 港口指数 5500/5000/4500 + CCTD 环渤海参考价 5000/4500 + **广州港进口煤场地含税价（澳洲5500/印尼4500）** + **神华巴图塔外购车板价（5500/5000/4500）**（ctctc.cn 市场快讯转载） | `prices`（港口沿用原 `coal_port_*`，进口/车板为新建 key） |
| 9c | `fetch_coal_intl.py` | **国际动力煤 FOB（美元/吨，周频）**：澳洲纽卡斯尔 / 南非理查兹港 / 欧洲三港，CCTD「国际煤炭」栏目周报 | `prices`（`coal_newc_fob` / `coal_richards_fob` / `coal_ara_fob`） |
| 10 | `fetch_cement.py` | CEMPI 水泥价格指数 + P.O42.5 均价（元/吨） | `index_prices` `prices` |
| 10b | `fetch_milk_price.py` | **生鲜乳收购价（元/公斤，周频）**：农业农村部畜牧兽医局《畜产品和饲料集贸市场价格情况》周报的 10 主产省均价。国内**没有牛奶期货**，这是原奶定价锚。双索引取并集（主站 message 数组 + `xmsyj.moa.gov.cn/jcyj` 翻页），进度文件续跑 | `prices`（`milk_raw`） |
| 11 | `fetch_ai_hardware.py` | ① DDR5/DDR4 颗粒现货（库里 11 个 key）② NAND/SSD/HDD 成品（110+ 品种，每 GB 价）③ GPU 云租价（6 型号 × 17 家，美元/卡·小时） | `dram_*` `storage_*` `gpu_*` |
| 12 | `fetch_commodity_spot.py` | 期货实时快照 | `commodity_spot` |
| 13 | `fetch_commodity_minutes.py` | 期货主力当日 1 分钟 K 线（20 个品种） | `commodity_minutes` |

### 外汇 / 基金 / 新股 / 船位

| # | 脚本 | 抓什么 | 落表 |
|---|---|---|---|
| 14 | `fetch_fx.py` | ① CFETS 人民币中间价 25 对（2006-01 起）② 新浪即期 19 对（USDCNY 1994 起）③ 即期实时快照 | `fx_pairs` `fx_rates` `fx_spot` |
| 15 | `fetch_fund_nav.py` | 公募基金**当日净值快照**（4 个请求拿全市场）+ 回写 `funds` 快照列 | `funds` `fund_nav` |
| 16 | `fetch_ipo_calendar.py` | 新股日历：A股 + 北交所（东财全量含未来排期）、港股 3 源合并 | `ipo_calendar` |
| 17 | `fetch_vlcc_fleet.py` | 中国船东 VLCC 名录（招商轮船在役 + 中远海能 2021-06 快照，99 艘） | `vlcc_vessels` |

### A 股股票（2026-11-08 新增，4 个脚本）

| 脚本 | 抓什么 | 落表 |
|---|---|---|
| `fetch_stock_info.py` | 东财 clist 全市场名录 + F10 公司资料 / `stock/get` 股本 / `slist` 多值板块。**默认增量**：逐只详情只跑 `profile_updated_at` / `boards_updated_at` 为空的股票（日常约 10 秒） | `stocks` `stock_industries` |
| `fetch_stock_prices.py` | `stock_zh_a_hist` 不复权日线（开高低收 + 成交量/成交额）。**默认增量**：从各股 `latest_price_date` 开抓，已到源侧最新日 0 请求；周/月/年线由 API 聚合日线，不落派生表 | `stock_prices` |
| `fetch_stock_fundamentals.py` | 每股财务快照：EPS(TTM) / PE(TTM·动态·静态) / 每股净资产 / 市净率 / 股息 / 股息率。全市场批量约 15 秒、覆盖写不产生新行；股息率按「近 12 个月已实施分红 ÷ 最新价」自算 | `stock_fundamentals` |
| `fetch_stock_reports.py` | **季度财报 PDF 链接**（Q1/中报/Q3/年报）：上交所 `queryCompanyBulletin.do` + 深交所 `annList`，只存链接不下载正文。**默认增量**只查最近 20 天新披露（约 70 秒），按 (code, 年, 季度) upsert | `stock_reports` |

> ⚠ **顺序不能反**（先名录后股价），首次全量要手动跑：
> `python fetch_stock_info.py`（约 10 分钟 / 5562 只）→ `python fetch_stock_prices.py --years 5`（约 14 分钟 / 约 640 万行）。
> 全历史 `python fetch_stock_prices.py --full` 另需约 1 小时、约 1600 万行。
> 财报链接首次全量另跑：`python fetch_stock_reports.py --no-recent --years 10`（近十年，约 16 分钟、17.5 万行，可续跑）。
> 数据源全免费（东方财富公开接口），口径与死路见 `.workbuddy/memory/DATASOURCES.md`「A 股股票信息」节。

> **原第 18 条 `fetch_vlcc_position_hifleet.py`（船位）2026-10-30 起默认不跑**，理由是耗时（实测 80.7s ≈ 整轮 16%）。
> 船位是「最新位置」快照、每次跑覆盖同一条 `(name_ais, ts)` 记录，**漏跑不丢历史**，
> 时效要求不如日频滚存序列（后者漏一天永久少一天），没必要每天跟着整轮 6 分钟走。
> 要看最新船位：
> ```bash
> python fetch_vlcc_position_hifleet.py          # 只刷船位，约 90 秒
> python refresh_all.py --fetch --with-vlcc      # 连整轮一起跑
> ```
> 缺 `HIFLEET_API_KEY` 时打印 `[SKIP]` 跳过、**不计失败**；实测查位 0 积分（余额 996→996），贵的是时间不是钱。
>
> 读状态时注意：`--status` 里 `today_rows` 才是「今天抓到多少行」，`last_write` 是 `max(created_at)`，
> 同一 `(name_ais, ts)` 重复命中只 UPDATE 不动 `created_at`，所以它可能停在前一天。
> 船位报文时间由 HiFleet 决定，早间跑可能拿到的还是前一晚的报文 —— 跑了但没新行。
> **现在默认不跑，`today_rows` 长期为 0 属预期**，不是坏了。

---

## 三、到底是不是增量抓取？

**结论：没有一个统一开关。各脚本自行实现，而且只有 7 个是「真增量」，其余靠「重拉固定窗口 + upsert 幂等」。**

按抓取方式分四类：

### A. 真增量 —— 先查库里 `MAX(date)` / 进度文件，只抓更新的期次（7 个）

| 脚本 | 判断依据 |
|---|---|
| `fetch_bdi.py` | 按序列查 `MAX(price_date)`，只 upsert 严格更新的行（源一次返回 ~5000 交易日全历史，但不重写） |
| `fetch_cement.py` | 同上，按序列查 `MAX(price_date)` |
| `fetch_ctfi_history.py` | 增量默认只解析**新报告**（`--full` 才全重抓） |
| `fetch_milk_price.py` | 按**发布日窗口**（库中最新采集日 − 20 天）裁期次 + `.workbuddy/milk_progress.json` 跳过已成功的 URL。周频，日常只 2~3 个请求 |
| `fetch_stock_info.py` | 名录每次全量 UPSERT，但逐只详情只挑 `profile_updated_at` / `boards_updated_at` 为空的股票（即新上市或上次失败的）；时间戳即「已入库」进度 |
| `fetch_stock_prices.py` | 按各股 `stocks.latest_price_date` 开抓（重抓最新日覆盖），`src_latest` 来自 clist f297；已到最新日的股票 **0 请求**。漏跑几天由 hist 从旧日期起一次性补回 |
| `fetch_stock_reports.py` | 默认只查最近 20 天新披露公告（`--recent-days`），按 (源, 关键字, 窗口) 记 `.workbuddy/stock_reports_progress.json`；`--no-recent` 才按年回填。失败任务不记进度、下轮自动补 |

### B. 窗口重拉 —— 每次重新拉一个固定窗口，全靠 `ON CONFLICT DO UPDATE` 去重（6 个）

不省请求，只保证不产生重复行；数据量靠 `HISTORY_LIMIT` 截断。

| 脚本 | 每次重拉的窗口 |
|---|---|
| `fetch_markets.py` | 指数全历史（上限 20 年）+ 资金流向全历史 |
| `fetch_commodities.py` | 主力连续 20 年（`start_date = now - 365*20`） |
| `fetch_ccfi_history.py` | GreenPacific 全序列（2023-04 起 13 线） |
| `fetch_ai_hardware.py` | 源站**只给滚动半年**，每次重拉整个半年窗口 → 存储段因此可以 3 天一次（见第四节） |
| `fetch_fx.py` | 中间价近 30 天（`--days` 可调）+ 即期日线近 7 天 |
| `fetch_vlcc_fleet.py` | 名录整表 upsert |

### C. 当日滚存 —— 源只给「当期」，历史只能靠每天跑一攒（6 个）

**漏一天就永久少一天**，这类最需要定时跑。

| 脚本 | 说明 |
|---|---|
| `fetch_ccfi.py` | 每周五发布；一次给上期+本期两期，**漏一周仍能补回一周**，漏两周就断 |
| `fetch_ctfi.py` | 日频，页面只给当期（历史是登录墙） |
| `fetch_market_minutes.py` | 只写当日 1 分钟 K 线（休市日跑出 0 行是正常的） |
| `fetch_commodity_minutes.py` | 只写当日分时（避免历史被换月合约的隔夜 tick 覆盖）；`--backfill` 可把源侧给的最近 ≈3 个交易日一起写回（换错品种后重建用） |
| `fetch_fund_nav.py`（默认模式） | 只写当日净值快照；全历史要 `--history`（约 2 小时） |
| `fetch_ipo_calendar.py` | 新股日历本身自带过去+未来，不算严格滚存，但港股「招股截止/暗盘」有时效性 |
| `fetch_coal_flash.py` | 港口/进口/车板价快讯只发当天，历史靠翻 ctctc 站点分页回补（`--pages N`，列表约 17 页到底 ≈ 11 个月） |
| `fetch_coal_intl.py` | CCTD 国际煤炭周报每周二发、数据截止上周五；全量 `--pages 24` 可回补 475 期 ≈ 9 年 |

### D. 快照覆盖 —— 每键一行，每次直接覆盖（4 个）

`fetch_index_spot.py` / `fetch_commodity_spot.py` / `fetch_fx.py` 的 `fx_spot`：
表里永远只有一行/一品种，看 `updated_at` 与 `quote_time` 判断新鲜度，**没有历史**（历史在对应的日线表里）。

`fetch_stock_fundamentals.py` 也是这一类：5562 行财务快照每次整批覆盖、不产生新行。
**刻意不做「同一行情日就跳过」的守卫** —— 分红方案会在同一天内新增，跳过就静默漏数据，
宁可每次 15 秒。二次复跑行数不变（5562）、`updated_at` 刷新。

#### 两个抓取时点上的坑（2026-10-30 实测）

- **美股日线要等次日 09:00 之后才有。** 10-29 美股收盘是北京时间 10-30 04:00，但新浪的
  美股日线到 09:00 左右才出现有 10-29 的那一根。06:56 跑 refresh 只拿到 10-28，
  **不是脚本坏了，是时间点太早**。要么把定时任务放到 09:30 后，要么盘前那次跑完再补一次。
- **盘前抓 `fetch_index_spot.py` 写不进任何东西，这是对的。** 新浪 spot 接口不带日期，
  开盘前返回的「最新价」是**集合竞价参考价**（实测上证 3651.4075 vs 昨收 3651.4340）、
  成交额为 0，深证成指/创业板指甚至直接给 0。旧版把这些原样记成 today，
  卡片就会显示「2026-10-30 · 实时」而数字是 10-29 的。现在三条判据（价格 ≤ 0 /
  价格等于某历史收盘 / 成交额 = 0）命中任一条就跳过，页面回落到日线。
  **所以盘前看页面看不到「实时」徽章是预期行为，开盘后重跑即恢复。**

---

## 四、耗时与失败语义

- **串行执行**，顺序 = `FETCH_SCRIPTS` 顺序；单个脚本失败**不影响后续**，最后统一打 `刷新汇总`。
- **2026-11-08 起多四个 A 股脚本**：日常增量下 `fetch_stock_info.py` 约 10 秒（只写名录）、
  `fetch_stock_fundamentals.py` 约 15 秒（全市场批量覆盖写）、`fetch_stock_reports.py` 约 70 秒
  （近 20 天公告，深交所一年窗口要翻几百页）、
  `fetch_stock_prices.py` 在全市场已到最新日时约 2 秒（0 请求）；隔了一个交易日才跑时后者
  约 5~8 分钟（逐只 hist 并发 12 线程、天然补洞）。下面 2026-10-30 的实测秒数**不含这四项**。
- **改前实测 8 分 14 秒**（2026-10-30 全量跑，20 个脚本合计 493.7s，0 失败）。
- **改后实测 3 分 45 秒**（同日再跑，19 个脚本 224.9s，0 失败）——即「非存储日」。
  存储日（每 3 天一次）再加上存储段的 ~140s，约 **6 分钟**；平均 ≈4.5 分钟。
  三项改动：船位移出默认刷新（-80.7s）、国际煤逐篇过滤（-42s）、AI 硬件存储段 3 天节流（-147s）。
- 真实耗时不用估：跑一次 `--fetch`，末尾汇总表就是每个脚本的实测秒数。

### 实测耗时（2026-10-30，按耗时降序）

| 脚本 | 秒 | 为什么慢 / 能不能砍 |
|---|---:|---|
| `fetch_ai_hardware.py` | 168.2 → **21** | 存储成品 **115 个品种 = 115 个详情请求**（140s）。**已加脚本内节流：最多每 3 天跑一次**（`STORAGE_INTERVAL_DAYS`，判断依据 `storage_products.MAX(updated_at)`）；DDR + GPU 仍日更（21s） |
| `fetch_vlcc_position_hifleet.py` | 80.7 | 95 次逐船查位 —— **已移出默认刷新**（`--with-vlcc` 才跑） |
| `fetch_coal_intl.py` | ~~47.7~~ → **5.3~5.7** | 旧版每天把 40 篇文章全下（周报 20 条/页 ≈ 20 周，页级停点永不触发）。**已改为按发布日逐篇过滤**，只下 3~4 篇 |
| `fetch_commodities.py` | 33.4 | 60+ 品种 × 20 年，逐品种请求；属「窗口重拉」，慢但必要 |
| `fetch_commodity_minutes.py` | 27.4 | 20 个品种当日 1 分钟 K 线，一品种一请求 |
| `fetch_markets.py` | 25.9 | 20 年全量重拉，一指数一请求（akshare 只给全量，砍不出增量） |
| `fetch_fund_nav.py` | 19.4 | 4 个批量请求拿全市场，慢在天天数仓响应 |
| `fetch_fx.py` | 16.1 | 中间价 25 对 + 即期 19 对（每对 1 请求、返回全历史） |
| `fetch_commodity_spot.py` | 14.8 | 期货实时快照，一品种一请求 |
| `fetch_market_minutes.py` | 13.8 | A 股指数分时 1 分钟 |
| `fetch_coal_flash.py` | 13.5 | ctctc 快讯 5 个关键词 × 翻页到停点 |
| `fetch_vlcc_fleet.py` | 8.4 | 名录 2 源 |
| `fetch_ipo_calendar.py` | 6.5 | 港股 3 源合并 |
| `fetch_ctfi_history.py` | 5.7 | 历史回补（增量时只查新增期） |
| 其余 6 个（`index_spot` / `ccfi_history` / `cement` / `bdi` / `ccfi` / `ctfi`） | 1.2~2.9 | 秒级，不用管 |

### 再想快？剩下这些不建议动

1. **`fetch_ai_hardware.py` 存储段已节流（-140s 已拿到）** —— 2026-10-30 按「降频」方案落地：
   最多每 3 天跑一次，判断依据是库里 `storage_products.MAX(updated_at)`，
   表空 / 源站 0 写入 → 下一轮自动重跑，不会被卡死；`--force-storage` 可强制。
   ⚠ 仍然**不要改成「一次性不跑了」**：源站只给滚动半年，超过半年没跑就永久缺一段。
   想再快的唯一剩余手段是 3~4 线程并发（140s → 40s），但那是对小站并发抓取，先跟对方确认再动。
2. 前 6 名之外全是秒级，再压收益极低。整轮的统计误差（每个子进程启动 +0.8s）都比它们大。
3. **别为了省时间把「当日滚存」类脚本降频**（`fetch_ctfi` / `fetch_ccfi` / `fetch_coal_flash`）：
   源侧只给「当期」，**漏一天就永久少一天**，跑得慢也得天天跑。
   能降频的只有「重拉固定窗口」那类（源站每次给整段窗口，隔几天跑照样全补齐）。

> ⚠ **`refresh_all.py` 的退出码恒为 0**，即使脚本全失败也一样（失败只体现在汇总里的红字）。
> 若挂 cron / 任务计划，**别用退出码判断成败**，要 `grep` 汇总行里的「失败: N」或自行解析日志。

---

## 五、状态输出怎么读

`--status` 按表分组，每组给「行数 / 键数 / 最早日期 / 最新日期」。以下是**看着像坏了、其实正常**的情形：

| 现象 | 真实原因 |
|---|---|
| `storage_prices` 里 `[hdd]` 的 latest 落后一两个月 | **HDD 是季更**，上游不调价，不是抓漏 |
| `index_prices · CCFI` 的 earliest 是 2023-04 | 上航所历史**付费墙**，免费只有当期；更早靠 GreenPacific 才到 2023-04 |
| 「北向资金」在 2024-08-19 后没有新值 | 港交所 2024-08-19 起**停止披露**北向每日净买入额 |
| `CTFI` 分两个块（当期 / `_avg` 期间均值） | 口径不同（日频 vs 月均/周均），**不能混着画** |
| `flow_north` 有缺口 | 同上，源侧停更 |
| `commodity_minutes` 休市日不增长 | 只写当日分时 |
| `dram_prices` 的 `earliest` 只到半年前 | 源站滚动半年窗口，属正常；跑满 125 个报价日/品种才算覆盖完整 |
| `coal_import_5500/4700/3800`、`coal_import_3800_fob/4700_fob`、`coal_ordos_5500`、`coal_datong_5500`、`coal_yulin_5800` 停在 2026-09-30 | 原源 sxcoal **付费墙化**（CCI 数值全返回 `*`）。这些是汾渭**自编指数**，无免费等价源，**不是抓取坏了**。港口 3 档已换 ctctc 源救活；进口/产地/FOB 用**近似口径新建 key** 顶上（`coal_gz_*` / `coal_shenhua_*` / `coal_*_fob`，见 `DATASOURCES.md`），未覆盖原序列 |
| `private_fund_nav` 只有几万只有净值里的 **6,975 只 / 2.81%** | 《私募证券投资基金运作指引》第六条把净值展示权绑定代销协议 / 合格投资者，这是**制度性上限**，不是抓漏。三源并集就这么多；**买付费源也不等于全量**（管理人自愿报送，且「买了」≠「有权在网页展示」），采购前必须用自己的目标清单打覆盖率探针 |
| `private_fund_nav` 按源看 `stale_days` 不一致 | 披露频率本就不同（sppw 周频 / em 日频 / licai 日周混合）。**看各自的数字，别看全表 MAX**；任一源 > 10 天就该跑 `--with-private` |

### ⚠ 已知小 bug

**`--status` 会把状态打印两遍**，且第二遍顶着「刷新后数据库状态」的标题（其实没有刷新）。
原因在 `main()`：最后一块 `if show_status and not args.fetch:` 没有排除 `args.status`。
不影响数据，只是输出重复 + 标题误导；想修就把该条件改成 `if show_status and not args.fetch and not args.status:`。

---

## 六、未接入 refresh_all 的脚本（务必手动跑）

耗时长或已下线，**故意不放进每日刷新**：

| 脚本 | 用途 | 建议命令 |
|---|---|---|
| `fetch_fund_nav.py` | 净值全历史回填（约 2 小时，可断点续跑） | `--history` |
| `fetch_fund_nav.py` | 补 L1 覆盖不到的品种（非 ETF / 定开等） | `--gap` |
| `fetch_funds.py` | 基金规模 / 季度持仓 | `--types 股票型,混合型,指数型` / `--years-back 5` |
| `fetch_private_funds.py` | 私募备案**全量重扫**（约 55 分钟 / 遍，可续跑） | 月度兜底 `--passes 2`；日常改用 `refresh_private.py` 的 4 分钟增量 |
| `fetch_private_funds.py` | 代销池净值**刷新**（约 13 分钟） | `--nav-only --full`；⚠ `--nav` 只补新进池，已有产品**永远不更新** |
| `fetch_crypto.py` | 加密货币（**2026-10-26 已暂时下线**） | 直接跑 / `--no-history` 只写当日快照 |
| `fetch_vlcc_position_hifleet.py` | **HiFleet 版船位（2026-10-30 移出默认刷新，约 90 秒）** | 直接跑，或 `refresh_all.py --fetch --with-vlcc` |
| `fetch_vlcc_position.py` | aisstream 版船位（**区域覆盖塌陷，已降副源**） | `--minutes 10` |
| `.workbuddy/load_simu_nav.py` | 排排网净值落库（需 `.cdp_session.json`） | 别手动跑，交给 `refresh_private.py` |
| **`refresh_private.py`** | **私募整轮更新（备案增量 + 三源净值 + 可选管理人）** | 单独跑，或 `refresh_all.py --fetch --with-private`；`--status` 看新鲜度 |
| **`fetch_stock_financials.py`** | **A 股财报三表 + 股东户数 + 员工数（季度频，2026-11-09 新增）** | 直接跑＝增量；首次 `--full`；或 `refresh_all.py --fetch --with-financials` |

> **2026-11-07 起私募有统一入口了**：以前这一域所有脚本都不在日常刷新里，净值停在
> 10-30、备案停在 9-16、管理人停在 9-17 都无人察觉。`refresh_private.py` 把四条链路
> 串起来，并修掉两个「看起来在跑、实际不更新」的语义坑：
> ① `--nav` 跳过所有已有净值产品（净值永久停在最后一次 `--full`）；
> ② 格上默认只补新进池产品（现加 `--refresh`）。
> 排排网需登录态，会话失效时**自检后 SKIP 该源**，其余三源照常刷新，
> 绝不把匿名态「成功但 0 点」的假数据写进库。细节见脚本 docstring。
> ⚠ 格上 `netvalue` 自 2026-11-07 起被源站反爬挡成 HTTP 403（三种出口 + 完整浏览器头
> 均无效，非本机可修），该步会明确报 FAIL、不再静默 OK；其余三源照常刷新完毕。
> 隔段时间 `python refresh_private.py --only licai` 重跑即可（`--stale-days 3` 接着补）。
> 另：`--nav-only --full` 已改为「只跳过其他源占用的 fund_no」，防止代销池刷新
> 撞上 sppw/licai 备案号造成混源（11-07 的 `SAUW09` 事故，已清理）。

> **2026-11-09 起财报三表**：季度频数据，全量约 8 分钟 / 43 个报告期（2016 起），
> **不进 `FETCH_SCRIPTS`**。⚠ 但也**不是「只在发季报时跑」**：源侧追溯更正全年发生
> （11-09 当天仍有 2024 年报的 `UPDATE_DATE` 修订），默认增量按 `UPDATE_DATE` 回看
> 14 天（约 1~2 分钟），建议挂**周频**定时任务，披露窗口（1~4 / 7~8 / 10 月）提到每日。
> 已进 `OPT_IN_SCRIPTS --with-financials` 与 `STATUS_QUERIES`（三段），
> **没进 `CLEAR_TABLES`**（清了不会自动补回，得重跑全量）。
> 单季净利润由 `v_stock_financials_quarterly` 视图 LAG 差分，不落派生表；
> 口径与死路见脚本 docstring / `DATASOURCES.md`。

**三种「不接入」的方式别搞混：**

| 方式 | 代码位置 | 典型场景 | 例子 |
|---|---|---|---|
| 注释掉 | `FETCH_SCRIPTS` 里被 `#` 掉的整行 | 功能已下线 | `fetch_crypto.py` |
| **`OPT_IN_SCRIPTS`** | 独立 dict，`--with-<键>` 打开 | **单条 ≥60 秒、且漏跑不丢历史**（快照/覆盖写） | `vlcc` → `--with-vlcc`；`licai` → `--with-licai`；`private` → `--with-private` |
| 根本不提 | 只写在注释里 | 一会儿就跑完 / 必须人工盯 | `fetch_fund_nav.py --history` |

判据：**「慢 + 漏跑不丢数据」才进 `OPT_IN_SCRIPTS`**。像 DRAM 现货那种只有滚动
半年窗口的（漏一天永久缺一天），再慢也得留在 `FETCH_SCRIPTS` 里日更。

新增按需脚本：在 `OPT_IN_SCRIPTS` 加一条 `"键": (脚本名, 说明, [参数], {选项})`，
再在 `main()` 里 `parser.add_argument("--with-<键>", action="store_true")`
（`main()` 用 `getattr(args, "with_" + k)` 自动收，不改循环）。

恢复加密货币接入：把 `refresh_all.py` 里被注释掉的
`("fetch_crypto.py", "…")` 一行加回 `FETCH_SCRIPTS`，并补回 `STATUS_QUERIES` 的 `crypto_*` 两块。

---

## 七、清库（`--clear` / `--force_clear`）

按依赖顺序 `TRUNCATE ... RESTART IDENTITY CASCADE`，表清单在 `CLEAR_TABLES`：

```
fetch_log, vlcc_positions, vlcc_vessels, fund_nav, fund_holdings, funds,
crypto_prices, crypto_coins, index_minutes, index_spot, index_prices, market_indices,
commodity_minutes, commodity_spot, prices, commodities
```

> ⚠ **它并不是「清空所有数据」**。以下表不在清单里，`--force_clear` 之后仍然留着旧数据，
> 而清单里的表已被清空 → 库处于**半新半旧**状态：
> `dram_products` / `dram_prices` / `storage_products` / `storage_prices` / `gpu_prices` /
> `fx_pairs` / `fx_rates` / `fx_spot` / `ipo_calendar` / `private_funds` / `private_managers` / `private_fund_nav`
>
> 真要做全库重建，把上面这批表补进 `CLEAR_TABLES`（注意 `storage_prices` 要排在 `storage_products` 前、`dram_prices` 排在 `dram_products` 前），
> 或者直接 `DROP DATABASE` 重来 —— 后者需要各脚本的 `SQL_ENSURE` 都能重建表（本仓库无 migrations，DDL 内联在脚本里）。

`--clear` 要手输 `YES`；`--force_clear` **无任何确认**，别在脚本里误用。

---

## 八、排错

| 症状 | 处理 |
|---|---|
| `ModuleNotFoundError: psycopg2` / `akshare` | 换成系统 Python 全路径 |
| `[FATAL] 无法连接数据库` | 检查根 `.env` 的 `DATABASE_URL`（注意 BOM / 引号 / CRLF 会让变量名悄悄变形） |
| 某脚本 `✘ 失败 exit=1` | 单独跑该脚本看输出：`python fetch_xxx.py --dry-run`（多数脚本支持） |
| 某个 `[SKIP]` | 缺 `HIFLEET_API_KEY` 或文件不存在，**不是失败** |
| 抓了但库里没新行 | 先比对「进程启动时间 vs 脚本 mtime」——进程起来后再改脚本，跑的还是旧版本；再确认当天源侧是否真有新数据（休市 / 周更 / 季更） |
| 想确认增量是否真的幂等 | 连跑两次，第二次看各表行数是否不变 |

后台跑长任务时建议无缓冲 + 落日志：

```bash
python -u refresh_all.py --fetch > .workbuddy/refresh_$(date +%F).log 2>&1 &
```

---

## 九、新增一个数据源要改哪里

1. 新建 `fetch_<name>.py`，对齐 `fetch_ccfi.py` 的 docstring 规范（源 URL / 字段映射 / 深度 / 死路 / 合规）
2. `refresh_all.py` 的 `FETCH_SCRIPTS` 加一行；需要展示的加 `STATUS_QUERIES` 一块
   （耗时长的**不要**接入；需 API key 的加 `{"requires_env": "XXX"}`）
3. 前端硬编码处同步：`Markets.vue::SECTIONS` / `Home.vue` / `App.vue::navItems` / `router/index.ts`
4. 验证：实跑 → 再跑一次增量应为 0 行 → `--status` → curl API → 页面截图
5. 落库结论写进 `.workbuddy/memory/DATASOURCES.md`（状态 / 口径 / 死路 / 待拍板）
