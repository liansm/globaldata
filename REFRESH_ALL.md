# refresh_all.py 使用文档

> 最后核对：2026-10-29（以 `refresh_all.py` 源码 + 各 `fetch_*.py` docstring 为准；脚本改动后请同步本文）

`refresh_all.py` **本身不抓任何数据**，它只做三件事：

1. **看状态** —— 按表打印行数 / 最新日期 / 覆盖率（`STATUS_QUERIES`）
2. **串行调度** —— 按 `FETCH_SCRIPTS` 顺序 `subprocess` 调用 18 个 `fetch_*.py`，逐个打印耗时与成败
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
# 默认：先看状态 → 跑全部 18 个脚本 → 再看一次状态（整轮约数分钟）
python refresh_all.py

# 只看库内现状，不抓（纯查询，安全）
python refresh_all.py --status

# 只刷新，不打印状态（适合 cron）
python refresh_all.py --fetch

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

## 二、会抓哪些数据（18 个脚本，按执行顺序）

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
| 18 | `fetch_vlcc_position_hifleet.py` | VLCC 逐船最新船位（HiFleet 付费，按 MMSI 查询） | `vlcc_positions` |

> 第 18 条缺 `HIFLEET_API_KEY` 时打印 `[SKIP]` 并跳过，**不计入失败**。它按查询次数计费，一轮约 99 次查位。

---

## 三、到底是不是增量抓取？

**结论：没有一个统一开关。18 个脚本各自实现，而且只有 3 个是「真增量」，其余靠「重拉固定窗口 + upsert 幂等」。**

按抓取方式分四类：

### A. 真增量 —— 先查库里 `MAX(date)`，只写更新的行（3 个）

| 脚本 | 判断依据 |
|---|---|
| `fetch_bdi.py` | 按序列查 `MAX(price_date)`，只 upsert 严格更新的行（源一次返回 ~5000 交易日全历史，但不重写） |
| `fetch_cement.py` | 同上，按序列查 `MAX(price_date)` |
| `fetch_ctfi_history.py` | 增量默认只解析**新报告**（`--full` 才全重抓） |

### B. 窗口重拉 —— 每次重新拉一个固定窗口，全靠 `ON CONFLICT DO UPDATE` 去重（6 个）

不省请求，只保证不产生重复行；数据量靠 `HISTORY_LIMIT` 截断。

| 脚本 | 每次重拉的窗口 |
|---|---|
| `fetch_markets.py` | 指数全历史（上限 20 年）+ 资金流向全历史 |
| `fetch_commodities.py` | 主力连续 20 年（`start_date = now - 365*20`） |
| `fetch_ccfi_history.py` | GreenPacific 全序列（2023-04 起 13 线） |
| `fetch_ai_hardware.py` | 源站**只给滚动半年**，每次重拉整个半年窗口 |
| `fetch_fx.py` | 中间价近 30 天（`--days` 可调）+ 即期日线近 7 天 |
| `fetch_vlcc_fleet.py` | 名录整表 upsert |

### C. 当日滚存 —— 源只给「当期」，历史只能靠每天跑一攒（6 个）

**漏一天就永久少一天**，这类最需要定时跑。

| 脚本 | 说明 |
|---|---|
| `fetch_ccfi.py` | 每周五发布；一次给上期+本期两期，**漏一周仍能补回一周**，漏两周就断 |
| `fetch_ctfi.py` | 日频，页面只给当期（历史是登录墙） |
| `fetch_market_minutes.py` | 只写当日 1 分钟 K 线（休市日跑出 0 行是正常的） |
| `fetch_commodity_minutes.py` | 只写当日分时（避免历史被换月合约的隔夜 tick 覆盖） |
| `fetch_fund_nav.py`（默认模式） | 只写当日净值快照；全历史要 `--history`（约 2 小时） |
| `fetch_ipo_calendar.py` | 新股日历本身自带过去+未来，不算严格滚存，但港股「招股截止/暗盘」有时效性 |
| `fetch_coal_flash.py` | 港口/进口/车板价快讯只发当天，历史靠翻 ctctc 站点分页回补（`--pages N`，列表约 17 页到底 ≈ 11 个月） |
| `fetch_coal_intl.py` | CCTD 国际煤炭周报每周二发、数据截止上周五；全量 `--pages 24` 可回补 475 期 ≈ 9 年 |

### D. 快照覆盖 —— 每键一行，每次直接覆盖（3 个）

`fetch_index_spot.py` / `fetch_commodity_spot.py` / `fetch_fx.py` 的 `fx_spot`：
表里永远只有一行/一品种，看 `updated_at` 与 `quote_time` 判断新鲜度，**没有历史**（历史在对应的日线表里）。

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
- 瓶颈是 `fetch_ai_hardware.py`（存储成品约 115 个品种 = 115 个请求，**约 2.5 分钟**）。
  只想要 DDR + GPU（约 30 秒）：`python fetch_ai_hardware.py --dram --gpu`（手动跑，不走 refresh_all）。
- 其余脚本普遍秒级到数十秒；`fetch_markets.py`（20 年全量重拉）与 `fetch_commodities.py`（60 品种 × 20 年）相对偏慢。
- 真实耗时不用估：跑一次 `--fetch`，末尾汇总表就是每个脚本的实测秒数。

> ⚠ **`refresh_all.py` 的退出码恒为 0**，即使 18 个脚本全部失败也一样（失败只体现在汇总里的红字）。
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
| `fetch_private_funds.py` | 私募备案全量（约 55 分钟，可续跑） | 直接跑 |
| `fetch_private_funds.py` | 代销池净值（约 13 分钟，幂等） | `--nav` |
| `fetch_crypto.py` | 加密货币（**2026-10-26 已暂时下线**） | 直接跑 / `--no-history` 只写当日快照 |
| `fetch_vlcc_position.py` | aisstream 版船位（**区域覆盖塌陷，已降副源**） | `--minutes 10` |
| `.workbuddy/load_simu_nav.py` | 私募净值第二源 | 直接跑 |

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
