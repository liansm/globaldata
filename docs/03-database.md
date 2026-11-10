# 数据库字段说明

> PostgreSQL。**38 张表 + 1 个视图**，表定义权威来源是 [`src/db/schema.ts`](../src/db/schema.ts)
> （Drizzle 定义，注释比本文详细）。
> 2026-11-10 已补：`stock_shareholders` 全表 + `stock_financials.other_income` 列都已写进
> `schema.ts`；同日 DROP 了遗留备份表。现在 **38 张基表 / 486 列与 `schema.ts` 零差异**
> （Drizzle 反射 + `information_schema` 双向比对，库里既没有未声明的表，也没有孤儿定义）。
>
> 行数是 2026-11-10 `refresh_all.py --status` 实测。写入方见 [`01-scripts.md`](./01-scripts.md)。

---

## 一、六条总约定（读任何表之前先看）

1. **本仓库没有 drizzle migrations**。DDL 内联在各抓取脚本的 `SQL_ENSURE` / `SCHEMA_SQL` 里，
   用幂等 `CREATE TABLE IF NOT EXISTS` + `ALTER ... ADD COLUMN IF NOT EXISTS`。
   `schema.ts` 是**类型与文档的同步镜像**，不是建表来源 —— 改表时必须两边都改。
2. **`numeric` 在 Drizzle/pg 里返回的是字符串**，不是 number。API 层必须显式转
   `Number(x)` 或用 `::float8`，否则前端 `toFixed` 会报错或算出字符串拼接。
3. **金额单位各表不同，没有全局约定**。看到 `numeric` 必须回去查该表的单位注释
   （本文每张表都标了）。最典型的三个雷：
   `dram_products.density_gb` 是 **Gb**、`storage_products.capacity_gb` 是 **GB**、
   `stock_shareholders.ratio` 是**百分数**不是小数。
4. **NULL ≠ 0**。没数据 / 不适用 / 亏损 / 停牌，一律 NULL，前端显示「—」。
   库里出现 0 就是真的 0（例如「涨跌幅 0%」「成交额 0」）。
5. **时间列分三种，不能混**：
   | 列型 | 含义 | 例子 |
   |---|---|---|
   | `timestamptz` | 库内写入时刻 / 带时区的事件时刻 | `updated_at`、`vlcc_positions.ts` |
   | `timestamp`（无时区） | **中国本地时间**，见列注释 | `index_minutes.dt`、`commodity_minutes.dt` |
   | `date` | 交易日 / 报告期 / 统计截止日 | `price_date`、`report_date` |
   ⚠ `TIMESTAMP` 无时区列**不要丢给 JS 算日期**：pg 按 UTC 解析，本地 +8h 会把 23:00 的夜盘末根
   推到第二天。日期一律用 SQL `to_char(MAX(dt),'YYYY-MM-DD')`。
6. **派生数据不落表**：周/月/年线由 API `date_trunc` 聚合 `stock_prices` 日线；
   财报单季由视图 `v_stock_financials_quarterly` LAG 差分。日线/累计值是唯一事实来源。

---

## 二、表清单总表

### 行情 / 指数

| 表 | 粒度（主键/唯一） | 行数 | 写入方 |
|---|---|---:|---|
| `commodities` | 品种 key | 69 | `fetch_commodities.py` `fetch_cement.py` `fetch_milk_price.py` |
| `prices` | `(commodity_key, price_date)` | 129,612 | 同上 + `fetch_coal_flash.py` `fetch_coal_intl.py` |
| `market_indices` | 序列 key | 80 | `fetch_markets.py` `fetch_bdi.py` `fetch_ccfi*.py` `fetch_ctfi*.py` `fetch_cement.py` |
| `index_prices` | `(index_key, price_date)` | 105,181 | 同上 |
| `index_minutes` | `(index_key, dt)` | 141,006 | `fetch_market_minutes.py` |
| `index_spot` | `index_key` | 9 | `fetch_index_spot.py` |
| `commodity_spot` | `commodity_key` | 22 | `fetch_commodity_spot.py` |
| `commodity_minutes` | `(commodity_key, dt)` | 143,504 | `fetch_commodity_minutes.py` |
| `crypto_coins` | 币种 key | 10 | `fetch_crypto.py`（已下线） |
| `crypto_prices` | `(coin_key, price_date)` | — | 同上 |
| `fetch_log` | 自增 id | — | `fetch_commodities.py` |

### 基金 / 私募 / 新股

| 表 | 粒度 | 行数 | 写入方 |
|---|---|---:|---|
| `funds` | `fund_code` | 28,348 | `fetch_funds.py` `fetch_fund_nav.py` |
| `fund_nav` | `(fund_code, nav_date)` | **34,620,625** | `fetch_fund_nav.py` |
| `fund_holdings` | `(fund_code, report_date, holding_type, security_code)` | 1,476,389 | `fetch_funds.py` |
| `private_managers` | `register_no` | 18,432 | `fetch_private_funds.py` |
| `private_funds` | `fund_no` | 251,491 | 同上 + `refresh_private.py` |
| `private_fund_nav` | `(fund_no, nav_date)` | ~320 万 | `load_simu_nav*.py` / `fetch_private_nav_licai.py` / `fetch_private_funds.py` |
| `ipo_calendar` | `(market, code)` | 4,109 | `fetch_ipo_calendar.py` |

### 外汇 / AI 硬件 / 船位

| 表 | 粒度 | 行数 | 写入方 |
|---|---|---:|---|
| `fx_pairs` | `key` | 25 | `fetch_fx.py` |
| `fx_rates` | `(pair_key, rate_date, rate_type)` | 109,995 | `fetch_fx.py` |
| `fx_spot` | `pair_key` | 19 | `fetch_fx.py` |
| `dram_products` / `dram_prices` | `dram_key` / `(dram_key, price_date)` | 11 / 1,438 | `fetch_ai_hardware.py` |
| `storage_products` / `storage_prices` | `storage_key` / `(storage_key, price_date)` | 107 / 107 | 同上 |
| `gpu_products` / `gpu_prices` | `gpu_key` / `(gpu_key, price_date, provider, region)` | 6 / 90 | 同上 |
| `vlcc_vessels` | `name_ais` | 99 | `fetch_vlcc_fleet.py` |
| `vlcc_positions` | `(name_ais, ts)` | 3,241 | `fetch_vlcc_position_hifleet.py`（主）/ `fetch_vlcc_position.py`（副） |

### A 股

| 表 | 粒度 | 行数 | 写入方 |
|---|---|---:|---|
| `stocks` | `code` | 5,562 | `fetch_stock_info.py`（名录列）`fetch_stock_prices.py`（两个日期列） |
| `stock_industries` | `(code, kind, name)` | 118,573 | `fetch_stock_info.py` |
| `stock_prices` | `(code, trade_date)` | 6,072,822 | `fetch_stock_prices.py` |
| `stock_fundamentals` | `code` | 5,562 | `fetch_stock_fundamentals.py` |
| `stock_reports` | `(code, report_year, quarter)` | 175,174 | `fetch_stock_reports.py` |
| `stock_financials` | `(code, report_date)` | 209,413 | `fetch_stock_financials.py` |
| `stock_holder_counts` | `(code, end_date)` | 398,451 | 同上 |
| `stock_employee_snapshots` | `(code, snapshot_year)` | 5,423 | 同上 |
| `stock_shareholders` | `(code, report_date, kind, holder_rank)` | 3,501,340 | `fetch_stock_shareholders.py` |

---

## 三、行情 / 指数（10 张）

### `commodities` — 大宗商品定义（69 行）

每次抓取 UPSERT，一行一个品种。

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `key` | varchar(60) **PK** | ✗ | 品种键，如 `coal_port_5500`、`milk_raw`、`cement_po425` |
| `symbol` | varchar(60) | ✗ | 源侧代码 / 合约代码 |
| `commodity` | varchar(200) | ✗ | 品种中文名 |
| `unit` | varchar(50) | ✗ | **计价单位**（元/吨、元/克、美元/桶、元/公斤…） |
| `source_api` | varchar(200) | ✓ | 来源接口名，追溯用 |
| `price_type` | varchar(100) | ✓ | 价格类型，如 CCTD 的「环渤海现货」 |
| `kcal` | integer | ✓ | 煤炭热值（大卡） |
| `grade_type` | varchar(50) | ✓ | `港口` / `进口` / `产地` |
| `updated_at` | timestamptz | ✗ | 默认 `NOW()` |

### `prices` — 大宗商品日线（129,612 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `commodity_key` | varchar(60) | ✗ | → `commodities.key`（**无外键约束**，靠脚本保证） |
| `price_date` | date | ✗ | 交易日 |
| `price` | numeric(14,4) | ✗ | 价格，单位见 `commodities.unit` |

> 唯一键 `(commodity_key, price_date)` 在各脚本 `SQL_ENSURE` 里建，**不在 schema.ts 声明** ——
> 建表语句里是 `UNIQUE` 约束而非 Drizzle 的 `unique()`。查重靠 `ON CONFLICT`。

### `market_indices` — 指数 / 资金流序列定义（80 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `key` | varchar(60) **PK** | ✗ | 序列键，如 `sh000001`、`bdi`、`ctfi_total`、`coal_newc_fob` |
| `symbol` | varchar(60) | ✗ | 源侧代码 |
| `name` | varchar(200) | ✗ | 中文名 |
| `market` | varchar(50) | ✗ | **分组键**：`A股`/`港股`/`美股`/`欧洲`/`亚太`/`航运`/`建材`/`资金流向`/`AI硬件` |
| `unit` | varchar(50) | ✓ | 点 / 美元/天 / 美元/吨 / 万元 |
| `updated_at` | timestamptz | ✗ | |

**`market` 取值 → 由哪个脚本写**（改前端分组前必看）：

| market | 脚本 |
|---|---|
| `A股` `港股` `美股` `欧洲` `亚太` | `fetch_markets.py` + `fetch_index_spot.py` |
| `资金流向` | `fetch_markets.py`（2026-11-02 起停抓） |
| `航运` | `fetch_bdi.py`（含全历史）、`fetch_ccfi*.py`（2023-04 起）、`fetch_ctfi*.py`（滚存） |
| `建材` | `fetch_cement.py`（CEMPI） |

⚠ **同表内各序列的「可回补性」差别极大**：BDI 系列含全历史，而 **CCFI / CTFI 只能靠定期运行累积，
跑漏的期次永久缺失**。

### `index_prices` — 指数日线（105,181 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `index_key` | varchar(60) | ✗ | → `market_indices.key` |
| `price_date` | date | ✗ | |
| `open` / `high` / `low` / `close` | numeric(16,4) | ✓ | 指数点位 |
| `volume` | numeric(24,4) | ✓ | 成交量 |
| `turnover` | numeric(24,4) | ✓ | 成交额（元） |

⚠ **不是每一行都有 OHLC**：CCFI / BDI / CTFI / CEMPI 这类指数源**只给单一值**，
统一落在 `close`，其余四列 NULL。**不要让前端把 NULL 当 0 画进 K 线**。

### `index_minutes` — A股指数 1 分钟 K 线（141,006 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `index_key` | varchar(60) | ✗ | 有外键 → `market_indices(key)` ON DELETE CASCADE |
| `dt` | **timestamp（无时区）** | ✗ | **中国本地时间**，不是 UTC |
| `open`/`high`/`low`/`close` | numeric(16,4) | ✓ | |
| `volume` / `turnover` | numeric(24,4) | ✓ | |

唯一 `(index_key, dt)`。只写当日（C 类滚存），休市日不增长是正常的。

### `index_spot` — A股/港股指数实时快照（9 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `index_key` | varchar(60) **PK** | ✗ | |
| `price` | numeric(16,4) | ✓ | 最新价 |
| `change_pct` | numeric(8,4) | ✓ | **1.23 表示 +1.23%**（已是百分数） |
| `turnover` | numeric(24,4) | ✓ | 成交额（元） |
| `prev_close` | numeric(16,4) | ✓ | 昨收 |
| `spot_date` | date | ✓ | 快照所属交易日 |
| `snapshot_kind` | varchar(8) | 默认 `intraday` | `intraday`=盘中实时（只有 `spot_date` 是今天才可信）/ `close`=收盘快照（带成交额） |
| `updated_at` | timestamptz | ✗ | |

### `commodity_spot` — 期货实时快照（22 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `commodity_key` | varchar(60) **PK** | ✗ | |
| `price` | numeric(20,6) | ✓ | 最新价 |
| `change_pct` | numeric(8,4) | ✓ | 百分数，1.23 = +1.23% |
| `change_amt` | numeric(20,6) | ✓ | 涨跌额 |
| `prev_close` | numeric(20,6) | ✓ | 昨结算 |
| `volume` / `turnover` | numeric(24,4) | ✓ | |
| `spot_date` | date | ✓ | |
| `snapshot_kind` | varchar(8) | 默认 `intraday` | `intraday`=盘中/夜盘实时；`close`=收市静态快照 |
| `updated_at` | timestamptz | ✗ | |

### `commodity_minutes` — 期货主力 1 分钟 K 线（143,504 行）

字段与 `index_minutes` 同构（`commodity_key` / `dt` 无时区中国时间 / OHLCV / turnover）。

### `crypto_coins` / `crypto_prices` — 加密货币（已下线，表保留）

| 表 | 字段要点 |
|---|---|
| `crypto_coins` | `key` PK / `symbol` / `name` / `unit`（默认 `USD`）/ `updated_at` |
| `crypto_prices` | `coin_key` / `price_date` / `close` numeric(20,6) / `change_pct` numeric(8,4) / `volume_24h` numeric(24,2) / `high_24h` / `low_24h`；唯一 `(coin_key, price_date)` |

⚠ `change_pct` / `high_24h` / `low_24h` **只有快照接口给**，历史接口没有这三样。
历史 UPSERT 用 `COALESCE` 保护，否则每天跑一遍会把历次积累的值清空。

### `fetch_log` — 抓取审计流水

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | serial **PK** | ✗ | |
| `fetched_at` | timestamptz | ✗ | |
| `commodity_key` | varchar(60) | ✗ | |
| `latest_date` | date | ✓ | 该品种本次拿到的最新日期 |
| `latest_price` | numeric(14,4) | ✓ | |
| `change_day` | numeric(10,4) | ✓ | 日涨跌 |

> 每次 `fetch_commodities.py` 跑一遍，**一个品种一行**。用于回答「这个品种哪天抓过、
> 当时拿到的最新值是多少」。不参与业务查询。

## 四、基金 / 私募 / 新股（7 张）

### `funds` — 公募基金名录 + 规模 + 净值快照（28,348 行）

**份额口径，A/C 类分开**（不是按基金合并）。

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `fund_code` | varchar(12) **PK** | ✗ | 6 位基金代码 |
| `fund_name` | varchar(200) | ✗ | |
| `fund_type` | varchar(60) | ✓ | `混合型-偏股` / `股票型` / `指数型` … |
| `fund_company` | varchar(200) | ✓ | 基金公司 |
| `fund_manager` | varchar(200) | ✓ | 基金经理（可能多个） |
| `scale` | numeric(16,4) | ✓ | 最新规模，**亿元** |
| `scale_raw` | varchar(50) | ✓ | 源侧原文，如 `39.38亿`（便于复核） |
| `inception_date` | date | ✓ | 成立日 |
| `scale_updated_at` | timestamptz | ✓ | 规模刷新时间（带 30 天节流） |
| `latest_nav` | numeric(14,4) | ✓ | 最新单位净值（由 `fetch_fund_nav.py` 维护） |
| `latest_acc_nav` | numeric(14,4) | ✓ | 最新累计净值 |
| `latest_nav_date` | date | ✓ | |
| `latest_daily_return` | numeric(10,4) | ✓ | 日增长率 % |
| `nav_kind` | varchar(10) | ✓ | **`unit`**=普通基金（单位/累计净值）/ **`money`**=货币基金（万份收益 元 / 七日年化 %） |
| `nav_updated_at` | timestamptz | ✓ | |
| `updated_at` | timestamptz | ✗ | |

⚠ 这一行同时被两个脚本写：`fetch_funds.py` 写名录+规模列，`fetch_fund_nav.py` 写净值列
（`latest_*` / `nav_kind` / `nav_updated_at`）。**两者都不清空对方的列**。

### `fund_nav` — 基金净值日线（34,620,625 行 / 27,537 只 / 2001-09-21 起）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `fund_code` | varchar(12) | ✗ | |
| `nav_date` | date | ✗ | 净值日期 |
| `unit_nav` | numeric(14,4) | ✓ | **普通基金**=单位净值 / **货币基金**=万份收益（元） |
| `acc_nav` | numeric(14,4) | ✓ | **普通基金**=累计净值 / **货币基金**=七日年化（%） |
| `daily_return` | numeric(10,4) | ✓ | 日增长率 %（货币基金**留空**） |
| `nav_kind` | varchar(10) | ✗ 默认 `unit` | `unit` / `money` |

唯一 `(fund_code, nav_date)`；索引 `(fund_code, nav_date)`、`(nav_date)`。

> ⚠⚠ **`nav_kind='money'` 时列含义完全不同** —— 货币基金没有「单位净值」，
> **别和普通基金混算、别混着画一条曲线**。这是本表最容易出的错。

### `fund_holdings` — 基金季度持仓（1,476,389 行 / 23,041 只）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `fund_code` | varchar(12) | ✗ | |
| `report_date` | date | ✗ | 报告期截止日（如 2026-06-30）。**少数是具体日期**，见下 |
| `holding_type` | varchar(10) | ✗ | `stock` / `bond` |
| `security_code` | varchar(12) | ✗ | 证券代码 |
| `security_name` | varchar(100) | ✓ | |
| `ratio` | numeric(10,4) | ✓ | 占**净值**比例 % |
| `shares` | numeric(20,4) | ✓ | 持股数（**万股**） |
| `market_value` | numeric(20,4) | ✓ | 持仓市值（**万元**） |

唯一 `(fund_code, report_date, holding_type, security_code)`。

**三个索引都有明确用途**，别删：
- `idx_fund_holdings_fund_date (fund_code, report_date)` —— 基金详情页；
- `idx_fund_holdings_security_date (security_code, report_date, holding_type)` ——
  **按个股反查「哪些基金持有它」**。147 万行表没有这个索引会全表扫描：
  2026-11-09 实测加了之后 601872 的查询从 **0.106s → 0.0055s**。

⚠ **`report_date` 不全是季末**：F10 的 `<h4>` 有两种写法（「YYYY年N季度」与具体日期 YYYY-MM-DD），
脚本优先取具体日期 → 2,630 行 / 267 只基金 / 141 个非季末日期（占 0.18%）。
**反查「某股票被哪些基金持有」时必须只统计季末**（03-31/06-30/09-30/12-31）。
⚠ **季报只有前十大重仓**，完整持仓仅半年报/年报 —— 行业规则。
⚠ **反查要加名称闸门**：东财把港股代码补零成 6 位，与深市 A 股撞号
（`000020` 深华发A vs 商汤-W、`000670` 盈方微 vs 中国东方航空股份）。

### `private_managers` — 私募管理人（18,432 家）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `register_no` | varchar(40) **PK** | ✗ | 登记编号 |
| `manager_id` | **bigint** | ✓ | 中基协内部 id（**超出 int4**，别改成 integer） |
| `manager_name` | varchar(200) | ✗ | |
| `artificial_person` | varchar(120) | ✓ | 法定代表人 |
| `invest_type` | varchar(80) | ✓ | 机构类型 |
| `register_province` | varchar(80) | ✓ | |
| `office_address` | varchar(300) | ✓ | |
| `establish_date` / `register_date` | date | ✓ | |
| `fund_count` | integer | ✓ | 在管基金数量 |
| `member_type` | varchar(80) | ✓ | |
| `has_special_tips` / `has_credit_tips` | boolean | ✓ | 中基协风险提示标记 |
| `updated_at` | timestamptz | ✗ | |

### `private_funds` — 私募产品备案（251,491 只）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `fund_no` | varchar(40) **PK** | ✗ | 备案编码（**与代销池同码**） |
| `fund_name` | varchar(300) | ✗ | |
| `manager_name` | varchar(200) | ✓ | |
| `manager_id` | bigint | ✓ | |
| `manager_type` | varchar(40) | ✓ | 受托管理 / … |
| `working_state` | varchar(40) | ✓ | `正在运作` / `延期清算` / … |
| `record_date` | date | ✓ | 备案时间 |
| `establish_date` | date | ✓ | 成立时间 |
| `mandator_name` | varchar(200) | ✓ | 托管人 |
| `is_depute_manage` | boolean | ✓ | |
| `in_registry` | boolean | ✗ 默认 false | 在中基协备案库 |
| `has_nav` | boolean | ✗ 默认 false | 有净值 |
| `latest_nav` / `latest_acc_nav` | numeric(14,4) | ✓ | 最新净值快照 |
| `latest_nav_date` | date | ✓ | |
| `latest_daily_return` | numeric(10,4) | ✓ | |
| `latest_fund_size` | numeric(20,2) | ✓ | **元**；多数源侧不给 |
| `nav_updated_at` / `updated_at` | timestamptz | ✓ / ✗ | |

⚠ **`in_registry` / `has_nav` 是两个字段，别合成一个**：「在备案库但没有净值」和
「有净值但不在备案库」是两种完全不同的状态，前端要分开渲染。

### `private_fund_nav` — 私募净值日线（≈320 万行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `fund_no` | varchar(40) | ✗ | |
| `nav_date` | date | ✗ | |
| `unit_nav` | numeric(14,4) | ✓ | 单位净值（**`sppw` 源恒为 NULL**） |
| `acc_nav` | numeric(14,4) | ✓ | **含分红口径**：累计净值（em/licai）/ **复权净值**（sppw） |
| `daily_return` | numeric(10,4) | ✓ | 日涨跌 %，首点留 NULL |
| `source` | varchar(16) | ✗ 默认 `em_gaoduan` | `em_gaoduan` / `licai` / `sppw` |

唯一 `(fund_no, nav_date)`；索引 `(fund_no, nav_date)`、`(source)`。

> ⚠⚠ **本表最重要的一条不变式：同一 `fund_no` 只有一个 `source`。**
> 复权口径（sppw）与累计口径（em/licai）在「有分红」标的上终身收益最多差 **2.5 倍**，
> **禁止跨源拼同一条序列**。写库时对任何已有 net 值的 `fund_no` 一律整只跳过。
> 前端标签必须跟 `source` 走，不能统一写「净值」。

### `ipo_calendar` — 新股日历（4,109 行）

**三个市场合一张表，市场特有列一律可空**（与本项目「绝对价格/指数」分层习惯一致）。

| 字段 | 类型 | 可空 | 市场 | 含义 |
|---|---|---|---|---|
| `id` | bigserial **PK** | ✗ | | |
| `market` | varchar(10) | ✗ | | `A股` / `北交所` / `港股` |
| `code` | varchar(16) | ✗ | | 股票代码 |
| `name` | varchar(120) | ✗ | | |
| `exchange` | varchar(30) | ✓ | | 交易所 |
| `board` | varchar(20) | ✓ | A股 | `非科创板` / `科创板` / `北交所` |
| `industry` | varchar(60) | ✓ | 港股 | 行业分类 |
| `issue_price` | numeric(14,4) | ✓ | | 发行价 / 招股价**下限** |
| `issue_price_high` | numeric(14,4) | ✓ | 港股 | 招股价区间上限 |
| `currency` | varchar(6) | ✓ | | `CNY` / `HKD` |
| `issue_shares` | numeric(24,4) | ✓ | | 发行总数（**股**，源侧万股已 ×1e4） |
| `raise_amount` | numeric(20,4) | ✓ | | 募集资金（**亿**，币种看 `currency`） |
| `lot_size` | numeric(14,2) | ✓ | 港股 | 每手股数 |
| `entry_fee` | numeric(14,2) | ✓ | 港股 | 入场费（港元） |
| `apply_date` | date | ✓ | 都有 | A股**申购日** / 港股**招股起始日**（前端统一当「申购/招股」） |
| `apply_end_date` | date | ✓ | 港股 | 招股截止日 |
| `pricing_date` | date | ✓ | 港股 | 定价日 |
| `allotment_date` | date | ✓ | 都有 | A股中签号公布 / 港股公布售股结果 |
| `pay_date` | date | ✓ | A股 | 中签缴款日 |
| `refund_date` | date | ✓ | 港股 | 退票寄发日 |
| `grey_date` | date | ✓ | 港股 | **暗盘日**（上市前一日场外交易，A股无对应节点） |
| `listing_date` | date | ✓ | 都有 | 上市日 |
| `pe_issue` | numeric(14,4) | ✓ | | 发行市盈率 |
| `pe_industry` | numeric(14,4) | ✓ | A股 | 行业市盈率 |
| `win_rate` | numeric(14,6) | ✓ | A股 | 中签率 % |
| `source` | varchar(120) | ✗ | | 多源时用 `+` 连接，便于追溯 |
| `updated_at` | timestamptz | ✗ | | |

唯一 `(market, code)`；索引 `listing_date` / `apply_date` / `market`。

⚠ **`raise_amount` 单位统一为「亿」，未上市港股留 NULL 显示「—」，不要用 0 充数。**
A股募资额 = 发行总数(万股) × 发行价 / 1e4，由脚本自算；港股只有**已上市**标的能拿到。

---

## 五、外汇 / AI 硬件 / 船位（11 张）

### `fx_pairs` — 货币对定义（25 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `key` | varchar(40) **PK** | ✗ | `usd_cny` |
| `base_code` | varchar(8) | ✗ | `USD` |
| `base_name` | varchar(40) | ✗ | `美元` |
| `quote_unit` | integer | ✗ 默认 1 | **展示倍数**：JPY=100，其余 1（**不改数值语义**） |
| `category` | varchar(20) | ✗ | `主要货币` / `其他货币` |
| `official_code` | varchar(24) | ✓ | CFETS 原始代码，如 `100JPY/CNY` |
| `spot_code` | varchar(24) | ✓ | 新浪即期代码，如 `fx_sjpycny` |
| `sort_order` | integer | ✗ 默认 100 | |
| `updated_at` | timestamptz | ✗ | |

### `fx_rates` — 汇率日线（109,995 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `pair_key` | varchar(40) | ✗ | |
| `rate_date` | date | ✗ | |
| `rate_type` | varchar(8) | ✗ | `mid` 中间价 / `spot` 即期 |
| `open`/`high`/`low`/`close` | numeric(20,10) | ✓ | **`close` = 1 单位外币兑人民币** |
| `source` | varchar(16) | ✗ | `cfets` / `sina` |

唯一 `(pair_key, rate_date, rate_type)`；索引 `(pair_key, rate_date)`、`(rate_type)`。

> ⚠⚠ **全表唯一数值口径：`close` = CNY per 1 unit foreign。**
> CFETS 对 10 对报「外币/人民币」、对另 15 对报「人民币/外币」→ 入库时已统一
> （`CNY/KRW 201.70` → `0.0049579`，取倒数）。
> 所以**数值上行 = 人民币贬值（红）**，全表唯一，跨货币对可直接比较方向。
> ⚠ `rate_type='mid'` **只有 `close`**（官方只给单一中间价），OHLC 为 NULL，
> **UI 上必须显示「—」，不要用 close 充数**。`spot` 才有 OHLC。

### `fx_spot` — 即期实时快照（19 行）

`pair_key` PK / `price` / `prev_close` / `open` / `high` / `low`（numeric(20,10)）/
`change_pct` numeric(10,4) / `change_amt` numeric(20,10) /
`quote_time` varchar(16)（源侧报价 `HH:MM:SS`）/ `spot_date` date / `updated_at`。

> 休市时会停更，**看 `quote_time` 判断新鲜度**，不要看 `updated_at`（那是我们跑脚本的时间）。

### `dram_products` / `dram_prices` — 存储颗粒（11 个 key / 1,438 行）

| 字段（products） | 类型 | 含义 |
|---|---|---|
| `key` | varchar(60) **PK** | 列名是 `dram_key`，如 `ddr5_24gb_5200` |
| `prodid` | varchar(20) | 源站内部 id，**会变**，仅供排查 |
| `name` | varchar(120) | 源侧原名 |
| `generation` | varchar(10) | `DDR5` / `DDR4` / `DDR3`（**DDR3 已停抓，但枚举值保留**，库里还有旧行） |
| `density_gb` | integer | **Gb 不是 GB**（24/16/8/4）→ 每 GB 价要 ÷8 |
| `speed` | varchar(30) | `5200/6400` |
| `grade` | varchar(10) | **`prime` 原厂正片 / `ett` 白牌** —— 必须参与分组，同规格差 5~6 倍 |
| `unit` | varchar(20) | 默认 `USD/chip` |
| `source` | varchar(40) | `dram.com.cn` |

| 字段（prices） | 类型 | 含义 |
|---|---|---|
| `id` | bigserial **PK** | |
| `key` | varchar(60) | 列名 `dram_key` |
| `price_date` | date | |
| `low` | numeric(12,3) | 列名 `price_low` |
| `open` | numeric(12,3) | 列名 `price_open` |
| `close` | numeric(12,3) | 列名 `price_close` |

> ⚠ **源站历史表没有最高价**，只有最低/开盘/收盘 —— **不要用 close 冒充 high**。
> ⚠ **这是「颗粒」现货价，不是内存条价**：`DDR5 16Gb(2Gx8)` = 16Gb 颗粒 = 2GB。
> ⚠ 历史只有**滚动半年**，且报价日不连续（周末无报价）→ 查询端**不要按自然日补空/前值填充**。

### `storage_products` / `storage_prices` — 存储成品 + NAND（107 品种）

| 字段（products） | 类型 | 含义 |
|---|---|---|
| `key` | varchar(60) **PK** | 列名 `storage_key` |
| `prodid` | varchar(20) | |
| `name` | varchar(120) | **上游原名** |
| `display_name` | varchar(120) | 展示名（清洗后） |
| `category` | varchar(20) | `nand_wafer` / `ssd_oem` / `ssd_street` / `hdd` |
| `brand` | varchar(30) | |
| `capacity_gb` | numeric(10,1) | **已归一化的 GB**（NAND 的 `1Tb` 已 ÷8 折成 128GB） |
| `capacity_raw` | varchar(24) | 上游原写法，如 `1Tb` |
| `form_factor` | varchar(16) | `2.5` / `M.2` / `3.5` / `Wafer` |
| `interface` | varchar(16) | `SATA3` / `PCIe4.0` … |
| `dtype` | varchar(10) | `TLC` / `QLC` / `MLC` |
| `usage` | varchar(16) | 桌面 / 监控 / 移动 / 笔记本 |
| `currency` | varchar(3) | `CNY`/`USD` **已按分类校正过，不是站方标的那一列** |
| `currency_note` | varchar(160) | 写明币种校正原因（站方标记有错，见下） |
| `unit` | varchar(20) | |
| `sort_order` | integer | 前端排序位 |

`storage_prices` 结构与 `dram_prices` 完全同构（`storage_key` / `price_date` / `low` / `open` / `close`）。

> ⚠⚠⚠ **三个必须记住的坑**：
> 1. **`capacity_gb` 存的是 GB，`dram_products.density_gb` 存的是 Gb** —— **语义相反**。
>    每 GB 价 = `close / capacity_gb`，**不要再 ÷8**，否则差 8 倍。
> 2. **`currency` 是按分类反推校正过的**。站方标记有错：SSD OEM 有 3 条标 `￥` 实为 USD
>    （0.15 元/GB 低于晶圆裸片价）、HDD 有 1 条标 `$` 实为 CNY（$810 买 1TB 机械盘）。
> 3. **更新频率差两个数量级**：NAND/SSD OEM 周级、SSD 零售日更、**HDD 半年只有 3~4 个点**。
>    HDD 曲线接近水平线**不是抓漏，是上游不调价**，页面必须标出来。

### `gpu_products` / `gpu_prices` — GPU 云租价（6 型号 / 90 条）

| 字段（products） | 类型 | 含义 |
|---|---|---|
| `key` | varchar(40) **PK** | 列名 `gpu_key`，如 `h100_sxm` |
| `name` / `vendor` | varchar | `H100 SXM` / NVIDIA |
| `vram_gb` | numeric(8,1) | 显存 GB |
| `bandwidth_tbs` | numeric(8,2) | 带宽 TB/s |
| `unit_price_low` / `unit_price_high` | numeric(12,0) | **单卡买断参考价**（NULL = 尚无公开报价，如 Rubin） |
| `unit_price_note` / `unit_price_asof` / `unit_price_src` | varchar | 参考价出处与口径备注 |
| `sort_order` | integer | |

| 字段（prices） | 类型 | 含义 |
|---|---|---|
| `key` | varchar(40) | 列名 `gpu_key` |
| `price_date` | date | |
| `provider` | varchar(60) | 云厂商 **或 `market_min`** |
| `region` | varchar(40) | |
| `price_hourly` | numeric(10,4) | **每卡**时租（美元） |
| `gpus_per_node` | integer | 节点卡数，**用来还原整机总价** |

唯一 `(gpu_key, price_date, provider, region)`。

> ⚠ **`price_hourly` 是「每卡」不是整节点价**。源站 `$113.93 (x8 GPUs)` 是 p5 整机一小时价，
> 直接当单卡会虚高 8 倍。落库前已 ÷ 卡数，`gpus_per_node` 留着还原。
> ⚠ **`provider='market_min'` 是源站总表给的「单卡 x1」最低价，不含整节点摊销价**
> → 会出现「最低 $3.67 但 Vultr 摊到 $3.20」，不是数据错。前端必须写清是哪一种。
> ⚠ **这是租价，不是买断价**。买断参考价在 `gpu_products.unit_price_*`，静态非日更，两者差三个数量级。

### `vlcc_vessels` — VLCC 名录（99 艘）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `name_ais` | varchar(120) | ✗ 唯一 | **英文船名规范化（大写+折叠空白）→ AIS 匹配键** |
| `name_en` | varchar(120) | ✗ | 英文船名 |
| `name_cn` | varchar(120) | ✓ | 中文船名，**拿不到就留空，不猜** |
| `owner` | varchar(60) | ✗ | `招商轮船` / `中远海能` |
| `owner_full` | varchar(200) | ✓ | 船东全称 |
| `dwt` | numeric(14,2) | ✓ | 载重吨（**VLCC 判据 = ≥200,000**） |
| `built_year` | integer | ✓ | 建造年 |
| `flag` | varchar(12) | ✓ | `CN`/`HK`/`SG`/`PA`/`LR`/`MH`/`MT` —— **只做参考，不参与筛选** |
| `source` | varchar(200) | ✗ | 来源文档 |
| `imo` / `mmsi` | varchar(16) | ✓ | **源侧都不给**，由 AIS `ShipStaticData` 反推回写 |
| `verified` | boolean | ✗ 默认 false | AIS 是否实见过。**false ≠ 这船不存在** |
| `roster_asof` | date | ✓ | 该条名录的**口径截止日**（招商=抓取日，中远=2021-06-30） |
| `roster_status` | varchar(12) | ✗ 默认 `active` | `active` / `retired` |
| `status_note` | varchar(200) | ✓ | retired 时写现名 / 转手时间 / 依据 |

唯一 `name_ais`；索引 `owner`。

> ⚠ **口径 = 中国船东，不是挂旗**。这两家的 VLCC 大量挂香港旗/新加坡旗/巴拿马旗/利比里亚旗。
> ⚠ `roster_asof` 必须显示出来：中远那 48 艘的语义是「**2021-06-30 在册**」，不是「当前在役」。
> ⚠ `roster_status` **唯一写入方是 `fetch_vlcc_position_hifleet.py`**（底部 `RETIRED` 表投影），
> 名录脚本 UPSERT 不碰它。**状态字段只能有一个写入方。**
> ⚠ 前端必须把 `retired` 显示成「已转手」，**不能和「AIS 静默期无船位」混为一谈**。

### `vlcc_positions` — 船位（3,241 行 / 95 艘）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `name_ais` | varchar(120) | ✗ | |
| `mmsi` | varchar(16) | ✓ | |
| `ts` | **timestamptz** | ✗ | **AIS 报文时间（UTC）**，不是我们收到的时间 |
| `lat` / `lon` | numeric(10,6) | ✗ | **度**（HiFleet 原始单位是「分」，落库前已 ÷60） |
| `sog` / `cog` / `heading` | numeric(8,2) | ✓ | 航速/航向/船首向 |
| `nav_status` | varchar(40) | ✓ | 航行状态 |
| `dest` | varchar(120) | ✓ | 目的港 |
| `draught` | numeric(8,2) | ✓ | 吃水 |
| `source` | varchar(40) | ✗ 默认 `aisstream` | `hifleet` / `aisstream`，**两源可共存** |
| `created_at` | timestamptz | ✗ | 库内写入时刻（**重复命中只 UPDATE 不动它**） |

唯一 `(name_ais, ts)`。

> ⚠ **`ts` 是报文时间，远洋船可能已过去几小时甚至几天** —— 这是 AIS 固有限制，不是数据坏了。
> 前端必须显示「更新于 X 小时前」，不能默认所有点都实时。
> ⚠ 读状态时 `today_rows` 才是「今天抓到多少行」，`last_write`（`max(created_at)`）
> 可能停在前一天。船位默认不跑，**`today_rows` 长期为 0 属预期，不是坏了**。

## 六、A 股（9 张）

范围：**沪深主板 + 创业板 + 科创板，5,562 只。不含北交所（8/4/920 开头）与 B 股。**

### `stocks` — 名录 + 静态属性 + 行情快照（5,562 行）

**这一张表的列被两个脚本分区写**，改之前先看清分工：
`fetch_stock_info.py` 写名录/静态属性/行情快照；`fetch_stock_prices.py` **只写两个日期列**
（`latest_price_date` / `price_checked_date`）。两者都不清空对方的列。

**标识与检索**

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `code` | varchar(10) **PK** | ✗ | 6 位代码 |
| `name` | varchar(120) | ✗ | 简称 |
| `name_search` | text | ✓ | **检索键**：NFKC + 去空白 + 大写（`万  科Ａ` → `万科A`） |
| `name_pinyin` | varchar(32) | ✓ | 拼音首字母，**东财 searchapi 权威值**（非本地生成）：招商轮船 → `ZSLC` |
| `name_pinyin_full` | varchar(64) | ✓ | 全拼（本地 pypinyin 生成，逐字用东财首字母约束多音字）：`ZHAOSHANGLUNCHUAN` |
| `quote_time` | timestamptz | ✓ | **源侧行情时刻**（东财 `f124`），≠ `updated_at` |
| `exchange` | varchar(20) | ✗ | `上交所` / `深交所` —— **由代码前缀定，不用源侧 f13** |
| `board` | varchar(20) | ✗ | `沪市主板` / `科创板` / `深市主板` / `创业板` |

⚠ **`quote_time` 不能判断「最后交易日」**：339 只停牌/退市/ST 股源侧给**当日 08:00:00 占位**，
不是最后成交时刻。它只回答「源侧行情时钟走到几点」。

**公司资料**

| 字段 | 类型 | 含义 |
|---|---|---|
| `company_name` / `name_en` / `former_name` | text | 公司全称 / 英文名 / 曾用简称 |
| `introduction` | text | 机构简介（= F10 基本介绍） |
| `main_business` | text | 主营业务 |
| `business_scope` | text | 经营范围 |
| `legal_person` | varchar(100) | 法人 |
| `registered_address` / `office_address` | text | 注册 / 办公地址 |
| `website` / `email` / `phone` | text | |
| `listing_date` / `founded_date` | date | 上市日 / 成立日 |

**股本与规模**

| 字段 | 类型 | 含义 |
|---|---|---|
| `total_shares` | numeric(24,2) | 总股本，**单位：股**（东财 f84） |
| `float_shares` | numeric(24,2) | 流通股，**单位：股**（东财 f85） |
| `registered_capital` | numeric(20,4) | 注册资金，**单位：万元**（与股本是两回事） |
| `market_cap` | numeric(24,2) | 总市值，**元** |
| `float_market_cap` | numeric(24,2) | 流通市值，**元** |

**行业 / 地域**

| 字段 | 类型 | 含义 |
|---|---|---|
| `industry` | varchar(120) | 东财**细分行业**（f127，单值，如 `白酒Ⅱ`） |
| `industry_csrc` | varchar(120) | **证监会门类**（巨潮，单值，如 `酒、饮料和精制茶制造业`） |
| `region` | varchar(40) | 东财地域板块 f128 去掉「板块」后缀。**境外注册源侧返回 `-` → NULL，不猜省份** |

> 「一只股票可属多个行业/概念」→ 走 `stock_industries` 多值表。

**行情快照与进度**

| 字段 | 类型 | 含义 |
|---|---|---|
| `latest_price` | numeric(16,4) | 最新价（clist 全市场批量，**盘中可能是未完成的一根 K 线**） |
| `change_pct` | numeric(10,4) | 涨跌幅 % |
| `quote_date` | date | 快照所属交易日 |
| `latest_price_date` | date | 日线最新日 —— **唯一写入方 = `fetch_stock_prices.py`** |
| `price_checked_date` | date | 最后一次「源侧已查过、确认没有更新」的行情日 |
| `profile_updated_at` | timestamptz | 详情（简介/股本等）刷新时间 → 增量的判断依据 |
| `boards_updated_at` | timestamptz | 多值板块标签刷新时间 → 增量的判断依据 |

> ⚠ **`price_checked_date` 千万别删**：退市/长期停牌股的 `latest_price_date` 永远追不上源侧最新日，
> 没有这列就会**每天白抓 345 只（30 秒）**。新交易日到来时源侧最新日变大 → 自然被重抓一次。
> ⚠ 三个时间戳（`profile_updated_at` / `boards_updated_at` / 上面两个 date）**就是增量进度本身**，
> 不要另外维护进度文件。

**索引（8 个）**：`name`、`name_search`、`name_pinyin`、`name_pinyin_full`、
`(exchange, board)`、`region`、`industry`、`industry_csrc`。

### `stock_industries` — 多值行业/概念/地域标签（118,573 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `code` | varchar(10) | ✗ | 股票代码 |
| `kind` | varchar(12) | ✗ | `industry` / `concept` / `region` / `unknown` |
| `name` | varchar(120) | ✗ | 板块名 |
| `board_code` | varchar(16) | ✓ | 东财板块代码 `BKxxxx` |

唯一 `(code, kind, name)`；索引 `name`、`code`。
当前：concept 76,664 行 / 5,223 只 / 530 个概念；industry 36,696 / 796 个；region 5,213 / 31 个。

### `stock_prices` — 不复权日线（6,072,822 行 / 5,415 只）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `code` | varchar(10) | ✗ | |
| `trade_date` | date | ✗ | 交易日 |
| `open`/`high`/`low`/`close` | numeric(16,4) | ✓ | **不复权** |
| `volume` | numeric(24,2) | ✓ | 成交量 **手** |
| `turnover` | numeric(24,2) | ✓ | 成交额 **元** |
| `amplitude` | numeric(10,4) | ✓ | 振幅 % |
| `change_pct` | numeric(10,4) | ✓ | 涨跌幅 % |
| `change_amt` | numeric(16,4) | ✓ | 涨跌额 |
| `turnover_rate` | numeric(10,4) | ✓ | 换手率 % |

唯一 `(code, trade_date)`；索引 `(code, trade_date)`、`(trade_date)`。

> ⚠ **一律不复权**（`adjust=''`）：除权日有跳空，**禁止与 qfq/hfq 序列拼接**。
> ⚠ **周/月/年线不在这张表**，由 `/api/stocks/:code/prices` 用 `date_trunc` 聚合日线。
> 日线是唯一事实来源 —— 别为了查询快就建派生表。

### `stock_fundamentals` — 每股财务指标快照（5,562 行）

| 字段 | 类型 | 含义 |
|---|---|---|
| `code` | varchar(10) **PK** | |
| `eps_ttm` | numeric(16,4) | **滚动 12 个月**每股收益 = `price / pe_ttm`（反推式） |
| `eps_report` | numeric(16,4) | 最新报告期**累计、未年化** EPS（clist f112） |
| `report_date` | date | 上述报告期的截止日 |
| `pe_ttm` / `pe_dynamic` / `pe_static` | numeric(16,4) | TTM / 动态(报告期年化) / 静态(上年年报) |
| `bvps` | numeric(16,4) | 每股净资产（最新报告期，f113） |
| `pb` | numeric(16,4) | 市净率（f23，**源侧口径**） |
| `dividend` | numeric(16,6) | **近 12 个月已实施**现金分红合计，**税前、元/股** |
| `dividend_yield` | numeric(10,4) | = `dividend / price × 100`（%，**自算可验算**） |
| `dividend_plans` | integer | 近 12 个月分红方案条数 |
| `dividend_last_ex` | date | 最近一次除权除息日 |
| `price` / `quote_date` | numeric(16,4) / date | 计算所用的最新价与行情日 |
| `source` / `updated_at` | | |

索引 `quote_date`、`pe_ttm`。

> ⚠ **`eps_ttm` 与 `eps_report` 绝不能混用**：茅台 2026 三季报累计 51.57，年化后约 68.7。
> ⚠ **三个 PE 数值可差 30%+**（五粮液 23.99 / 22.59 / 32.66）→ UI 主显示 `pe_ttm` 并标口径。
> ⚠ **`PB ≠ price / bvps`**：前 200 只里 5 只（科创板为主，688809/688678/688655/688610/688560）
> 偏差 7~28%，是源侧 f23 与 f113 的报告期/股本口径差异。保留源侧 PB，**不要宣称两者可验算**。
> ⚠ **不用东财 `f133` 股息率**（茅台只算最近一次 1.93%，平安银行算近 12 个月 5.16%）——
> 只用它做交叉校验。`dividend` 里**必须剔掉未来除权除息日**的方案（源侧会返回未来已公告方案）。

### `stock_reports` — 季度财报 PDF 链接（175,174 行）

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `id` | bigserial **PK** | ✗ | |
| `code` | varchar(10) | ✗ | |
| `exchange` | varchar(20) | ✗ | `上交所` / `深交所` |
| `report_year` | integer | ✗ | **报告期年份**（不是披露年份） |
| `quarter` | varchar(4) | ✗ | `Q1` / `H1` / `Q3` / `FY` |
| `quarter_num` | integer | ✗ | 1 / 2 / 3 / 4（用于倒序排序） |
| `title` | text | ✓ | 公告标题 |
| `pdf_url` | text | ✗ | PDF 直链 |
| `publish_date` | date | ✓ | 披露日 |
| `source` | varchar(12) | ✗ | `sse` / `szse` |

唯一 `(code, report_year, quarter)`；索引 `(code, report_year, quarter_num)`、`publish_date`。

> ⚠ **`report_year` 是报告期不是披露年份**：「2025年年度报告」通常 2026 年才披露。
> ⚠ 同一 (code, 年, 季度) **只留一条正文**：排除摘要/英文版/更正公告/问询函/专项报告；
> 有「（更正后）」正文时优先取更正版。
> ⚠ **沪市 PDF 直链只存不下载**：脚本 GET 撞阿里云 WAF 的 JS 挑战页（200 text/html），
> 浏览器能正常打开 —— 别据此判链接失效。
> ⚠ **深市不要加 `/download` 路径段**：那版带 `Content-Disposition: attachment`，
> 浏览器会下载而不是查看（两版 PDF 字节完全一致，库内统一存内联版）。

### `stock_financials` — 财报三表（209,413 行 / 5,515 只 / 2016-03 起 43 期）

**一行 = 一只股票 × 一个报告期，三张表合并成一行**（不是三张表三行）。
三表由脚本分三次 UPSERT，每次只 UPDATE 自己的列，`raw` 用 jsonb `||` 按键合并 ——
**任一表缺报不会清空其它两张表已入库的值**。

**通用**

| 字段 | 类型 | 含义 |
|---|---|---|
| `code` | varchar(10) **PK①** | |
| `report_date` | date **PK②** | 报告期截止日 |
| `report_year` | integer | 报告期年份 |
| `quarter_num` | integer | 1 / 2 / 3 / 4 |
| `report_type` | varchar(12) | `一季报` / `中报` / `三季报` / `年报` |
| `org_type` | varchar(12) | `通用` / `银行` / `证券` / `保险` |
| `notice_date` / `update_date` | date | 三表里**最晚**的那个（`GREATEST`） |
| `currency` | varchar(8) | `CNY` |
| `inc_source` / `bal_source` / `cf_source` | varchar(40) | 各表实际用的 reportName，便于排查 |
| `raw` | jsonb | `{"BALANCE":{…},"INCOME":{…},"CASHFLOW":{…}}`，只保留有值字段（319/203/254 个） |
| `source` / `updated_at` | | |

**利润表（年份至今累计，不是单季）**

| 字段 | 含义 |
|---|---|
| `total_operate_income` | 营业总收入（**含利息/手续费**） |
| `operate_income` | **营业收入** ← 毛利率分母必须用它 |
| `operate_cost` | 营业成本 |
| `total_operate_cost` | 营业总成本 |
| `operate_profit` | 营业利润 |
| `total_profit` | 利润总额 |
| `parent_netprofit` | 归母净利润 |
| `netprofit` | 净利润 |
| `income_tax` | 所得税 |
| `sale_expense` / `manage_expense` / `finance_expense` / `research_expense` | 销售/管理/财务/研发费用 |
| `other_income` | 其他收益（**可为负 = 净损失**；2026-11-09 后补的列，见文末） |
| `basic_eps` / `diluted_eps` | 基本 / 稀释每股收益 |

**资产负债表（季末时点值，绝不差分）**

`total_assets` 总资产 / `total_current_assets` 流动资产 / `total_liabilities` 总负债 /
`total_current_liab` 流动负债 / `total_equity` 所有者权益 / `total_parent_equity` 归母权益 /
`minority_equity` 少数股东权益 / `monetaryfunds` 货币资金 / `accounts_rece` 应收 /
`inventory` 存货 / `fixed_asset` 固定资产 / `cip` 在建工程 / `goodwill` 商誉 /
`short_loan` 短期借款 / `long_loan` 长期借款 / `accounts_payable` 应付。

**现金流量表（年份至今累计）**

`netcash_operate` 经营 / `netcash_invest` 投资 / `netcash_finance` 筹资净额 /
`sales_services` 销售商品收到现金 / `pay_staff_cash` 支付给职工的现金 /
`construct_long_asset` 购建长期资产 / `cce_add` 现金及等价物净增加 /
`begin_cce` / `end_cce` 期初/期末现金。

**除 `code`/`report_date` 外全部可空。金额单位一律「元」。**

主键 `(code, report_date)`；索引 `report_date`、`update_date`。

> ⚠⚠ **流量表是年初至今累计，不是单季**。要单季请查视图（见下一节），**不要自己减**
> —— 缺季时必须返回 NULL，不能当 0。
> ⚠ **金融业（`org_type ≠ '通用'`）**：G 模板的资产负债表/现金流表对金融股**返回空**，
> 必须走 B/S/I 模板。所以「某银行没有资产总计」不是抓漏。
> ⚠ `notice_date` / `update_date` 取三表最晚 —— 源侧追溯更正全年发生，
> **增量必须按 `UPDATE_DATE` 回看，而不是「只在披露季跑」**。

### `stock_holder_counts` — 股东户数（398,451 行 / 5,165 只）

| 字段 | 类型 | 含义 |
|---|---|---|
| `code` | varchar(10) **PK①** | |
| `end_date` | date **PK②** | **统计截止日**（不必等于季末） |
| `holder_num` | bigint | 股东户数 |
| `pre_holder_num` | bigint | 上期户数 |
| `holder_num_change` | bigint | 户数变化 |
| `holder_num_ratio` | numeric(24,4) | **百分比数值**（-6.0863 = −6.09%），**不是小数比例** |
| `avg_hold_num` | numeric(20,4) | 户均持股数 |
| `avg_market_cap` | numeric(24,2) | 户均市值 |
| `total_market_cap` | numeric(24,2) | 总市值 |
| `total_a_shares` | numeric(24,2) | 总 A 股股本 |
| `notice_date` | date | 公告日 |
| `change_reason` | text | 变动原因 |

> ⚠ **`holder_num_ratio` 精度必须是 numeric(24,4)**：新股上市首期 `PRE_HOLDER_NUM` 基数极小
> （个位数），涨幅可达 **1.6e8%**。用 numeric(12,4) 会在 34 万行处 overflow
> （2026-11-09 全量在 34 万行处回滚）—— 这是踩过的坑。
> ⚠ **映射到季度时取「该季内最后一个 end_date」，不硬凑季末**（茅台 64 期里 02 月 6 期、04 月 1 期）。
> ⚠ 写库必须 `with conn:`（事务上下文），否则静默 rollback。

### `stock_employee_snapshots` — 员工数年度快照（5,423 行）

| 字段 | 类型 | 含义 |
|---|---|---|
| `code` | varchar(10) **PK①** | |
| `snapshot_year` | integer **PK②** | 快照年份 |
| `emp_num` | integer | 员工人数 |
| `collected_at` | date | 采集日 |
| `source` / `updated_at` | | |

> ⚠ **源侧只有「当前快照」**（`RPT_F10_BASIC_ORGINFO.EMP_NUM`），**没有报告期字段**，
> 7 个按报告期端点候选全废；季报本来也不披露员工数
> → **一年存一条，逐年累积**。**不要宣称「逐季度员工数」。**

### `stock_shareholders` — 十大股东 / 十大流通股东（3,501,340 行 / 5,237 只 / 43 期）

> DDL 在 `fetch_stock_shareholders.py::SQL_ENSURE`；2026-11-10 起
> `src/db/schema.ts` 里也有 `stockShareholders` 定义（此前只有脚本建表）。

| 字段 | 类型 | 可空 | 含义 |
|---|---|---|---|
| `code` | varchar(10) **PK①** | ✗ | |
| `report_date` | date **PK②** | ✗ | **报告期末**（03-31/06-30/09-30/12-31），不是披露日 |
| `kind` | varchar(4) **PK③** | ✗ | `top`=十大股东（按**总股本**）/ `free`=十大流通股东（按**流通股**） |
| `holder_rank` | integer **PK④** | ✗ | 名次 1..10 |
| `holder_name` | varchar(300) | ✓ | 股东名 |
| `holder_code` | varchar(40) | ✓ | 股东代码 |
| `hold_num` | numeric(24,2) | ✓ | 持股数 |
| `total_ratio` | numeric(12,6) | ✓ | **占总股本 %**（两类都有） |
| `float_ratio` | numeric(12,6) | ✓ | **占流通股 %**（仅 `free`；`top` 恒 NULL） |
| `hold_change` | numeric(24,2) | ✓ | 增减**股数**（源侧是数字时） |
| `change_text` | varchar(24) | ✓ | 增减**文字**：`不变` / `新进` / `加仓` … |
| `shares_type` | varchar(24) | ✓ | 股份类型 |
| `holder_type` | varchar(24) | ✓ | 股东类型（仅 free：其它/投资公司/基金…） |
| `is_org` | smallint | ✓ | 是否机构（`IS_HOLDORG`） |
| `is_dg` | smallint | ✓ | 是否董监高（仅 free） |
| `is_sjkzr` | smallint | ✓ | 是否实际控制人（仅 free） |
| `market_cap` | numeric(24,2) | ✓ | 期末持仓市值 |
| `total_shares` | numeric(24,2) | ✓ | 总股本（源侧 `TOTAL_SHARES_NUM`，仅 top） |
| `notice_date` | date | ✓ | 披露日（**仅 free 有；top 为 NULL，不硬凑**） |
| `report_name` | varchar(24) | ✓ | 如 `2026三季报`（仅 free） |
| `raw` | jsonb | ✓ | 源侧原始行 |
| `source` / `updated_at` | | | |

索引 `(code, report_date DESC)`。

> ⚠ **`total_ratio` 与 `float_ratio` 必须分开存**：A+H / 有未流通股的公司差很多 ——
> 中芯国际 688981 2026H1 HKSCC 占流通股 **58.56%** 但占总股本只有 **54.82%**。
> 茅台这种几乎全流通的公司两者才近似相等（54.499 vs 54.499），**别拿它验口径**。
> ⚠ **`hold_change`（数字）与 `change_text`（中文）分开存**：源侧同一字段 `HOLD_NUM_CHANGE`
> 混着 `"189188123"` / `"-42000000"` 和 `"不变"` / `"新进"`，直接 numeric 化会**整批丢值**。
> ⚠ 比例单位是**百分数**（54.82 = 54.82%），不是小数。

---

## 七、视图 `v_stock_financials_quarterly`

配合 `stock_financials` 使用，**派生指标一律从视图取，不落表**（照 `stock_prices` 周/月线
不落派生表的先例）。定义在 `fetch_stock_financials.py::SQL_ENSURE`，
每次跑脚本会 `DROP VIEW` + `CREATE VIEW` 重建。

**单季差分（6 + 8 个派生列）** —— 规则统一：

```
Q1                → 直接取累计值
prev_q = 当前-1   → 累计值相减
其余（缺上一期）  → NULL，绝不用 0 充数
```

| 列 | 含义 |
|---|---|
| `q_parent_netprofit` | 单季归母净利润 |
| `q_revenue` | 单季营业总收入 |
| `q_operate_income` | 单季营业收入 |
| `q_operate_cost` | 单季营业成本 |
| `q_netcash_operate` | 单季经营现金流 |
| `q_sale_expense` / `q_manage_expense` / `q_research_expense` / `q_finance_expense` | 单季四项费用 |
| `q_operate_profit` / `q_income_tax` / `q_netprofit` | 单季营业利润/所得税/净利润 |
| `q_gross_profit` | 单季毛利润 = 单季营业收入 − 单季营业成本 |
| `q_other_income` | 单季其他收益（可为负） |

**派生比率**

| 列 | 公式 | 口径约束 |
|---|---|---|
| `gross_margin` | `(operate_income − operate_cost) / operate_income × 100` | **分母是营业收入，不是营业总收入**。茅台 2026Q3 = 89.581094%（与东财 XSMLL 一致）；用营业总收入会得 89.761896% |
| `net_margin` | `parent_netprofit / total_operate_income × 100` | |
| `quick_ratio` | `(total_current_assets − COALESCE(inventory,0)) / total_current_liab` | 实测 5.5505 = 东财 SD 5.550459 |
| `current_ratio` | `total_current_assets / total_current_liab` | |
| `debt_ratio` | `total_liabilities / total_assets × 100` | |

> ⚠ **金融业（`org_type <> '通用'`）的 `gross_margin` / `net_margin` / `quick_ratio` /
> `current_ratio` 在视图里直接置 NULL** —— 银行/证券/保险科目口径不同，算了也是错的。
> UI 显示「—」。
> ⚠ **给视图加列只能追加在末尾**：`CREATE OR REPLACE VIEW` 不允许改变已有列的顺序
> （所以 `q_finance_expense` / `q_other_income` 在最后）。

---

## 八、口径陷阱集中清单

跨表最容易搞错的九个，抄一份贴在显示器上：

| # | 陷阱 | 正确做法 |
|---|---|---|
| 1 | **Gb vs GB** | `dram_products.density_gb` 存 **Gb**（÷8 得 GB）；`storage_products.capacity_gb` 存**已归一化的 GB**（不再 ÷8） |
| 2 | **每 10 股 vs 每股** | `RPT_SHAREBONUS_DET.PRETAX_BONUS_RMB` 是每 10 股，÷10 才是 `stock_fundamentals.dividend` |
| 3 | **万股 vs 股** | 东财新股「发行总数」是**万股**；`stocks.total_shares` 才是**股** |
| 4 | **百分数 vs 小数** | `change_pct`=1.23 表示 +1.23%；`holder_num_ratio`=-6.0863 表示 −6.09%；`total_ratio`=54.82 表示 54.82% |
| 5 | **累计 vs 单季** | 财报流量表与 `fund_nav` 都不差分的；单季只能查视图 |
| 6 | **复权 vs 累计（私募）** | `acc_nav` 在 sppw 是**复权**、在 em/licai 是**累计**，差 2.5 倍，**禁止跨源拼序列** |
| 7 | **单位净值 vs 万份收益** | `nav_kind='money'` 时 `unit_nav`=万份收益、`acc_nav`=七日年化 |
| 8 | **分 vs 度** | HiFleet 位置接口是「分」（÷60），`gettraffic` 是「度」；落库统一为度 |
| 9 | **中间价只有 close** | `rate_type='mid'` 的 OHLC 全 NULL，UI 显示「—」不要拿 close 填 |

---

## 九、已知不一致 / 待办（2026-11-10 核对，同日已修 4 项）

写文档时对着实际库逐表核对出来的。**第 1、2、3、5 条当日已修**，只剩第 4 条待决定：

| # | 问题 | 状态 |
|---|---|---|
| 1 | `stock_shareholders` 没写进 `schema.ts`（表在库里 350 万行、脚本在写，Drizzle 定义缺失 → API 层拿不到类型） | ✅ 已补 `stockShareholders`，23 列与库里逐列一致 |
| 2 | `stock_financials.other_income` 没写进 `schema.ts`（列在库里、视图在用） | ✅ 已补 `otherIncome`（numeric(24,2)） |
| 3 | 遗留备份表 `commodity_minutes_bak_20261031`（10,474 行 / 甲醇 5,201 + 锌 5,273），2026-10-31 修「手写 mark 串种」时留下的 | ✅ 已 `DROP TABLE`（2026-11-10）。删前查过：无视图/外键依赖，`commodity_minutes` 14.35 万行未受影响；副本另存 `.workbuddy/tmp/commodity_minutes_bak_20261031.csv.gz`（129 KB / 10,474 行，确认无用可一并删） |
| 5 | `funds` / `fund_holdings` 全仓没有建表 DDL（库里有表，**新建空库时脚本不建表**，`fetch_funds.py` 会先撞 `relation "fund_holdings" does not exist`） | ✅ 已补：`fetch_funds.py::SQL_ENSURE` 建两张表 + 全列 `ALTER ... IF NOT EXISTS`；`fetch_fund_nav.py` 也加了最小 `funds` 骨架（它要 ALTER 该表，原先独立跑会炸） |
| 4 | `prices` 的唯一约束 `UNIQUE (commodity_key, price_date)` 只在脚本 DDL 里，`schema.ts` 没声明 `unique()` | ⬜ **待决定**：不影响运行（库里已有约束），只影响读代码的判断，加不加都可 |

**第 5 条的验证方式**（空库重建是真风险，不能只看代码）：建临时 schema、
`SET search_path` 后直接执行两个脚本的 `SQL_ENSURE`，四种顺序都跑通 ——
「先 funds 后 nav」「先 nav 后 funds」「nav 独立跑」「重复跑」；
建出的列并集与真实库一致。在真实库上重跑也确认幂等（无副作用）。

> 实际库里共 39 个 `information_schema.tables` 条目 =
> **38 张业务表 + 1 视图（`v_stock_financials_quarterly`）**，无遗留备份表。
> Drizzle 反射校验结果：38 张表全部声明，**486 个列零差异**。


