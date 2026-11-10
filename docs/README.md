# globaldata 文档

大宗商品 / 指数 / 基金 / 私募 / 新股 / 航运船位 / A 股 的行情数据平台。
Python 抓取脚本落 PostgreSQL，Fastify + Drizzle 提供 API，Vue 3 + Element Plus 做前端。

---

## 三份主文档

| 文档 | 回答什么问题 | 什么时候看 |
|---|---|---|
| [**01-scripts.md**](./01-scripts.md) | 每个抓取脚本怎么跑、参数是什么、增量怎么算、耗时多少、坑在哪 | 要跑某个源、要改脚本、要新增数据源 |
| [**02-datasources.md**](./02-datasources.md) | 每个数据源是哪个站、什么口径、覆盖多深、**哪条路是死的以及为什么** | 要判断数据可不可信、要评估换源、要对齐口径 |
| [**03-database.md**](./03-database.md) | 38 张表逐字段含义、类型、单位、索引、口径陷阱 | 写 API、查数据、改表、算指标 |
| [**04-deploy.md**](./04-deploy.md) | 怎么整体搬到腾讯云：选型/备案/12GB 数据迁移/systemd/nginx/cron 定时抓取，以及本项目专属的 11 个部署坑 | 要上云、要换机器、要给别人的看 |

配套文档（保留在原位，不重复搬运）：

- [`../REFRESH_ALL.md`](../REFRESH_ALL.md) — `refresh_all.py` 的完整手册：耗时表、`--status` 输出怎么读、
  排错表、清库注意事项。**细节比本文深，值得单独读**。
- [`../.workbuddy/memory/DATASOURCES.md`](../.workbuddy/memory/DATASOURCES.md) — 1,788 行的
  原始数据源台账（按时间流水记录，含每一次探针的实测细节）。本文 02 是它的**结构化重组**，
  台账里仍保留着更多单点历史细节。
- 各脚本 docstring — **权威实现说明**，口径与死路的第一手记录。文档与代码冲突时以代码为准。

---

## 快速开始

```bash
# 1. 看库里现在有什么（纯查询，安全）
%LOCALAPPDATA%\Programs\Python\Python313\python.exe refresh_all.py --status

# 2. 日常刷新（约 4.5~6 分钟，24 个脚本）
%LOCALAPPDATA%\Programs\Python\Python313\python.exe refresh_all.py --fetch

# 3. 起后端（3000 常驻，无热重载）
npx tsx src/server.ts

# 4. 起前端（vite HMR 只监听 IPv6）
cd web && npm run dev     # → http://localhost:5173
```

三条最容易踩的环境坑：

1. **必须用系统 Python**（`%LOCALAPPDATA%\Programs\Python\Python313\python.exe`）——
   managed venv 里没有 `akshare` / `psycopg2`。
2. **`curl` 测本机必须加 `--noproxy '*'`** —— 环境代理会把 localhost 也送进代理，
   造成假阴性，极易误判「后端挂了」而重复重启。
3. **前端环境变量放 `web/.env`**（vite `envDir` 默认是 `web/`）；根 `.env` 只给 Python 用。

---

## 项目结构（只看关键部分）

```
globaldata/
├── fetch_*.py               # 30 个抓取脚本，一域一到多个
├── refresh_all.py           # 一键刷新 + 状态查询 + 清库
├── refresh_private.py       # 私募整轮更新（备案增量 + 三源净值）
├── src/
│   ├── db/schema.ts         # ★ 38 张表的 Drizzle 定义（表定义的镜像，不是建表来源）
│   ├── routes/*.ts          # 一域一文件，src/app.ts 注册
│   └── server.ts            # 入口
├── web/src/pages/*.vue      # 前端页面
├── docs/                    # ← 你在这里
├── REFRESH_ALL.md           # refresh_all 详细手册
└── .workbuddy/memory/       # DATASOURCES.md 原始台账 + 每日工作日志
```

---

## 数据是怎么流的

```
源站 ──fetch_*.py──> PostgreSQL ──src/routes/*.ts──> 前端页面
         │                                              
         ├─ commodities/prices          绝对价格（元/吨、元/克…）
         ├─ market_indices/index_prices 指数（点、美元/天）
         ├─ 实体+时空（vlcc_vessels/positions）← 不塞进时间序列表
         └─ 派生数据不落表：周/月线由 API 聚合，财报单季由视图差分
```

**归表三条规则**（新增数据源时按这个走）：

1. **绝对价格** → `commodities` + `prices`
2. **指数** → `market_indices` + `index_prices`
3. **实体 + 时空流水不硬塞进时间序列表** —— 参考 `vlcc_vessels`（船）+ `vlcc_positions`（船位）。
   一艘船的位置没有「开高低收」，塞进 `index_prices.close` 会丢语义。

---

## 五个反复出现的教训

写在这里是因为它们不只属于某一个源：

1. **「连上了 / 收到报文了」要拆两步验证**：① 通道通 ② **目标对象真的出现**。
   aisstream 全球订阅 43 条/秒看着像有数据，六成来自北海，波斯湾/红海/印度洋全 0。
   → **选源必须用目标区域窄 bbox 单独测密度**，看目标命中数，不看总量。
2. **报错文案会骗人**。aisstream 报「key 无效」，真因是漏发订阅报文；
   深交所 `ProxyError` 看着像「封 IP」，真因是本机代理隧道；
   私募「page 从 1 开始」是把频控 400 误当语义限制。
   → 回到「协议规定的必发步骤」逐条对，别照文案走。
3. **文档字段名 / 单位可能全错**。aisstream 文档写 `MetaData.Latitude` 实测不可用；
   HiFleet 位置接口单位是「分」、`ti` 是 UTC+8，而**同一家**的 `gettraffic` 却是「度」。
   → **先 dump 一条原始报文再写解析**，并留一个不花额度的 `--selftest`。
4. **「过期名单」会伪装成「数据源缺数据」**。先怀疑名录时效，再怀疑覆盖。
   **领域状态 ≠ 缺失**（已转手/已退市 vs 没抓到），前端必须分开渲染。
5. **改了脚本必须整段重跑**。进程启动后再改脚本，跑的还是旧版本
   （2026-09-17 实测：21 分钟抓取一行未入库）。排查第一件事是比对
   **进程启动时间 vs 脚本 mtime**。

---

## 维护本文档

- 新增数据源 → 同时更新 `01-scripts.md`（怎么跑）+ `02-datasources.md`（源与死路），
  表结构变更再更新 `03-database.md`。
- **先改代码，再改文档**，并且两边对齐后跑一次：
  ```bash
  python refresh_all.py --status     # 行数/日期对不对
  ```
- 表格里的行数是**实测值不是估算**，会随时间变化。重建时请重跑 `--status` 再更新，
  不要把旧数字当承诺。
- 文档里凡是出现「死路」「付费墙」「覆盖塌陷」的地方，**都是花过时间换来的**，
  改动前请先确认那条路真的通了。
