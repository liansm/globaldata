# 04 · 部署到云服务器（腾讯云）

> 面向：把这个仓库整体搬到一台 Linux 服务器上，Web + API + PostgreSQL + Python 抓取全部常驻。
> 本文的每一项都是**针对本仓库实测过的约束**（库体积、PG 版本、构建会不会挂、时区、key 注入时机），
> 不是通用模板。通用做法只在你确认「和这里写的一样」时才照抄。

---

## 0. 结论先行

| 决策 | 建议 | 理由 |
|---|---|---|
| 机型 | **轻量应用服务器 4 核 8G**（12M / 2000GB 流量 / 180G SSD） | 库 12 GB，8G 内存才够 PG 分到 2 GB 缓存；轻量支持挂数据盘、不限流量包内出网 |
| 系统 | **Ubuntu 24.04 LTS** | PG 18 需要 PGDG 源，Ubuntu 系的 pg 包管理最顺 |
| 地域 | 只自己用 → **中国香港**；要绑域名对外 → **上海/广州 + ICP 备案** | 大陆地域 80/443 必须备案，这是第一个会卡住的地方 |
| 磁盘 | 系统盘 180G 够，**另挂 100G 数据盘到 `/data`** 放 PG 数据 + 备份 | 数据盘可卸载换机；系统盘跟实例生命周期绑定 |
| 部署方式 | **裸机 + systemd**（本项目推荐） | 单机、要跑 cron、后端免构建，Docker 只增加一层排障成本（见 §6） |
| 后端运行 | `node_modules/.bin/tsx src/server.ts`，**不 `tsc` 构建** | `npm run build` 有 1 处既有类型错误（`commodities.ts:83`），会非零退出 |
| 前端构建 | `npx vite build`，**不要 `npm run build`** | 后者先跑 `vue-tsc --noEmit`，有 2 处既有错误（`Detail.vue:374`、`MarketDetail.vue:424`）会挡住 |
| PostgreSQL | **必须是 18**（本机是 18.3） | Ubuntu 自带 16，`pg_restore` 会报 `unsupported version (1.x) in file header` |

---

## 1. 现状基线（2026-11-10 实测，用来做容量规划）

| 项 | 值 | 影响 |
|---|---|---|
| 数据库总大小 | **12 GB** | 磁盘起步 180G 起步没有压力，但别用 40G 系统盘的秒杀款 |
| 最大表 | `fund_nav` 5.96 GB | 全表扫会走盘，`shared_buffers` 给 2 GB |
| 其次 | `stock_financials` 1.90 GB / `stock_prices` 1.23 GB / `stock_shareholders` 1.15 GB / `private_fund_nav` 0.92 GB | — |
| 对象 | 39 张表 + 1 个视图（`v_stock_financials_quarterly`）+ 103 个索引 | `pg_dump` 会一并带走，空库不用先建表 |
| 本地 PG | **18.3** | 服务器必须同主版本 |
| 本地 Node / npm | v22.22.2 / 10.9.7 | 服务器装 Node 22 LTS |
| Python 依赖 | `requirements.txt`：akshare / tushare / pandas / requests / bs4 / lxml / psycopg2-binary / python-dotenv / pypinyin | **akshare 没钉版本**，见 §5.4 |
| 一轮全量刷新 | 约 **4.5~6 分钟**，24 个脚本 | cron 单次跑得完，不需要常驻进程 |

---

## 2. 目标形态

单机四层，3000 端口不对外：

```
公网 ──► Nginx :80/:443
          ├─ /            → /var/www/globaldata   （web/dist 静态文件，SPA fallback）
          ├─ /api/*       → 127.0.0.1:3000         （Fastify）
          └─ /health      → 127.0.0.1:3000
                              │
                          PostgreSQL 18 @ 127.0.0.1:5432，数据在 /data/pgdata
                              ▲
                    cron 定时跑 refresh_all.py（Python venv @ /opt/globaldata/.venv）
```

安全组只放 **22 / 80 / 443**，5432 和 3000 一律不放开。

---

## 3. 选型：买什么

### 3.1 轻量应用服务器 vs CVM

| | 轻量（Lighthouse） | CVM |
|---|---|---|
| 4核8G 参考价 | 约 630~880 元/年（活动价，含 10~12M 带宽 + 1500~2000GB 月流量） | 标准型 S5 4核8G 约 1385 元/年，**仅 1M 带宽**，加带宽另算 |
| 带宽 | 固定 M 数 + 流量包，超量按量计费（只算出网） | 固定带宽或按流量 |
| 数据盘 | **支持**，同可用区，SSD 20~4000GB，步长 10GB，每地域配额 20 块 | 支持，更灵活 |
| 升降配 | **只能升不能降** | 可升可降 |
| 适合 | 单机、自用、预算优先 | 需要精细调带宽/磁盘、要升降配 |

> 这个项目就一个进程 + 一个 PG + 一天一次 cron，**轻量 4核8G 是最省的选择**。
> 唯一要注意的是「不能降配」——别一上来买 8核16G。

### 3.2 地域与备案（最先会卡住的一步）

- **大陆地域（北京/上海/广州）**：域名解析到大陆 IP、走 **80/443 对外提供 Web 服务必须 ICP 备案**。
  个人备案一般 1~3 周。轻量应用服务器同样适用这条。
- **不想备案的两种做法**：
  1. 只走 `http://IP:8080` 这类**非标端口**，不绑域名。技术上能用，政策上不支持对外站，**内部自用常见**。
  2. 买**中国香港 / 新加坡**地域 —— 免备案，代价是跨境上传 12 GB 慢、大陆访问延迟 30~60 ms。
- **只想自己看数据 → 直接香港，别折腾备案。**

---

## 4. 数据迁移：12 GB 怎么搬上去

这是整个流程里最耗时的一步，先算清楚再动手。

```bash
# 本机（Windows，用系统 Python 所在环境的 pg_dump；版本必须 18.x）
pg_dump -h 127.0.0.1 -U postgres -Fc -Z9 globaldata > globaldata.dump
```

- `-Fc` 自定义格式 + `-Z9`：12 GB 数据预计压到 **2.5~4 GB**（大量 numeric/timestamp，压缩率不错）。
- 上传时间取决于**家宽上行**：30 Mbps → 约 20~35 分钟；10 Mbps → 1.5~2 小时。
- **必须用可断点续传的方式**，别用 `scp` 一断全废：

```bash
rsync --partial --info=progress2 -e "ssh -p 22" globaldata.dump ubuntu@<IP>:/tmp/
```

- 恢复（4 并行，8G 内存机器 `-j 4` 合适，别开 8）：

```bash
pg_restore -h 127.0.0.1 -U gd -d globaldata -j 4 --no-owner --no-privileges /tmp/globaldata.dump
psql -h 127.0.0.1 -U gd -d globaldata -c "ANALYZE;"
```

⚠ **`--no-owner` 必须加**：本机 dump 里的属主是 `postgres`，服务器上用普通角色 `gd` 恢复，
不加会在每条 `ALTER TABLE ... OWNER TO` 上报错（能恢复完，但日志一片红，容易误判成失败）。

⚠ 恢复速度参考：12 GB 在 4 核机器上 `-j 4` 大约 10~20 分钟，其中 `fund_nav` 的 103 个索引重建占一半。

---

## 5. 部署步骤（裸机 + systemd）

### 5.1 系统初始化

```bash
# 时区 —— 这条不做，cron 和 refresh_all 的交易日判断一起错 8 小时
sudo timedatectl set-timezone Asia/Shanghai
timedatectl                       # 确认 Time zone: Asia/Shanghai (CST, +0800)

sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl nginx python3-venv python3-dev build-essential libpq-dev ufw

# 数据盘挂到 /data（控制台挂载后）
lsblk                             # 找到 vdb 之类
sudo mkfs.ext4 /dev/vdb && sudo mkdir -p /data && sudo mount /dev/vdb /data
echo "/dev/vdb /data ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab

# 防火墙（安全组之外的第二道）
sudo ufw allow 22,80,443/tcp && sudo ufw --force enable
```

建运行用户：

```bash
sudo useradd -m -s /bin/bash globaldata
sudo mkdir -p /opt/globaldata /var/www/globaldata /var/log/globaldata /data/backups
sudo chown -R globaldata:globaldata /opt/globaldata /var/www/globaldata /var/log/globaldata
```

### 5.2 PostgreSQL 18（必须走 PGDG 源）

```bash
sudo install -d /usr/share/postgresql-common/pgdg
sudo curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
     https://www.postgresql.org/media/keys/ACCC4CF8.asc
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] \
https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt update && sudo apt install -y postgresql-18
```

> PGDG 在国内可能很慢，可把 `apt.postgresql.org` 换成清华镜像 `mirrors.tuna.tsinghua.edu.cn/postgresql/repos/apt`。

把数据目录挪到数据盘（**安装时自动建的 cluster 要先 drop**）：

```bash
sudo systemctl stop postgresql
sudo pg_dropcluster 18 main --stop
sudo mkdir -p /data/pgdata && sudo chown postgres:postgres /data/pgdata && sudo chmod 700 /data/pgdata
sudo pg_createcluster 18 main --datadir=/data/pgdata --start
```

调优（`/etc/postgresql/18/main/conf.d/zz-globaldata.conf`）：

```ini
listen_addresses = 'localhost'
max_connections = 50
shared_buffers = 2GB
effective_cache_size = 6GB
work_mem = 32MB
maintenance_work_mem = 512MB
max_wal_size = 4GB
min_wal_size = 1GB
random_page_cost = 1.1
effective_io_concurrency = 200
wal_compression = on
timezone = 'Asia/Shanghai'
log_timezone = 'Asia/Shanghai'
```

```bash
sudo systemctl restart postgresql
sudo -u postgres psql -c "CREATE ROLE gd LOGIN PASSWORD '换成强密码';"
sudo -u postgres psql -c "CREATE DATABASE globaldata OWNER gd ENCODING 'UTF8';"
```

> `random_page_cost=1.1` 不是抄来的：云盘是 SSD，PG 默认 4.0 是给机械盘的，
> 不改的话 `fund_nav` 这类大表的索引扫描会被优化器主动放弃，变成全表扫。

### 5.3 Node 与代码

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install -y nodejs        # Ubuntu 自带的 18 跑不动 Fastify 5

sudo -u globaldata git clone https://github.com/liansm/globaldata.git /opt/globaldata
cd /opt/globaldata && sudo -u globaldata npm ci
```

私有仓库要 token 的话，用 `https://<token>@github.com/...` 克隆，**别把 token 提交进 git remote**：

```bash
sudo -u globaldata git remote set-url origin https://github.com/liansm/globaldata.git   # 克隆后立刻改回
```

环境变量（**两个文件，别放混**）：

```bash
# /opt/globaldata/.env —— 给 Node 和 Python 用
DATABASE_URL=postgresql://gd:强密码@127.0.0.1:5432/globaldata
TUSHARE_TOKEN=...
AISSTREAM_API_KEY=...
HIFLEET_API_KEY=...
PORT=3000
HOST=127.0.0.1

# /opt/globaldata/web/.env —— 只给 vue 构建用（vite envDir = web/）
VITE_TMAP_KEY=...
VITE_TDT_KEY=...
```

```bash
sudo chown globaldata:globaldata /opt/globaldata/.env /opt/globaldata/web/.env
sudo chmod 600 /opt/globaldata/.env
```

⚠ **`HOST=127.0.0.1` 必须显式写**：`src/server.ts` 默认 `0.0.0.0`，漏了就是 3000 端口裸奔在公网
（虽然安全组挡着，但多一道没必要）。

### 5.4 Python 环境

```bash
cd /opt/globaldata
sudo -u globaldata python3 -m venv .venv
sudo -u globaldata .venv/bin/pip install -U pip
sudo -u globaldata .venv/bin/pip install -r requirements.txt
```

⚠ **akshare 必须钉版本**。`requirements.txt` 里它没写版本号，而 akshare 改接口很频繁
（本项目已经因为 akshare 的品种表串过种）。装完立刻锁：

```bash
sudo -u globaldata .venv/bin/pip freeze > requirements.lock.txt
```

以后重装一律 `pip install -r requirements.lock.txt`。**别在服务器上 `pip install -U akshare`**。

冒烟：

```bash
sudo -u globaldata .venv/bin/python refresh_all.py --status
```

`--status` 只查库不抓数据，输出行数/最新日期对得上就说明 Python + 数据库 + `.env` 都通了。

### 5.5 后端 systemd

`/etc/systemd/system/globaldata-api.service`：

```ini
[Unit]
Description=globaldata API (Fastify)
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=globaldata
WorkingDirectory=/opt/globaldata
EnvironmentFile=/opt/globaldata/.env
ExecStart=/opt/globaldata/node_modules/.bin/tsx src/server.ts
Restart=always
RestartSec=5
StandardOutput=append:/var/log/globaldata/api.log
StandardError=append:/var/log/globaldata/api.log

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now globaldata-api
curl -s --noproxy '*' http://127.0.0.1:3000/health      # {"status":"ok",...}
```

> 用 `tsx` 直跑而不是 `node dist/server.js`，原因是 `npm run build`（= `tsc`）有 1 处既有类型错误
> （`src/routes/commodities.ts:83`）。tsx 在生产跑这个体量完全够，启动快 1~2 秒，省一个构建步骤。
> 什么时候该改回去：等那处类型错误修了，或者要做多实例负载均衡。

### 5.6 前端构建与发布

```bash
cd /opt/globaldata/web
sudo -u globaldata npm ci
sudo -u globaldata npx vite build            # ⚠ 不要 npm run build
sudo rsync -a --delete dist/ /var/www/globaldata/
```

⚠ 这里有两个**必踩**的坑：

1. **`npm run build` 会失败** —— 它是 `vue-tsc --noEmit && vite build`，而
   `Detail.vue:374`、`MarketDetail.vue:424` 有 2 处既有类型错误（已知、暂不修）。
   `&&` 短路 → 打包直接不跑。用 `npx vite build` 绕过类型检查。
2. **`VITE_*` 是构建时注入的**，不是运行时读。所以 `web/.env` 必须在 `vite build` **之前**放好，
   而且**以后改地图 key 必须重新 build + 重新发布**，重启服务没用。

### 5.7 Nginx

`/etc/nginx/sites-available/globaldata`：

```nginx
server {
    listen 80;
    server_name _;                      # 绑域名后换成你的域名

    root /var/www/globaldata;
    index index.html;

    gzip on;
    gzip_min_length 1k;
    gzip_types text/css application/javascript application/json image/svg+xml;

    # SPA 路由：不加这条，刷新 /stocks/600519 会 404
    location / {
        try_files $uri $uri/ /index.html;
    }

    location /api/ {
        proxy_pass         http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header   Host $host;
        proxy_set_header   X-Real-IP $remote_addr;
        proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 120s;        # 财报/基金大查询会超过默认 60s
    }

    location = /health {
        proxy_pass http://127.0.0.1:3000;
    }

    # 静态资源长缓存（文件名带 hash）
    location /assets/ {
        expires 30d;
        add_header Cache-Control "public, immutable";
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/globaldata /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
```

有域名后上 HTTPS：

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your.domain.com      # 自动改上面的 443 配置并续期
```

### 5.8 定时抓取（cron）

```bash
sudo -u globaldata crontab -e
```

```cron
# 工作日 18:30 刷一轮（A 股收盘 + 部分源当日更新后）
30 18 * * 1-5 cd /opt/globaldata && .venv/bin/python -u refresh_all.py --fetch >> /var/log/globaldata/refresh.log 2>&1

# 每天 03:00 备份
0 3 * * * pg_dump -Fc globaldata | gzip > /data/backups/gd-$(date +\%F).dump.gz
15 3 * * * find /data/backups -name 'gd-*.dump.gz' -mtime +7 -delete
```

⚠ 三件事：

1. **cron 里的 `%` 必须写成 `\%`**（crontab 把 `%` 当换行符），写错就是备份任务天天失败。
2. **时区**：cron 用系统时区，§5.1 那条 `timedatectl` 不做的话，这条 18:30 实际是北京时间 02:30。
3. **哪些脚本不该进 cron**：
   - `refresh_all.py` 默认已排除耗时的（`fetch_funds.py`、`fetch_fund_nav.py --history`、
     `fetch_private_funds.py`、`--with-financials`、`--with-vlcc`）——这些保持**手动跑**，
     它们是「慢且漏跑不丢历史」或「要跑 1~2 小时」的，塞进每日 cron 只会互相踩。
   - `fetch_private_nav_licai.py` 在 `OPT_IN_SCRIPTS` 里、约 8~11 分钟，可以单独挂一条周任务。
   - **需要登录态/人肉操作的源，一律不要进 cron**。

日志轮转 `/etc/logrotate.d/globaldata`：

```
/var/log/globaldata/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
```

---

## 6. Docker Compose 方案（备选）

**什么时候值得选它**：你要频繁换机器、要开第二套环境（测试/生产）、或者以后想把 PG 换成云数据库。
**什么时候不值得**：就这一台机器、就你一个人用 —— 多一层网络和卷的排障成本，换不来什么。

大致形态（真要用我再出完整文件）：

```yaml
services:
  db:
    image: postgres:18
    environment:
      POSTGRES_DB: globaldata
      POSTGRES_PASSWORD: ${PG_PASSWORD}
      TZ: Asia/Shanghai
    volumes:
      - /data/pgdata:/var/lib/postgresql/data
    shm_size: 1gb                    # ← 忘了这条，pg_restore -j4 会撞共享内存不足
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
  api:
    build: .
    env_file: .env
    depends_on: { db: { condition: service_healthy } }
  nginx:
    image: nginx:alpine
    ports: ["80:80"]
    volumes:
      - ./web/dist:/usr/share/nginx/html:ro
      - ./deploy/nginx.conf:/etc/nginx/conf.d/default.conf:ro
  # Python 抓取不建议进容器：要么加个 cron 容器，要么直接宿主机 venv 跑 cron（更简单）
```

Docker 路线的额外注意点：

- `postgres:18` 的数据卷必须落在数据盘，否则换机器要重搬 12 GB。
- 容器的时区默认 UTC，**每个 service 都要显式 `TZ=Asia/Shanghai`**。
- `shm_size` 默认 64 MB，`pg_restore -j 4` 会直接报错。
- 前端构建仍然要在**镜像构建阶段**注入 `VITE_*`，`ARG`/`ENV` 传进去。

---

## 7. 这个项目特有的坑（上线前逐条对）

| # | 坑 | 症状 | 处理 |
|---|---|---|---|
| 1 | `npm run build` 被既有类型错误挡住 | 打包直接中断 | 用 `npx vite build`；或先修 `Detail.vue:374`、`MarketDetail.vue:424`、`commodities.ts:83` |
| 2 | `VITE_TMAP_KEY` / `VITE_TDT_KEY` 是构建时注入 | 页面正常但**地图整片空白/弹「鉴权失败」** | 构建前放好 `web/.env`；改 key 要重新 build |
| 3 | 地图 key 有 Referer 白名单 | 天地图 403、底图连陆地都没有；腾讯弹鉴权条 | 在腾讯位置服务 / 天地图控制台把**服务器域名或 IP** 加进白名单 |
| 4 | 服务器默认 UTC | cron 比预期晚/早 8 小时；`refresh_all` 的交易时段判断错位 | `timedatectl set-timezone Asia/Shanghai`，PG 的 `timezone` 也一起改 |
| 5 | 本机 PG 是 **18.3** | `pg_restore: unsupported version (1.x) in file header` | 服务器必须用 PGDG 装 `postgresql-18`，不能用 Ubuntu 自带的 16 |
| 6 | `src/server.ts` 默认 `HOST=0.0.0.0` | 3000 端口公网可达 | `.env` 里显式 `HOST=127.0.0.1`，安全组只开 22/80/443 |
| 7 | `@fastify/cors` 是 `origin: true` | 所有源都能跨域调你的 API | 走 Nginx 同域反代后其实不跨域，风险低；要收就改成自己的域名，并同步改 `src/app.ts` |
| 8 | akshare 没钉版本 | 某天所有 akshare 源的脚本一起报错、字段全变 | 装好立刻 `pip freeze > requirements.lock.txt` |
| 9 | 长任务铁律在服务器上同样成立 | 「跑了但一行没入库」 | 改脚本后确认 cron 那一轮用的是新版本；比对「进程启动时间 vs 脚本 mtime」 |
| 10 | `data/`、`.workbuddy/`、各类 `*.progress.json` 都在 `.gitignore` 里 | clone 下来缺进度文件 → 断点续跑从头开始 | 需要续跑状态的话单独 `rsync` 过去，别指望 git |
| 11 | `000020 / 000670` 这类港股与深市 A 股撞号 | 基金持仓反查串到另一家公司 | 已修（名称闸门），但迁移后建议抽一只基金验一下持仓公司名 |

---

## 8. 上线验收清单

按顺序做完，每条都要有可见的输出，别凭感觉：

```bash
# 1. 数据完整性：行数与你本机一致
sudo -u globaldata .venv/bin/python refresh_all.py --status

# 2. 库能扫、索引在
sudo -u postgres psql -d globaldata -c "ANALYZE;"
sudo -u postgres psql -d globaldata -c "select count(*) from pg_indexes where schemaname='public';"   # 期望 103

# 3. 后端活着
curl -s --noproxy '*' http://127.0.0.1:3000/health

# 4. 走 Nginx 也活着（同域，不跨域）
curl -s http://127.0.0.1/api/markets | head -c 200

# 5. 一个真实业务接口
curl -s http://127.0.0.1/api/stocks/600519/financials | head -c 300

# 6. 前端
curl -sI http://127.0.0.1/ | head -3
# 浏览器打开，逐页点一遍，重点看：地图页（key/白名单）、股票详情「财务报表」tab、
# 基金页（fund_nav 5.96 GB，大表查询延迟）、YTD 区间切换

# 7. 抓取能跑（先只跑一个快的，别一上来全量）
sudo -u globaldata .venv/bin/python -u fetch_ccfi.py --dry-run

# 8. cron 真的会触发
sudo systemctl status cron
grep CRON /var/log/syslog | tail -5
```

**第 7 步别跳过**：很多人是「服务起来了、页面能看」就当部署完成，三天后发现 cron 里
Python 一直 `ModuleNotFoundError` —— 因为 cron 的 PATH 和你 SSH 里不一样，`cd` + 相对路径
`.venv/bin/python` 就是为了避开这个。

---

## 9. 成本量级（2026-11 活动价，以官网实时价为准）

| 项 | 估算 |
|---|---|
| 轻量 4核8G（12M / 2000GB 流量 / 180G SSD） | 约 **630~880 元/年**（首单活动价） |
| 数据盘 100G SSD | 约 **70 元/月**（或先用系统盘 180G，暂不买） |
| 域名 | 约 50~100 元/年（不绑域名可省） |
| 备案 | 免费，但要等 1~3 周 |
| 对象存储 COS（备份异地留一份，可选） | 12 GB × 3 版本 ≈ **几元/月** |

**建议先按月买一台试跑 1~2 周**（按量/月付），确认内存够（`free -h` 看 PG 有没有被 OOM kill）、
流量够，再转包年。轻量**只升不降**，别一步到位买大。

---

## 10. 还没定的两件事

1. **部署方式**：本文主线是裸机 + systemd。如果你更想 Docker，说一声，我把 `Dockerfile`、
   `docker-compose.yml`、`deploy/nginx.conf` 一起写出来（并处理 `shm_size`、`TZ`、`VITE_*` 构建参数）。
2. **对外访问还是自用**：只自用 → 香港地域 + IP 直连，今天就能上；
   要绑域名给别人的看 → 先把备案排上，这是唯一有日历等待的环节。

---

*最后更新：2026-11-10。库体积、PG 版本、构建失败项均为当日本机实测值，改动机器或修了类型错误后请更新本文。*
