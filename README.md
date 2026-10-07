# Game Archive

轻量**单用户**游戏归档库。扫描本地 / 云盘目录，自动匹配元数据、抓取封面与截图、翻译标签与简介，并支持云盘转存与后台下载。

当前版本 **v1.7.1**

---

## 功能特性

**多数据源元数据**
- 内置 4 个数据源客户端：**RAWG / Steam / DLsite / VNDB**，扫描时按优先级依次匹配，刷新时多来源补充
- 匹配失败自动落 `custom`（自定义）来源，后续可手动匹配
- 跨源择优：封面 / 截图 / 标签 / 简介按来源优先级解析

**翻译链路**
- 标签术语表（JSON 导入 + 分类筛选 + 翻页），术语表优先 → 机器翻译兜底 → 自动学习
- 简介与标签机器翻译（Google / Tencent 可切换，带重试退避）
- 质量校验：截断、音译垃圾等异常结果保留原文

**字段语义与用户锁定**
- 游戏名拆分为 `title`（原文名）与 `title_cn`（中文译名），展示中文优先，改名不再被原文覆盖
- `locked_fields` 用户锁定：手动改过的简介 / 标签 / 中文名**不会被翻译或术语表流程静默覆盖**

**归档与下载**
- 目录扫描（增量 / 全量）、定时扫描（Cron）
- 云盘链接与 NAS 本地路径并存
- 云盘游戏转存到本地目录，带后台任务与进度
- 「下载到本机」支持 **HTTP Range**（IDM / 迅雷等多连接下载器可并行、可续传），
  目录打包为 **STORED zip**（不压缩、带精确 `Content-Length`，客户端能显示进度）
- 下载方式可选 **内置下载** / **外置下载（aria2）**，见下文「外置下载」

**界面**
- 首页卡片分页（18 / 36 / 54 每页）、类型筛选
- 游戏平台类型徽章：`pc` / `android` / `gal`
- 详情页：显示原文对照、截图预览、重新翻译、手动匹配元数据

---

## 技术栈

| 层 | 技术 |
|---|---|
| 后端 | FastAPI 0.115 · SQLAlchemy 2.0（async）· SQLite（aiosqlite）· httpx · APScheduler |
| 前端 | Vue 3.5 · Vite 6 · Tailwind CSS 4（单文件 `App.vue`） |
| 部署 | Docker / Docker Compose |

---

## 项目结构

```text
game-archive/
├── backend/
│   ├── app/
│   │   ├── main.py                  # 应用入口 / 启动迁移
│   │   ├── config.py                # 配置
│   │   ├── database.py              # 异步 SQLAlchemy + SQLite
│   │   ├── models.py                # ORM 模型
│   │   ├── schemas.py               # Pydantic 模型
│   │   ├── services.py              # 核心业务：匹配 / 刮削 / 截图
│   │   ├── scanner.py               # 目录扫描与自动匹配
│   │   ├── scheduler.py             # 定时扫描（APScheduler）
│   │   ├── task_manager.py          # 后台任务
│   │   ├── translation_service.py   # 翻译链路 + 术语表
│   │   ├── cover_service.py         # 封面解析
│   │   ├── net.py                   # 网络工具（IPv4 优先）
│   │   ├── clients/
│   │   │   ├── rawg_client.py
│   │   │   ├── steam_client.py
│   │   │   ├── dlsite_client.py
│   │   │   ├── translator.py / translator_base.py
│   │   │   ├── google_translator.py
│   │   │   └── tencent_translator.py
│   │   └── routers/
│   │       ├── games.py             # 游戏记录
│   │       ├── metadata.py          # 元数据搜索 / 匹配
│   │       ├── downloads.py         # 下载任务
│   │       ├── scans.py             # 扫描
│   │       ├── glossary.py          # 术语表
│   │       └── settings.py          # 设置
│   ├── tools/
│   │   └── refresh_check.py         # 回归自检工具
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── App.vue                  # 单文件应用（全部 UI）
│   │   ├── main.js
│   │   └── style.css
│   ├── public/icons/                # pc / android / gal 平台图标
│   ├── index.html
│   ├── package.json
│   ├── postcss.config.js
│   └── vite.config.js
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

---

## 快速开始

```bash
git clone git@github.com:DirectDesk/game-archive-station.git
cd game-archive-station
```

### 本地开发

```bash
# 后端（默认 http://127.0.0.1:8000）
cd backend && pip install -r requirements.txt
uvicorn app.main:app --reload

# 前端（默认 http://127.0.0.1:5173）
cd frontend && npm install && npm run dev
```

### Docker 部署

1. 在 [RAWG](https://rawg.io/signup) 注册账号（国内邮箱即可，免费额度每月 20000 次请求）。
   复制 `.env.example` 为 `.env`，填写 `RAWG_API_KEY`。
2. 按实际目录修改 `docker-compose.yml` 中各 volume 的**左侧宿主路径**
   （数据目录、游戏源目录、下载目录）。
3. 构建并启动：

   ```bash
   docker compose up -d --build
   ```

4. 浏览器访问 `http://<NAS_IP>:8080`（宿主 8080 → 容器 8000）。

> **改了前端只改 `frontend/`？** `frontend/dist` 在 compose 里是 bind mount，
> 镜像内的构建产物会被挂载覆盖。改前端后执行 `npm run build`，把 `dist/` 部署到挂载目录即可，
> **不必重建镜像**。

### 外置下载（aria2，可选）

**设置 → 下载设置** 里可切换下载方式：

| 方式 | 说明 |
|---|---|
| **内置下载**（默认） | 应用自己读源文件：挂载源（云盘挂载）用多线程并发直读；直链源用 httpx 多连接。 |
| **外置下载** | 把**直链源**交给外部 aria2（JSON-RPC）下载，应用轮询 `tellStatus` 显示进度。 |

> ⚠️ **挂载源（fuse，如 `/vol/baidu`）始终走内置下载**——aria2 只接受 URL，
> 读不了挂载路径。外置下载只对「有 `http(s)` 直链」的来源生效。
> 进度不会丢：应用通过 `aria2.tellStatus` 轮询 `completedLength / totalLength`，外置同样有进度条。

#### 1. 部署 aria2（NAS 宿主）

`/home/admin/oldl/aria2.conf`：

```ini
enable-rpc=true
rpc-listen-all=true
rpc-listen-port=6800
rpc-secret=<你的密钥>       # 32 位 [A-Za-z0-9]（不是 hex）
file-allocation=none        # 默认 falloc 会让表观大小瞬间到位，误导进度
continue=true
```

`/etc/systemd/system/oldl-aria2.service`（注意 **User=admin**）：

```ini
[Unit]
Description=aria2 for game-archive
[Service]
User=admin
ExecStart=/usr/bin/aria2c --conf-path=/home/admin/oldl/aria2.conf
Restart=on-failure
[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now oldl-aria2
```

#### 2. 在 `.env` 里填密钥并让 compose 引用

```bash
# 与 docker-compose.yml 同目录的 .env（chmod 600，已在 .gitignore）
ARIA_RPC=http://<NAS_IP>:6800/jsonrpc
ARIA_SECRET=<与 aria2.conf 里一致的密钥>
DOWNLOAD_ENGINE=external
```

⚠️ **光建 `.env` 不生效**：compose 的 `environment` 必须写成 `${...}` 引用：

```yaml
environment:
  - ARIA_RPC=${ARIA_RPC:-http://192.168.20.10:6800/jsonrpc}
  - ARIA_SECRET=${ARIA_SECRET:-}
  - DOWNLOAD_ENGINE=${DOWNLOAD_ENGINE:-internal}
```

改完 `docker compose up -d`（无需重建镜像）。

#### 3. 目录权限

aria2 以 **admin** 跑，而下载目录常是容器 root 建的 0755：

```bash
sudo chmod 777 <下载目录>          # 例如 /vol1/1000/docker/game-archive/downloads
```

#### 常见坑

- **`Download aborted.` 且 `tellStatus` 里 `pieces` 已正确** → 先看
  `/home/admin/oldl/aria2.log`，多半是 `dir` 传了**容器内路径**（如 `/vol/download`，
  宿主不存在）或目录权限 `Permission denied`，**不是网络/密钥/直链问题**。
- **`dir` 必须给宿主真实路径**：`/vol1/1000/docker/game-archive/downloads`。
- secret 是 32 位 `[A-Za-z0-9]`，**不是 hex**；读取时不要用 `sudo -S`（密码回显会污染）。
- 设置页「测试 aria2 连接」可直接验证 RPC 是否可达。

---

## 环境变量

见 `.env.example`：

| 变量 | 默认 | 说明 |
|---|---|---|
| `RAWG_API_KEY` | 空 | RAWG API Key，**必填**（否则 RAWG 源不可用） |
| `SCAN_ENABLE` | `true` | 是否启用定时扫描 |
| `SCAN_CRON` | `0 3 * * *` | 定时扫描的 Cron 表达式 |
| `SCAN_THROTTLE_MS` | `50` | 扫描节流间隔（毫秒） |
| `SCAN_WEEKLY_FULL_CHECK` | `false` | 是否每周做一次全量校验 |
| `SCAN_ROOT` | `/vol/baidu` | 容器内扫描根目录（WebDAV 挂载点） |
| `FUSE_THREADS` | `8` | 挂载源并发直读的线程数（4~8 最佳，16 反而降速） |
| `DOWNLOAD_ENGINE` | `internal` | 下载方式：`internal` 内置 / `external` 外置（aria2） |
| `ARIA_RPC` | `http://192.168.20.10:6800/jsonrpc` | aria2 JSON-RPC 地址（仅外置下载用） |
| `ARIA_SECRET` | 空 | aria2 RPC 密钥（仅外置下载用，**放 `.env`，勿入库**） |

`.env` 已在 `.gitignore` 中，**不会入库**。

---

## 说明

- **单用户应用**：无账号体系，请勿直接暴露到公网，建议仅在局域网内使用。
- **数据全部落在 `/app/data`**：SQLite 数据库 `games.db`、封面与截图缓存都在这里，
  备份该目录即可。
- **网络**：容器显式配置了纯 IPv4 公共 DNS。若你的网络 IPv6 出口不可用，
  不要改回默认 DNS，否则容器内域名解析会间歇性失败。

---

## 更新日志

应用内 **设置 → 关于** 可查看完整更新日志（v1.2.0 至今）。
