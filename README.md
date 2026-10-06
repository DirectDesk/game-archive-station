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
