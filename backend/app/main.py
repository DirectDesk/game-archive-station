from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

from .config import settings
from .database import Base, SessionLocal, engine
from .models import SystemConfig
from .routers import downloads, games, glossary, metadata, scans, settings as settings_router
from .scheduler import scan_scheduler
from .translation_service import translation_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        columns = (await connection.execute(text("PRAGMA table_info(games)"))).mappings().all()
        if "screenshots" not in {column["name"] for column in columns}:
            await connection.execute(text("ALTER TABLE games ADD COLUMN screenshots TEXT DEFAULT ''"))
        config_columns = (await connection.execute(text("PRAGMA table_info(system_config)"))).mappings().all()
        for name, definition in {"scan_enable": "BOOLEAN DEFAULT 1", "scan_cron": "VARCHAR(50) DEFAULT '0 3 * * *'", "scan_throttle_ms": "INTEGER DEFAULT 50", "scan_root": "VARCHAR(500) DEFAULT '/vol/baidu'", "local_game_root": "VARCHAR(500) DEFAULT '/vol/games'", "download_dir": "VARCHAR(500) DEFAULT '/vol/download/game'", "rawg_api_key": "VARCHAR(200) DEFAULT ''", "auto_translate": "BOOLEAN DEFAULT 0", "translator_type": "VARCHAR(20) DEFAULT 'none'", "tencent_secret_id": "VARCHAR(200) DEFAULT ''", "tencent_secret_key": "VARCHAR(200) DEFAULT ''", "tencent_region": "VARCHAR(50) DEFAULT 'ap-guangzhou'"}.items():
            if name not in {column["name"] for column in config_columns}:
                await connection.execute(text(f"ALTER TABLE system_config ADD COLUMN {name} {definition}"))
        await connection.execute(text("UPDATE games SET resource_type = 'nas_cloud' WHERE resource_type = 'nas_path'"))
        await connection.execute(text("UPDATE games SET resource_type = 'web_link' WHERE resource_type = 'cloud_link'"))
    async with SessionLocal() as db:
        if not await db.get(SystemConfig, 1):
            db.add(SystemConfig(id=1))
            await db.commit()
        await translation_service.load(db)
    # 只注册每日增量任务；不会在服务启动时扫描，更不会自动执行全量扫描。
    async with SessionLocal() as db:
        config = await db.get(SystemConfig, 1)
        if config:
            settings.scan_root = Path(config.scan_root)
            settings.local_game_root = Path(config.local_game_root)
            settings.download_dir = Path(config.download_dir)
            settings.scan_throttle_ms = config.scan_throttle_ms
            settings.rawg_api_key = config.rawg_api_key or settings.rawg_api_key
        scan_scheduler.start(config)
    yield
    scan_scheduler.stop()


app = FastAPI(title="Game Archive", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/data", StaticFiles(directory=settings.data_dir, check_dir=False), name="data")
app.include_router(games.router)
app.include_router(metadata.router)
app.include_router(downloads.router)
app.include_router(scans.router)
app.include_router(glossary.router)
app.include_router(settings_router.router)

frontend = Path(__file__).resolve().parents[1] / "frontend" / "dist"
if frontend.exists():
    app.mount("/", StaticFiles(directory=frontend, html=True), name="frontend")
