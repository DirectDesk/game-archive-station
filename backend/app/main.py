from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, text

from .config import settings
from .database import Base, SessionLocal, engine
from .models import Game, SystemConfig
from .routers import downloads, games, glossary, metadata, scans, settings as settings_router
from .scheduler import scan_scheduler
from .translation_service import translation_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    # 复制默认标签术语表到 data 目录（容器重建后自动恢复）
    import shutil as _shutil
    _default_glossary = Path(__file__).parent / "data" / "tag_glossary.json"
    _target_glossary = settings.data_dir / "tag_glossary.json"
    if _default_glossary.exists() and not _target_glossary.exists():
        _shutil.copy(_default_glossary, _target_glossary)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        columns = (await connection.execute(text("PRAGMA table_info(games)"))).mappings().all()
        existing_game_columns = {column["name"] for column in columns}
        for name, definition in {"screenshots": "TEXT DEFAULT ''", "original_data": "TEXT DEFAULT ''", "version": "VARCHAR(100) DEFAULT ''", "cover_source": "VARCHAR(20) DEFAULT ''", "steam_appid": "VARCHAR(20) DEFAULT ''", "screenshot_source": "VARCHAR(20) DEFAULT ''", "source_ids": "TEXT DEFAULT '{}'", "source_data": "TEXT DEFAULT '{}'", "file_size": "INTEGER DEFAULT 0"}.items():
            if name not in existing_game_columns:
                await connection.execute(text(f"ALTER TABLE games ADD COLUMN {name} {definition}"))
        config_columns = (await connection.execute(text("PRAGMA table_info(system_config)"))).mappings().all()
        for name, definition in {"scan_enable": "BOOLEAN DEFAULT 1", "scan_cron": "VARCHAR(50) DEFAULT '0 3 * * *'", "scan_throttle_ms": "INTEGER DEFAULT 50", "scan_root": "VARCHAR(500) DEFAULT '/vol/baidu'", "local_game_root": "VARCHAR(500) DEFAULT '/vol/games'", "download_dir": "VARCHAR(500) DEFAULT '/vol/download/game'", "rawg_api_key": "VARCHAR(200) DEFAULT ''", "auto_translate": "BOOLEAN DEFAULT 0", "translator_type": "VARCHAR(20) DEFAULT 'none'", "tencent_secret_id": "VARCHAR(200) DEFAULT ''", "tencent_secret_key": "VARCHAR(200) DEFAULT ''", "tencent_region": "VARCHAR(50) DEFAULT 'ap-guangzhou'", "metadata_source_priority": "VARCHAR(200) DEFAULT '[\"rawg\",\"vndb\",\"dlsite\"]'", "cover_source_priority": "VARCHAR(200) DEFAULT '[\"steam\",\"vndb\",\"dlsite\",\"rawg\"]'", "screenshot_source_priority": "VARCHAR(200) DEFAULT '[\"rawg\",\"steam\",\"vndb\",\"dlsite\"]'", "scan_fetch_screenshots": "BOOLEAN DEFAULT 0", "max_screenshots": "INTEGER DEFAULT 5"}.items():
            if name not in {column["name"] for column in config_columns}:
                await connection.execute(text(f"ALTER TABLE system_config ADD COLUMN {name} {definition}"))
        await connection.execute(text("UPDATE games SET resource_type = 'nas_cloud' WHERE resource_type = 'nas_path'"))
        await connection.execute(text("UPDATE games SET resource_type = 'web_link' WHERE resource_type = 'cloud_link'"))
        await connection.execute(text("UPDATE games SET play_status = 'favorite' WHERE play_status = 'archived'"))
        await connection.execute(text("UPDATE games SET play_status = 'playing' WHERE play_status = 'downloading'"))
        # 清理 resource_url 重复的记录（保留 id 最小的）
        await connection.execute(text("""
            DELETE FROM games WHERE id NOT IN (
                SELECT MIN(id) FROM games WHERE resource_url != '' GROUP BY resource_url
            ) AND resource_url != ''
        """))
    async with SessionLocal() as db:
        if not await db.get(SystemConfig, 1):
            db.add(SystemConfig(id=1))
            await db.commit()
        # 迁移旧游戏 original_data → source_data/source_ids
        import json as _json
        all_games = (await db.execute(select(Game))).scalars().all()
        migrated_count = 0
        for _g in all_games:
            if _g.source_data and _g.source_data != "{}":
                continue
            try:
                _old = _json.loads(_g.original_data or "{}")
            except _json.JSONDecodeError:
                _old = {}
            _sd = {}
            _sids = {}
            if _g.source_type and _g.source_type != "custom" and _g.source_id:
                _sd[_g.source_type] = _old
                _sids[_g.source_type] = _g.source_id
            if _old.get("steam_appid") and "steam" not in _sids:
                _sids["steam"] = _old["steam_appid"]
            _g.source_data = _json.dumps(_sd, ensure_ascii=False)
            _g.source_ids = _json.dumps(_sids, ensure_ascii=False)
            migrated_count += 1
        if migrated_count:
            await db.commit()
            print(f"[迁移] 已迁移 {migrated_count} 个游戏的 original_data → source_data/source_ids")
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
