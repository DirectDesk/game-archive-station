from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ..clients.rawg_client import RawgClient
from ..clients.translator import get_translator
from ..config import settings
from ..database import get_db
from ..models import SystemConfig
from ..scheduler import scan_scheduler
from ..schemas import SettingsUpdate
from ..translation_service import translation_service

router = APIRouter(prefix="/api/settings", tags=["settings"])
SECRET_FIELDS = {"rawg_api_key", "tencent_secret_id", "tencent_secret_key"}


async def _config(db: AsyncSession) -> SystemConfig:
    config = await db.get(SystemConfig, 1)
    if not config:
        config = SystemConfig(id=1)
        db.add(config)
        await db.commit()
        await db.refresh(config)
    return config


def _output(config: SystemConfig) -> dict:
    data = {column.name: getattr(config, column.name) for column in config.__table__.columns}
    for field in SECRET_FIELDS:
        data[field] = "******" if data[field] else ""
        data[f"{field}_configured"] = bool(getattr(config, field))
    return data


@router.get("")
async def get_settings(db: AsyncSession = Depends(get_db)):
    return _output(await _config(db))


@router.put("")
async def update_settings(payload: SettingsUpdate, db: AsyncSession = Depends(get_db)):
    config = await _config(db)
    for field, value in payload.model_dump(exclude_none=True).items():
        if field in SECRET_FIELDS and not value:
            continue
        setattr(config, field, value)
    await db.commit()
    await db.refresh(config)
    settings.scan_root = Path(config.scan_root)
    settings.local_game_root = Path(config.local_game_root)
    settings.scan_throttle_ms = config.scan_throttle_ms
    settings.rawg_api_key = config.rawg_api_key
    settings.download_dir = Path(config.download_dir)
    scan_scheduler.reload(config)
    await translation_service.load(db)
    return _output(config)


@router.post("/test-translator")
async def test_translator(db: AsyncSession = Depends(get_db)):
    config = await _config(db)
    translated = await get_translator(config.translator_type, secret_id=config.tencent_secret_id, secret_key=config.tencent_secret_key, region=config.tencent_region).translate("Hello")
    if translated == "Hello" and config.translator_type != "none":
        raise HTTPException(502, "翻译服务连接失败")
    return {"ok": True, "result": translated}


@router.post("/test-rawg")
async def test_rawg():
    try:
        await RawgClient().search_games("test", page_size=1)
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(502, f"RAWG 连接失败：{exc}") from exc