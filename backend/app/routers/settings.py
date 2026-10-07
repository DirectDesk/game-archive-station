from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ..clients.rawg_client import RawgClient
from ..clients.translator import get_translator
from ..config import settings
from ..database import get_db
from ..models import SystemConfig
from ..scheduler import scan_scheduler
from ..schemas import SettingsUpdate, TranslatorTestPayload
from ..translation_service import translation_service

router = APIRouter(prefix="/api/settings", tags=["settings"])


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
    for field in {"rawg_api_key", "tencent_secret_id", "tencent_secret_key"}:
        data[f"{field}_configured"] = bool(getattr(config, field))
    # 外置下载（aria2）：只暴露「是否已配置」，不外泄 secret
    data["aria_configured"] = bool(settings.aria_secret or settings.aria_secret_file)
    data["aria_rpc"] = settings.aria_rpc
    return data


@router.get("")
async def get_settings(db: AsyncSession = Depends(get_db)):
    return _output(await _config(db))


@router.put("")
async def update_settings(payload: SettingsUpdate, db: AsyncSession = Depends(get_db)):
    config = await _config(db)
    for field, value in payload.model_dump(exclude_none=True).items():
        setattr(config, field, value)
    await db.commit()
    await db.refresh(config)
    settings.scan_root = Path(config.scan_root)
    settings.local_game_root = Path(config.local_game_root)
    settings.scan_throttle_ms = config.scan_throttle_ms
    settings.rawg_api_key = config.rawg_api_key
    settings.download_dir = Path(config.download_dir)
    settings.download_engine = config.download_engine or "internal"
    scan_scheduler.reload(config)
    await translation_service.load(db)
    return _output(config)


@router.post("/test-translator")
async def test_translator(payload: TranslatorTestPayload | None = None, db: AsyncSession = Depends(get_db)):
    """测试翻译服务连接。

    支持两种用法：
      - 不带 payload：用已保存的配置测试（设置页"测试连接"按钮）
      - 带 payload：用表单里的临时配置测试（保存前验证，避免存了坏配置）
    """
    config = await _config(db)
    ttype = (payload.translator_type if payload and payload.translator_type else config.translator_type) or "none"
    if ttype == "none":
        raise HTTPException(400, "未选择翻译 API")
    secret_id = (payload.tencent_secret_id if payload and payload.tencent_secret_id else config.tencent_secret_id) or ""
    secret_key = (payload.tencent_secret_key if payload and payload.tencent_secret_key else config.tencent_secret_key) or ""
    region = (payload.tencent_region if payload and payload.tencent_region else config.tencent_region) or "ap-guangzhou"
    if ttype == "tencent" and (not secret_id or not secret_key):
        raise HTTPException(400, "腾讯翻译需要 SecretId 与 SecretKey")
    try:
        translator = get_translator(
            ttype,
            secret_id=secret_id,
            secret_key=secret_key,
            region=region,
        )
    except Exception as exc:
        raise HTTPException(400, f"翻译器初始化失败：{exc}") from exc
    # 用一句英文测试（中文->中文字典类翻译器可能原样返回，测不出问题）
    probe = "Good morning"
    try:
        translated = await translator.translate(probe)
    except Exception as exc:
        raise HTTPException(502, f"翻译服务连接失败：{exc}") from exc
    if not translated:
        raise HTTPException(502, "翻译服务返回空结果")
    suspicious = translated == probe
    if suspicious:
        raise HTTPException(502, "翻译服务无响应（返回原文），请检查密钥或网络")
    return {"ok": True, "result": translated, "translator": ttype}


@router.post("/test-aria2")
async def test_aria2():
    """测试外置下载（aria2 JSON-RPC）连通性。"""
    from ..download_engine import aria2_version

    try:
        info = aria2_version()
    except Exception as exc:                                      # noqa: BLE001
        raise HTTPException(502, f"aria2 连接失败：{exc}") from exc
    return {"ok": True, **info}


@router.post("/test-rawg")
async def test_rawg():
    try:
        await RawgClient().search_games("test", page_size=1)
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(502, f"RAWG 连接失败：{exc}") from exc

@router.post("/recompute-game-types")
async def recompute_game_types(db: AsyncSession = Depends(get_db)):
    """按数据源/标签推断规则重算所有游戏的平台类型（存量数据回填）。"""
    from ..services import recompute_all_game_types

    changed = await recompute_all_game_types()
    return {"ok": True, "changed": changed}
