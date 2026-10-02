from datetime import datetime, timedelta
from pathlib import Path

import json

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..clients.rawg_client import RawgClient
from ..cover_service import cache_cover
from ..database import get_db
from ..models import Game, SystemConfig
from ..schemas import GameCreate, GameOut, GameUpdate, RefreshMetadataTaskOut
from ..services import fetch_game_screenshots, refresh_game_metadata, refresh_rawg_game_metadata, refresh_vndb_game_metadata, search_steam, resolve_tags
from ..task_manager import task_manager

router = APIRouter(prefix="/api/games", tags=["games"])


@router.get("", response_model=list[GameOut])
async def list_games(db: AsyncSession = Depends(get_db), q: str = "", source_type: str = "", play_status: str = "", tag: str = "", sort: str = "updated"):
    query = select(Game)
    if q:
        term = f"%{q}%"
        query = query.where(or_(Game.title.ilike(term), Game.alias.ilike(term), Game.developer.ilike(term), Game.publisher.ilike(term), Game.series.ilike(term)))
    if source_type:
        query = query.where(Game.source_type == source_type)
    if play_status:
        query = query.where(Game.play_status == play_status)
    tags = [value.strip() for value in tag.split(",") if value.strip()]
    if tags:
        query = query.where(or_(*(Game.tags.ilike(f"%{value}%") for value in tags)))
    query = query.order_by(desc(Game.rating) if sort == "rating" else desc(Game.created_at) if sort == "created" else desc(Game.updated_at))
    return list((await db.scalars(query)).all())


@router.get("/all-tags")
async def all_tags(db: AsyncSession = Depends(get_db)):
    counts: dict[str, int] = {}
    for value in await db.scalars(select(Game.tags).where(Game.tags != "")):
        for tag in {item.strip() for item in value.split(",") if item.strip()}:
            counts[tag] = counts.get(tag, 0) + 1
    return [{"tag": tag, "count": count} for tag, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]


@router.post("", response_model=GameOut)
async def create_game(payload: GameCreate, db: AsyncSession = Depends(get_db)):
    game = Game(**payload.model_dump())
    db.add(game)
    await db.commit()
    await db.refresh(game)
    return game


@router.get("/{game_id}", response_model=GameOut)
async def get_game(game_id: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    return game


@router.put("/{game_id}", response_model=GameOut)
async def update_game(game_id: int, payload: GameUpdate, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    for key, value in payload.model_dump().items():
        setattr(game, key, value)
    await db.commit()
    await db.refresh(game)
    return game


@router.delete("/{game_id}", status_code=204)
async def delete_game(game_id: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    await db.delete(game)
    await db.commit()


@router.post("/{game_id}/cover", response_model=GameOut)
async def upload_cover(game_id: int, file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if (file.content_type or "") not in {"image/jpeg", "image/png"}:
        raise HTTPException(415, "封面仅支持 JPG 或 PNG 格式")
    cover_dir = settings.data_dir / "covers"
    cover_dir.mkdir(parents=True, exist_ok=True)
    path = cover_dir / f"{game_id}_custom.jpg"
    path.write_bytes(await file.read())
    game.cover_url = f"/data/covers/{path.name}"
    game.cover_source = "custom"
    await db.commit()
    await db.refresh(game)
    return game


@router.post("/{game_id}/cover-source", response_model=GameOut)
async def change_cover_source(
    game_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
        source = payload.get("source")
        file = None
    else:
        form = await request.form()
        source = form.get("source")
        file = form.get("file")
    allowed = {"auto", "steam", "vndb", "dlsite", "rawg", "custom"}
    if source not in allowed:
        raise HTTPException(422, "无效的封面来源")

    try:
        metadata = json.loads(game.original_data or "{}")
        if not isinstance(metadata, dict):
            metadata = {}
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    urls = metadata.get("cover_urls", {})
    if not isinstance(urls, dict):
        urls = {}
    existing_source = game.cover_source or game.source_type
    if game.cover_url.startswith(("http://", "https://")) and existing_source != "custom":
        urls.setdefault(existing_source, game.cover_url)

    if source == "auto":
        config = await db.get(SystemConfig, 1)
        try:
            priority = json.loads(config.cover_source_priority if config else "[]")
        except (TypeError, json.JSONDecodeError):
            priority = []
        source = next((item for item in priority if item in allowed - {"auto", "custom"} and (
            (item == "steam" and bool(game.steam_appid)) or
            (item in urls and bool(urls[item])) or
            (item == game.source_type and bool(game.cover_url))
        )), "")
        if not source:
            raise HTTPException(422, "没有可用的封面来源")

    if source == "custom":
        if not file or (file.content_type or "") not in {"image/jpeg", "image/png"}:
            raise HTTPException(415, "自定义封面仅支持 JPG 或 PNG 格式")
        cover_dir = settings.data_dir / "covers"
        cover_dir.mkdir(parents=True, exist_ok=True)
        path = cover_dir / f"{game_id}_custom.jpg"
        path.write_bytes(await file.read())
        cover_url = f"/data/covers/{path.name}"
    else:
        metadata["cover_urls"] = urls
        game.original_data = json.dumps(metadata, ensure_ascii=False)
        cover_url = urls.get(source, "")
        if source == "steam" and game.steam_appid:
            cover_url = RawgClient.steam_cover_url(game.steam_appid)
        elif source == existing_source and game.cover_url and not game.cover_url.startswith("/data/covers/"):
            cover_url = game.cover_url
        if not cover_url:
            raise HTTPException(422, f"没有可用的 {source} 封面")
        try:
            cover_url = await cache_cover(game.id, cover_url, source)
        except Exception as exc:
            raise HTTPException(502, f"封面下载失败：{exc}") from exc
    game.cover_url = cover_url
    game.cover_source = source
    await db.commit()
    await db.refresh(game)
    return game


@router.get("/{game_id}/tags/resolve", response_model=GameOut)
async def resolve_game_tags(game_id: int, source: str = "", db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="游戏不存在")
    config = await db.get(SystemConfig, 1)
    if not config:
        config = SystemConfig(id=1)
    await resolve_tags(game, config, source)
    await db.commit()
    await db.refresh(game)
    return game


@router.get("/{game_id}/screenshots/fetch", response_model=GameOut)
async def fetch_screenshots(game_id: int, source: str = "", db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    try:
        existing = json.loads(game.screenshots or "[]")
    except json.JSONDecodeError:
        existing = []
    # 已有 URL 但全是远程 URL 时，仍执行缓存到本地。
    has_local = any(url.startswith("/data/") for url in existing)
    if existing and not source and has_local:
        return game
    config = await db.get(SystemConfig, 1)
    if not config:
        config = SystemConfig(id=1)
        db.add(config)
        await db.flush()
    await fetch_game_screenshots(game, config, source)
    await db.commit()
    await db.refresh(game)
    return game


@router.post("/{game_id}/screenshots", response_model=GameOut)
async def upload_screenshot(game_id: int, files: list[UploadFile] = File(...), db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if any((file.content_type or "") not in {"image/jpeg", "image/png"} for file in files):
        raise HTTPException(415, "截图仅支持 JPG 或 PNG 格式")
    directory = settings.data_dir / "screenshots"
    directory.mkdir(parents=True, exist_ok=True)
    try:
        values = json.loads(game.screenshots or "[]")
    except json.JSONDecodeError:
        values = []
    for file in files:
        suffix = ".png" if file.content_type == "image/png" else ".jpg"
        path = directory / f"{game_id}_custom_{len(values)}{suffix}"
        path.write_bytes(await file.read())
        values.append(f"/data/screenshots/{path.name}")
    game.screenshots = json.dumps(values)
    game.screenshot_source = "custom"
    await db.commit()
    await db.refresh(game)
    return game


@router.delete("/{game_id}/screenshots/{index}", response_model=GameOut)
async def delete_screenshot(game_id: int, index: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    try:
        values = json.loads(game.screenshots or "[]")
    except json.JSONDecodeError:
        values = []
    if index < 0 or index >= len(values):
        raise HTTPException(404, "截图不存在")
    url = values.pop(index)
    if url.startswith("/data/screenshots/"):
        path = (settings.data_dir / "screenshots" / Path(url).name).resolve()
        if path.parent == (settings.data_dir / "screenshots").resolve() and path.exists():
            path.unlink()
    game.screenshots = json.dumps(values)
    if not values:
        game.screenshot_source = ""
    await db.commit()
    await db.refresh(game)
    return game


@router.post("/{game_id}/refresh-metadata", response_model=RefreshMetadataTaskOut, status_code=202)
async def refresh_metadata(game_id: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if game.source_type == "custom":
        # custom 游戏按标题搜索 RAWG（优先）和 VNDB，取第一个匹配结果。
        from ..services import search_vndb

        matched = None
        try:
            rawg_results = await RawgClient().search_games(game.title, page_size=1)
            if rawg_results:
                matched = {"source_type": "rawg", "source_id": rawg_results[0]["source_id"]}
        except Exception:
            pass
        if not matched:
            # Steam fallback：中文名搜索支持好
            try:
                import re as _re
                steam_results = await search_steam(game.title)
                steam_results = [
                    r for r in steam_results
                    if not _re.search(r"(?i)(soundtrack|ost|original soundtrack|dlc|demo|trial)", r.get("title", ""))
                ]
                if steam_results:
                    matched = {"source_type": "steam", "source_id": steam_results[0]["source_id"]}
            except Exception:
                pass
        if not matched:
            try:
                vndb_results = await search_vndb(game.title)
                if vndb_results:
                    matched = {"source_type": "vndb", "source_id": vndb_results[0]["source_id"]}
            except Exception:
                pass
        if not matched:
            # DLsite fallback：优先从标题提取 RJ/VJ/BJ 编号精准查询
            try:
                from ..clients.dlsite_client import DlsiteClient
                dlsite = DlsiteClient()
                workno = dlsite.extract_workno(game.title)
                if workno and await dlsite.get_work_by_id(workno):
                    matched = {"source_type": "dlsite", "source_id": workno}
                else:
                    dlsite_results = await dlsite.search_games(game.title, page_size=1)
                    if dlsite_results:
                        matched = {"source_type": "dlsite", "source_id": dlsite_results[0]["source_id"]}
            except Exception:
                pass
        if not matched:
            raise HTTPException(404, "未在 RAWG/Steam/VNDB/DLsite 找到匹配的游戏，请手动编辑")
        game.source_type = matched["source_type"]
        game.source_id = matched["source_id"]
        await db.commit()
    elif game.source_type not in {"rawg", "steam", "vndb", "dlsite"} or not game.source_id:
        raise HTTPException(422, "仅支持刷新具有 RAWG/Steam/VNDB/DLsite 数据源 ID 的游戏")
    last_refresh = task_manager.game_refreshes.get(game_id)
    if last_refresh and datetime.utcnow() - last_refresh < timedelta(seconds=60):
        raise HTTPException(429, "请在 60 秒后再次刷新元数据")
    task_manager.game_refreshes[game_id] = datetime.utcnow()
    task = task_manager.create(f"等待刷新 {game.source_type.upper()} 元数据")
    task["result_game_id"] = game_id

    if game.source_type == "rawg":
        refresh_task = refresh_rawg_game_metadata(game_id)
    elif game.source_type == "vndb":
        refresh_task = refresh_vndb_game_metadata(game_id)
    else:
        # steam / dlsite 用通用 refresh_game_metadata（已在 services.py 中支持）
        refresh_task = refresh_game_metadata(game_id)
    task_manager.run(task, refresh_task)
    return task
