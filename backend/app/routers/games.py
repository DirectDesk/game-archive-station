from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import get_db
from ..models import Game
from ..schemas import GameCreate, GameOut, GameUpdate, RefreshMetadataTaskOut
from ..services import refresh_rawg_game_metadata
from ..task_manager import task_manager

router = APIRouter(prefix="/api/games", tags=["games"])


@router.get("", response_model=list[GameOut])
async def list_games(db: AsyncSession = Depends(get_db), q: str = "", source_type: str = "", play_status: str = "", tag: str = "", sort: str = "updated"):
    query = select(Game)
    if q:
        query = query.where(Game.title.ilike(f"%{q}%"))
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
    cover_dir = settings.data_dir / "covers"
    cover_dir.mkdir(parents=True, exist_ok=True)
    path = cover_dir / f"{game_id}_{Path(file.filename or 'cover.jpg').name}"
    path.write_bytes(await file.read())
    game.cover_url = f"/data/covers/{path.name}"
    await db.commit()
    await db.refresh(game)
    return game


@router.post("/{game_id}/refresh-metadata", response_model=RefreshMetadataTaskOut, status_code=202)
async def refresh_metadata(game_id: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if game.source_type != "rawg" or not game.source_id:
        raise HTTPException(422, "仅支持刷新具有 RAWG 数据源 ID 的游戏")
    last_refresh = task_manager.game_refreshes.get(game_id)
    if last_refresh and datetime.utcnow() - last_refresh < timedelta(seconds=60):
        raise HTTPException(429, "请在 60 秒后再次刷新元数据")
    task_manager.game_refreshes[game_id] = datetime.utcnow()
    task = task_manager.create("等待刷新 RAWG 元数据")
    task["result_game_id"] = game_id

    task_manager.run(task, refresh_rawg_game_metadata(game_id))
    return task


@router.get("/{game_id}/resource")
async def open_resource(game_id: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game or not game.resource_url:
        raise HTTPException(404, "该游戏没有资源")
    if game.resource_type == "web_link":
        return RedirectResponse(game.resource_url)
    if game.resource_type in {"nas_cloud", "nas_local"}:
        root = settings.scan_root if game.resource_type == "nas_cloud" else settings.local_game_root
        path = Path(game.resource_url).resolve()
        try:
            path.relative_to(root.resolve())
        except ValueError:
            raise HTTPException(400, "资源路径不在对应的 NAS 挂载目录内")
        if not path.exists():
            raise HTTPException(404, "NAS 文件不存在或路径不可访问")
        if path.is_dir():
            return {"path": str(path), "files": [{"name": child.name, "is_dir": child.is_dir()} for child in sorted(path.iterdir())]}
        return FileResponse(path, filename=path.name)
    raise HTTPException(400, "资源类型无效")
