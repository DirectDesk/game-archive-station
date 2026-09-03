import asyncio
import queue
import shutil
import threading
import zipfile
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import SessionLocal, get_db
from ..models import Game, SystemConfig
from ..task_manager import task_manager

router = APIRouter(prefix="/api", tags=["downloads"])


class TransferRequest(BaseModel):
    target_subdir: str = Field(default="", max_length=255)


def _safe_path(game: Game) -> Path:
    root = settings.scan_root if game.resource_type == "nas_cloud" else settings.local_game_root
    path = Path(game.resource_url).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise HTTPException(400, "资源路径不在允许的挂载目录内") from exc
    if not path.exists():
        raise HTTPException(404, "NAS 文件不存在或路径不可访问")
    return path


def _zip_stream(directory: Path):
    chunks: queue.Queue[bytes | None] = queue.Queue(maxsize=32)

    class Writer:
        def write(self, data):
            for offset in range(0, len(data), 8192):
                chunks.put(data[offset : offset + 8192])
            return len(data)
        def flush(self): pass
        def seekable(self): return False

    def produce():
        try:
            with zipfile.ZipFile(Writer(), "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
                for file in directory.rglob("*"):
                    if file.is_file():
                        archive.write(file, file.relative_to(directory.parent))
        finally:
            chunks.put(None)

    threading.Thread(target=produce, daemon=True).start()
    while (chunk := chunks.get()) is not None:
        yield chunk


@router.get("/games/{game_id}/download-to-pc")
async def download_to_pc(game_id: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if game.resource_type not in {"nas_cloud", "nas_local"}:
        raise HTTPException(400, "仅 NAS 资源支持下载")
    path = _safe_path(game)
    if path.is_file():
        return FileResponse(path, filename=path.name, media_type="application/octet-stream")
    filename = quote(f"{path.name}.zip")
    return StreamingResponse(_zip_stream(path), media_type="application/zip", headers={"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"})


def _copy(source: Path, target: Path, task: dict):
    files = [item for item in source.rglob("*") if item.is_file()] if source.is_dir() else [source]
    task["total_files"] = len(files)
    target.mkdir(parents=True, exist_ok=False)
    for file in files:
        destination = target / file.relative_to(source) if source.is_dir() else target / file.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file, destination)
        task["copied_files"] += 1


async def _transfer(game_id: int, target_subdir: str, task: dict):
    task_manager.start(task)
    try:
        async with SessionLocal() as db:
            game = await db.get(Game, game_id)
            config = await db.get(SystemConfig, 1)
            if not game or not config:
                raise RuntimeError("游戏或系统配置不存在")
            source = _safe_path(game)
        name = target_subdir.strip() or game.title
        if not name or Path(name).name != name:
            raise RuntimeError("目标子目录名无效")
        root = Path(config.download_dir).resolve()
        target, index = root / name, 1
        while target.exists():
            target = root / f"{name}_{index}"
            index += 1
        task["target_path"] = str(target)
        await asyncio.to_thread(_copy, source, target, task)
        task_manager.complete(task, f"转存完成：{target}")
    except Exception as exc:
        task_manager.fail(task, str(exc))


@router.post("/games/{game_id}/transfer-to-local", status_code=202)
async def transfer_to_local(game_id: int, payload: TransferRequest, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if game.resource_type != "nas_cloud":
        raise HTTPException(400, "仅 NAS 挂载云盘资源支持转存")
    _safe_path(game)
    task = task_manager.create("等待转存到 NAS 本地")
    task.update({"game_id": game_id, "copied_files": 0, "total_files": 0, "error": "", "target_path": ""})
    asyncio.create_task(_transfer(game_id, payload.target_subdir, task))
    return task


@router.get("/downloads/transfer/{task_id}")
async def transfer_status(task_id: str):
    task = task_manager.get(task_id)
    if not task or "copied_files" not in task:
        raise HTTPException(404, "转存任务不存在")
    return task