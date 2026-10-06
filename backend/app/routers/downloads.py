import asyncio
import os
import queue
import threading
import time
import zipfile
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import SessionLocal, get_db
from ..download_engine import copy_path, iter_file_parallel, pick_engine, threads_for
from ..models import AsyncTask, Game, SystemConfig
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


def _zip_stream(directory: Path, threads: int):
    """目录打包成 zip。

    每个文件用**并行分段读**喂给 zip writer（读端提速），
    写 zip 本身仍单线程（zip 必须顺序写）。
    """
    chunks: queue.Queue[bytes | None] = queue.Queue(maxsize=32)

    class Writer:
        def write(self, data):
            for offset in range(0, len(data), 8192):
                chunks.put(data[offset: offset + 8192])
            return len(data)

        def flush(self):
            pass

        def seekable(self):
            return False

    def produce():
        try:
            with zipfile.ZipFile(Writer(), "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
                for file in sorted(directory.rglob("*")):
                    if not file.is_file():
                        continue
                    arc = str(file.relative_to(directory.parent)).replace(os.sep, "/")
                    try:
                        st = file.stat()
                        info = zipfile.ZipInfo(arc, date_time=time.localtime(st.st_mtime)[:6])
                        info.compress_type = zipfile.ZIP_DEFLATED
                        info.external_attr = (st.st_mode & 0xFFFF) << 16
                    except Exception:                             # noqa: BLE001
                        info = arc
                    with archive.open(info, "w") as target:
                        if threads > 1:
                            for chunk in iter_file_parallel(file, threads):
                                target.write(chunk)
                        else:
                            with open(file, "rb") as fh:
                                while True:
                                    data = fh.read(1024 * 1024)
                                    if not data:
                                        break
                                    target.write(data)
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
    engine = pick_engine(game.resource_type, path)
    threads = threads_for(engine)
    if path.is_file():
        filename = quote(path.name)
        headers = {"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"}
        if engine == "fuse" and threads > 1:
            # 挂载源：分段并发读 → 流式响应（替代单流 FileResponse，实测快约 4 倍）
            headers["Content-Length"] = str(path.stat().st_size)
            return StreamingResponse(iter_file_parallel(path, threads),
                                     media_type="application/octet-stream", headers=headers)
        return FileResponse(path, filename=path.name, media_type="application/octet-stream")
    filename = quote(f"{path.name}.zip")
    return StreamingResponse(_zip_stream(path, threads), media_type="application/zip",
                             headers={"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"})


async def _transfer(game_id: int, target_subdir: str, task: dict):
    task_manager.start(task)
    try:
        async with SessionLocal() as db:
            game = await db.get(Game, game_id)
            config = await db.get(SystemConfig, 1)
            if not game or not config:
                raise RuntimeError("游戏或系统配置不存在")
            source = _safe_path(game)
            resource_type = game.resource_type
        name = target_subdir.strip() or game.title
        if not name or Path(name).name != name:
            raise RuntimeError("目标子目录名无效")
        root = Path(config.download_dir).resolve()
        target, index = root / name, 1
        while target.exists():
            target = root / f"{name}_{index}"
            index += 1
        task_manager.update_progress(task, target_path=str(target))

        cancel = threading.Event()

        def on_progress(done_bytes, total_bytes, done_files, total_files):
            task_manager.update_progress(
                task, copied_bytes=int(done_bytes), total_bytes=int(total_bytes),
                copied_files=int(done_files), total_files=int(total_files))

        ok, err, _files, _bytes = await asyncio.to_thread(
            copy_path, source, target, resource_type, on_progress, cancel)
        if not ok:
            raise RuntimeError(err or "转存失败")
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
    task = task_manager.create("等待转存到 NAS 本地", task_type="transfer", game_id=game_id)
    asyncio.create_task(_transfer(game_id, payload.target_subdir, task))
    return task


@router.get("/downloads/transfer/{task_id}")
async def transfer_status(task_id: str):
    async with SessionLocal() as db:
        row = await db.get(AsyncTask, task_id)
        if not row or row.task_type != "transfer":
            raise HTTPException(404, "转存任务不存在")
        task = {key: getattr(row, key) for key in (
            "id", "status", "message", "started_at", "finished_at", "task_type",
            "game_id", "target_path", "copied_files", "total_files",
            "copied_bytes", "total_bytes")}
    task_manager.tasks[task_id] = task
    return task
