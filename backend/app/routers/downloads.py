import asyncio
import os
import queue
import threading
import time
import zipfile
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import SessionLocal, get_db
from ..download_engine import (copy_path, iter_file_parallel, iter_file_range,
                                pick_engine, threads_for, zip_stored_size)
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
    """目录打包成 zip（**STORED 不压缩**）。

    每个文件用**并行分段读**喂给 zip writer（读端提速），
    写 zip 本身仍单线程（zip 必须顺序写）。

    ⚠️ 用 STORED 而非 DEFLATED：游戏文件（apk/obb/rar/zip）本身已是压缩格式，
    再 deflate 纯属浪费 CPU（实测把整包下载压到 ~10MB/s）。STORED 后大小可精确
    预计算（`download_engine.zip_stored_size`），从而给出 Content-Length 让客户端显示进度。
    """
    # 缓冲别太小：8KB 分片 + maxsize=32 只有 256KB，生产者会被频繁阻塞，
    # 实测把目录下载压在 ~12MB/s。改为整块入队 + 更大缓冲，减少锁/调度开销。
    chunks: queue.Queue[bytes | None] = queue.Queue(maxsize=64)

    class Writer:
        def write(self, data):
            chunks.put(data)
            return len(data)

        def flush(self):
            pass

        def seekable(self):
            return False

    def produce():
        try:
            with zipfile.ZipFile(Writer(), "w", zipfile.ZIP_STORED, allowZip64=True) as archive:
                for file in sorted(directory.rglob("*")):
                    if not file.is_file():
                        continue
                    arc = str(file.relative_to(directory.parent)).replace(os.sep, "/")
                    try:
                        st = file.stat()
                        info = zipfile.ZipInfo(arc, date_time=time.localtime(st.st_mtime)[:6])
                        info.compress_type = zipfile.ZIP_STORED
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


def _parse_range(value: str | None, size: int):
    """解析单段 Range：bytes=start-end / bytes=start- / bytes=-suffix。

    返回 (start, end)（含端点）或 None。只支持单段（IDM 的并发连接每条约一段）。
    """
    if not value:
        return None
    value = value.strip()
    if not value.lower().startswith("bytes="):
        return None
    spec = value[6:].split(",")[0].strip()
    if "-" not in spec:
        return None
    a, b = spec.split("-", 1)
    try:
        if a == "":
            n = int(b)
            if n <= 0:
                return None
            start, end = max(0, size - n), size - 1
        else:
            start = int(a)
            end = int(b) if b else size - 1
    except ValueError:
        return None
    end = min(end, size - 1)
    if start < 0 or start > end or start >= size:
        return None
    return start, end


def _download_headers(path: Path, filename: str) -> dict:
    return {"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"}


@router.get("/games/{game_id}/download-to-pc")
async def download_to_pc(game_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if game.resource_type not in {"nas_cloud", "nas_local"}:
        raise HTTPException(400, "仅 NAS 资源支持下载")
    path = _safe_path(game)
    engine = pick_engine(game.resource_type, path)
    threads = threads_for(engine)
    if path.is_file():
        size = path.stat().st_size
        headers = _download_headers(path, quote(path.name))
        headers["Accept-Ranges"] = "bytes"
        if engine == "fuse" and threads > 1:
            # 挂载源：分段并发读 → 流式响应；**支持 Range**，让 IDM 等外部下载器
            # 开多条连接并行（每条连接内部仍多线程读，实测能跑满）。
            rng = _parse_range(request.headers.get("range"), size)
            if rng is None:
                start, end, status = 0, size - 1, 200
                body = iter_file_parallel(path, threads, start, end - start + 1)
            else:
                start, end = rng
                status = 206
                headers["Content-Range"] = f"bytes {start}-{end}/{size}"
                # Range 走**单线程顺序读**：并行度交给客户端的多条连接，
                # 避免「连接数 × 内部线程数」超额订阅（实测 8 连接反而更慢）。
                body = iter_file_range(path, start, end - start + 1)
            headers["Content-Length"] = str(end - start + 1)
            return StreamingResponse(
                body, status_code=status, media_type="application/octet-stream", headers=headers)
        # 本地磁盘：FileResponse 原生支持 Range + Content-Length
        return FileResponse(path, filename=path.name,
                            media_type="application/octet-stream", headers=headers)
    # 目录：STORED 流式 zip；预计算大小 → 客户端能显示下载进度
    filename = quote(f"{path.name}.zip")
    headers = _download_headers(path, filename)
    headers["Accept-Ranges"] = "none"
    zsize = zip_stored_size(path)
    if zsize:
        headers["Content-Length"] = str(zsize)
    return StreamingResponse(_zip_stream(path, threads), media_type="application/zip",
                             headers=headers)


@router.head("/games/{game_id}/download-to-pc")
async def download_to_pc_head(game_id: int, db: AsyncSession = Depends(get_db)):
    """HEAD 探测：让 IDM 等外部下载器知道是否支持 Range 与文件大小（不传 body）。"""
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if game.resource_type not in {"nas_cloud", "nas_local"}:
        raise HTTPException(400, "仅 NAS 资源支持下载")
    path = _safe_path(game)
    if path.is_file():
        headers = _download_headers(path, quote(path.name))
        headers["Accept-Ranges"] = "bytes"
        headers["Content-Length"] = str(path.stat().st_size)
        headers["Content-Type"] = "application/octet-stream"
        return Response(status_code=200, headers=headers)
    headers = _download_headers(path, quote(f"{path.name}.zip"))
    headers["Accept-Ranges"] = "none"
    headers["Content-Type"] = "application/zip"
    zsize = zip_stored_size(path)
    if zsize:
        headers["Content-Length"] = str(zsize)
    return Response(status_code=200, headers=headers)


async def _transfer(game_id: int, target_subdir: str, task: dict):
    task_manager.start(task, "正在转存…")
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
