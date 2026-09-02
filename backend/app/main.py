from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select

from .config import settings
from .database import Base, SessionLocal, engine
from .downloader import DownloadWorker
from .models import DownloadTask
from .routers import downloads, games, metadata, scans
from .scheduler import scan_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    worker = DownloadWorker()
    downloads.set_worker(worker)
    await worker.start()
    async with SessionLocal() as db:
        pending_tasks = await db.scalars(
            select(DownloadTask).where(DownloadTask.status.in_(["pending", "running"]))
        )
        for task in pending_tasks:
            task.status = "pending"
            await worker.enqueue(task.id)
        await db.commit()
    # 只注册每日增量任务；不会在服务启动时扫描，更不会自动执行全量扫描。
    scan_scheduler.start()
    yield
    scan_scheduler.stop()
    await worker.stop()


app = FastAPI(title="Game Archive", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/data", StaticFiles(directory=settings.data_dir, check_dir=False), name="data")
app.include_router(games.router)
app.include_router(metadata.router)
app.include_router(downloads.router)
app.include_router(scans.router)

frontend = Path(__file__).resolve().parents[1] / "frontend" / "dist"
if frontend.exists():
    app.mount("/", StaticFiles(directory=frontend, html=True), name="frontend")
