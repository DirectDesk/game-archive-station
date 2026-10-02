import asyncio
import threading
from datetime import datetime
from uuid import uuid4

from .database import SessionLocal
from .models import AsyncTask


class TaskManager:
    """进程内轻量任务池；任务仅执行网络元数据请求，不访问 WebDAV。"""

    def __init__(self):
        self.tasks: dict[str, dict] = {}
        self.game_refreshes: dict[int, datetime] = {}
        self.latest_scan_id: str | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self._persist_lock: asyncio.Lock | None = None

    def create(self, message: str = "", task_type: str = "", game_id: int | None = None) -> dict:
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        task = {"id": str(uuid4()), "status": "pending", "message": message, "started_at": None, "finished_at": None, "task_type": task_type, "game_id": game_id, "target_path": "", "copied_files": 0, "total_files": 0, "result_game_id": None}
        self.tasks[task["id"]] = task
        self._persist(task)
        return task

    def _persist(self, task: dict):
        snapshot = {key: task.get(key) for key in ("status", "message", "started_at", "finished_at", "task_type", "game_id", "target_path", "copied_files", "total_files")}
        async def save():
            if self._persist_lock is None:
                self._persist_lock = asyncio.Lock()
            async with self._persist_lock:
                async with SessionLocal() as db:
                    row = await db.get(AsyncTask, task["id"])
                    if row is None:
                        row = AsyncTask(id=task["id"], **snapshot)
                        db.add(row)
                    else:
                        for key, value in snapshot.items():
                            setattr(row, key, value)
                    await db.commit()
        try:
            loop = self.loop or asyncio.get_running_loop()
        except RuntimeError:
            return
        if loop.is_running():
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is loop:
                asyncio.create_task(save())
            else:
                asyncio.run_coroutine_threadsafe(save(), loop)

    def start(self, task: dict):
        task["status"], task["started_at"] = "running", datetime.utcnow()
        self._persist(task)

    def complete(self, task: dict, message: str | None = None):
        task["status"], task["finished_at"] = "completed", datetime.utcnow()
        if message is not None:
            task["message"] = message
        self._persist(task)

    def fail(self, task: dict, message: str):
        task["status"], task["message"], task["finished_at"] = "failed", message, datetime.utcnow()
        self._persist(task)

    def update_progress(self, task: dict, **values):
        if threading.current_thread() is threading.main_thread():
            task.update(values)
            self._persist(task)
            return
        loop = self.loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(self.update_progress, task, **values)

    def get(self, task_id: str) -> dict | None:
        return self.tasks.get(task_id)

    def run(self, task: dict, coroutine):
        async def wrapped():
            self.start(task)
            try:
                await coroutine
            except Exception as exc:
                self.fail(task, str(exc))
            finally:
                if task["status"] == "running":
                    self.complete(task)

        asyncio.create_task(wrapped())


task_manager = TaskManager()