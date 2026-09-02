import asyncio
from datetime import datetime
from uuid import uuid4


class TaskManager:
    """进程内轻量任务池；任务仅执行网络元数据请求，不访问 WebDAV。"""

    def __init__(self):
        self.tasks: dict[str, dict] = {}
        self.game_refreshes: dict[int, datetime] = {}
        self.latest_scan_id: str | None = None

    def create(self, message: str = "") -> dict:
        task = {"id": str(uuid4()), "status": "pending", "message": message, "started_at": None, "finished_at": None, "result_game_id": None}
        self.tasks[task["id"]] = task
        return task

    def start(self, task: dict):
        task["status"], task["started_at"] = "running", datetime.utcnow()

    def complete(self, task: dict, message: str | None = None):
        task["status"], task["finished_at"] = "completed", datetime.utcnow()
        if message is not None:
            task["message"] = message

    def fail(self, task: dict, message: str):
        task["status"], task["message"], task["finished_at"] = "failed", message, datetime.utcnow()

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