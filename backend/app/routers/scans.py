from datetime import datetime

from fastapi import APIRouter, HTTPException

from ..scanner import scanner
from ..scheduler import scan_scheduler
from ..schemas import AsyncTaskOut, ScanStatusOut, ScanTrigger
from ..task_manager import task_manager

router = APIRouter(prefix="/api/scans", tags=["scans"])


def _scan_status(task: dict | None) -> dict:
    return {
        "id": task["id"] if task else "",
        "status": task["status"] if task else "idle",
        "message": task["message"] if task else "暂无扫描任务",
        "started_at": task["started_at"] if task else None,
        "finished_at": task["finished_at"] if task else None,
        "result_game_id": None,
        "mode": task.get("mode") if task else None,
        "scanned_directories": task.get("scanned_directories", 0) if task else 0,
        "discovered_games": task.get("discovered_games", 0) if task else 0,
        "skipped_directories": task.get("skipped_directories", 0) if task else 0,
        "logs": task.get("logs", []) if task else [],
    }


@router.get("/status", response_model=ScanStatusOut)
async def scan_status():
    task = task_manager.get(task_manager.latest_scan_id) if task_manager.latest_scan_id else None
    status = _scan_status(task)
    status["next_run_at"] = scan_scheduler.next_run_time()
    return status


async def _trigger(full: bool) -> dict:
    if scanner.running:
        raise HTTPException(409, "已有扫描任务正在执行")
    task = task_manager.create("等待开始扫描")
    task_manager.latest_scan_id = task["id"]
    task_manager.run(task, scanner.scan(task, full=full))
    return task


@router.post("/incremental", response_model=ScanStatusOut)
async def trigger_incremental():
    return _scan_status(await _trigger(full=False))


@router.post("/full", response_model=ScanStatusOut)
async def trigger_full(payload: ScanTrigger):
    if not payload.confirm:
        raise HTTPException(422, "全量扫描需要二次确认")
    return _scan_status(await _trigger(full=True))


@router.get("/tasks/{task_id}", response_model=AsyncTaskOut)
async def get_task(task_id: str):
    task = task_manager.get(task_id)
    if not task:
        raise HTTPException(404, "任务不存在")
    return task