import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import settings
from .scanner import scanner
from .task_manager import task_manager

logger = logging.getLogger(__name__)


class ScanScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()

    async def run_incremental(self):
        if scanner.running:
            logger.info("定时增量扫描跳过：已有扫描正在执行")
            return
        task = task_manager.create("定时增量扫描")
        task_manager.latest_scan_id = task["id"]
        task_manager.run(task, scanner.scan(task, full=False))

    async def run_weekly_check(self):
        if scanner.running:
            logger.info("每周目录校验跳过：已有扫描正在执行")
            return
        task = task_manager.create("每周目录数量校验")
        task_manager.latest_scan_id = task["id"]
        task_manager.run(task, scanner.count_directories(task))

    def start(self):
        if not settings.scan_enable:
            logger.info("定时增量扫描已通过 SCAN_ENABLE=false 禁用")
            return
        try:
            trigger = CronTrigger.from_crontab(settings.scan_cron)
        except ValueError:
            logger.exception("SCAN_CRON 无效，未启动扫描调度器：%s", settings.scan_cron)
            return
        self.scheduler.add_job(self.run_incremental, trigger, id="incremental-scan", replace_existing=True, coalesce=True, max_instances=1)
        if settings.scan_weekly_full_check:
            self.scheduler.add_job(self.run_weekly_check, "cron", day_of_week="sun", hour=4, minute=0, id="weekly-directory-check", replace_existing=True, coalesce=True, max_instances=1)
        self.scheduler.start()
        logger.info("定时增量扫描已启动，cron=%s", settings.scan_cron)

    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    def next_run_time(self):
        job = self.scheduler.get_job("incremental-scan")
        return job.next_run_time if job else None


scan_scheduler = ScanScheduler()