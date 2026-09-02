import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from .clients.rawg_client import RawgClient
from .config import settings
from .database import SessionLocal
from .models import Game, SystemConfig

logger = logging.getLogger(__name__)


class LibraryScanner:
    """串行目录扫描器：只读取目录条目及目录 mtime，绝不读取游戏文件内容。"""

    def __init__(self):
        self.running = False

    @staticmethod
    async def _directories(path: Path) -> list[Path]:
        def read_directories():
            # scandir 仅读取当前层的目录元数据；不读取文件内容或文件级 mtime。
            with os.scandir(path) as entries:
                return [Path(entry.path) for entry in entries if entry.is_dir(follow_symlinks=False)]

        return await asyncio.to_thread(read_directories)

    @staticmethod
    async def _mtime(path: Path) -> float:
        return (await asyncio.to_thread(path.stat)).st_mtime

    @staticmethod
    async def _get_config() -> SystemConfig:
        async with SessionLocal() as db:
            config = await db.get(SystemConfig, 1)
            if not config:
                config = SystemConfig(id=1)
                db.add(config)
                await db.commit()
                await db.refresh(config)
            return config

    async def _create_game_from_folder(self, folder: Path) -> bool:
        async with SessionLocal() as db:
            exists = await db.scalar(select(Game.id).where(Game.resource_url == str(folder)))
            if exists:
                return False

            # 文件夹名是唯一可用的低 IO 发现信息；不进入文件夹读取文件。
            client = RawgClient()
            candidates = await client.search_games(folder.name, page_size=1)
            if not candidates:
                return False
            metadata = await client.get_game_detail(candidates[0]["source_id"])
            metadata.update({"resource_type": "nas_path", "resource_url": str(folder), "play_status": "archived"})
            db.add(Game(**metadata))
            await db.commit()
            logger.info("扫描发现并归档游戏目录：%s", folder)
            return True

    async def scan(self, task: dict, full: bool = False) -> None:
        if self.running:
            raise RuntimeError("已有扫描任务正在执行")
        self.running = True
        task.update({"mode": "full" if full else "incremental", "scanned_directories": 0, "discovered_games": 0, "skipped_directories": 0, "logs": []})
        started_at = datetime.utcnow()
        try:
            if not await asyncio.to_thread(settings.scan_root.is_dir):
                raise RuntimeError(f"扫描根目录不可访问：{settings.scan_root}")
            config = await self._get_config()
            last_scan_at = None if full else config.last_scan_at
            if not full and last_scan_at is None:
                # 首次运行只建立根目录 mtime 基线，绝不能隐式变成一次全量遍历。
                async with SessionLocal() as db:
                    current = await db.get(SystemConfig, 1)
                    if current:
                        current.last_scan_at = datetime.utcnow()
                        await db.commit()
                task["message"] = "首次增量扫描仅建立时间基线，未遍历网盘；如需索引已有游戏请手动执行全量扫描"
                logger.info(task["message"])
                return
            stack = [settings.scan_root]
            while stack:
                directory = stack.pop()
                try:
                    directory_mtime = await self._mtime(directory)
                    # mtime 未变的目录整个子树均被剪枝，日常扫描只访问极少目录。
                    if last_scan_at and directory_mtime <= last_scan_at.timestamp():
                        task["skipped_directories"] += 1
                        continue
                    children = await self._directories(directory)
                except OSError as exc:
                    logger.exception("读取 WebDAV 目录失败，停止扫描：%s", directory)
                    raise RuntimeError(f"WebDAV 目录读取失败：{exc}") from exc

                task["scanned_directories"] += 1
                task["logs"] = (task["logs"] + [f"已读取：{directory}"])[-20:]
                # 叶子目录视为游戏目录，避免读取其中的游戏文件。
                if not children and directory != settings.scan_root:
                    if await self._create_game_from_folder(directory):
                        task["discovered_games"] += 1
                else:
                    stack.extend(children)
                await asyncio.sleep(settings.scan_throttle_ms / 1000)

            # 仅完整、无错误的扫描才推进时间戳，避免失败后遗漏目录。
            async with SessionLocal() as db:
                config = await db.get(SystemConfig, 1)
                if config:
                    config.last_scan_at = datetime.utcnow()
                    await db.commit()
            task["message"] = f"扫描完成：读取 {task['scanned_directories']} 个目录，发现 {task['discovered_games']} 个游戏"
            logger.info("%s扫描完成，耗时 %s", "全量" if full else "增量", datetime.utcnow() - started_at)
        except Exception:
            logger.exception("%s扫描失败", "全量" if full else "增量")
            raise
        finally:
            self.running = False

    async def count_directories(self, task: dict) -> None:
        """每周可选校验：只计数目录，不创建游戏、不请求元数据。"""
        if self.running:
            logger.info("每周目录校验跳过：已有扫描正在执行")
            return
        self.running = True
        task.update({"mode": "weekly-check", "scanned_directories": 0, "discovered_games": 0, "skipped_directories": 0, "logs": []})
        try:
            if not await asyncio.to_thread(settings.scan_root.is_dir):
                raise RuntimeError(f"扫描根目录不可访问：{settings.scan_root}")
            stack = [settings.scan_root]
            while stack:
                directory = stack.pop()
                try:
                    children = await self._directories(directory)
                except OSError as exc:
                    logger.exception("每周目录校验读取失败：%s", directory)
                    raise RuntimeError(f"WebDAV 目录读取失败：{exc}") from exc
                task["scanned_directories"] += 1
                stack.extend(children)
                await asyncio.sleep(settings.scan_throttle_ms / 1000)
            task["message"] = f"每周目录校验完成：共 {task['scanned_directories']} 个目录，未执行元数据刮削"
            logger.info(task["message"])
        finally:
            self.running = False


scanner = LibraryScanner()