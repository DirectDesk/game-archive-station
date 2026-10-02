import asyncio
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from .clients.rawg_client import RawgClient
from .config import settings
from .database import SessionLocal
from .models import Game, SystemConfig
from .translation_service import translation_service
from .services import fetch_game_screenshots

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

    async def _create_game_from_folder(self, folder: Path, resource_type: str, config: SystemConfig) -> bool:
        async with SessionLocal() as db:
            folder_path = str(folder.resolve()).rstrip("/")
            exists = await db.scalar(
                select(Game.id).where(
                    (Game.resource_url == folder_path)
                    | ((Game.title == folder.name) & (Game.resource_type == resource_type))
                )
            )
            if exists:
                return False

            # 文件夹名是唯一可用的低 IO 发现信息；不进入文件夹读取文件。
            client = RawgClient()
            try:
                candidates = await client.search_games(folder.name, page_size=1)
            except Exception:
                logger.exception("RAWG 搜索失败，创建基础游戏记录：%s", folder_path)
                candidates = []
            source_type = "rawg"
            if not candidates:
                try:
                    from .services import search_vndb

                    vndb_results = await search_vndb(folder.name)
                except Exception:
                    logger.exception("VNDB fallback 搜索失败：%s", folder_path)
                    vndb_results = []
                if vndb_results:
                    candidates = [vndb_results[0]]
                    source_type = "vndb"
            if not candidates:
                game = Game(
                    title=folder.name,
                    version=(re.search(r"(?i)(?<![a-z0-9])v\d+(?:\.\d+){1,3}(?![a-z0-9])", folder.name) or [""])[0],
                    alias="",
                    source_type="custom",
                    source_id="",
                    resource_type=resource_type,
                    resource_url=folder_path,
                    play_status="favorite",
                )
                db.add(game)
                if config.scan_fetch_screenshots:
                    await db.flush()
                    try:
                        await fetch_game_screenshots(game, config)
                    except Exception:
                        logger.exception("扫描截图抓取失败：%s", folder_path)
                await db.commit()
                logger.info("扫描发现游戏目录（未匹配元数据，待手动补充）：%s", folder_path)
                return True
            try:
                if source_type == "vndb":
                    from .services import get_vndb_detail

                    metadata = await get_vndb_detail(candidates[0]["source_id"])
                else:
                    metadata = await client.get_game_detail(candidates[0]["source_id"])
            except Exception:
                logger.exception("%s 详情获取失败，创建基础游戏记录：%s", source_type.upper(), folder_path)
                metadata = {
                    "title": folder.name,
                    "alias": "",
                    "source_type": "custom",
                    "source_id": "",
                }
            version_match = re.search(r"(?i)(?<![a-z0-9])v\d+(?:\.\d+){1,3}(?![a-z0-9])", folder.name)
            if version_match and not metadata.get("version"):
                metadata["version"] = version_match.group(0)
            metadata = await translation_service.translate_metadata(db, metadata)
            metadata.setdefault("source_type", source_type)
            metadata.update({"resource_type": resource_type, "resource_url": folder_path, "play_status": "favorite"})
            game = Game(**metadata)
            db.add(game)
            if config.scan_fetch_screenshots:
                await db.flush()
                try:
                    await fetch_game_screenshots(game, config)
                except Exception:
                    logger.exception("扫描截图抓取失败：%s", folder_path)
            await db.commit()
            logger.info("扫描发现游戏目录（已匹配元数据）：%s", folder_path)
            return True

    async def scan(self, task: dict, full: bool = False) -> None:
        if self.running:
            raise RuntimeError("已有扫描任务正在执行")
        self.running = True
        task.update({"mode": "full" if full else "incremental", "scanned_directories": 0, "discovered_games": 0, "skipped_directories": 0, "logs": []})
        started_at = datetime.utcnow()
        try:
            roots = [(settings.scan_root, "nas_cloud"), (settings.local_game_root, "nas_local")]
            available_roots = [(root, resource_type) for root, resource_type in roots if await asyncio.to_thread(root.is_dir)]
            if not available_roots:
                raise RuntimeError(f"扫描根目录不可访问：{settings.scan_root}")
            config = await self._get_config()
            last_scan_at = None if full else config.last_scan_at
            stack = available_roots[:]
            while stack:
                directory, resource_type = stack.pop()
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
                if not children and directory not in {root for root, _ in available_roots}:
                    if await self._create_game_from_folder(directory, resource_type, config):
                        task["discovered_games"] += 1
                else:
                    stack.extend((child, resource_type) for child in children)
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
            roots = [root for root in (settings.scan_root, settings.local_game_root) if await asyncio.to_thread(root.is_dir)]
            if not roots:
                raise RuntimeError(f"扫描根目录不可访问：{settings.scan_root}")
            stack = roots[:]
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