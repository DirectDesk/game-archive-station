import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from .clients.rawg_client import RawgClient
from .clients.steam_client import SteamClient
from .services import resolve_tags

def _is_non_main_title(title: str) -> bool:
    """简单内联过滤：标题包含非主游戏关键词返回True。"""
    t = title.lower()
    keywords = ["soundtrack", "ost", "dlc", "demo", "trial", "beta",
                "typing", "quiz", "expansion", "pack"]
    return any(kw in t for kw in keywords)
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
    def _clean_folder_name_for_search(name: str) -> str:
        """清洗文件夹名用于元数据搜索：替换分隔符为空格、去版本号、去常见后缀。"""
        cleaned = name
        # 去掉版本号 v1.2.3 / Ver1.2 / [v1.0] 等
        cleaned = re.sub(r"(?i)\[?v(?:er)?\.?\d+(?:\.\d+){0,3}\]?", " ", cleaned)
        # 替换 . _ - 为空格（连续的合并为一个）
        cleaned = re.sub(r"[._\-]+", " ", cleaned)
        # 去掉方括号内容（如 [GuruGuru Craft]）
        cleaned = re.sub(r"\[.*?\]", " ", cleaned)
        # 去掉常见破解组/发布组前缀后缀（3DMGAME、DARKSiDERS、CODEX 等）
        scene_groups = [
            "3DMGAME", "DARKSiDERS", "CODEX", "RELOADED", "CPY", "SKIDROW",
            "PLAZA", "HOODLUM", "FitGirl", "EMPRESS", "RAZOR", "TiNYiSO",
            "DOGE", "KaOs", "VACE", "GOG", "DINOByTES", "FLT", "FAIRLIGHT",
            "DEVIANCE", "MYTH", "P2P", "RVT", "SiMPLEX", "TENOKE", "RUNE",
            "Chronos", "Goldberg", "0x0007", "Brixton", "Kapi", "RedDevil",
        ]
        for group in scene_groups:
            cleaned = re.sub(rf"(?i)\b{re.escape(group)}\b", " ", cleaned)
        # 去掉首尾空格，合并多空格
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    @staticmethod
    def _title_match(search_name: str, result_title: str) -> bool:
        """rawg 结果标题与搜索词的单词重叠率 >= 60% 才视为匹配，避免不相关结果挡住 vndb。"""
        def words(text):
            return set(re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text.lower()).split())
        sw = words(search_name)
        rw = words(result_title)
        if not sw:
            return False
        return len(sw & rw) / len(sw) >= 0.7

    @staticmethod
    def _is_main_game_title(title: str) -> bool:
        """过滤掉原声集/DLC/试玩版/衍生小游戏等非主游戏结果，避免挡住正确的主游戏匹配。"""
        non_main_patterns = [
            r"(?i)soundtrack", r"(?i)ost", r"(?i)original soundtrack",
            r"(?i)dlc", r"(?i)expansion", r"(?i)pack",
            r"(?i)demo", r"(?i)trial", r"(?i)beta",
            r"(?i)typing", r"(?i)quiz", r"(?i)puzzle.*pack",
            r"(?i)remake.*demo",
        ]
        for pattern in non_main_patterns:
            if re.search(pattern, title):
                return False
        return True

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
            # 仅按标准化后的 resource_url 去重；title 会被元数据覆盖，不能用于去重。
            folder_path = str(folder.resolve()).rstrip("/").replace("//", "/")
            exists_id = await db.scalar(select(Game.id).where(Game.resource_url == folder_path))
            if exists_id:
                exists = await db.get(Game, exists_id)
                if exists and (not exists.file_size or exists.file_size == 0):
                    try:
                        _sz = 0
                        for _root, _dirs, _files in os.walk(folder_path):
                            for _f in _files:
                                try: _sz += os.path.getsize(os.path.join(_root, _f))
                                except OSError: pass
                        exists.file_size = _sz
                        await db.commit()
                        logger.info("更新游戏容量：game_id=%s, size=%d", exists.id, _sz)
                    except Exception:
                        pass
                return False

            # 文件夹名是唯一可用的低 IO 发现信息；不进入文件夹读取文件。
            client = RawgClient()
            search_name = self._clean_folder_name_for_search(folder.name)
            candidates = []
            try:
                # page_size=5 取多个结果，按标题相似度过滤，避免不相关结果挡住 vndb
                for item in await client.search_games(search_name, page_size=5):
                    if _is_non_main_title(item.get("title", "")):
                        continue
                    if self._title_match(search_name, item.get("title", "")):
                        candidates = [item]
                        break
                # 清洗名无相关结果时，用原始名再试一次
                if not candidates:
                    for item in await client.search_games(folder.name, page_size=5):
                        if _is_non_main_title(item.get("title", "")):
                            continue
                        if self._title_match(folder.name, item.get("title", "")):
                            candidates = [item]
                            break
            except Exception:
                logger.exception("RAWG 搜索失败，创建基础游戏记录：%s", folder_path)
                candidates = []
            source_type = "rawg"
            if not candidates:
                # Steam fallback：中文名搜索支持好，appdetails 返回中文元数据
                try:
                    steam_client = SteamClient()
                    # 优先用父目录名搜索（用户常用中文名作为父目录，Steam中文名搜索效果好）
                    steam_results = []
                    if folder.parent and folder.parent.name and folder.parent.name not in (".", ".."):
                        steam_results = await steam_client.search_games(folder.parent.name, page_size=5)
                    if not steam_results:
                        steam_results = await steam_client.search_games(search_name, page_size=5)
                    if not steam_results:
                        steam_results = await steam_client.search_games(folder.name, page_size=5)
                    # 过滤掉原声集/DLC等非主游戏结果
                    steam_results = [
                        r for r in steam_results
                        if not re.search(r"(?i)(soundtrack|ost|original soundtrack|dlc|demo|trial|art pack|artbook|art work|artworks|wallpaper|theme|cosmetic|skin pack)", r.get("title", ""))
                        and not re.search(r"(艺术|画集|原画|原声|壁纸|主题|皮肤|道具|礼包)", r.get("title", ""))
                    ]
                except Exception:
                    logger.exception("Steam fallback 搜索失败：%s", folder_path)
                    steam_results = []
                if steam_results:
                    # Steam 搜索按相关性排序，第一个通常即主游戏
                    candidates = [steam_results[0]]
                    source_type = "steam"
                    logger.info("Steam fallback 匹配成功：%s -> %s (appid=%s)", folder.name, steam_results[0].get("title",""), steam_results[0].get("source_id",""))
            if not candidates:
                try:
                    from .services import search_vndb

                    search_name = self._clean_folder_name_for_search(folder.name)
                    vndb_results = await search_vndb(search_name)
                    if not vndb_results:
                        # 清洗后仍搜不到，用原始名再试一次
                        vndb_results = await search_vndb(folder.name)
                except Exception:
                    logger.exception("VNDB fallback 搜索失败：%s", folder_path)
                    vndb_results = []
                if vndb_results:
                    # vndb 标题多为日文/罗马音，与英文搜索词单词重叠率低，不做相似度校验；
                    # vndb 搜索 API 本身按相关性排序，第一个结果通常即目标游戏。
                    candidates = [vndb_results[0]]
                    source_type = "vndb"
                    logger.info("VNDB fallback 匹配成功：%s -> %s (%s)", folder.name, vndb_results[0].get("title",""), vndb_results[0].get("source_id",""))
            if not candidates:
                # DLsite fallback：同人游戏（RJ/VJ/BJ编号）优先用编号精准查询
                try:
                    from .clients.dlsite_client import DlsiteClient

                    dlsite = DlsiteClient()
                    workno = dlsite.extract_workno(folder.name)
                    dlsite_results = []
                    if workno:
                        # 有编号：精准查询，不做搜索
                        work_detail = await dlsite.get_work_by_id(workno)
                        if work_detail:
                            dlsite_results = [{
                                "source_type": "dlsite",
                                "source_id": workno,
                                "title": work_detail.get("work_name", ""),
                                "cover_url": dlsite._full_url((work_detail.get("image_main") or {}).get("url", "")),
                            }]
                    else:
                        # 无编号：关键词搜索
                        dlsite_results = await dlsite.search_games(search_name, page_size=5)
                        if not dlsite_results:
                            dlsite_results = await dlsite.search_games(folder.name, page_size=5)
                except Exception:
                    logger.exception("DLsite fallback 搜索失败：%s", folder_path)
                    dlsite_results = []
                if dlsite_results:
                    # DLsite 编号查询是精准匹配；搜索结果按相关性排序，取第一个
                    candidates = [dlsite_results[0]]
                    source_type = "dlsite"
                    logger.info("DLsite fallback 匹配成功：%s -> %s (%s)", folder.name, dlsite_results[0].get("title",""), dlsite_results[0].get("source_id",""))
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
                elif source_type == "dlsite":
                    from .clients.dlsite_client import DlsiteClient

                    metadata = await DlsiteClient().get_game_detail(candidates[0]["source_id"])
                elif source_type == "steam":
                    metadata = await SteamClient().get_game_detail(candidates[0]["source_id"])
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
            # 把该数据源的标签存入 original_data，供标签数据源优先级选择
            if metadata.get("tags"):
                try:
                    orig = json.loads(metadata.get("original_data") or "{}")
                except json.JSONDecodeError:
                    orig = {}
                orig[f"{source_type}_tags"] = metadata["tags"]
                metadata["original_data"] = json.dumps(orig, ensure_ascii=False)
            # 构建 source_data/source_ids：主来源数据存入，其他来源待刷新时补充
            _sd = {}
            _sids = {}
            if source_type and source_type != "custom":
                # 主来源的完整 metadata（翻译后）存入 source_data
                _sd[source_type] = {k: v for k, v in metadata.items() if k not in ("resource_type", "resource_url", "play_status", "original_data")}
                _sids[source_type] = candidates[0].get("source_id", "")
            # 如果主来源同步到了 steam_appid，补充 steam 来源ID
            if metadata.get("steam_appid") and "steam" not in _sids:
                _sids["steam"] = metadata["steam_appid"]
            metadata["source_data"] = json.dumps(_sd, ensure_ascii=False)
            metadata["source_ids"] = json.dumps(_sids, ensure_ascii=False)
            # 计算文件夹大小
            _dir_size = 0
            try:
                for _root, _dirs, _files in os.walk(folder_path):
                    for _f in _files:
                        try: _dir_size += os.path.getsize(os.path.join(_root, _f))
                        except OSError: pass
            except Exception:
                pass
            metadata.update({"resource_type": resource_type, "resource_url": folder_path, "play_status": "favorite", "file_size": _dir_size})
            game = Game(**metadata)
            try:
                await resolve_tags(game, config, "", db)
            except Exception:
                logger.exception("标签解析失败：%s", folder_path)
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