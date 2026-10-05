import asyncio
import json
import logging
import os
import re
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import select

from .clients.rawg_client import RawgClient
from .clients.steam_client import SteamClient
from .services import resolve_tags, normalize_release_date, alias_candidates, title_similarity, normalize_core

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


class _SkipSearch(Exception):
    pass


class LibraryScanner:
    """目录扫描器（参照飞牛影视刮削逻辑）：
    - 扫描根目录的一级子项（目录或文件）即游戏单位，不再递归深层目录；
    - 文件夹内含多个有效游戏单元时（过滤文本文件、压缩包分包合并后），
      按文件夹内的文件名分别刮削；否则按文件夹名刮削。
    """

    # 忽略的文件类型（文本/图片/字幕/校验文件等，不算游戏）
    _IGNORE_EXTS = {
        ".txt", ".md", ".doc", ".docx", ".nfo", ".pdf", ".rtf", ".odt",
        ".srt", ".ass", ".ssa", ".vtt", ".url", ".htm", ".html",
        ".ini", ".log", ".csv", ".xls", ".xlsx", ".ppt", ".pptx",
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".ico",
        ".sfv", ".par2", ".rev", ".db", ".torrent",
    }
    # 非游戏附件关键词（补丁/修改器/存档/攻略等，不单独刮削）
    _NON_GAME_KEYWORDS = (
        "补丁", "修改器", "存档", "攻略", "原声", "画集", "设定集",
        "soundtrack", "trainer", "artbook", "wallpaper", ".patch.",
    )
    # 压缩包分包命名模式（xxx.part01.rar / xxx.r00 / xxx.z01 / xxx.001 / xxx.7z.001 / xxx.zip.001）
    _PART_RES = [
        re.compile(r"(?i)\.part0*\d+\.rar$"),
        re.compile(r"(?i)\.part0*\d+\.zip$"),
        re.compile(r"(?i)\.r\d{2,3}$"),
        re.compile(r"(?i)\.z\d{2,3}$"),
        re.compile(r"(?i)\.7z\.\d{3}$"),
        re.compile(r"(?i)\.zip\.\d{2,3}$"),
        re.compile(r"(?i)\.\d{3}$"),
    ]

    def __init__(self):
        self.running = False

    @classmethod
    def _unit_key(cls, filename: str) -> str:
        """游戏单元分组 key：压缩包分包归并到同一 key（去掉分包后缀和扩展名）。"""
        for pattern in cls._PART_RES:
            match = pattern.search(filename)
            if match:
                return filename[: match.start()].lower()
        stem = filename.rsplit(".", 1)[0] if "." in filename else filename
        return stem.lower()

    @classmethod
    def _is_ignored_file(cls, filename: str) -> bool:
        """文本/图片等非游戏文件，或补丁/修改器等附件文件。"""
        lower = filename.lower()
        ext = ("." + lower.rsplit(".", 1)[1]) if "." in lower else ""
        # 分包后缀的文件扩展名（.001/.r00）不在忽略表内，先判断扩展名
        if ext in cls._IGNORE_EXTS:
            return True
        stem = lower.rsplit(".", 1)[0]
        return any(keyword in stem for keyword in cls._NON_GAME_KEYWORDS)

    @classmethod
    def _group_files(cls, files: list) -> list:
        """把文件列表按单元 key 分组，返回 [(单元名, 主文件路径, 总大小)]。

        主文件选择：优先无分包标记的压缩包主包（.rar/.zip/.7z/.iso 等），否则组内第一个文件。
        """
        groups: dict[str, list] = {}
        for file_path, size in files:
            key = cls._unit_key(file_path.name)
            groups.setdefault(key, []).append((file_path, size))
        units = []
        for key, members in groups.items():
            main = None
            for file_path, _size in members:
                name = file_path.name.lower()
                is_part = any(pattern.search(name) for pattern in cls._PART_RES)
                if not is_part and name.rsplit(".", 1)[-1] in ("rar", "zip", "7z", "iso", "exe", "msi", "pkg"):
                    main = file_path
                    break
            if main is None:
                main = members[0][0]
            total = sum(size for _p, size in members)
            units.append((main.name, main, total))
        return units

    @staticmethod
    async def _list_files_recursive(path: Path) -> list:
        """递归收集文件夹内所有文件（路径, 大小），只读目录条目。"""
        def collect():
            result = []
            for root_dir, _dirs, filenames in os.walk(path):
                for filename in filenames:
                    full = Path(root_dir) / filename
                    try:
                        result.append((full, full.stat().st_size))
                    except OSError:
                        result.append((full, 0))
            return result

        return await asyncio.to_thread(collect)

    @classmethod
    async def _analyze_folder(cls, path: Path) -> list:
        """分析文件夹内的游戏单元：过滤非游戏文件、压缩包分包合并后按单元分组。"""
        all_files = await cls._list_files_recursive(path)
        valid = [(fp, size) for fp, size in all_files if not cls._is_ignored_file(fp.name)]
        if not valid:
            return []
        return cls._group_files(valid)

    @staticmethod
    async def _entries(path: Path) -> tuple[list, list]:
        """读取目录的直接子项，返回 (文件列表, 目录列表)。"""
        def read_entries():
            files, dirs = [], []
            with os.scandir(path) as entries:
                for entry in entries:
                    if entry.is_dir(follow_symlinks=False):
                        dirs.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        try:
                            size = entry.stat(follow_symlinks=False).st_size
                        except OSError:
                            size = 0
                        files.append((Path(entry.path), size))
            return files, dirs

        return await asyncio.to_thread(read_entries)

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
        """rawg 结果标题与搜索词的相似度 >= 0.7 才视为匹配，避免不相关结果挡住 vndb。"""
        return title_similarity(search_name, result_title) >= 0.7

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

    async def _create_game_from_folder(self, folder: Path, resource_type: str, config: SystemConfig, search_name: str | None = None, known_size: int | None = None) -> bool:
        async with SessionLocal() as db:
            # 仅按标准化后的 resource_url 去重；title 会被元数据覆盖，不能用于去重。
            folder_path = str(folder.resolve()).rstrip("/").replace("//", "/")
            exists_id = await db.scalar(select(Game.id).where(Game.resource_url == folder_path))
            old_deeper = False
            if not exists_id:
                # 前缀判重：刮削粒度变更（深层叶子 -> 一级目录 / 文件夹 -> 文件夹内文件）时，
                # 旧记录路径与新条目路径互为前缀，视为同一游戏，避免重复入库。
                # 优先级：尾斜杠归一等值 = 旧记录更深 > 旧记录更浅（父路径，最弱仅兜底）。
                parent_hit = None
                rows = (await db.execute(select(Game.id, Game.resource_url).where(Game.resource_url != ""))).all()
                for gid, url in rows:
                    norm = (url or "").rstrip("/")
                    if not norm:
                        continue
                    if norm == folder_path:
                        # 尾斜杠差异导致的"假不存在"：归一等值，复用旧记录
                        exists_id = gid
                        old_deeper = True  # 顺便把 resource_url 归一化为无尾斜杠
                        break
                    if norm.startswith(folder_path + "/"):
                        # 旧记录更深（旧叶子目录）：更新为新粒度路径，沿用原记录
                        exists_id = gid
                        old_deeper = True
                        break
                    if folder_path.startswith(norm + "/"):
                        # 扫描根目录本身的记录不作为"已入库"依据（脏数据）
                        root_strs = {str(settings.scan_root).rstrip("/"), str(settings.local_game_root).rstrip("/")}
                        if norm not in root_strs and parent_hit is None:
                            parent_hit = gid
                if not exists_id and parent_hit:
                    exists_id = parent_hit
            if exists_id:
                exists = await db.get(Game, exists_id)
                if exists and old_deeper:
                    exists.resource_url = folder_path
                    await db.commit()
                    logger.info("刮削粒度/路径归一：game_id=%s resource_url -> %s", exists.id, folder_path)
                if exists and (not exists.file_size or exists.file_size == 0):
                    try:
                        _sz = known_size or 0
                        if not _sz:
                            if os.path.isfile(folder_path):
                                _sz = os.path.getsize(folder_path)
                            else:
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

            # 搜索名：文件夹名，或指定的单元名（文件基名，多游戏文件夹情形）
            client = RawgClient()
            raw_name = search_name or folder.name
            # 文件情形去掉扩展名（如 your diary+h.zip -> your diary+h）
            if folder.is_file() and "." in raw_name:
                raw_name = raw_name.rsplit(".", 1)[0]
            search_name = self._clean_folder_name_for_search(raw_name)
            candidates = []
            source_type = "rawg"
            # RJ/VJ/BJ 编号优先 DLsite 精准查询：同人编号是精确 ID，
            # 先于 RAWG 模糊搜索，避免编号作品被错配（如 NEKOPARA 同人志乱码匹配）
            try:
                from .clients.dlsite_client import DlsiteClient

                _dlsite_early = DlsiteClient()
                _workno = _dlsite_early.extract_workno(raw_name)
                if _workno:
                    _work = await _dlsite_early.get_work_by_id(_workno)
                    if _work:
                        candidates = [{
                            "source_type": "dlsite",
                            "source_id": _workno,
                            "title": _work.get("work_name", ""),
                        }]
                        source_type = "dlsite"
                        logger.info("DLsite 编号精准匹配：%s -> %s (%s)", raw_name, _work.get("work_name", ""), _workno)
            except Exception:
                logger.exception("DLsite 编号优先查询失败：%s", raw_name)
            try:
                if candidates:
                    raise _SkipSearch  # 已有 DLsite 精准结果，跳过 RAWG 搜索
                # page_size=5 取多个结果，按标题相似度过滤，避免不相关结果挡住 vndb
                for item in await client.search_games(search_name, page_size=5):
                    if _is_non_main_title(item.get("title", "")):
                        continue
                    if self._title_match(search_name, item.get("title", "")):
                        candidates = [item]
                        break
                # 清洗名无相关结果时，用原始名再试一次
                if not candidates:
                    for item in await client.search_games(raw_name, page_size=5):
                        if _is_non_main_title(item.get("title", "")):
                            continue
                        if self._title_match(raw_name, item.get("title", "")):
                            candidates = [item]
                            break
                # 中文别名映射的英文名候选（帝国时代 -> Age of Empires）
                if not candidates:
                    for _alias in alias_candidates(raw_name):
                        for item in await client.search_games(_alias, page_size=5):
                            if _is_non_main_title(item.get("title", "")):
                                continue
                            if self._title_match(_alias, item.get("title", "")) or title_similarity(_alias, item.get("title", "")) >= 0.5:
                                candidates = [item]
                                break
                        if candidates:
                            logger.info("RAWG 别名匹配成功：%s -> %s (alias=%s)", raw_name, candidates[0].get("title", ""), _alias)
                            break
            except _SkipSearch:
                pass
            except Exception:
                logger.exception("RAWG 搜索失败，创建基础游戏记录：%s", folder_path)
                candidates = []
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
                        steam_results = await steam_client.search_games(raw_name, page_size=5)
                    if not steam_results:
                        for _alias in alias_candidates(raw_name):
                            steam_results = await steam_client.search_games(_alias, page_size=5)
                            if steam_results:
                                break
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
                    # 先按标题相似度降序，再校验 type（storesearch 对 DLC 也返回 type=app），
                    # 避免顺位拿到资料片/原声集。
                    _steam_group = [folder.parent.name if folder.parent else "", search_name, raw_name] + alias_candidates(raw_name)
                    def _scan_steam_rank(cand):
                        t = cand.get("title", "")
                        return max((title_similarity(g, t) for g in _steam_group if g), default=0.0)
                    steam_results = sorted(steam_results, key=_scan_steam_rank, reverse=True)
                    picked = None
                    for candidate in steam_results[:8]:
                        try:
                            if await steam_client.get_app_type(candidate["source_id"]) == "game":
                                picked = candidate
                                break
                        except Exception:
                            continue
                    if picked is None and steam_results and _scan_steam_rank(steam_results[0]) >= 0.5:
                        picked = steam_results[0]
                    if picked:
                        candidates = [picked]
                        source_type = "steam"
                        logger.info("Steam fallback 匹配成功：%s -> %s (appid=%s)", raw_name, picked.get("title",""), picked.get("source_id",""))
                    else:
                        logger.info("Steam fallback 候选均为 DLC 或相似度过低，跳过：%s", raw_name)
            if not candidates:
                try:
                    from .services import search_vndb

                    search_name = self._clean_folder_name_for_search(folder.name)
                    # 候选池：清洗名 + 原始名 + 别名候选，合并统一打分（避免只试首个来源漏配）
                    vndb_results = []
                    for _q in [search_name, raw_name] + alias_candidates(raw_name):
                        if not _q:
                            continue
                        try:
                            vndb_results = vndb_results + await search_vndb(_q)
                        except Exception:
                            pass
                    _seen_v, _uniq = set(), []
                    for _it in vndb_results:
                        _sid = _it.get("source_id")
                        if _sid and _sid not in _seen_v:
                            _seen_v.add(_sid)
                            _uniq.append(_it)
                    vndb_results = _uniq
                except Exception:
                    logger.exception("VNDB fallback 搜索失败：%s", folder_path)
                    vndb_results = []
                if vndb_results:
                    # 相似度校验：vndb 搜索可能因一个宽泛词（如 "sandbox"）返回大量无关作品，
                    # 直接取首个结果会造成乱配；取相似度最高者且需达阈值。
                    def _vndb_score(item):
                        return max(
                            title_similarity(raw_name, item.get("title", "")),
                            title_similarity(raw_name, item.get("alias", "") or ""),
                            title_similarity(search_name, item.get("title", "")),
                            title_similarity(search_name, item.get("alias", "") or ""),
                        )

                    best = max(vndb_results, key=_vndb_score)
                    if _vndb_score(best) >= 0.5:
                        candidates = [best]
                        source_type = "vndb"
                        logger.info("VNDB fallback 匹配成功：%s -> %s (%s)", raw_name, best.get("title",""), best.get("source_id",""))
                    else:
                        logger.info("VNDB 候选相似度过低，跳过（避免乱配）：%s 最高=%.2f %s", raw_name, _vndb_score(best), best.get("title",""))
            if not candidates:
                # DLsite fallback：同人游戏（RJ/VJ/BJ编号）优先用编号精准查询
                try:
                    from .clients.dlsite_client import DlsiteClient

                    dlsite = DlsiteClient()
                    workno = dlsite.extract_workno(raw_name)
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
                        # 无编号：关键词搜索（原名 + 别名候选），合并后按相似度挑选
                        _ds_pool = []
                        for _q in [search_name, raw_name] + alias_candidates(raw_name):
                            if not _q:
                                continue
                            try:
                                _ds_pool = _ds_pool + await dlsite.search_games(_q, page_size=8)
                            except Exception:
                                pass
                        # 无编号搜索结果需相似度校验，避免乱配（如 "SEX beach 4"）
                        _ds_best, _ds_best_score = None, 0.0
                        for _c in _ds_pool:
                            _score = max(
                                title_similarity(raw_name, _c.get("title", "")),
                                title_similarity(search_name, _c.get("title", "")),
                                max((title_similarity(g, _c.get("title", "")) for g in alias_candidates(raw_name)), default=0.0),
                            )
                            if _score > _ds_best_score:
                                _ds_best, _ds_best_score = _c, _score
                        if _ds_best and _ds_best_score >= 0.5:
                            dlsite_results = [_ds_best]
                        else:
                            dlsite_results = []
                except Exception:
                    logger.exception("DLsite fallback 搜索失败：%s", folder_path)
                    dlsite_results = []
                if dlsite_results:
                    # 有编号为精准匹配；搜索匹配已在上方做相似度校验
                    candidates = [dlsite_results[0]]
                    source_type = "dlsite"
                    logger.info("DLsite fallback 匹配成功：%s -> %s (%s)", folder.name, dlsite_results[0].get("title",""), dlsite_results[0].get("source_id",""))
            if not candidates:
                game = Game(
                    title=raw_name,
                    version=(re.search(r"(?i)(?<![a-z0-9])v\d+(?:\.\d+){1,3}(?![a-z0-9])", raw_name) or [""])[0],
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
                    "title": raw_name,
                    "alias": "",
                    "source_type": "custom",
                    "source_id": "",
                }
            version_match = re.search(r"(?i)(?<![a-z0-9])v\d+(?:\.\d+){1,3}(?![a-z0-9])", raw_name)
            if version_match and not metadata.get("version"):
                metadata["version"] = version_match.group(0)
            raw_metadata = dict(metadata)  # 翻译前快照
            metadata = await translation_service.translate_metadata(db, metadata)
            metadata.setdefault("source_type", source_type)
            # 把该数据源的原文标签存入 original_data，供标签数据源优先级选择
            if raw_metadata.get("tags"):
                try:
                    orig = json.loads(metadata.get("original_data") or "{}")
                except json.JSONDecodeError:
                    orig = {}
                orig[f"{source_type}_tags"] = raw_metadata["tags"]
                metadata["original_data"] = json.dumps(orig, ensure_ascii=False)
            # 构建 source_data/source_ids：主来源数据存入，其他来源待刷新时补充
            _sd = {}
            _sids = {}
            if source_type and source_type != "custom":
                # 主来源的完整 metadata（翻译前原文）存入 source_data
                _sd[source_type] = {k: v for k, v in raw_metadata.items() if k not in ("resource_type", "resource_url", "play_status", "original_data")}
                _sids[source_type] = candidates[0].get("source_id", "")
            # 如果主来源同步到了 steam_appid，补充 steam 来源ID
            if metadata.get("steam_appid") and "steam" not in _sids:
                _sids["steam"] = metadata["steam_appid"]
            metadata["source_data"] = json.dumps(_sd, ensure_ascii=False)
            metadata["source_ids"] = json.dumps(_sids, ensure_ascii=False)
            # 计算资源大小（文件夹递归 / 单文件 / 已知的单元合计）
            _dir_size = known_size or 0
            if not _dir_size:
                try:
                    if os.path.isfile(folder_path):
                        _dir_size = os.path.getsize(folder_path)
                    else:
                        for _root, _dirs, _files in os.walk(folder_path):
                            for _f in _files:
                                try: _dir_size += os.path.getsize(os.path.join(_root, _f))
                                except OSError: pass
                except Exception:
                    pass
            metadata.update({"resource_type": resource_type, "resource_url": folder_path, "play_status": "favorite", "file_size": _dir_size})
            # 过滤掉 Game 模型不存在的字段（如 steam 的 english_name 等），避免创建时报错
            _valid_fields = {column.name for column in Game.__table__.columns}
            metadata = {k: v for k, v in metadata.items() if k in _valid_fields}
            # release_date 统一归一化为 date 对象（字符串/列表/date 均兼容）
            metadata["release_date"] = normalize_release_date(metadata.get("release_date"))
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
            for root, resource_type in available_roots:
                try:
                    files, dirs = await self._entries(root)
                except OSError as exc:
                    logger.exception("读取扫描根目录失败，停止扫描：%s", root)
                    raise RuntimeError(f"扫描根目录读取失败：{exc}") from exc
                task["scanned_directories"] += 1
                task["logs"] = (task["logs"] + [f"已读取：{root}"])[-20:]

                # 根目录下的直接文件：过滤后按单元分组（分包合并），每个单元一个游戏
                file_units = self._group_files([(fp, size) for fp, size in files if not self._is_ignored_file(fp.name)])
                for unit_name, unit_path, unit_size in file_units:
                    if await self._create_game_from_folder(unit_path, resource_type, config, search_name=unit_name, known_size=unit_size):
                        task["discovered_games"] += 1
                    await asyncio.sleep(settings.scan_throttle_ms / 1000)

                # 根目录下一级目录：游戏单位（参照飞牛影视）
                for directory in dirs:
                    # mtime 剪枝：nas_cloud 不剪枝（挂载目录 mtime 不可靠）；nas_local 正常剪枝
                    if resource_type == "nas_local" and last_scan_at:
                        try:
                            if (await self._mtime(directory)) <= last_scan_at.timestamp():
                                task["skipped_directories"] += 1
                                continue
                        except OSError:
                            pass
                    task["scanned_directories"] += 1
                    task["logs"] = (task["logs"] + [f"已读取：{directory}"])[-20:]
                    try:
                        units = await self._analyze_folder(directory)
                    except OSError as exc:
                        logger.warning("分析文件夹失败，按文件夹名刮削：%s (%s)", directory, exc)
                        units = []
                    if len(units) > 1:
                        # 文件夹下有多个游戏：按文件夹内文件名分别刮削
                        logger.info("文件夹含 %d 个游戏单元，按文件名分别刮削：%s", len(units), directory)
                        for unit_name, unit_path, unit_size in units:
                            if await self._create_game_from_folder(unit_path, resource_type, config, search_name=unit_name, known_size=unit_size):
                                task["discovered_games"] += 1
                            await asyncio.sleep(settings.scan_throttle_ms / 1000)
                    else:
                        # 0/1 个游戏单元：按文件夹名刮削
                        if await self._create_game_from_folder(directory, resource_type, config):
                            task["discovered_games"] += 1
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