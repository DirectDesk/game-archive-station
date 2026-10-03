from datetime import date
import hashlib
import json
import logging
import re
from pathlib import Path

import httpx

from .clients.rawg_client import RawgClient
from .clients.steam_client import SteamClient
from .cover_service import cache_cover
from .database import SessionLocal
from .models import Game, SystemConfig
from .translation_service import translation_service
from .config import settings

logger = logging.getLogger(__name__)



async def resolve_tags(game: Game, config: SystemConfig, requested_source: str = "", db: AsyncSession = None) -> str:
    """按配置优先级从 original_data 中选择标签；返回选中的标签字符串。"""
    try:
        priority = json.loads(config.tag_source_priority or "[]")
    except json.JSONDecodeError:
        priority = ["steam", "rawg", "vndb", "dlsite"]
    if requested_source:
        priority = [requested_source]
    # 优先从 source_data 取，fallback 到 original_data
    try:
        source_data = json.loads(game.source_data or "{}")
    except json.JSONDecodeError:
        source_data = {}
    try:
        metadata = json.loads(game.original_data or "{}")
    except json.JSONDecodeError:
        metadata = {}
    for source in priority:
        tags = ""
        # 1. 优先从 source_data[source].tags 取
        if source in source_data:
            tags = source_data[source].get("tags", "")
        # 2. fallback 到 original_data 的 {source}_tags
        if not tags:
            tags = metadata.get(f"{source}_tags", "")
        # 3. 再 fallback：游戏本身就是该来源，从 original_data.tags 取
        if not tags and game.source_type == source:
            tags = metadata.get("tags", "")
        if tags:
            # 原文标签存 source_data
            try:
                sd = json.loads(game.source_data or "{}")
            except json.JSONDecodeError:
                sd = {}
            if source not in sd:
                sd[source] = {}
            sd[source]["tags"] = tags
            game.source_data = json.dumps(sd, ensure_ascii=False, default=str)
            # 翻译标签
            translated_tags = tags
            if db and config and config.auto_translate and config.translator_type != "none":
                try:
                    from app.translation_service import TranslationService
                    ts = TranslationService()
                    await ts.load(db)
                    parts = [p.strip() for p in tags.split(",") if p.strip()]
                    translated_parts = [await ts.translate(p, "tag") for p in parts]
                    translated_tags = ", ".join(translated_parts)
                except Exception as e:
                    logger.warning("标签翻译失败 game_id=%s: %s", game.id, e)
            game.tags = translated_tags
            game.tag_source = source
            logger.info("标签解析：game_id=%s, source=%s, tags=%.80s", game.id, source, translated_tags)
            return translated_tags
    game.tags = ""
    game.tag_source = ""
    logger.warning("标签解析失败：game_id=%s，所有来源均无标签，已清空", game.id)
    return ""


async def fetch_game_screenshots(game: Game, config: SystemConfig, requested_source: str = "") -> list[str]:
    """按配置优先级获取并缓存截图；失败的来源会继续尝试下一个来源。"""
    logger.info("开始获取截图：game_id=%s, source_type=%s, source_id=%s, screenshots_field=%.100s",
                game.id, game.source_type, game.source_id, game.screenshots or "")
    try:
        priority = json.loads(config.screenshot_source_priority or "[]")
    except json.JSONDecodeError:
        priority = ["steam", "rawg", "vndb", "dlsite"]
    limit = max(0, int(config.max_screenshots or 5))
    metadata = {}
    try:
        metadata = json.loads(game.original_data or "{}")
    except json.JSONDecodeError:
        pass
    source_data = {}
    try:
        source_data = json.loads(game.source_data or "{}")
    except json.JSONDecodeError:
        pass
    candidates: dict[str, list[str]] = {}
    # 优先从 source_data 取各来源的截图 URL
    for _src in ["rawg", "steam", "vndb", "dlsite"]:
        if _src in source_data and source_data[_src].get("screenshots"):
            try:
                candidates[_src] = json.loads(source_data[_src]["screenshots"])
            except (json.JSONDecodeError, TypeError):
                pass
    # fallback：从 original_data 或 game.screenshots 取
    if not candidates.get("rawg"):
        if metadata.get("screenshots"):
            candidates["rawg"] = metadata["screenshots"]
        else:
            try:
                candidates["rawg"] = json.loads(game.screenshots or "[]") if game.source_type == "rawg" else []
            except json.JSONDecodeError:
                candidates["rawg"] = []
    steam_appid = game.steam_appid or metadata.get("steam_appid", "")
    if not steam_appid and game.source_type == "rawg" and game.source_id:
        try:
            rawg_metadata = await RawgClient().get_game_detail(game.source_id)
            steam_appid = rawg_metadata.get("steam_appid", "")
            if rawg_metadata.get("screenshots") and not candidates.get("rawg"):
                candidates["rawg"] = json.loads(rawg_metadata["screenshots"])
        except Exception:
            logger.warning("读取 RAWG Steam 商店信息失败：%s", game.source_id, exc_info=True)
    # Steam 截图：steam 来源游戏直接用已有 screenshots；其他来源有 steam_appid 时调 appdetails 补截图
    if game.source_type == "steam":
        # 优先从 original_data.screenshots 读远程 URL，不依赖可能被清空的 game.screenshots
        od_steam = metadata.get("screenshots", "")
        if od_steam:
            try:
                candidates["steam"] = json.loads(od_steam)
            except json.JSONDecodeError:
                pass
        if not candidates.get("steam"):
            try:
                existing_steam = json.loads(game.screenshots or "[]")
            except json.JSONDecodeError:
                existing_steam = []
            if existing_steam:
                candidates["steam"] = existing_steam
    elif steam_appid:
        try:
            steam_detail = await SteamClient().get_game_detail(steam_appid)
            steam_shots = json.loads(steam_detail.get("screenshots") or "[]")
            if steam_shots:
                candidates["steam"] = steam_shots
                logger.info("游戏 %s 从 Steam appdetails 获取 %d 张截图", game.id, len(steam_shots))
        except Exception:
            logger.warning("游戏 %s 从 Steam appdetails 获取截图失败", game.id, exc_info=True)
    if game.source_type == "vndb" and game.source_id:
        candidates["vndb"] = await _vndb_screenshots(game.source_id)
    # VNDB 无截图时，按标题从 RAWG 补截图。
    if game.source_type == "vndb" and not candidates.get("vndb"):
        try:
            rawg_candidates = await RawgClient().search_games(game.title, page_size=1)
            if rawg_candidates:
                rawg_detail = await RawgClient().get_game_detail(rawg_candidates[0]["source_id"])
                rawg_shots = json.loads(rawg_detail.get("screenshots") or "[]")
                if rawg_shots:
                    candidates["rawg"] = rawg_shots
        except Exception:
            logger.warning("VNDB 游戏从 RAWG 补截图失败：%s", game.title, exc_info=True)
    if game.source_type == "dlsite":
        # 优先用扫描时已存入的 API 截图 URL（dlsite_client._map_detail 已填充）
        try:
            existing_shots = json.loads(game.screenshots or "[]")
        except json.JSONDecodeError:
            existing_shots = []
        if existing_shots:
            candidates["dlsite"] = existing_shots
        elif game.source_id:
            candidates["dlsite"] = await _dlsite_screenshots(game.source_id)

    logger.info("截图候选：rawg=%d, steam=%d, vndb=%d, dlsite=%d",
                len(candidates.get("rawg", [])), len(candidates.get("steam", [])),
                len(candidates.get("vndb", [])), len(candidates.get("dlsite", [])))
    directory = settings.data_dir / "screenshots"
    directory.mkdir(parents=True, exist_ok=True)
    if requested_source:
        priority = [requested_source]
        # 指定来源时，无论是否找到截图，都记录用户选择的来源
        game.screenshot_source = requested_source
    elif game.source_type == "steam" and "steam" in priority:
        priority = ["steam"] + [s for s in priority if s != "steam"]
    for source in priority:
        urls = candidates.get(source, [])[:limit]
        if not urls:
            continue
        cached = []
        dl_headers = {"User-Agent": "Mozilla/5.0"}
        # DLSite 图片有防盗链，需带 Referer 才能下载
        if any("dlsite.jp" in u for u in urls):
            dl_headers["Referer"] = "https://www.dlsite.com/"
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=dl_headers) as client:
            for index, url in enumerate(urls):
                if not url:
                    continue
                # 本地路径候选：校验文件名来源前缀匹配，避免把其他来源的本地路径当作当前来源候选
                if url.startswith("/data/screenshots/"):
                    fname = url.split("/")[-1]
                    expected_prefix = f"{game.id}_{source}_"
                    if not fname.startswith(expected_prefix):
                        continue  # 来源不匹配，跳过
                    # 来源匹配且文件存在，直接复用
                    if (directory / fname).exists():
                        cached.append(url)
                        continue
                    continue  # 文件不存在，跳过（本地路径无法重新下载）
                suffix = ".png" if ".png" in url.lower() else ".jpg"
                filename = f"{game.id}_{source}_{index}{suffix}"
                path = directory / filename
                try:
                    if not path.exists():
                        response = await client.get(url)
                        response.raise_for_status()
                        path.write_bytes(response.content)
                    cached.append(f"/data/screenshots/{filename}")
                except (httpx.HTTPError, OSError):
                    continue
        if cached:
            game.screenshots = json.dumps(cached, ensure_ascii=False)
            game.screenshot_source = source
            logger.info("截图缓存完成：game_id=%s, source=%s, cached=%d", game.id, source, len(cached))
            return cached
    logger.warning("截图获取失败：game_id=%s，所有来源均无有效截图", game.id)
    return []


async def _vndb_screenshots(source_id: str) -> list[str]:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post("https://api.vndb.org/kana/vn", json={"filters": ["id", "=", source_id], "fields": "image.url"})
        response.raise_for_status()
        return [(item.get("image") or {}).get("url", "") for item in response.json().get("results", [])]


async def _dlsite_screenshots(source_id: str) -> list[str]:
    async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
        response = await client.get(f"https://www.dlsite.com/maniax/work/=/product_id/{source_id}.html")
        response.raise_for_status()
    urls = re.findall(r'https?://[^"\']+\.(?:jpg|jpeg|png)', response.text, re.I)
    return list(dict.fromkeys(urls))


async def search_vndb(query: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post("https://api.vndb.org/kana/vn", json={"filters": ["search", "=", query], "fields": "id,title,alttitle,description,image.url,developers.name,released,rating,tags.name", "results": 20})
        response.raise_for_status()
        results = []
        for item in response.json().get("results", []):
            released = item.get("released")
            try:
                rel_date = date.fromisoformat(released) if released else None
            except (ValueError, TypeError):
                rel_date = None
            results.append({"source_type": "vndb", "source_id": item.get("id", ""), "title": item.get("title", ""), "alias": item.get("alttitle", ""), "cover_url": (item.get("image") or {}).get("url", ""), "description": item.get("description", ""), "developer": ", ".join(x.get("name", "") for x in item.get("developers", [])), "publisher": "", "release_date": rel_date, "rating": item.get("rating"), "tags": ", ".join(x.get("name", "") for x in item.get("tags", [])), "series": ""})
        return results


async def get_vndb_detail(source_id: str) -> dict:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            "https://api.vndb.org/kana/vn",
            json={"filters": ["id", "=", source_id], "fields": "id,title,alttitle,description,image.url,developers.name,released,rating,tags.name"},
        )
        response.raise_for_status()
        item = response.json().get("results", [])
        if not item:
            raise ValueError("VNDB 游戏不存在")
        value = item[0]
        released = value.get("released")
        try:
            release_date = date.fromisoformat(released) if released else None
        except (ValueError, TypeError):
            release_date = None
        vndb_title = value.get("title", "")
        vndb_alttitle = value.get("alttitle", "")
        # VNDB 的 title 是罗马音，alttitle 是日文原名；优先用日文原名作为标题（翻译更准确），罗马音存为别名
        if vndb_alttitle and vndb_alttitle != vndb_title:
            display_title = vndb_alttitle
            display_alias = vndb_title
        else:
            display_title = vndb_title
            display_alias = vndb_alttitle
        return {"source_type": "vndb", "source_id": value.get("id", ""), "title": display_title, "alias": display_alias, "cover_url": (value.get("image") or {}).get("url", ""), "description": value.get("description", ""), "developer": ", ".join(x.get("name", "") for x in value.get("developers", [])), "publisher": "", "release_date": release_date, "rating": value.get("rating"), "tags": ", ".join(x.get("name", "") for x in value.get("tags", [])), "series": "", "screenshots": "", "version": ""}


async def search_dlsite(query: str) -> list[dict]:
    from .clients.dlsite_client import DlsiteClient
    return await DlsiteClient().search_games(query, page_size=20)


async def get_dlsite_detail(source_id: str) -> dict:
    from .clients.dlsite_client import DlsiteClient
    return await DlsiteClient().get_game_detail(source_id)


async def search_steam(query: str) -> list[dict]:
    return await SteamClient().search_games(query, page_size=20)


async def get_steam_detail(source_id: str) -> dict:
    return await SteamClient().get_game_detail(source_id)


async def refresh_game_metadata(game_id: int) -> None:
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game or not game.source_id:
            raise RuntimeError("游戏不存在或缺少数据源 ID")
        if game.source_type == "rawg":
            metadata = await RawgClient().get_game_detail(game.source_id)
        elif game.source_type == "steam":
            metadata = await get_steam_detail(game.source_id)
        elif game.source_type == "vndb":
            metadata = await get_vndb_detail(game.source_id)
        elif game.source_type == "dlsite":
            metadata = await get_dlsite_detail(game.source_id)
        else:
            raise RuntimeError("当前数据源不支持刷新元数据")
        # 翻译前保存不参与翻译但需要保留的字段
        _preserved = {k: metadata.get(k) for k in ("english_name", "cover_url", "release_date", "rating", "version") if metadata.get(k) is not None}
        metadata = await translation_service.translate_metadata(session, metadata)
        # 翻译后合并回保留字段（翻译服务可能丢失这些字段）
        metadata.update(_preserved)
        for field in ("title", "alias", "description", "developer", "publisher", "release_date", "rating", "tags", "series", "source_type", "source_id", "screenshots", "version", "original_data", "steam_appid"):
            if field in metadata:
                setattr(game, field, metadata[field])
        game.cover_url = metadata.get("cover_url", "")
        await session.commit()
        try:
            game.cover_url = await cache_cover(game.id, metadata.get("cover_url", ""), game.source_type)
            game.cover_source = game.source_type
            await session.commit()
        except Exception:
            await session.rollback()

        # 多来源刷新：遍历 source_ids 中其他来源，逐个更新 source_data
        try:
            source_ids = json.loads(game.source_ids or "{}")
        except json.JSONDecodeError:
            source_ids = {}
        try:
            source_data = json.loads(game.source_data or "{}")
        except json.JSONDecodeError:
            source_data = {}
        # 主来源数据也存入 source_data
        if game.source_type and game.source_type != "custom":
            source_data[game.source_type] = {k: v for k, v in metadata.items() if k not in ("resource_type", "resource_url", "play_status", "original_data")}
            source_ids[game.source_type] = game.source_id
        # 刷新其他已有来源
        for src, sid in list(source_ids.items()):
            if src == game.source_type or not sid:
                continue
            try:
                if src == "rawg":
                    src_meta = await RawgClient().get_game_detail(sid)
                elif src == "steam":
                    src_meta = await get_steam_detail(sid)
                elif src == "vndb":
                    src_meta = await get_vndb_detail(sid)
                elif src == "dlsite":
                    src_meta = await get_dlsite_detail(sid)
                else:
                    continue
                source_data[src] = src_meta
                logger.info("多来源刷新：game_id=%s, source=%s 成功", game.id, src)
            except Exception as e:
                logger.warning("多来源刷新：game_id=%s, source=%s 失败: %s", game.id, src, e)
        # 尝试补充新来源（搜索匹配）
        for src in ["steam", "rawg", "vndb", "dlsite"]:
            if src in source_ids and source_ids[src]:
                continue
            try:
                if src == "steam":
                    results = await search_steam(game.title)
                    if results:
                        source_ids["steam"] = results[0]["source_id"]
                        source_data["steam"] = await get_steam_detail(results[0]["source_id"])
                elif src == "rawg":
                    rawg_client = RawgClient()
                    def _filter_main(results):
                        return [r for r in results if not re.search(r"(?i)(typing|dlc|demo|trial|soundtrack|ost|art pack)", r.get("title", ""))]
                    # 优先用 steam 英文名搜索（更准确）
                    steam_eng = ""
                    if isinstance(source_data.get("steam"), dict):
                        steam_eng = source_data["steam"].get("english_name", "")
                    elif "steam" in source_data:
                        try:
                            steam_eng = json.loads(source_data["steam"]).get("english_name", "") if isinstance(source_data["steam"], str) else ""
                        except:
                            pass
                    results = []
                    if steam_eng:
                        try:
                            eng_results = _filter_main(await rawg_client.search_games(steam_eng, page_size=5))
                            if eng_results:
                                results = eng_results
                                logger.info("多来源刷新：game_id=%s, 通过 steam 英文名 '%s' 搜索到 rawg=%s", game.id, steam_eng, eng_results[0]["source_id"])
                        except Exception as e:
                            logger.warning("多来源刷新：game_id=%s, steam 英文名搜索 rawg 失败: %s", game.id, e)
                    # 英文名没找到时，用中文名搜索作为 fallback
                    if not results:
                        results = _filter_main(await rawg_client.search_games(game.title, page_size=5))
                    if results:
                        source_ids["rawg"] = results[0]["source_id"]
                        source_data["rawg"] = await rawg_client.get_game_detail(results[0]["source_id"])
                elif src == "vndb":
                    results = await search_vndb(game.title)
                    if results:
                        source_ids["vndb"] = results[0]["source_id"]
                        source_data["vndb"] = await get_vndb_detail(results[0]["source_id"])
                elif src == "dlsite":
                    from .clients.dlsite_client import DlsiteClient
                    dlsite = DlsiteClient()
                    workno = dlsite.extract_workno(game.title) or dlsite.extract_workno(game.resource_url or "")
                    if workno:
                        source_ids["dlsite"] = workno
                        source_data["dlsite"] = await get_dlsite_detail(workno)
                if src in source_ids and source_ids[src]:
                    logger.info("多来源刷新：game_id=%s, 补充新来源 %s=%s", game.id, src, source_ids[src])
            except Exception as e:
                logger.warning("多来源刷新：game_id=%s, 补充来源 %s 失败: %s", game.id, src, e)
        game.source_ids = json.dumps(source_ids, ensure_ascii=False, default=str)
        game.source_data = json.dumps(source_data, ensure_ascii=False, default=str)
        await session.commit()


async def refresh_vndb_game_metadata(game_id: int) -> None:
    """按当前标题重新搜索 VNDB，并用首个结果的详情刷新游戏元数据。"""
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")

        results = await search_vndb(game.title)
        if not results:
            raise RuntimeError("VNDB 未找到匹配的游戏")
        source_id = results[0].get("source_id", "")
        if not source_id:
            raise RuntimeError("VNDB 搜索结果缺少数据源 ID")

        metadata = await get_vndb_detail(source_id)
        metadata = await translation_service.translate_metadata(session, metadata)
        for field in (
            "title", "alias", "description", "developer", "publisher", "release_date",
            "rating", "tags", "series", "source_type", "source_id", "screenshots", "version",
            "original_data",
        ):
            if field in metadata:
                setattr(game, field, metadata[field])
        game.cover_url = metadata.get("cover_url", "")
        await session.commit()

        try:
            game.cover_url = await cache_cover(game.id, metadata.get("cover_url", ""), "vndb")
            game.cover_source = "vndb"
            await session.commit()
        except Exception:
            await session.rollback()


async def refresh_rawg_game_metadata(game_id: int) -> None:
    """仅访问 RAWG、SQLite 和 /app/data 封面缓存，绝不访问 WebDAV。"""
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")
        if game.source_type != "rawg" or not game.source_id:
            raise RuntimeError("仅支持刷新具有 RAWG 数据源 ID 的游戏")

        metadata = await RawgClient().get_game_detail(game.source_id)
        metadata = await translation_service.translate_metadata(session, metadata)
        for field in (
            "title", "alias", "description", "developer", "publisher", "release_date",
            "rating", "tags", "series", "source_type", "source_id", "screenshots", "version", "original_data",
            "steam_appid",
        ):
            setattr(game, field, metadata[field])
        # 先保存远程 URL；封面 CDN 临时失败不应使元数据刷新失败。
        game.cover_url = metadata["cover_url"]
        await session.commit()

        steam_url = ""
        if metadata.get("steam_appid"):
            steam_url = RawgClient.steam_cover_url(metadata["steam_appid"])
        cover_url = steam_url or metadata["cover_url"]
        cover_source = "steam" if steam_url else "rawg"
        try:
            game.cover_url = await cache_cover(game.id, cover_url, cover_source)
            game.cover_source = cover_source
            await session.commit()
        except Exception:
            await session.rollback()
