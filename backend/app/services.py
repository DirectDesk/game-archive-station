from datetime import date
import hashlib
import json
import logging
import re
from pathlib import Path

import httpx

from .clients.rawg_client import RawgClient
from .cover_service import cache_cover
from .database import SessionLocal
from .models import Game, SystemConfig
from .translation_service import translation_service
from .config import settings

logger = logging.getLogger(__name__)


async def fetch_game_screenshots(game: Game, config: SystemConfig, requested_source: str = "") -> list[str]:
    """按配置优先级获取并缓存截图；失败的来源会继续尝试下一个来源。"""
    logger.info("开始获取截图：game_id=%s, source_type=%s, source_id=%s, screenshots_field=%.100s",
                game.id, game.source_type, game.source_id, game.screenshots or "")
    try:
        priority = json.loads(config.screenshot_source_priority or "[]")
    except json.JSONDecodeError:
        priority = ["rawg", "steam", "vndb", "dlsite"]
    limit = max(0, int(config.max_screenshots or 5))
    metadata = {}
    try:
        metadata = json.loads(game.original_data or "{}")
    except json.JSONDecodeError:
        pass
    candidates: dict[str, list[str]] = {}
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
    # Steam 无公开截图 CDN 规则，移除无效的 ss_N.jpg 猜测；
    # 截图统一从 RAWG 获取（RAWG 的 short_screenshots 已包含 Steam 来源截图）。
    if steam_appid:
        logger.info("游戏 %s 有 steam_appid=%s，但截图来源跳过 Steam（无有效截图 CDN）", game.id, steam_appid)
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
    if game.source_type == "dlsite" and game.source_id:
        candidates["dlsite"] = await _dlsite_screenshots(game.source_id)

    logger.info("截图候选：rawg=%d, vndb=%d, dlsite=%d",
                len(candidates.get("rawg", [])), len(candidates.get("vndb", [])), len(candidates.get("dlsite", [])))
    directory = settings.data_dir / "screenshots"
    directory.mkdir(parents=True, exist_ok=True)
    if requested_source:
        priority = [requested_source]
    for source in priority:
        urls = candidates.get(source, [])[:limit]
        if not urls:
            continue
        cached = []
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            for index, url in enumerate(urls):
                if not url:
                    continue
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


async def refresh_game_metadata(game_id: int) -> None:
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game or not game.source_id:
            raise RuntimeError("游戏不存在或缺少数据源 ID")
        if game.source_type == "rawg":
            metadata = await RawgClient().get_game_detail(game.source_id)
        elif game.source_type == "vndb":
            metadata = await get_vndb_detail(game.source_id)
        else:
            raise RuntimeError("当前数据源不支持刷新元数据")
        metadata = await translation_service.translate_metadata(session, metadata)
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
