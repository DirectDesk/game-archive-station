import hashlib
import json
import re
from pathlib import Path

import httpx

from .clients.rawg_client import RawgClient
from .cover_service import cache_cover
from .database import SessionLocal
from .models import Game, SystemConfig
from .translation_service import translation_service
from .config import settings


async def fetch_game_screenshots(game: Game, config: SystemConfig, requested_source: str = "") -> list[str]:
    """按配置优先级获取并缓存截图；失败的来源会继续尝试下一个来源。"""
    try:
        priority = json.loads(config.screenshot_source_priority or "[]")
    except json.JSONDecodeError:
        priority = []
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
    if game.steam_appid:
        candidates["steam"] = [
            f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{game.steam_appid}/ss_{index}.jpg"
            for index in range(1, limit + 1)
        ]
    if game.source_type == "vndb" and game.source_id:
        candidates["vndb"] = await _vndb_screenshots(game.source_id)
    if game.source_type == "dlsite" and game.source_id:
        candidates["dlsite"] = await _dlsite_screenshots(game.source_id)

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
            return cached
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
            results.append({"source_type": "vndb", "source_id": item.get("id", ""), "title": item.get("title", ""), "alias": item.get("alttitle", ""), "cover_url": (item.get("image") or {}).get("url", ""), "description": item.get("description", ""), "developer": ", ".join(x.get("name", "") for x in item.get("developers", [])), "publisher": "", "release_date": item.get("released"), "rating": item.get("rating"), "tags": ", ".join(x.get("name", "") for x in item.get("tags", [])), "series": ""})
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
        return {"source_type": "vndb", "source_id": value.get("id", ""), "title": value.get("title", ""), "alias": value.get("alttitle", ""), "cover_url": (value.get("image") or {}).get("url", ""), "description": value.get("description", ""), "developer": ", ".join(x.get("name", "") for x in value.get("developers", [])), "publisher": "", "release_date": value.get("released"), "rating": value.get("rating"), "tags": ", ".join(x.get("name", "") for x in value.get("tags", [])), "series": "", "screenshots": "", "version": ""}


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

        try:
            game.cover_url = await cache_cover(game.id, metadata["cover_url"], "rawg")
            game.cover_source = "rawg"
            await session.commit()
        except Exception:
            await session.rollback()
