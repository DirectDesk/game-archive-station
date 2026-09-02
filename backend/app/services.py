import httpx

from .clients.rawg_client import RawgClient
from .cover_service import cache_cover
from .database import SessionLocal
from .models import Game


async def search_vndb(query: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post("https://api.vndb.org/kana/vn", json={"filters": ["search", "=", query], "fields": "id,title,alttitle,description,image.url,developers.name,released,rating,tags.name", "results": 20})
        response.raise_for_status()
        results = []
        for item in response.json().get("results", []):
            results.append({"source_type": "vndb", "source_id": item.get("id", ""), "title": item.get("title", ""), "alias": item.get("alttitle", ""), "cover_url": (item.get("image") or {}).get("url", ""), "description": item.get("description", ""), "developer": ", ".join(x.get("name", "") for x in item.get("developers", [])), "publisher": "", "release_date": item.get("released"), "rating": item.get("rating"), "tags": ", ".join(x.get("name", "") for x in item.get("tags", [])), "series": ""})
        return results


async def refresh_rawg_game_metadata(game_id: int) -> None:
    """仅访问 RAWG、SQLite 和 /app/data 封面缓存，绝不访问 WebDAV。"""
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")
        if game.source_type != "rawg" or not game.source_id:
            raise RuntimeError("仅支持刷新具有 RAWG 数据源 ID 的游戏")

        metadata = await RawgClient().get_game_detail(game.source_id)
        for field in (
            "title", "alias", "description", "developer", "publisher", "release_date",
            "rating", "tags", "series", "source_type", "source_id",
        ):
            setattr(game, field, metadata[field])
        # 先保存远程 URL；封面 CDN 临时失败不应使元数据刷新失败。
        game.cover_url = metadata["cover_url"]
        await session.commit()

        try:
            game.cover_url = await cache_cover(game.id, metadata["cover_url"])
            await session.commit()
        except Exception:
            await session.rollback()
