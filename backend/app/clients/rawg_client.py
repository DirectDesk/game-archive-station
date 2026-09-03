import json
from datetime import date

import httpx

from ..config import settings
from ..database import SessionLocal
from ..models import SystemConfig


class RawgClient:
    base_url = "https://api.rawg.io/api"

    def __init__(self):
        pass

    async def _api_key(self) -> str:
        async with SessionLocal() as db:
            config = await db.get(SystemConfig, 1)
            key = config.rawg_api_key if config and config.rawg_api_key else settings.rawg_api_key
        if not key:
            raise ValueError("未配置 RAWG_API_KEY")
        return key

    async def search_games(self, query: str, page_size: int = 10) -> list[dict]:
        key = await self._api_key()
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{self.base_url}/games",
                params={"key": key, "search": query, "page_size": page_size},
            )
            response.raise_for_status()
            return [
                {"source_type": "rawg", "source_id": str(item["id"]), "title": item.get("name", ""), "cover_url": item.get("background_image", "")}
                for item in response.json().get("results", [])
            ]

    async def get_game_detail(self, rawg_id: str) -> dict:
        key = await self._api_key()
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{self.base_url}/games/{rawg_id}",
                params={"key": key},
            )
            response.raise_for_status()
            return self._map_detail(response.json())

    @staticmethod
    def _map_detail(item: dict) -> dict:
        developers = ", ".join(value.get("name", "") for value in item.get("developers", []))
        publishers = ", ".join(value.get("name", "") for value in item.get("publishers", []))
        tags = ", ".join(value.get("name", "") for value in item.get("tags", []))
        screenshots = [value["image"] for value in item.get("short_screenshots", [])[1:] if value.get("image")]
        release_date = item.get("released")
        return {
            "title": item.get("name", ""),
            "alias": "",
            "cover_url": item.get("background_image", ""),
            "screenshots": json.dumps(screenshots),
            "description": item.get("description_raw", "") or "",
            "developer": developers,
            "publisher": publishers,
            "release_date": date.fromisoformat(release_date) if release_date else None,
            "rating": (item.get("rating") or 0) * 20 if item.get("rating") is not None else None,
            "tags": tags,
            "series": "",
            "source_type": "rawg",
            "source_id": str(item.get("id", "")),
        }
