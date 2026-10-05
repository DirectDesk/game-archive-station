import json
import os
import re
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
            key = config.rawg_api_key if config and config.rawg_api_key else os.getenv("RAWG_API_KEY", "") or settings.rawg_api_key
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

    # 【已删除】get_game_by_steam_appid(steam_appid)
    #
    # 原实现走 GET /games?steam_appid=<id>，但 RAWG 的 /games 端点**不支持**该过滤参数：
    # 参数被静默忽略，接口只是返回游戏列表的默认第一条，与传入的 appid 无关。
    # 实测对照（三个不同 appid 返回完全相同的结果）：
    #     steam_appid=3101040    -> rawg_id=3498 'Grand Theft Auto V'
    #     steam_appid=999999999  -> rawg_id=3498 'Grand Theft Auto V'  (编造)
    #     steam_appid=1          -> rawg_id=3498 'Grand Theft Auto V'
    # 参数有效性对照：
    #     ?steam_appid=3101040   -> count=901101（等于全库总数，参数无效）
    #     ?stores=1              -> count=123597（RAWG 支持 stores 过滤，参数有效）
    #
    # 因此该方法是个「静默返回错误数据」的坏接口，任何调用方都会把 GTA V 的元数据
    # 写到目标游戏上。因无实际需求（steam 为主来源、rawg 仅作补充），直接移除而非修复。
    # 若将来确需「Steam AppID -> RAWG」反查，正确思路是：
    #   1) 用 stores=1 限定 Steam 商店，再用 search=<Steam 游戏名> 搜索；
    #   2) 对候选逐个 get_game_detail()，取返回的 steam_appid 与目标 appid 相等者。
    #   切勿依赖 /games?steam_appid= 这类不存在的过滤参数。

    async def get_game_detail(self, rawg_id: str) -> dict:
        key = await self._api_key()
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{self.base_url}/games/{rawg_id}",
                params={"key": key},
            )
            response.raise_for_status()
            stores = None
            try:
                stores_response = await client.get(
                    f"{self.base_url}/games/{rawg_id}/stores",
                    params={"key": key},
                )
                stores_response.raise_for_status()
                stores = stores_response.json()
            except Exception:
                # 商店接口不是主元数据依赖；失败时保留 RAWG 主详情并使用空 steam_appid。
                stores = None
            # 截图接口：RAWG 详情接口不返回 short_screenshots，需单独调 /games/{id}/screenshots
            screenshots = []
            try:
                shots_response = await client.get(
                    f"{self.base_url}/games/{rawg_id}/screenshots",
                    params={"key": key},
                )
                shots_response.raise_for_status()
                screenshots = [s.get("image", "") for s in shots_response.json().get("results", []) if s.get("image")]
            except Exception:
                # 截图接口失败不影响主元数据
                pass
            return self._map_detail(response.json(), stores, screenshots)

    @staticmethod
    def steam_cover_url(appid: str | int) -> str:
        return f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{appid}/library_600x900.jpg"

    @staticmethod
    def _map_detail(item: dict, stores: dict | None = None, screenshots: list | None = None) -> dict:
        developers = ", ".join(value.get("name", "") for value in item.get("developers", []))
        publishers = ", ".join(value.get("name", "") for value in item.get("publishers", []))
        tags = ", ".join(value.get("name", "") for value in item.get("tags", []))
        # 优先用截图接口返回的截图；回退到详情中的 short_screenshots（去掉第1张封面）
        if screenshots is None:
            screenshots = [value["image"] for value in item.get("short_screenshots", [])[1:] if value.get("image")]
        release_date = item.get("released")
        version_match = re.search(r"(?i)(?<![a-z0-9])v\d+(?:\.\d+){1,3}(?![a-z0-9])", item.get("name", ""))
        steam_appid = ""
        for store in (stores or {}).get("results", []):
            if str(store.get("store_id")) == "1":
                match = re.search(r"/app/(\d+)", store.get("url_en") or store.get("url") or "")
                if match:
                    steam_appid = match.group(1)
                    break
        return {
            "title": item.get("name", ""),
            "alias": "",
            "cover_url": item.get("background_image", ""),
            "screenshots": json.dumps(screenshots),
            "version": version_match.group(0) if version_match else "",
            "description": item.get("description_raw", "") or "",
            "developer": developers,
            "publisher": publishers,
            "release_date": release_date if release_date else None,  # RAWG 返回的已是 ISO 字符串，不转 date 对象
            "rating": (item.get("rating") or 0) * 20 if item.get("rating") is not None else None,
            "tags": tags,
            "series": "",
            "source_type": "rawg",
            "source_id": str(item.get("id", "")),
            "steam_appid": steam_appid,
        }
