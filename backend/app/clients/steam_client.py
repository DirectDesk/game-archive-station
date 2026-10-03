import asyncio
import json
import re
from datetime import date

import httpx


class SteamClient:
    """Steam 数据源客户端：商店搜索 + appdetails 反查。
    Steam 对中文名搜索支持好，appdetails 返回中文元数据，无需翻译。
    """
    search_url = "https://store.steampowered.com/api/storesearch"
    detail_url = "https://store.steampowered.com/api/appdetails"
    # 限速：Steam API 有频率限制，≥1秒/请求
    _lock = asyncio.Lock()
    _last_request = 0.0

    def __init__(self):
        pass

    async def _throttle(self):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = 1.0 - (now - self._last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = asyncio.get_event_loop().time()

    async def search_games(self, query: str, page_size: int = 10) -> list[dict]:
        """Steam 商店搜索，支持中文名。返回统一格式列表。"""
        await self._throttle()
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            response = await client.get(
                self.search_url,
                params={"term": query, "l": "schinese", "cc": "cn"},
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            data = response.json()
            results = []
            for item in data.get("items", [])[:page_size]:
                appid = str(item.get("id", ""))
                if not appid:
                    continue
                # 只保留主游戏（type=app），过滤 DLC/原声集/试玩版等
                item_type = item.get("type", "")
                if item_type and item_type != "app":
                    continue
                results.append({
                    "source_type": "steam",
                    "source_id": appid,
                    "title": item.get("name", ""),
            "english_name": item.get("name", ""),
                    "cover_url": item.get("tiny_image", ""),
                })
            return results

    async def get_game_detail(self, appid: str) -> dict:
        """Steam appdetails 反查，返回完整元数据（中文）。"""
        await self._throttle()
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            response = await client.get(
                self.detail_url,
                params={"appids": appid, "l": "schinese", "cc": "cn"},
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
            data = response.json()
            app_data = data.get(str(appid), {})
            if not app_data.get("success"):
                raise ValueError(f"Steam appdetails 返回失败: appid={appid}")
            return self._map_detail(app_data.get("data", {}), appid)

    @staticmethod
    def library_cover_url(appid: str) -> str:
        """Steam 竖版封面（600x900），作为封面首选。"""
        return f"https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/{appid}/library_600x900.jpg"

    @staticmethod
    def _parse_release_date(date_str: str) -> date | None:
        """解析 Steam 中文日期格式：'2022 年 1 月 21 日' 或 '2022-01-21'。"""
        if not date_str:
            return None
        # 尝试 ISO 格式
        try:
            return date.fromisoformat(date_str.strip())
        except (ValueError, AttributeError):
            pass
        # 中文格式：2022 年 1 月 21 日
        match = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", date_str)
        if match:
            try:
                return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            except ValueError:
                return None
        return None

    @staticmethod
    def _map_detail(item: dict, appid: str) -> dict:
        """将 Steam appdetails 映射为统一格式。"""
        developers = ", ".join(item.get("developers", []))
        publishers = ", ".join(item.get("publishers", []))
        # 标签：genres + categories（去重）
        tag_set = []
        for g in item.get("genres", []):
            desc = g.get("description", "")
            if desc and desc not in tag_set:
                tag_set.append(desc)
        for c in item.get("categories", []):
            desc = c.get("description", "")
            if desc and desc not in tag_set:
                tag_set.append(desc)
        tags = ", ".join(tag_set)
        # 截图：path_full（原图），去掉第一张（通常是封面/宣传图）
        screenshots = []
        for s in item.get("screenshots", []):
            url = s.get("path_full", "")
            if url:
                screenshots.append(url)
        # 封面：优先用竖版 library_600x900
        cover_url = SteamClient.library_cover_url(appid)
        # 评分：metacritic 转百分制（rawg 是 0-5 分转 0-100，Steam 用 metacritic 直接是百分制）
        rating = None
        metacritic = item.get("metacritic")
        if metacritic and metacritic.get("score") is not None:
            rating = float(metacritic["score"])
        # 发售日
        release_date = SteamClient._parse_release_date(
            (item.get("release_date") or {}).get("date", "")
        )
        # 简介：优先用 detailed_description（去掉 HTML 标签），回退 short_description
        description = item.get("detailed_description", "") or item.get("short_description", "") or ""
        # 去掉 HTML 标签
        description = re.sub(r"<[^>]+>", "", description).strip()
        original_data = json.dumps({"steam_tags": tags}, ensure_ascii=False)
        return {
            "title": item.get("name", ""),
            "english_name": item.get("name", ""),
            "alias": "",
            "cover_url": cover_url,
            "screenshots": json.dumps(screenshots, ensure_ascii=False),
            "version": "",
            "description": description,
            "developer": developers,
            "publisher": publishers,
            "release_date": release_date,
            "rating": rating,
            "tags": tags,
            "series": "",
            "source_type": "steam",
            "source_id": str(appid),
            "steam_appid": str(appid),
            "original_data": original_data,
        }
