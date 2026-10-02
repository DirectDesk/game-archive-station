import asyncio
import json
import re
import time
from datetime import date

import httpx


class DlsiteClient:
    """DLsite 官方 API 客户端（同人游戏 maniax 分类）

    反爬策略：
    - 每次请求间隔 >= 1 秒（类级别限速）
    - 带浏览器 UA
    - 失败指数退避重试（最多3次）
    """

    base_url = "https://www.dlsite.com/maniax/api/=/product.json"
    _user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    _min_interval = 1.0  # 秒
    _last_request_time = 0.0
    _lock = asyncio.Lock()

    def __init__(self):
        pass

    @classmethod
    async def _throttle(cls):
        """类级别限速：确保两次请求间隔 >= _min_interval"""
        async with cls._lock:
            now = time.monotonic()
            elapsed = now - cls._last_request_time
            if elapsed < cls._min_interval:
                await asyncio.sleep(cls._min_interval - elapsed)
            cls._last_request_time = time.monotonic()

    async def _request(self, params: dict, retries: int = 3) -> dict:
        """带重试退避的 GET 请求"""
        headers = {"User-Agent": self._user_agent, "Accept": "application/json"}
        for attempt in range(retries):
            await self._throttle()
            try:
                async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                    response = await client.get(self.base_url, params=params, headers=headers)
                    response.raise_for_status()
                    data = response.json()
                    # DLsite API 对不同 UA/请求方式返回结构不同：
                    # 有时返回 {"value": [...], "Count": N}，有时直接返回 [...]
                    if isinstance(data, list):
                        return {"value": data}
                    return data
            except Exception as exc:
                if attempt == retries - 1:
                    raise
                wait = 2 ** attempt  # 1s, 2s, 4s 退避
                await asyncio.sleep(wait)
        return {}

    async def get_work_by_id(self, workno: str) -> dict | None:
        """按作品编号（RJ/VJ/BJ+数字）查询商品详情"""
        data = await self._request({"workno": workno})
        results = data.get("value", [])
        if not results:
            return None
        return results[0]

    async def search_games(self, query: str, page_size: int = 10) -> list[dict]:
        """按关键词搜索（备用，编号精准查询优先）"""
        data = await self._request({"keyword": query, "per_page": page_size})
        results = data.get("value", [])
        return [
            {
                "source_type": "dlsite",
                "source_id": item.get("workno", ""),
                "title": item.get("work_name", ""),
                "cover_url": self._full_url((item.get("image_main") or {}).get("url", "")),
            }
            for item in results[:page_size]
        ]

    async def get_game_detail(self, workno: str) -> dict:
        """按编号获取完整游戏元数据，映射为统一格式"""
        item = await self.get_work_by_id(workno)
        if not item:
            raise ValueError(f"DLsite 作品不存在: {workno}")
        return self._map_detail(item)

    @staticmethod
    def _full_url(url: str) -> str:
        """补全协议相对 URL（//img.dlsite.jp/... -> https://img.dlsite.jp/...）"""
        if url.startswith("//"):
            return "https:" + url
        return url

    @staticmethod
    def extract_workno(text: str) -> str | None:
        """从文件夹名中提取 RJ/VJ/BJ 编号"""
        match = re.search(r"(?:RJ|VJ|BJ)\d{6,}", text, re.IGNORECASE)
        if match:
            return match.group(0).upper()
        return None

    @staticmethod
    def _map_detail(item: dict) -> dict:
        """将 DLsite API 返回映射为统一元数据格式"""
        workno = item.get("workno", "")
        title = item.get("work_name", "")
        alias = item.get("work_name_kana", "")
        developer = item.get("maker_name", "")
        description = item.get("intro_s", "") or ""

        # 发售日：regist_date 格式 "2019-08-31 16:00:00"
        regist_date = item.get("regist_date", "")
        release_date = None
        if regist_date:
            try:
                release_date = date.fromisoformat(regist_date.split(" ")[0])
            except (ValueError, TypeError):
                release_date = None

        # 评分：rate_average_star 是 0-50（对应0-5星），转为百分制
        rating = None
        rate_star = item.get("rate_average_star")
        if rate_star is not None:
            try:
                rating = float(rate_star) * 2  # 0-50 -> 0-100
            except (ValueError, TypeError):
                rating = None

        # 标签：genres[].name
        tags = ", ".join(g.get("name", "") for g in item.get("genres", []) if g.get("name"))

        # 封面：image_main.url
        cover_url = DlsiteClient._full_url((item.get("image_main") or {}).get("url", ""))

        # 截图：DLSite API 返回的 image_samples URL 对旧游戏常返回 404（DLSite 清理了旧截图存储），
        # 暂不存入 screenshots，避免前端显示裂开的图片；后续可从详情页或其他数据源补截图。
        screenshots = []

        version_match = re.search(
            r"(?i)(?<![a-z0-9])v\d+(?:\.\d+){1,3}(?![a-z0-9])", title
        )

        return {
            "title": title,
            "alias": alias,
            "cover_url": cover_url,
            "screenshots": json.dumps(screenshots, ensure_ascii=False),
            "version": version_match.group(0) if version_match else "",
            "description": description,
            "developer": developer,
            "publisher": developer,  # 同人游戏开发商=发行商
            "release_date": release_date,
            "rating": rating,
            "tags": tags,
            "series": "",
            "source_type": "dlsite",
            "source_id": workno,
            "steam_appid": "",
        }
