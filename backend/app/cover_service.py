from pathlib import Path

import httpx

from .config import settings


async def cache_cover(game_id: int, cover_url: str) -> str:
    """下载封面到应用数据卷；只操作 /app/data，不触碰 WebDAV 挂载。"""
    if not cover_url.startswith(("http://", "https://")):
        return cover_url
    cover_dir = settings.data_dir / "covers"
    cover_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{game_id}.jpg"
    target = cover_dir / filename
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        response = await client.get(cover_url)
        response.raise_for_status()
        target.write_bytes(response.content)
    return f"/data/covers/{filename}"