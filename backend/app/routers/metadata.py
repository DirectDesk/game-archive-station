from fastapi import APIRouter, HTTPException, Query

from ..clients.dlsite_client import DlsiteClient
from ..clients.rawg_client import RawgClient
from ..services import search_vndb

router = APIRouter(prefix="/api/metadata", tags=["metadata"])


@router.get("/search")
async def search_metadata(source_type: str = Query(..., pattern="^(rawg|vndb|dlsite)$"), q: str = Query(..., min_length=1)):
    try:
        if source_type == "vndb":
            return await search_vndb(q)
        if source_type == "dlsite":
            return await DlsiteClient().search_games(q)
        client = RawgClient()
        candidates = await client.search_games(q)
        # 搜索接口只返回候选摘要，详情接口负责补齐游戏表所需字段。
        return [await client.get_game_detail(item["source_id"]) for item in candidates]
    except Exception as exc:
        raise HTTPException(502, f"元数据搜索失败：{exc}") from exc
