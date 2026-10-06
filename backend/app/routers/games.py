from datetime import datetime, timedelta
from pathlib import Path

import json

from fastapi import APIRouter, Body, Depends, File, HTTPException, Query, Request, UploadFile
from sqlalchemy import desc, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..clients.rawg_client import RawgClient
from ..cover_service import cache_cover
from ..database import get_db
from ..models import Game, SystemConfig
from ..schemas import GameCreate, GameOut, GameUpdate, MatchApplyPayload, RefreshMetadataTaskOut, RetranslatePayload
from ..services import apply_match, fetch_game_screenshots, find_match_candidates, refresh_game_metadata, refresh_rawg_game_metadata, refresh_vndb_game_metadata, retranslate_game, search_steam, resolve_tags, get_steam_detail, get_vndb_detail, get_dlsite_detail, normalize_release_date
from ..task_manager import task_manager

router = APIRouter(prefix="/api/games", tags=["games"])


@router.get("", response_model=list[GameOut])
async def list_games(db: AsyncSession = Depends(get_db), q: str = "", source_type: str = "", play_status: str = "", tag: str = "", tag_source: str = "", game_type: str = "", sort: str = "updated", original: bool = False):
    query = select(Game)
    if q:
        term = f"%{q}%"
        # v1.7.0：title=原始名、title_cn=中文译名，两者都要能搜到
        query = query.where(or_(Game.title.ilike(term), Game.title_cn.ilike(term), Game.alias.ilike(term), Game.developer.ilike(term), Game.publisher.ilike(term), Game.series.ilike(term)))
    if source_type:
        query = query.where(Game.source_type == source_type)
    # 游戏平台类型（pc/android/gal，可多选逗号分隔）。
    # game_type 存的是逗号分隔串（如 "gal,pc"），用 LIKE 做包含匹配；
    # 逗号包裹两端避免 "pc" 误命中 "pcx" 这类前缀。
    gtypes = [v.strip() for v in game_type.split(",") if v.strip()]
    if gtypes:
        query = query.where(
            or_(*(Game.game_type.ilike(f"%{value}%") for value in gtypes))
        )
    if play_status:
        query = query.where(Game.play_status == play_status)
    tags = [value.strip() for value in tag.split(",") if value.strip()]
    # 标签筛选是**精确匹配**：Game.tags 以「, 」分隔存整串，
    # 历史上的 ilike('%单人%') 会连「单人游戏」一起命中（实测「单人」应 6 款却筛出 11 款、
    # 「萝莉」把「小胸女主角（非萝莉）」也算进去）。这里去掉空格后用逗号包裹再 LIKE，
    # 只有「整个标签相等」才算命中。
    if tags and not original and (not tag_source or tag_source == "all"):
        _norm = func.replace(Game.tags, " ", "")
        _padded = literal(",") + _norm + literal(",")
        query = query.where(or_(*(_padded.like(f"%,{value.replace(' ', '')},%") for value in tags)))
    query = query.order_by(desc(Game.rating) if sort == "rating" else desc(Game.created_at) if sort == "created" else desc(Game.updated_at))
    rows = list((await db.scalars(query)).all())
    # 标签筛选分两种口径，必须与 /api/games/all-tags 面板保持一致：
    #  · 面板「所有来源」(tag_source 空/all)：
    #      译文模式 → 打 Game.tags（统一译名，已在上面的 SQL 里精确匹配）；
    #      原文模式 → 在 Python 侧比对 original_data / source_data 里的各来源原文标签。
    #  · 面板选定某来源 (tag_source=rawg/steam/...)：面板列的是「该来源的标签」，
    #      而 Game.tags 只存该游戏主标签源的标签，两者不同源 →
    #      必须按该来源的「显示标签集合」匹配，否则会出现"面板列了却一条都筛不到"。
    if tags and (not tag_source or tag_source == "all"):
        if original:
            rows = [game for game in rows if any(value in _raw_tags_of(game) for value in tags)]
    elif tags:
        _local_glossary = await _load_tag_glossary(db)
        _keep = []
        for game in rows:
            _disp = await _display_tags_of(game, tag_source, original, db, _local_glossary)
            if any(value in _disp for value in tags):
                _keep.append(game)
        rows = _keep
    return rows


def _raw_tags_of(game: Game) -> set[str]:
    """游戏的**原文**标签集合：各来源 source_data[*].tags + original_data.tags（不含 Game.tags 译文）。

    这是「显示原文」模式下标签面板与筛选共用的唯一口径。
    """
    result: set[str] = set()

    def _collect(value):
        if isinstance(value, str):
            result.update(part.strip() for part in value.split(",") if part.strip())
        elif isinstance(value, list):
            result.update(str(part).strip() for part in value if str(part).strip())

    for blob in (game.original_data, game.source_data):
        try:
            data = json.loads(blob or "{}")
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        _collect(data.get("tags"))
        for block in data.values():
            if isinstance(block, dict):
                _collect(block.get("tags"))
    return result


def _all_tags_of(game: Game) -> set[str]:
    """游戏身上的全部标签：译文串 + 各来源原文串（含译文，保留给历史调用）。"""
    result = _raw_tags_of(game)
    result.update(part.strip() for part in str(game.tags or "").split(",") if part.strip())
    return result


async def _load_tag_glossary(db) -> dict:
    """术语表 source_text -> target_text（仅 category='tag'），与 /all-tags 面板同源。"""
    from sqlalchemy import select as _select
    from ..models import TranslationGlossary as _TG
    result: dict = {}
    async for _item in await db.stream_scalars(_select(_TG).where(_TG.category == "tag")):
        result[_item.source_text] = _item.target_text
    return result


async def _panel_tag_display(tag: str, db, local_glossary: dict) -> str:
    """单个标签的「面板显示值」，与 /all-tags 单来源译文模式完全一致。

    已是中文（且无假名）的标签优先查术语表修正，否则用翻译服务（术语表优先 + 谷歌兜底）。
    """
    from ..translation_service import translation_service as _ts
    if not _ts.glossary:
        await _ts.load(db)
    _chinese_count = sum(1 for _ch in tag if "\u4e00" <= _ch <= "\u9fff")
    _has_kana = any("\u3040" <= _ch <= "\u309f" or "\u30a0" <= _ch <= "\u30ff" for _ch in tag)
    if not _has_kana and len(tag) and _chinese_count / len(tag) > 0.3:
        return local_glossary.get(tag, tag)
    return await _ts.translate(tag, "tag", db)


async def _display_tags_of(game: Game, source: str, original: bool, db, local_glossary: dict) -> set:
    """某游戏在指定来源下、按面板口径会显示的标签集合（供单来源标签筛选使用）。"""
    if source == "all":
        return _raw_tags_of(game) if original else {t.strip() for t in str(game.tags or "").split(",") if t.strip()}
    try:
        data = json.loads(game.source_data or "{}")
    except Exception:
        data = {}
    if not isinstance(data, dict):
        return set()
    block = data.get(source, {})
    if not isinstance(block, dict):
        return set()
    raw = {t.strip() for t in str(block.get("tags", "")).split(",") if t.strip()}
    if original:
        return raw
    return {await _panel_tag_display(t, db, local_glossary) for t in raw}


@router.get("/all-tags")
async def all_tags(source: str = "all", original: bool = False, page: int = 1, size: int = 25, db: AsyncSession = Depends(get_db)):
    counts: dict[str, int] = {}
    # 术语表一次性加载，供 _display_tags_of 复用
    _local_glossary: dict[str, str] = await _load_tag_glossary(db)
    # 统一按「游戏 → 该游戏显示的标签集合」聚合：同一游戏对同一显示标签**只计一次**。
    # 这样面板 count 恰好等于点该标签筛出的游戏数（与 list_games 同口径），
    # 避免「同游戏多来源同名词 / 多原文标签译成同一中文」被重复计数。
    for game in await db.scalars(select(Game)):
        for tag in await _display_tags_of(game, source, original, db, _local_glossary):
            counts[tag] = counts.get(tag, 0) + 1

    all_items = [{"tag": tag, "count": count} for tag, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]
    total = len(all_items)
    start = (page - 1) * size
    end = start + size
    return {"items": all_items[start:end], "total": total, "page": page, "size": size}


@router.post("", response_model=GameOut)
async def create_game(payload: GameCreate, db: AsyncSession = Depends(get_db)):
    game = Game(**payload.model_dump())
    db.add(game)
    await db.commit()
    await db.refresh(game)
    return game


@router.post("/from-source", response_model=GameOut)
async def create_game_from_source(
    source_type: str = Body(...),
    source_id: str = Body(""),
    title: str = Body(""),
    resource_type: str = Body("none"),
    resource_url: str = Body(""),
    db: AsyncSession = Depends(get_db),
):
    """从数据源创建游戏：复用元数据获取流程，自动翻译并补充多来源数据。"""
    from ..translation_service import TranslationService
    import json as _json

    # 1. 获取元数据详情
    metadata = {}
    if source_type == "rawg":
        if not source_id:
            raise HTTPException(400, "rawg 来源需要 source_id")
        metadata = await RawgClient().get_game_detail(source_id)
    elif source_type == "steam":
        if not source_id:
            raise HTTPException(400, "steam 来源需要 source_id")
        metadata = await get_steam_detail(source_id)
    elif source_type == "vndb":
        if not source_id:
            raise HTTPException(400, "vndb 来源需要 source_id")
        metadata = await get_vndb_detail(source_id)
    elif source_type == "dlsite":
        if not source_id:
            raise HTTPException(400, "dlsite 来源需要 source_id")
        metadata = await get_dlsite_detail(source_id)
    elif source_type == "custom":
        metadata = {"title": title or "未命名游戏", "source_type": "custom", "source_id": ""}
    else:
        raise HTTPException(400, f"不支持的来源类型：{source_type}")

    # 2. 自定义标题覆盖
    if title:
        metadata["title"] = title

    # 3. 翻译元数据
    config = await db.get(SystemConfig, 1)
    if not config:
        config = SystemConfig(id=1)
    raw_metadata = dict(metadata)  # 翻译前快照：source_data 统一存原文
    try:
        ts = TranslationService()
        metadata = await ts.translate_metadata(db, metadata)
    except Exception as e:
        logger.warning("新增游戏翻译失败：%s", e)

    # 4. 存入 source_data/source_ids（存翻译前原文）
    source_data = {}
    source_ids = {}
    if source_type != "custom" and source_id:
        source_data[source_type] = {k: v for k, v in raw_metadata.items() if k not in ("resource_type", "resource_url", "play_status", "original_data")}
        source_ids[source_type] = source_id

    # 5. 创建游戏
    # v1.7.0：title 为原始名，title_cn 为翻译产出的中文译名
    game = Game(
        title=metadata.get("title", ""),
        title_cn=metadata.get("title_cn", ""),
        alias=metadata.get("alias", ""),
        cover_url=metadata.get("cover_url", ""),
        cover_source=source_type,
        steam_appid=metadata.get("steam_appid", ""),
        screenshots=metadata.get("screenshots", "[]"),
        description=metadata.get("description", ""),
        developer=metadata.get("developer", ""),
        publisher=metadata.get("publisher", ""),
        release_date=normalize_release_date(metadata.get("release_date")),
        rating=metadata.get("rating"),
        tags=metadata.get("tags", ""),
        tag_source=source_type,
        series=metadata.get("series", ""),
        version=metadata.get("version", ""),
        source_type=source_type,
        source_id=source_id,
        source_ids=_json.dumps(source_ids, ensure_ascii=False),
        source_data=_json.dumps(source_data, ensure_ascii=False, default=str),
        original_data=metadata.get("original_data", "{}"),
        resource_type=resource_type,
        resource_url=resource_url,
        play_status="favorite",
    )
    db.add(game)
    await db.commit()
    await db.refresh(game)

    # 6. 自动补充多来源数据（后台执行，不阻塞响应）
    try:
        await refresh_game_metadata(game.id)
        await db.refresh(game)
    except Exception as e:
        logger.warning("新增游戏多来源补充失败 game_id=%s: %s", game.id, e)

    return game


@router.get("/{game_id}", response_model=GameOut)
async def get_game(game_id: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    return game


@router.put("/{game_id}", response_model=GameOut)
async def update_game(game_id: int, payload: GameUpdate, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    for key, value in payload.model_dump().items():
        setattr(game, key, value)
    await db.commit()
    await db.refresh(game)
    return game


@router.delete("/{game_id}", status_code=204)
async def delete_game(game_id: int, delete_files: bool = False, db: AsyncSession = Depends(get_db)):
    """删除游戏记录；delete_files=true 时同时删除 NAS 上的源文件/文件夹。

    安全约束：仅允许删除位于扫描根目录（scan_root / local_game_root）之内的路径，
    防止构造恶意 resource_url 误删系统文件。
    """
    import shutil as _shutil

    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    target = None
    if delete_files and game.resource_type in {"nas_cloud", "nas_local"} and game.resource_url:
        root = settings.scan_root if game.resource_type == "nas_cloud" else settings.local_game_root
        path = Path(game.resource_url).resolve()
        try:
            path.relative_to(root.resolve())
        except ValueError as exc:
            raise HTTPException(400, "资源路径不在允许的挂载目录内，已取消删除源文件") from exc
        # 防呆：不允许直接删除扫描根目录本身
        if path == root.resolve():
            raise HTTPException(400, "不允许删除扫描根目录")
        target = path
    await db.delete(game)
    await db.commit()
    if target is not None and target.exists():
        if target.is_dir():
            await __import__("asyncio").to_thread(_shutil.rmtree, target)
        else:
            await __import__("asyncio").to_thread(target.unlink)


@router.post("/{game_id}/cover", response_model=GameOut)
async def upload_cover(game_id: int, file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if (file.content_type or "") not in {"image/jpeg", "image/png"}:
        raise HTTPException(415, "封面仅支持 JPG 或 PNG 格式")
    cover_dir = settings.data_dir / "covers"
    cover_dir.mkdir(parents=True, exist_ok=True)
    path = cover_dir / f"{game_id}_custom.jpg"
    path.write_bytes(await file.read())
    game.cover_url = f"/data/covers/{path.name}"
    game.cover_source = "custom"
    await db.commit()
    await db.refresh(game)
    return game


@router.post("/{game_id}/cover-source", response_model=GameOut)
async def change_cover_source(
    game_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        payload = await request.json()
        source = payload.get("source")
        file = None
    else:
        form = await request.form()
        source = form.get("source")
        file = form.get("file")
    allowed = {"auto", "steam", "vndb", "dlsite", "rawg", "custom"}
    if source not in allowed:
        raise HTTPException(422, "无效的封面来源")

    try:
        metadata = json.loads(game.original_data or "{}")
        if not isinstance(metadata, dict):
            metadata = {}
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    urls = metadata.get("cover_urls", {})
    if not isinstance(urls, dict):
        urls = {}
    existing_source = game.cover_source or game.source_type
    if game.cover_url.startswith(("http://", "https://")) and existing_source != "custom":
        urls.setdefault(existing_source, game.cover_url)

    if source == "auto":
        config = await db.get(SystemConfig, 1)
        try:
            priority = json.loads(config.cover_source_priority if config else "[]")
        except (TypeError, json.JSONDecodeError):
            priority = []
        source = next((item for item in priority if item in allowed - {"auto", "custom"} and (
            (item == "steam" and bool(game.steam_appid)) or
            (item in urls and bool(urls[item])) or
            (item == game.source_type and bool(game.cover_url))
        )), "")
        if not source:
            raise HTTPException(422, "没有可用的封面来源")

    if source == "custom":
        if not file or (file.content_type or "") not in {"image/jpeg", "image/png"}:
            raise HTTPException(415, "自定义封面仅支持 JPG 或 PNG 格式")
        cover_dir = settings.data_dir / "covers"
        cover_dir.mkdir(parents=True, exist_ok=True)
        path = cover_dir / f"{game_id}_custom.jpg"
        path.write_bytes(await file.read())
        cover_url = f"/data/covers/{path.name}"
    else:
        metadata["cover_urls"] = urls
        game.original_data = json.dumps(metadata, ensure_ascii=False)
        # 优先从 source_data[source].cover_url 取
        try:
            _sd = json.loads(game.source_data or "{}")
        except json.JSONDecodeError:
            _sd = {}
        cover_url = ""
        if source in _sd and _sd[source].get("cover_url"):
            cover_url = _sd[source]["cover_url"]
        if not cover_url:
            cover_url = urls.get(source, "")
        if source == "steam" and game.steam_appid:
            cover_url = RawgClient.steam_cover_url(game.steam_appid)
        elif source == existing_source and game.cover_url and not game.cover_url.startswith("/data/covers/"):
            cover_url = game.cover_url
        if not cover_url:
            raise HTTPException(422, f"没有可用的 {source} 封面")
        try:
            cover_url = await cache_cover(game.id, cover_url, source)
        except Exception as exc:
            raise HTTPException(502, f"封面下载失败：{exc}") from exc
    game.cover_url = cover_url
    game.cover_source = source
    await db.commit()
    await db.refresh(game)
    return game


@router.get("/{game_id}/tags/resolve", response_model=GameOut)
async def resolve_game_tags(game_id: int, source: str = "", db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(status_code=404, detail="游戏不存在")
    config = await db.get(SystemConfig, 1)
    if not config:
        config = SystemConfig(id=1)
    await resolve_tags(game, config, source, db)
    await db.commit()
    await db.refresh(game)
    return game


@router.get("/{game_id}/screenshots/fetch", response_model=GameOut)
async def fetch_screenshots(game_id: int, source: str = "", db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    try:
        existing = json.loads(game.screenshots or "[]")
    except json.JSONDecodeError:
        existing = []
    # 已有 URL 但全是远程 URL 时，仍执行缓存到本地。
    has_local = any(url.startswith("/data/") for url in existing)
    if existing and not source and has_local:
        return game
    config = await db.get(SystemConfig, 1)
    if not config:
        config = SystemConfig(id=1)
        db.add(config)
        await db.flush()
    await fetch_game_screenshots(game, config, source)
    await db.commit()
    await db.refresh(game)
    return game


@router.post("/{game_id}/screenshots", response_model=GameOut)
async def upload_screenshot(game_id: int, files: list[UploadFile] = File(...), db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if any((file.content_type or "") not in {"image/jpeg", "image/png"} for file in files):
        raise HTTPException(415, "截图仅支持 JPG 或 PNG 格式")
    directory = settings.data_dir / "screenshots"
    directory.mkdir(parents=True, exist_ok=True)
    try:
        values = json.loads(game.screenshots or "[]")
    except json.JSONDecodeError:
        values = []
    for file in files:
        suffix = ".png" if file.content_type == "image/png" else ".jpg"
        path = directory / f"{game_id}_custom_{len(values)}{suffix}"
        path.write_bytes(await file.read())
        values.append(f"/data/screenshots/{path.name}")
    game.screenshots = json.dumps(values)
    game.screenshot_source = "custom"
    await db.commit()
    await db.refresh(game)
    return game


@router.delete("/{game_id}/screenshots/{index}", response_model=GameOut)
async def delete_screenshot(game_id: int, index: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    try:
        values = json.loads(game.screenshots or "[]")
    except json.JSONDecodeError:
        values = []
    if index < 0 or index >= len(values):
        raise HTTPException(404, "截图不存在")
    url = values.pop(index)
    if url.startswith("/data/screenshots/"):
        path = (settings.data_dir / "screenshots" / Path(url).name).resolve()
        if path.parent == (settings.data_dir / "screenshots").resolve() and path.exists():
            path.unlink()
    game.screenshots = json.dumps(values)
    if not values:
        game.screenshot_source = ""
    await db.commit()
    await db.refresh(game)
    return game


@router.get("/{game_id}/match-candidates")
async def match_candidates(game_id: int, db: AsyncSession = Depends(get_db)):
    """手动匹配：返回各数据源的候选元数据（含相似度），供用户挑选。"""
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    try:
        return await find_match_candidates(game_id)
    except Exception as exc:
        raise HTTPException(502, f"候选搜索失败：{exc}") from exc


@router.post("/{game_id}/apply-match", response_model=GameOut)
async def apply_match_route(game_id: int, payload: MatchApplyPayload, db: AsyncSession = Depends(get_db)):
    """应用手动匹配结果：把选定来源的元数据写入当前游戏。"""
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    try:
        await apply_match(game_id, payload.source_type, payload.source_id, payload.set_primary)
    except Exception as exc:
        raise HTTPException(502, f"应用匹配失败：{exc}") from exc
    db.expire_all()
    fresh = await db.get(Game, game_id)
    return fresh


@router.post("/{game_id}/retranslate", response_model=GameOut)
async def retranslate_route(game_id: int, payload: RetranslatePayload | None = None, db: AsyncSession = Depends(get_db)):
    """重新翻译：用库内原文快照重走术语表/翻译 API，不联网刮削（改术语表后秒级生效）。"""
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    force = bool(payload.force) if payload else False
    try:
        await retranslate_game(game_id, force=force)
    except Exception as exc:
        raise HTTPException(502, f"重新翻译失败：{exc}") from exc
    db.expire_all()
    fresh = await db.get(Game, game_id)
    return fresh


@router.post("/{game_id}/refresh-metadata", response_model=RefreshMetadataTaskOut, status_code=202)
async def refresh_metadata(game_id: int, db: AsyncSession = Depends(get_db)):
    game = await db.get(Game, game_id)
    if not game:
        raise HTTPException(404, "游戏不存在")
    if game.source_type == "custom":
        # custom 游戏按标题搜索 RAWG（优先）和 VNDB，取第一个匹配结果。
        from ..services import search_vndb

        matched = None
        try:
            rawg_results = await RawgClient().search_games(game.title, page_size=1)
            if rawg_results:
                matched = {"source_type": "rawg", "source_id": rawg_results[0]["source_id"]}
        except Exception:
            pass
        if not matched:
            # Steam fallback：中文名搜索支持好
            try:
                import re as _re
                steam_results = await search_steam(game.title)
                steam_results = [
                    r for r in steam_results
                    if not _re.search(r"(?i)(soundtrack|ost|original soundtrack|dlc|demo|trial)", r.get("title", ""))
                ]
                if steam_results:
                    matched = {"source_type": "steam", "source_id": steam_results[0]["source_id"]}
            except Exception:
                pass
        if not matched:
            try:
                vndb_results = await search_vndb(game.title)
                if vndb_results:
                    matched = {"source_type": "vndb", "source_id": vndb_results[0]["source_id"]}
            except Exception:
                pass
        if not matched:
            # DLsite fallback：优先从标题提取 RJ/VJ/BJ 编号精准查询
            try:
                from ..clients.dlsite_client import DlsiteClient
                dlsite = DlsiteClient()
                workno = dlsite.extract_workno(game.title)
                if workno and await dlsite.get_work_by_id(workno):
                    matched = {"source_type": "dlsite", "source_id": workno}
                else:
                    dlsite_results = await dlsite.search_games(game.title, page_size=1)
                    if dlsite_results:
                        matched = {"source_type": "dlsite", "source_id": dlsite_results[0]["source_id"]}
            except Exception:
                pass
        if not matched:
            raise HTTPException(404, "未在 RAWG/Steam/VNDB/DLsite 找到匹配的游戏，请手动编辑")
        game.source_type = matched["source_type"]
        game.source_id = matched["source_id"]
        await db.commit()
    elif game.source_type not in {"rawg", "steam", "vndb", "dlsite"} or not game.source_id:
        raise HTTPException(422, "仅支持刷新具有 RAWG/Steam/VNDB/DLsite 数据源 ID 的游戏")
    last_refresh = task_manager.game_refreshes.get(game_id)
    if last_refresh and datetime.utcnow() - last_refresh < timedelta(seconds=60):
        raise HTTPException(429, "请在 60 秒后再次刷新元数据")
    task_manager.game_refreshes[game_id] = datetime.utcnow()
    task = task_manager.create(f"等待刷新 {game.source_type.upper()} 元数据")
    task["result_game_id"] = game_id

    if game.source_type == "rawg":
        refresh_task = refresh_rawg_game_metadata(game_id)
    elif game.source_type == "vndb":
        refresh_task = refresh_vndb_game_metadata(game_id)
    else:
        # steam / dlsite 用通用 refresh_game_metadata（已在 services.py 中支持）
        refresh_task = refresh_game_metadata(game_id)
    task_manager.run(task, refresh_task)
    return task
