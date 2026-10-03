from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..schemas import GlossaryCreate, GlossaryOut, GlossaryUpdate
from ..translation_service import translation_service
from ..config import settings
import json as _json
from pathlib import Path

router = APIRouter(prefix="/api/glossary", tags=["glossary"])


@router.get("", response_model=list[GlossaryOut])
async def list_glossary(category: str = "", q: str = "", page: int = Query(1, ge=1), db: AsyncSession = Depends(get_db)):
    return await translation_service.list_items(db, category, q, page)


@router.post("", response_model=GlossaryOut)
async def add_glossary(payload: GlossaryCreate, db: AsyncSession = Depends(get_db)):
    try:
        return await translation_service.add(db, payload.model_dump())
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(409, "原文术语已存在") from exc


@router.put("/{item_id}", response_model=GlossaryOut)
async def update_glossary(item_id: int, payload: GlossaryUpdate, db: AsyncSession = Depends(get_db)):
    try:
        item = await translation_service.update(db, item_id, payload.model_dump())
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(409, "原文术语已存在") from exc
    if not item:
        raise HTTPException(404, "术语不存在")
    return item


@router.delete("/{item_id}", status_code=204)
async def delete_glossary(item_id: int, db: AsyncSession = Depends(get_db)):
    if not await translation_service.delete(db, item_id):
        raise HTTPException(404, "术语不存在")


@router.post("/batch", response_model=list[GlossaryOut])
async def batch_add_glossary(payload: list[GlossaryCreate], db: AsyncSession = Depends(get_db)):
    try:
        return await translation_service.batch_add(db, [item.model_dump() for item in payload])
    except IntegrityError as exc:
        await db.rollback()
        raise HTTPException(409, "批量术语导入失败") from exc

# === JSON 标签术语表接口 ===
@router.get("/tags")
async def list_tag_glossary(q: str = ""):
    """读取 JSON 标签术语表"""
    glossary_path = settings.data_dir / "tag_glossary.json"
    if not glossary_path.exists():
        return {}
    try:
        with open(glossary_path, 'r', encoding='utf-8') as f:
            data = _json.load(f)
        if q:
            data = {k: v for k, v in data.items() if q in k or q in v}
        return data
    except Exception as e:
        raise HTTPException(500, f"读取标签术语表失败：{e}")


@router.post("/tags")
async def save_tag_glossary(payload: dict):
    """全量保存 JSON 标签术语表"""
    glossary_path = settings.data_dir / "tag_glossary.json"
    try:
        glossary_path.parent.mkdir(parents=True, exist_ok=True)
        with open(glossary_path, 'w', encoding='utf-8') as f:
            _json.dump(payload, f, ensure_ascii=False, indent=2)
        # 刷新内存中的术语表
        if hasattr(translation_service, '_load_tag_glossary'):
            translation_service._load_tag_glossary()
        return {"status": "ok", "count": len(payload)}
    except Exception as e:
        raise HTTPException(500, f"保存标签术语表失败：{e}")


@router.put("/tags/{source_text}")
async def update_tag_glossary_item(source_text: str, payload: dict):
    """更新单个标签术语"""
    glossary_path = settings.data_dir / "tag_glossary.json"
    try:
        data = {}
        if glossary_path.exists():
            with open(glossary_path, 'r', encoding='utf-8') as f:
                data = _json.load(f)
        data[source_text] = payload.get("target_text", "")
        with open(glossary_path, 'w', encoding='utf-8') as f:
            _json.dump(data, f, ensure_ascii=False, indent=2)
        if hasattr(translation_service, '_load_tag_glossary'):
            translation_service._load_tag_glossary()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, f"更新标签术语失败：{e}")


@router.delete("/tags/{source_text}")
async def delete_tag_glossary_item(source_text: str):
    """删除单个标签术语"""
    glossary_path = settings.data_dir / "tag_glossary.json"
    try:
        if glossary_path.exists():
            with open(glossary_path, 'r', encoding='utf-8') as f:
                data = _json.load(f)
            if source_text in data:
                del data[source_text]
                with open(glossary_path, 'w', encoding='utf-8') as f:
                    _json.dump(data, f, ensure_ascii=False, indent=2)
                if hasattr(translation_service, '_load_tag_glossary'):
                    translation_service._load_tag_glossary()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, f"删除标签术语失败：{e}")
