from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..schemas import GlossaryCreate, GlossaryOut, GlossaryUpdate
from ..translation_service import translation_service

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