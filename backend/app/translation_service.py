from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .clients.translator import get_translator
from .models import SystemConfig, TranslationGlossary


class TranslationService:
    def __init__(self):
        self.glossary: dict[str, str] = {}
        self.translator = get_translator("none")

    async def load(self, db: AsyncSession) -> None:
        config = await db.get(SystemConfig, 1)
        self.translator = get_translator(
            config.translator_type if config else "none",
            secret_id=config.tencent_secret_id if config else "",
            secret_key=config.tencent_secret_key if config else "",
            region=config.tencent_region if config else "",
        )
        self.glossary = {row.source_text: row.target_text for row in await db.scalars(select(TranslationGlossary))}

    async def translate(self, text: str, category: str | None = None) -> str:
        if not text:
            return text
        return self.glossary[text] if text in self.glossary else await self.translator.translate(text)

    async def translate_fields(self, data: dict, field_map: dict) -> dict:
        result = data.copy()
        for field, category in field_map.items():
            value = result.get(field)
            if not value:
                continue
            if field == "tags":
                parts = [part.strip() for part in value.split(",")]
                result[field] = ", ".join([await self.translate(part, category or None) for part in parts])
            else:
                result[field] = await self.translate(value, category or None)
        return result

    async def translate_rawg_metadata(self, db: AsyncSession, metadata: dict) -> dict:
        config = await db.get(SystemConfig, 1)
        if not config or not config.auto_translate:
            return metadata
        await self.load(db)
        original_title = metadata.get("title", "")
        translated = await self.translate_fields(metadata, {"title": "game_title", "description": "", "tags": "tag", "developer": "developer", "publisher": "developer"})
        if original_title and not translated.get("alias"):
            translated["alias"] = original_title
        return translated

    async def list_items(self, db: AsyncSession, category: str = "", q: str = "", page: int = 1, size: int = 50) -> list[TranslationGlossary]:
        query = select(TranslationGlossary)
        if category:
            query = query.where(TranslationGlossary.category == category)
        if q:
            query = query.where(TranslationGlossary.source_text.ilike(f"%{q}%"))
        return list((await db.scalars(query.order_by(TranslationGlossary.id).offset((page - 1) * size).limit(size))).all())

    async def add(self, db: AsyncSession, data: dict) -> TranslationGlossary:
        item = TranslationGlossary(**data)
        db.add(item)
        await db.commit()
        await db.refresh(item)
        await self.load(db)
        return item

    async def update(self, db: AsyncSession, item_id: int, data: dict) -> TranslationGlossary | None:
        item = await db.get(TranslationGlossary, item_id)
        if not item:
            return None
        for key, value in data.items():
            setattr(item, key, value)
        await db.commit()
        await db.refresh(item)
        await self.load(db)
        return item

    async def delete(self, db: AsyncSession, item_id: int) -> bool:
        item = await db.get(TranslationGlossary, item_id)
        if not item:
            return False
        await db.delete(item)
        await db.commit()
        await self.load(db)
        return True

    async def batch_add(self, db: AsyncSession, items: list[dict]) -> list[TranslationGlossary]:
        for data in items:
            existing = await db.scalar(select(TranslationGlossary).where(TranslationGlossary.source_text == data["source_text"]))
            if existing:
                for key, value in data.items():
                    setattr(existing, key, value)
            else:
                db.add(TranslationGlossary(**data))
        await db.commit()
        await self.load(db)
        return await self.list(db, size=max(len(items), 1))


translation_service = TranslationService()