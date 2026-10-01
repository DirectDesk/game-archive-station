from sqlalchemy import select
import json
import re
from sqlalchemy.ext.asyncio import AsyncSession

from .clients.translator import get_translator
from .models import SystemConfig, TranslationGlossary


class TranslationService:
    def __init__(self):
        self.glossary: dict[tuple[str, str], str] = {}
        self.translator = get_translator("none")

    async def load(self, db: AsyncSession) -> None:
        config = await db.get(SystemConfig, 1)
        self.translator = get_translator(
            config.translator_type if config else "none",
            secret_id=config.tencent_secret_id if config else "",
            secret_key=config.tencent_secret_key if config else "",
            region=config.tencent_region if config else "",
        )
        self.glossary = {
            (row.source_text, row.category or ""): row.target_text
            for row in await db.scalars(select(TranslationGlossary))
        }

    async def translate(self, text: str, category: str | None = None) -> str:
        if not text:
            return text
        if any("\u4e00" <= char <= "\u9fff" for char in text):
            return text
        translated = self.glossary.get((text, category or "")) or self.glossary.get((text, ""))
        if translated:
            return translated
        try:
            return await self.translator.translate(text)
        except Exception:
            return text

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
        original = {key: metadata.get(key, "") for key in ("title", "alias", "description", "developer", "publisher", "tags", "series")}
        metadata["original_data"] = json.dumps(original, ensure_ascii=False)
        title = metadata.get("title", "")
        version_match = re.search(r"(?i)(?:^|[ ._-])(v\d+(?:\.\d+){1,3})(?:$|[ ._-])", title)
        metadata.setdefault("version", version_match.group(1) if version_match else "")
        # 明确游戏本地化名称优先，避免机器翻译遗漏标点/语义。
        title_fixes = {"AI*Shoujo": "AI*少女", "AI＊Shoujo": "AI＊少女", "Magical Girl Witch Trial": "魔法少女的魔女审判"}
        normalized = re.sub(r"[._]+", " ", re.sub(r"(?i)\bv\d+(?:\.\d+){1,3}\b", "", title)).strip()
        if title in title_fixes or normalized.casefold() in {key.casefold() for key in title_fixes}:
            metadata["title"] = next(value for key, value in title_fixes.items() if key.casefold() in {title.casefold(), normalized.casefold()})
        config = await db.get(SystemConfig, 1)
        if not config or not config.auto_translate:
            return metadata
        await self.load(db)
        original_title = title
        translated = await self.translate_fields(metadata, {"title": "game_title", "description": "", "tags": "tag"})
        if title in title_fixes or normalized.casefold() in {key.casefold() for key in title_fixes}:
            translated["title"] = metadata["title"]
        if original_title and not any("\u4e00" <= char <= "\u9fff" for char in original_title) and not translated.get("alias"):
            translated["alias"] = original_title
        if original_title and not any("\u4e00" <= char <= "\u9fff" for char in original_title) and translated.get("title") == original_title:
            translated["title"] = original_title
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
        return await self.list_items(db, size=max(len(items), 1))


translation_service = TranslationService()