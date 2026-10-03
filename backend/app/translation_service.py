from sqlalchemy import select
import json
import re
from sqlalchemy.ext.asyncio import AsyncSession
import logging
import httpx

from .clients.translator import get_translator
from .models import SystemConfig, TranslationGlossary

logger = logging.getLogger(__name__)


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
        pass

    async def _google_translate(self, text: str, retries: int = 3) -> str:
        """谷歌翻译（非官方免费接口，带重试退避）"""
        import os as _os
        import asyncio as _asyncio
        proxy = _os.environ.get("HTTP_PROXY") or _os.environ.get("http_proxy") or _os.environ.get("HTTPS_PROXY") or _os.environ.get("https_proxy")
        for attempt in range(retries):
            try:
                async with httpx.AsyncClient(timeout=10, proxy=proxy) as client:
                    resp = await client.get(
                        "https://translate.googleapis.com/translate_a/single",
                        params={"client": "gtx", "sl": "ja", "tl": "zh-CN", "dt": "t", "q": text}
                    )
                    if resp.status_code == 429:
                        wait = 2 ** (attempt + 1)
                        logger.info("谷歌翻译429限流，%.1f秒后重试(%d/%d)：%s", wait, attempt+1, retries, text)
                        await _asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    result = "".join(part[0] for part in data[0] if part[0])
                    return result.strip()
            except Exception as e:
                if attempt < retries - 1:
                    await _asyncio.sleep(1)
                    continue
                logger.warning("谷歌翻译失败：%s -> %s", text, e)
                return text
        return text

    async def translate(self, text: str, category: str | None = None, db=None) -> str:
        if not text:
            return text
        # 标签(category="tag")：优先用内置术语表，没有的保留原文（腾讯翻译君对galgame标签翻译质量差）
        if category == "tag":
            # 先查数据库术语表（category=tag）
            cached = self.glossary.get((text, "tag")) or self.glossary.get((text, ""))
            if cached:
                return cached
            # 谷歌翻译兜底
            translated = await self._google_translate(text)
            if translated and translated != text and db is not None:
                # 自动写入数据库术语表
                try:
                    from .models import TranslationGlossary
                    db.add(TranslationGlossary(source_text=text, target_text=translated, category="tag"))
                    await db.commit()
                    self.glossary[(text, "tag")] = translated
                    logger.info("标签谷歌翻译并加入术语表：%s -> %s", text, translated)
                except Exception as e:
                    logger.warning("标签术语写入失败：%s -> %s", text, e)
            return translated or text
        if category != "tag":
            # 中文字符占比超过 30% 且不含日文假名，才认为是中文文本，跳过翻译；
            # 日文也使用汉字，需检测假名（平假名/片假名）区分；英文简介混入少量中文专有名词仍需翻译。
            chinese_count = sum(1 for char in text if "\u4e00" <= char <= "\u9fff")
            has_japanese_kana = any("\u3040" <= char <= "\u309f" or "\u30a0" <= char <= "\u30ff" for char in text)
            if not has_japanese_kana and chinese_count / len(text) > 0.3:
                return text
        translated = self.glossary.get((text, category or "")) or self.glossary.get((text, ""))
        if translated:
            logger.info("术语表命中：%s -> %s", text, translated)
            return translated
        try:
            result = await self.translator.translate(text)
            # 质量校验：标签翻译结果异常时保留原文
            if category == "tag" and result != text:
                # 1. 长度校验：翻译结果长度 < 原文50%，认为被截断
                if len(result) < len(text) * 0.5:
                    logger.info("标签翻译被截断，保留原文：%s -> %s", text, result)
                    return text
                # 2. 音译垃圾检测：翻译结果中出现连续的平假名/片假名（中文翻译不应有日文假名）
                import re as _re
                if _re.search(r'[\u3040-\u309f\u30a0-\u30ff]{2,}', result):
                    logger.info("标签翻译含日文假名（音译垃圾），保留原文：%s -> %s", text, result)
                    return text
            if result != text:
                logger.info("翻译成功（%s）：%s -> %s", category or "default", text, result)
            else:
                logger.info("翻译返回原文（%s）：%s", category or "default", text)
            return result
        except Exception as exc:
            logger.warning("翻译失败（%s）：%s -> %s", category or "default", text, exc, exc_info=True)
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
                if field == "description" and len(value) > 500:
                    chunks = [value[index:index + 500] for index in range(0, len(value), 500)]
                    result[field] = "".join([await self.translate(chunk, category or None) for chunk in chunks])
                else:
                    result[field] = await self.translate(value, category or None)
        return result

    async def translate_metadata(self, db: AsyncSession, metadata: dict) -> dict:
        original = {key: metadata.get(key, "") for key in ("title", "alias", "description", "developer", "publisher", "tags", "series", "screenshots", "steam_appid")}
        metadata["original_data"] = json.dumps(original, ensure_ascii=False)
        config = await db.get(SystemConfig, 1)
        if not config or not config.auto_translate:
            if config and config.translator_type != "none":
                logger.warning("自动翻译已关闭，但 translator_type=%s", config.translator_type)
            return metadata
        await self.load(db)
        # 已知游戏名的中文映射兜底（翻译君对日文游戏名常原样返回）
        title_fixes = {
            "魔法少女ノ魔女裁判": "魔法少女的魔女审判",
            "魔法少女の魔女裁判": "魔法少女的魔女审判",
            "魔法少女ノ魔女裁判": "魔法少女的魔女审判",
        }
        raw_title = metadata.get("title", "")
        if raw_title in title_fixes:
            metadata["title"] = title_fixes[raw_title]
        translated = await self.translate_fields(metadata, {"title": "game_title", "description": "", "tags": "tag"})
        return translated

    async def translate_rawg_metadata(self, db: AsyncSession, metadata: dict) -> dict:
        metadata = await self.translate_metadata(db, metadata)
        title = metadata.get("title", "")
        version_match = re.search(r"(?i)(?<![a-z0-9])v\d+(?:\.\d+){1,3}(?![a-z0-9])", title)
        metadata.setdefault("version", version_match.group(0) if version_match else "")
        # 明确游戏本地化名称优先，避免机器翻译遗漏标点/语义。
        title_fixes = {"AI*Shoujo": "AI*少女", "AI＊Shoujo": "AI＊少女", "Magical Girl Witch Trial": "魔法少女的魔女审判"}
        normalized = re.sub(r"[._]+", " ", re.sub(r"(?i)\bv\d+(?:\.\d+){1,3}\b", "", title)).strip()
        if title in title_fixes or normalized.casefold() in {key.casefold() for key in title_fixes}:
            metadata["title"] = next(value for key, value in title_fixes.items() if key.casefold() in {title.casefold(), normalized.casefold()})
        original_title = json.loads(metadata.get("original_data", "{}")).get("title", title)
        translated = metadata
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

    async def count_items(self, db: AsyncSession, category: str = "", q: str = "") -> int:
        from sqlalchemy import func as _func
        query = select(_func.count(TranslationGlossary.id))
        if category:
            query = query.where(TranslationGlossary.category == category)
        if q:
            query = query.where(TranslationGlossary.source_text.ilike(f"%{q}%"))
        return (await db.scalar(query)) or 0

    async def count_items(self, db: AsyncSession, category: str = "", q: str = "") -> int:
        from sqlalchemy import func as _func
        query = select(_func.count(TranslationGlossary.id))
        if category:
            query = query.where(TranslationGlossary.category == category)
        if q:
            query = query.where(TranslationGlossary.source_text.ilike(f"%{q}%"))
        return (await db.scalar(query)) or 0

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