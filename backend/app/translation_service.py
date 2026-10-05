from sqlalchemy import select
import json
import re
from sqlalchemy.ext.asyncio import AsyncSession
import logging
import httpx

from .clients.translator import get_translator
from .models import SystemConfig, TranslationGlossary

logger = logging.getLogger(__name__)


def _has_han(text: str) -> bool:
    """是否含汉字（CJK 统一表意文字）。

    用于判定「翻译结果是否真的译成了中文」——翻译器对不认识的名字
    常原样返回，此时不应把原名写进 title_cn（否则 title_cn == title，
    展示层 "title_cn or title" 虽不出错，但语义冗余）。
    """
    return any("\u4e00" <= c <= "\u9fff" for c in (text or ""))


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

    @staticmethod
    def _looks_garbled(source: str, result: str) -> bool:
        """腾讯翻译君对日文常返回乱码（"Ÿ ðŸ"等 Latin-1 扩展字符）。
        源含日文假名且结果含 >=2 个 Latin 扩展字符时判定为乱码。
        """
        if not result or result == source:
            return False
        has_kana = any("\u3040" <= c <= "\u30ff" for c in source)
        if not has_kana:
            return False
        latin_ext = sum(1 for c in result if "\u0080" <= c <= "\u024f")
        return latin_ext >= 2

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
            # 先查数据库术语表：分类专用 > 通用(general) > 无分类
            cached = (
                self.glossary.get((text, "tag"))
                or self.glossary.get((text, "general"))
                or self.glossary.get((text, ""))
            )
            if cached:
                return cached
            # 含中文（含中英混合如"Steam 云"）且无日文假名的标签不再机翻：
            # 谷歌接口按 sl=ja 处理，会把官方中文二次翻成"蒸汽云/中国人"等垃圾结果
            _cn = sum(1 for _ch in text if "\u4e00" <= _ch <= "\u9fff")
            _kana = any("\u3040" <= _ch <= "\u309f" or "\u30a0" <= _ch <= "\u30ff" for _ch in text)
            if _cn > 0 and not _kana:
                return text
            # 谷歌翻译兜底
            translated = await self._google_translate(text)
            if translated and translated != text and db is not None:
                # 自动学习写入「通用(general)」术语：同一词条可同时作用于标签、游戏名、简介，
                # 用户可在术语表中把它改成具体分类（tag/title/description）以限制作用范围。
                try:
                    from .models import TranslationGlossary
                    db.add(TranslationGlossary(source_text=text, target_text=translated, category="general"))
                    await db.commit()
                    self.glossary[(text, "general")] = translated
                    logger.info("标签谷歌翻译并加入通用术语表：%s -> %s", text, translated)
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
        translated = (
            self.glossary.get((text, category or ""))
            or self.glossary.get((text, "general"))
            or self.glossary.get((text, ""))
        )
        if translated:
            logger.info("术语表命中：%s -> %s", text, translated)
            return translated
        # 通用(general)术语的"词内替换"：整串未命中时，把 general 词条作为子串替换。
        # 例如术语 Shoujo->少女 应作用于标题 "AI*Shoujo"；标签 Slam->大满贯 应作用于 "Slam Dunk"。
        _partial = text
        _hit = False
        for (src_text, cat), tgt in self.glossary.items():
            if cat == "general" and src_text and tgt and src_text != tgt and src_text in _partial:
                _partial = _partial.replace(src_text, tgt)
                _hit = True
        if _hit and _partial != text:
            logger.info("通用术语部分替换：%s -> %s", text, _partial)
            return _partial
        try:
            result = await self.translator.translate(text)
            # 腾讯翻译君对日文乱码检测：回退谷歌翻译
            if self._looks_garbled(text, result):
                logger.info("翻译结果疑似乱码，回退谷歌：%s -> %.40s", text, result)
                result = await self._google_translate(text)
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
                # title 分类自动写入术语表（游戏名可能重复，缓存有意义）
                # description 不缓存：太长且通常唯一，占用空间大
                if category == "title" and db is not None and len(text) < 200:
                    try:
                        from .models import TranslationGlossary
                        db.add(TranslationGlossary(source_text=text, target_text=result, category="title"))
                        await db.commit()
                        self.glossary[(text, "title")] = result
                        logger.info("游戏名翻译并加入术语表：%s -> %s", text, result)
                    except Exception as e:
                        logger.warning("游戏名术语写入失败：%s -> %s", text, e)
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
        """翻译元数据。

        v1.7.0 语义变更（重要）：
          * ``title`` 不再被翻译结果覆盖——它保留**原始名**（外文/日文原名）。
          * 游戏名的翻译结果写入 ``title_cn``（中文译名）。
          * 展示层统一用 ``display_title = title_cn or title``。

        拆开之后：① 用户改中文名改的是 title_cn，原文 title 岿然不动，
        不会再出现「改名被原文打回」；② 术语表变更可放心重翻译，
        不必再靠 keep_user_title 全局禁用 title 同步。
        """
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
        explicit_cn = title_fixes.get(raw_title, "")
        # 简介/标签照常翻译；游戏名单独处理（走 title_cn）
        translated = await self.translate_fields(metadata, {"description": "description", "tags": "tag"})
        cn_title = explicit_cn
        if not cn_title and raw_title:
            cn_title = await self.translate(raw_title, "title", db)
        # 只在确实译出「不同的中文名」时才写；原样返回则不写，
        # 避免 title_cn 与 title 内容重复。
        if cn_title and cn_title != raw_title and _has_han(cn_title):
            translated["title_cn"] = cn_title
        return translated

    async def translate_rawg_metadata(self, db: AsyncSession, metadata: dict) -> dict:
        """RAWG 元数据翻译。v1.7.0 起 title 保留原始名，译名进 title_cn。

        旧逻辑里有一段「把原文名回填 alias、并在 title 被翻译时复原 title」的
        补救代码——那是在绕开「title 被翻译覆盖」的副作用。
        现在 title 根本不会被覆盖，原文名天然就在 title 里，补救已无意义，
        故整段删除（原代码见 git 历史 v1.6.x）。
        """
        metadata = await self.translate_metadata(db, metadata)
        title = metadata.get("title", "")
        version_match = re.search(r"(?i)(?<![a-z0-9])v\d+(?:\.\d+){1,3}(?![a-z0-9])", title)
        metadata.setdefault("version", version_match.group(0) if version_match else "")
        # 明确游戏本地化名称优先于机器翻译（这类名字机器翻译会漏标点/语义）。
        # 注意：写入 title_cn，title 仍是来源原名。
        title_fixes = {"AI*Shoujo": "AI*少女", "AI＊Shoujo": "AI＊少女", "Magical Girl Witch Trial": "魔法少女的魔女审判"}
        normalized = re.sub(r"[._]+", " ", re.sub(r"(?i)\bv\d+(?:\.\d+){1,3}\b", "", title)).strip()
        if title in title_fixes or normalized.casefold() in {key.casefold() for key in title_fixes}:
            metadata["title_cn"] = next(value for key, value in title_fixes.items() if key.casefold() in {title.casefold(), normalized.casefold()})
        elif not metadata.get("title_cn") and normalized and normalized != title:
            # 原名带版本号/分隔符时，用清洗后的名字再试一次翻译
            cn = await self.translate(normalized, "title", db)
            if cn and cn != normalized and _has_han(cn):
                metadata["title_cn"] = cn
        return metadata

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