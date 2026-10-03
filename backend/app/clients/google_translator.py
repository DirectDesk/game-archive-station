import asyncio
import logging

import httpx

from .translator_base import TranslatorBase

logger = logging.getLogger(__name__)


class GoogleTranslator(TranslatorBase):
    """谷歌翻译（非官方免费接口，带重试退避，走系统代理）"""

    async def translate(self, text: str, source: str = "auto", target: str = "zh-CN") -> str:
        if not text:
            return text
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(
                        "https://translate.googleapis.com/translate_a/single",
                        params={"client": "gtx", "sl": source, "tl": target, "dt": "t", "q": text}
                    )
                    if resp.status_code == 429:
                        wait = 2 ** (attempt + 1)
                        logger.info("谷歌翻译429限流，%.1f秒后重试(%d/3)：%s", wait, attempt + 1, text[:50])
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    result = "".join(part[0] for part in data[0] if part[0])
                    return result.strip()
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
                    continue
                logger.warning("谷歌翻译失败：%s -> %s", text[:50], e)
                return text
        return text

    async def batch_translate(self, texts: list[str], source: str = "auto", target: str = "zh-CN") -> list[str]:
        return [await self.translate(t, source, target) for t in texts]
