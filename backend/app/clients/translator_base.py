from abc import ABC, abstractmethod


class TranslatorBase(ABC):
    @abstractmethod
    async def translate(self, text: str, source: str = "en", target: str = "zh") -> str:
        """翻译单段文本，失败时返回原文不抛异常"""

    @abstractmethod
    async def batch_translate(self, texts: list[str], source: str = "en", target: str = "zh") -> list[str]:
        """批量翻译，保持顺序，单条失败返回原文"""