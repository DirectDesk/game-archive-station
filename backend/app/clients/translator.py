from .tencent_translator import TencentTranslator
from .translator_base import TranslatorBase


class NullTranslator(TranslatorBase):
    async def translate(self, text: str, source: str = "en", target: str = "zh") -> str:
        return text

    async def batch_translate(self, texts: list[str], source: str = "en", target: str = "zh") -> list[str]:
        return texts


TRANSLATORS = {"tencent": TencentTranslator, "none": NullTranslator}
# 预留："openai": OpenAITranslator, "deepl": DeepLTranslator


def get_translator(translator_type: str, **kwargs) -> TranslatorBase:
    translator = TRANSLATORS.get(translator_type, NullTranslator)
    return translator(**kwargs) if translator is TencentTranslator else translator()