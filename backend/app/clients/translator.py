from .tencent_translator import TencentTranslator
from .google_translator import GoogleTranslator
from .translator_base import TranslatorBase


class NullTranslator(TranslatorBase):
    async def translate(self, text: str, source: str = "en", target: str = "zh") -> str:
        return text

    async def batch_translate(self, texts: list[str], source: str = "en", target: str = "zh") -> list[str]:
        return texts


TRANSLATORS = {"tencent": TencentTranslator, "google": GoogleTranslator, "none": NullTranslator}
# 预留："openai": OpenAITranslator, "deepl": DeepLTranslator


def get_translator(translator_type: str, **kwargs) -> TranslatorBase:
    """按类型构造翻译器。
    腾讯翻译君需要 secret_id/secret_key/region 等参数；谷歌/空翻译器无参构造。
    """
    translator = TRANSLATORS.get(translator_type, NullTranslator)
    if translator is TencentTranslator:
        filtered = {k: v for k, v in kwargs.items() if k in ("secret_id", "secret_key", "region")}
        return translator(**filtered)
    return translator()