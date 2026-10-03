from sqlalchemy import select
import json
import re
from sqlalchemy.ext.asyncio import AsyncSession
import logging

from .clients.translator import get_translator
from .models import SystemConfig, TranslationGlossary

logger = logging.getLogger(__name__)


class TranslationService:
    # 内置常见 galgame 标签术语表（腾讯翻译君对galgame标签翻译质量差，内置可靠映射）
    BUILTIN_TAG_GLOSSARY = {
        "3D作品": "3D作品", "2D作品": "2D作品", "動画": "动画", "アニメ": "动画",
        "おもちゃ": "玩具", "少女": "少女", "ロリ": "萝莉", "ロリータ": "萝莉",
        "学校/学園": "学校/学园", "学園": "学园", "学校": "学校", "学園もの": "校园",
        "強制/無理矢理": "强制/强迫", "強制": "强制", "無理矢理": "强迫", "レイプ": "强奸",
        "放尿/おしっこ": "放尿/小便", "放尿": "放尿", "おしっこ": "小便", "お漏らし": "失禁",
        "貧乳/微乳": "贫乳/微乳", "貧乳": "贫乳", "微乳": "微乳", "巨乳": "巨乳", "爆乳": "爆乳",
        "萌え": "萌", "ツンデレ": "傲娇", "ヤンデレ": "病娇", "クーデレ": "高冷",
        "幼なじみ": "青梅竹马", "幼馴染": "青梅竹马", "妹": "妹妹", "義妹": "义妹", "姉": "姐姐", "義姉": "义姐",
        "母": "母亲", "義母": "义母", "叔母": "叔母", "伯母": "伯母", "いとこ": "表亲",
        "催眠": "催眠", "催眠術": "催眠术", "暗示": "暗示", "洗脳": "洗脑", "記憶操作": "记忆操作",
        "凌辱": "凌辱", "輪姦": "轮奸", "中出し": "内射", "アナル": "肛交", "フェラ": "口交",
        "パイズリ": "乳交", "手コキ": "手交", "足コキ": "足交", "淫語": "淫语", "調教": "调教",
        "奴隷": "奴隶", "監禁": "监禁", "拷問": "拷问", "緊縛": "紧缚", "縛り": "捆绑", "SM": "SM",
        "異世界": "异世界", "ファンタジー": "奇幻", "戦国": "战国", "幕末": "幕末", "明治": "明治",
        "純愛": "纯爱", "NTR": "NTR", "寝取られ": "寝取", "寝取り": "寝取", "逆レイプ": "逆强奸",
        "レズビアン": "百合", "百合": "百合", "ボーイズラブ": "耽美", "BL": "BL", "トランス": "变性",
        "男の娘": "伪娘", "女装": "女装", "男装": "男装", "人妻": "人妻", "不倫": "不伦", "浮気": "出轨",
        "ハーレム": "后宫", "逆ハーレム": "逆后宫", "一夫多妻": "一夫多妻", "妊娠": "妊娠", "出産": "分娩",
        "授乳": "授乳", "痴漢": "痴汉", "痴女": "痴女", "盗撮": "偷拍", "覗き": "偷窥", "露出": "露出",
        "ペット": "宠物", "メイド": "女仆", "ナース": "护士", "女教師": "女教师", "女学生": "女学生",
        "巫女": "巫女", "忍者": "忍者", "くノ一": "女忍者", "サキュバス": "魅魔", "魔族": "魔族",
        "天使": "天使", "悪魔": "恶魔", "吸血鬼": "吸血鬼", "人狼": "人狼", "獣人": "兽人", "エルフ": "精灵",
        "獣耳": "兽耳", "猫耳": "猫耳", "犬耳": "犬耳", "ウサギ耳": "兔耳", "狐耳": "狐耳", "ネコミミ": "猫耳",
        "メイド服": "女仆装", "スクール水着": "学校泳装", "ブルマ": "运动裤", "セーラー服": "水手服",
        "チャイナドレス": "旗袍", "ランジェリー": "内衣", "ストッキング": "丝袜", "パンスト": "连裤袜",
        "制服": "制服", "水着": "泳装", "浴衣": "浴衣", "和服": "和服", "下着": "内衣", "裸": "裸体",
        "全裸": "全裸", "半裸": "半裸", "おっぱい": "胸部", "乳首": "乳头", "尻": "臀部", "太もも": "大腿",
        "美少女": "美少女", "熟女": "熟女", "御姐": "御姐", "JK": "女高中生", "JC": "女初中生", "JS": "女小学生",
        "社会人": "社会人", "OL": "白领", "ボイン": "巨乳", "むちむち": "丰满", "スレンダー": "苗条",
        "眼鏡": "眼镜", "ツインテール": "双马尾", "ポニーテール": "马尾", "ショートヘア": "短发",
        "ロングヘア": "长发", "黒髪": "黑发", "金髪": "金发", "茶髪": "棕发", "銀髪": "银发",
        "赤髪": "红发", "青髪": "蓝发", "ピンク髪": "粉发", "紫髪": "紫发", "緑髪": "绿发",
        "同人": "同人", "同人誌": "同人志", "同人ゲーム": "同人游戏", "エロゲー": "美少女游戏",
        "ギャルゲー": "辣妹游戏", "乙女ゲーム": "乙女游戏", "BLゲーム": "BL游戏", "百合ゲーム": "百合游戏",
        "RPG": "RPG", "アクション": "动作", "アドベンチャー": "冒险", "シミュレーション": "模拟",
        "パズル": "解谜", "ノベル": "小说", "ビジュアルノベル": "视觉小说", "サウンドノベル": "有声小说",
        "恋愛": "恋爱", "育成": "养成", "経営": "经营", "戦略": "战略", "シューティング": "射击",
        "ホラー": "恐怖", "サイコロジカルホラー": "心理恐怖", "ミステリー": "推理", "サスペンス": "悬疑",
        "コメディ": "喜剧", "ギャグ": "搞笑", "パロディ": "恶搞", "感動": "感动", "涙もの": "催泪",
        "ハッピーエンド": "好结局", "バッドエンド": "坏结局", "トゥルーエンド": "真结局", "マルチエンディング": "多结局",
        "魔法": "魔法", "魔術": "魔术", "呪い": "诅咒", "契約": "契约", "召喚": "召唤", "錬金術": "炼金术",
        "神": "神", "女神": "女神", "天使": "天使", "堕天使": "堕天使", "悪魔": "恶魔", "魔王": "魔王",
        "ドラゴン": "龙", "竜": "龙", "吸血鬼": "吸血鬼", "ヴァンパイア": "吸血鬼", "人狼": "人狼",
        "ゾンビ": "僵尸", "ゴースト": "幽灵", "幽霊": "幽灵", "魔女": "魔女", "魔法少女": "魔法少女",
        "魔法使い": "魔法师", "魔導師": "魔导师", "剣士": "剑士", "剣客": "剑客", "侍": "武士", "武士": "武士",
        "冒険者": "冒险者", "勇者": "勇者", "王女": "王女", "王子": "王子", "姫": "公主", "騎士": "骑士",
        "傭兵": "佣兵", "盗賊": "盗贼", "狩人": "猎人", "戦士": "战士", "僧侶": "僧侣", "神官": "神官",
        "学者": "学者", "研究者": "研究者", "発明家": "发明家", "エンジニア": "工程师", "パイロット": "飞行员",
    }
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
        # 标签(category="tag")：优先用内置术语表，没有的保留原文（腾讯翻译君对galgame标签翻译质量差）
        if category == "tag":
            builtin = self.BUILTIN_TAG_GLOSSARY.get(text)
            if builtin:
                return builtin
            return text
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