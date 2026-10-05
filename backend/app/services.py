from datetime import date
import hashlib
import json
import logging
import re
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .clients.rawg_client import RawgClient
from .clients.steam_client import SteamClient
from .cover_service import cache_cover
from .database import SessionLocal
from .models import Game, SystemConfig
from .translation_service import translation_service
from .config import settings

logger = logging.getLogger(__name__)



async def resolve_tags(game: Game, config: SystemConfig, requested_source: str = "", db: AsyncSession = None) -> str:
    """按配置优先级从 original_data 中选择标签；返回选中的标签字符串。"""
    try:
        priority = json.loads(config.tag_source_priority or "[]")
    except json.JSONDecodeError:
        priority = ["steam", "rawg", "vndb", "dlsite"]
    if requested_source:
        priority = [requested_source]
    # 优先从 source_data 取，fallback 到 original_data
    try:
        source_data = json.loads(game.source_data or "{}")
    except json.JSONDecodeError:
        source_data = {}
    try:
        metadata = json.loads(game.original_data or "{}")
    except json.JSONDecodeError:
        metadata = {}
    for source in priority:
        tags = ""
        # 1. 优先从 source_data[source].tags 取
        if source in source_data:
            tags = source_data[source].get("tags", "")
        # 2. fallback 到 original_data 的 {source}_tags
        if not tags:
            tags = metadata.get(f"{source}_tags", "")
        # 3. 再 fallback：游戏本身就是该来源，从 original_data.tags 取
        if not tags and game.source_type == source:
            tags = metadata.get("tags", "")
        if tags:
            # 原文标签存 source_data
            try:
                sd = json.loads(game.source_data or "{}")
            except json.JSONDecodeError:
                sd = {}
            if source not in sd:
                sd[source] = {}
            sd[source]["tags"] = tags
            game.source_data = json.dumps(sd, ensure_ascii=False, default=str)
            # 翻译标签：逐个翻译，内置术语表按单个标签匹配
            translated_tags = tags
            if db and config and config.auto_translate and config.translator_type != "none":
                try:
                    from app.translation_service import TranslationService
                    ts = TranslationService()
                    await ts.load(db)
                    parts = [p.strip() for p in tags.split(",") if p.strip()]
                    translated_parts = [await ts.translate(p, "tag", db) for p in parts]
                    translated_tags = ", ".join(translated_parts)
                except Exception as e:
                    logger.warning("标签翻译失败 game_id=%s: %s", game.id, e)
            game.tags = translated_tags
            game.tag_source = source
            logger.info("标签解析：game_id=%s, source=%s, 原文=%.60s, 译文=%.60s", game.id, source, tags[:60], translated_tags[:60])
            return translated_tags
    game.tags = ""
    game.tag_source = ""
    logger.warning("标签解析失败：game_id=%s，所有来源均无标签，已清空", game.id)
    return ""


# 常见中文别名 -> 英文名，用于跨数据源搜索（跨源搜索通用增强）
_ALIAS_MAP = {
    "帝国时代": "Age of Empires",
    "帝国时代2": "Age of Empires II",
    "兰斯": "Rance",
    "兰斯3": "Rance 3",
    "兰斯10": "Rance 10",
    "最终幻想": "Final Fantasy",
    "生化危机": "Resident Evil",
    "塞尔达传说": "The Legend of Zelda",
    "女神异闻录": "Persona",
    "真三国无双": "Dynasty Warriors",
    "合金装备": "Metal Gear Solid",
    "巫师": "The Witcher",
    "上古卷轴": "The Elder Scrolls",
    "使命召唤": "Call of Duty",
    "战地": "Battlefield",
    "模拟人生": "The Sims",
    "暗黑破坏神": "Diablo",
    "星际争霸": "StarCraft",
    "魔兽争霸": "Warcraft",
    "刺客信条": "Assassin's Creed",
    "孤岛惊魂": "Far Cry",
    "看门狗": "Watch Dogs",
    "荒野大镖客": "Red Dead Redemption",
    "战神": "God of War",
    "神秘海域": "Uncharted",
    "地平线": "Horizon",
    "死亡搁浅": "Death Stranding",
    "尼尔": "NieR",
    "如龙": "Yakuza",
    "只狼": "Sekiro",
    "黑暗之魂": "Dark Souls",
    "艾尔登法环": "Elden Ring",
    "鬼泣": "Devil May Cry",
    "忍者龙剑传": "Ninja Gaiden",
    "仁王": "Nioh",
    "拳皇": "The King of Fighters",
    "街头霸王": "Street Fighter",
    "铁拳": "Tekken",
    "塞尔达": "Zelda",
    "牧场物语": "Story of Seasons",
    "符文工房": "Rune Factory",
    "炼金工房": "Atelier",
    "弹丸论破": "Danganronpa",
    "逆转裁判": "Ace Attorney",
    "命运石之门": "Steins;Gate",
    "秋之回忆": "Memories Off",
    "沙耶之歌": "Saya no Uta",
    "白色相簿": "White Album",
    "月姬": "Tsukihime",
    "寒蝉鸣泣之时": "Higurashi",
    "海猫鸣泣之时": "Umineko",
    "缘之空": "Yosuga no Sora",
    "日在校园": "School Days",
    "恋姬无双": "Koihime Musou",
    "千恋万花": "Senren Banka",
    "魔女的夜宴": "Sabbat of the Witch",
    "图书馆": "Library",
    "三国志": "Romance of the Three Kingdoms",
    "信长之野望": "Nobunaga's Ambition",
    "太阁立志传": "Taiko Risshiden",
    "樱花大战": "Sakura Wars",
    "梦幻模拟战": "Langrisser",
    "火焰纹章": "Fire Emblem",
    "异度之刃": "Xenoblade",
    "异度神剑": "Xenoblade",
    "勇者斗恶龙": "Dragon Quest",
    "真女神转生": "Shin Megami Tensei",
    "怪物猎人": "Monster Hunter",
    "逆转检事": "Ace Attorney Investigations",
    "弹丸": "Danganronpa",
    "女神异闻录5": "Persona 5",
    "P5": "Persona 5",
    # 日系 / galgame / 常见系列（Steam/VNDB 英文名）
    "兰斯": "Rance",
    "性感沙滩": "Sexy Beach",
    "性感海滩": "Sexy Beach",
    "沙滩": "Sexy Beach",
    "尾行": "Biko",
    "欲望之血": "Des Blood",
    "电车之狼": "RapeLay",
    "人工少女": "Artificial Girl",
    "同校生": "Schoolmate",
    "真实女友": "Real Kanojo",
    "欲望格斗": "Battle Raper",
    "苍之彼方的四重奏": "Aokana",
    "千恋": "Senren Banka",
    "魔女的夜宴": "Sabbat of the Witch",
    "夏空": "Natsuzora",
    "星空": "Hoshizora",
    "大图书馆的牧羊人": "The Sheep Herders of the Great Library",
    "悠之空": "Haru no Sora",
    "天神乱漫": "Tenshi no Nichou",
    "DRACU-RIOT": "DRACU-RIOT",
    "夏空彼方": "Natsuzora Kanata",
    "魔女夜宴": "Sabbat of the Witch",
    "樱之诗": "Sakura no Uta",
    "樱之刻": "Sakura no Toki",
    "白色相簿2": "White Album 2",
    "悠久之翼": "ef - a fairy tale of the two",
    "CLANNAD": "CLANNAD",
    "AIR": "AIR",
    "Kanon": "Kanon",
    "Little Busters": "Little Busters",
    "Rewrite": "Rewrite",
    "Summer Pockets": "Summer Pockets",
    "Angel Beats": "Angel Beats",
    "智代": "Tomoyo After",
    "智代后记": "Tomoyo After",
    "星之梦": "Planetarian",
    "Little Busters!": "Little Busters",
    "ef": "ef - a fairy tale of the two",
    "eden": "eden*",
    "G弦上的魔王": "G-senjou no Maou",
    "车轮之国": "Sharin no Kuni",
    "秽翼的尤斯蒂娅": "Eustia",
    "遥仰凰华": "Haruka na Sora",
    "恋爱选举与巧克力": "Koi to Senkyo to Chocolate",
    "初雪樱": "Hatsuyuki Sakura",
    "纸上的魔法使": "Kami-sama no You na Kimi e",
    "纸魔": "Kami-sama no You na Kimi e",
    "ATRI": "ATRI -My Dear Moments-",
    "苍之彼方": "Aokana",
    "牛顿与苹果树": "Newton and the Apple Tree",
    "宿星": "Sukimazakura to Uso no Machi",
    "Fate": "Fate/stay night",
    "Fate/stay night": "Fate/stay night",
    "月姫": "Tsukihime",
    "魔法使之夜": "Mahoutsukai no Yoru",
    "魔法师之夜": "Mahoutsukai no Yoru",
    "空之境界": "Kara no Kyoukai",
    "恋爱随意链接": "Kokoro Connect",
    "银魂": "Gintama",
    "绯月": "Hiiro",
}


def normalize_core(text: str) -> str:
    """提取标题核心：去掉副标题分隔符后的部分、括号内容、年份、场景组后缀。"""
    if not text:
        return ""
    value = text
    # 去掉方括号/圆括号内容（发布组、年份、汉化组等）
    value = re.sub(r"\[[^\]]*\]", " ", value)
    value = re.sub(r"[\(（][^\)）]*[\)）]", " ", value)
    # 去掉年份/编号样式（[111229]、20141230）
    value = re.sub(r"\b\d{6,8}\b", " ", value)
    # 副标题分隔符统一（冒号/破折号/波浪号/中点）
    value = re.split(r"[:：\-—–~～]|(?<=\w)\s+[·・]\s*(?=\w)", value, maxsplit=1)[0]
    # 去版本号
    value = re.sub(r"(?i)\bv\d+(?:\.\d+){1,3}\b", " ", value)
    value = re.sub(r"[._]+", " ", value)
    return re.sub(r"\s+", " ", value).strip().lower()


# 中文/日文版本后缀（剥离后得到"游戏主体名"，提升跨源命中率）
_EDITION_SUFFIXES = [
    "决定版", "決定版", "重制版", "重置版", "重製版", "高清版", "高畫質版", "复刻版", "復刻版",
    "完整版", "完全版", "豪华版", "豪華版", "终极版", "終極版", "典藏版", "珍藏版", "限定版",
    "增强版", "加強版", "年度版", "年度遊戲版", "周年纪念版", "紀念版", "导演剪辑版", "導剪版",
    "终极", "合集", "合辑", "全集", "汉化版", "漢化版", "中文版", "正式版", "抢先体验版",
    "definitive edition", "remastered", "remake", "complete edition", "deluxe edition",
    "ultimate edition", "enhanced edition", "game of the year edition", "goty edition",
    "anniversary edition", "special edition", "collector's edition", "hd edition",
    "the complete edition", "final edition",
]

# 阿拉伯数字 -> 罗马数字
_ROMAN = {1: "I", 2: "II", 3: "III", 4: "IV", 5: "V", 6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
          11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV"}


def _strip_edition(text: str) -> str:
    """剥离版本后缀，得到游戏主体名。"""
    if not text:
        return ""
    value = text
    for suffix in _EDITION_SUFFIXES:
        value = re.sub(re.escape(suffix), " ", value, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", value).strip()


def _num_variants(text: str) -> list[str]:
    """为标题中的数字生成变体：2 -> II / 02 / 2（解决 'Rance 3' vs 'Rance 03'）。"""
    out = []

    def repl(match):
        n = int(match.group(1))
        variants = {match.group(0)}
        variants.add("%02d" % n)  # 3 -> 03
        variants.add(str(n))       # 03 -> 3
        if n in _ROMAN:
            variants.add(_ROMAN[n])  # 3 -> III
        return variants

    # 逐个数字位置生成组合（最多 3 个数字，避免组合爆炸）
    positions = list(re.finditer(r"\b(\d{1,2})\b", text))
    if not positions:
        return []
    from itertools import product
    groups = []
    for m in positions[:3]:
        n = int(m.group(1))
        variants = {m.group(0)}
        variants.add("%02d" % n)
        variants.add(str(n))
        if n in _ROMAN:
            variants.add(_ROMAN[n])
        groups.append(list(variants))
    for combo in product(*groups):
        value = text
        for m, rep in reversed(list(zip(positions[:3], combo))):
            value = value[:m.start()] + rep + value[m.end():]
        out.append(value)
    return out


def _alias_hit(key: str, haystack: str) -> bool:
    """别名命中判定：
    - 含 CJK 的 key：直接子串匹配（中文无词边界问题）；
    - 纯 ASCII 的短 key（如 ef / p5 / air）：必须作为独立词出现，避免 'ef' 命中 'definitive'；
    - 其余 ASCII key：要求词边界匹配。
    """
    if not key or not haystack:
        return False
    if re.search(r"[\u4e00-\u9fff\u3040-\u30ff]", key):
        return key in haystack
    return re.search(r"(?<![a-z0-9])" + re.escape(key) + r"(?![a-z0-9])", haystack) is not None


def alias_candidates(text: str) -> list[str]:
    """由（中文）标题生成跨源搜索候选。
    策略：命中别名表时，剥离中文版本后缀后组合英文名；再对数字生成罗马/补零变体。
    产出顺序：最精确 -> 最宽泛，调用方按顺序尝试即可。
    """
    if not text:
        return []
    cleaned = normalize_core(text) or (text or "").lower()
    base = _strip_edition(cleaned)          # 已剥离版本后缀的核心名
    base_raw = _strip_edition(text or "")
    out = []
    for cn, en in _ALIAS_MAP.items():
        key = cn.lower()
        if _alias_hit(key, cleaned) or _alias_hit(key, (text or "").lower()):
            # 去掉中文别名后剩下的部分（可能是数字/版本，如 "帝国时代2决定版" -> "2"）
            rest = base.replace(key, " ").strip()
            rest = re.sub(r"\s+", " ", rest).strip()
            # rest 里若只剩中文/标点则忽略，避免 "Age of Empires 2决定版" 这类垃圾候选
            rest = re.sub(r"[\u4e00-\u9fff]+", " ", rest).strip()
            if rest:
                cand = f"{en} {rest}".strip()
                out.append(cand)
                out.extend(_num_variants(cand))
            out.append(en)
            out.extend(_num_variants(en))
    # 未命中别名表：用剥离版本后缀后的原名（对英文标题尤其有效，如 "Age of Empires II: DE"）
    if not out:
        if base_raw and base_raw.lower() != (text or "").lower():
            out.append(base_raw)
        out.extend(_num_variants(base_raw))
    # 去重保序（忽略大小写与多余空格）
    seen, result = set(), []
    for item in out:
        item = re.sub(r"\s+", " ", item).strip()
        if len(item) < 2:
            continue
        low = item.lower()
        if low not in seen:
            seen.add(low)
            result.append(item)
    return result


def _series_numbers(text: str) -> set[str]:
    """提取标题中的"作品序号"数字（含罗马数字归一化），用于系列作区分。
    如 "Sexy Beach 4" -> {"4"}，"Age of Empires II" -> {"2"}，"兰斯 03" -> {"3"}。
    """
    if not text:
        return set()
    nums: set[str] = set()
    # 阿拉伯数字
    for m in re.finditer(r"(?<![a-z0-9])(\d{1,2})(?![a-z0-9])", text):
        nums.add(str(int(m.group(1))))
    # 罗马数字（独立词）
    for m in re.finditer(r"(?<![a-z0-9])([ivx]{1,4})(?![a-z0-9])", text.lower()):
        val = _roman_to_int(m.group(1))
        if val is not None:
            nums.add(str(val))
    return nums


def _roman_to_int(token: str) -> int | None:
    roman = {"i": 1, "v": 5, "x": 10, "l": 50, "c": 100}
    if not token or any(ch not in roman for ch in token):
        return None
    total, prev = 0, 0
    for ch in reversed(token):
        val = roman[ch]
        if val < prev:
            total -= val
        else:
            total += val
            prev = val
    # 只认可 <= 30 的小序号（避免把 "mix"/"civil" 等误判为罗马数字）
    return total if 1 <= total <= 30 else None


def title_similarity(query: str, result_title: str) -> float:
    """双向包含 + 词元重叠的相似度：query 是结果标题的一部分（或反之）时高分。
    额外规则：若两侧的"作品序号"不同（如 4 vs 2、4 vs 无），判定为不同作品，分数压低。
    """
    if not query or not result_title:
        return 0.0
    q, r = normalize_core(query), normalize_core(result_title)

    def tokens(text):
        return set(re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text.lower()).split())

    q_tokens, r_tokens = tokens(q), tokens(r)
    if not q_tokens or not r_tokens:
        return 0.0
    overlap = len(q_tokens & r_tokens) / len(q_tokens)
    if q and r and (q in r or r in q):
        overlap = max(overlap, 0.9)
    # 序号一致性：任一侧有序号且不一致时，说明是系列中的不同作品，大幅降分
    # 序号一致性（系列作区分）：
    # 1) 两侧都有序号且不一致 -> 不同作品，重降分（如 Sexy Beach 4 vs Sexy Beaches 2）；
    # 2) query 有序号但结果无序号 -> 结果可能是同系列外传/无关作，降分（如 "SEX beach 4" vs "SEX ON THE BEACH"）；
    # 3) 结果有序号但 query 无 -> 不降分（避免误伤，如中文 query 序号被别名吸收的情形）。
    q_nums, r_nums = _series_numbers(query), _series_numbers(result_title)
    if q_nums and r_nums and q_nums != r_nums:
        overlap = min(overlap, 0.25)
    elif q_nums and not r_nums:
        overlap = min(overlap, 0.35)
    return overlap


def normalize_release_date(value):
    """把各数据源的 release_date（字符串/列表/date）统一为 date 对象或 None。
    Game.release_date 是 SQLite Date 类型，只接受 Python date 对象。
    """
    if not value:
        return None
    if isinstance(value, list):
        value = value[0] if value else None
        if value is None:
            return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.split("T")[0].split(" ")[0])
        except (ValueError, TypeError):
            return None
    return None

async def retag_games_from_glossary(db: AsyncSession) -> int:
    """术语表变更后，把最新的 tag 映射重新应用到所有游戏已持久化的标签上。

    策略：
    - 仅使用术语表正向映射（source_text -> target_text），不调用翻译器，速度快且无外部依赖。
    - 优先取原文标签（original_data.tags 快照 / source_data[tag_source].tags），
      与现有 game.tags 按位置对齐：新译文 = 术语表[原文] or 术语表[现有] or 保留现有译文。
    - 原文与现有译文数量不一致时，退化为对现有 tags 逐条正向查术语表。
    返回更新的游戏数量。
    """
    await translation_service.load(db)
    glossary = translation_service.glossary

    def lookup(text: str) -> str:
        return glossary.get((text, "tag")) or glossary.get((text, "")) or ""

    # 通用(general)术语：原本按标签翻译、已持久化的字段，术语表新增 general 后需同步
    # （如 "Shoujo" -> "少女"）；仅替换主字段，不误改别名等无关文本。
    def _apply_general(value: str) -> str:
        if not value:
            return value
        for (source_text, cat), target in glossary.items():
            if cat == "general" and source_text and source_text != target:
                value = value.replace(source_text, target)
        return value

    # 标题(title)类术语：整串命中优先，否则做词内子串替换
    def _apply_title(value: str) -> str:
        if not value:
            return value
        exact = glossary.get((value, "title"))
        if exact:
            return exact
        for (source_text, cat), target in glossary.items():
            if cat == "title" and source_text and target and source_text != target and source_text in value:
                value = value.replace(source_text, target)
        return value

    updated = 0
    games = list((await db.scalars(select(Game))).all())
    for game in games:
        current_tags = (game.tags or "").strip()
        if not current_tags:
            continue
        cur_parts = [part.strip() for part in current_tags.split(",") if part.strip()]
        # 收集原文标签候选
        orig_tags = ""
        try:
            source_data = json.loads(game.source_data or "{}")
        except json.JSONDecodeError:
            source_data = {}
        src = game.tag_source or game.source_type or ""
        if src and isinstance(source_data.get(src), dict):
            orig_tags = source_data[src].get("tags", "") or ""
        if not orig_tags:
            try:
                original = json.loads(game.original_data or "{}")
            except json.JSONDecodeError:
                original = {}
            orig_tags = original.get("tags", "") or ""
        orig_parts = [part.strip() for part in orig_tags.split(",") if part.strip()] if orig_tags else []
        def _is_chinese(text: str) -> bool:
            if not text:
                return False
            cn = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
            kana = any("\u3040" <= ch <= "\u30ff" for ch in text)
            return cn > 0 and not kana

        if orig_parts and len(orig_parts) == len(cur_parts):
            # 位置对齐：术语表[原文] 优先；原文是中文时直接用原文（中文不再机翻，
            # 历史上"单人->中国人"等错误译文应被还原）；否则 术语表[现有] 或保留现有
            new_parts = [
                lookup(orig) or (orig if _is_chinese(orig) else (lookup(cur) or cur))
                for orig, cur in zip(orig_parts, cur_parts)
            ]
        else:
            # 数量对不上：只能对现有译文正向查表（覆盖术语表修正/中英互改）
            new_parts = [lookup(cur) or cur for cur in cur_parts]
        new_tags = ", ".join(new_parts)
        changed = new_tags != current_tags
        if changed:
            game.tags = new_tags
        # title 类 + general 通用术语同步到标题
        new_title = _apply_general(_apply_title(game.title or ""))
        if new_title != (game.title or ""):
            game.title = new_title
            changed = True
        new_desc = _apply_general(game.description or "")
        if new_desc != (game.description or ""):
            game.description = new_desc
            changed = True
        if changed:
            updated += 1
    if updated:
        await db.commit()
    logger.info("术语表重应用完成：更新 %d 个游戏的标签", updated)
    return updated

async def fetch_game_screenshots(game: Game, config: SystemConfig, requested_source: str = "") -> list[str]:
    """按配置优先级获取并缓存截图；失败的来源会继续尝试下一个来源。"""
    logger.info("开始获取截图：game_id=%s, source_type=%s, source_id=%s, screenshots_field=%.100s",
                game.id, game.source_type, game.source_id, game.screenshots or "")
    try:
        priority = json.loads(config.screenshot_source_priority or "[]")
    except json.JSONDecodeError:
        priority = ["steam", "rawg", "vndb", "dlsite"]
    limit = max(0, int(config.max_screenshots or 5))
    metadata = {}
    try:
        metadata = json.loads(game.original_data or "{}")
    except json.JSONDecodeError:
        pass
    source_data = {}
    try:
        source_data = json.loads(game.source_data or "{}")
    except json.JSONDecodeError:
        pass
    candidates: dict[str, list[str]] = {}
    # 优先从 source_data 取各来源的截图 URL
    for _src in ["rawg", "steam", "vndb", "dlsite"]:
        if _src in source_data and source_data[_src].get("screenshots"):
            try:
                candidates[_src] = json.loads(source_data[_src]["screenshots"])
            except (json.JSONDecodeError, TypeError):
                pass
    # fallback：从 original_data 或 game.screenshots 取
    if not candidates.get("rawg"):
        if metadata.get("screenshots"):
            candidates["rawg"] = metadata["screenshots"]
        else:
            try:
                candidates["rawg"] = json.loads(game.screenshots or "[]") if game.source_type == "rawg" else []
            except json.JSONDecodeError:
                candidates["rawg"] = []
    steam_appid = game.steam_appid or metadata.get("steam_appid", "")
    if not steam_appid and game.source_type == "rawg" and game.source_id:
        try:
            rawg_metadata = await RawgClient().get_game_detail(game.source_id)
            steam_appid = rawg_metadata.get("steam_appid", "")
            if rawg_metadata.get("screenshots") and not candidates.get("rawg"):
                candidates["rawg"] = json.loads(rawg_metadata["screenshots"])
        except Exception:
            logger.warning("读取 RAWG Steam 商店信息失败：%s", game.source_id, exc_info=True)
    # Steam 截图：steam 来源游戏直接用已有 screenshots；其他来源有 steam_appid 时调 appdetails 补截图
    if game.source_type == "steam":
        # 优先从 original_data.screenshots 读远程 URL，不依赖可能被清空的 game.screenshots
        od_steam = metadata.get("screenshots", "")
        if od_steam:
            try:
                candidates["steam"] = json.loads(od_steam)
            except json.JSONDecodeError:
                pass
        if not candidates.get("steam"):
            try:
                existing_steam = json.loads(game.screenshots or "[]")
            except json.JSONDecodeError:
                existing_steam = []
            if existing_steam:
                candidates["steam"] = existing_steam
    elif steam_appid:
        try:
            steam_detail = await SteamClient().get_game_detail(steam_appid)
            steam_shots = json.loads(steam_detail.get("screenshots") or "[]")
            if steam_shots:
                candidates["steam"] = steam_shots
                logger.info("游戏 %s 从 Steam appdetails 获取 %d 张截图", game.id, len(steam_shots))
        except Exception:
            logger.warning("游戏 %s 从 Steam appdetails 获取截图失败", game.id, exc_info=True)
    if game.source_type == "vndb" and game.source_id:
        candidates["vndb"] = await _vndb_screenshots(game.source_id)
    # VNDB 无截图时，按标题从 RAWG 补截图。
    if game.source_type == "vndb" and not candidates.get("vndb"):
        try:
            rawg_candidates = await RawgClient().search_games(game.title, page_size=1)
            if rawg_candidates:
                rawg_detail = await RawgClient().get_game_detail(rawg_candidates[0]["source_id"])
                rawg_shots = json.loads(rawg_detail.get("screenshots") or "[]")
                if rawg_shots:
                    candidates["rawg"] = rawg_shots
        except Exception:
            logger.warning("VNDB 游戏从 RAWG 补截图失败：%s", game.title, exc_info=True)
    if game.source_type == "dlsite":
        # 优先用扫描时已存入的 API 截图 URL（dlsite_client._map_detail 已填充）
        try:
            existing_shots = json.loads(game.screenshots or "[]")
        except json.JSONDecodeError:
            existing_shots = []
        if existing_shots:
            candidates["dlsite"] = existing_shots
        elif game.source_id:
            try:
                candidates["dlsite"] = await _dlsite_screenshots(game.source_id)
            except Exception:
                pass

    logger.info("截图候选：rawg=%d, steam=%d, vndb=%d, dlsite=%d",
                len(candidates.get("rawg", [])), len(candidates.get("steam", [])),
                len(candidates.get("vndb", [])), len(candidates.get("dlsite", [])))
    directory = settings.data_dir / "screenshots"
    directory.mkdir(parents=True, exist_ok=True)
    if requested_source:
        priority = [requested_source]
        # 指定来源时，无论是否找到截图，都记录用户选择的来源
        game.screenshot_source = requested_source
    elif game.source_type == "steam" and "steam" in priority:
        priority = ["steam"] + [s for s in priority if s != "steam"]
    for source in priority:
        urls = candidates.get(source, [])[:limit]
        if not urls:
            continue
        cached = []
        dl_headers = {"User-Agent": "Mozilla/5.0"}
        # DLSite 图片有防盗链，需带 Referer 才能下载
        if any("dlsite.jp" in u for u in urls):
            dl_headers["Referer"] = "https://www.dlsite.com/"
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=dl_headers) as client:
            for index, url in enumerate(urls):
                if not url:
                    continue
                # 本地路径候选：校验文件名来源前缀匹配，避免把其他来源的本地路径当作当前来源候选
                if url.startswith("/data/screenshots/"):
                    fname = url.split("/")[-1]
                    expected_prefix = f"{game.id}_{source}_"
                    if not fname.startswith(expected_prefix):
                        continue  # 来源不匹配，跳过
                    # 来源匹配且文件存在，直接复用
                    if (directory / fname).exists():
                        cached.append(url)
                        continue
                    continue  # 文件不存在，跳过（本地路径无法重新下载）
                suffix = ".png" if ".png" in url.lower() else ".jpg"
                filename = f"{game.id}_{source}_{index}{suffix}"
                path = directory / filename
                try:
                    if not path.exists():
                        response = await client.get(url)
                        response.raise_for_status()
                        path.write_bytes(response.content)
                    cached.append(f"/data/screenshots/{filename}")
                except (httpx.HTTPError, OSError):
                    continue
        if cached:
            game.screenshots = json.dumps(cached, ensure_ascii=False)
            game.screenshot_source = source
            logger.info("截图缓存完成：game_id=%s, source=%s, cached=%d", game.id, source, len(cached))
            return cached
    logger.warning("截图获取失败：game_id=%s，所有来源均无有效截图", game.id)
    return []


async def _vndb_screenshots(source_id: str) -> list[str]:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post("https://api.vndb.org/kana/vn", json={"filters": ["id", "=", source_id], "fields": "image.url"})
        response.raise_for_status()
        return [(item.get("image") or {}).get("url", "") for item in response.json().get("results", [])]


async def _dlsite_screenshots(source_id: str) -> list[str]:
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers={"User-Agent": "Mozilla/5.0"}) as client:
            response = await client.get(f"https://www.dlsite.com/maniax/work/=/product_id/{source_id}.html")
            response.raise_for_status()
        urls = re.findall(r'https?://[^"\']+\.(?:jpg|jpeg|png)', response.text, re.I)
        return list(dict.fromkeys(urls))
    except Exception as e:
        logger.warning("DLsite 截图获取失败 %s: %s", source_id, e)
        return []


async def search_vndb(query: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post("https://api.vndb.org/kana/vn", json={"filters": ["search", "=", query], "fields": "id,title,alttitle,description,image.url,developers.name,released,rating,tags.name", "results": 20})
        response.raise_for_status()
        results = []
        for item in response.json().get("results", []):
            released = item.get("released")
            try:
                rel_date = released if released else None
            except (ValueError, TypeError):
                rel_date = None
            results.append({"source_type": "vndb", "source_id": item.get("id", ""), "title": item.get("title", ""), "alias": item.get("alttitle") or "", "cover_url": (item.get("image") or {}).get("url", ""), "description": item.get("description", ""), "developer": ", ".join(x.get("name", "") for x in item.get("developers", [])), "publisher": "", "release_date": rel_date, "rating": item.get("rating"), "tags": ", ".join(x.get("name", "") for x in item.get("tags", [])), "series": ""})
        return results


async def get_vndb_detail(source_id: str) -> dict:
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(
            "https://api.vndb.org/kana/vn",
            json={"filters": ["id", "=", source_id], "fields": "id,title,alttitle,description,image.url,developers.name,released,rating,tags.name"},
        )
        response.raise_for_status()
        item = response.json().get("results", [])
        if not item:
            raise ValueError("VNDB 游戏不存在")
        value = item[0]
        released = value.get("released")
        try:
            release_date = released if released else None
        except (ValueError, TypeError):
            release_date = None
        vndb_title = value.get("title", "")
        vndb_alttitle = value.get("alttitle", "")
        # VNDB 的 title 是罗马音，alttitle 是日文原名；优先用日文原名作为标题（翻译更准确），罗马音存为别名
        if vndb_alttitle and vndb_alttitle != vndb_title:
            display_title = vndb_alttitle
            display_alias = vndb_title
        else:
            display_title = vndb_title
            display_alias = vndb_alttitle or ""

        return {"source_type": "vndb", "source_id": value.get("id", ""), "title": display_title, "alias": display_alias, "cover_url": (value.get("image") or {}).get("url", ""), "description": value.get("description", ""), "developer": ", ".join(x.get("name", "") for x in value.get("developers", [])), "publisher": "", "release_date": release_date, "rating": value.get("rating"), "tags": ", ".join(x.get("name", "") for x in value.get("tags", [])), "series": "", "screenshots": "", "version": ""}


async def search_dlsite(query: str) -> list[dict]:
    from .clients.dlsite_client import DlsiteClient
    return await DlsiteClient().search_games(query, page_size=20)


async def get_dlsite_detail(source_id: str) -> dict:
    from .clients.dlsite_client import DlsiteClient
    return await DlsiteClient().get_game_detail(source_id)


async def search_steam(query: str) -> list[dict]:
    return await SteamClient().search_games(query, page_size=20)


async def get_steam_detail(source_id: str) -> dict:
    return await SteamClient().get_game_detail(source_id)


async def _resolve_primary_source(game) -> tuple[str, str]:
    """为没有数据源 ID 的游戏（如手动入库的 custom）自动搜索一个主来源。
    返回 (source_type, source_id)；全部失败时返回 ("", "")。
    搜索顺序：Steam -> VNDB -> DLsite，均带相似度校验，避免误配。
    """
    title = (game.title or "").strip()
    if not title:
        return "", ""
    probes = [title] + alias_candidates(title)

    # 1) Steam
    try:
        steam_client = SteamClient()
        results = []
        for p in probes:
            results = await steam_client.search_games(p, page_size=20)
            if results:
                break
        if results:
            def _srank(c):
                t = c.get("title", "")
                return max((title_similarity(g, t) for g in probes if g), default=0.0)
            for cand in sorted(results, key=_srank, reverse=True)[:8]:
                if _srank(cand) < 0.5:
                    continue
                try:
                    if await steam_client.get_app_type(cand["source_id"]) == "game":
                        logger.info("刷新自动匹配主来源=steam %s (%s)", cand.get("title"), cand["source_id"])
                        return "steam", cand["source_id"]
                except Exception:
                    continue
    except Exception as e:
        logger.warning("刷新自动匹配 steam 失败：%s", e)

    # 2) VNDB
    try:
        pool = []
        for p in probes:
            try:
                pool += await search_vndb(p)
            except Exception:
                pass
        _seen, uniq = set(), []
        for it in pool:
            sid = it.get("source_id")
            if sid and sid not in _seen:
                _seen.add(sid)
                uniq.append(it)
        if uniq:
            def _vrank(it):
                t, a = it.get("title", ""), it.get("alias", "") or ""
                return max((title_similarity(g, t) for g in probes if g), default=0.0) + \
                       0.8 * max((title_similarity(g, a) for g in probes if g), default=0.0)
            best = max(uniq, key=_vrank)
            if _vrank(best) >= 0.5:
                logger.info("刷新自动匹配主来源=vndb %s (%s)", best.get("title"), best["source_id"])
                return "vndb", best["source_id"]
    except Exception as e:
        logger.warning("刷新自动匹配 vndb 失败：%s", e)

    # 3) DLsite（日系 galgame 兜底）
    try:
        from .clients.dlsite_client import DlsiteClient
        dlsite = DlsiteClient()
        pool = []
        for p in probes:
            try:
                pool += await dlsite.search_games(p, page_size=8)
            except Exception:
                pass
        best, bs = None, 0.0
        for c in pool:
            sc = max((title_similarity(g, c.get("title", "")) for g in probes if g), default=0.0)
            if sc > bs:
                best, bs = c, sc
        if best and bs >= 0.5:
            logger.info("刷新自动匹配主来源=dlsite %s (%s)", best.get("title"), best["source_id"])
            return "dlsite", best["source_id"]
    except Exception as e:
        logger.warning("刷新自动匹配 dlsite 失败：%s", e)

    return "", ""


async def refresh_game_metadata(game_id: int) -> None:
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")
        # 手动入库/无数据源的游戏：自动搜索匹配出主来源后再刷新
        if not game.source_id:
            stype, sid = await _resolve_primary_source(game)
            if not stype:
                raise RuntimeError("未能在各数据源中找到匹配的游戏，请手动指定数据源或修改游戏名")
            game.source_type = stype
            game.source_id = sid
            await session.commit()
        if game.source_type == "rawg":
            metadata = await RawgClient().get_game_detail(game.source_id)
        elif game.source_type == "steam":
            metadata = await get_steam_detail(game.source_id)
        elif game.source_type == "vndb":
            metadata = await get_vndb_detail(game.source_id)
        elif game.source_type == "dlsite":
            metadata = await get_dlsite_detail(game.source_id)
        else:
            raise RuntimeError("当前数据源不支持刷新元数据")
        # 翻译前保存不参与翻译但需要保留的字段
        _preserved = {k: metadata.get(k) for k in ("english_name", "cover_url", "release_date", "rating", "version") if metadata.get(k) is not None}
        raw_metadata = dict(metadata)  # 翻译前快照：source_data 统一存原文，供标签重翻译对齐
        metadata = await translation_service.translate_metadata(session, metadata)
        # 翻译后合并回保留字段（翻译服务可能丢失这些字段）
        metadata.update(_preserved)
        metadata["release_date"] = normalize_release_date(metadata.get("release_date"))
        for field in ("title", "alias", "description", "developer", "publisher", "release_date", "rating", "tags", "series", "source_type", "source_id", "screenshots", "version", "original_data", "steam_appid"):
            if field in metadata:
                setattr(game, field, metadata[field])
        game.cover_url = metadata.get("cover_url", "")
        await session.commit()
        try:
            game.cover_url = await cache_cover(game.id, metadata.get("cover_url", ""), game.source_type)
            game.cover_source = game.source_type
            await session.commit()
        except Exception:
            await session.rollback()

        # 多来源刷新：遍历 source_ids 中其他来源，逐个更新 source_data
        try:
            source_ids = json.loads(game.source_ids or "{}")
        except json.JSONDecodeError:
            source_ids = {}
        try:
            source_data = json.loads(game.source_data or "{}")
        except json.JSONDecodeError:
            source_data = {}
        # 主来源数据也存入 source_data（存翻译前原文，与其他来源行为一致）
        if game.source_type and game.source_type != "custom":
            source_data[game.source_type] = {k: v for k, v in raw_metadata.items() if k not in ("resource_type", "resource_url", "play_status", "original_data")}
            source_ids[game.source_type] = game.source_id
        # 刷新其他已有来源
        for src, sid in list(source_ids.items()):
            if src == game.source_type or not sid:
                continue
            try:
                if src == "rawg":
                    src_meta = await RawgClient().get_game_detail(sid)
                elif src == "steam":
                    src_meta = await get_steam_detail(sid)
                elif src == "vndb":
                    src_meta = await get_vndb_detail(sid)
                elif src == "dlsite":
                    src_meta = await get_dlsite_detail(sid)
                else:
                    continue
                source_data[src] = src_meta
                logger.info("多来源刷新：game_id=%s, source=%s 成功", game.id, src)
            except Exception as e:
                logger.warning("多来源刷新：game_id=%s, source=%s 失败: %s", game.id, src, e)
        # 尝试补充新来源（搜索匹配）
        for src in ["steam", "rawg", "vndb", "dlsite"]:
            if src in source_ids and source_ids[src]:
                continue
            try:
                if src == "steam":
                    results = []
                    # 搜索顺序：先精确标题（含英文原名），再别名候选（按精确->宽泛）
                    probes = []
                    eng_name = (source_data.get("rawg") or {}).get("english_name", "")
                    if eng_name:
                        probes.append(eng_name)
                    if game.title:
                        probes.append(game.title)
                    probes.extend(alias_candidates(game.title))
                    seen_probe = set()
                    for _probe in probes:
                        if not _probe or _probe.lower() in seen_probe:
                            continue
                        seen_probe.add(_probe.lower())
                        results = await search_steam(_probe)
                        if results:
                            break
                    # 候选按与原始标题/英文名的相似度降序，避免顺位拿到 DLC/资料片
                    rank_group = [eng_name, game.title] + alias_candidates(game.title)

                    def _steam_rank(cand):
                        t = cand.get("title", "")
                        return max(
                            title_similarity(game.title, t),
                            title_similarity(eng_name, t) if eng_name else 0.0,
                            max((title_similarity(g, t) for g in rank_group), default=0.0),
                        )

                    ordered = sorted(results, key=_steam_rank, reverse=True)
                    picked = None
                    # storesearch 对 DLC/原声集也返回 type=app，需 appdetails 校验 type=="game"
                    for candidate in ordered[:8]:
                        try:
                            if await SteamClient().get_app_type(candidate["source_id"]) == "game":
                                picked = candidate
                                break
                        except Exception:
                            continue
                    if picked is None and ordered:
                        # 相似度太低直接放弃，避免乱配（如 "sandbox" -> 无关游戏）
                        if _steam_rank(ordered[0]) >= 0.4:
                            picked = ordered[0]
                    if picked:
                        source_ids["steam"] = picked["source_id"]
                        source_data["steam"] = await get_steam_detail(picked["source_id"])
                elif src == "rawg":
                    rawg_client = RawgClient()
                    def _filter_main(results):
                        return [r for r in results if not re.search(r"(?i)(typing|dlc|demo|trial|soundtrack|ost|art pack)", r.get("title", ""))]
                    # 优先用 steam 英文名搜索（更准确）
                    steam_eng = ""
                    if isinstance(source_data.get("steam"), dict):
                        steam_eng = source_data["steam"].get("english_name", "")
                    elif "steam" in source_data:
                        try:
                            steam_eng = json.loads(source_data["steam"]).get("english_name", "") if isinstance(source_data["steam"], str) else ""
                        except:
                            pass
                    results = []
                    if steam_eng:
                        try:
                            eng_results = _filter_main(await rawg_client.search_games(steam_eng, page_size=5))
                            if eng_results:
                                results = eng_results
                                logger.info("多来源刷新：game_id=%s, 通过 steam 英文名 '%s' 搜索到 rawg=%s", game.id, steam_eng, eng_results[0]["source_id"])
                        except Exception as e:
                            logger.warning("多来源刷新：game_id=%s, steam 英文名搜索 rawg 失败: %s", game.id, e)
                    # 中文别名映射的英文名候选（帝国时代 -> Age of Empires）
                    if not results:
                        for _alias in alias_candidates(game.title):
                            results = _filter_main(await rawg_client.search_games(_alias, page_size=5))
                            if results:
                                logger.info("多来源刷新：game_id=%s, 别名 '%s' 搜索到 rawg=%s", game.id, _alias, results[0]["source_id"])
                                break
                    # 仍没找到时，用中文名搜索作为 fallback
                    if not results:
                        results = _filter_main(await rawg_client.search_games(game.title, page_size=5))
                    if results:
                        # 相似度排序：优先采纳标题最接近的，避免顺位误配其他作品
                        rank_group = [steam_eng, game.title] + alias_candidates(game.title)
                        def _rawg_rank(cand):
                            t = cand.get("title", "")
                            return max((title_similarity(g, t) for g in rank_group if g), default=0.0)
                        best = max(results, key=_rawg_rank)
                        if _rawg_rank(best) >= 0.4:
                            source_ids["rawg"] = best["source_id"]
                            source_data["rawg"] = await rawg_client.get_game_detail(best["source_id"])
                elif src == "vndb":
                    # 搜索候选池：原名 + 别名候选，全部合并后统一打分
                    vndb_pool = await search_vndb(game.title)
                    for _alias in alias_candidates(game.title):
                        try:
                            vndb_pool = vndb_pool + await search_vndb(_alias)
                        except Exception:
                            pass
                    # 去重
                    _seen_v, vndb_pool = set(), []
                    for item in vndb_pool:
                        sid = item.get("source_id")
                        if sid and sid not in _seen_v:
                            _seen_v.add(sid)
                            vndb_pool.append(item)
                    if vndb_pool:
                        rank_group = [game.title] + alias_candidates(game.title)
                        def _vndb_rank(item):
                            t = item.get("title", "")
                            a = item.get("alias", "") or ""
                            return max((title_similarity(g, t) for g in rank_group if g), default=0.0) * 1.0 + \
                                   max((title_similarity(g, a) for g in rank_group if g), default=0.0) * 0.8
                        best = max(vndb_pool, key=_vndb_rank)
                        # 相似度校验：标题差太远则不采纳，避免乱配（如 "sandbox" -> 无关作品）
                        if _vndb_rank(best) >= 0.5:
                            source_ids["vndb"] = best["source_id"]
                            source_data["vndb"] = await get_vndb_detail(best["source_id"])
                elif src == "dlsite":
                    from .clients.dlsite_client import DlsiteClient
                    dlsite = DlsiteClient()
                    # 1) RJ/VJ/BJ 编号精准查询（标题或路径里可直接提取）
                    workno = dlsite.extract_workno(game.title) or dlsite.extract_workno(game.resource_url or "")
                    if not workno:
                        # 2) 按标题/别名搜索 DLsite（日系 galgame 在 Steam/VNDB 常查不到）
                        ds_query = []
                        for q in [game.title] + alias_candidates(game.title):
                            if q and q not in ds_query:
                                ds_query.append(q)
                        ds_best, ds_best_score = None, 0.0
                        for q in ds_query[:3]:
                            try:
                                cands = await dlsite.search_games(q, page_size=8)
                            except Exception:
                                cands = []
                            for c in cands:
                                score = title_similarity(game.title, c.get("title", ""))
                                if score > ds_best_score:
                                    ds_best, ds_best_score = c, score
                            if ds_best_score >= 0.6:
                                break
                        # 相似度达标才采纳，避免乱配
                        if ds_best and ds_best_score >= 0.5:
                            workno = ds_best["source_id"]
                    if workno:
                        source_ids["dlsite"] = workno
                        source_data["dlsite"] = await get_dlsite_detail(workno)
                if src in source_ids and source_ids[src]:
                    logger.info("多来源刷新：game_id=%s, 补充新来源 %s=%s", game.id, src, source_ids[src])
            except Exception as e:
                logger.warning("多来源刷新：game_id=%s, 补充来源 %s 失败: %s", game.id, src, e)
        game.source_ids = json.dumps(source_ids, ensure_ascii=False, default=str)
        game.source_data = json.dumps(source_data, ensure_ascii=False, default=str)
        await session.commit()


async def refresh_vndb_game_metadata(game_id: int) -> None:
    """按当前标题重新搜索 VNDB，并用首个结果的详情刷新游戏元数据。"""
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")

        # 已有 source_id 时精确查询（按标题搜索首结果可能错配，如 'your diary' 搜到 v4470）
        source_id = game.source_id or ""
        if not source_id:
            results = await search_vndb(game.title)
            if not results:
                raise RuntimeError("VNDB 未找到匹配的游戏")
            source_id = results[0].get("source_id", "")
            if not source_id:
                raise RuntimeError("VNDB 搜索结果缺少数据源 ID")

        metadata = await get_vndb_detail(source_id)
        metadata = await translation_service.translate_metadata(session, metadata)
        metadata["release_date"] = normalize_release_date(metadata.get("release_date"))
        for field in (
            "title", "alias", "description", "developer", "publisher", "release_date",
            "rating", "tags", "series", "source_type", "source_id", "screenshots", "version",
            "original_data",
        ):
            if field in metadata:
                setattr(game, field, metadata[field])
        game.cover_url = metadata.get("cover_url", "")
        await session.commit()

        try:
            game.cover_url = await cache_cover(game.id, metadata.get("cover_url", ""), "vndb")
            game.cover_source = "vndb"
            await session.commit()
        except Exception:
            await session.rollback()


async def refresh_rawg_game_metadata(game_id: int) -> None:
    """仅访问 RAWG、SQLite 和 /app/data 封面缓存，绝不访问 WebDAV。"""
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")
        if game.source_type != "rawg" or not game.source_id:
            raise RuntimeError("仅支持刷新具有 RAWG 数据源 ID 的游戏")

        metadata = await RawgClient().get_game_detail(game.source_id)
        metadata = await translation_service.translate_metadata(session, metadata)
        metadata["release_date"] = normalize_release_date(metadata.get("release_date"))
        for field in (
            "title", "alias", "description", "developer", "publisher", "release_date",
            "rating", "tags", "series", "source_type", "source_id", "screenshots", "version", "original_data",
            "steam_appid",
        ):
            if field in metadata:
                setattr(game, field, metadata[field])
        # 先保存远程 URL；封面 CDN 临时失败不应使元数据刷新失败。
        game.cover_url = metadata["cover_url"]
        await session.commit()

        steam_url = ""
        if metadata.get("steam_appid"):
            steam_url = RawgClient.steam_cover_url(metadata["steam_appid"])
        cover_url = steam_url or metadata["cover_url"]
        cover_source = "steam" if steam_url else "rawg"
        try:
            game.cover_url = await cache_cover(game.id, cover_url, cover_source)
            game.cover_source = cover_source
            await session.commit()
        except Exception:
            await session.rollback()
