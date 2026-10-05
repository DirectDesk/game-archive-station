from datetime import date
import asyncio
import difflib
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



# ==================== 游戏平台类型推断 ====================
# 与「数据来源 source_type」彻底解耦：source_type 表示元数据从哪个站抓的，
# game_type 表示这个游戏实际跑在什么平台，多选（pc/android/gal）。

_GAL_TAG_KEYWORDS = (
    "visual novel", "视觉小说", "アドベンチャー", "adventure game",
    "galgame", "ギャルゲー", "エロゲ", "eroge", "美少女", "恋爱", "恋愛",
    "adv", "ノベル", "novel", "bishoujo", "dating sim", "乙女", "otome",
)
_GAL_SOURCES = {"vndb", "dlsite"}


# ==================== 跨源匹配采纳门槛 ====================
# 历史问题：AI Shoujo 被 VNDB 的 "Shoujo Settai"（相似度 0.5）抢占主来源，
# 而 Steam/RAWG 明明有 100% 命中的记录。根因是阈值太低 + 来源优先级不合理。
# 现在统一为：自动匹配必须达到 MATCH_ACCEPT（0.7）才采纳；
# MATCH_STRONG（0.85）以上视为高置信命中，可跳过后续来源尝试。
MATCH_ACCEPT = 0.70
MATCH_STRONG = 0.85

# 各来源的单条候选采纳门槛（DLsite 标题风格差异大，略放宽）
_MATCH_THRESHOLD = {
    "steam": MATCH_ACCEPT,
    "rawg": MATCH_ACCEPT,
    "vndb": MATCH_ACCEPT,
    "dlsite": 0.62,
}


# 资源路径探测缓存：避免批量重算时反复 stat WebDAV 挂载点
_APK_PROBE_CACHE: dict[str, bool] = {}
_APK_PROBE_MAX_DEPTH = 2
_APK_PROBE_MAX_ENTRIES = 400


def path_has_apk(resource_url: str | None) -> bool:
    """探测资源路径是否含 .apk 文件（判断安卓游戏）。

    - 路径本身是 .apk 文件 -> True
    - 路径是目录：浅层（<=2 层，最多 400 个条目）查找 .apk
    - 探测结果按路径缓存；异常一律返回 False（不影响主流程）
    """
    if not resource_url:
        return False
    key = resource_url.rstrip("/")
    if key in _APK_PROBE_CACHE:
        return _APK_PROBE_CACHE[key]
    result = False
    try:
        import os as _os
        from pathlib import Path as _P

        p = _P(key)
        if p.is_file():
            result = p.suffix.lower() == ".apk"
        elif p.is_dir():
            entries = 0
            base_depth = len(p.parts)
            for root, dirs, files in _os.walk(key):
                # 深度控制：只探浅层，避免大目录全量遍历拖慢 WebDAV
                if len(_P(root).parts) - base_depth >= _APK_PROBE_MAX_DEPTH:
                    dirs[:] = []
                for fn in files:
                    entries += 1
                    if entries > _APK_PROBE_MAX_ENTRIES:
                        dirs[:] = []
                        break
                    if fn.lower().endswith(".apk"):
                        result = True
                        break
                if result or entries > _APK_PROBE_MAX_ENTRIES:
                    break
    except Exception:
        result = False
    _APK_PROBE_CACHE[key] = result
    return result


def clear_apk_probe_cache() -> None:
    _APK_PROBE_CACHE.clear()


def infer_game_type(game: "Game", source_data: dict | None = None) -> list[str]:
    """按数据源与标签推断游戏平台类型，返回 ['pc','android','gal'] 的子集（有序）。

    规则（与产品确认）：
      - 匹配到 vndb / dlsite 来源，或标签含 gal 关键词 -> gal
      - 匹配到 steam / rawg 来源 -> pc
      - 标签含 android/安卓/手机/移植 关键词 -> android
    未命中任何来源时默认 pc（PC 是绝大多数情况）。
    """
    types: list[str] = []
    try:
        source_ids = json.loads(game.source_ids or "{}")
    except Exception:
        source_ids = {}
    sources = {k for k, v in source_ids.items() if v}
    if not sources and game.source_type and game.source_type != "custom":
        sources.add(game.source_type)

    # 聚合所有来源的标签（原文 + 译文都看，提高命中率）
    tag_blob = (game.tags or "").lower()
    if isinstance(source_data, dict):
        for v in source_data.values():
            if isinstance(v, dict) and v.get("tags"):
                tag_blob += " " + str(v["tags"]).lower()

    if sources & _GAL_SOURCES:
        types.append("gal")
    elif any(kw in tag_blob for kw in _GAL_TAG_KEYWORDS):
        types.append("gal")

    # 磁盘证据优先：资源路径实际含 .apk（探到浅层文件）是比"来源站"更强的信号。
    # 安卓移植版同样会在 Steam/RAWG 建页，仅凭来源加 pc 会把安卓游戏误标成 pc。
    _path_android = path_has_apk(getattr(game, "resource_url", None))

    if sources & {"steam", "rawg"} and not _path_android:
        types.append("pc")

    # 标签关键词命中，或资源路径实际含 .apk 文件 -> android
    _tag_android = any(kw in tag_blob for kw in ("android", "安卓", "手机版", "手游", "apk", "移植"))
    if _tag_android or _path_android:
        types.append("android")

    if not types:
        types.append("pc")
    # 去重保序
    seen, out = set(), []
    for t in types:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def game_type_str(game: "Game", source_data: dict | None = None) -> str:
    return ",".join(infer_game_type(game, source_data))


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


async def resolve_cover(game: Game, config: SystemConfig, requested_source: str = "") -> tuple[str, str]:
    """按配置优先级（cover_source_priority）从已收集的各来源封面中选一个。

    返回 (cover_url, source)；无可用封面时返回 ("", "")。

    设计说明（v1.6.4 修复）：
      旧逻辑里 game.cover_source 直接等于 game.source_type（主来源），
      导致"封面数据源"这个设置项在扫描/刷新流程中完全失效——
      即便配置了 steam 优先，只要主来源是 rawg，封面就永远是 rawg 的。
      本函数与 resolve_tags 对称，统一从 source_data 里按优先级取封面。

    steam 特殊处理：
      Steam 封面 URL 可由 appid 直接构造（library_600x900.jpg），
      即使 source_data 里没存 steam 封面（如仅从 rawg 反查到 appid），
      只要拿到 appid 就优先用 Steam 封面——这正是"steam 封面优先"的预期行为。
    """
    try:
        priority = json.loads(config.cover_source_priority or "[]") if config else []
    except (TypeError, json.JSONDecodeError):
        priority = []
    if not priority:
        priority = ["steam", "vndb", "dlsite", "rawg"]
    if requested_source:
        priority = [requested_source]

    try:
        source_data = json.loads(game.source_data or "{}")
    except (TypeError, json.JSONDecodeError):
        source_data = {}
    if not isinstance(source_data, dict):
        source_data = {}

    # 可用来源 = source_data 里已匹配的来源
    available = set(source_data.keys())
    # Steam 额外放行：只要游戏持有 steam_appid 就可用 Steam 封面。
    # 例：孢子(id=22) 由 rawg 匹配，source_data 只有 rawg，
    #     但 rawg 详情返回了 steam_appid=17390，此时仍应用 Steam 封面。
    #     若要求 source_data 必须有 steam 条目，会白白丢掉"steam 优先"。
    _has_steam_appid = bool(str(game.steam_appid or "").strip())

    for source in priority:
        if source not in available and not (source == "steam" and _has_steam_appid):
            continue
        url = ""
        meta = source_data.get(source)
        if isinstance(meta, dict):
            url = meta.get("cover_url", "") or ""
        # steam：无封面 URL 时按 appid 构造
        if not url and source == "steam":
            appid = ""
            if isinstance(meta, dict):
                appid = meta.get("steam_appid") or meta.get("source_id") or ""
            if not appid:
                appid = str(game.steam_appid or "")
            if appid:
                url = RawgClient.steam_cover_url(appid)
        if url:
            return url, source

    return "", ""


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
    # 中文常见译名补充（用户库里实际出现的）
    "主播女孩重度依赖": "NEEDY GIRL OVERDOSE",
    "主播女孩重度依赖症": "NEEDY GIRL OVERDOSE",
    "命运/留夜": "Fate/stay night",
    "命运留夜": "Fate/stay night",
    "命运之夜": "Fate/stay night",
    "暑假学校": "School in Summer Vacation",
    "美少女万华镜": "Bishoujo Mangekyou",
    "美少女万華鏡": "Bishoujo Mangekyou",
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


def _to_halfwidth(text: str) -> str:
    """全角字符转半角（ＡＢＣ１２３＊！ -> ABC123*!），并统一常见特殊符号。

    这是跨源匹配的关键：AI＊少女 / ＡＩ少女 / AI*少女 应当归一为同一串。
    """
    if not text:
        return ""
    out = []
    for ch in text:
        code = ord(ch)
        # 全角 ASCII 区（！ to ～）映射到半角
        if 0xFF01 <= code <= 0xFF5E:
            out.append(chr(code - 0xFEE0))
        elif code == 0x3000:          # 全角空格
            out.append(" ")
        else:
            out.append(ch)
    value = "".join(out)
    # 各类星号/中点/连接符统一
    value = re.sub(r"[＊*✳✱✲✴☆★·・‧∙⋅]", "*", value)
    # 波浪线统一
    value = re.sub(r"[〜～〰︴]", "~", value)
    # 弯引号统一
    value = re.sub(r"[‘’“”]", "'", value)
    return value


def normalize_core(text: str) -> str:
    """提取标题核心：全角转半角、去副标题、去括号内容、去年份、去版本号。"""
    if not text:
        return ""
    value = _to_halfwidth(text)
    # 去掉方括号/圆括号内容（发布组、年份、汉化组等）
    value = re.sub(r"\[[^\]]*\]", " ", value)
    value = re.sub(r"[\(（][^\)）]*[\)）]", " ", value)
    # 剥离发布组前缀：3DMGAME-X / CODEX-X / [组名]X / "3DMGAME_X"
    # 注意要在副标题切分**之前**做，否则 "3DMGAME-Senran.Kagura" 会被 '-' 切成只剩 "3dmgame"
    _stripped = value.strip()
    for _grp in _RELEASE_GROUP_PREFIXES:
        m = re.match(r"(?i)^" + re.escape(_grp) + r"\s*[-_–—:：.]+\s*", _stripped)
        if m:
            _rest = _stripped[m.end():].strip()
            if len(_rest) >= 3:          # 剥完不能为空/过短
                _stripped = _rest
                break
    # "3DMGAME_X"（下划线分隔无空格）也处理一次
    _stripped = re.sub(
        r"(?i)^(" + "|".join(re.escape(g) for g in _RELEASE_GROUP_PREFIXES) + r")[_]+",
        "",
        _stripped,
    ).strip()
    value = _stripped
    # 去掉年份/编号样式（[111229]、20141230）
    value = re.sub(r"\b\d{6,8}\b", " ", value)
    value = _strip_subtitle(value)
    # 去版本号
    value = re.sub(r"(?i)\bv\d+(?:\.\d+){1,3}\b", " ", value)
    value = re.sub(r"[._]+", " ", value)
    # '*' 视作分隔符（AI*Shoujo -> "ai shoujo"），保留两侧词
    value = value.replace("*", " ")
    return re.sub(r"\s+", " ", value).strip().lower()


def _strip_subtitle(value: str) -> str:
    """切掉副标题（冒号/破折号/波浪号/中点），但保留仍有 >=2 词元的主标题。
    否则 "AI: Rampage" 会被切成 "ai"，使 AI＊少女 与 AI: Rampage 误判。
    """
    _parts = re.split(r"[:：\-—–~]|(?<=\w)\s+[·・]\s*(?=\w)", value, maxsplit=1)
    if len(_parts) > 1:
        _head = _parts[0].strip()
        _head_tokens = [t for t in re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", _head.lower()).split() if t]
        if len(_head_tokens) >= 2:
            return _parts[0]
    return value


def normalize_core_full(text: str) -> str:
    """与 normalize_core 相同的清洗，但**不切副标题**（用于取较高相似度）。"""
    if not text:
        return ""
    value = _to_halfwidth(text)
    value = re.sub(r"\[[^\]]*\]", " ", value)
    value = re.sub(r"[\(（][^\)）]*[\)）]", " ", value)
    _stripped = value.strip()
    for _grp in _RELEASE_GROUP_PREFIXES:
        m = re.match(r"(?i)^" + re.escape(_grp) + r"\s*[-_–—:：.]+\s*", _stripped)
        if m:
            _rest = _stripped[m.end():].strip()
            if len(_rest) >= 3:
                _stripped = _rest
                break
    _stripped = re.sub(
        r"(?i)^(" + "|".join(re.escape(g) for g in _RELEASE_GROUP_PREFIXES) + r")[_]+",
        "",
        _stripped,
    ).strip()
    value = _stripped
    value = re.sub(r"\b\d{6,8}\b", " ", value)
    # 不切副标题：把分隔符替换为空格（保留两侧词）
    value = re.sub(r"[:：\-—–~·・]", " ", value)
    value = re.sub(r"(?i)\bv\d+(?:\.\d+){1,3}\b", " ", value)
    value = re.sub(r"[._]+", " ", value)
    value = value.replace("*", " ")
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

# 常见资源发布组 / 破解组前缀（归一化前先剥掉，否则 "3DMGAME-A.B" 会被当成副标题切掉真名）
_RELEASE_GROUP_PREFIXES = (
    "3dmgame", "3dm", "codex", "fitgirl", "reloaded", "skidrow", "plaza", "empress",
    "cpy", "razor1911", "rune", "tenoke", "goldberg", "elamigos", "darksiders",
    "hoodlum", "kaos", "flt", "prophet", "steamunlocked", "gog", "epic",
    "galgame", "kagura", "kagura games", "steam", "dl", "dlsite",
)

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


def build_probes(title: str, extra: list[str] | None = None, limit: int = 6) -> list[str]:
    """构造跨源搜索探针（按优先级：额外英文名 > 归一化标题 > 原标题 > 别名候选）。

    关键改进：把 `normalize_core(title)` 的结果也纳入探针。
    历史问题：`3DMGAME-Senran.Kagura.Shinovi.Versus.CHS.Repack-3DM` 这种资源站
    文件名直接拿去搜 Steam 会返回 0 条，而归一化后的 `senran kagura shinovi versus`
    能正常搜到。同时把点号还原成空格（`A.B.C` -> `A B C`）。
    """
    out: list[str] = []

    def _push(value):
        if not value:
            return
        v = str(value).strip()
        if len(v) < 2:
            return
        # 清理后必须还剩至少一个"实义词元"：
        # 过滤 'Rance - -'、'- -' 这类仅由分隔符/残留符号构成的探针，
        # 它们会被归一化折叠成单个泛称词，命中整个系列（如 Rance 全系）。
        if not re.search(r"[A-Za-z0-9\u4e00-\u9fff]", v):
            return
        if len([t for t in re.split(r"[^A-Za-z0-9\u4e00-\u9fff]+", v) if t]) < 1:
            return
        if v.lower() not in {x.lower() for x in out}:
            out.append(v)

    for e in (extra or []):
        _push(e)
        # extra（多为英文名）也补一个归一化版本：
        # 如 'Little Nightmares II - DEMO' -> 'little nightmares ii'
        _e_norm = normalize_core(e)
        if _e_norm and _e_norm != str(e).strip().lower():
            _push(_e_norm)

    normalized = normalize_core(title)
    if normalized:
        _push(normalized)
        # 点号在归一化时已被吃掉，这里补一个"点号转空格"的变体
        _push(re.sub(r"[._]+", " ", str(title)).strip())

    _push(title)
    for a in alias_candidates(title):
        _push(a)
        if len(out) >= limit:
            break
    return out[:limit]


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
    """标题相似度（0~1），用于跨源匹配候选打分。

    设计要点（修复历史误配）：
      1. 双向包含：query 是结果标题一部分（或反之）时给高分，但**受长度比约束**，
         避免 "AI Shoujo" 命中 "AI: Rampage" 这类只共享短前缀的情况。
      2. 词元重叠：按 query 词元被覆盖的比例算，但对**词元数少的短标题**做惩罚，
         因为短标题（1~2 个词）的覆盖率指标极不稳定。
      3. 字符级相似：用 difflib 序列相似度兜底，衡量整串的接近程度。
      4. 最终取「包含分 / 词元分」的较大者，再与字符分做加权融合，
         并乘上长度比惩罚系数。
      5. 序号一致性：系列作序号不同时大幅降分（Sexy Beach 4 vs Sexy Beaches 2）。
    """
    if not query or not result_title:
        return 0.0

    # 结果标题可能是多别名写法（Steam 用 "/" 分隔，如 "AI*Shoujo/AI*少女"）。
    # 对每个别名段分别打分，取最高值——这才是"该来源是否收录了这款游戏"的正确语义。
    raw_r = _to_halfwidth(str(result_title))
    raw_q = _to_halfwidth(str(query))
    r_segs = [s for s in re.split(r"[/|｜]", raw_r) if s.strip()] or [raw_r]
    q_segs = [s for s in re.split(r"[/|｜]", raw_q) if s.strip()] or [raw_q]
    # 任一多段写法（query 或 result）都需按别名逐段比对取最高：
    #   * result 侧多段（Steam 常见 "AI*Shoujo/AI*少女"）：比较"该来源是否收录此作"；
    #   * query 侧多段（探针可能带合并别名）：防止 "/" 被当成普通字符参与整体打分，
    #     否则 "AI＊Shoujo/AI＊少女" 会与 "Mahou Shoujo Ai" 共享 {ai,shoujo} 而虚高。
    if len(r_segs) > 1 or len(q_segs) > 1:
        best = 0.0
        for _qs in q_segs:
            for _rs in r_segs:
                s = _title_similarity_single(_qs, _rs)
                if s > best:
                    best = s
        return round(min(1.0, best), 4)

    return _title_similarity_single(query, result_title)


def _cjk_char_overlap(a: str, b: str) -> float:
    """CJK 字符集合的 Jaccard 相似度，用于中日文标题的部分命中兜底。

    "ai 少女" vs "ai shoujo ai 少女" -> 共享 {ai, 少女} 中的 "少女"，
    而 "ai少女" vs "ai shoujo ai 少女" -> 共享 "ai" 与 "少女" 两个 CJK/拉丁块。
    """
    def blocks(text):
        # 把连续 CJK 视为一个块，连续拉丁数字视为一个块
        return set(re.findall(r"[\u4e00-\u9fff]+|[a-z0-9]+", text.lower()))

    ba, bb = blocks(a), blocks(b)
    if not ba or not bb:
        return 0.0
    inter = ba & bb
    if not inter:
        return 0.0
    # 以较短一侧为基准：短串的块被长串覆盖的比例
    return len(inter) / len(ba) if len(ba) <= len(bb) else len(inter) / len(bb)


def _title_similarity_single(query: str, result_title: str) -> float:
    """单段标题相似度（内部实现，不含多别名分段）。"""
    _a = _title_similarity_single_norm(query, result_title, normalize_core)
    _b = _title_similarity_single_norm(query, result_title, normalize_core_full)
    return max(_a, _b)


def _title_similarity_single_norm(query: str, result_title: str, norm_fn) -> float:
    q, r = norm_fn(query), norm_fn(result_title)
    if not q or not r:
        return 0.0

    # 词元分割：拉丁/数字/CJK 汉字/**日文假名** 均视为词元字符，
    # 否则日文标题（如 '呪われし伝説の少女'）会被拆成孤立汉字，
    # 导致 "少女" 这类通用字造成假性重叠（历史缺陷）。
    def tokens(text):
        return set(re.sub(r"[^a-z0-9\u4e00-\u9fff\u3040-\u30ff]+", " ", text.lower()).split())

    q_tokens, r_tokens = tokens(q), tokens(r)
    if not q_tokens or not r_tokens:
        return 0.0

    # --- 词元覆盖率（对称，取较严格的一侧）---
    cover_q = len(q_tokens & r_tokens) / len(q_tokens)
    cover_r = len(q_tokens & r_tokens) / len(r_tokens)
    token_score = min(cover_q, cover_r) if len(q_tokens) > 1 else cover_q

    # --- 双向包含（带长度比约束）---
    # **按词元序列判定**，而非裸字符子串：字符子串会跨词边界误判。
    # 反例（修复）：'ai shoujo' 是 'keitai shoujo' 的字符子串（keit[ai shoujo]），
    #   但词元序列 [ai, shoujo] 不是 [keitai, shoujo] 的连续子序列 —— 二者无关，
    #   旧逻辑给出 0.9 造成 vndb 误配「ケータイ少女」。
    contain_score = 0.0
    _q_segs = q.split()
    _r_segs = r.split()
    _contain_dir = 0  # 1: q 被 r 包含；2: r 被 q 包含
    if _q_segs and _r_segs:
        def _is_consecutive_sub(short_segs, long_segs):
            n = len(short_segs)
            if n == 0 or n > len(long_segs):
                return False
            for _i in range(len(long_segs) - n + 1):
                if long_segs[_i:_i + n] == short_segs:
                    return True
            return False
        if len(_q_segs) <= len(_r_segs) and _is_consecutive_sub(_q_segs, _r_segs):
            _contain_dir = 1
        elif len(_r_segs) < len(_q_segs) and _is_consecutive_sub(_r_segs, _q_segs):
            _contain_dir = 2
        # 单 token 且短：允许字符级子串（如 "carrion" in "carrion fields" 已由前缀逻辑处理）
        if _contain_dir == 0 and len(_q_segs) == 1 and len(_r_segs) == 1:
            if q in r or r in q:
                _contain_dir = 1 if len(q) <= len(r) else 2
    if _contain_dir:
        shorter, longer = (q, r) if _contain_dir == 1 else (r, q)
        ratio = len(shorter) / max(len(longer), 1)
        # 长度比太悬殊（如 "ai" 包含于超长标题）时，包含关系不具说服力
        if ratio >= 0.6:
            contain_score = 0.9
        elif ratio >= 0.4:
            contain_score = 0.7
        else:
            contain_score = 0.45

    # --- 字符级相似（整串）---
    char_score = difflib.SequenceMatcher(None, q, r).ratio()

    base = max(contain_score, token_score)
    # 融合字符级相似：整串接近时明显加分
    score = max(base, base * 0.65 + char_score * 0.35)
    if char_score >= 0.85:
        score = max(score, char_score)

    # --- 短标题惩罚：query 词元 <=2 且结果词元偏多时，覆盖率不可信 ---
    if len(q_tokens) <= 2 and len(r_tokens) >= 3:
        score *= 0.75
    if len(q_tokens) == 1 and len(r_tokens) >= 2:
        score *= 0.8

    # --- CJK 块重叠兜底（跨语言）---
    # 本兜底专门用于「CJK 标题 ↔ 拉丁转写标题」的部分命中：
    # 例："ai 少女" vs "ai shoujo ai 少女" —— 拉丁侧不相交，但 CJK 块 "少女" 重合。
    # **仅当两侧的 CJK 块存在实质交集时启用**：
    #   * 若任一侧含 CJK 就整串启用（旧逻辑），会把 "AI＊Shoujo/AI＊少女"
    #     与 "Mahou Shoujo Ai" 这类**仅共享拉丁词**的不同作品抬到 0.82（误配）；
    #   * 纯拉丁标题的共同词本已由 token_score 正确度量，无需兜底。
    if re.search(r"[\u4e00-\u9fff]", q) or re.search(r"[\u4e00-\u9fff]", r):
        cjk = _cjk_char_overlap(q, r)
        if cjk > 0:
            # 至少两个块重合才算有说服力（单块重合容易误伤，如都含 "ai"）
            shared = len(set(re.findall(r"[\u4e00-\u9fff]+|[a-z0-9]+", q)) &
                         set(re.findall(r"[\u4e00-\u9fff]+|[a-z0-9]+", r)))
            if shared >= 2:
                score = max(score, 0.55 + 0.4 * cjk)
            elif shared == 1 and len(q) >= 4:
                score = max(score, 0.5 * cjk)

    # --- 序号一致性 ---
    # 数字相同的系列作（"尾行3" vs "Biko 3"）是**正面**信号，不应降分；
    # 数字不同（"Sexy Beach 4" vs "Sexy Beaches 2"）则是不同作品，重罚。
    q_nums, r_nums = _series_numbers(query), _series_numbers(result_title)
    if q_nums and r_nums and q_nums != r_nums:
        score = min(score, 0.25)
    elif q_nums and not r_nums:
        score = min(score, 0.35)

    # --- 纯数字巧合惩罚 ---
    # 只靠一个共同数字（"尾行3" vs "Doom 3"）不该得高分；
    # 但跨语言别名映射（"尾行3" -> "Biko 3"）本来就没有共同词，
    # 这种情况**由调用方的别名探针负责命中**，此处只做温和压制，
    # 避免把 0.5 级的"同号不同作"抬到门槛之上。
    def _strip_nums(text):
        return re.sub(r"\b\d{1,2}\b", " ", text).strip()

    q_text, r_text = _strip_nums(q), _strip_nums(r)
    if q_text and r_text:
        q_blocks = set(re.findall(r"[\u4e00-\u9fff]+|[a-z]+", q_text))
        r_blocks = set(re.findall(r"[\u4e00-\u9fff]+|[a-z]+", r_text))
        if q_blocks and r_blocks and not (q_blocks & r_blocks):
            # 去掉数字后毫无共同词 —— 典型的"数字巧合"。
            # 分两档：
            #   * 只有一侧带数字（"Biko" vs "Arma 3"）：噪声可能性最高，压到 0.3；
            #   * 两侧都带且相同数字（"尾行3" vs "Biko 3" 这类同号但不是同一作的）：
            #     压到 0.5。注意真正的别名命中（探针 "Biko 3"）会走另一条路径得 1.0，
            #     不受此处影响。
            if not (q_nums and r_nums and q_nums == r_nums):
                score = min(score, 0.3)
            else:
                score = min(score, 0.5)

    # --- 衍生作保护（硬上限，置于所有加分逻辑之后）---
    # 归一化后完全相等，但**原始标题**长度差异悬殊时，视为「本体 vs 本体:副标题」
    # 的衍生作关系（副标题在归一化时被切掉，导致两者归一化结果相同）。
    # 例：'NEEDY STREAMER OVERLOAD' vs 'NEEDY STREAMER OVERLOAD: Typing of The Net'
    # 反例（不触发）：'Little Nightmares II' vs 'Little Nightmares II - DEMO'
    #   长度比 20/25 = 0.8 >= 0.7，保持高分。
    if q == r:
        _sh, _lo = sorted([query, result_title], key=len)   # 升序：[短, 长]
        if len(_sh) / max(len(_lo), 1) < 0.7:
            score = min(score, 0.55)
    # 词首/词尾保护 + 系列作分代保护（均在「归一化不相等」时生效）。
    # 例（前缀）："carrion" vs "carrion fields"（不同作品）；
    # 例（后缀）："rance" vs "sengoku rance"（Rance 系列不同代数，"Rance" 是泛称词）；
    # 例（系列分代）："美少女万華鏡 -呪われし伝説-" vs "美少女万華鏡 -神が造りたもうた-"。
    # 注：真正的别名命中（探针为完整标题）不受影响，因为此时 token 数比 / 整体分数更高。
    else:
        # 用与 tokens 同源的清理序列（去除 "-"、"/"、"." 等仅作分隔的符号 token），
        # 否则 'Rance - -' 这类残留符号会让 token 序列判定失准（逃过前缀/后缀保护）。
        _qt = [t for t in re.split(r"[^a-z0-9\u4e00-\u9fff\u3040-\u30ff]+", q.lower()) if t]
        _rt = [t for t in re.split(r"[^a-z0-9\u4e00-\u9fff\u3040-\u30ff]+", r.lower()) if t]

        # ---- 系列作分代保护 ----
        # 两侧共享「系列名前缀」，但其后各代副标题**无任何交集**时，判为同系列不同代。
        if len(_qt) >= 2 and len(_rt) >= 2:
            _plen = 0
            for _a, _b in zip(_qt, _rt):
                if _a == _b:
                    _plen += 1
                else:
                    break
            if _plen >= 1:
                _q_rest = set(_qt[_plen:])
                _r_rest = set(_rt[_plen:])
                if _q_rest and _r_rest and not (_q_rest & _r_rest):
                    score = min(score, 0.55)

        # ---- 词首/词尾保护 ----
        _short_t, _long_t = (_qt, _rt) if len(_qt) <= len(_rt) else (_rt, _qt)
        if _short_t and len(_short_t) < len(_long_t):
            _n = len(_short_t)
            _is_prefix = _long_t[:_n] == _short_t
            _is_suffix = _long_t[-_n:] == _short_t
            # 中英文名拼接豁免：若长串中「未被短串覆盖」的部分含 CJK，
            # 说明这是「中文名 + 英文名」的同一作品写法
            # （如 'Tricolour Lovestory' vs '三色△绘恋 tricolour lovestory'），
            # 不视为泛称前缀/后缀，跳过降分。
            if _is_prefix:
                _rest_t = _long_t[_n:]
            else:
                _rest_t = _long_t[: len(_long_t) - _n]
            _rest_has_cjk = any(re.search(r"[\u4e00-\u9fff]", _t) for _t in _rest_t)
            if (_is_prefix or _is_suffix) and _n / len(_long_t) < 0.8 and not _rest_has_cjk:
                score = min(score, 0.55)
    return round(max(0.0, min(1.0, score)), 4)
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

async def _keep_user_title_default(db: AsyncSession) -> bool:
    """返回是否启用「保留用户译名」。读 SystemConfig.keep_user_title，
    字段不存在或无配置时默认 True（保护优先）。"""
    try:
        from .models import SystemConfig
        cfg = await db.get(SystemConfig, 1)
        if cfg is None:
            return True
        val = getattr(cfg, "keep_user_title", True)
        return True if val is None else bool(val)
    except Exception:
        return True


async def retag_games_from_glossary(db: AsyncSession, apply_title: bool | None = None) -> int:
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
        # 保护：默认不覆盖 game.title，避免回归/术语表变更时冲掉用户手工改好的译名。
        # 如需强制同步，显式传 apply_title=True（或在系统配置里关闭 keep_user_title）。
        _apply_title_enabled = apply_title
        if _apply_title_enabled is None:
            _apply_title_enabled = not await _keep_user_title_default(db)
        if _apply_title_enabled:
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

    改进（修复历史误配）：
      * 四个来源**并行**搜索，各自算出最佳候选及分数；
      * 只有当最佳分数 >= 该来源门槛（默认 0.70）时才作为候选参与竞争；
      * 在所有达标来源中，取**分数最高**者，而不是按固定顺序先撞先得。
        这样 "AI Shoujo" 会选 Steam(1.0) 而不是 VNDB(0.516)。
      * 高置信（>=0.85）时若多个来源都命中，仍优先 Steam/RAWG（PC 游戏收录更全），
        但分数差距明显时以分数为准。
    """
    title = (game.title or "").strip()
    if not title:
        return "", ""
    probes = build_probes(title, limit=6)

    async def _try_steam():
        """返回 (score, source_id, human_title) 或 None。"""
        try:
            sc = SteamClient()
            results = []
            for p in probes:
                results = await sc.search_games(p, page_size=20)
                if results:
                    break
            if not results:
                return None
            probe_group = [g for g in probes if g]
            ranked = sorted(
                results,
                key=lambda c: max((title_similarity(g, c.get("title", "")) for g in probe_group), default=0.0),
                reverse=True,
            )
            for cand in ranked[:8]:
                score = max((title_similarity(g, cand.get("title", "")) for g in probe_group), default=0.0)
                if score < _MATCH_THRESHOLD["steam"]:
                    continue
                try:
                    # app_type 校验容错：空字符串多为 appdetails 失败/限流，重试后仍为空则采纳
                    _at = ""
                    for _try in range(2):
                        try:
                            _at = await sc.get_app_type(cand["source_id"])
                        except Exception:
                            _at = ""
                        if _at:
                            break
                    if _at and _at != "game":
                        continue
                    return (score, cand["source_id"], cand.get("title", ""))
                except Exception:
                    continue
            return None
        except Exception as e:
            logger.warning("自动匹配 steam 失败：%s", e)
            return None

    async def _try_rawg():
        try:
            client = RawgClient()
            probe_group = [g for g in probes if g]
            for p in probes:
                results = await client.search_games(p, page_size=8)
                if not results:
                    continue
                best, bs = None, 0.0
                for cand in results:
                    s = max((title_similarity(g, cand.get("title", "")) for g in probe_group), default=0.0)
                    if s > bs:
                        best, bs = cand, s
                if best and bs >= _MATCH_THRESHOLD["rawg"]:
                    return (bs, best["source_id"], best.get("title", ""))
            return None
        except Exception as e:
            logger.warning("自动匹配 rawg 失败：%s", e)
            return None

    async def _try_vndb():
        try:
            pool = []
            for p in probes:
                try:
                    pool += await search_vndb(p)
                except Exception:
                    pass
            seen, uniq = set(), []
            for it in pool:
                sid = it.get("source_id")
                if sid and sid not in seen:
                    seen.add(sid)
                    uniq.append(it)
            if not uniq:
                return None
            probe_group = [g for g in probes if g]

            def _rank(it):
                t, a = it.get("title", ""), it.get("alias", "") or ""
                st = max((title_similarity(g, t) for g in probe_group), default=0.0)
                sa = max((title_similarity(g, a) for g in probe_group), default=0.0) if a else 0.0
                # 别名权重降到 0.7：历史 0.8 会把弱命中抬进门槛
                return max(st, sa * 0.7)

            best = max(uniq, key=_rank)
            bs = _rank(best)
            if bs >= _MATCH_THRESHOLD["vndb"]:
                return (bs, best["source_id"], best.get("title", ""))
            return None
        except Exception as e:
            logger.warning("自动匹配 vndb 失败：%s", e)
            return None

    async def _try_dlsite():
        try:
            from .clients.dlsite_client import DlsiteClient
            dlsite = DlsiteClient()
            # RJ/VJ/BJ 编号精准命中，直接给满分
            workno = dlsite.extract_workno(title) or dlsite.extract_workno(game.resource_url or "")
            if workno:
                return (1.0, workno, workno)
            probe_group = [g for g in probes if g]
            pool = []
            for p in probes:
                try:
                    pool += await dlsite.search_games(p, page_size=8)
                except Exception:
                    pass
            best, bs = None, 0.0
            for c in pool:
                s = max((title_similarity(g, c.get("title", "")) for g in probe_group), default=0.0)
                if s > bs:
                    best, bs = c, s
            if best and bs >= _MATCH_THRESHOLD["dlsite"]:
                return (bs, best["source_id"], best.get("title", ""))
            return None
        except Exception as e:
            logger.warning("自动匹配 dlsite 失败：%s", e)
            return None

    # 四源并行，谁先命中都保留，最后统一比分数
    results = await asyncio.gather(
        _try_steam(), _try_rawg(), _try_vndb(), _try_dlsite(),
        return_exceptions=True,
    )
    names = ["steam", "rawg", "vndb", "dlsite"]
    hits: list[tuple[float, str, str, str]] = []
    for name, res in zip(names, results):
        if isinstance(res, Exception):
            logger.warning("自动匹配 %s 异常：%s", name, res)
            continue
        if res:
            score, sid, human = res
            hits.append((score, name, str(sid), human or ""))

    if not hits:
        return "", ""

    # 择优：分数优先；分数接近（差值 < 0.06）时 PC 向来源（steam/rawg）优先
    hits.sort(key=lambda h: (-h[0], 0 if h[1] in ("steam", "rawg") else 1))
    top = hits[0]
    for h in hits[1:]:
        if abs(h[0] - top[0]) < 0.06 and h[1] in ("steam", "rawg") and top[1] not in ("steam", "rawg"):
            top = h
            break
    logger.info(
        "自动匹配主来源=%s %s (%.3f)  [全部候选: %s]",
        top[1], top[2], top[0],
        ", ".join("%s=%.3f" % (h[1], h[0]) for h in hits),
    )
    return top[1], top[2]

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
        # 注意：不覆盖 game.title（保留用户/扫描命名，如"尾行3 中文版"）；
        # 数据源标题存放在 source_data[src]["title"] 中，界面需要时从此取用。
        for field in ("alias", "description", "developer", "publisher", "release_date", "rating", "tags", "series", "source_type", "source_id", "screenshots", "version", "original_data", "steam_appid"):
            if field in metadata:
                setattr(game, field, metadata[field])
        # 封面按 cover_source_priority 选取（而非直接跟随主来源）
        _cfg = await session.get(SystemConfig, 1)
        _cover_url, _cover_src = await resolve_cover(game, _cfg)
        game.cover_url = _cover_url or metadata.get("cover_url", "")
        await session.commit()
        try:
            game.cover_url = await cache_cover(game.id, game.cover_url, _cover_src or game.source_type)
            game.cover_source = _cover_src or game.source_type
            await session.commit()
        except Exception:
            await session.rollback()


# ===== 新的「多来源全量重匹配」段落（替换旧的第42行起至函数末） =====
        # ==================== 多来源「全量重匹配」 ====================
        # 设计变更（v1.6.2）：
        #   旧逻辑只对「已有来源」沿用旧 ID 重拉、对「缺失来源」补充搜索，
        #   结果是一旦某来源被写入错误 ID（如 vndb=Fate/stay night），
        #   刷新时会照旧拉回同一份脏数据，永远无法纠正。
        #   新逻辑：无论该来源此前是否有 ID，一律**重新搜索 + 重新打分匹配**；
        #     - 匹配达标（>= 该来源门槛）→ 用新结果覆盖；
        #     - 匹配不达标 → 移除该来源（清掉脏数据），避免污染留存。
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
            source_data[game.source_type] = {
                k: v for k, v in raw_metadata.items()
                if k not in ("resource_type", "resource_url", "play_status", "original_data")
            }
            source_ids[game.source_type] = game.source_id

        # ---- 探针上下文：用于各来源重新匹配 ----
        # 说明：主源（game.source_type）是已确认的权威锚点，其 english_name 优先；
        # 其余来源的 english_name 可能已被历史误配污染，排在后面，仅作补充。
        _eng_names: list[str] = []
        # 记录「每个来源自己贡献的英文名」，用于按来源隔离探针（防止自证污染）。
        _src_eng_names: dict[str, list[str]] = {}
        _src_order = [game.source_type] + [s for s in ("steam", "rawg", "vndb", "dlsite") if s != game.source_type]
        for _k in _src_order:
            if not _k:
                continue
            _v = source_data.get(_k)
            if not isinstance(_v, dict):
                continue
            _bucket = _src_eng_names.setdefault(_k, [])
            # (a) 显式 english_name
            if _v.get("english_name"):
                _en = str(_v["english_name"]).strip()
                if _en and _en not in _eng_names:
                    _eng_names.append(_en)
                if _en and _en not in _bucket:
                    _bucket.append(_en)
            # (b) 标题本身就是英文原名（如 rawg 的 "Gnosia"）——含 ASCII 字母才采纳
            _t = str(_v.get("title") or "").strip()
            if _t and re.search(r"[A-Za-z]", _t) and _t not in _eng_names:
                _eng_names.append(_t)
            if _t and re.search(r"[A-Za-z]", _t) and _t not in _bucket:
                _bucket.append(_t)
        # 英文名候选最多取前 4 个，避免探针过多拖慢搜索
        _eng_names = _eng_names[:4]
        _probes = build_probes(game.title, extra=_eng_names, limit=6)
        _probe_group = [p for p in _probes if p]

        def _probes_for_src(src: str) -> list[str]:
            """构造「排除该来源自身贡献英文名」的探针组，防止脏数据自证。

            背景（v1.6.2 修复）：探针曾无条件纳入所有来源的 title/english_name，
            若某来源已被写入错误数据（如 rawg.title='FNF: Summer Vacation'），
            该错误标题会作为探针与自身候选得 1.0，形成「永远匹配自己、永不清除」的闭环。
            按来源剔除其自身贡献的英文名后，匹配只依赖**其他来源 + 用户原始标题**，
            脏数据无法自证。
            """
            _others: list[str] = []
            for _k, _names in _src_eng_names.items():
                if _k == src:
                    continue
                for _n in _names:
                    if _n not in _others:
                        _others.append(_n)
            _others = _others[:4]
            _ps = build_probes(game.title, extra=_others, limit=6)
            return [p for p in _ps if p]

        async def _match_source(src: str):
            """对单个来源重新搜索并打分，返回 (source_id, meta_dict, score)；无匹配返回 None。

            网络/接口异常时抛 _SourceUnavailable，以便调用方保留旧数据、不误删。
            """
            probe_group = _probes_for_src(src)
            if not probe_group:
                probe_group = _probe_group
            if not probe_group:
                return None
            try:
                return await _match_source_inner(src, probe_group)
            except _SourceUnavailable:
                raise
            except Exception as _e:
                raise _SourceUnavailable("%s: %s" % (type(_e).__name__, _e)) from _e

        async def _match_source_inner(src: str, probe_group):
            if src == "steam":
                sc = SteamClient()
                results = []
                for p in probe_group:
                    results = await sc.search_games(p, page_size=20)
                    if results:
                        break
                if not results:
                    return None

                def _rank_title(t: str) -> float:
                    return max((title_similarity(g, t) for g in probe_group), default=0.0)

                # 不按本地相似度重排：Steam storesearch 的返回顺序本就是相关性排序，
                # 中文标题的本体（如"红怪"=CARRION）本地相似度 0.000，若重排会被挤到末尾而丢失。
                # 因此按原顺序遍历，仅对每个候选取 detail 后用 english_name 精排。
                ordered = list(results)
                best = None  # (score, sid, meta)
                for cand in ordered[:12]:
                    _t = cand.get("title", "") or ""
                    # 过滤试玩/捆绑包/原声等衍生条目
                    if re.search(r"(?i)(\bdemo\b|bundle|soundtrack|\bost\b|art ?book|artbook|\btrial\b|playtest)", _t):
                        continue
                    _sid = str(cand["source_id"])
                    try:
                        meta = await get_steam_detail(_sid)
                    except Exception:
                        continue
                    if not meta:
                        continue
                    # detail 里的 type 若非 game（dlc/music 等）则跳过
                    _type = (meta.get("original_data") or {}).get("type") if isinstance(meta.get("original_data"), dict) else None
                    if _type and _type != "game":
                        continue
                    # 用 english_name + detail title 双重打分
                    _cand_score = max(
                        _rank_title(_t),
                        _rank_title(meta.get("english_name", "") or ""),
                        _rank_title(meta.get("title", "") or ""),
                    )
                    if _cand_score >= _MATCH_THRESHOLD["steam"]:
                        if best is None or _cand_score > best[0]:
                            best = (_cand_score, _sid, meta)
                            if _cand_score >= 0.98:
                                break       # 已高度确信，无需继续
                if best:
                    return (best[1], best[2], best[0])
                return None

            if src == "rawg":
                client = RawgClient()

                def _filter_main(results):
                    return [
                        r for r in results
                        if not re.search(r"(?i)(typing|dlc|demo|trial|soundtrack|ost|art pack)", r.get("title", ""))
                    ]

                results = []
                # 优先用已知英文名（更准确），其次别名，最后原名
                _queries = []
                for _q in (_eng_names + [game.title] + alias_candidates(game.title)):
                    if _q and _q not in _queries:
                        _queries.append(_q)
                _net_err = None
                for _q in _queries[:4]:
                    try:
                        _r = _filter_main(await client.search_games(_q, page_size=5))
                    except Exception as _e:
                        _net_err = _e
                        _r = []
                    if _r:
                        results = _r
                        break
                if not results:
                    if _net_err is not None:
                        raise _SourceUnavailable("rawg 搜索异常: %s" % _net_err)
                    return None

                def _rawg_rank(cand):
                    return max((title_similarity(g, cand.get("title", "")) for g in probe_group if g), default=0.0)

                best = max(results, key=_rawg_rank)
                bs = _rawg_rank(best)
                if bs >= _MATCH_THRESHOLD["rawg"]:
                    return (str(best["source_id"]), await client.get_game_detail(best["source_id"]), bs)
                return None

            if src == "vndb":
                pool = []
                _net_err = None
                for p in probe_group:
                    try:
                        pool += await search_vndb(p)
                    except Exception as _e:
                        _net_err = _e
                if not pool:
                    if _net_err is not None:
                        raise _SourceUnavailable("vndb 搜索异常: %s" % _net_err)
                    return None
                _seen_v, uniq = set(), []
                for item in pool:
                    sid = item.get("source_id")
                    if sid and sid not in _seen_v:
                        _seen_v.add(sid)
                        uniq.append(item)

                def _vndb_rank(item):
                    t = item.get("title", "")
                    a = item.get("alias", "") or ""
                    st = max((title_similarity(g, t) for g in probe_group), default=0.0)
                    sa = max((title_similarity(g, a) for g in probe_group), default=0.0) if a else 0.0
                    return max(st, sa * 0.7)

                best = max(uniq, key=_vndb_rank)
                bs = _vndb_rank(best)
                if bs >= _MATCH_THRESHOLD["vndb"]:
                    return (str(best["source_id"]), await get_vndb_detail(best["source_id"]), bs)
                return None

            if src == "dlsite":
                from .clients.dlsite_client import DlsiteClient
                dlsite = DlsiteClient()
                # 1) RJ/VJ/BJ 编号精准查询（标题或路径里可直接提取）
                workno = dlsite.extract_workno(game.title) or dlsite.extract_workno(game.resource_url or "")
                if workno:
                    return (str(workno), await get_dlsite_detail(workno), 1.0)
                # 2) 按标题/别名搜索
                ds_best, ds_best_score = None, 0.0
                _net_err = None
                _any_ok = False
                for q in probe_group[:3]:
                    try:
                        cands = await dlsite.search_games(q, page_size=8)
                        _any_ok = True
                    except Exception as _e:
                        _net_err = _e
                        cands = []
                    for c in cands:
                        score = title_similarity(game.title, c.get("title", ""))
                        if score > ds_best_score:
                            ds_best, ds_best_score = c, score
                    if ds_best_score >= 0.75:
                        break
                if ds_best is None and not _any_ok and _net_err is not None:
                    raise _SourceUnavailable("dlsite 搜索异常: %s" % _net_err)
                if ds_best and ds_best_score >= _MATCH_THRESHOLD["dlsite"]:
                    return (str(ds_best["source_id"]), await get_dlsite_detail(ds_best["source_id"]), ds_best_score)
                return None

            return None

        async def _fetch_detail_by_source(src: str, sid: str):
            """按 ID 直取某来源的详情（不做搜索匹配）。"""
            if src == "steam":
                return await get_steam_detail(sid)
            if src == "rawg":
                return await RawgClient().get_game_detail(sid)
            if src == "vndb":
                return await get_vndb_detail(sid)
            if src == "dlsite":
                return await get_dlsite_detail(sid)
            return None

        # ---- 主源：按 source_id 直取（权威锚点，不重新搜索，避免被误配覆盖）----
        # 主源 ID 是用户/首次匹配确认过的可靠锚点；若对其重新搜索，
        # 短标题游戏（如"主播女孩重度依赖"）可能被同名衍生作（"网络梗打字通"）顶掉。
        _matched: dict[str, str] = {}
        _primary = game.source_type
        if _primary and _primary != "custom" and game.source_id:
            try:
                _pm = await _fetch_detail_by_source(_primary, game.source_id)
                if _pm:
                    source_ids[_primary] = str(game.source_id)
                    source_data[_primary] = _pm
                    _matched[_primary] = str(game.source_id)
                    logger.info("全量重匹配：game_id=%s 主源 %s=%s 直取成功", game.id, _primary, game.source_id)
                else:
                    logger.warning("全量重匹配：game_id=%s 主源 %s=%s 直取无数据", game.id, _primary, game.source_id)
            except Exception as e:
                logger.warning("全量重匹配：game_id=%s 主源 %s 直取失败: %s", game.id, _primary, e)

        class _SourceUnavailable(Exception):
            """来源查询因网络/接口异常而不可用（区别于"搜索成功但无匹配"）。"""

        # ---- 非主源：全部重新搜索匹配，并发执行 ----
        _OTHER_SRC = [s for s in ("steam", "rawg", "vndb", "dlsite") if s != _primary]
        _re_results = await asyncio.gather(
            *(_match_source(s) for s in _OTHER_SRC),
            return_exceptions=True,
        )
        for _src, _r in zip(_OTHER_SRC, _re_results):
            if isinstance(_r, _SourceUnavailable):
                # 网络/接口异常：本次无法判断，**保留旧数据**，避免因临时故障误删
                logger.warning("全量重匹配：game_id=%s, source=%s 查询异常，保留旧数据: %s", game.id, _src, _r)
                if source_data.get(_src):
                    _matched[_src] = str(source_ids.get(_src, ""))
                continue
            if isinstance(_r, Exception):
                logger.warning("全量重匹配：game_id=%s, source=%s 失败: %s", game.id, _src, _r)
                if source_data.get(_src):
                    _matched[_src] = str(source_ids.get(_src, ""))
                continue
            if _r:
                _sid, _meta, _score = _r
                source_ids[_src] = _sid
                source_data[_src] = _meta
                _matched[_src] = _sid
                logger.info("全量重匹配：game_id=%s, source=%s -> %s (%.3f)", game.id, _src, _sid, _score)
            else:
                # 搜索成功但无达标候选：视为脏数据，移除
                if source_ids.pop(_src, None):
                    logger.info("全量重匹配：game_id=%s, 移除未匹配到的旧来源 %s", game.id, _src)
                source_data.pop(_src, None)

        # ---- 主来源：若当前主来源在重匹配中失败，则从命中结果里重新择优 ----
        if game.source_type not in _matched:
            if _matched:
                # 优先 PC 向来源；否则取任意命中的
                _order = [s for s in ("steam", "rawg", "vndb", "dlsite") if s in _matched]
                _new_primary = _order[0]
                game.source_type = _new_primary
                game.source_id = _matched[_new_primary]
                logger.info("全量重匹配：game_id=%s 主来源改为 %s=%s", game.id, _new_primary, _matched[_new_primary])
            else:
                logger.warning("全量重匹配：game_id=%s 所有来源均未匹配到，保留原主来源", game.id)

        game.source_ids = json.dumps(source_ids, ensure_ascii=False, default=str)
        game.source_data = json.dumps(source_data, ensure_ascii=False, default=str)
        # 刷新元数据后重算游戏类型（来源可能变化）
        game.game_type = game_type_str(game, source_data)
        await session.commit()

# ==================== 手动匹配 / 重新翻译 ====================


async def find_match_candidates(game_id: int, limit_per_source: int = 6) -> list[dict]:
    """为指定游戏在各数据源搜索候选，返回带相似度的候选列表（按相似度降序）。

    类似飞牛影视的「手动匹配」：把多个来源的候选一起列出来，由用户选择。

    改进：
      * 四个来源 + 多个探针**并发**搜索，显著缩短等待时间；
      * 用修复后的 title_similarity 打分（短标题不再虚高）；
      * 过滤掉相似度近乎 0 的噪声候选（低于 _MATCH_FLOOR 直接丢弃），
        避免出现"相似度 0%"的无意义列表；
      * 已关联来源的候选置顶并标记。
    """
    # 手动匹配的下限：低于此值的候选不展示（用户仍可通过修改游戏名重试）
    _MATCH_FLOOR = 0.30

    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")
        try:
            source_ids = json.loads(game.source_ids or "{}")
        except Exception:
            source_ids = {}
        try:
            source_data = json.loads(game.source_data or "{}")
        except Exception:
            source_data = {}

        # 搜索探针：英文名（若有）优先，其次标题与别名候选
        eng_names: list[str] = []
        for v in source_data.values():
            if isinstance(v, dict) and v.get("english_name"):
                eng_names.append(str(v["english_name"]))
        # 多来源的英文名都收集（不同来源可能给出不同的英文名）
        probes = build_probes(game.title, extra=eng_names, limit=6)

        rank_group = list(dict.fromkeys(
            [g for g in (eng_names + probes) if g]
        ))
        candidates: list[dict] = []
        seen: set[tuple[str, str]] = set()

        def _add(source: str, item: dict):
            sid = str(item.get("source_id") or "")
            if not sid or (source, sid) in seen:
                return
            seen.add((source, sid))
            t = item.get("title", "")
            a = item.get("alias", "") or ""
            # 标题与别名分别算，取较大者（别名权重 0.9：别名可信度略低于正式标题）
            sim_t = max((title_similarity(g, t) for g in rank_group), default=0.0)
            sim_a = max((title_similarity(g, a) for g in rank_group), default=0.0) if a else 0.0
            sim = max(sim_t, sim_a * 0.9)
            if sim < _MATCH_FLOOR:
                return          # 过滤噪声候选
            candidates.append({
                "source_type": source,
                "source_id": sid,
                "title": t,
                "alias": a,
                "release_date": item.get("release_date"),
                "developer": item.get("developer", ""),
                "cover_url": item.get("cover_url", ""),
                "similarity": round(float(sim), 3),
                "already_linked": str(source_ids.get(source, "")) == sid,
            })

        # ---- 并发搜索：每个探针 x 每个来源 ----
        async def _search_rawg(probe: str):
            try:
                items = await RawgClient().search_games(probe, page_size=limit_per_source)
                for it in items[:limit_per_source]:
                    _add("rawg", it)
            except Exception as e:
                logger.warning("手动匹配 rawg 搜索失败 %s: %s", probe, e)

        async def _search_vndb(probe: str):
            try:
                for it in (await search_vndb(probe))[:limit_per_source]:
                    _add("vndb", it)
            except Exception as e:
                logger.warning("手动匹配 vndb 搜索失败 %s: %s", probe, e)

        async def _search_steam(probe: str):
            try:
                # Steam storesearch 是摘要，需补 detail 才有封面/日期；只对高分候选补
                items = (await search_steam(probe))[:limit_per_source]
                for it in items:
                    _add("steam", it)
            except Exception as e:
                logger.warning("手动匹配 steam 搜索失败 %s: %s", probe, e)

        async def _search_dlsite(probe: str):
            try:
                from .clients.dlsite_client import DlsiteClient
                items = await DlsiteClient().search_games(probe, page_size=limit_per_source)
                for it in items[:limit_per_source]:
                    _add("dlsite", it)
            except Exception as e:
                logger.warning("手动匹配 dlsite 搜索失败 %s: %s", probe, e)

        tasks = []
        for probe in probes:
            tasks.append(_search_rawg(probe))
            tasks.append(_search_vndb(probe))
            tasks.append(_search_steam(probe))
            tasks.append(_search_dlsite(probe))
        await asyncio.gather(*tasks, return_exceptions=True)

        # 按相似度降序；同分优先已关联来源
        candidates.sort(key=lambda c: (c["similarity"], c["already_linked"]), reverse=True)
        return candidates[:40]


async def apply_match(game_id: int, source_type: str, source_id: str, set_primary: bool = True) -> dict:
    """把用户手动选定的候选元数据应用到游戏。"""
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")
        if source_type == "rawg":
            metadata = await RawgClient().get_game_detail(source_id)
        elif source_type == "steam":
            metadata = await get_steam_detail(source_id)
        elif source_type == "vndb":
            metadata = await get_vndb_detail(source_id)
        elif source_type == "dlsite":
            metadata = await get_dlsite_detail(source_id)
        else:
            raise RuntimeError(f"不支持的来源类型：{source_type}")

        raw_metadata = dict(metadata)
        _preserved = {k: metadata.get(k) for k in ("english_name",) if metadata.get(k)}
        metadata = await translation_service.translate_metadata(session, metadata)
        metadata.update(_preserved)
        metadata["release_date"] = normalize_release_date(metadata.get("release_date"))

        # 写入 source_ids / source_data（存翻译前原文）
        try:
            source_ids = json.loads(game.source_ids or "{}")
        except Exception:
            source_ids = {}
        try:
            source_data = json.loads(game.source_data or "{}")
        except Exception:
            source_data = {}
        source_ids[source_type] = source_id
        source_data[source_type] = {
            k: v for k, v in raw_metadata.items()
            if k not in ("resource_type", "resource_url", "play_status", "original_data")
        }

        if set_primary:
            for field in ("title", "alias", "description", "developer", "publisher",
                          "release_date", "rating", "tags", "series", "source_type",
                          "source_id", "screenshots", "version", "original_data", "steam_appid"):
                if field in metadata:
                    setattr(game, field, metadata[field])
            game.source_type = source_type
            game.source_id = source_id
            game.tag_source = source_type

        game.source_ids = json.dumps(source_ids, ensure_ascii=False, default=str)
        game.source_data = json.dumps(source_data, ensure_ascii=False, default=str)
        # 应用匹配后重算游戏类型
        game.game_type = game_type_str(game, source_data)
        await session.commit()

        if set_primary:
            # 注意：必须在 game.source_data 赋值之后再解析封面，
            # 否则 resolve_cover 读不到本次新写入的来源封面。
            _cfg = await session.get(SystemConfig, 1)
            _cover_url, _cover_src = await resolve_cover(game, _cfg)
            try:
                _final = _cover_url or metadata.get("cover_url", "")
                game.cover_url = await cache_cover(game.id, _final, _cover_src or source_type)
                game.cover_source = _cover_src or source_type
                await session.commit()
            except Exception:
                await session.rollback()
        await session.refresh(game)
        return {"id": game.id, "game_type": game.game_type, "title": game.title}


async def retranslate_game(game_id: int, force: bool = False, keep_title: bool | None = None) -> dict:
    """用库内 original_data（翻译前原文）重新走一遍翻译，不联网刮削，用于改术语表后快速生效。

    force=True 时即使 auto_translate 关闭也强制翻译。
    """
    async with SessionLocal() as session:
        game = await session.get(Game, game_id)
        if not game:
            raise RuntimeError("游戏不存在")
        config = await session.get(SystemConfig, 1)
        if not config:
            config = SystemConfig(id=1)

        # original_data 里存的是「主来源 + 各来源」的翻译前原文快照
        try:
            original = json.loads(game.original_data or "{}")
        except Exception:
            original = {}
        try:
            source_data = json.loads(game.source_data or "{}")
        except Exception:
            source_data = {}

        await translation_service.load(session)

        def _ensure_translatable():
            # 没有 original_data 时，用 source_data 里主来源的原文兜底
            nonlocal original
            if not original and isinstance(source_data, dict):
                primary = source_data.get(game.source_type)
                if isinstance(primary, dict):
                    original = dict(primary)

        _ensure_translatable()

        changed = False
        # 重新翻译标题
        # 保护：keep_title（默认读 keep_user_title 配置）为 True 时只翻译不覆盖 game.title，
        # 避免用户手工改好的译名被冲掉。
        _keep = keep_title
        if _keep is None:
            _keep = await _keep_user_title_default(session)
        src_title = original.get("title") or ""
        if src_title:
            new_title = await translation_service.translate(src_title, "title", session)
            if new_title and new_title != game.title and not _keep:
                game.title = new_title
                changed = True
        # 重新翻译简介
        src_desc = original.get("description") or ""
        if src_desc:
            new_desc = await translation_service.translate(src_desc, "description", session)
            if new_desc and new_desc != game.description:
                game.description = new_desc
                changed = True
        # 重新翻译标签（按当前标签来源的原文）
        tag_src = game.tag_source or game.source_type
        raw_tags = ""
        if isinstance(source_data.get(tag_src), dict):
            raw_tags = source_data[tag_src].get("tags", "")
        if not raw_tags:
            raw_tags = original.get("tags", "")
        if raw_tags:
            parts = [p.strip() for p in raw_tags.split(",") if p.strip()]
            translated = []
            for p in parts:
                translated.append(await translation_service.translate(p, "tag", session))
            new_tags = ", ".join(translated)
            if new_tags != game.tags:
                game.tags = new_tags
                changed = True

        game.game_type = game_type_str(game, source_data)
        await session.commit()
        return {"id": game.id, "changed": changed, "title": game.title}


async def recompute_all_game_types() -> int:
    """按推断规则重算所有游戏的 game_type（存量数据一次性回填）。"""
    async with SessionLocal() as session:
        count = 0
        for game in (await session.scalars(select(Game))).all():
            try:
                sd = json.loads(game.source_data or "{}")
            except Exception:
                sd = {}
            new_type = game_type_str(game, sd)
            if new_type != game.game_type:
                game.game_type = new_type
                count += 1
        await session.commit()
        return count


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
            _cfg = await session.get(SystemConfig, 1)
            _cover_url, _cover_src = await resolve_cover(game, _cfg)
            _final = _cover_url or metadata.get("cover_url", "")
            game.cover_url = await cache_cover(game.id, _final, _cover_src or "vndb")
            game.cover_source = _cover_src or "vndb"
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

        # 原实现硬编码「有 appid 就用 steam 封面」，现改为统一走 cover_source_priority。
        # 由于 rawg 详情里能拿到 steam_appid，resolve_cover 对 steam 分支做了
        # 「有 appid 即构造封面 URL」的处理，因此默认优先级(steam 在前)行为不变，
        # 但用户把 rawg 拖到 steam 之前时也能正确生效。
        try:
            _cfg = await session.get(SystemConfig, 1)
            _cover_url, _cover_src = await resolve_cover(game, _cfg)
            _final = _cover_url or metadata["cover_url"]
            _src = _cover_src or "rawg"
            game.cover_url = await cache_cover(game.id, _final, _src)
            game.cover_source = _src
            await session.commit()
        except Exception:
            await session.rollback()
