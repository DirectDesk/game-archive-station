from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class Game(Base):
    __tablename__ = "games"
    __table_args__ = (
        UniqueConstraint("resource_url", name="uq_games_resource_url"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # title 是「原始名」——来源站点的原名（英文/日文），**不再被翻译覆盖**。
    # 语义变更（v1.7.0）：历史上翻译会把中文译名直接写进 title，导致
    # 「用户想要的名字」与「来源给的名字」共用一个字段——改名会被原文打回，
    # 且原文只能靠 original_data 那个 Text 大 JSON 留档。
    # 现在拆开：title=原始名，title_cn=中文译名，
    # 展示统一用 display_title = title_cn or title。
    title: Mapped[str] = mapped_column(String(255), index=True)
    # 中文译名；为空表示暂无译名，展示时回退到 title
    title_cn: Mapped[str] = mapped_column(String(255), default="", index=True)
    alias: Mapped[str] = mapped_column(String(500), default="")
    cover_url: Mapped[str] = mapped_column(String(1000), default="")
    cover_source: Mapped[str] = mapped_column(String(20), default="")
    steam_appid: Mapped[str] = mapped_column(String(20), default="")
    screenshots: Mapped[str] = mapped_column(Text, default="")
    screenshot_source: Mapped[str] = mapped_column(String(20), default="")
    original_data: Mapped[str] = mapped_column(Text, default="")
    description: Mapped[str] = mapped_column(Text, default="")
    developer: Mapped[str] = mapped_column(String(255), default="")
    publisher: Mapped[str] = mapped_column(String(255), default="")
    release_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    tags: Mapped[str] = mapped_column(String(1000), default="")
    tag_source: Mapped[str] = mapped_column(String(50), default="")
    # 用户锁定的字段名（JSON 数组，如 ["description","tags"]）。
    # 锁定后，所有「从数据源 / 翻译流程重新生成」的操作（刮削刷新、手动匹配、
    # 重新翻译、术语表同步）都会跳过这些字段，保护用户手改的内容不被静默覆盖。
    # 可锁字段见 services.LOCKABLE_FIELDS：title_cn / tags / description。
    locked_fields: Mapped[str] = mapped_column(String(500), default="")
    series: Mapped[str] = mapped_column(String(255), default="")
    version: Mapped[str] = mapped_column(String(100), default="")
    source_type: Mapped[str] = mapped_column(String(20), default="custom")
    source_id: Mapped[str] = mapped_column(String(100), default="")
    source_ids: Mapped[str] = mapped_column(Text, default="{}")
    source_data: Mapped[str] = mapped_column(Text, default="{}")
    resource_type: Mapped[str] = mapped_column(String(20), default="none")
    resource_url: Mapped[str] = mapped_column(String(2000), default="")
    file_size: Mapped[int] = mapped_column(Integer, default=0)
    play_status: Mapped[str] = mapped_column(String(20), default="favorite")
    # 游戏平台类型，多选，逗号分隔（pc/android/gal），与数据来源 source_type 解耦
    game_type: Mapped[str] = mapped_column(String(50), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class SystemConfig(Base):
    """单行系统状态；create_all 会在已有数据库上平滑创建该新表。"""

    __tablename__ = "system_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    scan_enable: Mapped[bool] = mapped_column(Boolean, default=True)
    scan_cron: Mapped[str] = mapped_column(String(50), default="0 3 * * *")
    scan_throttle_ms: Mapped[int] = mapped_column(Integer, default=50)
    last_scan_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    scan_root: Mapped[str] = mapped_column(String(500), default="/vol/baidu")
    local_game_root: Mapped[str] = mapped_column(String(500), default="/vol/games")
    download_dir: Mapped[str] = mapped_column(String(500), default="/vol/download/game")
    rawg_api_key: Mapped[str] = mapped_column(String(200), default="")
    auto_translate: Mapped[bool] = mapped_column(Boolean, default=False)
    translator_type: Mapped[str] = mapped_column(String(20), default="none")
    tencent_secret_id: Mapped[str] = mapped_column(String(200), default="")
    tencent_secret_key: Mapped[str] = mapped_column(String(200), default="")
    tencent_region: Mapped[str] = mapped_column(String(50), default="ap-guangzhou")
    metadata_source_priority: Mapped[str] = mapped_column(String(200), default='["rawg","vndb","dlsite"]')
    cover_source_priority: Mapped[str] = mapped_column(String(200), default='["steam","vndb","dlsite","rawg"]')
    screenshot_source_priority: Mapped[str] = mapped_column(String(200), default='["rawg","steam","vndb","dlsite"]')
    tag_source_priority: Mapped[str] = mapped_column(String(200), default='["steam","rawg","vndb","dlsite"]')
    scan_fetch_screenshots: Mapped[bool] = mapped_column(Boolean, default=False)
    max_screenshots: Mapped[int] = mapped_column(Integer, default=5)
    # 保护用户手工译名：术语表变更/重翻译时默认不覆盖 game.title
    keep_user_title: Mapped[bool] = mapped_column(Boolean, default=True)


class TranslationGlossary(Base):
    __tablename__ = "translation_glossary"
    __table_args__ = (UniqueConstraint("source_text", name="uq_translation_glossary_source_text"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_text: Mapped[str] = mapped_column(String(500), index=True)
    target_text: Mapped[str] = mapped_column(String(500))
    category: Mapped[str] = mapped_column(String(50), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class AsyncTask(Base):
    __tablename__ = "async_tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    message: Mapped[str] = mapped_column(String(1000), default="")
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    task_type: Mapped[str] = mapped_column(String(50), default="")
    game_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_path: Mapped[str] = mapped_column(String(2000), default="")
    copied_files: Mapped[int] = mapped_column(Integer, default=0)
    total_files: Mapped[int] = mapped_column(Integer, default=0)
