from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class Game(Base):
    __tablename__ = "games"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(255), index=True)
    alias: Mapped[str] = mapped_column(String(500), default="")
    cover_url: Mapped[str] = mapped_column(String(1000), default="")
    screenshots: Mapped[str] = mapped_column(Text, default="")
    description: Mapped[str] = mapped_column(Text, default="")
    developer: Mapped[str] = mapped_column(String(255), default="")
    publisher: Mapped[str] = mapped_column(String(255), default="")
    release_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    tags: Mapped[str] = mapped_column(String(1000), default="")
    series: Mapped[str] = mapped_column(String(255), default="")
    source_type: Mapped[str] = mapped_column(String(20), default="custom")
    source_id: Mapped[str] = mapped_column(String(100), default="")
    resource_type: Mapped[str] = mapped_column(String(20), default="none")
    resource_url: Mapped[str] = mapped_column(String(2000), default="")
    play_status: Mapped[str] = mapped_column(String(20), default="favorite")
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


class TranslationGlossary(Base):
    __tablename__ = "translation_glossary"
    __table_args__ = (UniqueConstraint("source_text", name="uq_translation_glossary_source_text"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_text: Mapped[str] = mapped_column(String(500), index=True)
    target_text: Mapped[str] = mapped_column(String(500))
    category: Mapped[str] = mapped_column(String(50), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())
