from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class GameBase(BaseModel):
    title: str = Field(min_length=1, max_length=255)
    alias: str = ""
    cover_url: str = ""
    screenshots: str = ""
    description: str = ""
    developer: str = ""
    publisher: str = ""
    release_date: date | None = None
    rating: float | None = Field(default=None, ge=0, le=100, multiple_of=0.1)
    tags: str = ""
    series: str = ""
    source_type: str = "custom"
    source_id: str = ""
    resource_type: Literal["nas_cloud", "nas_local", "web_link", "none"] = "none"
    resource_url: str = ""
    play_status: str = "favorite"


class GameCreate(GameBase):
    pass


class GameUpdate(GameBase):
    pass


class GameOut(GameBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    created_at: datetime
    updated_at: datetime


class MetadataResult(BaseModel):
    source_type: str
    source_id: str
    title: str
    alias: str = ""
    cover_url: str = ""
    description: str = ""
    developer: str = ""
    publisher: str = ""
    release_date: date | None = None
    rating: float | None = None
    tags: str = ""
    series: str = ""


class AsyncTaskOut(BaseModel):
    id: str
    status: str
    message: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None
    result_game_id: int | None = None


class RefreshMetadataTaskOut(AsyncTaskOut):
    """单游戏元数据刷新提交后的异步任务状态。"""

    pass


class ScanStatusOut(AsyncTaskOut):
    mode: str | None = None
    scanned_directories: int = 0
    discovered_games: int = 0
    skipped_directories: int = 0
    logs: list[str] = []
    next_run_at: datetime | None = None


class ScanTrigger(BaseModel):
    confirm: bool = False


class GlossaryBase(BaseModel):
    source_text: str = Field(min_length=1, max_length=500)
    target_text: str = Field(min_length=1, max_length=500)
    category: str = Field(default="", max_length=50)


class GlossaryCreate(GlossaryBase):
    pass


class GlossaryUpdate(GlossaryBase):
    pass


class GlossaryOut(GlossaryBase):
    model_config = ConfigDict(from_attributes=True)
    id: int
    created_at: datetime
    updated_at: datetime


class SettingsUpdate(BaseModel):
    scan_enable: bool | None = None
    scan_cron: str | None = Field(default=None, max_length=50)
    scan_throttle_ms: int | None = Field(default=None, ge=0)
    scan_root: str | None = Field(default=None, max_length=500)
    local_game_root: str | None = Field(default=None, max_length=500)
    download_dir: str | None = Field(default=None, max_length=500)
    rawg_api_key: str | None = Field(default=None, max_length=200)
    auto_translate: bool | None = None
    translator_type: Literal["none", "tencent"] | None = None
    tencent_secret_id: str | None = Field(default=None, max_length=200)
    tencent_secret_key: str | None = Field(default=None, max_length=200)
    tencent_region: str | None = Field(default=None, max_length=50)
