import os
from pathlib import Path


class Settings:
    data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
    database_url = f"sqlite+aiosqlite:///{data_dir / 'games.db'}"
    rawg_api_key = os.getenv("RAWG_API_KEY", "")
    download_timeout = float(os.getenv("DOWNLOAD_TIMEOUT", "60"))
    scan_enable = os.getenv("SCAN_ENABLE", "true").lower() == "true"
    scan_cron = os.getenv("SCAN_CRON", "0 3 * * *")
    scan_throttle_ms = int(os.getenv("SCAN_THROTTLE_MS", "50"))
    scan_weekly_full_check = os.getenv("SCAN_WEEKLY_FULL_CHECK", "false").lower() == "true"
    scan_root = Path(os.getenv("SCAN_ROOT", "/vol/baidu"))


settings = Settings()
