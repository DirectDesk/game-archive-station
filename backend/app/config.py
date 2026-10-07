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
    local_game_root = Path(os.getenv("LOCAL_GAME_ROOT", "/vol/games"))
    download_dir = Path(os.getenv("DOWNLOAD_DIR", "/vol/download/game"))
    # 多线程下载引擎（见 app/download_engine.py）
    # fuse_threads：挂载源分段并发直读的线程数（百度实测 4~8 是甜点，16 反而降速）
    fuse_threads = int(os.getenv("FUSE_THREADS", "8"))
    # 下载方式：internal=内置下载（挂载源并发直读 / 直链源 httpx 多连接）；
    # external=外置下载（直链源交给 aria2，需另行部署，见 README「外置下载」）。
    # ⚠️ 挂载源（fuse，无 http 直链）**始终走内置**，aria2 读不了挂载路径。
    download_engine = os.getenv("DOWNLOAD_ENGINE", "internal")
    # aria2 通道：仅用于「有直链的云端网盘」（UC/PiKPak/夸克）；挂载源不走它
    aria_rpc = os.getenv("ARIA_RPC", "http://192.168.20.10:6800/jsonrpc")
    aria_secret = os.getenv("ARIA_SECRET", "")
    aria_secret_file = os.getenv("ARIA_SECRET_FILE", "")


settings = Settings()
