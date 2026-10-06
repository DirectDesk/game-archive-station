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
    # aria2 通道：仅用于「有直链的云端网盘」（UC/PiKPak/夸克）；挂载源不走它
    aria_rpc = os.getenv("ARIA_RPC", "http://192.168.20.10:6800/jsonrpc")
    aria_secret = os.getenv("ARIA_SECRET", "")
    aria_secret_file = os.getenv("ARIA_SECRET_FILE", "")


settings = Settings()
