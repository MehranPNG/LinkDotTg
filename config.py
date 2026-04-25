import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
CONF_DIR = BASE_DIR / "conf"
SESSIONS_DIR = CONF_DIR / "sessions"
FILES_DIR = CONF_DIR / "files"
DB_PATH = CONF_DIR / "bot_database.sqlite3"


def ensure_dirs() -> None:
    CONF_DIR.mkdir(parents=True, exist_ok=True)
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)


ensure_dirs()
load_dotenv(CONF_DIR / ".env")

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
TG_TOKEN = os.getenv("TG_TOKEN")
RUBIKA_SESSION_NAME = str(SESSIONS_DIR / "rubika_session")
RUBIKA_MIRROR_CHANNEL = os.getenv("RUBIKA_MIRROR_CHANNEL", "data_saves").strip()

AUTH_KEY_LENGTH = 64
ZIP_PASSWORD_LENGTH = 32
STATUS_EDIT_INTERVAL = 0.8
ALBUM_COLLECT_DELAY = 1.2
CHUNK_SIZE = 1024 * 1024
MAX_UPLOAD_SIZE = 500 * 1024 * 1024
SERVER_DISK_GB_DEFAULT = 2.0
SERVER_DISK_GB = float(os.getenv("SERVER_DISK_GB", SERVER_DISK_GB_DEFAULT))
SERVER_DISK_BYTES = int(SERVER_DISK_GB * 1024 * 1024 * 1024)
SPECIAL_CHARS = "!@#$%^&*()-_=+[]{};:,.?/"

UPLOAD_TIMEOUT_SECONDS = 20 * 60
AUTO_CANCEL_SECONDS = 20 * 60
RETRY_WINDOW_SECONDS = 5 * 60
MAX_CONCURRENT_PROCESSES = int(os.getenv("MAX_CONCURRENT_PROCESSES", "5"))

DAILY_FREE_QUOTA_MB = int(os.getenv("DAILY_FREE_QUOTA_MB", "100"))
DAILY_FREE_QUOTA_BYTES = DAILY_FREE_QUOTA_MB * 1024 * 1024
ADMIN_IDS = {
    int(item.strip())
    for item in os.getenv("ADMIN_IDS", "1267525462").split(",")
    if item.strip().isdigit()
}
