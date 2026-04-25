import asyncio
import html
import json
import os
import re
import shutil
import sqlite3
import time
import pyzipper
import aiohttp
from collections import deque
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from pyrogram import Client, filters as tg_filters, enums
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    ReplyKeyboardMarkup,
)
from rubpy import Client as RubikaClient
from rubpy import handlers as rub_handlers
from urllib.parse import urlparse, unquote
from config import (
    AUTH_KEY_LENGTH,
    ALBUM_COLLECT_DELAY,
    API_HASH,
    API_ID,
    AUTO_CANCEL_SECONDS,
    CHUNK_SIZE,
    DAILY_FREE_QUOTA_BYTES,
    DB_PATH,
    FILES_DIR,
    MAX_UPLOAD_SIZE,
    MAX_CONCURRENT_PROCESSES,
    RETRY_WINDOW_SECONDS,
    RUBIKA_SESSION_NAME,
    RUBIKA_MIRROR_CHANNEL,
    SESSIONS_DIR,
    SERVER_DISK_BYTES,
    STATUS_EDIT_INTERVAL,
    TG_TOKEN,
    UPLOAD_TIMEOUT_SECONDS,
    ADMIN_IDS,
)
from utils import (
    generate_auth_key,
    generate_password,
    human_size,
    mb_text,
    now_ts,
    percent_text,
    random_text,
    safe_name,
)

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
cursor.executescript(
    """
    CREATE TABLE IF NOT EXISTS users (
        telegram_id INTEGER PRIMARY KEY,
        auth_key TEXT,
        rubika_guid TEXT,
        successful_uploads INTEGER DEFAULT 0,
        daily_free_remaining INTEGER DEFAULT 0,
        main_remaining INTEGER DEFAULT 0,
        daily_reset_date TEXT
    );

    CREATE TABLE IF NOT EXISTS batches (
        batch_id TEXT PRIMARY KEY,
        telegram_id INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        total_files INTEGER NOT NULL,
        total_size INTEGER NOT NULL,
        status TEXT NOT NULL,
        password TEXT,
        zip_name TEXT,
        rubika_guid TEXT
    );

    CREATE TABLE IF NOT EXISTS batch_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        telegram_message_id INTEGER NOT NULL,
        file_name TEXT NOT NULL,
        file_size INTEGER NOT NULL,
        local_path TEXT,
        status TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS disk_reservations (
        batch_id TEXT PRIMARY KEY,
        telegram_id INTEGER NOT NULL,
        bytes INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        status TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
    );

    CREATE TABLE IF NOT EXISTS tickets (
        ticket_id INTEGER PRIMARY KEY,
        telegram_id INTEGER NOT NULL,
        message_text TEXT NOT NULL,
        admin_reply TEXT,
        status TEXT NOT NULL DEFAULT 'open',
        created_at INTEGER NOT NULL,
        answered_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS payments (
        payment_code TEXT PRIMARY KEY,
        telegram_id INTEGER NOT NULL,
        plan_id TEXT NOT NULL,
        plan_title TEXT NOT NULL,
        volume_bytes INTEGER NOT NULL,
        stars_amount INTEGER NOT NULL,
        currency TEXT NOT NULL,
        total_amount INTEGER NOT NULL,
        invoice_payload TEXT NOT NULL,
        telegram_payment_charge_id TEXT NOT NULL,
        provider_payment_charge_id TEXT,
        created_at INTEGER NOT NULL
    );
    """
)
user_cols = {
    row["name"] for row in cursor.execute("PRAGMA table_info(users)").fetchall()
}
if "successful_uploads" not in user_cols:
    cursor.execute("ALTER TABLE users ADD COLUMN successful_uploads INTEGER DEFAULT 0")
if "daily_free_remaining" not in user_cols:
    cursor.execute(
        "ALTER TABLE users ADD COLUMN daily_free_remaining INTEGER DEFAULT 0"
    )
if "main_remaining" not in user_cols:
    cursor.execute("ALTER TABLE users ADD COLUMN main_remaining INTEGER DEFAULT 0")
if "daily_reset_date" not in user_cols:
    cursor.execute("ALTER TABLE users ADD COLUMN daily_reset_date TEXT")
conn.commit()

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

telegram_app = Client(
    "telegram_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=TG_TOKEN,
    workdir=str(SESSIONS_DIR),
)

rubika_app = RubikaClient(RUBIKA_SESSION_NAME, timeout=UPLOAD_TIMEOUT_SECONDS)

pending_collections: Dict[str, Dict[str, Any]] = {}
active_batches: Dict[str, Dict[str, Any]] = {}
active_jobs: Dict[str, Dict[str, Any]] = {}
background_tasks: set[asyncio.Task[Any]] = set()
db_lock = asyncio.Lock()
queue_lock = asyncio.Lock()
processing_queue: deque[str] = deque()
running_batches: set[str] = set()
running_users: set[int] = set()
retry_deadlines: Dict[str, Dict[str, int]] = {}
admin_states: Dict[int, Dict[str, Any]] = {}
user_states: Dict[int, Dict[str, Any]] = {}
mirror_target_guid_cache: Dict[str, str] = {}


def tehran_today() -> str:
    return datetime.now(ZoneInfo("Asia/Tehran")).date().isoformat()


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [["👤 حساب کاربری", "💎 خرید حجم"], ["☎ پشتیبانی", "📘 راهنما"]],
        resize_keyboard=True,
    )


def load_volume_plans() -> List[Dict[str, Any]]:
    path = Path(__file__).resolve().parent / "volume_plans.json"
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    plans: List[Dict[str, Any]] = []
    if not isinstance(data, list):
        return plans
    for item in data:
        if not isinstance(item, dict):
            continue
        plan_id = str(item.get("id") or "").strip()
        title = str(item.get("title") or "").strip()
        stars = int(item.get("stars") or 0)
        volume_mb = int(item.get("volume_mb") or 0)
        if not plan_id or not title or stars <= 0 or volume_mb <= 0:
            continue
        plans.append(
            {
                "id": plan_id,
                "title": title,
                "stars": stars,
                "volume_mb": volume_mb,
                "volume_bytes": volume_mb * 1024 * 1024,
                "description": str(item.get("description") or "").strip(),
            }
        )
    return plans


def get_volume_plan(plan_id: str) -> Optional[Dict[str, Any]]:
    for plan in load_volume_plans():
        if plan["id"] == plan_id:
            return plan
    return None


def get_reserved_bytes() -> int:
    row = cursor.execute(
        "SELECT COALESCE(SUM(bytes), 0) AS total FROM disk_reservations WHERE status = 'active'"
    ).fetchone()
    return int(row["total"] or 0)


def get_available_bytes() -> int:
    return max(SERVER_DISK_BYTES - get_reserved_bytes(), 0)


def get_user(telegram_id: int) -> Optional[Dict[str, Any]]:
    row = cursor.execute(
        """
        SELECT telegram_id, auth_key, rubika_guid, successful_uploads,
               daily_free_remaining, main_remaining, daily_reset_date
        FROM users WHERE telegram_id = ?
        """,
        (telegram_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "telegram_id": row["telegram_id"],
        "auth_key": row["auth_key"],
        "rubika_guid": row["rubika_guid"],
        "successful_uploads": int(row["successful_uploads"] or 0),
        "daily_free_remaining": int(row["daily_free_remaining"] or 0),
        "main_remaining": int(row["main_remaining"] or 0),
        "daily_reset_date": row["daily_reset_date"],
    }


def upsert_user(
    telegram_id: int, auth_key: str, rubika_guid: Optional[str] = None
) -> None:
    today = tehran_today()
    existing = get_user(telegram_id)
    keep_rubika_guid = existing["rubika_guid"] if existing else None
    if rubika_guid is not None:
        keep_rubika_guid = rubika_guid
    successful_uploads = existing["successful_uploads"] if existing else 0
    daily_free_remaining = (
        existing["daily_free_remaining"] if existing else DAILY_FREE_QUOTA_BYTES
    )
    if existing and existing.get("daily_reset_date") != today:
        daily_free_remaining = DAILY_FREE_QUOTA_BYTES
    main_remaining = existing["main_remaining"] if existing else 0
    cursor.execute(
        """
        INSERT INTO users
        (telegram_id, auth_key, rubika_guid, successful_uploads, daily_free_remaining, main_remaining, daily_reset_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            auth_key = excluded.auth_key,
            rubika_guid = excluded.rubika_guid,
            successful_uploads = excluded.successful_uploads,
            daily_free_remaining = excluded.daily_free_remaining,
            main_remaining = excluded.main_remaining,
            daily_reset_date = excluded.daily_reset_date
        """,
        (
            telegram_id,
            auth_key,
            keep_rubika_guid,
            successful_uploads,
            daily_free_remaining,
            main_remaining,
            today,
        ),
    )
    conn.commit()


def reset_daily_quotas_if_needed() -> None:
    today = tehran_today()
    row = cursor.execute(
        "SELECT value FROM meta WHERE key = 'daily_reset_date'"
    ).fetchone()
    last_reset = row["value"] if row else None
    if last_reset == today:
        return
    cursor.execute(
        """
        UPDATE users
        SET daily_free_remaining = ?, daily_reset_date = ?
        """,
        (DAILY_FREE_QUOTA_BYTES, today),
    )
    cursor.execute(
        """
        INSERT INTO meta (key, value) VALUES ('daily_reset_date', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (today,),
    )
    conn.commit()


def get_or_create_user(telegram_id: int) -> Dict[str, Any]:
    reset_daily_quotas_if_needed()
    user = get_user(telegram_id)
    if user:
        return user
    key = generate_auth_key()
    upsert_user(telegram_id, key, None)
    return get_user(telegram_id) or {}


def get_user_total_remaining(user: Dict[str, Any]) -> int:
    return int(user.get("daily_free_remaining", 0) or 0) + int(
        user.get("main_remaining", 0) or 0
    )


def consume_user_quota(telegram_id: int, amount: int) -> None:
    if amount <= 0:
        return
    user = get_or_create_user(telegram_id)
    daily = int(user.get("daily_free_remaining", 0) or 0)
    main = int(user.get("main_remaining", 0) or 0)
    use_daily = min(daily, amount)
    remaining = amount - use_daily
    use_main = min(main, remaining)
    cursor.execute(
        """
        UPDATE users
        SET daily_free_remaining = ?, main_remaining = ?, successful_uploads = successful_uploads + 1
        WHERE telegram_id = ?
        """,
        (max(daily - use_daily, 0), max(main - use_main, 0), telegram_id),
    )
    conn.commit()


def update_user_main_volume(telegram_id: int, amount_bytes: int) -> None:
    user = get_or_create_user(telegram_id)
    current = int(user.get("main_remaining", 0) or 0)
    target = max(current + amount_bytes, 0)
    cursor.execute(
        "UPDATE users SET main_remaining = ? WHERE telegram_id = ?",
        (target, telegram_id),
    )
    conn.commit()


reset_daily_quotas_if_needed()


def set_user_rubika_guid(telegram_id: int, rubika_guid: str) -> None:
    cursor.execute(
        "UPDATE users SET rubika_guid = ? WHERE telegram_id = ?",
        (rubika_guid, telegram_id),
    )
    conn.commit()


def disconnect_user(telegram_id: int) -> None:
    cursor.execute(
        "UPDATE users SET rubika_guid = NULL WHERE telegram_id = ?",
        (telegram_id,),
    )
    cursor.execute(
        "UPDATE disk_reservations SET status = 'released' WHERE telegram_id = ? AND status = 'active'",
        (telegram_id,),
    )
    cursor.execute(
        "UPDATE batches SET status = 'released' WHERE telegram_id = ? AND status IN ('pending', 'confirmed', 'active')",
        (telegram_id,),
    )
    conn.commit()


def create_batch_record(
    batch_id: str,
    telegram_id: int,
    total_files: int,
    total_size: int,
    status: str = "pending",
) -> None:
    cursor.execute(
        """
        INSERT INTO batches
        (batch_id, telegram_id, created_at, total_files, total_size, status, password, zip_name, rubika_guid)
        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
        ON CONFLICT(batch_id) DO UPDATE SET
            telegram_id = excluded.telegram_id,
            created_at = excluded.created_at,
            total_files = excluded.total_files,
            total_size = excluded.total_size,
            status = excluded.status
        """,
        (batch_id, telegram_id, now_ts(), total_files, total_size, status),
    )
    conn.commit()


def set_batch_status(
    batch_id: str,
    status: str,
    password: Optional[str] = None,
    zip_name: Optional[str] = None,
    rubika_guid: Optional[str] = None,
) -> None:
    fields = ["status = ?"]
    params: List[Any] = [status]
    if password is not None:
        fields.append("password = ?")
        params.append(password)
    if zip_name is not None:
        fields.append("zip_name = ?")
        params.append(zip_name)
    if rubika_guid is not None:
        fields.append("rubika_guid = ?")
        params.append(rubika_guid)
    params.append(batch_id)
    cursor.execute(f"UPDATE batches SET {', '.join(fields)} WHERE batch_id = ?", params)
    conn.commit()


async def reserve_space(
    telegram_id: int, batch_id: str, bytes_needed: int
) -> Tuple[bool, int]:
    async with db_lock:
        current_available = get_available_bytes()
        if bytes_needed > current_available:
            return False, current_available
        cursor.execute(
            """
            INSERT OR REPLACE INTO disk_reservations (batch_id, telegram_id, bytes, created_at, status)
            VALUES (?, ?, ?, ?, 'active')
            """,
            (batch_id, telegram_id, bytes_needed, now_ts()),
        )
        conn.commit()
        return True, get_available_bytes()


async def reduce_reservation(batch_id: str, amount: int) -> None:
    async with db_lock:
        cursor.execute(
            "UPDATE disk_reservations SET bytes = MAX(0, bytes - ?) WHERE batch_id = ?",
            (amount, batch_id),
        )
        conn.commit()


async def release_space(batch_id: str, status: str = "released") -> None:
    async with db_lock:
        cursor.execute(
            "UPDATE disk_reservations SET status = ? WHERE batch_id = ?",
            (status, batch_id),
        )
        conn.commit()


async def cleanup_paths(*paths: Optional[str]) -> None:
    for path in paths:
        if not path:
            continue
        with suppress(Exception):
            p = Path(path)
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p, ignore_errors=True)


async def admin_cleanup_storage() -> Tuple[int, int]:
    cancelled_jobs = 0
    for job in list(active_jobs.values()):
        task = job.get("task")
        if task and not task.done():
            task.cancel()
            cancelled_jobs += 1

    for batch in list(active_batches.values()):
        task = batch.get("processing_task")
        if task and not task.done():
            task.cancel()
            cancelled_jobs += 1

    pending_collections.clear()
    active_jobs.clear()
    active_batches.clear()
    retry_deadlines.clear()
    processing_queue.clear()
    running_batches.clear()
    running_users.clear()

    removed_entries = 0
    base_dir = Path(FILES_DIR)
    if base_dir.exists():
        for child in base_dir.iterdir():
            removed_entries += 1
            with suppress(Exception):
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink()
    else:
        base_dir.mkdir(parents=True, exist_ok=True)

    cursor.execute(
        "UPDATE disk_reservations SET status = 'released' WHERE status = 'active'"
    )
    cursor.execute("UPDATE batch_items SET local_path = NULL")
    conn.commit()
    return cancelled_jobs, removed_entries


def get_media_info(message) -> Optional[Dict[str, Any]]:
    media = (
        message.document
        or message.video
        or message.audio
        or message.photo
        or message.animation
        or message.voice
        or message.video_note
        or message.sticker
    )
    if not media:
        return None
    file_size = int(getattr(media, "file_size", 0) or 0)
    if message.document and getattr(message.document, "file_name", None):
        file_name = message.document.file_name
    elif message.video and getattr(message.video, "file_name", None):
        file_name = message.video.file_name
    elif message.audio and getattr(message.audio, "file_name", None):
        file_name = message.audio.file_name
    elif message.animation:
        file_name = (
            getattr(message.animation, "file_name", None)
            or f"animation_{message.id}.mp4"
        )
    elif message.voice:
        file_name = f"voice_{message.id}.ogg"
    elif message.video_note:
        file_name = f"video_note_{message.id}.mp4"
    elif message.sticker:
        is_animated = getattr(message.sticker, "is_animated", False) or getattr(
            message.sticker, "is_video", False
        )
        ext = ".tgs" if is_animated else ".webp"
        file_name = f"sticker_{message.id}{ext}"
    elif message.photo:
        file_name = f"photo_{message.id}.jpg"
    else:
        file_name = f"file_{message.id}.bin"
    return {
        "message_id": message.id,
        "file_name": safe_name(file_name),
        "file_size": file_size,
        "message": message,
    }


def batch_key_for_message(message) -> str:
    return f"chat:{message.chat.id}"


def confirm_keyboard(batch_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "❌ لغو", callback_data=f"batch_cancel:{batch_id}"
                ),
                InlineKeyboardButton(
                    "✅ تایید و شروع", callback_data=f"batch_confirm:{batch_id}"
                ),
            ]
        ]
    )


def disconnect_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔌 قطع اتصال روبیکا", callback_data="disconnect_prompt"
                )
            ]
        ]
    )


def disconnect_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬅️ لغو", callback_data="disconnect_cancel"),
                InlineKeyboardButton(
                    "✅ تایید قطع اتصال", callback_data="disconnect_confirm"
                ),
            ]
        ]
    )


def progress_keyboard(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⛔ لغو فرایند", callback_data=f"job_cancel:{job_id}")]]
    )


def job_cancel_confirm_keyboard(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅️ انصراف", callback_data=f"job_cancel_cancel:{job_id}"
                ),
                InlineKeyboardButton(
                    "✅ تایید و لغو", callback_data=f"job_cancel_confirm:{job_id}"
                ),
            ]
        ]
    )


def upload_retry_keyboard(batch_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔄 امتحان مجدد", callback_data=f"upload_retry:{batch_id}"
                ),
            ]
        ]
    )


def admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔎 جستجوی کاربر", callback_data="admin_search_user"
                ),
                InlineKeyboardButton(
                    "🧹 پاک سازی", callback_data="admin_cleanup_prompt"
                ),
            ],
            [
                InlineKeyboardButton("🎫 تیکت‌ها", callback_data="admin_tickets_panel"),
                InlineKeyboardButton(
                    "💳 پرداخت‌ها", callback_data="admin_payments_panel"
                ),
            ],
        ]
    )


def admin_search_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ لغو", callback_data="admin_cancel_search")]]
    )


def admin_user_manage_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➖ کاهش حجم اصلی", callback_data=f"admin_volume_dec:{user_id}"
                ),
                InlineKeyboardButton(
                    "➕ افزایش حجم اصلی", callback_data=f"admin_volume_inc:{user_id}"
                ),
            ],
            [InlineKeyboardButton("⬅️ بازگشت", callback_data="admin_back_main")],
        ]
    )


def support_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ لغو", callback_data="support_cancel")]]
    )


def admin_ticket_reply_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ لغو", callback_data="admin_cancel_ticket_reply")]]
    )


def admin_ticket_view_keyboard(ticket_id: int, answered: bool) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if not answered:
        rows.append(
            [
                InlineKeyboardButton(
                    "✍️ پاسخ", callback_data=f"admin_ticket_reply_prompt:{ticket_id}"
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="admin_tickets_panel")])
    return InlineKeyboardMarkup(rows)


def admin_tickets_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📄 لیست تیکت‌های بی‌پاسخ",
                    callback_data="admin_tickets_unanswered_list",
                )
            ],
            [
                InlineKeyboardButton("⬅️ بازگشت", callback_data="admin_back_main"),
                InlineKeyboardButton(
                    "🔎 جستجوی تیکت", callback_data="admin_ticket_search"
                ),
            ],
        ]
    )


def admin_ticket_notify_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✍️ پاسخ", callback_data=f"admin_ticket_reply_prompt:{ticket_id}"
                )
            ]
        ]
    )


def admin_ticket_search_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ لغو", callback_data="admin_cancel_ticket_search")]]
    )


def buy_volume_entry_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⭐ خرید با استارز", callback_data="buy_stars_plans")]]
    )


def buy_volume_plans_keyboard(plans: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    current: List[InlineKeyboardButton] = []
    for plan in plans:
        button = InlineKeyboardButton(
            f"{plan['title']} - ⭐ {int(plan['stars'])}",
            callback_data=f"buy_plan_select:{plan['id']}",
        )
        current.append(button)
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    rows.append([InlineKeyboardButton("⬅️ بازگشت", callback_data="buy_volume_back")])
    return InlineKeyboardMarkup(rows)


def buy_volume_plan_details_keyboard(
    plan_id: str, stars_amount: int
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"⭐ پرداخت {int(stars_amount)} استار",
                    callback_data=f"buy_plan_pay:{plan_id}",
                )
            ],
            [InlineKeyboardButton("⬅️ بازگشت", callback_data="buy_stars_plans")],
        ]
    )


def payments_stats() -> Tuple[int, int, int]:
    tehran_now = datetime.now(ZoneInfo("Asia/Tehran"))
    today_start = int(
        tehran_now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    )
    row = cursor.execute(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(stars_amount), 0) AS successful_stars,
            SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS today_count
        FROM payments
        """,
        (today_start,),
    ).fetchone()
    return (
        int(row["total"] or 0),
        int(row["successful_stars"] or 0),
        int(row["today_count"] or 0),
    )


def generate_payment_code() -> str:
    while True:
        code = f"PAY-{random_text(10).upper()}"
        exists = cursor.execute(
            "SELECT 1 FROM payments WHERE payment_code = ?",
            (code,),
        ).fetchone()
        if not exists:
            return code


def create_payment_record(
    telegram_id: int,
    plan: Dict[str, Any],
    successful_payment: Any,
) -> str:
    payment_code = generate_payment_code()
    cursor.execute(
        """
        INSERT INTO payments (
            payment_code, telegram_id, plan_id, plan_title, volume_bytes, stars_amount,
            currency, total_amount, invoice_payload, telegram_payment_charge_id,
            provider_payment_charge_id, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payment_code,
            int(telegram_id),
            str(plan["id"]),
            str(plan["title"]),
            int(plan["volume_bytes"]),
            int(plan["stars"]),
            str(successful_payment.currency),
            int(successful_payment.total_amount),
            str(successful_payment.invoice_payload),
            str(successful_payment.telegram_payment_charge_id),
            str(successful_payment.provider_payment_charge_id or ""),
            now_ts(),
        ),
    )
    conn.commit()
    return payment_code


def get_payment_by_code(payment_code: str) -> Optional[sqlite3.Row]:
    return cursor.execute(
        """
        SELECT payment_code, telegram_id, plan_id, plan_title, volume_bytes, stars_amount,
               currency, total_amount, invoice_payload, telegram_payment_charge_id,
               provider_payment_charge_id, created_at
        FROM payments
        WHERE payment_code = ?
        """,
        (payment_code,),
    ).fetchone()


def payment_details_text(payment: sqlite3.Row) -> str:
    created = datetime.fromtimestamp(
        int(payment["created_at"]), ZoneInfo("Asia/Tehran")
    )
    return (
        "💳 اطلاعات پرداخت\n\n"
        f"🧾 کد پرداخت: <code>{html.escape(payment['payment_code'])}</code>\n"
        f"👤 چت آیدی کاربر: <code>{int(payment['telegram_id'])}</code>\n"
        f"📦 پلن: {html.escape(payment['plan_title'])}\n"
        f"📁 حجم افزوده‌شده: {volume_text(int(payment['volume_bytes']))}\n"
        f"⭐ استار پرداختی: {int(payment['stars_amount'])}\n"
        f"💱 ارز: <code>{html.escape(payment['currency'])}</code>\n"
        f"🔢 total_amount: <code>{int(payment['total_amount'])}</code>\n"
        f"🧷 payload: <code>{html.escape(payment['invoice_payload'])}</code>\n"
        f"🆔 telegram_charge_id:\n<code>{html.escape(payment['telegram_payment_charge_id'])}</code>\n"
        f"🕒 زمان: {created.strftime('%Y-%m-%d %H:%M:%S')}"
    )


def admin_payments_overview_text() -> str:
    total, successful_stars, today_count = payments_stats()
    return (
        "💳 مدیریت پرداخت‌ها\n\n"
        f"✅ تعداد پرداخت موفق: {total}\n"
        f"⭐ مجموع استار دریافتی: {successful_stars}\n"
        f"📆 پرداخت‌های امروز: {today_count}"
    )


def admin_payments_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📄 لیست پرداخت‌های موفق",
                    callback_data="admin_payments_success_list",
                )
            ],
            [
                InlineKeyboardButton(
                    "🔎 جستجوی پرداخت",
                    callback_data="admin_payment_search",
                )
            ],
            [InlineKeyboardButton("⬅️ بازگشت", callback_data="admin_back_main")],
        ]
    )


def admin_payment_search_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ لغو", callback_data="admin_cancel_payment_search")]]
    )


def generate_ticket_id() -> int:
    while True:
        ticket_id = 10000 + (int.from_bytes(os.urandom(2), "big") % 90000)
        exists = cursor.execute(
            "SELECT 1 FROM tickets WHERE ticket_id = ?",
            (ticket_id,),
        ).fetchone()
        if not exists:
            return ticket_id


def ticket_id_monospace(ticket_id: int) -> str:
    return f"<code>{int(ticket_id)}</code>"


def create_ticket(telegram_id: int, message_text: str) -> int:
    ticket_id = generate_ticket_id()
    cursor.execute(
        """
        INSERT INTO tickets (ticket_id, telegram_id, message_text, status, created_at)
        VALUES (?, ?, ?, 'open', ?)
        """,
        (ticket_id, telegram_id, message_text, now_ts()),
    )
    conn.commit()
    return ticket_id


def get_ticket(ticket_id: int) -> Optional[sqlite3.Row]:
    return cursor.execute(
        """
        SELECT ticket_id, telegram_id, message_text, admin_reply, status, created_at, answered_at
        FROM tickets
        WHERE ticket_id = ?
        """,
        (ticket_id,),
    ).fetchone()


def answer_ticket(ticket_id: int, reply_text: str) -> None:
    cursor.execute(
        """
        UPDATE tickets
        SET admin_reply = ?, status = 'answered', answered_at = ?
        WHERE ticket_id = ?
        """,
        (reply_text, now_ts(), ticket_id),
    )
    conn.commit()


def tickets_stats() -> Tuple[int, int, int]:
    row = cursor.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'answered' THEN 1 ELSE 0 END) AS answered,
            SUM(CASE WHEN status != 'answered' THEN 1 ELSE 0 END) AS unanswered
        FROM tickets
        """
    ).fetchone()
    return (
        int(row["total"] or 0),
        int(row["answered"] or 0),
        int(row["unanswered"] or 0),
    )


def admin_tickets_overview_text() -> str:
    total, answered, unanswered = tickets_stats()
    return (
        "🎫 مدیریت تیکت‌ها\n\n"
        f"📌 تعداد کل تیکت‌ها: {total}\n"
        f"✅ تعداد پاسخ داده شده‌ها: {answered}\n"
        f"🕓 تعداد بی‌پاسخ‌ها: {unanswered}"
    )


def admin_ticket_details_text(ticket: sqlite3.Row) -> str:
    status_text = "✅ پاسخ داده شده" if ticket["status"] == "answered" else "🕓 بی‌پاسخ"
    reply = (ticket["admin_reply"] or "—").strip() or "—"
    ticket_text = html.escape(ticket["message_text"])
    reply_text = html.escape(reply)
    return (
        "🎫 اطلاعات تیکت\n\n"
        f"🆔 تیکت آیدی: {ticket_id_monospace(int(ticket['ticket_id']))}\n"
        f"👤 چت آیدی کاربر: <code>{int(ticket['telegram_id'])}</code>\n"
        f"📄 متن تیکت:\n{ticket_text}\n\n"
        f"💬 پاسخ ثبت‌شده:\n{reply_text}\n\n"
        f"📍 وضعیت: {status_text}"
    )


def volume_text(size: int) -> str:
    normalized = max(int(size or 0), 0)
    if normalized >= 1024**3:
        return f"{normalized / (1024**3):.2f} گیگابایت"
    return mb_text(normalized)


def user_account_text(user: Dict[str, Any]) -> str:
    return (
        "👤 حساب کاربر\n\n"
        f"🆔 چت آیدی: `{user['telegram_id']}`\n"
        f"✅ تعداد آپلود موفق کل: {int(user.get('successful_uploads', 0))}\n"
        f"🎁 باقی مانده حجم رایگان روزانه: {volume_text(int(user.get('daily_free_remaining', 0)))}\n"
        f"💼 باقی مانده حجم اصلی: {volume_text(int(user.get('main_remaining', 0)))}"
    )


def help_text() -> str:
    return (
        "📘 راهنمای ربات\n\n"
        "<b>🔹 کارایی ربات</b>\n"
        "این ربات فایل‌ها و لینک‌های مستقیم شما را از تلگرام دریافت می‌کند و به حساب روبیکای متصل شما می‌فرستد. "
        "فرایند انتقال به‌صورت مرحله‌ای انجام می‌شود تا پایداری ارسال بهتر باشد.\n\n"
        "<b>🔹 پشتیبانی از لینک مستقیم</b>\n"
        "کافی است لینک معتبر HTTP/HTTPS ارسال کنید. ربات لینک را بررسی کرده، فایل را دریافت می‌کند "
        "و بعد از آماده‌سازی به روبیکا منتقل می‌کند.\n\n"
        "<b>🔹 پشتیبانی از ارسال گروهی فایل</b>\n"
        "می‌توانید چند فایل را پشت‌سرهم بفرستید تا در یک بسته پردازش شوند. "
        "قبل از شروع نهایی، خلاصه بسته نمایش داده می‌شود تا با اطمینان تایید کنید.\n\n"
        "<b>🔹 فشرده سازی و رمزگذاری</b>\n"
        "فایل‌ها پیش از ارسال در یک خروجی فشرده سازی شده و رمزدار قرار می‌گیرند. "
        "این کار برای انتقال منظم‌تر و امن‌تر انجام می‌شود.\n\n"
        "<b>🔹 حجم رایگان روزانه و حجم اصلی</b>\n"
        "هر روز ۱۰۰ مگابایت حجم رایگان دارید و فقط آپلودهای موفق از سهم شما کسر می‌شوند. "
        "اگر حجم اصلی هم داشته باشید، بعد از اتمام سهم روزانه از آن استفاده می‌شود.\n\n"
        "<b>🔹 پیشنهاد زمان ارسال فایل‌های حجیم</b>\n"
        "برای سرعت و پایداری بهتر، ترجیحاً فایل‌های حجیم را در ساعت‌های خلوت‌تر (مثلاً ۳ تا ۸ صبح) ارسال کنید."
    )


def admin_overview_text() -> str:
    total_users = cursor.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    total_uploads = cursor.execute(
        "SELECT COALESCE(SUM(successful_uploads), 0) AS c FROM users"
    ).fetchone()["c"]
    queued = len(processing_queue)
    free_disk = get_available_bytes()
    return (
        "🛠️ پنل ادمین\n\n"
        f"👥 تعداد کل کاربران: {int(total_users)}\n"
        f"📤 تعداد فایل‌های کل آپلود شده: {int(total_uploads)}\n"
        f"⏳ افراد در صف: {queued}\n"
        f"💽 حجم خالی کلی دیسک سرور: {human_size(free_disk)}"
    )


def admin_user_info_text(user: Dict[str, Any]) -> str:
    return (
        "👤 اطلاعات کاربر\n\n"
        f"🆔 چت آیدی: `{user['telegram_id']}`\n"
        f"✅ تعداد آپلود موفق کل: {int(user.get('successful_uploads', 0))}\n"
        f"🎁 باقی مانده حجم رایگان روزانه: {volume_text(int(user.get('daily_free_remaining', 0)))}\n"
        f"💼 باقی مانده حجم اصلی: {volume_text(int(user.get('main_remaining', 0)))}"
    )


async def safe_edit(message, text: str, reply_markup=None):
    try:
        return await message.edit_text(text, reply_markup=reply_markup)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        with suppress(Exception):
            return await message.edit_text(text, reply_markup=reply_markup)
    except Exception:
        return message


async def safe_answer_callback(
    callback_query, text: Optional[str] = None, show_alert: bool = False
):
    with suppress(Exception):
        await callback_query.answer(text or "", show_alert=show_alert)


async def add_eye_reaction(chat_id: int, message_id: int) -> None:
    with suppress(Exception):
        await telegram_app.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction=["👀"],
            is_big=False,
        )


async def clear_reaction(chat_id: int, message_id: int) -> None:
    with suppress(Exception):
        await telegram_app.set_message_reaction(
            chat_id=chat_id,
            message_id=message_id,
            reaction=[],
            is_big=False,
        )


def queue_status_text(position: int, total_queued: int) -> str:
    return (
        "⏳ بسته شما در صف قرار گرفت.\n\n"
        f"🔢 جایگاه در صف: {position} از {total_queued}\n"
        f"⚙️ ظرفیت پردازش همزمان: {MAX_CONCURRENT_PROCESSES}\n\n"
        "مراحل بعدی:\n"
        "1) دانلود فایل‌ها\n"
        "2) فشرده سازی فایل‌ها\n"
        "3) آپلود به روبیکا"
    )


async def refresh_queue_messages_locked() -> None:
    total_queued = len(processing_queue)
    for index, queued_batch_id in enumerate(processing_queue, start=1):
        queued_batch = active_batches.get(queued_batch_id)
        if not queued_batch:
            continue
        queued_message = queued_batch.get("queue_message")
        if not queued_message:
            continue
        text = queue_status_text(index, total_queued)
        if text == queued_batch.get("last_queue_text"):
            continue
        queued_batch["queue_message"] = await safe_edit(queued_message, text)
        queued_batch["last_queue_text"] = text


async def maybe_start_queued_batches() -> None:
    async with queue_lock:
        made_progress = True
        while (
            made_progress
            and len(running_batches) < MAX_CONCURRENT_PROCESSES
            and processing_queue
        ):
            made_progress = False
            for batch_id in list(processing_queue):
                batch = active_batches.get(batch_id)
                if not batch or batch.get("status") not in {"queued", "confirmed"}:
                    with suppress(ValueError):
                        processing_queue.remove(batch_id)
                    made_progress = True
                    continue
                telegram_id = int(batch["telegram_id"])
                if telegram_id in running_users:
                    continue
                processing_queue.remove(batch_id)
                running_batches.add(batch_id)
                running_users.add(telegram_id)
                batch["status"] = "active"
                set_batch_status(batch_id, "active")
                batch["processing_task"] = asyncio.create_task(
                    process_confirmed_batch(batch_id, telegram_id)
                )
                made_progress = True
                if len(running_batches) >= MAX_CONCURRENT_PROCESSES:
                    break
        await refresh_queue_messages_locked()


async def enqueue_batch(batch_id: str) -> int:
    async with queue_lock:
        if batch_id not in processing_queue:
            processing_queue.append(batch_id)
        await refresh_queue_messages_locked()
        return list(processing_queue).index(batch_id) + 1


async def update_status(job: Dict[str, Any], stage: str, force: bool = False) -> None:
    if "status_lock" not in job:
        job["status_lock"] = asyncio.Lock()

    async with job["status_lock"]:
        if job.get("cancel_confirm_shown"):
            return
        if job.get("cancelled"):
            raise asyncio.CancelledError()

        total_size = int(job.get("total_size", 0) or 0)
        current_file_name = job.get("current_file_name")
        current_file_done = int(job.get("current_file_done", 0) or 0)
        current_file_total = int(job.get("current_file_total", 0) or 0)
        package_done = int(job.get("package_done", 0) or 0)
        total_files = int(job.get("total_files", 0) or 0)
        speed_text = job.get("speed_text")
        lines = [stage]
        if stage == "📥 در حال دانلود...":
            if speed_text:
                lines.append(f"🚀 سرعت: {speed_text}")
            if current_file_name:
                lines.append(f"📄 فایل فعلی: {current_file_name}")
            if total_files > 1:
                lines.append(
                    f"📤 پیشرفت فایل: {mb_text(current_file_done)} / {mb_text(current_file_total)}"
                )
            lines.append(
                f"📦 پیشرفت کل: {mb_text(package_done)} / {mb_text(total_size)}"
            )
            lines.append(f"📊 درصد پیشرفت کل: {percent_text(package_done, total_size)}")
        elif stage == "🗜️ در حال فشرده‌سازی...":
            if current_file_name:
                lines.append(f"📄 نام فایل: {current_file_name}")
            lines.append(
                f"📦 حجم: {mb_text(current_file_done)} / {mb_text(current_file_total)}"
            )
            lines.append(
                f"📊 درصد پیشرفت: {percent_text(current_file_done, current_file_total)}"
            )
        elif stage == "☁️ در حال آپلود به روبیکا...":
            if current_file_name:
                lines.append(f"📄 نام فایل: {current_file_name}")
            lines.append(f"📦 حجم کل: {mb_text(current_file_total)}")
        else:
            if speed_text:
                lines.append(f"🚀 سرعت: {speed_text}")
            if current_file_name:
                lines.append(f"📄 فایل فعلی: {current_file_name}")
            lines.append(
                f"📤 فایل فعلی: {human_size(current_file_done)} / {human_size(current_file_total)}"
            )
            lines.append(
                f"📦 کل بسته: {human_size(package_done)} / {human_size(total_size)}"
            )
            lines.append(f"📊 پیشرفت بسته: {percent_text(package_done, total_size)}")
        text = "\n".join(lines)

        now = time.monotonic()
        if (
            not force
            and text == job.get("last_text")
            and now - job.get("last_edit", 0) < STATUS_EDIT_INTERVAL
        ):
            return
        if not force and now - job.get("last_edit", 0) < STATUS_EDIT_INTERVAL:
            return

        job["status_message"] = await safe_edit(
            job["status_message"],
            text,
            reply_markup=progress_keyboard(job["job_id"]),
        )
        job["last_text"] = text
        job["last_edit"] = now


async def monitored_download(
    message,
    target_path: Path,
    job: Dict[str, Any],
    file_name: str,
    file_index: int,
    file_count: int,
    total_before: int,
    url: Optional[str] = None,
) -> str:
    total = 0
    if not url:
        media = (
            message.document
            or message.video
            or message.audio
            or message.photo
            or message.animation
            or message.voice
            or message.video_note
            or message.sticker
        )
        total = int(getattr(media, "file_size", 0) or 0)

    job["current_file_name"] = file_name
    job["current_file_total"] = total
    job["current_file_done"] = 0
    job["downloaded_files"] = file_index - 1
    job["downloaded_bytes"] = total_before
    job["download_speed"] = "0.0"
    job["speed_text"] = "0.0 MB/s"
    await update_status(job, "📥 در حال دانلود...", force=True)

    last_percent = -1
    last_time = time.monotonic()
    last_bytes = 0

    async def progress(current: int, total_bytes: int):
        nonlocal last_percent, last_time, last_bytes
        if job.get("cancelled"):
            raise asyncio.CancelledError()
        total_value = int(total_bytes or total or 0)
        current_value = int(current or 0)
        percent = int((current_value / total_value) * 100) if total_value else 0
        now = time.monotonic()
        if (
            current_value == total_value
            or percent != last_percent
            or now - last_time >= 1.0
        ):
            delta_time = now - last_time
            delta_bytes = current_value - last_bytes
            if delta_time > 0:
                speed = delta_bytes / delta_time / (1024**2)
                job["speed_text"] = f"{speed:.1f} MB/s"
            else:
                job["speed_text"] = "0.0 MB/s"
            last_percent = percent
            last_bytes = current_value
            last_time = now
            job["current_file_done"] = current_value
            job["current_file_total"] = total_value
            job["downloaded_bytes"] = total_before + current_value
            job["package_done"] = total_before + current_value
            job["downloaded_files"] = (
                file_index - 1 + (1 if current_value == total_value else 0)
            )
            await update_status(job, "📥 در حال دانلود...")

    if url:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                total = int(resp.headers.get("Content-Length", total) or total)
                job["current_file_total"] = total
                current_value = 0
                with open(target_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        if job.get("cancelled"):
                            raise asyncio.CancelledError()
                        f.write(chunk)
                        current_value += len(chunk)
                        await progress(current_value, total)
    else:
        path = await message.download(file_name=str(target_path), progress=progress)

    if job.get("cancelled"):
        raise asyncio.CancelledError()
    job["downloaded_bytes"] = total_before + total
    job["package_done"] = total_before + total
    job["downloaded_files"] = file_index
    job["current_file_done"] = total
    job["current_file_total"] = total
    job["speed_text"] = "0.0 MB/s"
    await update_status(job, "📥 در حال دانلود...", force=True)
    return str(target_path)


async def zip_files_with_progress(
    job: Dict[str, Any],
    files: List[Dict[str, Any]],
    zip_path: Path,
    password: str,
) -> str:
    total_size = sum(item["file_size"] for item in files)
    processed = 0
    job["processed_files"] = 0
    job["processed_bytes"] = 0
    job["speed_text"] = "0.0 MB/s"
    job["current_file_name"] = None
    await update_status(job, "🗜️ در حال فشرده‌سازی...", force=True)
    last_time = time.monotonic()
    with pyzipper.AESZipFile(
        zip_path,
        "w",
        compression=pyzipper.ZIP_DEFLATED,
        encryption=pyzipper.WZ_AES,
    ) as zf:
        zf.setpassword(password.encode())
        for index, item in enumerate(files, start=1):
            if job.get("cancelled"):
                raise asyncio.CancelledError()
            source_path = item["local_path"]
            arcname = item["file_name"]
            file_size = item["file_size"]
            current_done = 0
            job["current_file_name"] = arcname
            job["current_file_total"] = file_size
            job["current_file_done"] = 0
            job["last_processed_file_done"] = 0
            await update_status(job, "🗜️ در حال فشرده‌سازی...")
            with open(source_path, "rb") as src, zf.open(arcname, "w") as dest:
                while True:
                    if job.get("cancelled"):
                        raise asyncio.CancelledError()
                    chunk = src.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    dest.write(chunk)
                    current_done += len(chunk)
                    processed += len(chunk)
                    await asyncio.sleep(0)
                    now = time.monotonic()
                    if current_done == file_size or now - last_time >= 1.0:
                        delta_time = now - last_time
                        if delta_time > 0:
                            delta_bytes = current_done - int(
                                job.get("last_processed_file_done", 0) or 0
                            )
                            speed = max(delta_bytes, 0) / delta_time / (1024**2)
                            job["speed_text"] = f"{speed:.1f} MB/s"
                        else:
                            job["speed_text"] = "0.0 MB/s"
                        job["last_processed_file_done"] = current_done
                        last_time = now
                        job["processed_bytes"] = processed
                        job["package_done"] = processed
                        job["processed_files"] = (
                            index - 1 + (1 if current_done >= file_size else 0)
                        )
                        job["current_file_done"] = current_done
                        job["current_file_total"] = file_size
                        await update_status(job, "🗜️ در حال فشرده‌سازی...")
            with suppress(Exception):
                os.remove(source_path)
            await reduce_reservation(job["batch_id"], file_size)

    job["processed_bytes"] = total_size
    job["package_done"] = total_size
    job["speed_text"] = "0.0 MB/s"
    job["processed_files"] = len(files)
    await update_status(job, "🗜️ فشرده‌سازی کامل شد", force=True)
    return str(zip_path)


async def monitored_rubika_send(
    file_path: str,
    file_name: str,
    job: Optional[Dict[str, Any]],
    rubika_guid: str,
) -> Any:
    size = os.path.getsize(file_path)
    if job is not None:
        job["uploaded_bytes"] = 0
        job["uploaded_files"] = 0
        job["current_file_name"] = file_name
        job["current_file_done"] = 0
        job["current_file_total"] = size
        job["package_done"] = 0
        job["speed_text"] = None
        await update_status(job, "☁️ در حال آپلود به روبیکا...", force=True)

    sent = None
    primary_task = None
    fallback_task = None
    try:
        primary_task = asyncio.create_task(
            rubika_app.send_document(
                object_guid=rubika_guid,
                document=file_path,
                caption=file_name,
            )
        )
        sent = await asyncio.wait_for(primary_task, timeout=UPLOAD_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        if primary_task and not primary_task.done():
            primary_task.cancel()
        raise TimeoutError("Timeout20")
    except Exception as e1:
        try:
            fallback_task = asyncio.create_task(
                rubika_app.send_message(
                    object_guid=rubika_guid,
                    text=file_name,
                    file_inline=file_path,
                    type="File",
                )
            )
            sent = await asyncio.wait_for(fallback_task, timeout=UPLOAD_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            if fallback_task and not fallback_task.done():
                fallback_task.cancel()
            raise TimeoutError("Timeout20")
        except Exception as e2:
            raise RuntimeError(f"خطا در ارسال به روبیکا: {e1} | {e2}")

    if job is not None and job.get("cancelled"):
        raise asyncio.CancelledError()

    if job is not None:
        job["uploaded_bytes"] = size
        job["uploaded_files"] = 1
        job["current_file_done"] = size
        job["current_file_total"] = size
        job["package_done"] = size
        await update_status(job, "☁️ آپلود به روبیکا کامل شد", force=True)
    return sent


def normalize_mirror_target(value: str) -> str:
    target = (value or "").strip()
    if not target:
        return ""
    if target.startswith("@") or target.startswith("c0"):
        return target
    return f"@{target}"


def extract_chat_guid(chat_info: Any) -> Optional[str]:
    if chat_info is None:
        return None

    candidates: List[Any] = []
    if isinstance(chat_info, dict):
        candidates.extend(
            [
                chat_info.get("object_guid"),
                chat_info.get("chat_guid"),
                chat_info.get("guid"),
            ]
        )
        data = chat_info.get("data")
        if isinstance(data, dict):
            chat = data.get("chat")
            if isinstance(chat, dict):
                candidates.extend(
                    [chat.get("object_guid"), chat.get("chat_guid"), chat.get("guid")]
                )

    for key in ("object_guid", "chat_guid", "guid"):
        candidates.append(getattr(chat_info, key, None))

    data_attr = getattr(chat_info, "data", None)
    if data_attr is not None:
        for key in ("object_guid", "chat_guid", "guid"):
            candidates.append(getattr(data_attr, key, None))
        if isinstance(data_attr, dict):
            chat_dict = data_attr.get("chat")
            if isinstance(chat_dict, dict):
                candidates.extend(
                    [
                        chat_dict.get("object_guid"),
                        chat_dict.get("chat_guid"),
                        chat_dict.get("guid"),
                    ]
                )

    for item in candidates:
        if not item:
            continue
        text = str(item).strip()
        if text.startswith("c0"):
            return text
    return None


async def resolve_mirror_target_guid(target: str) -> Optional[str]:
    if not target:
        return None
    if target in mirror_target_guid_cache:
        return mirror_target_guid_cache[target]
    if target.startswith("c0"):
        mirror_target_guid_cache[target] = target
        return target

    lookup_candidates = [target]
    if target.startswith("@"):
        lookup_candidates.append(target[1:])
    else:
        lookup_candidates.append(f"@{target}")

    for candidate in lookup_candidates:
        try:
            info = await rubika_app.get_chat_info(candidate)
        except Exception:
            continue
        guid = extract_chat_guid(info)
        if guid:
            mirror_target_guid_cache[target] = guid
            mirror_target_guid_cache[candidate] = guid
            return guid
    return None


def extract_rubika_message_id(sent: Any) -> Optional[str]:
    if sent is None:
        return None
    candidates: List[Any] = []
    if isinstance(sent, dict):
        candidates.extend(
            [
                sent.get("message_id"),
                sent.get("msg_id"),
                sent.get("id"),
                (
                    sent.get("data", {}).get("message_id")
                    if isinstance(sent.get("data"), dict)
                    else None
                ),
            ]
        )
    for key in ("message_id", "msg_id", "id"):
        candidates.append(getattr(sent, key, None))
    nested = getattr(sent, "data", None)
    if nested is not None:
        candidates.append(getattr(nested, "message_id", None))
        if isinstance(nested, dict):
            candidates.append(nested.get("message_id"))
    new_message = getattr(sent, "new_message", None)
    if new_message is not None:
        candidates.append(getattr(new_message, "message_id", None))
        if isinstance(new_message, dict):
            candidates.append(new_message.get("message_id"))
    for item in candidates:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            return text
    return None


async def forward_to_mirror_in_background(source_guid: str, sent: Any) -> None:
    mirror_target = normalize_mirror_target(RUBIKA_MIRROR_CHANNEL)
    if not mirror_target:
        return
    mirror_target_guid = await resolve_mirror_target_guid(mirror_target)
    if not mirror_target_guid:
        print(f"[mirror] failed to resolve mirror target guid: {mirror_target}")
        return
    message_id = extract_rubika_message_id(sent)
    if not message_id:
        print("[mirror] message_id was not found; skip mirror forward")
        return
    try:
        await rubika_app.forward_messages(
            from_object_guid=source_guid,
            message_ids=[message_id],
            to_object_guid=mirror_target_guid,
        )
    except Exception as mirror_error:
        print(
            f"[mirror] failed to forward message {message_id} to {mirror_target_guid}: {mirror_error}"
        )


def schedule_background(coro: Any) -> None:
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


async def cleanup_upload_failed(batch_id: str) -> None:
    download_dir = FILES_DIR / batch_id
    zip_path = FILES_DIR / f"{batch_id}.zip"
    await cleanup_paths(str(zip_path))
    await cleanup_paths(str(download_dir))
    cursor.execute(
        "UPDATE batch_items SET local_path = NULL WHERE batch_id = ?",
        (batch_id,),
    )
    conn.commit()


async def cron_watchdog() -> None:
    while True:
        reset_daily_quotas_if_needed()
        now = now_ts()
        for job_id, job in list(active_jobs.items()):
            deadline = int(job.get("auto_cancel_deadline", 0) or 0)
            if deadline and now >= deadline and not job.get("cancelled"):
                job["cancelled"] = True
                task = job.get("task")
                if task and not task.done():
                    task.cancel()
        for batch_id, data in list(retry_deadlines.items()):
            deadline = int(data.get("deadline", 0) or 0)
            if now < deadline:
                continue
            batch = active_batches.get(batch_id)
            if not batch or not batch.get("upload_failed", False):
                retry_deadlines.pop(batch_id, None)
                continue
            chat_id = int(data["chat_id"])
            retry_msg_id = int(data["retry_msg_id"])
            with suppress(Exception):
                await telegram_app.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=retry_msg_id,
                    reply_markup=None,
                )
            set_batch_status(batch_id, "cancelled")
            await cleanup_upload_failed(batch_id)
            active_batches.pop(batch_id, None)
            retry_deadlines.pop(batch_id, None)
        await asyncio.sleep(1)


def get_rubika_chat_guid(update) -> Optional[str]:
    candidates = [
        getattr(update, "object_guid", None),
        getattr(update, "chat_guid", None),
        getattr(update, "chat_id", None),
        getattr(update, "guid", None),
    ]
    new_message = getattr(update, "new_message", None)
    if new_message is not None:
        candidates.extend(
            [
                getattr(new_message, "object_guid", None),
                getattr(new_message, "chat_guid", None),
                getattr(new_message, "chat_id", None),
                getattr(new_message, "guid", None),
                getattr(new_message, "author_object_guid", None),
            ]
        )
    for candidate in candidates:
        if candidate:
            return str(candidate)
    return None


def get_rubika_text(update) -> str:
    new_message = getattr(update, "new_message", None)
    if new_message is not None:
        for attr in ("text", "message", "caption"):
            value = getattr(new_message, attr, None)
            if isinstance(value, str) and value.strip():
                return value.strip()
    for attr in ("text", "message", "caption"):
        value = getattr(update, attr, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


async def rubika_reply(update, text: str):
    reply = getattr(update, "reply", None)
    if callable(reply):
        try:
            return await reply(text)
        except Exception:
            pass
    chat_guid = get_rubika_chat_guid(update)
    if chat_guid:
        return await rubika_app.send_message(object_guid=chat_guid, text=text)
    return None


async def create_confirmation_prompt(chat_id: int, batch: Dict[str, Any]) -> None:
    available = get_available_bytes()
    file_lines = [
        f"• {item['file_name']} — {human_size(item['file_size'])}"
        for item in batch["files"]
    ]
    details = "\n".join(file_lines[:10])
    if len(file_lines) > 10:
        details += f"\n• و {len(file_lines) - 10} فایل دیگر"
    text = (
        f"📦 بسته آماده شد\n\n"
        f"🔢 تعداد فایل‌ها: {batch['total_files']}\n"
        f"💾 حجم کل: {human_size(batch['total_size'])}\n"
        f"🧮 فضای خالی فعلی: {human_size(available)}\n\n"
        f"{details}\n\n"
        f"✅ با تایید، همه فایل‌ها به‌صورت فشرده سازی شده و ارسال می‌شوند."
    )
    msg = await telegram_app.send_message(
        chat_id,
        text,
        reply_markup=confirm_keyboard(batch["batch_id"]),
    )
    batch["prompt_message_id"] = msg.id
    active_batches[batch["batch_id"]] = batch


async def finalize_collection(collection_key: str) -> None:
    await asyncio.sleep(ALBUM_COLLECT_DELAY)
    batch = pending_collections.pop(collection_key, None)
    if not batch:
        return
    total_size = sum(item["file_size"] for item in batch["files"])
    total_files = len(batch["files"])
    if total_files == 0:
        return
    batch_id = random_text(20)
    batch["batch_id"] = batch_id
    batch["total_size"] = total_size
    batch["total_files"] = total_files
    batch["last_source_message_id"] = batch["files"][-1]["message_id"]
    batch["status"] = "pending"
    create_batch_record(
        batch_id, batch["telegram_id"], total_files, total_size, "pending"
    )
    for item in batch["files"]:
        cursor.execute(
            """
            INSERT INTO batch_items
            (batch_id, telegram_message_id, file_name, file_size, local_path, status)
            VALUES (?, ?, ?, ?, NULL, 'pending')
            """,
            (batch_id, item["message_id"], item["file_name"], item["file_size"]),
        )
    conn.commit()
    await create_confirmation_prompt(batch["chat_id"], batch)


async def queue_media_message(message) -> None:
    user = get_or_create_user(message.chat.id)
    if not user or not user.get("rubika_guid"):
        await message.reply_text("🔐 ابتدا اتصال روبیکا را برقرار کنید.")
        return

    media = None
    if message.text:
        urls = re.findall(r"(https?://[^\s]+)", message.text)
        if urls:
            url = urls[0]
            msg = await message.reply_text("🔍 در حال بررسی لینک...")
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.head(url, allow_redirects=True) as resp:
                        size = int(resp.headers.get("Content-Length", 0))
                        filename = "downloaded_file"
                        cd = resp.headers.get("Content-Disposition")
                        if cd and "filename=" in cd:
                            filename = cd.split("filename=")[1].strip("\"'")
                        else:
                            parsed = urlparse(str(resp.url))
                            name = Path(unquote(parsed.path)).name
                            if name:
                                filename = name
                        media = {
                            "message_id": message.id,
                            "file_name": safe_name(filename),
                            "file_size": size,
                            "url": url,
                            "message": message,
                        }
            except Exception:
                await msg.edit_text("❌ خطا در بررسی لینک.")
                return
            await msg.delete()
        else:
            return
    else:
        media = get_media_info(message)

    if not media:
        return
    if media["file_size"] > MAX_UPLOAD_SIZE:
        await message.reply_text("📛 حجم هر آپلود نباید بیشتر از 500 مگابایت باشد.")
        return
    collection_key = batch_key_for_message(message)
    batch = pending_collections.get(collection_key)
    if not batch:
        batch = {
            "telegram_id": message.chat.id,
            "chat_id": message.chat.id,
            "files": [],
            "reaction_message_id": None,
        }
        pending_collections[collection_key] = batch
    task = batch.get("task")
    if task and not task.done():
        task.cancel()
    previous_reaction_message_id = batch.get("reaction_message_id")
    if previous_reaction_message_id:
        await clear_reaction(message.chat.id, int(previous_reaction_message_id))
    await add_eye_reaction(message.chat.id, int(message.id))
    batch["reaction_message_id"] = message.id
    batch["files"].append(media)
    batch["task"] = asyncio.create_task(finalize_collection(collection_key))


async def process_confirmed_batch(batch_id: str, chat_id: int) -> None:
    batch = active_batches.get(batch_id)
    if not batch:
        return
    if batch.get("status") in {"done"}:
        return
    if batch["total_size"] > MAX_UPLOAD_SIZE:
        await telegram_app.send_message(
            chat_id, "📛 حجم کل این بسته بیشتر از 500 مگابایت است."
        )
        return
    ok, available = await reserve_space(chat_id, batch_id, batch["total_size"] * 2)
    if not ok:
        await telegram_app.send_message(
            chat_id,
            f"⛔️ فضای کافی فعلا نداریم.\n\nفضای خالی: {human_size(available)}\nحجم موردنیاز: {human_size(batch['total_size'] * 2)}",
        )
        return
    batch["status"] = "active"
    set_batch_status(batch_id, "active")
    job_id = random_text(18)
    status_message = await telegram_app.send_message(
        chat_id,
        "⏳ در حال آماده‌سازی...",
        reply_markup=progress_keyboard(job_id),
    )
    job = {
        "job_id": job_id,
        "batch_id": batch_id,
        "telegram_id": chat_id,
        "status_message": status_message,
        "status_lock": asyncio.Lock(),
        "cancelled": False,
        "last_text": None,
        "last_edit": 0,
        "total_size": batch["total_size"],
        "total_files": batch["total_files"],
        "downloaded_files": 0,
        "downloaded_bytes": 0,
        "processed_files": 0,
        "processed_bytes": 0,
        "uploaded_files": 0,
        "uploaded_bytes": 0,
        "package_done": 0,
        "current_file_name": None,
        "current_file_total": 0,
        "current_file_done": 0,
        "cancel_confirm_shown": False,
        "cancel_prompt_message": None,
        "auto_cancel_deadline": now_ts() + AUTO_CANCEL_SECONDS,
        "task": asyncio.current_task(),
    }
    active_jobs[job_id] = job
    batch["job_id"] = job_id
    download_dir = FILES_DIR / batch_id
    zip_path = FILES_DIR / f"{batch_id}.zip"
    local_paths: List[str] = []
    password = generate_password()
    zip_name = safe_name(zip_path.name)
    try:
        download_dir.mkdir(parents=True, exist_ok=True)
        total_before = 0
        for index, item in enumerate(batch["files"], start=1):
            if job.get("cancelled"):
                raise asyncio.CancelledError()
            source_message = item["message"]
            dest_name = safe_name(item["file_name"])
            target_path = download_dir / dest_name
            local_path = await monitored_download(
                source_message,
                target_path,
                job,
                dest_name,
                index,
                batch["total_files"],
                total_before,
                item.get("url"),
            )
            local_paths.append(local_path)
            total_before += item["file_size"]
            cursor.execute(
                """
                UPDATE batch_items
                SET local_path = ?, status = 'downloaded'
                WHERE batch_id = ? AND telegram_message_id = ?
                """,
                (local_path, batch_id, item["message_id"]),
            )
            conn.commit()
        if not local_paths:
            raise RuntimeError("هیچ فایلی برای پردازش پیدا نشد")
        files_for_zip = []
        for i in range(len(batch["files"])):
            files_for_zip.append(
                {
                    "local_path": local_paths[i],
                    "file_name": batch["files"][i]["file_name"],
                    "file_size": batch["files"][i]["file_size"],
                }
            )
        await zip_files_with_progress(job, files_for_zip, zip_path, password)
        if job.get("cancelled"):
            raise asyncio.CancelledError()
        for item in batch["files"]:
            cursor.execute(
                """
                UPDATE batch_items
                SET status = 'zipped'
                WHERE batch_id = ? AND telegram_message_id = ?
                """,
                (batch_id, item["message_id"]),
            )
        conn.commit()

        rubika_guid = batch.get("rubika_guid")
        if not rubika_guid:
            user = get_user(chat_id)
            rubika_guid = user["rubika_guid"] if user else None
        if not rubika_guid:
            raise RuntimeError("اتصال روبیکا پیدا نشد")

        upload_success = False
        upload_error = None
        sent_message = None
        try:
            sent_message = await monitored_rubika_send(
                str(zip_path), zip_name, job, rubika_guid
            )
            upload_success = True
        except Exception as e:
            upload_error = e
            upload_success = False

        if upload_success:
            consume_user_quota(chat_id, int(batch["total_size"]))
            set_batch_status(
                batch_id,
                "done",
                password=password,
                zip_name=zip_name,
                rubika_guid=rubika_guid,
            )
            await safe_edit(
                job["status_message"],
                "✅ عملیات با موفقیت به پایان رسید.",
                reply_markup=None,
            )
            await telegram_app.send_message(
                chat_id,
                f"✅ ارسال با موفقیت انجام شد\n\n🗂️ نام فایل:\n`{zip_name}`\n🔐 پسورد فایل:\n`{password}`",
                reply_to_message_id=batch.get("last_source_message_id"),
            )
            schedule_background(
                forward_to_mirror_in_background(rubika_guid, sent_message)
            )
        else:
            set_batch_status(
                batch_id,
                "upload_failed",
                password=password,
                zip_name=zip_name,
                rubika_guid=rubika_guid,
            )
            batch["upload_failed"] = True
            batch["zip_name"] = zip_name
            batch["password"] = password
            batch["rubika_guid"] = rubika_guid
            error_text = f"❌ خطا در آپلود به روبیکا یا اتمام زمان مجاز.\n\n{str(upload_error)[:500]}\n\n🔄 برای امتحان مجدد روی دکمه زیر کلیک کنید. (تا ۵ دقیقه اعتبار دارد)"
            retry_markup = upload_retry_keyboard(batch_id)
            retry_msg = await telegram_app.send_message(
                chat_id,
                error_text,
                reply_markup=retry_markup,
            )
            retry_deadlines[batch_id] = {
                "chat_id": chat_id,
                "retry_msg_id": retry_msg.id,
                "deadline": now_ts() + RETRY_WINDOW_SECONDS,
            }
    except asyncio.CancelledError:
        set_batch_status(batch_id, "cancelled")
        cursor.execute(
            "UPDATE batch_items SET status = 'cancelled', local_path = NULL WHERE batch_id = ?",
            (batch_id,),
        )
        conn.commit()
        prompt_message = job.get("cancel_prompt_message")
        if prompt_message:
            with suppress(Exception):
                await prompt_message.delete()
        job["cancel_prompt_message"] = None
        with suppress(Exception):
            await safe_edit(
                job["status_message"], "⛔ فرایند لغو شد", reply_markup=None
            )
    except Exception as exc:
        set_batch_status(batch_id, "failed")
        cursor.execute(
            "UPDATE batch_items SET status = 'failed', local_path = NULL WHERE batch_id = ?",
            (batch_id,),
        )
        conn.commit()
        with suppress(Exception):
            await telegram_app.send_message(
                chat_id,
                f"❌ خطا در پردازش فایل\n\n{str(exc)[:500]}",
            )
    finally:
        async with queue_lock:
            with suppress(ValueError):
                processing_queue.remove(batch_id)
            running_batches.discard(batch_id)
            running_users.discard(chat_id)
        await release_space(batch_id, status="released")
        should_cleanup = True
        if batch_id in active_batches and active_batches[batch_id].get(
            "upload_failed", False
        ):
            should_cleanup = False
        if should_cleanup:
            await cleanup_paths(*local_paths)
            await cleanup_paths(str(zip_path))
            await cleanup_paths(str(download_dir))
        if batch_id in retry_deadlines:
            retry_deadlines.pop(batch_id, None)
        active_jobs.pop(job_id, None)
        if batch_id in active_batches and not active_batches[batch_id].get(
            "upload_failed", False
        ):
            active_batches.pop(batch_id, None)
        await maybe_start_queued_batches()


async def on_rubika_update(update):
    text = get_rubika_text(update)
    chat_guid = get_rubika_chat_guid(update)
    if not chat_guid:
        return
    if text in {"/start", "start", "/help"}:
        linked = cursor.execute(
            "SELECT telegram_id FROM users WHERE rubika_guid = ? LIMIT 1",
            (chat_guid,),
        ).fetchone()
        if linked:
            await rubika_reply(update, "✅ اتصال فعال است")
        return
    if len(text) != AUTH_KEY_LENGTH:
        return
    linked = cursor.execute(
        "SELECT telegram_id FROM users WHERE rubika_guid = ? LIMIT 1",
        (chat_guid,),
    ).fetchone()
    if linked:
        await rubika_reply(update, "✅ از قبل ثبت شده")
        return
    row = cursor.execute(
        "SELECT telegram_id FROM users WHERE auth_key = ?",
        (text,),
    ).fetchone()
    if row:
        telegram_id = row["telegram_id"]
        set_user_rubika_guid(telegram_id, chat_guid)
        await rubika_reply(update, "✅ اتصال برقرار شد")
        await telegram_app.send_message(
            telegram_id,
            f"✅ اتصال روبیکا برقرار شد\n\n🤖 حساب متصل: `{chat_guid}`\n\n📤 فایل یا لینک مستقیم خود را ارسال کنید.",
        )
    else:
        await rubika_reply(update, "❌ کلید نامعتبر است")


@telegram_app.on_message(tg_filters.command("start") & tg_filters.private)
async def tg_start(client, message):
    user = get_or_create_user(message.chat.id)
    key = generate_auth_key()
    upsert_user(message.chat.id, key, user["rubika_guid"] if user else None)
    available = get_available_bytes()
    current_user = get_or_create_user(message.chat.id)
    if current_user and current_user.get("rubika_guid"):
        text = (
            f"✅ اتصال شما برقرار است\n\n"
            f"🤖 حساب روبیکا: `{current_user['rubika_guid']}`\n\n"
            f"📤 فایل‌ها یا لینک‌های مستقیم خود را ارسال کنید تا در این حساب دریافت شوند.\n\n"
            f"🧮 فضای خالی فعلی: {human_size(available)}\n\n"
            f"🔌 برای قطع اتصال از دکمه زیر استفاده کنید."
        )
        await message.reply_text(
            text,
            reply_markup=disconnect_keyboard(),
        )
        await telegram_app.send_message(
            message.chat.id, "منو:", reply_markup=main_menu_keyboard()
        )
    else:
        text = (
            f"👋 خوش آمدید\n\n"
            f"🔑 کلید دسترسی شما:\n`{key}`\n\n"
            f"📩 این کلید را برای `@acc1192` در روبیکا ارسال کنید."
        )
        await message.reply_text(text, reply_markup=main_menu_keyboard())


@telegram_app.on_message(tg_filters.command("panel") & tg_filters.private)
async def admin_panel_command(client, message):
    if message.chat.id not in ADMIN_IDS:
        return
    reset_daily_quotas_if_needed()
    await message.reply_text(
        admin_overview_text(),
        reply_markup=admin_panel_keyboard(),
    )


@telegram_app.on_message(tg_filters.text & tg_filters.private)
async def on_private_text_menu(client, message):
    text = (message.text or "").strip()
    user_id = message.chat.id
    user_state = user_states.get(user_id)

    if text == "🆘 پشتیبانی":
        prompt = await message.reply_text(
            "🛟 پیام خودرا برای پشتیبانی ارسال کنید.",
            reply_markup=support_cancel_keyboard(),
        )
        user_states[user_id] = {
            "action": "await_support_message",
            "prompt_message_id": prompt.id,
        }
        return

    if text == "🛒 خرید حجم":
        plans = load_volume_plans()
        if not plans:
            await message.reply_text(
                "⚠️ در حال حاضر پلن فعالی برای خرید حجم تعریف نشده است."
            )
            return
        await message.reply_text(
            "🛒 خرید حجم\n\nبرای مشاهده پلن‌ها روی دکمه زیر بزنید:",
            reply_markup=buy_volume_entry_keyboard(),
        )
        return

    if user_state and user_state.get("action") == "await_support_message":
        if not text:
            await message.reply_text("⚠️ لطفاً پیام متنی ارسال کنید.")
            return
        safe_user_text = html.escape(text)
        ticket_id = create_ticket(user_id, text)
        prompt_message_id = int(user_state.get("prompt_message_id") or 0)
        user_states.pop(user_id, None)
        if prompt_message_id:
            with suppress(Exception):
                await telegram_app.delete_messages(user_id, prompt_message_id)
        await message.reply_text(
            f"✅ پیام شما ارسال شد.\n🎫 تیکت {ticket_id_monospace(ticket_id)} ثبت شد.",
            parse_mode=enums.ParseMode.HTML,
        )
        admin_text = (
            "📩 تیکت جدید دریافت شد\n\n"
            f"🎫 تیکت آیدی: {ticket_id_monospace(ticket_id)}\n"
            f"👤 چت آیدی کاربر: <code>{user_id}</code>\n\n"
            f"📄 متن پیام:\n{safe_user_text}"
        )
        for admin_chat_id in ADMIN_IDS:
            with suppress(Exception):
                await telegram_app.send_message(
                    admin_chat_id,
                    admin_text,
                    parse_mode=enums.ParseMode.HTML,
                    reply_markup=admin_ticket_notify_keyboard(ticket_id),
                )
        return

    if text == "👤 حساب کاربری":
        user = get_or_create_user(user_id)
        await message.reply_text(
            user_account_text(user), reply_markup=main_menu_keyboard()
        )
        return
    if text == "/help" or "راهنما" in text:
        await message.reply_text(
            help_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode=enums.ParseMode.HTML,
        )
        return

    state = admin_states.get(user_id)
    if user_id not in ADMIN_IDS or not state:
        await queue_media_message(message)
        return
    if state.get("action") == "await_user_search":
        if not text.isdigit():
            await message.reply_text("⚠️ لطفاً فقط چت آیدی عددی ارسال کنید.")
            return
        target_id = int(text)
        target_user = get_user(target_id)
        if not target_user:
            await message.reply_text("❌ کاربر یافت نشد.")
            return
        prompt_message_id = state.get("prompt_message_id")
        admin_states.pop(user_id, None)
        if prompt_message_id:
            with suppress(Exception):
                await telegram_app.delete_messages(user_id, prompt_message_id)
        with suppress(Exception):
            await message.delete()
        await message.reply_text(
            admin_user_info_text(target_user),
            reply_markup=admin_user_manage_keyboard(target_id),
        )
        return
    if state.get("action") == "await_volume_change":
        if not re.fullmatch(r"\d+(\.\d+)?", text):
            await message.reply_text("⚠️ مقدار حجم را به مگابایت و عددی ارسال کنید.")
            return
        mb_value = float(text)
        if mb_value <= 0:
            await message.reply_text("⚠️ مقدار باید بزرگ‌تر از صفر باشد.")
            return
        delta = int(mb_value * 1024 * 1024)
        if state.get("mode") == "dec":
            delta = -delta
        target_id = int(state["target_user_id"])
        update_user_main_volume(target_id, delta)
        target_user = get_or_create_user(target_id)
        admin_states.pop(user_id, None)
        await message.reply_text(
            "✅ حجم اصلی کاربر با موفقیت به‌روزرسانی شد.\n\n"
            + admin_user_info_text(target_user),
            reply_markup=admin_user_manage_keyboard(target_id),
        )
        return
    if state.get("action") == "await_ticket_search":
        if not text.isdigit():
            await message.reply_text("⚠️ لطفاً فقط تیکت آیدی عددی ارسال کنید.")
            return
        ticket_id = int(text)
        ticket = get_ticket(ticket_id)
        if not ticket:
            await message.reply_text("❌ تیکتی با این شناسه یافت نشد.")
            return
        prompt_message_id = int(state.get("prompt_message_id") or 0)
        admin_states.pop(user_id, None)
        if prompt_message_id:
            with suppress(Exception):
                await telegram_app.delete_messages(user_id, prompt_message_id)
        with suppress(Exception):
            await message.delete()
        await telegram_app.send_message(
            user_id,
            admin_ticket_details_text(ticket),
            parse_mode=enums.ParseMode.HTML,
            reply_markup=admin_ticket_view_keyboard(
                ticket_id=ticket_id,
                answered=ticket["status"] == "answered",
            ),
        )
        return
    if state.get("action") == "await_ticket_reply":
        if not text:
            await message.reply_text("⚠️ لطفاً پاسخ متنی ارسال کنید.")
            return
        safe_admin_reply = html.escape(text)
        ticket_id = int(state["ticket_id"])
        ticket = get_ticket(ticket_id)
        if not ticket:
            admin_states.pop(user_id, None)
            await message.reply_text("❌ این تیکت دیگر وجود ندارد.")
            return
        answer_ticket(ticket_id, text)
        prompt_message_id = int(state.get("prompt_message_id") or 0)
        admin_states.pop(user_id, None)
        if prompt_message_id:
            with suppress(Exception):
                await telegram_app.delete_messages(user_id, prompt_message_id)
        with suppress(Exception):
            await telegram_app.send_message(
                int(ticket["telegram_id"]),
                f"📬 پاسخ تیکت {ticket_id_monospace(ticket_id)}:\n\n{safe_admin_reply}",
                parse_mode=enums.ParseMode.HTML,
            )
        await message.reply_text(
            f"✅ پاسخ برای تیکت {ticket_id_monospace(ticket_id)} ارسال شد.",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=admin_tickets_keyboard(),
        )
        return
    if state.get("action") == "await_payment_search":
        payment_code = text.strip().upper()
        payment = get_payment_by_code(payment_code)
        if not payment:
            await message.reply_text("❌ پرداختی با این کد پیدا نشد.")
            return
        prompt_message_id = int(state.get("prompt_message_id") or 0)
        admin_states.pop(user_id, None)
        if prompt_message_id:
            with suppress(Exception):
                await telegram_app.delete_messages(user_id, prompt_message_id)
        with suppress(Exception):
            await message.delete()
        await telegram_app.send_message(
            user_id,
            payment_details_text(payment),
            parse_mode=enums.ParseMode.HTML,
            reply_markup=admin_payments_keyboard(),
        )
        return


@telegram_app.on_message(
    (
        tg_filters.document
        | tg_filters.video
        | tg_filters.audio
        | tg_filters.photo
        | tg_filters.animation
        | tg_filters.voice
        | tg_filters.video_note
        | tg_filters.sticker
    )
    & tg_filters.private
)
async def on_media_message(client, message):
    user_state = user_states.get(message.chat.id)
    if user_state and user_state.get("action") == "await_support_message":
        await message.reply_text("⚠️ لطفاً پیام پشتیبانی را به‌صورت متنی ارسال کنید.")
        return
    if message.chat.id in ADMIN_IDS and admin_states.get(message.chat.id):
        return
    await queue_media_message(message)


@telegram_app.on_pre_checkout_query()
async def on_pre_checkout_query(client, query):
    payload = (query.invoice_payload or "").strip()
    if not payload.startswith("stars_buy:"):
        await query.answer(ok=False, error_message="Invalid payment payload.")
        return
    plan_id = payload.split(":", 1)[1]
    plan = get_volume_plan(plan_id)
    if not plan:
        await query.answer(
            ok=False, error_message="Selected plan is no longer available."
        )
        return
    await query.answer(ok=True)


@telegram_app.on_message(tg_filters.successful_payment & tg_filters.private)
async def on_successful_payment(client, message):
    payment = message.successful_payment
    if not payment:
        return
    payload = (payment.invoice_payload or "").strip()
    if not payload.startswith("stars_buy:"):
        return
    plan_id = payload.split(":", 1)[1]
    plan = get_volume_plan(plan_id)
    if not plan:
        await message.reply_text(
            "⚠️ پرداخت ثبت شد اما پلن خرید دیگر در دسترس نیست. لطفاً به پشتیبانی پیام دهید."
        )
        return
    update_user_main_volume(message.chat.id, int(plan["volume_bytes"]))
    payment_code = create_payment_record(message.chat.id, plan, payment)
    user = get_or_create_user(message.chat.id)
    await message.reply_text(
        "✅ پرداخت شما با موفقیت انجام شد.\n\n"
        f"🧾 کد پرداخت: <code>{payment_code}</code>\n"
        f"📦 پلن خریداری‌شده: {plan['title']}\n"
        f"📁 حجم افزوده‌شده: {volume_text(int(plan['volume_bytes']))}\n"
        f"⭐ مبلغ پرداختی: {int(plan['stars'])} استار\n\n"
        f"💼 حجم اصلی فعلی شما: {volume_text(int(user.get('main_remaining', 0)))}",
        parse_mode=enums.ParseMode.HTML,
    )
    admin_text = (
        "💳 پرداخت موفق جدید\n\n"
        f"🧾 کد پرداخت: <code>{payment_code}</code>\n"
        f"👤 چت آیدی کاربر: <code>{int(message.chat.id)}</code>\n"
        f"📦 پلن: {html.escape(plan['title'])}\n"
        f"📁 حجم افزوده‌شده: {volume_text(int(plan['volume_bytes']))}\n"
        f"⭐ مبلغ: {int(plan['stars'])} استار\n"
        f"🆔 telegram_charge_id:\n<code>{html.escape(payment.telegram_payment_charge_id)}</code>"
    )
    for admin_chat_id in ADMIN_IDS:
        with suppress(Exception):
            await telegram_app.send_message(
                admin_chat_id,
                admin_text,
                parse_mode=enums.ParseMode.HTML,
            )


@telegram_app.on_callback_query()
async def on_callback_query(client, callback_query):
    data = callback_query.data or ""
    admin_id = callback_query.message.chat.id

    if data == "support_cancel":
        await safe_answer_callback(callback_query, "لغو شد")
        user_states.pop(callback_query.message.chat.id, None)
        with suppress(Exception):
            await callback_query.message.delete()
        return

    if data == "disconnect_prompt":
        await safe_answer_callback(callback_query)
        await safe_edit(
            callback_query.message,
            "⚠️ مطمئن هستید می‌خواهید اتصال روبیکا را قطع کنید؟",
            reply_markup=disconnect_confirm_keyboard(),
        )
        return

    if data == "disconnect_cancel":
        await safe_answer_callback(callback_query)
        user = get_user(callback_query.message.chat.id)
        if user and user.get("rubika_guid"):
            available = get_available_bytes()
            text = (
                f"✅ اتصال شما برقرار است\n\n"
                f"🤖 حساب روبیکا: `{user['rubika_guid']}`\n\n"
                f"📤 فایل‌ها یا لینک‌های مستقیم خود را ارسال کنید تا در این حساب دریافت شوند.\n\n"
                f"🧮 فضای خالی فعلی: {human_size(available)}\n\n"
                f"🔌 برای قطع اتصال از دکمه زیر استفاده کنید."
            )
            await safe_edit(
                callback_query.message,
                text,
                reply_markup=disconnect_keyboard(),
            )
        else:
            await safe_edit(callback_query.message, "🔒 هیچ اتصالی فعال نیست")
        return

    if data == "disconnect_confirm":
        await safe_answer_callback(callback_query)
        disconnect_user(callback_query.message.chat.id)
        await safe_edit(callback_query.message, "✅ اتصال روبیکا قطع شد")
        return

    if data == "buy_volume_back":
        await safe_answer_callback(callback_query)
        await safe_edit(
            callback_query.message,
            "🛒 خرید حجم\n\nبرای مشاهده پلن‌ها روی دکمه زیر بزنید:",
            reply_markup=buy_volume_entry_keyboard(),
        )
        return

    if data == "buy_stars_plans":
        await safe_answer_callback(callback_query)
        plans = load_volume_plans()
        if not plans:
            await safe_edit(
                callback_query.message,
                "⚠️ در حال حاضر پلن فعالی برای خرید حجم تعریف نشده است.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ بازگشت", callback_data="buy_volume_back"
                            )
                        ]
                    ]
                ),
            )
            return
        await safe_edit(
            callback_query.message,
            "⭐ یکی از پلن‌های حجم را انتخاب کنید:",
            reply_markup=buy_volume_plans_keyboard(plans),
        )
        return

    if data.startswith("buy_plan_select:"):
        await safe_answer_callback(callback_query)
        plan_id = data.split(":", 1)[1]
        plan = get_volume_plan(plan_id)
        if not plan:
            await safe_answer_callback(
                callback_query,
                "این پلن دیگر در دسترس نیست.",
                show_alert=True,
            )
            return
        description = plan["description"] or "افزایش حجم اصلی حساب"
        await safe_edit(
            callback_query.message,
            "🧾 تایید پلن خرید\n\n"
            f"📦 پلن: {plan['title']}\n"
            f"📁 حجم: {volume_text(int(plan['volume_bytes']))}\n"
            f"⭐ هزینه: {int(plan['stars'])} استار\n"
            f"ℹ️ توضیح: {description}",
            reply_markup=buy_volume_plan_details_keyboard(plan_id, int(plan["stars"])),
        )
        return

    if data.startswith("buy_plan_pay:"):
        await safe_answer_callback(callback_query)
        plan_id = data.split(":", 1)[1]
        plan = get_volume_plan(plan_id)
        if not plan:
            await safe_answer_callback(
                callback_query,
                "این پلن دیگر در دسترس نیست.",
                show_alert=True,
            )
            return
        await telegram_app.send_invoice(
            callback_query.message.chat.id,
            title=f"خرید حجم {plan['title']}",
            description=plan["description"] or f"افزایش حجم اصلی به {plan['title']}",
            currency="XTR",
            prices=[LabeledPrice(label=plan["title"], amount=int(plan["stars"]))],
            payload=f"stars_buy:{plan_id}",
        )
        return

    if data == "admin_search_user":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query)
        admin_states[admin_id] = {
            "action": "await_user_search",
            "cancelled": False,
        }
        prompt = await telegram_app.send_message(
            admin_id,
            "🔎 چت آیدی کاربر را ارسال کنید:",
            reply_markup=admin_search_cancel_keyboard(),
        )
        admin_states[admin_id]["prompt_message_id"] = prompt.id
        return

    if data == "admin_tickets_panel":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query)
        admin_states.pop(admin_id, None)
        await safe_edit(
            callback_query.message,
            admin_tickets_overview_text(),
            reply_markup=admin_tickets_keyboard(),
        )
        return

    if data == "admin_payments_panel":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query)
        admin_states.pop(admin_id, None)
        await safe_edit(
            callback_query.message,
            admin_payments_overview_text(),
            reply_markup=admin_payments_keyboard(),
        )
        return

    if data == "admin_payments_success_list":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        rows = cursor.execute(
            """
            SELECT payment_code, telegram_id, plan_title, volume_bytes, stars_amount, created_at
            FROM payments
            ORDER BY created_at DESC
            """
        ).fetchall()
        if not rows:
            await safe_answer_callback(
                callback_query,
                "پرداخت موفقی ثبت نشده است.",
                show_alert=True,
            )
            return
        await safe_answer_callback(callback_query)
        file_path = FILES_DIR / f"successful_payments_{now_ts()}_{admin_id}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            for row in rows:
                created = datetime.fromtimestamp(
                    int(row["created_at"]), ZoneInfo("Asia/Tehran")
                ).strftime("%Y-%m-%d %H:%M:%S")
                f.write(
                    f"{row['payment_code']} | user:{int(row['telegram_id'])} | "
                    f"plan:{row['plan_title']} | volume:{volume_text(int(row['volume_bytes']))} | "
                    f"stars:{int(row['stars_amount'])} | time:{created}\n"
                )
        await telegram_app.send_document(
            admin_id,
            document=str(file_path),
            caption=f"📄 تعداد پرداخت‌های موفق: {len(rows)}",
        )
        with suppress(Exception):
            os.remove(file_path)
        return

    if data == "admin_payment_search":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query)
        admin_states[admin_id] = {
            "action": "await_payment_search",
            "prompt_message_id": None,
        }
        prompt = await telegram_app.send_message(
            admin_id,
            "🔎 کد پرداخت را ارسال کنید (مثال: PAY-ABC12345):",
            reply_markup=admin_payment_search_cancel_keyboard(),
        )
        admin_states[admin_id]["prompt_message_id"] = prompt.id
        return

    if data == "admin_cancel_payment_search":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query, "لغو شد")
        state = admin_states.get(admin_id)
        if state and state.get("action") == "await_payment_search":
            admin_states.pop(admin_id, None)
        with suppress(Exception):
            await callback_query.message.delete()
        return

    if data == "admin_tickets_unanswered_list":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        rows = cursor.execute(
            "SELECT ticket_id FROM tickets WHERE status != 'answered' ORDER BY created_at ASC"
        ).fetchall()
        if not rows:
            await safe_answer_callback(
                callback_query,
                "تیکت بی‌پاسخی وجود ندارد.",
                show_alert=True,
            )
            return
        await safe_answer_callback(callback_query)
        file_path = FILES_DIR / f"unanswered_tickets_{now_ts()}_{admin_id}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(f"{int(row['ticket_id'])}\n")
        await telegram_app.send_document(
            admin_id,
            document=str(file_path),
            caption=f"📄 تعداد تیکت‌های بی‌پاسخ: {len(rows)}",
        )
        with suppress(Exception):
            os.remove(file_path)
        return

    if data == "admin_ticket_search":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query)
        admin_states[admin_id] = {
            "action": "await_ticket_search",
            "prompt_message_id": None,
        }
        prompt = await telegram_app.send_message(
            admin_id,
            "🔎 تیکت آیدی را ارسال کنید:",
            reply_markup=admin_ticket_search_cancel_keyboard(),
        )
        admin_states[admin_id]["prompt_message_id"] = prompt.id
        return

    if data == "admin_cancel_ticket_search":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query, "لغو شد")
        state = admin_states.get(admin_id)
        if state and state.get("action") == "await_ticket_search":
            admin_states.pop(admin_id, None)
        with suppress(Exception):
            await callback_query.message.delete()
        return

    if data.startswith("admin_ticket_reply_prompt:"):
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        ticket_id = int(data.split(":", 1)[1])
        ticket = get_ticket(ticket_id)
        if not ticket:
            await safe_answer_callback(callback_query, "تیکت یافت نشد", show_alert=True)
            return
        if ticket["status"] == "answered":
            await safe_answer_callback(
                callback_query, "این تیکت قبلاً پاسخ داده شده است."
            )
            return
        await safe_answer_callback(callback_query)
        prompt = await telegram_app.send_message(
            admin_id,
            f"✍️ پاسخ تیکت {ticket_id_monospace(ticket_id)} را ارسال کنید:",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=admin_ticket_reply_cancel_keyboard(),
        )
        admin_states[admin_id] = {
            "action": "await_ticket_reply",
            "ticket_id": ticket_id,
            "prompt_message_id": prompt.id,
        }
        return

    if data == "admin_cancel_ticket_reply":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query, "لغو شد")
        state = admin_states.get(admin_id)
        if state and state.get("action") == "await_ticket_reply":
            admin_states.pop(admin_id, None)
        with suppress(Exception):
            await callback_query.message.delete()
        return

    if data == "admin_cancel_search":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query, "لغو شد")
        state = admin_states.get(admin_id)
        if state and state.get("action") == "await_user_search":
            admin_states.pop(admin_id, None)
        with suppress(Exception):
            await callback_query.message.delete()
        return

    if data == "admin_back_main":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query)
        admin_states.pop(admin_id, None)
        await safe_edit(
            callback_query.message,
            admin_overview_text(),
            reply_markup=admin_panel_keyboard(),
        )
        return

    if data == "admin_cleanup_prompt":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query)
        await safe_edit(
            callback_query.message,
            "⚠️ با تایید پاک سازی، همه فایل‌های موقت و ذخیره‌شده از سرور حذف می‌شوند.\n"
            "آیا مطمئن هستید؟",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "❌ انصراف", callback_data="admin_back_main"
                        ),
                        InlineKeyboardButton(
                            "✅ تایید پاک سازی", callback_data="admin_cleanup_confirm"
                        ),
                    ]
                ]
            ),
        )
        return

    if data == "admin_cleanup_confirm":
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        await safe_answer_callback(callback_query)
        cancelled_jobs, removed_entries = await admin_cleanup_storage()
        await safe_edit(
            callback_query.message,
            "✅ پاک سازی انجام شد.\n\n"
            f"🧾 تعداد ورودی‌های حذف‌شده از فضای فایل: {removed_entries}\n"
            f"🛑 تعداد فرایندهای متوقف‌شده: {cancelled_jobs}\n"
            f"💽 فضای خالی فعلی: {human_size(get_available_bytes())}",
            reply_markup=admin_panel_keyboard(),
        )
        return

    if data.startswith("admin_volume_inc:") or data.startswith("admin_volume_dec:"):
        if admin_id not in ADMIN_IDS:
            await safe_answer_callback(callback_query, "دسترسی ندارید", show_alert=True)
            return
        mode = "inc" if data.startswith("admin_volume_inc:") else "dec"
        target_id = int(data.split(":", 1)[1])
        await safe_answer_callback(callback_query)
        admin_states[admin_id] = {
            "action": "await_volume_change",
            "mode": mode,
            "target_user_id": target_id,
        }
        await telegram_app.send_message(
            admin_id,
            (
                f"{'➕' if mode == 'inc' else '➖'} مقدار حجم را به مگابایت ارسال کنید.\n"
                "مثال: 250"
            ),
        )
        return

    if data.startswith("batch_cancel:"):
        batch_id = data.split(":", 1)[1]
        await safe_answer_callback(callback_query, "لغو شد")
        batch = active_batches.get(batch_id)
        if batch:
            async with queue_lock:
                with suppress(ValueError):
                    processing_queue.remove(batch_id)
                running_batches.discard(batch_id)
                running_users.discard(int(batch["telegram_id"]))
            set_batch_status(batch_id, "cancelled")
            cursor.execute(
                "UPDATE batch_items SET status = 'cancelled', local_path = NULL WHERE batch_id = ?",
                (batch_id,),
            )
            conn.commit()
            await release_space(batch_id, status="released")
            task = batch.get("processing_task")
            if task and not task.done():
                task.cancel()
            if batch.get("upload_failed"):
                await cleanup_upload_failed(batch_id)
            active_batches.pop(batch_id, None)
            with suppress(Exception):
                await safe_edit(callback_query.message, "⛔ بسته لغو شد")
            await maybe_start_queued_batches()
        return

    if data.startswith("batch_confirm:"):
        batch_id = data.split(":", 1)[1]
        await safe_answer_callback(callback_query)
        batch = active_batches.get(batch_id)
        if not batch:
            await safe_edit(callback_query.message, "⚠️ این بسته دیگر در دسترس نیست")
            return
        user = get_or_create_user(callback_query.message.chat.id)
        if not user or not user.get("rubika_guid"):
            await safe_edit(
                callback_query.message, "🔐 ابتدا اتصال روبیکا را برقرار کنید"
            )
            return
        total_remaining = get_user_total_remaining(user)
        if batch["total_size"] > total_remaining:
            await safe_edit(
                callback_query.message,
                "⛔️ حجم کافی ندارید.\n\n"
                f"🎁+💼 حجم قابل استفاده شما: {mb_text(total_remaining)}\n"
                f"📦 حجم بسته: {mb_text(batch['total_size'])}",
            )
            return
        batch["rubika_guid"] = user["rubika_guid"]
        set_batch_status(batch_id, "confirmed", rubika_guid=user["rubika_guid"])
        async with queue_lock:
            can_start_now = (
                len(running_batches) < MAX_CONCURRENT_PROCESSES
                and int(batch["telegram_id"]) not in running_users
                and len(processing_queue) == 0
            )
        if can_start_now:
            with suppress(Exception):
                await callback_query.message.delete()
            async with queue_lock:
                running_batches.add(batch_id)
                running_users.add(int(batch["telegram_id"]))
            batch["status"] = "active"
            set_batch_status(batch_id, "active", rubika_guid=user["rubika_guid"])
            batch["processing_task"] = asyncio.create_task(
                process_confirmed_batch(batch_id, callback_query.message.chat.id)
            )
        else:
            batch["status"] = "queued"
            batch["queue_message"] = callback_query.message
            set_batch_status(batch_id, "queued", rubika_guid=user["rubika_guid"])
            position = await enqueue_batch(batch_id)
            await safe_edit(
                callback_query.message,
                queue_status_text(position, len(processing_queue)),
            )
            await maybe_start_queued_batches()
        return

    if data.startswith("job_cancel:"):
        job_id = data.split(":", 1)[1]
        await safe_answer_callback(callback_query)
        job = active_jobs.get(job_id)
        if job and not job.get("cancelled"):
            async with job.get("status_lock", asyncio.Lock()):
                job["cancel_confirm_shown"] = True
                old_prompt = job.get("cancel_prompt_message")
                if old_prompt:
                    with suppress(Exception):
                        await old_prompt.delete()
                job["cancel_prompt_message"] = await telegram_app.send_message(
                    callback_query.message.chat.id,
                    "⚠️ مطمئن هستید می‌خواهید فرایند را لغو کنید؟\n\nاین عملیات غیرقابل بازگشت است.",
                    reply_markup=job_cancel_confirm_keyboard(job_id),
                )
        return

    if data.startswith("job_cancel_confirm:"):
        job_id = data.split(":", 1)[1]
        await safe_answer_callback(callback_query, "فرایند لغو شد")
        job = active_jobs.get(job_id)
        if job:
            async with job.get("status_lock", asyncio.Lock()):
                job["cancelled"] = True
                job["cancel_confirm_shown"] = False
                task = job.get("task")
                if task and not task.done():
                    task.cancel()
                prompt_message = job.get("cancel_prompt_message")
                if prompt_message:
                    with suppress(Exception):
                        await prompt_message.delete()
                job["cancel_prompt_message"] = None
        return

    if data.startswith("job_cancel_cancel:"):
        job_id = data.split(":", 1)[1]
        await safe_answer_callback(callback_query, "لغو درخواست لغو")
        job = active_jobs.get(job_id)
        if job:
            async with job.get("status_lock", asyncio.Lock()):
                job["cancel_confirm_shown"] = False
                prompt_message = job.get("cancel_prompt_message")
                if prompt_message:
                    with suppress(Exception):
                        await prompt_message.delete()
                job["cancel_prompt_message"] = None
        return

    if data.startswith("upload_retry:"):
        batch_id = data.split(":", 1)[1]
        await safe_answer_callback(callback_query)
        if batch_id not in active_batches or not active_batches[batch_id].get(
            "upload_failed"
        ):
            await safe_edit(
                callback_query.message,
                "⚠️ این درخواست امتحان مجدد دیگر معتبر نیست",
            )
            return
        batch = active_batches[batch_id]
        retry_deadlines.pop(batch_id, None)
        await safe_edit(
            callback_query.message,
            "☁️ در حال آپلود مجدد به روبیکا...",
            reply_markup=None,
        )
        zip_path_str = str(FILES_DIR / f"{batch_id}.zip")
        try:
            sent_message = await monitored_rubika_send(
                zip_path_str, batch["zip_name"], None, batch["rubika_guid"]
            )
            consume_user_quota(callback_query.message.chat.id, int(batch["total_size"]))
            set_batch_status(
                batch_id,
                "done",
                password=batch.get("password"),
                zip_name=batch["zip_name"],
                rubika_guid=batch["rubika_guid"],
            )
            await telegram_app.send_message(
                callback_query.message.chat.id,
                f"✅ ارسال با موفقیت انجام شد\n\n🗂️ نام فایل:\n`{batch['zip_name']}`\n🔐 پسورد فایل:\n`{batch.get('password')}`",
                reply_to_message_id=batch.get("last_source_message_id"),
            )
            schedule_background(
                forward_to_mirror_in_background(batch["rubika_guid"], sent_message)
            )
            await cleanup_upload_failed(batch_id)
            active_batches.pop(batch_id, None)
        except Exception as retry_exc:
            await safe_edit(
                callback_query.message,
                f"❌ خطا در آپلود مجدد:\n{str(retry_exc)[:500]}\n\n🔄 می‌توانید دوباره امتحان کنید.",
                reply_markup=upload_retry_keyboard(batch_id),
            )
            retry_deadlines[batch_id] = {
                "chat_id": callback_query.message.chat.id,
                "retry_msg_id": callback_query.message.id,
                "deadline": now_ts() + RETRY_WINDOW_SECONDS,
            }
        return

    await safe_answer_callback(callback_query)


async def main_runner():
    await telegram_app.start()
    watchdog_task = asyncio.create_task(cron_watchdog())
    try:
        rubika_app.add_handler(on_rubika_update, rub_handlers.MessageUpdates())
    except Exception:
        pass
    try:
        await rubika_app.run()
    finally:
        watchdog_task.cancel()


if __name__ == "__main__":
    try:
        loop.run_until_complete(main_runner())
    except KeyboardInterrupt:
        pass
