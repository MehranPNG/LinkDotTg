import json
from pathlib import Path
from rubpy import Client, handlers

BASE_DIR = Path(__file__).resolve().parent
CONF_DIR = BASE_DIR / "conf"
SESSION_DIR = CONF_DIR / "sessions"
FILES_DIR = CONF_DIR / "files"
SEEN_FILE = CONF_DIR / "seen_messages.json"
TARGET_FILE = CONF_DIR / "target.json"

SESSION_DIR.mkdir(parents=True, exist_ok=True)
FILES_DIR.mkdir(parents=True, exist_ok=True)
CONF_DIR.mkdir(parents=True, exist_ok=True)

SESSION_NAME = str(SESSION_DIR / "rubika_session")
TARGET_USERNAME = "acc1192"

app = Client(SESSION_NAME)


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default


def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


seen_messages = set(load_json(SEEN_FILE, []))
target_cache = load_json(TARGET_FILE, {})


def normalize_username(username: str) -> str:
    username = username.strip()
    return username[1:].lower() if username.startswith("@") else username.lower()


def is_file_message(msg: dict) -> bool:
    return msg.get("type") in {
        "File",
        "Image",
        "Video",
        "VideoMessage",
        "Music",
        "Voice",
        "Gif",
    }


def get_file_name(msg: dict) -> str:
    name = (
        msg.get("file_name")
        or msg.get("name")
        or msg.get("text")
        or f'{msg.get("message_id", "unknown")}.bin'
    )
    return name.replace("/", "_").replace("\\", "_")


async def resolve_object_guid_by_username(username: str):
    username = normalize_username(username)

    if username in target_cache:
        return target_cache[username]

    chats = await app.get_chats()
    items = []
    if isinstance(chats, dict):
        for k in ("chats", "data", "items", "list"):
            if isinstance(chats.get(k), list):
                items.extend(chats[k])

    for item in items:
        if not isinstance(item, dict):
            continue

        candidates = [
            item.get("username"),
            (
                item.get("user", {}).get("username")
                if isinstance(item.get("user"), dict)
                else None
            ),
            (
                item.get("chat", {}).get("username")
                if isinstance(item.get("chat"), dict)
                else None
            ),
        ]

        if any(normalize_username(x) == username for x in candidates if x):
            guid = (
                item.get("object_guid")
                or item.get("user_guid")
                or item.get("guid")
                or (item.get("chat") or {}).get("object_guid")
                or (item.get("chat") or {}).get("user_guid")
            )
            if guid:
                target_cache[username] = guid
                save_json(TARGET_FILE, target_cache)
                return guid

    raise ValueError(
        f"GUID برای @{username} پیدا نشد. اول مطمئن شو این پیوی در لیست چت‌ها وجود دارد."
    )


async def download_file(msg: dict):
    file_inline = msg.get("file_inline")
    if not file_inline:
        print("file_inline پیدا نشد")
        print(msg)
        return

    file_path = FILES_DIR / get_file_name(msg)
    data = await app.download(file_inline)

    with open(file_path, "wb") as f:
        f.write(data)

    print("downloaded:", file_path)


def extract_full_message(result):
    if isinstance(result, dict):
        for k in ("messages", "data", "items", "list"):
            v = result.get(k)
            if isinstance(v, list) and v:
                return v[0]
    return None


async def on_updates(update):
    try:
        print("raw update:", update)

        if not isinstance(update, dict):
            return

        target_guid = await resolve_object_guid_by_username(TARGET_USERNAME)

        if update.get("object_guid") != target_guid:
            return

        chat = update.get("chat") or {}
        last_message = chat.get("last_message") or {}
        message_id = last_message.get("message_id") or chat.get("last_message_id")

        if not message_id:
            return

        key = f"{target_guid}:{message_id}"
        if key in seen_messages:
            return

        result = await app.get_messages_by_id(target_guid, [message_id])
        print("get_messages_by_id result:", result)

        full_msg = extract_full_message(result)
        print("full message:", full_msg)

        if full_msg and is_file_message(full_msg):
            await download_file(full_msg)
        else:
            print("فایل تشخیص داده نشد")

        seen_messages.add(key)
        save_json(SEEN_FILE, list(seen_messages))

    except Exception as e:
        print("handler error:", repr(e))


app.add_handler(on_updates, handlers.ChatUpdates)
app.run()
