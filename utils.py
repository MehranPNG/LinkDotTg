import os
import random
import re
import string
from typing import Optional

from config import AUTH_KEY_LENGTH, SPECIAL_CHARS, ZIP_PASSWORD_LENGTH


def now_ts() -> int:
    import time

    return int(time.time())


def random_text(length: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_auth_key() -> str:
    return random_text(AUTH_KEY_LENGTH)


def generate_password() -> str:
    alphabet = string.ascii_letters + string.digits + SPECIAL_CHARS
    rng = random.SystemRandom()
    while True:
        value = "".join(rng.choice(alphabet) for _ in range(ZIP_PASSWORD_LENGTH))
        if any(ch in SPECIAL_CHARS for ch in value):
            return value


def safe_name(name: str, fallback: str = "file") -> str:
    name = (name or "").strip().replace("\x00", "")
    name = os.path.basename(name)
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or fallback


def human_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    n = float(max(size, 0))
    for unit in units:
        if n < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.2f} {unit}"
        n /= 1024
    return "0 B"


def percent_text(done: int, total: int) -> str:
    if not total:
        return "0%"
    return f"{(done / total) * 100:.1f}%"


def mb_text(size: int) -> str:
    value = max(int(size or 0), 0) / (1024**2)
    return f"{value:.2f} مگابایت"
