# core/auth.py
import sqlite3
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta
from typing import Optional, List, Dict

DB_PATH = "users.db"

# ── Параметры pbkdf2 ──
PBKDF2_ITERATIONS = 200_000
PBKDF2_DKLEN = 32

# ── Дефолтный срок подписки при создании ──
DEFAULT_TRIAL_DAYS = 30


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Создаёт таблицу пользователей при первом запуске."""
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                login TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                hwid TEXT,
                activated_at TEXT,
                expires_at TEXT,
                tariff TEXT DEFAULT 'trial',
                is_active INTEGER DEFAULT 1
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_login ON users(login)")
        conn.commit()


# ============================================================
# Хеширование пароля
# ============================================================
def _hash_pbkdf2(password: str, salt_hex: str) -> str:
    """Современный хеш пароля: PBKDF2-HMAC-SHA256."""
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt_hex),
        PBKDF2_ITERATIONS,
        dklen=PBKDF2_DKLEN,
    )
    return dk.hex()


def _hash_legacy_sha256(password: str, salt_hex: str) -> str:
    """Старый формат (для миграции)."""
    return hashlib.sha256((password + salt_hex).encode()).hexdigest()


def hash_password(password: str) -> tuple:
    """Возвращает (hash, salt_hex)."""
    salt_hex = os.urandom(16).hex()
    return _hash_pbkdf2(password, salt_hex), salt_hex


def verify_password(password: str, hash_val: str, salt_hex: str) -> bool:
    """
    Проверяет пароль. Поддерживает оба формата:
      - новый: pbkdf2 (64 hex)
      - старый: sha256 (64 hex)
    Возвращает True если совпал.
    """
    # Пробуем оба — константное время
    new_hash = _hash_pbkdf2(password, salt_hex)
    old_hash = _hash_legacy_sha256(password, salt_hex)
    return hmac.compare_digest(new_hash, hash_val) or \
           hmac.compare_digest(old_hash, hash_val)


def _needs_rehash(hash_val: str) -> bool:
    """True, если хеш в старом формате — надо перехешировать при логине."""
    # Обновляем форматы: теперь sha256 почти неотличим от pbkdf2 по длине,
    # поэтому маркируем: если хеш совпал со старым форматом — перехешируем.
    return True  # всегда перехешируем при успешном логине (дёшево)


# ============================================================
# Даты
# ============================================================
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _calc_expires(days: int) -> str:
    return (datetime.now() + timedelta(days=days)).isoformat(timespec="seconds")


def _days_left(expires_at: Optional[str]) -> int:
    """Сколько дней осталось до expires_at. Округляем вниз."""
    if not expires_at:
        return 0
    try:
        exp = datetime.fromisoformat(expires_at)
    except Exception:
        return 0
    delta = exp - datetime.now()
    return max(0, delta.days)


def _days_total(activated_at: Optional[str], expires_at: Optional[str]) -> int:
    """Сколько всего дней было в подписке (для прогресс-бара)."""
    if not activated_at or not expires_at:
        return 30
    try:
        a = datetime.fromisoformat(activated_at)
        e = datetime.fromisoformat(expires_at)
    except Exception:
        return 30
    return max(1, (e - a).days)


# ============================================================
# Пользователи — CRUD
# ============================================================
def create_user(login: str, password: str, tariff: str = "trial",
                days: int = DEFAULT_TRIAL_DAYS) -> bool:
    """
    Создаёт пользователя.
    days — на сколько дней активировать (expires_at = now + days).
    """
    hash_val, salt = hash_password(password)
    activated = _now_iso()
    expires = _calc_expires(days)
    with get_db() as conn:
        try:
            conn.execute(
                """INSERT INTO users
                   (login, password_hash, salt, tariff,
                    activated_at, expires_at, is_active)
                   VALUES (?, ?, ?, ?, ?, ?, 1)""",
                (login, hash_val, salt, tariff, activated, expires),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def authenticate_user(login: str, password: str) -> Optional[dict]:
    """
    Возвращает dict с данными пользователя, включая days_left/days_total,
    либо None если неверный логин/пароль/отключён/истёк.
    """
    with get_db() as conn:
        user = conn.execute(
            """SELECT id, login, password_hash, salt, hwid, tariff,
                      activated_at, expires_at, is_active
               FROM users WHERE login = ?""",
            (login,),
        ).fetchone()
        if not user:
            return None
        if not user["is_active"]:
            return None
        if not verify_password(password, user["password_hash"], user["salt"]):
            return None

        # Проверка срока
        days_left = _days_left(user["expires_at"])
        if days_left <= 0 and user["expires_at"]:
            return None  # подписка истекла

        # Автомиграция хеша: если старый sha256 — перехешируем
        try:
            new_hash = _hash_pbkdf2(password, user["salt"])
            if new_hash != user["password_hash"]:
                conn.execute(
                    "UPDATE users SET password_hash = ? WHERE id = ?",
                    (new_hash, user["id"]),
                )
                conn.commit()
        except Exception:
            pass

        return {
            "id": user["id"],
            "login": user["login"],
            "tariff": user["tariff"],
            "hwid": user["hwid"],
            "activated_at": user["activated_at"],
            "expires_at": user["expires_at"],
            "is_active": bool(user["is_active"]),
            "days_left": days_left,
            "days_total": _days_total(user["activated_at"], user["expires_at"]),
        }


def set_user_hwid(login: str, hwid: str) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE users SET hwid = ? WHERE login = ?", (hwid, login)
        )
        conn.commit()
        return cur.rowcount > 0


# ============================================================
# Admin: list / update / delete
# ============================================================
def list_users() -> List[Dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, login, tariff, activated_at, expires_at,
                      is_active, hwid
               FROM users ORDER BY id"""
        ).fetchall()
    result = []
    for r in rows:
        result.append({
            "id": r["id"],
            "login": r["login"],
            "tariff": r["tariff"],
            "activated_at": r["activated_at"],
            "expires_at": r["expires_at"],
            "is_active": bool(r["is_active"]),
            "hwid": r["hwid"],
            "days_left": _days_left(r["expires_at"]),
        })
    return result


def delete_user(login: str) -> bool:
    with get_db() as conn:
        cur = conn.execute("DELETE FROM users WHERE login = ?", (login,))
        conn.commit()
        return cur.rowcount > 0


def reset_user_hwid(login: str) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE users SET hwid = NULL WHERE login = ?", (login,)
        )
        conn.commit()
        return cur.rowcount > 0


def set_user_active(login: str, active: bool) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE users SET is_active = ? WHERE login = ?",
            (1 if active else 0, login),
        )
        conn.commit()
        return cur.rowcount > 0


def set_user_tariff(login: str, tariff: str) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE users SET tariff = ? WHERE login = ?", (tariff, login)
        )
        conn.commit()
        return cur.rowcount > 0


def extend_user(login: str, days: int) -> bool:
    """
    Продлевает подписку на N дней от текущего expires_at
    (или от now, если уже истёк).
    """
    with get_db() as conn:
        row = conn.execute(
            "SELECT expires_at FROM users WHERE login = ?", (login,)
        ).fetchone()
        if not row:
            return False
        current = row["expires_at"]
        try:
            base = datetime.fromisoformat(current) if current else datetime.now()
        except Exception:
            base = datetime.now()
        if base < datetime.now():
            base = datetime.now()
        new_expires = (base + timedelta(days=days)).isoformat(timespec="seconds")
        conn.execute(
            "UPDATE users SET expires_at = ? WHERE login = ?",
            (new_expires, login),
        )
        conn.commit()
        return True


def set_user_expiry(login: str, expires_at_iso: str) -> bool:
    """Установить конкретную дату окончания (ISO-строка)."""
    try:
        datetime.fromisoformat(expires_at_iso)
    except Exception:
        return False
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE users SET expires_at = ? WHERE login = ?",
            (expires_at_iso, login),
        )
        conn.commit()
        return cur.rowcount > 0


def change_password(login: str, new_password: str) -> bool:
    h, s = hash_password(new_password)
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE users SET password_hash = ?, salt = ? WHERE login = ?",
            (h, s, login),
        )
        conn.commit()
        return cur.rowcount > 0