# core/auth.py
import sqlite3
import hashlib
import os
from datetime import datetime
from typing import Optional

DB_PATH = "users.db"


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
        # Индекс для быстрого поиска по логину
        conn.execute("CREATE INDEX IF NOT EXISTS idx_login ON users(login)")


def hash_password(password: str, salt: Optional[str] = None) -> tuple:
    """Хеширует пароль с солью. Возвращает (hash, salt)."""
    if salt is None:
        salt = os.urandom(16).hex()
    pwd = password + salt
    hash_val = hashlib.sha256(pwd.encode()).hexdigest()
    return hash_val, salt


def verify_password(password: str, hash_val: str, salt: str) -> bool:
    """Проверяет пароль."""
    pwd = password + salt
    return hashlib.sha256(pwd.encode()).hexdigest() == hash_val


def create_user(login: str, password: str, tariff: str = "trial") -> bool:
    """Создаёт нового пользователя."""
    hash_val, salt = hash_password(password)
    with get_db() as conn:
        try:
            conn.execute(
                "INSERT INTO users (login, password_hash, salt, tariff, activated_at) VALUES (?, ?, ?, ?, ?)",
                (login, hash_val, salt, tariff, datetime.now().isoformat())
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def authenticate_user(login: str, password: str) -> Optional[dict]:
    """Проверяет логин/пароль, возвращает данные пользователя или None."""
    with get_db() as conn:
        user = conn.execute(
            "SELECT id, login, password_hash, salt, hwid, tariff, expires_at, is_active FROM users WHERE login = ?",
            (login,)
        ).fetchone()
        if not user:
            return None
        if not user["is_active"]:
            return None
        if not verify_password(password, user["password_hash"], user["salt"]):
            return None
        return dict(user)