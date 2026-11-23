"""
JWT-аутентификация для API и WebSocket.

Генерирует JWT-токены при успешном логине.
Валидирует токены в защищённых эндпоинтах.

БЕЗОПАСНОСТЬ:
  - Токен хранится на клиенте (в localStorage фронтенда)
  - Срок жизни: ACCESS_TOKEN_MINUTES (короткий, для сессии)
  - Refresh-токен: 12 часов (для обновления access)
  - Секрет берётся из JWT_SECRET или генерируется при старте
"""
import os
import time
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

try:
    import jwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False
    logging.warning("PyJWT не установлен — JWT отключён")

logger = logging.getLogger(__name__)

# ── Конфигурация ──
JWT_SECRET = os.getenv("JWT_SECRET", "").strip()
if not JWT_SECRET:
    # Если не задан — генерируем при старте (не сохраняется между перезапусками)
    import secrets
    JWT_SECRET = secrets.token_urlsafe(64)
    logger.warning("⚠️ JWT_SECRET не задан — генерируется новый при каждом старте")

ACCESS_TOKEN_MINUTES = int(os.getenv("JWT_ACCESS_MINUTES", "120"))   # 2 часа
REFRESH_TOKEN_HOURS = int(os.getenv("JWT_REFRESH_HOURS", "12"))       # 12 часов


def _ensure_jwt():
    """Проверяет, что PyJWT доступен."""
    if not JWT_AVAILABLE:
        raise RuntimeError("PyJWT не установлен. Установите: pip install PyJWT")


# ============================================================
# Генерация токенов
# ============================================================
def create_access_token(user_data: Dict[str, Any]) -> str:
    """Создаёт access-токен (короткоживущий)."""
    _ensure_jwt()
    now = datetime.utcnow()
    payload = {
        "sub": user_data["login"],
        "id": user_data["id"],
        "tariff": user_data.get("tariff", "trial"),
        "iat": now,
        "exp": now + timedelta(minutes=ACCESS_TOKEN_MINUTES),
        "type": "access",
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    logger.info(f"JWT: access-токен создан для {user_data['login']}")
    return token


def create_refresh_token(user_data: Dict[str, Any]) -> str:
    """Создаёт refresh-токен (долгоживущий, для обновления access)."""
    _ensure_jwt()
    now = datetime.utcnow()
    payload = {
        "sub": user_data["login"],
        "id": user_data["id"],
        "iat": now,
        "exp": now + timedelta(hours=REFRESH_TOKEN_HOURS),
        "type": "refresh",
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    return token


# ============================================================
# Валидация токенов
# ============================================================
def decode_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Расшифровывает и проверяет токен.
    Возвращает payload или None.
    """
    if not JWT_AVAILABLE:
        return None
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        logger.debug("JWT: токен истёк")
        return None
    except jwt.InvalidTokenError as e:
        logger.debug(f"JWT: невалидный токен: {e}")
        return None


def verify_access_token(token: str) -> Optional[Dict[str, Any]]:
    """Проверяет access-токен и возвращает user data."""
    payload = decode_token(token)
    if not payload:
        return None
    if payload.get("type") != "access":
        return None
    return {
        "login": payload["sub"],
        "id": payload["id"],
        "tariff": payload.get("tariff", "trial"),
    }


def verify_refresh_token(token: str) -> Optional[Dict[str, Any]]:
    """Проверяет refresh-токен."""
    payload = decode_token(token)
    if not payload:
        return None
    if payload.get("type") != "refresh":
        return None
    return {
        "login": payload["sub"],
        "id": payload["id"],
    }


# ============================================================
# Middleware для FastAPI (автоматическая проверка токена)
# ============================================================
def jwt_required(token: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Хелпер для зависимостей FastAPI.
    Проверяет Bearer-токен в заголовке Authorization.
    """
    if not token:
        return None
    # Ожидаем формат "Bearer <token>"
    if token.startswith("Bearer "):
        token = token[7:]
    return verify_access_token(token)


def jwt_optional(token: Optional[str]) -> Optional[Dict[str, Any]]:
    """Та же проверка, но не блокирует если токен отсутствует."""
    if not token:
        return None
    if token.startswith("Bearer "):
        token = token[7:]
    return verify_access_token(token)
