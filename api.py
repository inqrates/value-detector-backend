# api.py
import os
import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, HTTPException, Request, Header, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from core.aggregator import OddsAggregator
from core.auth import (
    authenticate_user, create_user, init_db, get_db,
    list_users, delete_user, reset_user_hwid, set_user_active,
    set_user_tariff, extend_user, set_user_expiry, change_password,
    set_user_hwid,
)
from core.jwt_auth import (
    create_access_token, create_refresh_token,
    verify_access_token, jwt_optional,
)
from pydantic import BaseModel

import json
import logging

logger = logging.getLogger(__name__)

app = FastAPI(title="Odds Aggregator API", version="1.0")

aggregator: Optional[OddsAggregator] = None


# ============================================================
# БЕЗОПАСНОСТЬ: Middleware
# ============================================================

# ── CORS: разрешаем только доверенные домены ──
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "").split(",")
if ALLOWED_ORIGINS == [""]:
    ALLOWED_ORIGINS = [
        "http://localhost:8000",
        "http://localhost:3000",
        "https://respectfully-smiling-hake.cloudpub.ru",
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "X-Admin-Token"],
)

# ── TrustedHost: разрешаем только доверенные хосты ──
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"],  # Можно сузить через ALLOWED_HOSTS
)

# ── Security headers ──
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    # HSTS (только если HTTPS)
    if request.url.scheme == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# ============================================================
# WS-клиент с подписками на каналы и матчи
# ============================================================
@dataclass
class WsClient:
    ws: WebSocket
    channels: Set[str] = field(default_factory=lambda: {
        # дефолт = «всё, что было раньше» (обратная совместимость)
        "signals", "arbitrage", "value", "corridor",
        "advisor", "health",
    })
    match_keys: Set[str] = field(default_factory=set)


websocket_clients: Dict[WebSocket, WsClient] = {}

_health_monitor = None


def set_aggregator(agg: OddsAggregator):
    global aggregator
    aggregator = agg


def set_health_monitor(monitor):
    global _health_monitor
    _health_monitor = monitor


# ============================================================
# ADMIN TOKEN
# ============================================================
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

if not ADMIN_TOKEN:
    logger.warning(
        "⚠️ ADMIN_TOKEN не задан в переменных окружения! "
        "Админка будет недоступна. "
        "Задай переменную ADMIN_TOKEN=<длинный_секрет> и перезапусти."
    )


def _check_admin(token: Optional[str]):
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="ADMIN_TOKEN not configured")
    if not token or token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")


# ============================================================
# RATE LIMIT на /auth/login (по IP)
# ============================================================
_login_attempts: Dict[str, list] = defaultdict(list)
_LOGIN_WINDOW_SEC = 60
_LOGIN_MAX_ATTEMPTS = 5


def _check_rate_limit(ip: str):
    now = time.time()
    attempts = _login_attempts[ip]
    _login_attempts[ip] = [t for t in attempts if now - t < _LOGIN_WINDOW_SEC]
    if len(_login_attempts[ip]) >= _LOGIN_MAX_ATTEMPTS:
        raise HTTPException(
            status_code=429,
            detail=f"Слишком много попыток. Подожди {_LOGIN_WINDOW_SEC} сек.",
        )
    _login_attempts[ip].append(now)


# ============================================================
# WebSocket
# ============================================================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: Optional[str] = Query(None)):
    """
    WebSocket с поддержкой JWT-аутентификации.
    Токен передаётся через query-параметр: ws://host/ws?token=JWT
    """
    # ── Проверка JWT токена (если передан) ──
    if token:
        from core.jwt_auth import verify_access_token
        user = verify_access_token(token)
        if not user:
            # Токен невалиден — закрываем без accept
            await websocket.close(code=4001, reason="Invalid or expired JWT token")
            return
        logger.info(f"🔐 WS подключился: {user['login']}")
    else:
        logger.info("🔓 WS подключился (без JWT — только для отладки)")

    await websocket.accept()
    client = WsClient(ws=websocket)
    websocket_clients[websocket] = client
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            t = msg.get("type")
            if t == "subscribe":
                ch = set(msg.get("channels") or [])
                if ch:
                    client.channels = ch
                    logger.info(f"📡 WS subscribe: channels={sorted(ch)}")
            elif t == "subscribe_match":
                k = msg.get("key")
                if k:
                    client.match_keys.add(k)
                    logger.info(f"🔔 WS subscribe_match: {k}")
                    snap = _build_match_state_snapshot(k)
                    if snap:
                        try:
                            await websocket.send_text(json.dumps(
                                {"type": "match_state", **snap},
                                ensure_ascii=False,
                            ))
                        except Exception:
                            pass
            elif t == "unsubscribe_match":
                k = msg.get("key")
                if k:
                    client.match_keys.discard(k)
                    logger.info(f"🔕 WS unsubscribe_match: {k}")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        websocket_clients.pop(websocket, None)


async def broadcast_message(message: dict, channel: Optional[str] = None):
    if not websocket_clients:
        return
    data = json.dumps(message, ensure_ascii=False)
    for ws, cl in list(websocket_clients.items()):
        if channel and channel not in cl.channels:
            continue
        try:
            await ws.send_text(data)
        except Exception:
            websocket_clients.pop(ws, None)


async def push_match_state(key: str, bks_payload: dict, now: float):
    """Пуш только тем, кто подписан на этот match_key."""
    if not websocket_clients:
        return
    data = json.dumps(
        {"type": "match_state", "key": key, "bks": bks_payload, "now": now},
        ensure_ascii=False,
    )
    for ws, cl in list(websocket_clients.items()):
        if key in cl.match_keys:
            try:
                await ws.send_text(data)
            except Exception:
                websocket_clients.pop(ws, None)


def _build_match_state_snapshot(key: str) -> Optional[dict]:
    if aggregator is None:
        return None
    bk_data = aggregator._matches.get(key)
    if not bk_data:
        return None
    bks = {}
    for bk_id, m in bk_data.items():
        bks[bk_id] = {
            "match_id": m.match_id,
            "score1": m.score1,
            "score2": m.score2,
            "sub_score1": m.sub_score1,
            "sub_score2": m.sub_score2,
            "odds1": m.odds1,
            "odds2": m.odds2,
            "raw_time": getattr(m, "raw_time", "") or "",
            "sport": getattr(m, "sport", "table_tennis"),
            "timestamp": m.timestamp,
            "match_url": getattr(m, "match_url", "") or "",
        }
    return {"key": key, "bks": bks, "now": time.time()}


# ============================================================
# Основные эндпоинты (не трогаем)
# ============================================================
@app.get("/matches", response_model=List[Dict])
async def get_matches():
    if aggregator is None:
        return []
    return await asyncio.to_thread(aggregator.get_all_matches)


@app.get("/arbitrage", response_model=List[Dict])
async def get_arbitrage(min_profit: float = Query(0.5, ge=0)):
    if aggregator is None:
        return []
    return await asyncio.to_thread(aggregator.find_arbitrage, min_profit)


@app.get("/value", response_model=List[Dict])
async def get_value(threshold: float = Query(1.05, ge=1.01)):
    if aggregator is None:
        return []
    return await asyncio.to_thread(aggregator.find_value_bets, threshold)


@app.get("/corridors", response_model=List[Dict])
async def get_corridors():
    if aggregator is None:
        return []
    return await asyncio.to_thread(aggregator.find_corridors)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/health/parsers")
async def health_parsers():
    if _health_monitor is None:
        return {"error": "health monitor not initialized"}
    return _health_monitor.snapshot()


# ============================================================
# Match State (REST — оставлен для отладки и fallback)
# ============================================================

# ── Rate-limit для match_state ──
# ВАЖНО: легитимный клиент опрашивает /match_state РАЗ В СЕКУНДУ
# на каждый мониторимый матч (ui/after_goal_engine.py:
# FAST_STATE_POLL_INTERVAL = 1.0). При 2-5 одновременных матчах
# это 2-5 запросов/сек = 120-300 в минуту с одного IP.
# Поэтому лимит 900/мин (15 сек/сек) — отсекает DDoS, но не мешает
# работе. WS-пуш (push_match_state) — основной канал, REST это fallback.
_match_state_attempts: Dict[str, list] = defaultdict(list)
_MATCH_STATE_WINDOW = 60
_MATCH_STATE_MAX = 900
_MATCH_STATE_PRUNE_EVERY = 300   # раз в N запросов чистим пустые IP
_match_state_counter = 0


def _check_match_state_limit(ip: str):
    """Ограничивает запросы к /match_state: 900 в минуту с одного IP."""
    global _match_state_counter
    now = time.time()
    attempts = _match_state_attempts[ip]
    _match_state_attempts[ip] = [t for t in attempts if now - t < _MATCH_STATE_WINDOW]
    if len(_match_state_attempts[ip]) >= _MATCH_STATE_MAX:
        raise HTTPException(
            status_code=429,
            detail=f"Слишком много запросов. Подожди {_MATCH_STATE_WINDOW} сек.",
        )
    _match_state_attempts[ip].append(now)

    # Периодическая чистка: не даём словарю расти бесконечно
    _match_state_counter += 1
    if _match_state_counter >= _MATCH_STATE_PRUNE_EVERY:
        _match_state_counter = 0
        for stale_ip in [
            k for k, v in _match_state_attempts.items()
            if not any(now - t < _MATCH_STATE_WINDOW for t in v)
        ]:
            _match_state_attempts.pop(stale_ip, None)


@app.get("/match_state")
async def get_match_state(
    sport: str = Query(..., min_length=1, max_length=50),
    player1: str = Query(..., min_length=2, max_length=100),
    player2: str = Query(..., min_length=2, max_length=100),
    request: Request = None,
):
    """
    Возвращает состояние матча по именам игроков.
    Защищён rate-limit (30 запросов/мин с одного IP).
    """
    client_ip = request.client.host if request.client else "unknown"
    _check_match_state_limit(client_ip)

    if aggregator is None:
        return {"found": False, "reason": "aggregator not ready"}

    from core.normalizer import normalizer
    p1 = normalizer.normalize_name(player1 or "")
    p2 = normalizer.normalize_name(player2 or "")
    if not p1 or not p2:
        return {"found": False, "reason": "empty players"}
    if p1 > p2:
        p1, p2 = p2, p1

    sport_key = (sport or "table_tennis").lower()
    key = f"{sport_key}::{p1}||{p2}"

    bk_data = aggregator._matches.get(key)
    if not bk_data:
        return {"found": False, "key": key, "now": time.time()}

    bks_payload = {}
    for bk_id, m in bk_data.items():
        bks_payload[bk_id] = {
            "match_id": m.match_id,
            "score1": m.score1,
            "score2": m.score2,
            "sub_score1": m.sub_score1,
            "sub_score2": m.sub_score2,
            "odds1": m.odds1,
            "odds2": m.odds2,
            "raw_time": getattr(m, "raw_time", "") or "",
            "sport": getattr(m, "sport", sport_key),
            "timestamp": m.timestamp,
            "match_url": getattr(m, "match_url", "") or "",
        }

    return {
        "found": True,
        "key": key,
        "now": time.time(),
        "bks": bks_payload,
    }


# ============================================================
# АВТОРИЗАЦИЯ
# ============================================================
init_db()


class LoginRequest(BaseModel):
    login: str
    password: str
    hwid: Optional[str] = None


@app.post("/auth/login")
async def login(req: LoginRequest, request: Request):
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    user = authenticate_user(req.login, req.password)
    if not user:
        logger.info(f"❌ Неудачный логин: {req.login} с {client_ip}")
        return {"success": False, "message": "Неверный логин или пароль (или подписка истекла)"}

    logger.info(f"✅ Логин: {req.login}, IP: {client_ip}")

    if req.hwid:
        if user["hwid"] is None:
            set_user_hwid(req.login, req.hwid)
            logger.info(f"🔗 HWID привязан к {req.login}")
            user["hwid"] = req.hwid
        elif user["hwid"] != req.hwid:
            logger.warning(
                f"🚫 Попытка входа с чужого устройства: {req.login}, "
                f"HWID={req.hwid[:12]}…, ожидался={user['hwid'][:12]}…"
            )
            return {"success": False, "message": "Аккаунт привязан к другому устройству"}

    # ── JWT токены ──
    access_token = create_access_token(user)
    refresh_token = create_refresh_token(user)

    return {
        "success": True,
        "user": {
            "id": user["id"],
            "login": user["login"],
            "tariff": user["tariff"],
            "expires_at": user["expires_at"],
            "days_left": user["days_left"],
            "days_total": user["days_total"],
        },
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "Bearer",
    }


class RefreshRequest(BaseModel):
    refresh_token: str


@app.post("/auth/refresh")
async def refresh(req: RefreshRequest):
    """Обновляет access-токен по refresh-токену."""
    from core.jwt_auth import verify_refresh_token
    user_info = verify_refresh_token(req.refresh_token)
    if not user_info:
        return {"success": False, "message": "Refresh-токен недействителен или истёк"}

    # Генерируем новый access
    from core.auth import get_db
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, login, tariff FROM users WHERE login = ? AND is_active = 1",
            (user_info["login"],),
        ).fetchone()
        if not row:
            return {"success": False, "message": "Пользователь не найден или деактивирован"}
        user_data = {"id": row["id"], "login": row["login"], "tariff": row["tariff"]}

    access_token = create_access_token(user_data)
    return {
        "success": True,
        "access_token": access_token,
        "token_type": "Bearer",
    }


class RegisterRequest(BaseModel):
    login: str
    password: str
    tariff: str = "trial"
    days: int = 30


@app.post("/auth/register")
async def register(req: RegisterRequest, x_admin_token: Optional[str] = Header(None)):
    _check_admin(x_admin_token)
    if create_user(req.login, req.password, req.tariff, req.days):
        return {"success": True, "message": f"Пользователь {req.login} создан"}
    return {"success": False, "message": "Логин уже занят"}


# ============================================================
# АДМИН API
# ============================================================
def _admin_guard(x_admin_token: Optional[str] = Header(None)):
    _check_admin(x_admin_token)


class AdminCreateUser(BaseModel):
    login: str
    password: str
    tariff: str = "trial"
    days: int = 30


class AdminUpdateUser(BaseModel):
    tariff: Optional[str] = None
    days_left: Optional[int] = None
    expires_at: Optional[str] = None
    is_active: Optional[bool] = None
    new_password: Optional[str] = None
    reset_hwid: bool = False


@app.get("/admin/users", dependencies=[Depends(_admin_guard)])
async def admin_list_users():
    users = await asyncio.to_thread(list_users)
    return {"success": True, "users": users}


@app.post("/admin/users", dependencies=[Depends(_admin_guard)])
async def admin_create_user(req: AdminCreateUser):
    ok = create_user(req.login, req.password, req.tariff, req.days)
    return {"success": ok, "message": "Создан" if ok else "Логин занят"}


@app.delete("/admin/users/{login}", dependencies=[Depends(_admin_guard)])
async def admin_delete_user(login: str):
    ok = delete_user(login)
    return {"success": ok, "message": "Удалён" if ok else "Не найден"}


@app.patch("/admin/users/{login}", dependencies=[Depends(_admin_guard)])
async def admin_update_user(login: str, upd: AdminUpdateUser):
    changes = []
    if upd.tariff is not None:
        if set_user_tariff(login, upd.tariff):
            changes.append(f"тариф → {upd.tariff}")
    if upd.days_left is not None and upd.days_left > 0:
        if extend_user(login, upd.days_left):
            changes.append(f"продлено на {upd.days_left} дн.")
    if upd.expires_at:
        if set_user_expiry(login, upd.expires_at):
            changes.append(f"expires_at → {upd.expires_at}")
    if upd.is_active is not None:
        if set_user_active(login, upd.is_active):
            changes.append(f"is_active → {upd.is_active}")
    if upd.new_password:
        if change_password(login, upd.new_password):
            changes.append("пароль изменён")
    if upd.reset_hwid:
        if reset_user_hwid(login):
            changes.append("HWID сброшен")
    return {"success": True, "changes": changes}


# ============================================================
# HTML-АДМИНКА — не менял, скопирована 1-в-1 из твоего файла
# ============================================================
ADMIN_HTML = r"""
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Admin · Users</title>
<style>
  * { box-sizing: border-box; }
  body { margin: 0; padding: 16px; background: #0e1116; color: #e6edf3;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 14px; }
  h1 { font-size: 18px; margin: 0 0 12px; color: #7dd3e8; }
  .card { background: #161b22; border: 1px solid #30363d;
    border-radius: 10px; padding: 14px; margin-bottom: 14px; }
  label { display: block; margin: 6px 0 4px; color: #9aa7b4; font-size: 12px; }
  input, select { width: 100%; padding: 8px 10px; background: #0d1117;
    border: 1px solid #30363d; border-radius: 6px; color: #e6edf3; font-size: 14px; }
  button { padding: 8px 14px; border: none; border-radius: 6px;
    background: #08a7c8; color: #fff; font-weight: 600; cursor: pointer; font-size: 13px; }
  button:hover { background: #21c1de; }
  button.danger { background: #da3633; }
  button.danger:hover { background: #f85149; }
  button.small { padding: 4px 10px; font-size: 12px; }
  table { width: 100%; border-collapse: collapse; margin-top: 8px; }
  th, td { padding: 8px 6px; text-align: left;
    border-bottom: 1px solid #21262d; font-size: 12px; }
  th { color: #7dd3e8; font-weight: 600; }
  tr.expired td { color: #f85149; }
  tr.inactive td { color: #8b949e; }
  .row-actions { display: flex; gap: 6px; flex-wrap: wrap; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  @media (max-width: 600px) { .grid { grid-template-columns: 1fr; }
    th, td { padding: 6px 4px; font-size: 11px; } }
  .msg { padding: 8px 12px; border-radius: 6px; margin: 8px 0; font-size: 13px; }
  .msg.ok { background: #1a3a1f; color: #7ee787; }
  .msg.err { background: #3d1f1f; color: #f85149; }
  .hidden { display: none; }
</style>
</head>
<body>
<h1>🛠 Value Detector · Admin</h1>
<div class="card">
  <label>Admin Token</label>
  <input id="token" type="password" placeholder="вставь токен" autocomplete="off">
  <button style="margin-top:10px;" onclick="saveToken()">Войти</button>
  <div id="loginMsg"></div>
</div>
<div id="panel" class="hidden">
  <div class="card">
    <h3 style="margin-top:0;font-size:15px;color:#7dd3e8;">➕ Создать пользователя</h3>
    <div class="grid">
      <div><label>Логин</label><input id="newLogin" autocomplete="off"></div>
      <div><label>Пароль</label><input id="newPassword" type="text" autocomplete="off"></div>
      <div><label>Тариф</label>
        <select id="newTariff">
          <option value="trial">trial</option>
          <option value="pro">pro</option>
          <option value="premium">premium</option>
          <option value="lifetime">lifetime</option>
        </select></div>
      <div><label>Дней</label><input id="newDays" type="number" value="30" min="1"></div>
    </div>
    <button style="margin-top:12px;" onclick="createUser()">Создать</button>
  </div>
  <div class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <h3 style="margin:0;font-size:15px;color:#7dd3e8;">👥 Пользователи</h3>
      <button class="small" onclick="loadUsers()">↻ Обновить</button>
    </div>
    <div id="usersTable"></div>
  </div>
  <div class="card">
    <button class="danger small" onclick="logout()">Выйти из админки</button>
  </div>
</div>
<script>
const API = "";
let TOKEN = "";
function msg(text, ok=true) {
  const el = document.getElementById("loginMsg");
  el.innerHTML = '<div class="msg ' + (ok ? 'ok' : 'err') + '">' + text + '</div>';
  setTimeout(() => el.innerHTML = "", 4000);
}
function saveToken() {
  TOKEN = document.getElementById("token").value.trim();
  if (!TOKEN) return;
  localStorage.setItem("admin_token", TOKEN);
  checkToken();
}
function logout() {
  localStorage.removeItem("admin_token");
  TOKEN = "";
  document.getElementById("panel").classList.add("hidden");
  document.getElementById("token").value = "";
}
async function api(method, path, body) {
  const opts = { method, headers: { "X-Admin-Token": TOKEN } };
  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const r = await fetch(API + path, opts);
  if (r.status === 403 || r.status === 503) {
    throw new Error("Неверный токен или админка не настроена");
  }
  return await r.json();
}
async function checkToken() {
  try {
    await api("GET", "/admin/users");
    document.getElementById("panel").classList.remove("hidden");
    msg("OK", true); loadUsers();
  } catch (e) {
    msg(e.message, false);
    document.getElementById("panel").classList.add("hidden");
  }
}
async function loadUsers() {
  try {
    const data = await api("GET", "/admin/users");
    if (!data.success) throw new Error("Ошибка");
    renderUsers(data.users);
  } catch (e) { msg(e.message, false); }
}
function renderUsers(users) {
  if (!users.length) {
    document.getElementById("usersTable").innerHTML = '<p style="color:#8b949e;">Пусто</p>';
    return;
  }
  let html = '<table><thead><tr>' +
    '<th>Логин</th><th>Тариф</th><th>Истекает</th><th>Дней</th>' +
    '<th>Активен</th><th>HWID</th><th></th></tr></thead><tbody>';
  for (const u of users) {
    const cls = u.days_left <= 0 ? "expired" : (!u.is_active ? "inactive" : "");
    html += '<tr class="' + cls + '">' +
      '<td><b>' + esc(u.login) + '</b></td>' +
      '<td>' + esc(u.tariff) + '</td>' +
      '<td>' + (u.expires_at ? u.expires_at.slice(0,10) : '—') + '</td>' +
      '<td>' + u.days_left + '</td>' +
      '<td>' + (u.is_active ? '✅' : '❌') + '</td>' +
      '<td>' + (u.hwid ? esc(u.hwid.slice(0,10)) + '…' : '—') + '</td>' +
      '<td><div class="row-actions">' +
        '<button class="small" onclick="extendUser(\'' + esc(u.login) + '\')">+30д</button>' +
        '<button class="small" onclick="resetHwid(\'' + esc(u.login) + '\')">HWID</button>' +
        '<button class="small" onclick="toggleActive(\'' + esc(u.login) + '\',' + (u.is_active?1:0) + ')">' + (u.is_active?'Стоп':'Вкл') + '</button>' +
        '<button class="small" onclick="changePwd(\'' + esc(u.login) + '\')">Пароль</button>' +
        '<button class="small danger" onclick="deleteUser(\'' + esc(u.login) + '\')">✕</button>' +
      '</div></td></tr>';
  }
  html += '</tbody></table>';
  document.getElementById("usersTable").innerHTML = html;
}
function esc(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[c]);
}
async function createUser() {
  const body = {
    login: document.getElementById("newLogin").value.trim(),
    password: document.getElementById("newPassword").value.trim(),
    tariff: document.getElementById("newTariff").value,
    days: parseInt(document.getElementById("newDays").value, 10) || 30,
  };
  if (!body.login || !body.password) return msg("Заполни логин и пароль", false);
  try {
    const r = await api("POST", "/admin/users", body);
    msg(r.message, r.success);
    if (r.success) {
      document.getElementById("newLogin").value = "";
      document.getElementById("newPassword").value = "";
      loadUsers();
    }
  } catch (e) { msg(e.message, false); }
}
async function deleteUser(login) {
  if (!confirm("Удалить " + login + "?")) return;
  try {
    const r = await api("DELETE", "/admin/users/" + encodeURIComponent(login));
    msg(r.message, r.success); loadUsers();
  } catch (e) { msg(e.message, false); }
}
async function extendUser(login) {
  try {
    const r = await api("PATCH", "/admin/users/" + encodeURIComponent(login), { days_left: 30 });
    msg(r.changes.join(", "), true); loadUsers();
  } catch (e) { msg(e.message, false); }
}
async function resetHwid(login) {
  if (!confirm("Сбросить HWID для " + login + "?")) return;
  try {
    const r = await api("PATCH", "/admin/users/" + encodeURIComponent(login), { reset_hwid: true });
    msg(r.changes.join(", "), true); loadUsers();
  } catch (e) { msg(e.message, false); }
}
async function toggleActive(login, isActive) {
  try {
    const r = await api("PATCH", "/admin/users/" + encodeURIComponent(login), { is_active: !isActive });
    msg(r.changes.join(", "), true); loadUsers();
  } catch (e) { msg(e.message, false); }
}
async function changePwd(login) {
  const pwd = prompt("Новый пароль для " + login + ":");
  if (!pwd) return;
  try {
    const r = await api("PATCH", "/admin/users/" + encodeURIComponent(login), { new_password: pwd });
    msg(r.changes.join(", "), true);
  } catch (e) { msg(e.message, false); }
}
window.addEventListener("load", () => {
  const saved = localStorage.getItem("admin_token");
  if (saved) {
    document.getElementById("token").value = saved;
    TOKEN = saved;
    checkToken();
  }
});
</script>
</body>
</html>
"""


@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    return HTMLResponse(ADMIN_HTML)