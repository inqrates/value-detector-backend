# api.py
import os
import time
from collections import defaultdict
from typing import List, Dict, Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, HTTPException, Request, Header, Depends
from fastapi.responses import HTMLResponse, JSONResponse

from core.aggregator import OddsAggregator
from core.auth import (
    authenticate_user, create_user, init_db, get_db,
    list_users, delete_user, reset_user_hwid, set_user_active,
    set_user_tariff, extend_user, set_user_expiry, change_password,
    set_user_hwid,
)
from pydantic import BaseModel

import json
import logging

logger = logging.getLogger(__name__)

app = FastAPI(title="Odds Aggregator API", version="1.0")

aggregator: Optional[OddsAggregator] = None
websocket_clients = set()

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
    # чистим старые
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
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        websocket_clients.discard(websocket)
    except Exception:
        websocket_clients.discard(websocket)


async def broadcast_message(message: dict):
    if not websocket_clients:
        return
    data = json.dumps(message)
    for client in list(websocket_clients):
        try:
            await client.send_text(data)
        except Exception:
            websocket_clients.discard(client)


# ============================================================
# Основные эндпоинты
# ============================================================
@app.get("/matches", response_model=List[Dict])
async def get_matches():
    if aggregator is None:
        return []
    return aggregator.get_all_matches()


@app.get("/arbitrage", response_model=List[Dict])
async def get_arbitrage(min_profit: float = Query(0.5, ge=0)):
    if aggregator is None:
        return []
    return aggregator.find_arbitrage(min_profit)


@app.get("/value", response_model=List[Dict])
async def get_value(threshold: float = Query(1.05, ge=1.01)):
    if aggregator is None:
        return []
    return aggregator.find_value_bets(threshold)


@app.get("/corridors", response_model=List[Dict])
async def get_corridors():
    if aggregator is None:
        return []
    return aggregator.find_corridors()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/health/parsers")
async def health_parsers():
    if _health_monitor is None:
        return {"error": "health monitor not initialized"}
    return _health_monitor.snapshot()


# ============================================================
# Match State
# ============================================================
@app.get("/match_state")
async def get_match_state(sport: str, player1: str, player2: str):
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
    # Rate limit по IP
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)

    user = authenticate_user(req.login, req.password)
    if not user:
        logger.info(f"❌ Неудачный логин: {req.login} с {client_ip}")
        return {"success": False, "message": "Неверный логин или пароль (или подписка истекла)"}

    logger.info(f"✅ Логин: {req.login}, HWID: {req.hwid}, IP: {client_ip}")

    # HWID-привязка
    if req.hwid:
        if user["hwid"] is None:
            set_user_hwid(req.login, req.hwid)
            logger.info(f"🔗 HWID привязан к {req.login}")
            user["hwid"] = req.hwid
        elif user["hwid"] != req.hwid:
            logger.warning(
                f"🚫 Попытка входа с чужого устройства: {req.login}, "
                f"HWID={req.hwid}, ожидался={user['hwid']}"
            )
            return {"success": False, "message": "Аккаунт привязан к другому устройству"}

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
    }


# ── Публичная регистрация ЗАКРЫТА. Только через админку. ──
# Оставлен старый эндпоинт, но теперь требует admin-токен.
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
    days_left: Optional[int] = None    # для продления на N дней
    expires_at: Optional[str] = None   # конкретная дата (ISO)
    is_active: Optional[bool] = None
    new_password: Optional[str] = None
    reset_hwid: bool = False


@app.get("/admin/users", dependencies=[Depends(_admin_guard)])
async def admin_list_users():
    return {"success": True, "users": list_users()}


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
# HTML-АДМИНКА (одна страница, с телефона тоже работает)
# ============================================================
ADMIN_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Admin · Users</title>
<style>
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 16px;
    background: #0e1116; color: #e6edf3;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 14px;
  }
  h1 { font-size: 18px; margin: 0 0 12px; color: #7dd3e8; }
  .card {
    background: #161b22; border: 1px solid #30363d;
    border-radius: 10px; padding: 14px; margin-bottom: 14px;
  }
  label { display: block; margin: 6px 0 4px; color: #9aa7b4; font-size: 12px; }
  input, select {
    width: 100%; padding: 8px 10px;
    background: #0d1117; border: 1px solid #30363d;
    border-radius: 6px; color: #e6edf3; font-size: 14px;
  }
  button {
    padding: 8px 14px; border: none; border-radius: 6px;
    background: #08a7c8; color: #fff; font-weight: 600;
    cursor: pointer; font-size: 13px;
  }
  button:hover { background: #21c1de; }
  button.danger { background: #da3633; }
  button.danger:hover { background: #f85149; }
  button.small { padding: 4px 10px; font-size: 12px; }
  table { width: 100%; border-collapse: collapse; margin-top: 8px; }
  th, td {
    padding: 8px 6px; text-align: left;
    border-bottom: 1px solid #21262d; font-size: 12px;
  }
  th { color: #7dd3e8; font-weight: 600; }
  tr.expired td { color: #f85149; }
  tr.inactive td { color: #8b949e; }
  .row-actions { display: flex; gap: 6px; flex-wrap: wrap; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  @media (max-width: 600px) {
    .grid { grid-template-columns: 1fr; }
    th, td { padding: 6px 4px; font-size: 11px; }
  }
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
      <div>
        <label>Логин</label>
        <input id="newLogin" autocomplete="off">
      </div>
      <div>
        <label>Пароль</label>
        <input id="newPassword" type="text" autocomplete="off">
      </div>
      <div>
        <label>Тариф</label>
        <select id="newTariff">
          <option value="trial">trial</option>
          <option value="pro">pro</option>
          <option value="premium">premium</option>
          <option value="lifetime">lifetime</option>
        </select>
      </div>
      <div>
        <label>Дней</label>
        <input id="newDays" type="number" value="30" min="1">
      </div>
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
  const opts = {
    method,
    headers: { "X-Admin-Token": TOKEN },
  };
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
    msg("OK", true);
    loadUsers();
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
  } catch (e) {
    msg(e.message, false);
  }
}

function renderUsers(users) {
  if (!users.length) {
    document.getElementById("usersTable").innerHTML =
      '<p style="color:#8b949e;">Пусто</p>';
    return;
  }
  let html = '<table><thead><tr>' +
    '<th>Логин</th><th>Тариф</th><th>Истекает</th><th>Дней</th>' +
    '<th>Активен</th><th>HWID</th><th></th>' +
    '</tr></thead><tbody>';
  for (const u of users) {
    const cls = u.days_left <= 0 ? "expired"
              : (!u.is_active ? "inactive" : "");
    html += '<tr class="' + cls + '">' +
      '<td><b>' + esc(u.login) + '</b></td>' +
      '<td>' + esc(u.tariff) + '</td>' +
      '<td>' + (u.expires_at ? u.expires_at.slice(0,10) : '—') + '</td>' +
      '<td>' + u.days_left + '</td>' +
      '<td>' + (u.is_active ? '✅' : '❌') + '</td>' +
      '<td>' + (u.hwid ? esc(u.hwid.slice(0,10)) + '…' : '—') + '</td>' +
      '<td><div class="row-actions">' +
        '<button class="small" onclick="extendUser(\\'' + esc(u.login) + '\\')">+30д</button>' +
        '<button class="small" onclick="resetHwid(\\'' + esc(u.login) + '\\')">HWID</button>' +
        '<button class="small" onclick="toggleActive(\\'' + esc(u.login) + '\\',' + (u.is_active?1:0) + ')">' + (u.is_active?'Стоп':'Вкл') + '</button>' +
        '<button class="small" onclick="changePwd(\\'' + esc(u.login) + '\\')">Пароль</button>' +
        '<button class="small danger" onclick="deleteUser(\\'' + esc(u.login) + '\\')">✕</button>' +
      '</div></td>' +
    '</tr>';
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
    msg(r.message, r.success);
    loadUsers();
  } catch (e) { msg(e.message, false); }
}

async function extendUser(login) {
  try {
    const r = await api("PATCH", "/admin/users/" + encodeURIComponent(login),
                        { days_left: 30 });
    msg(r.changes.join(", "), true);
    loadUsers();
  } catch (e) { msg(e.message, false); }
}

async function resetHwid(login) {
  if (!confirm("Сбросить HWID для " + login + "?")) return;
  try {
    const r = await api("PATCH", "/admin/users/" + encodeURIComponent(login),
                        { reset_hwid: true });
    msg(r.changes.join(", "), true);
    loadUsers();
  } catch (e) { msg(e.message, false); }
}

async function toggleActive(login, isActive) {
  try {
    const r = await api("PATCH", "/admin/users/" + encodeURIComponent(login),
                        { is_active: !isActive });
    msg(r.changes.join(", "), true);
    loadUsers();
  } catch (e) { msg(e.message, false); }
}

async function changePwd(login) {
  const pwd = prompt("Новый пароль для " + login + ":");
  if (!pwd) return;
  try {
    const r = await api("PATCH", "/admin/users/" + encodeURIComponent(login),
                        { new_password: pwd });
    msg(r.changes.join(", "), true);
  } catch (e) { msg(e.message, false); }
}

// Автологин, если токен сохранён
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
    """HTML-админка. Токен вводится на странице, хранится в localStorage."""
    return HTMLResponse(ADMIN_HTML)