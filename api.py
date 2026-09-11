# api.py
import os

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from typing import List, Dict, Optional
from core.aggregator import OddsAggregator
import json
import asyncio
import logging
from core.auth import authenticate_user, create_user, init_db, get_db
from pydantic import BaseModel

logger = logging.getLogger(__name__)

app = FastAPI(title="Odds Aggregator API", version="1.0")

aggregator: Optional[OddsAggregator] = None
websocket_clients = set()

def set_aggregator(agg: OddsAggregator):
    global aggregator
    aggregator = agg

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    websocket_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        websocket_clients.remove(websocket)

async def broadcast_message(message: dict):
    if not websocket_clients:
        return
    data = json.dumps(message)
    # Не логируем каждую отправку – только если нужно отладить, включить DEBUG
    # logger.debug(f"📤 Отправка WebSocket: {message.get('type')}")
    for client in list(websocket_clients):
        try:
            await client.send_text(data)
        except:
            websocket_clients.remove(client)

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

# ----- АВТОРИЗАЦИЯ -----
init_db()

class LoginRequest(BaseModel):
    login: str
    password: str
    hwid: Optional[str] = None

class RegisterRequest(BaseModel):
    login: str
    password: str
    tariff: str = "trial"

@app.post("/auth/login")
async def login(req: LoginRequest):
    user = authenticate_user(req.login, req.password)
    if not user:
        return {"success": False, "message": "Неверный логин или пароль"}

    logger.info(f"Логин: {req.login}, HWID получен: {req.hwid}, HWID в БД: {user['hwid']}")

    if req.hwid:
        if user["hwid"] is None:
            with get_db() as conn:
                conn.execute("UPDATE users SET hwid = ? WHERE id = ?", (req.hwid, user["id"]))
                conn.commit()
            logger.info(f"✅ HWID привязан к {req.login}: {req.hwid}")
            user["hwid"] = req.hwid
        elif user["hwid"] != req.hwid:
            logger.warning(f"❌ Попытка входа с другого устройства: {req.login}, HWID={req.hwid}, ожидался={user['hwid']}")
            return {"success": False, "message": "Этот аккаунт привязан к другому устройству"}

    return {
        "success": True,
        "user": {
            "id": user["id"],
            "login": user["login"],
            "tariff": user["tariff"],
            "expires_at": user["expires_at"],
        }
    }

@app.post("/auth/register")
async def register(req: RegisterRequest):
    if create_user(req.login, req.password, req.tariff):
        return {"success": True, "message": "Пользователь создан"}
    return {"success": False, "message": "Логин уже занят"}

UPDATE_INFO_PATH = "update_info.json"