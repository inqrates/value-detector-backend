# parsers/onewin_api.py
"""
OneWin (1win) — гибридный парсер: Socket.IO WS + Playwright для кук.

Как работает:
  1. Playwright открывает лайв-страницу one-vv7078.com/betting/live:
     - перехватывает HTTP answers matches/get-many → ID матчей + имена команд;
     - снимает cookies → cookies/onewin.json (для WS-авторизации).
  2. WebSocket (Socket.IO / Engine.IO v4) на api-gateway.top-parser.com:
     - handshake: ждём "0{...}" → шлём "40" → ждём "40{...sid...}";
     - heartbeat: сами шлём "2" каждые pingInterval, отвечаем "3" на "2";
     - подписка:  42["subscribe",{"messageType":"subscribe-match-info",
                  "data":{"matchIds":[...]}}] + subscribe-match-odds;
     - данные:    42["u",{"messageType":"match-info"|"match-odds","data":{...}}].

Отличия от других парсеров:
  - 1win отдаёт куки только через реальный браузер (антибот), поэтому
    Playwright нужен при старте и при протухании кук (серия WS-ошибок
    или длительная тишина → переинициализация).
  - Зеркала/шлюзы меняются → вынесены в константы с .env override:
      ONEWIN_SITE_URL, ONEWIN_ORIGIN, ONEWIN_WS_URL

Кэши названы так же, как в остальных парсерах — global_cache_cleaner
и health_monitor работают с onewin без изменений:
  _matches_cache, _first_seen, _last_sent_time, _last_update_time,
  _last_successful_response_at
"""
import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import websockets

from core.models import Match
from core.sport_map import (
    format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, FOOTBALL,
)

logger = logging.getLogger(__name__)

COOKIES_DIR = Path("cookies")
COOKIES_PATH = COOKIES_DIR / "onewin.json"

# sportId OneWin → универсальный sport_key
SPORT_IDS = {
    24: TABLE_TENNIS,
    27: VOLLEYBALL,
    23: BASKETBALL,
    18: FOOTBALL,
}


class OneWinApiParser:
    bk_id = "onewin"

    # ── Домены (зеркала меняются — переопределяются через .env) ──
    SITE_URL = os.getenv("ONEWIN_SITE_URL", "https://one-vv7078.com/betting/live")
    ORIGIN = os.getenv("ONEWIN_ORIGIN", "https://one-vv7078.com")
    WS_URL = os.getenv(
        "ONEWIN_WS_URL",
        "wss://api-gateway.top-parser.com/push-server-v2/"
        "?Language=ru-RU&externalPartnerId=44ba10e5-7df2-47ab-a44d-dc93803c7a6e"
        "&EIO=4&transport=websocket",
    )

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    # ── Тайминги ──
    SEND_INTERVAL = 0.7            # период отправки матчей в детектор
    WS_RECONNECT_DELAY = 2.0
    WS_RECONNECT_DELAY_MAX = 30.0
    WATCHDOG_INTERVAL = 15.0
    DATA_SILENCE_REINIT = 45.0     # тишина 45с → свежие куки через Playwright
    COOKIES_MAX_AGE = 900.0        # куки из файла старше 15 мин — протухли
    PW_REFRESH_INTERVAL = 300.0    # reload страницы раз в 5 мин (свежие имена)

    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        self.detector = detector
        self.aggregator = aggregator
        self.enabled_sports = set(enabled_sports or [TABLE_TENNIS])
        self.is_running = False

        # ── Кэши (имена ждут main.py, global_cache_cleaner, health_monitor) ──
        self._matches_cache: Dict[str, Match] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self._last_update_time: Dict[str, float] = {}
        self._last_sent_state: Dict[str, tuple] = {}
        self._match_names: Dict[str, Tuple[str, str]] = {}

        self._last_successful_response_at = 0.0

        # ── Playwright ──
        self._pw = None
        self._playwright_browser = None
        self._playwright_context = None
        self._pw_page = None
        self._pw_ready = False
        self._last_pw_refresh = 0.0

        # ── WS ──
        self._ws = None
        self._cookie_string = ""
        self._reconnects_error = 0
        self._msg_log_count = 0

        self._tasks: List[asyncio.Task] = []

    # ============================================================
    # Cookies
    # ============================================================
    def _load_cookies_from_file(self) -> bool:
        """Читает cookies/onewin.json (кладёт их туда Playwright)."""
        if not COOKIES_PATH.exists():
            return False
        try:
            age = time.time() - COOKIES_PATH.stat().st_mtime
            if age > self.COOKIES_MAX_AGE:
                return False
            data = json.loads(COOKIES_PATH.read_text(encoding="utf-8"))
            cookies = data.get("cookies", [])
            header = "; ".join(
                f"{c['name']}={c['value']}" for c in cookies
                if c.get("name") and c.get("value")
            )
            if not header:
                return False
            self._cookie_string = header
            logger.info(
                f"[{self.bk_id}] 🍪 Куки из файла ({len(cookies)} шт, "
                f"{age / 60:.1f} мин)"
            )
            return True
        except Exception:
            return False

    def _save_cookies_to_file(self, cookies: list):
        try:
            COOKIES_DIR.mkdir(exist_ok=True)
            COOKIES_PATH.write_text(
                json.dumps({"cookies": cookies}, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as e:
            logger.debug(f"[{self.bk_id}] save cookies: {e}")

    # ============================================================
    # Playwright: снапшот матчей + куки
    # ============================================================
    async def _init_playwright(self, reason: str = "старт"):
        """
        Запускает браузер, открывает лайв-страницу.
        Перехватывает matches/get-many (имена матчей) и снимает куки.
        """
        await self._close_playwright()
        logger.info(f"[{self.bk_id}] 🌐 Playwright запуск ({reason})...")
        try:
            self._pw = await __import__(
                "playwright.async_api", fromlist=["async_playwright"]
            ).async_playwright().start()
            self._playwright_browser = await self._pw.chromium.launch(headless=True)
            self._playwright_context = await self._playwright_browser.new_context(
                user_agent=self.USER_AGENT,
                viewport={"width": 1920, "height": 1080},
            )
            page = await self._playwright_context.new_page()
            self._pw_page = page

            async def handle_response(response):
                if "matches/get-many" not in response.url:
                    return
                if response.status != 200:
                    return
                try:
                    data = await response.json()
                    items = data.get("result", {}).get("items", [])
                    for item in items:
                        if item.get("service") != "LIVE":
                            continue
                        sport_id = item.get("sportId")
                        sport_key = SPORT_IDS.get(sport_id)
                        if not sport_key or sport_key not in self.enabled_sports:
                            continue
                        match_id = str(item.get("id"))
                        home = item.get("homeTeam", {}).get("name", "")
                        away = item.get("awayTeam", {}).get("name", "")
                        if home and away:
                            self._match_names[match_id] = (home, away)
                except Exception:
                    pass

            page.on("response", handle_response)

            await page.goto(
                self.SITE_URL, wait_until="domcontentloaded", timeout=20000
            )
            await asyncio.sleep(3)

            cookies = await self._playwright_context.cookies()
            if cookies:
                self._cookie_string = "; ".join(
                    f"{c['name']}={c['value']}" for c in cookies
                    if c.get("name") and c.get("value")
                )
                self._save_cookies_to_file(cookies)
                logger.info(
                    f"[{self.bk_id}] 🔑 Снято {len(cookies)} кук "
                    f"(сохранены в {COOKIES_PATH})"
                )

            self._pw_ready = True
            self._last_pw_refresh = time.time()
            logger.info(
                f"[{self.bk_id}] 📥 Playwright готов. Матчей найдено: "
                f"{len(self._match_names)}"
            )
        except Exception as e:
            logger.error(f"[{self.bk_id}] Ошибка Playwright: {e}")
            self._pw_ready = False

    async def _refresh_playwright_page(self):
        """Reload страницы — обновляет список матчей (новые появляются)."""
        if not self._pw_page or self._pw_page.is_closed():
            return False
        try:
            await self._pw_page.reload(
                wait_until="domcontentloaded", timeout=20000
            )
            self._last_pw_refresh = time.time()
            return True
        except Exception as e:
            logger.debug(f"[{self.bk_id}] pw refresh: {e}")
            return False

    async def _reconnect_ws_for_updated_matches(self):
        """Пересоздаёт WS, чтобы подписаться на ID из обновлённой страницы."""
        if self._ws is None:
            return False
        logger.info(
            f"[{self.bk_id}] 🔄 Список матчей обновлён → "
            "WS переподключается для новой подписки"
        )
        await self._ws.close()
        return True

    async def _close_playwright(self):
        for coro in (
            self._playwright_browser.close if self._playwright_browser else None,
            self._pw.stop if self._pw else None,
        ):
            if coro:
                try:
                    await coro()
                except Exception:
                    pass
        self._playwright_browser = None
        self._playwright_context = None
        self._pw_page = None
        self._pw = None
        self._pw_ready = False

    # ============================================================
    # Жизненный цикл
    # ============================================================
    async def start(self):
        # Куки из файла только если уже есть список матчей от прошлого запуска.
        # Список ID живёт в памяти процесса — при старте его нет,
        # поэтому Playwright обязателен на первом запуске.
        await self._init_playwright("старт")

    async def stop(self):
        self.is_running = False
        for t in self._tasks:
            if t and not t.done():
                t.cancel()
        self._tasks.clear()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        await self._close_playwright()
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")

    async def run(self):
        self.is_running = True
        await self.start()
        logger.info(f"[{self.bk_id}] 🚀 WS-парсер OneWin запущен")

        ws_task = asyncio.create_task(self._ws_loop())
        send_task = asyncio.create_task(self._send_loop())
        watchdog_task = asyncio.create_task(self._watchdog_loop())
        self._tasks = [ws_task, send_task, watchdog_task]

        try:
            await asyncio.gather(ws_task, send_task, watchdog_task)
        except asyncio.CancelledError:
            pass
        finally:
            for t in self._tasks:
                t.cancel()
            try:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            except Exception:
                pass

    # ============================================================
    # Watchdog: тишина → свежие куки; периодический refresh страницы
    # ============================================================
    async def _watchdog_loop(self):
        while self.is_running:
            try:
                await asyncio.sleep(self.WATCHDOG_INTERVAL)

                # Периодический refresh списка матчей
                if (self._pw_ready and self._pw_page
                        and not self._pw_page.is_closed()
                        and time.time() - self._last_pw_refresh
                        >= self.PW_REFRESH_INTERVAL):
                    refreshed = await self._refresh_playwright_page()
                    # WS получает обновления только для ID, переданных при
                    # subscribe. После reload страницы появились новые матчи,
                    # поэтому нужно создать сессию и подписку заново.
                    if refreshed:
                        await self._reconnect_ws_for_updated_matches()

                # Долгая тишина → переинициализация (протухли куки?)
                silence = time.time() - self._last_successful_response_at
                if silence > self.DATA_SILENCE_REINIT:
                    logger.warning(
                        f"[{self.bk_id}] ⏱️ Тишина {silence:.0f}с → "
                        f"переинициализация Playwright (свежие куки)"
                    )
                    await self._init_playwright("долгая тишина")
                    # Разрываем текущее WS — реконнект пойдёт с новыми куками
                    if self._ws is not None:
                        try:
                            await self._ws.close()
                        except Exception:
                            pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.bk_id}] watchdog: {e}")

    # ============================================================
    # WS-цикл с реконнектом
    # ============================================================
    async def _ws_loop(self):
        delay = self.WS_RECONNECT_DELAY
        while self.is_running:
            try:
                await self._ws_connect_and_listen()
                # Нормальное завершение → быстрый реконнект
                if not self.is_running:
                    break
                self._reconnects_error = 0
                delay = self.WS_RECONNECT_DELAY
            except asyncio.CancelledError:
                break
            except websockets.exceptions.ConnectionClosed as e:
                if not self.is_running:
                    break
                self._reconnects_error += 1
                delay = min(delay * 2, self.WS_RECONNECT_DELAY_MAX)
                logger.info(
                    f"[{self.bk_id}] WS закрыт ({e.code or '1005'}) → "
                    f"реконнект через {delay:.1f}с"
                )
                # Серия ошибок → свежие куки из Playwright
                if self._reconnects_error >= 5 and not self._pw_ready:
                    await self._init_playwright("серия WS-ошибок")
                    self._reconnects_error = 0
            except Exception as e:
                if not self.is_running:
                    break
                self._reconnects_error += 1
                delay = min(delay * 2, self.WS_RECONNECT_DELAY_MAX)
                logger.warning(
                    f"[{self.bk_id}] WS ошибка ({type(e).__name__}): {e} → "
                    f"реконнект через {delay:.1f}с"
                )

            if not self.is_running:
                break
            await asyncio.sleep(delay)

    async def _ws_connect_and_listen(self):
        headers = {
            "User-Agent": self.USER_AGENT,
            "Origin": self.ORIGIN,
            "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
        }
        if self._cookie_string:
            headers["Cookie"] = self._cookie_string

        logger.info(f"[{self.bk_id}] 🔌 WS подключаемся...")

        # ping_interval=None — Engine.IO v4 сам пингует пакетами "2"/"3".
        async with websockets.connect(
            self.WS_URL,
            additional_headers=headers,
            max_size=50 * 1024 * 1024,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
            open_timeout=15,
        ) as ws:
            self._ws = ws
            self._last_successful_response_at = time.time()
            logger.info(f"[{self.bk_id}] ✅ WS подключен (с куками)")

            # ── Socket.IO handshake (обязателен!) ──
            # 1. Ждём open-пакет "0{...}"
            open_pkt = await asyncio.wait_for(ws.recv(), timeout=10)
            open_str = self._to_text(open_pkt)
            if not open_str.startswith("0"):
                raise RuntimeError(f"Первый пакет не open: {open_str[:100]!r}")
            try:
                open_data = json.loads(open_str[1:])
                interval_ms = open_data.get("pingInterval", 25000)
                self._heartbeat_interval = max(5.0, interval_ms / 1000.0)
            except Exception:
                pass
            logger.debug(f"[{self.bk_id}] open: {open_str[:120]}")

            # 2. CONNECT к namespace "/" → "40"
            await ws.send("40")

            # 3. Ждём подтверждение "40{...sid...}"
            ack = self._to_text(await asyncio.wait_for(ws.recv(), timeout=10))
            if ack.startswith("44"):
                raise RuntimeError(f"Socket.IO connect_error: {ack[:200]}")
            if not ack.startswith("40"):
                logger.warning(
                    f"[{self.bk_id}] ⚠️ Ожидался '40...', получено: {ack[:100]!r}"
                )
            else:
                logger.info(f"[{self.bk_id}] 🤝 Socket.IO сессия установлена")

            # 4. Подписка на матчи + чтение потока
            #    (в Engine.IO v4 ping инициирует СЕРВЕР, клиент отвечает "3")
            if not await self._subscribe_all(ws):
                # Нет ID (куки из файла без Playwright-снапшота) —
                # ждём watchdog-переинициализацию и переподключаемся
                await asyncio.sleep(3)
                return
            try:
                async for message in ws:
                    if not self.is_running:
                        break
                    if await self._on_message(self._to_text(message)):
                        # Ping/pong подтверждает только транспорт. Для
                        # watchdog и health это не данные букмекерской линии.
                        self._last_successful_response_at = time.time()
            finally:
                self._ws = None

    @staticmethod
    def _to_text(message) -> str:
        if isinstance(message, (bytes, bytearray)):
            return bytes(message).decode("utf-8", errors="replace")
        return str(message)

    # ============================================================
    # Подписка
    # ============================================================
    async def _subscribe_all(self, ws) -> bool:
        ids = sorted(set(self._match_names.keys()) | set(self._matches_cache.keys()))
        ids = [i for i in ids if i.isdigit()]
        if not ids:
            logger.info(f"[{self.bk_id}] ⏳ Нет ID матчей — подписка отложена")
            return False
        for i in range(0, len(ids), 50):
            batch = [int(x) for x in ids[i:i + 50]]
            msg_info = (
                '42["subscribe",{"messageType":"subscribe-match-info",'
                f'"data":{{"matchIds":{batch}}}}}]'
            )
            msg_odds = (
                '42["subscribe",{"messageType":"subscribe-match-odds",'
                f'"data":{{"matchIds":{batch},"isBaseOddsGroups":true}}}}]'
            )
            await ws.send(msg_info)
            await asyncio.sleep(0.1)
            await ws.send(msg_odds)
        logger.info(f"[{self.bk_id}] ✅ Подписан на {len(ids)} матчей")
        return True

    # ============================================================
    # Разбор пакетов Socket.IO
    # ============================================================
    async def _on_message(self, msg: str):
        # Диагностика: первые сообщений показываем всегда
        if self._msg_log_count < 8:
            self._msg_log_count += 1
            logger.info(f"[{self.bk_id}] 📨 MSG#{self._msg_log_count}: {msg[:200]}")

        if msg.startswith("2"):            # серверный ping → pong
            try:
                await self._ws.send("3")
            except Exception:
                pass
            return False
        if msg.startswith("44"):           # connect_error
            logger.error(f"[{self.bk_id}] ❌ Socket.IO connect_error: {msg[:200]}")
            return False
        if not msg.startswith("42"):       # 0/40/3/41 — служебные
            return False

        try:
            data = json.loads(msg[2:])
        except json.JSONDecodeError:
            return False
        if not isinstance(data, list) or len(data) < 2:
            return False

        event = data[0]
        inner = data[1] if isinstance(data[1], dict) else {}
        payload = inner.get("data", {}) if isinstance(inner.get("data"), dict) else {}

        if event != "u" or not payload.get("matchId"):
            return False

        # ── Определение типа по содержимому ──
        # Сервер шлёт 42["u",{"data":{...}}] БЕЗ messageType.
        # match-info:   sportId, matchScore/periodsScore, score, etc.
        # match-odds:   oddsGroups, odds
        if "oddsGroups" in payload or "odds" in payload:
            return self._handle_match_odds(payload)
        elif "sportId" in payload:
            return self._handle_match_info(payload)
        return False

    # ============================================================
    # match-info
    # ============================================================
    def _handle_match_info(self, payload: dict):
        match_id = str(payload.get("matchId"))
        if match_id == "None":
            return False
        sport_id = payload.get("sportId")
        sport_key = SPORT_IDS.get(sport_id)
        if not sport_key or sport_key not in self.enabled_sports:
            return False

        status = payload.get("status", "") or ""
        match_score = payload.get("matchScore", {}) or {}
        periods = payload.get("periodsScore", []) or []

        # Общий счёт матча (табло): matchScore
        try:
            score1 = int(match_score.get("t1", 0) or 0)
            score2 = int(match_score.get("t2", 0) or 0)
        except (ValueError, TypeError):
            score1 = score2 = 0

        # Текущий период (партия/сет/четверть): последний в periodsScore
        sub1 = sub2 = 0
        if periods and isinstance(periods[-1], dict):
            try:
                sub1 = int(periods[-1].get("t1", 0) or 0)
                sub2 = int(periods[-1].get("t2", 0) or 0)
            except (ValueError, TypeError):
                pass

        phase_num = self._resolve_phase_num(sport_key, status, periods)

        names = self._match_names.get(match_id, ("TeamA", "TeamB"))
        cached = self._matches_cache.get(match_id)

        match = Match(
            bk_id=self.bk_id,
            match_id=match_id,
            player1=cached.player1 if cached else names[0],
            player2=cached.player2 if cached else names[1],
            score1=score1,
            score2=score2,
            sub_score1=sub1,
            sub_score2=sub2,
            tournament="1win_Live",
            odds1=cached.odds1 if cached else 0.0,
            odds2=cached.odds2 if cached else 0.0,
            timestamp=time.time(),
            raw_time=format_phase(sport_key, phase_num),
            sport=sport_key,
            match_url=f"{self.ORIGIN}/betting/live/event/{match_id}",
        )

        self._matches_cache[match_id] = match
        self._last_update_time[match_id] = time.time()
        if match_id not in self._first_seen:
            self._first_seen[match_id] = time.time()
        return True

    def _resolve_phase_num(self, sport_key: str, status: str, periods: list) -> int:
        """
        Фаза матча: приоритет — цифра из статуса ('2-й сет', 'К2', '2T'),
        fallback — число непустых периодов.
        """
        m = re.search(r"\d+", status or "")
        if m:
            n = int(m.group())
            if 1 <= n <= 10:
                return n
        done = 0
        for p in periods:
            if not isinstance(p, dict):
                continue
            try:
                if int(p.get("t1", 0) or 0) or int(p.get("t2", 0) or 0):
                    done += 1
            except (ValueError, TypeError):
                pass
        return done or 1

    # ============================================================
    # match-odds
    # ============================================================
    def _handle_match_odds(self, payload: dict):
        match_id = str(payload.get("matchId"))
        if match_id not in self._matches_cache:
            return False
        odds_groups = payload.get("oddsGroups", []) or []

        odds1 = odds2 = 0.0
        for group in odds_groups:
            if not group.get("isBase"):
                continue
            if group.get("renderType", "") not in ("cols-2", "cols-3"):
                continue
            for odd in group.get("oddsList", []) or []:
                if odd.get("status") != 1:
                    continue
                try:
                    cf = float(odd.get("cf", 0))
                except (ValueError, TypeError):
                    continue
                outcome = str(odd.get("outcome", ""))
                if outcome == "1" and cf > 1.0:
                    odds1 = cf
                elif outcome == "2" and cf > 1.0:
                    odds2 = cf

        if odds1 <= 0 and odds2 <= 0:
            return False

        match = self._matches_cache[match_id]
        if odds1 > 0:
            match.odds1 = odds1
        if odds2 > 0:
            match.odds2 = odds2
        match.timestamp = time.time()
        self._last_update_time[match_id] = time.time()
        return True

    # ============================================================
    # Отправка матчей (send_loop — как у Pari/Winline)
    # ============================================================
    async def _send_loop(self):
        while self.is_running:
            try:
                await asyncio.sleep(self.SEND_INTERVAL)
                await self._try_send_matches()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.bk_id}] send_loop: {e}", exc_info=True)
                await asyncio.sleep(2)

    async def _try_send_matches(self):
        now = time.time()
        sent = 0

        for match_id, match in list(self._matches_cache.items()):
            if match.sport not in self.enabled_sports:
                continue
            if not match.player1 or not match.player2:
                continue
            if match.player1 == "TeamA":     # имена ещё не пришли
                continue

            # Не отправляем без кэфов — они придут отдельным пакетом
            # (match-odds), тогда матч уйдёт в детектор.
            if match.odds1 == 0 and match.odds2 == 0:
                continue

            # Не чаще раза в секунду
            if now - self._last_sent_time.get(match_id, 0) < 1.0:
                continue

            # Дедупликация: не слать неизменённое состояние
            state = (
                match.score1, match.score2, match.sub_score1, match.sub_score2,
                match.odds1, match.odds2, match.raw_time,
            )
            if self._last_sent_state.get(match_id) == state:
                continue

            match.timestamp = now
            logger.info(
                f"[{self.bk_id}] 🟢 [{match.sport}] {match.player1} vs "
                f"{match.player2} | матч {match.score1}:{match.score2} | "
                f"{match.raw_time} {match.sub_score1}:{match.sub_score2} | "
                f"К: {match.odds1:.2f}/{match.odds2:.2f}"
            )

            try:
                if self.detector:
                    await self.detector.process(match)
                if self.aggregator:
                    self.aggregator.update(match)
            except Exception as e:
                logger.error(f"[{self.bk_id}] detector/aggregator: {e}")
                continue

            self._last_sent_state[match_id] = state
            self._last_sent_time[match_id] = now
            sent += 1

        if sent:
            logger.info(
                f"[{self.bk_id}] ✅ Отправлено: {sent} "
                f"(в кеше: {len(self._matches_cache)})"
            )
