# parsers/ligastavok_api.py
"""
LigaStavok — HTTP + WS парсер с auto-refresh cookies Qrator.

ОБНОВЛЕНО 2026-11-10 (LigaStavok мигрировал на новый хост + новые заголовки):
  - BASE:    https://lds-api.ligastavok.ru
  - WS_URL:  wss://lds-api.ligastavok.ru/ws?Country-Code=RUS&Locale=ru-RU
  - meta JSON-RPC теперь содержит locale=ru-RU
  - User-Agent Chrome/150.0.0.0
  - ID в subscribe шлём строками

ОБНОВЛЕНО 2026-11-10 (заголовки Qrator):
  - В HTTP и WS добавлены ОБЯЗАТЕЛЬНЫЕ заголовки:
      country-code, locale, x-application-name, x-req-id (динамический)

ОБНОВЛЕНО 2026-11-10 (прокси):
  - LIGASTAVOK_PROXY применяется ко всем трём каналам:
    HTTP (curl_cffi), WS (websockets + python-socks), Playwright.

ОБНОВЛЕНО 2026-11-10 (real-time eventAdded):
  - Обработка /notifications/v3/eventAdded — новый матч создаётся
    в кеше сразу при появлении, без ожидания HTTP-снапшота.
  - Динамическая подписка на /notifications/v3/eventUpdated для
    новых ID через отдельную задачу _subscribe_loop (≤1 сек задержки).
  - _subscribed_ids — защита от повторной подписки.
  - SNAPSHOT_INTERVAL = 120с (fallback на случай потери eventAdded).

ОБНОВЛЕНО 2026-11-10 (Qrator 403 — защита):
  - _load_cookies игнорирует файл старше 10 минут: протухший qrator_jsr
    вызывает 403 у Qrator, тогда как БЕЗ куки Qrator пускает по заголовкам.
  - _rollback_refresh_on_fail: если refresh Playwright провалился, откатываем
    _last_refresh_at, чтобы следующая 403 запустила новую попытку через
    ~60 сек, а не через 10 минут. После 3 фейлов — cooldown 10 мин (защита
    от спама Playwright).
  - _fetch_snapshot: gameId = [наши виды спорта] — вместо 120 событий
    (3 страницы) сервер отдаёт ~46 (1-2 страницы). Снижает HTTP-нагрузку.

ОБНОВЛЕНО 2026-11-10 (WS heartbeat против code=1006):
  - Браузер повторяет subscribe на /notifications/v2/eventUpdated
    с пустым ids каждые 15-30 сек — это application-level heartbeat.
    Без него Qrator/WS-сервер рвёт TCP по idle через ~30-60 сек
    (code=1006 'no close frame received or sent').
  - Добавлена задача _ws_heartbeat_loop: шлёт v2/eventUpdated раз в 15 сек.
  - WS ping отключён (ping_interval=None, ping_timeout=None) — сервер
    не отвечает на WS ping, из-за чего были ложные разрывы.
  - _ws_last_payload_at — для диагностики: сколько прошло с последнего
    реального payload до разрыва соединения.

Требует: pip install "python-socks[asyncio]"
"""
import asyncio
import json
import logging
import os
import re
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import websockets
from curl_cffi.requests import AsyncSession
import ssl
try:
    import certifi
    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    _SSL_CTX = ssl.create_default_context()

from core.models import Match
from core.sport_map import (
    SPORT_MAP, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)

logger = logging.getLogger(__name__)

COOKIES_PATH = Path("cookies/ligastavok.json")

SEO_TO_SPORT = {
    "nastolnyi-tennis": TABLE_TENNIS,
    "voleibol":         VOLLEYBALL,
    "basketbol":        BASKETBALL,
    "kiberbasketbol":   CYBER_BASKETBALL,
}


_TRANSLIT = {
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z',
    'и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r',
    'с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'shch',
    'ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',
}


def _slug(text: str) -> str:
    if not text:
        return "x"
    text = text.lower().strip()
    text = "".join(_TRANSLIT.get(ch, ch) for ch in text)
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class LigaStavokApiParser:
    bk_id = "ligastavok"

    BASE = "https://lds-api.ligastavok.ru"
    EVENTS_URL = f"{BASE}/rest/events/v8/eventsList"
    WS_URL = "wss://lds-api.ligastavok.ru/ws?Country-Code=RUS&Locale=ru-RU"

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ru-RU,ru;q=0.9",
        "content-type": "application/json",
        "origin": "https://www.ligastavok.ru",
        "referer": "https://www.ligastavok.ru/",
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "country-code": "RUS",
        "locale": "ru-RU",
        "x-application-name": "mobile",
    }

    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    )

    IMPERSONATE = "chrome150"
    LIMIT = 40
    # ── ФИКС: real-time eventAdded ──
    # Снапшот — только fallback. Всё real-time идёт через eventAdded
    # (новые матчи, ≤1-2 сек). Вернули 120 сек (было 60), чтобы не
    # триггерить Qrator: 3 HTTP-запроса/мин вместо 6.
    SNAPSHOT_INTERVAL = 120.0
    WS_RECONNECT_DELAY = 3.0

    REFRESH_MIN_INTERVAL = 600.0
    REFRESH_AFTER_403 = 1

    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        self.detector = detector
        self.aggregator = aggregator
        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]
        self.is_running = False

        self._matches_cache: Dict[str, dict] = {}
        self._finished: set = set()
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self._last_update_time: Dict[str, float] = {}

        self._sport_ids: Dict[str, str] = {}
        for sk in self.enabled_sports:
            cfg = SPORT_MAP.get(self.bk_id, {}).get(sk, {})
            for sid in cfg.get("ids", []):
                self._sport_ids[str(sid)] = sk
        logger.info(f"[{self.bk_id}] sport_ids: {self._sport_ids}")

        self._session: Optional[AsyncSession] = None
        self._ws = None
        self._ws_connected = False
        # ── ФИКС: real-time eventAdded ──
        self._subscribed_ids: set = set()
        self._pending_subscribe: set = set()
        self._last_snapshot_at: float = 0.0
        self._cookie_header: str = ""
        # ── ФИКС 2026-11-10: application-level heartbeat ──
        # Браузер повторяет v2/eventUpdated с пустым ids каждые 15-30 сек.
        # Без этого Qrator/WS-сервер рвёт TCP по idle (code=1006).
        self._ws_last_payload_at: float = 0.0

        self._refresh_lock = asyncio.Lock()
        self._last_refresh_at: float = 0.0
        self._consecutive_403: int = 0
        self._refresh_event = asyncio.Event()
        self._last_successful_response_at: float = 0.0
        self._qrator_cooldown_until: float = 0.0

        self.qrator_fails: int = 0
        self.last_qrator_success_at: float = 0.0

        self._proxy_url: str = os.getenv("LIGASTAVOK_PROXY", "").strip()
        self._proxy_cfg: Optional[dict] = self._build_playwright_proxy(
            self._proxy_url
        )
        if self._proxy_url:
            _safe = (
                self._proxy_url.split("@")[-1]
                if "@" in self._proxy_url
                else self._proxy_url
            )
            logger.info(
                f"[{self.bk_id}] 🌐 Прокси настроен для HTTP + WS + Playwright: "
                f"{_safe}"
            )
        else:
            logger.warning(
                f"[{self.bk_id}] ⚠️ LIGASTAVOK_PROXY не задан — "
                f"WS и Qrator пойдут с IP сервера (часто = 403 на VDS)"
            )

    # ============================================================
    # Прокси-хелпер
    # ============================================================
    @staticmethod
    def _build_playwright_proxy(proxy_url: str) -> Optional[dict]:
        if not proxy_url:
            return None
        try:
            p = urlparse(proxy_url)
            if not p.hostname or not p.port:
                return None
            return {
                "server": f"{p.scheme}://{p.hostname}:{p.port}",
                "username": p.username or "",
                "password": p.password or "",
            }
        except Exception as e:
            logger.warning(f"[ligastavok] Не разобрал прокси URL: {e}")
            return None

    # ============================================================
    # Заголовки
    # ============================================================
    def _make_headers(self, with_cookies: bool = True) -> dict:
        h = dict(self.HEADERS)
        h["user-agent"] = self.USER_AGENT
        h["x-req-id"] = str(uuid.uuid4())
        if with_cookies and self._cookie_header:
            h["cookie"] = self._cookie_header
        return h

    # ============================================================
    # Cookies
    # ============================================================
    def _load_cookies(self) -> list:
        if not COOKIES_PATH.exists():
            return []
        try:
            # ── ФИКС 2026-11-10: протухшая куки хуже, чем её отсутствие ──
            # Qrator явно распознаёт невалидный qrator_jsr и блокирует (403),
            # тогда как без куки пускает по заголовкам (200). qrator_jsr живёт
            # ~5 минут, так что файл старше 10 минут — точно мусор.
            age = time.time() - COOKIES_PATH.stat().st_mtime
            if age > 600.0:   # 10 минут
                logger.warning(
                    f"[{self.bk_id}] 🍪 Файл cookies старше "
                    f"{age/60:.1f} мин — игнорируем (протухший "
                    f"qrator_jsr вызывает 403 у Qrator)"
                )
                return []
            return json.loads(
                COOKIES_PATH.read_text(encoding="utf-8")
            ).get("cookies", [])
        except Exception:
            return []

    def _build_cookie_header(self, cookies: list) -> str:
        return "; ".join(
            f"{c['name']}={c['value']}" for c in cookies
            if c.get("name") and c.get("value")
        )

    async def _apply_cookies_to_session(self, cookies: list):
        if self._session is None:
            return
        try:
            self._session.cookies.clear()
        except Exception:
            try:
                await self._session.close()
            except Exception:
                pass
            kwargs = dict(
                impersonate=self.IMPERSONATE,
                timeout=30,
                headers=self.HEADERS,
            )
            if self._proxy_url:
                kwargs["proxies"] = {
                    "http": self._proxy_url, "https": self._proxy_url,
                }
            self._session = AsyncSession(**kwargs)
        for c in cookies:
            try:
                self._session.cookies.set(
                    c["name"], c["value"],
                    domain=c.get("domain"), path=c.get("path", "/"),
                )
            except Exception:
                pass
        self._cookie_header = self._build_cookie_header(cookies)

    async def _ensure_session(self):
        if self._session is not None:
            return

        kwargs = dict(
            impersonate=self.IMPERSONATE,
            timeout=30,
            headers=self.HEADERS,
        )
        if self._proxy_url:
            kwargs["proxies"] = {
                "http": self._proxy_url, "https": self._proxy_url,
            }
            _safe = (
                self._proxy_url.split("@")[-1]
                if "@" in self._proxy_url
                else self._proxy_url
            )
            logger.info(f"[{self.bk_id}] 🌐 HTTP через прокси: {_safe}")

        self._session = AsyncSession(**kwargs)
        cookies = self._load_cookies()
        await self._apply_cookies_to_session(cookies)
        logger.info(f"[{self.bk_id}] 🍪 Загружено {len(cookies)} cookies")

    # ============================================================
    # Refresh cookies через invisible_playwright
    # ============================================================
    async def _refresh_cookies_via_playwright(self, reason: str = ""):
        async with self._refresh_lock:
            now = time.time()
            if now - self._last_refresh_at < self.REFRESH_MIN_INTERVAL:
                return False

            # ── ФИКС 2026-11-10: запоминаем ДО сдвига, чтобы при провале
            # откатить _last_refresh_at. ──
            prev_last_refresh = self._last_refresh_at
            self._last_refresh_at = now
            logger.warning(f"[{self.bk_id}] 🔄 Refresh cookies {reason}")

            try:
                from invisible_playwright.async_api import InvisiblePlaywright
            except ImportError:
                logger.error(
                    f"[{self.bk_id}] ❌ invisible_playwright не установлен."
                )
                self._rollback_refresh_on_fail(prev_last_refresh)
                return False

            try:
                async with InvisiblePlaywright(headless=True) as browser:
                    if self._proxy_cfg:
                        logger.info(
                            f"[{self.bk_id}] 🌐 Playwright через прокси: "
                            f"{self._proxy_cfg['server']}"
                        )
                        try:
                            page = await browser.new_page(
                                proxy=self._proxy_cfg
                            )
                        except TypeError:
                            context = await browser.new_context(
                                proxy=self._proxy_cfg
                            )
                            page = await context.new_page()
                    else:
                        page = await browser.new_page()

                    try:
                        await page.set_viewport_size(
                            {"width": 1920, "height": 1080}
                        )
                    except Exception:
                        pass

                    logger.info(f"[{self.bk_id}] Фаза 1: Qrator")

                    try:
                        await page.goto(
                            "https://www.ligastavok.ru/live",
                            wait_until="domcontentloaded",
                            timeout=60000,
                        )
                    except Exception as e:
                        logger.warning(f"[{self.bk_id}] goto: {e}")
                        self._rollback_refresh_on_fail(prev_last_refresh)
                        return False

                    MAX_QRATOR_WAIT = 60.0
                    start = time.monotonic()
                    last_log = 0.0
                    passed = False

                    while time.monotonic() - start < MAX_QRATOR_WAIT:
                        await asyncio.sleep(1.5)
                        elapsed = time.monotonic() - start

                        try:
                            await page.mouse.move(
                                200 + int((elapsed * 137) % 1200),
                                150 + int((elapsed * 91) % 600),
                                steps=5,
                            )
                        except Exception:
                            pass
                        try:
                            await page.evaluate(
                                f"window.scrollTo(0, "
                                f"{int((elapsed * 150) % 800)})"
                            )
                        except Exception:
                            pass

                        try:
                            cur = await page.context.cookies()
                        except Exception:
                            cur = []
                        names = {
                            (c.get("name") or "").lower() for c in cur
                        }
                        has_qrator = "qrator_jsr" in names

                        try:
                            title = (await page.title()) or ""
                        except Exception:
                            title = ""
                        try:
                            body_len = len(
                                await page.locator("body").inner_text()
                            )
                        except Exception:
                            body_len = 0

                        tl = title.lower()
                        blocked = any(m in tl for m in (
                            "проверка", "qrator", "challenge",
                            "just a moment", "ddos-guard",
                            "безопасности",
                        ))

                        if has_qrator and not blocked and body_len > 500:
                            passed = True
                            logger.info(
                                f"[{self.bk_id}] ✅ Qrator пройден "
                                f"за {elapsed:.1f}с "
                                f"(cookies={len(cur)}, body={body_len})"
                            )
                            break

                        if elapsed - last_log >= 5.0:
                            last_log = elapsed
                            logger.info(
                                f"[{self.bk_id}] Qrator: {elapsed:.0f}/"
                                f"{MAX_QRATOR_WAIT:.0f}с "
                                f"cookies={len(cur)} "
                                f"qrator_jsr={has_qrator} "
                                f"title={title[:50]!r} body={body_len}"
                            )

                    if not passed:
                        self.qrator_fails += 1
                        logger.warning(
                            f"[{self.bk_id}] ❌ Qrator не пройден за "
                            f"{MAX_QRATOR_WAIT:.0f}с. "
                            f"qrator_fails={self.qrator_fails}"
                        )
                        self._rollback_refresh_on_fail(prev_last_refresh)
                        return False

                    logger.info(f"[{self.bk_id}] Фаза 2: 1920×4000")
                    try:
                        await page.set_viewport_size(
                            {"width": 1920, "height": 4000}
                        )
                    except Exception as e:
                        logger.debug(f"[{self.bk_id}] set_viewport: {e}")

                    await asyncio.sleep(2.0)

                    for y in [1000, 2000, 3000, 4000, 5000, 6000,
                              7000, 8000, 5000, 3000, 1000, 0]:
                        try:
                            await page.evaluate(
                                f"window.scrollTo(0, {y})"
                            )
                        except Exception:
                            pass
                        await asyncio.sleep(0.4)

                    await asyncio.sleep(1.5)

                    cookies = await page.context.cookies()
                    if not cookies:
                        logger.warning(
                            f"[{self.bk_id}] cookies пусты — не сохраняю"
                        )
                        self._rollback_refresh_on_fail(prev_last_refresh)
                        return False

                    names = {
                        (c.get("name") or "").lower() for c in cookies
                    }
                    if "qrator_jsr" not in names:
                        logger.warning(
                            f"[{self.bk_id}] qrator_jsr пропал после "
                            f"ресайза — не сохраняю"
                        )
                        self.qrator_fails += 1
                        self._rollback_refresh_on_fail(prev_last_refresh)
                        return False

                    COOKIES_PATH.parent.mkdir(exist_ok=True)
                    tmp = COOKIES_PATH.with_suffix(".tmp")
                    tmp.write_text(
                        json.dumps(
                            {"cookies": cookies},
                            ensure_ascii=False, indent=2,
                        ),
                        encoding="utf-8",
                    )
                    tmp.replace(COOKIES_PATH)

                    await self._apply_cookies_to_session(cookies)
                    logger.info(
                        f"[{self.bk_id}] ✅ Cookies обновлены "
                        f"({len(cookies)} шт.)"
                    )

                    self.qrator_fails = 0
                    self.last_qrator_success_at = time.time()
                    self._consecutive_403 = 0
                    self._last_snapshot_at = 0.0
                    self._refresh_event.set()
                    return True

            except Exception as e:
                logger.error(
                    f"[{self.bk_id}] Refresh cookies ошибка: {e}",
                    exc_info=True,
                )
                self._rollback_refresh_on_fail(prev_last_refresh)
                return False

    def _rollback_refresh_on_fail(self, prev_last_refresh: float):
        """
        При провале refresh откатываем _last_refresh_at к предыдущему
        значению, чтобы следующая 403 снова вызвала refresh. Но не
        бесконечно: после 3 фейлов подряд — оставляем свежий timestamp,
        уходим в cooldown 10 минут (защита от спама Playwright).
        """
        if self.qrator_fails >= 3:
            logger.warning(
                f"[{self.bk_id}] 🧊 {self.qrator_fails} фейлов refresh "
                f"подряд — cooldown 10 мин (спам Playwright не нужен)"
            )
        else:
            self._last_refresh_at = prev_last_refresh
            logger.info(
                f"[{self.bk_id}] 🔄 refresh откатан — "
                f"следующая 403 запустит новую попытку"
            )

    # ============================================================
    # Жизненный цикл
    # ============================================================
    async def start(self):
        await self._ensure_session()

    async def stop(self):
        self.is_running = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
        logger.info(f"[{self.bk_id}] 🛑 Остановка HTTP+WS парсера...")

    async def run(self):
        self.is_running = True
        await self.start()
        logger.info(
            f"[{self.bk_id}] 🚀 HTTP+WS парсер LigaStavok запущен"
        )

        try:
            await self._fetch_snapshot()
        except Exception as e:
            logger.error(f"[{self.bk_id}] начальный снапшот: {e}")

        # ── ФИКС: real-time eventAdded + heartbeat ──
        subscribe_task = asyncio.create_task(self._subscribe_loop())
        heartbeat_task = asyncio.create_task(self._ws_heartbeat_loop())
        ws_task = asyncio.create_task(self._ws_loop())

        try:
            while self.is_running:
                t0 = time.monotonic()
                try:
                    now = time.time()
                    if now - self._last_snapshot_at >= self.SNAPSHOT_INTERVAL:
                        await self._fetch_snapshot()
                    await self._try_send_matches()
                except Exception as e:
                    logger.error(
                        f"[{self.bk_id}] цикл: {e}", exc_info=True
                    )
                    await asyncio.sleep(3)
                    continue
                elapsed = time.monotonic() - t0
                await asyncio.sleep(max(0, 0.5 - elapsed))
        finally:
            self.is_running = False
            subscribe_task.cancel()
            heartbeat_task.cancel()
            ws_task.cancel()
            for t in (subscribe_task, heartbeat_task, ws_task):
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

    # ============================================================
    # HTTP-снапшот
    # ============================================================
    async def _fetch_snapshot(self):
        if time.time() < self._qrator_cooldown_until:
            self._last_snapshot_at = time.time()
            return
        all_items = []
        skip = 0
        page = 0
        got_403 = False

        # единый ts на весь цикл пагинации
        ts = int(time.time() * 1000)

        # ── ФИКС: фильтр по нашим видам спорта ──
        # Вместо 120 событий (3 страницы) — только наши ~46 (1-2 страницы).
        # Снижает HTTP-нагрузку на Qrator в 1.5-2 раза.
        our_game_ids: List[int] = []
        for sk in self.enabled_sports:
            cfg = SPORT_MAP.get(self.bk_id, {}).get(sk, {})
            for gid in cfg.get("ids", []):
                our_game_ids.append(gid)

        while self.is_running:
            body = {
                "gameId": our_game_ids,   # ── было [], теперь наши ID ──
                "limit": self.LIMIT,
                "ns": ["live"],
                "topEvents": False,
                "view": "priority",
                "widgetVideo": False,
                "ts": ts,
            }
            if skip > 0:
                body["skip"] = skip

            try:
                r = await self._session.post(
                    self.EVENTS_URL,
                    headers=self._make_headers(),
                    json=body,
                )
            except Exception as e:
                logger.warning(
                    f"[{self.bk_id}] snapshot HTTP error skip={skip}: {e}"
                )
                break

            if r.status_code == 403:
                got_403 = True
                self._consecutive_403 += 1
                logger.warning(
                    f"[{self.bk_id}] snapshot HTTP 403 skip={skip} "
                    f"(подряд: {self._consecutive_403})"
                )
                break

            if r.status_code in (502, 503):
                logger.debug(
                    f"[{self.bk_id}] snapshot HTTP {r.status_code} "
                    f"(Qrator)"
                )
                break

            if r.status_code != 200:
                logger.warning(
                    f"[{self.bk_id}] snapshot HTTP {r.status_code} "
                    f"skip={skip}"
                )
                break

            try:
                data = r.json()
            except Exception:
                break

            result = data.get("result") or {}
            items = result.get("data") or []
            if not items:
                break

            all_items.extend(items)
            for it in items:
                self._process_http_item(it)

            _total_api = result.get("total") or result.get(
                "totalCount") or 0
            if _total_api and skip + len(items) >= _total_api:
                break
            if len(items) < self.LIMIT:
                break

            skip += self.LIMIT
            page += 1
            if page > 20:
                break

            await asyncio.sleep(0.5)

        if not got_403:
            self._consecutive_403 = 0
            self._qrator_cooldown_until = 0.0

        if all_items:
            self._last_successful_response_at = time.time()

        self._last_snapshot_at = time.time()
        if all_items:
            logger.info(
                f"[{self.bk_id}] 📸 Снапшот: {len(all_items)} событий "
                f"({page + 1} стр.), в кеше: {len(self._matches_cache)}"
            )
            # ── ФИКС: real-time eventAdded ──
            for mid in list(self._matches_cache.keys()):
                if mid not in self._subscribed_ids:
                    self._pending_subscribe.add(mid)

        if got_403:
            if self._consecutive_403 >= 5:
                now = time.time()
                if now < self._qrator_cooldown_until:
                    pass
                else:
                    self._qrator_cooldown_until = now + 600.0
                    logger.warning(
                        f"[{self.bk_id}] 🧊 {self._consecutive_403} 403 "
                        f"подряд — cooldown 10 мин"
                    )
            elif self._consecutive_403 >= self.REFRESH_AFTER_403:
                asyncio.create_task(
                    self._refresh_cookies_via_playwright("(после 403)")
                )

    def _process_http_item(self, m: dict):
        if not isinstance(m, dict):
            return
        match_id = str(m.get("id", ""))
        if not match_id or match_id in self._finished:
            return

        sport_key = self._resolve_sport_key(m)
        if not sport_key:
            return

        if match_id not in self._matches_cache:
            self._matches_cache[match_id] = {
                "event": {}, "scores": {}, "outcomes": {}, "markets": {},
                "player1": "", "player2": "", "tournament": "",
                "sport": sport_key, "_last_sent": None,
            }
            self._first_seen[match_id] = time.time()

        cache = self._matches_cache[match_id]
        cache["sport"] = sport_key

        event = m.get("event")
        if isinstance(event, dict) and event:
            cache.setdefault("event", {})
            cache["event"].update(event)
            if event.get("team1"):
                cache["player1"] = event["team1"]
            if event.get("team2"):
                cache["player2"] = event["team2"]
            t = event.get("tournamentTitle") or event.get("topicTitle")
            if t:
                cache["tournament"] = t

        if m.get("scores"):
            new_scores = m["scores"] or {}
            old_scores = cache.get("scores") or {}

            new_total1 = str(
                (new_scores.get("total") or {}).get("ScoreTeam1", "0")
            )
            new_total2 = str(
                (new_scores.get("total") or {}).get("ScoreTeam2", "0")
            )
            old_total1 = str(
                (old_scores.get("total") or {}).get("ScoreTeam1", "0")
            )
            old_total2 = str(
                (old_scores.get("total") or {}).get("ScoreTeam2", "0")
            )

            new_cur = new_scores.get("current") or {}
            new_cur1 = str(new_cur.get("ScoreTeam1", "0"))
            new_cur2 = str(new_cur.get("ScoreTeam2", "0"))
            new_cur_is_zero = (
                new_cur1 in ("0", "") and new_cur2 in ("0", "")
            )

            if (new_total1 in ("0", "") and new_total2 in ("0", "")
                    and (old_total1 not in ("0", "")
                         or old_total2 not in ("0", ""))):
                if not new_cur_is_zero:
                    cache.setdefault("scores", {})["current"] = new_cur
                if new_scores.get("all"):
                    cache.setdefault("scores", {})["all"] = \
                        new_scores["all"]
            else:
                cache["scores"] = new_scores

        if m.get("markets"):
            cache["markets"] = m["markets"]
        if m.get("outcomes"):
            cache["outcomes"] = m["outcomes"]

        if not cache.get("tournament"):
            cache["tournament"] = "LigaStavok"

        if cache.get("event", {}).get("statusTranslated") == "Матч завершен":
            self._finished.add(match_id)

        self._last_update_time[match_id] = time.time()

    def _resolve_sport_key(self, m: dict) -> Optional[str]:
        seo = (m.get("categorySeoName") or "").strip()
        sport = SEO_TO_SPORT.get(seo)
        if sport and sport in self.enabled_sports:
            return sport
        gid = str(m.get("gameId", ""))
        sport = self._sport_ids.get(gid)
        if sport and sport in self.enabled_sports:
            return sport
        return None

    def _resolve_sport_key_from_event(self, event: dict) -> Optional[str]:
        """Определяет sport_key по полям event из WS-нотификации."""
        if not isinstance(event, dict):
            return None
        seo = (event.get("categorySeoName") or "").strip()
        sport = SEO_TO_SPORT.get(seo)
        if sport and sport in self.enabled_sports:
            return sport
        gid = str(event.get("gameId", ""))
        sport = self._sport_ids.get(gid)
        if sport and sport in self.enabled_sports:
            return sport
        sid = str(event.get("sportId", "") or "")
        sport = self._sport_ids.get(sid)
        if sport and sport in self.enabled_sports:
            return sport
        return None

    # ============================================================
    # WS: heartbeat (application-level)
    # ============================================================
    async def _ws_heartbeat_loop(self):
        """
        ── ФИКС 2026-11-10: application-level heartbeat ──
        Браузер повторяет subscribe на /notifications/v2/eventUpdated
        с пустым ids каждые 15-30 сек — это видно в DevTools. Без этого
        Qrator/WS-сервер рвёт TCP по idle через ~30-60 сек, что даёт
        code=1006 'no close frame received or sent'. Шлём каждые 15 сек.
        """
        while self.is_running:
            try:
                await asyncio.sleep(15.0)
                if self._ws is None or not self._ws_connected:
                    continue
                await self._send_subscribe(
                    self._ws, 777, "/notifications/v2/eventUpdated", []
                )
                logger.debug(f"[{self.bk_id}] 💓 WS heartbeat")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.bk_id}] heartbeat: {e}")

    # ============================================================
    # WS: динамическая подписка на новые матчи
    # ============================================================
    async def _subscribe_loop(self):
        """
        ── ФИКС: real-time eventAdded ──
        Каждую секунду проверяет очередь _pending_subscribe и подписывает
        новые ID на /notifications/v3/eventUpdated. Задержка появления
        нового матча в WS ≈ 1–2 сек вместо 120 сек (HTTP-snapshot).
        """
        while self.is_running:
            try:
                await asyncio.sleep(1.0)
                if not self._pending_subscribe:
                    continue
                if self._ws is None or not self._ws_connected:
                    continue

                pending = list(self._pending_subscribe)
                self._pending_subscribe.clear()
                new_ids = [
                    i for i in pending
                    if i not in self._subscribed_ids
                ]
                if not new_ids:
                    continue

                sent_ok = True
                for chunk_start in range(0, len(new_ids), 100):
                    chunk = new_ids[chunk_start:chunk_start + 100]
                    try:
                        await self._send_subscribe(
                            self._ws, 2000 + chunk_start // 100,
                            "/notifications/v3/eventUpdated", chunk,
                        )
                    except Exception as e:
                        logger.debug(
                            f"[{self.bk_id}] subscribe батч: {e}"
                        )
                        self._pending_subscribe |= set(chunk)
                        sent_ok = False
                        break

                if sent_ok:
                    self._subscribed_ids.update(new_ids)
                    logger.info(
                        f"[{self.bk_id}] 📤 Подписка (динамич.): "
                        f"+{len(new_ids)} "
                        f"(всего подписано: {len(self._subscribed_ids)})"
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"[{self.bk_id}] subscribe loop: {e}", exc_info=True
                )
                await asyncio.sleep(1)

    # ============================================================
    # WS: основной цикл
    # ============================================================
    async def _ws_loop(self):
        while self.is_running:
            try:
                self._refresh_event.clear()
                await self._ws_connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(
                    f"[{self.bk_id}] WS ошибка: "
                    f"{type(e).__name__}: {e}"
                )
                asyncio.create_task(
                    self._refresh_cookies_via_playwright("(WS fail)")
                )
            if not self.is_running:
                break
            if self._refresh_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._refresh_event.wait(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    pass
            await asyncio.sleep(self.WS_RECONNECT_DELAY)

    async def _ws_connect_and_listen(self):
        headers = [
            ("Origin", "https://www.ligastavok.ru"),
            ("User-Agent", self.USER_AGENT),
            ("country-code", "RUS"),
            ("locale", "ru-RU"),
            ("x-application-name", "mobile"),
            ("x-req-id", str(uuid.uuid4())),
        ]
        if self._cookie_header:
            headers.append(("Cookie", self._cookie_header))

        logger.info(f"[{self.bk_id}] 🔌 WS подключаемся...")

        connect_kwargs = dict(
            additional_headers=headers,
            max_size=10 * 1024 * 1024,
            # ── ФИКС 2026-11-10: отключаем WS ping ──
            # Сервер не отвечает на WS ping, наш ping_timeout=20 давал
            # ложные разрывы (code=1006). Используем application-level
            # heartbeat через v2/eventUpdated (см. _ws_heartbeat_loop).
            ping_interval=None,
            ping_timeout=None,
            ssl=_SSL_CTX,
        )
        if self._proxy_url:
            connect_kwargs["proxy"] = self._proxy_url
            _safe = (
                self._proxy_url.split("@")[-1]
                if "@" in self._proxy_url
                else self._proxy_url
            )
            logger.info(f"[{self.bk_id}] 🌐 WS через прокси: {_safe}")

        async with websockets.connect(self.WS_URL, **connect_kwargs) as ws:
            self._ws = ws
            self._ws_connected = True
            self._ws_last_payload_at = time.time()
            logger.info(f"[{self.bk_id}] ✅ WS подключён")

            # при реконнекте — сбрасываем set подписок и подписываемся заново
            self._subscribed_ids.clear()
            await self._subscribe_to_cached_ids(ws)

            await self._send_subscribe(
                ws, 2, "/notifications/v3/eventUpdated", []
            )
            await self._send_subscribe(
                ws, 3, "/notifications/v2/eventUpdated", []
            )
            await self._send_subscribe(
                ws, 4, "/notifications/v3/eventAdded", None
            )
            await self._send_subscribe(
                ws, 5, "/notifications/v3/eventRemoved", None
            )
            await self._send_subscribe(
                ws, 6, "/notifications/v3/countersUpdated", None
            )

            async for frame in ws:
                if not self.is_running:
                    break
                if self._refresh_event.is_set():
                    break
                try:
                    self._process_ws_frame(frame)
                except Exception as e:
                    logger.debug(f"[{self.bk_id}] ws frame: {e}")

        # ── ФИКС 2026-11-10: диагностика закрытия ──
        silence = time.time() - self._ws_last_payload_at
        logger.info(
            f"[{self.bk_id}] WS закрыт "
            f"(последний payload: {silence:.1f}с назад)"
        )
        self._ws = None
        self._ws_connected = False

    async def _send_subscribe(self, ws, id_: int, method: str, ids):
        params = {"method": method}
        if ids is not None:
            params["args"] = {"ids": ids}
        msg = {
            "id": id_,
            "jsonrpc": "2.0",
            "meta": {"applicationName": "mobile", "locale": "ru-RU"},
            "method": "subscribe",
            "params": params,
        }
        await ws.send(json.dumps(msg))

    async def _subscribe_to_cached_ids(self, ws):
        current_ids = {
            str(k) for k in self._matches_cache.keys() if k.isdigit()
        }
        new_ids = current_ids - self._subscribed_ids
        if not new_ids:
            return
        ids = list(new_ids)
        for chunk_start in range(0, len(ids), 100):
            chunk = ids[chunk_start:chunk_start + 100]
            await self._send_subscribe(
                ws, 1000 + chunk_start // 100,
                "/notifications/v3/eventUpdated", chunk,
            )
        self._subscribed_ids.update(new_ids)
        logger.info(
            f"[{self.bk_id}] 📤 Подписка: +{len(new_ids)} "
            f"(всего подписано: {len(self._subscribed_ids)})"
        )

    def _process_ws_frame(self, frame):
        try:
            payload_str = frame if isinstance(frame, str) else (
                frame.payload if hasattr(frame, "payload") else str(frame)
            )
            data = json.loads(payload_str)
        except json.JSONDecodeError:
            return
        if not isinstance(data, dict):
            return
        if data.get("id") is not None:
            return
        result = data.get("result")
        if not isinstance(result, dict):
            return
        payload = result.get("payload")
        if not isinstance(payload, list):
            return

        # ── ФИКС 2026-11-10: обновляем timestamp активности WS ──
        self._ws_last_payload_at = time.time()
        self._last_successful_response_at = time.time()

        for item in payload:
            if not isinstance(item, dict):
                continue
            match_id = str(item.get("id", ""))
            if not match_id or match_id in self._finished:
                continue

            ws_data = item.get("data") or {}
            if not isinstance(ws_data, dict):
                ws_data = {}

            # ── ФИКС: real-time eventAdded ──
            if match_id not in self._matches_cache:
                event = ws_data.get("event") or {}
                if not isinstance(event, dict):
                    event = {}
                p1 = event.get("team1") or ""
                p2 = event.get("team2") or ""
                if not p1 or not p2:
                    continue

                sport_key = self._resolve_sport_key_from_event(event)
                if not sport_key:
                    continue

                self._matches_cache[match_id] = {
                    "event": {}, "scores": {}, "outcomes": {},
                    "markets": {},
                    "player1": p1, "player2": p2,
                    "tournament": event.get("tournamentTitle", "")
                                  or event.get("topicTitle", "")
                                  or "LigaStavok",
                    "sport": sport_key, "_last_sent": None,
                }
                self._first_seen[match_id] = time.time()
                self._pending_subscribe.add(match_id)
                logger.info(
                    f"[{self.bk_id}] 🆕 Новый матч из WS (eventAdded): "
                    f"{p1} vs {p2} [{sport_key}] → подписка ~1с"
                )

            cache = self._matches_cache[match_id]
            sport_key = cache.get("sport", TABLE_TENNIS)
            self._apply_ws_data(cache, ws_data, sport_key)

            if cache.get("event", {}).get(
                    "statusTranslated") == "Матч завершен":
                self._finished.add(match_id)
                logger.info(
                    f"[{self.bk_id}] 🏁 Матч {match_id} завершён"
                )

    def _apply_ws_data(self, cache: dict, ws_data: dict, sport_key: str):
        headers = ws_data.get("headers") or []
        if isinstance(headers, list):
            for h in headers:
                if not isinstance(h, dict):
                    continue
                path = h.get("path", "")
                val = h.get("value")
                if val is None:
                    continue

                scores = cache.setdefault("scores", {})
                if not isinstance(scores, dict):
                    scores = {}
                    cache["scores"] = scores

                if path == "/scores/all":
                    if isinstance(val, list) and val:
                        scores["all"] = val
                        self._recompute_scores_from_all(
                            cache, val, sport_key
                        )
                    continue

                if path == "/scores/total/ScoreTeam1":
                    t = scores.get("total")
                    if not isinstance(t, dict):
                        t = {}
                        scores["total"] = t
                    old_val = t.get("ScoreTeam1", "0")
                    if str(val) != "0" or str(old_val) in (
                            "0", "None", ""):
                        t["ScoreTeam1"] = str(val)
                    continue
                if path == "/scores/total/ScoreTeam2":
                    t = scores.get("total")
                    if not isinstance(t, dict):
                        t = {}
                        scores["total"] = t
                    old_val = t.get("ScoreTeam2", "0")
                    if str(val) != "0" or str(old_val) in (
                            "0", "None", ""):
                        t["ScoreTeam2"] = str(val)
                    continue

                if path == "/scores/current/ScoreTeam1":
                    c = scores.get("current")
                    if not isinstance(c, dict):
                        c = {}
                        scores["current"] = c
                    c["ScoreTeam1"] = str(val)
                    continue
                if path == "/scores/current/ScoreTeam2":
                    c = scores.get("current")
                    if not isinstance(c, dict):
                        c = {}
                        scores["current"] = c
                    c["ScoreTeam2"] = str(val)
                    continue

                if path == "/event/statusTranslated":
                    ev = cache.setdefault("event", {})
                    if not isinstance(ev, dict):
                        ev = {}
                        cache["event"] = ev
                    ev["statusTranslated"] = val

                    s = str(val).strip()
                    mm = re.search(r"(\d+)", s)
                    if mm:
                        cache["phase_num"] = int(mm.group(1))

                    all_parts = (
                        cache.get("scores") or {}
                    ).get("all") or []
                    if all_parts:
                        sport_key_cache = cache.get("sport", TABLE_TENNIS)
                        self._recompute_scores_from_all(
                            cache, all_parts, sport_key_cache
                        )
                    continue

                if path.startswith("/markets/"):
                    markets = cache.setdefault("markets", {})
                    if not isinstance(markets, dict):
                        markets = {}
                        cache["markets"] = markets
                    mid = path.split("/", 2)[-1]
                    if isinstance(val, dict):
                        markets[mid] = val
                    continue

                if path in ("/hash", "/event/server",
                            "/event/matchTime"):
                    continue

        outcomes_upd = ws_data.get("outcomes") or []
        if isinstance(outcomes_upd, list):
            for op in outcomes_upd:
                if not isinstance(op, dict):
                    continue
                op_type = op.get("op")
                path = op.get("path", "")
                val = op.get("value")
                if op_type not in ("add", "replace"):
                    continue
                parts = path.split("/")
                if len(parts) >= 3 and parts[1] == "outcomes":
                    out_id = parts[2]
                    field = parts[3] if len(parts) >= 4 else None
                    outcomes_cache = cache.setdefault("outcomes", {})
                    if not isinstance(outcomes_cache, dict):
                        outcomes_cache = {}
                        cache["outcomes"] = outcomes_cache
                    if isinstance(val, dict):
                        outcomes_cache[out_id] = {
                            "outcomeKey": val.get("outcomeKey"),
                            "value": float(val.get("value", 0) or 0),
                            "adValue": val.get("adValue"),
                            "marketId": val.get("marketId"),
                        }
                    elif field == "value":
                        o = outcomes_cache.setdefault(out_id, {})
                        if isinstance(o, dict):
                            o["value"] = float(val or 0)
                    elif field == "adValue":
                        o = outcomes_cache.setdefault(out_id, {})
                        if isinstance(o, dict):
                            o["adValue"] = val

    def _recompute_scores_from_all(self, cache: dict, all_parts: list,
                                    sport_key: str):
        scores = cache.setdefault("scores", {})
        if not isinstance(scores, dict):
            scores = {}
            cache["scores"] = scores

        if not all_parts:
            return

        status_str = (
            (cache.get("event") or {}).get("statusTranslated") or ""
        ).strip()
        phase_num = None
        if status_str:
            mm = re.search(r"(\d+)", status_str)
            if mm:
                phase_num = int(mm.group(1))
        if phase_num is None:
            phase_num = len(all_parts)
        cache["phase_num"] = phase_num

        if 0 < phase_num <= len(all_parts):
            active = all_parts[phase_num - 1] or {}
        else:
            active = all_parts[-1] or {}

        try:
            c1 = int(active.get("ScoreTeam1", 0) or 0)
            c2 = int(active.get("ScoreTeam2", 0) or 0)
        except (ValueError, TypeError):
            c1 = c2 = 0
        c = scores.get("current")
        if not isinstance(c, dict):
            c = {}
            scores["current"] = c
        c["ScoreTeam1"] = str(c1)
        c["ScoreTeam2"] = str(c2)

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            total1 = sum(
                int(p.get("ScoreTeam1", 0) or 0) for p in all_parts
            )
            total2 = sum(
                int(p.get("ScoreTeam2", 0) or 0) for p in all_parts
            )
            t = scores.get("total")
            if not isinstance(t, dict):
                t = {}
                scores["total"] = t
            t["ScoreTeam1"] = str(total1)
            t["ScoreTeam2"] = str(total2)
            return

        won1 = 0
        won2 = 0
        for i, p in enumerate(all_parts):
            if i >= phase_num - 1:
                break
            try:
                s1 = int(p.get("ScoreTeam1", 0) or 0)
                s2 = int(p.get("ScoreTeam2", 0) or 0)
            except (ValueError, TypeError):
                continue
            if s1 > s2:
                won1 += 1
            elif s2 > s1:
                won2 += 1

        if phase_num > 1:
            t = scores.get("total")
            if not isinstance(t, dict):
                t = {}
                scores["total"] = t
            t["ScoreTeam1"] = str(won1)
            t["ScoreTeam2"] = str(won2)

    # ============================================================
    # Сборка Match
    # ============================================================
    def _parse_match_data(self, match_id: str) -> Optional[dict]:
        m = self._matches_cache.get(match_id)
        if not m:
            return None

        player1 = m.get("player1") or ""
        player2 = m.get("player2") or ""
        if not player1 or not player2:
            return None

        sport_key = m.get("sport", TABLE_TENNIS)

        scores = m.get("scores") or {}
        total = scores.get("total") or {}
        current = scores.get("current") or {}
        all_scores = scores.get("all") or []

        try:
            score1 = int(total.get("ScoreTeam1", 0) or 0)
            score2 = int(total.get("ScoreTeam2", 0) or 0)
        except (ValueError, TypeError):
            score1 = score2 = 0

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            st = (
                (m.get("event") or {}).get("statusTranslated") or ""
            ).strip()
            if "перерыв" in st.lower():
                phase_num = max(1, len(all_scores))
                sub1 = sub2 = 0
            else:
                mm = re.search(r"(\d+)", st)
                if mm:
                    phase_num = int(mm.group(1))
                else:
                    phase_num = max(1, len(all_scores))

                sub1 = sub2 = 0
                if 0 < phase_num <= len(all_scores):
                    cur = all_scores[phase_num - 1] or {}
                    try:
                        sub1 = int(cur.get("ScoreTeam1", 0) or 0)
                        sub2 = int(cur.get("ScoreTeam2", 0) or 0)
                    except (ValueError, TypeError):
                        sub1 = sub2 = 0
                if sub1 == 0 and sub2 == 0:
                    try:
                        sub1 = int(current.get("ScoreTeam1", 0) or 0)
                        sub2 = int(current.get("ScoreTeam2", 0) or 0)
                    except (ValueError, TypeError):
                        sub1 = sub2 = 0
        else:
            status_str = (
                (m.get("event") or {}).get("statusTranslated") or ""
            ).strip()
            phase_num = None
            if status_str:
                mm = re.search(r"(\d+)", status_str)
                if mm:
                    phase_num = int(mm.group(1))

            if phase_num is None and all_scores:
                phase_num = len(all_scores)

            if phase_num is None:
                phase_num = score1 + score2 + 1

            sub1 = sub2 = 0

            if all_scores and 0 < phase_num <= len(all_scores):
                cur = all_scores[phase_num - 1] or {}
                try:
                    sub1 = int(cur.get("ScoreTeam1", 0) or 0)
                    sub2 = int(cur.get("ScoreTeam2", 0) or 0)
                except (ValueError, TypeError):
                    sub1 = sub2 = 0

            if sub1 == 0 and sub2 == 0:
                try:
                    sub1 = int(current.get("ScoreTeam1", 0) or 0)
                    sub2 = int(current.get("ScoreTeam2", 0) or 0)
                except (ValueError, TypeError):
                    sub1 = sub2 = 0

        outcomes = m.get("outcomes") or {}
        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0

        for out in outcomes.values():
            if not isinstance(out, dict):
                continue
            key = out.get("outcomeKey")
            try:
                val = float(out.get("value", 0) or 0)
            except (ValueError, TypeError):
                val = 0.0
            adval_raw = out.get("adValue")
            try:
                adval = float(adval_raw) if adval_raw is not None else 0.0
            except (ValueError, TypeError):
                adval = 0.0
            if key == "_1":
                odds1 = val
            elif key == "_2":
                odds2 = val
            elif key == "gross":
                total_over = val
                total_line = adval
            elif key == "less":
                total_under = val
                total_line = adval
            elif key == "1":
                if h1 == 0.0 and h_o1 == 0.0:
                    h1 = adval
                    h_o1 = val
            elif key == "2":
                if h2 == 0.0 and h_o2 == 0.0:
                    h2 = adval
                    h_o2 = val

        event = m.get("event") or {}
        return {
            "sport": sport_key,
            "player1": player1, "player2": player2,
            "score1": score1, "score2": score2,
            "sub1": sub1, "sub2": sub2,
            "phase_num": phase_num,
            "tournament": m.get("tournament", ""),
            "odds1": odds1, "odds2": odds2,
            "total_line": total_line,
            "total_over": total_over,
            "total_under": total_under,
            "handicap1": h1, "handicap2": h2,
            "handicap_odds1": h_o1, "handicap_odds2": h_o2,
            "p_id": self._extract_p_id(event),
            "ext_id": str(event.get("extId") or ""),
        }

    def _extract_p_id(self, event: dict) -> str:
        competitors = event.get("competitors") or []
        if isinstance(competitors, list) and competitors:
            first = competitors[0]
            if isinstance(first, dict):
                pid = first.get("id")
                if pid:
                    return str(pid)
        return "0"

    # ============================================================
    # Отправка
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if match_id in self._finished:
                continue
            parsed = self._parse_match_data(match_id)
            if not parsed:
                continue

            sport_key = parsed["sport"]
            first_seen = self._first_seen.get(match_id, current_time)
            if parsed["odds1"] == 0 and parsed["odds2"] == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (
                parsed["score1"], parsed["score2"],
                parsed["sub1"], parsed["sub2"],
                parsed["phase_num"],
                parsed["odds1"], parsed["odds2"],
                parsed["total_line"], parsed["total_over"],
                parsed["total_under"],
                parsed["handicap1"], parsed["handicap2"],
                parsed["handicap_odds1"], parsed["handicap_odds2"],
            )
            if m.get("_last_sent") == current_state:
                continue

            phase_name = format_phase(sport_key, parsed["phase_num"])
            slug = get_url_slug(self.bk_id, sport_key) or "table-tennis"

            ext_id = parsed.get("ext_id") or match_id

            p1_slug = _slug(parsed["player1"])
            p2_slug = _slug(parsed["player2"])

            match_url = (
                f"https://www.ligastavok.ru/sports/{slug}/"
                f"{p1_slug}-{p2_slug}-id-{match_id}"
                f"-service-id-27-ext-id-{ext_id}"
            )

            match = Match(
                bk_id=self.bk_id,
                match_id=match_id,
                player1=parsed["player1"],
                player2=parsed["player2"],
                score1=parsed["score1"],
                score2=parsed["score2"],
                sub_score1=parsed["sub1"],
                sub_score2=parsed["sub2"],
                tournament=parsed["tournament"],
                odds1=parsed["odds1"],
                odds2=parsed["odds2"],
                total_line=parsed["total_line"],
                total_over=parsed["total_over"],
                total_under=parsed["total_under"],
                handicap1=parsed["handicap1"],
                handicap2=parsed["handicap2"],
                handicap_odds1=parsed["handicap_odds1"],
                handicap_odds2=parsed["handicap_odds2"],
                timestamp=current_time,
                raw_time=phase_name,
                sport=sport_key,
                match_url=match_url,
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            m["_last_sent"] = current_state
            self._last_sent_time[match_id] = current_time
            sent += 1

        if sent:
            logger.info(
                f"[{self.bk_id}] ✅ Отправлено: {sent} "
                f"(в кеше: {len(self._matches_cache)})"
            )

    async def parse(self) -> List[Match]:
        return []