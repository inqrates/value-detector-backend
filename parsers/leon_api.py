# parsers/leon_api.py
"""
Leon — HTTP-парсер через curl_cffi с авто-восстановлением.

Особенности:
  - delta polling: снапшот → changes?vtag=X → changes?vtag=новый → ...
  - vtag живёт ~10-30 минут, потом 400 → перезапрос снапшота
  - watchdog: если 90 сек нет успешных ответов → перезапуск снапшота
  - авто-refresh cookies при 401/403/500
  - автоснятие cookies через Playwright при старте, если файла нет
  - периодический snapshot раз в 10 минут (страховка)

ВАЖНО ПРО URL ПАРАМЕТРЫ:
  Leon периодически меняет ctag и flags. Если парсер отдаёт 500 —
  проверить актуальные значения:
    1. Открыть leon.ru/live в Chrome
    2. F12 → Network → любой запрос к /api-2/betline/events/inplayupcoming
    3. Скопировать параметры ctag и flags
    4. Обновить в .env (без правки кода):
         LEON_CTAG=ru-RU-XXXXX
         LEON_FLAGS=reg,urlv2,orn2,cn,mm3,ssn,rrc,nodup,cmg
  Текущие актуальные значения (на 2026-09-24):
    ctag=ru-RU-7da6p
    flags=reg,urlv2,orn2,cn,mm3,ssn,rrc,nodup,cmg
  (старые mm2 → новые mm3,ssn; к ctag добавился суффикс -7da6p)
"""
import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

from curl_cffi.requests import AsyncSession

from core.models import Match
from core.sport_map import (
    get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)

logger = logging.getLogger(__name__)

COOKIES_DIR = Path("cookies")
COOKIES_PATH = COOKIES_DIR / "leon.json"


FAMILY_TO_SPORT = {
    "TableTennis": TABLE_TENNIS,
    "Volleyball":  VOLLEYBALL,
    "Basketball":  BASKETBALL,
}


def _slug(text: str) -> str:
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class LeonApiParser:
    bk_id = "leon"

    BASE = "https://leon.ru/api-2/betline"

    # ─── Параметры URL (могут меняться, переопределяются через .env) ───
    # Взяты из реального фронта Leon (DevTools → Network).
    # Если Leon их снова изменит — обновить без правки кода:
    #   .env:
    #     LEON_CTAG=ru-RU-XXXXX
    #     LEON_FLAGS=reg,urlv2,...
    CTAG = os.getenv("LEON_CTAG", "ru-RU-7da6p").strip()
    FLAGS = os.getenv(
        "LEON_FLAGS",
        "reg,urlv2,orn2,cn,mm3,ssn,rrc,nodup,cmg",
    ).strip()

    EVENTS_URL = f"{BASE}/events/inplayupcoming?ctag={CTAG}&hideClosed=true&flags={FLAGS}"
    SPORTS_URL = f"{BASE}/sports?ctag={CTAG}&to=120&flags=urlv2"
    CHANGES_TPL = f"{BASE}/changes/inplayupcoming?ctag={CTAG}&vtag={{vtag}}&hideClosed=true&flags={FLAGS}"

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "referer": "https://leon.ru/live",
        "x-app-platform": "web",
        "x-app-referrer": "https://leon.ru/live",
        "x-app-modernity": "modern",
        "x-requested-uri": "/live",
        "x-app-rendering": "csr",
        "x-app-browser": "chrome",
        "x-app-env": "prod",
        "x-app-skin": "default",
        "x-app-os": "windows",
        "x-app-version": "6.144.2",
        "x-app-layout": "desktop",
        "x-app-theme": "LIGHT",
        "x-app-language": "ru_RU",
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua-mobile": "?0",
    }

    IMPERSONATE = "chrome150"
    POLL_INTERVAL = 3.0

    # Watchdog / восстановление
    SNAPSHOT_INTERVAL = 600.0     # 10 мин — принудительный снапшот
    SILENCE_TIMEOUT = 90.0        # 90 сек тишины → перезапуск
    REFRESH_MIN_INTERVAL = 120.0  # не чаще 1 refresh в 2 минуты

    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        self.detector = detector
        self.aggregator = aggregator
        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]
        self.is_running = False

        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self._last_update_time: Dict[str, float] = {}

        self._sport_id_to_family: Dict[str, str] = {}

        self._vtag: Optional[str] = None
        self._session: Optional[AsyncSession] = None

        # Watchdog
        self._last_successful_response_at: float = 0.0
        self._last_snapshot_at: float = 0.0
        self._last_refresh_at: float = 0.0
        self._consecutive_errors: int = 0
        self._empty_vtag_strikes: int = 0
        self._refresh_lock = asyncio.Lock()

    # ============================================================
    # Cookies
    # ============================================================
    def _load_cookies(self) -> list:
        if not COOKIES_PATH.exists():
            return []
        try:
            return json.loads(COOKIES_PATH.read_text(encoding="utf-8")).get("cookies", [])
        except Exception:
            return []

    async def _ensure_session(self):
        if self._session is not None:
            return
        self._session = AsyncSession(
            impersonate=self.IMPERSONATE,
            timeout=30,
            headers=self.HEADERS,
        )

        cookies = self._load_cookies()

        # ── Если cookies пусты — сразу снимаем через Playwright ──
        # Иначе стартуем с пустой сессией и получаем 500/403.
        if not cookies:
            logger.warning(
                f"[{self.bk_id}] cookies пусты — снимаю через Playwright"
            )
            # Обход REFRESH_MIN_INTERVAL через force
            self._last_refresh_at = 0.0
            await self._refresh_cookies_via_playwright()
            cookies = self._load_cookies()

        for c in cookies:
            try:
                self._session.cookies.set(
                    c["name"], c["value"],
                    domain=c.get("domain"), path=c.get("path", "/"),
                )
            except Exception:
                pass
        logger.info(f"[{self.bk_id}] 🍪 Загружено {len(cookies)} cookies")

    async def _refresh_cookies_via_playwright(self):
        async with self._refresh_lock:
            now = time.time()
            if now - self._last_refresh_at < self.REFRESH_MIN_INTERVAL:
                return False
            self._last_refresh_at = now

            logger.warning(f"[{self.bk_id}] 🔄 Refresh cookies через Playwright")
            page = None
            try:
                from core.browser_manager import browser_manager
                page = await browser_manager.new_page()
                await page.goto("https://leon.ru/live",
                                wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(5000)
                try:
                    await page.evaluate("window.scrollTo(0, 2000)")
                    await page.wait_for_timeout(1000)
                except Exception:
                    pass
                cookies = await page.context.cookies()
                if not cookies:
                    return False
                COOKIES_DIR.mkdir(exist_ok=True)
                COOKIES_PATH.write_text(
                    json.dumps({"cookies": cookies}, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                # Обновляем в сессии
                try:
                    self._session.cookies.clear()
                except Exception:
                    pass
                for c in cookies:
                    try:
                        self._session.cookies.set(
                            c["name"], c["value"],
                            domain=c.get("domain"), path=c.get("path", "/"),
                        )
                    except Exception:
                        pass
                logger.info(f"[{self.bk_id}] ✅ Cookies обновлены ({len(cookies)} шт.)")
                return True
            except Exception as e:
                logger.error(f"[{self.bk_id}] refresh ошибка: {e}")
                return False
            finally:
                if page is not None:
                    try:
                        await page.close()
                    except Exception:
                        pass

    # ============================================================
    # Жизненный цикл
    # ============================================================
    async def start(self):
        await self._ensure_session()
        await self._load_sports_dict()
        await self._fetch_initial_snapshot()
        logger.info(f"[{self.bk_id}] ✅ HTTP-парсер инициализирован")

    async def stop(self):
        self.is_running = False
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
        logger.info(f"[{self.bk_id}] 🛑 Остановка HTTP-парсера...")

    async def run(self):
        self.is_running = True
        await self.start()
        logger.info(f"[{self.bk_id}] 🚀 HTTP-парсер Leon запущен")

        while self.is_running:
            t0 = time.monotonic()
            try:
                now = time.time()

                # 1) Watchdog: молчание > SILENCE_TIMEOUT → полный рестарт снапшота
                if (self._last_successful_response_at > 0
                        and now - self._last_successful_response_at > self.SILENCE_TIMEOUT):
                    logger.warning(
                        f"[{self.bk_id}] ⚠️ Тишина {now - self._last_successful_response_at:.0f}с "
                        f"→ перезапрос снапшота"
                    )
                    await self._fetch_initial_snapshot()
                    self._last_successful_response_at = time.time()

                    # Если даже снапшот не помог — refresh cookies
                    if self._consecutive_errors >= 3:
                        logger.warning(f"[{self.bk_id}] много ошибок подряд → refresh cookies")
                        await self._refresh_cookies_via_playwright()
                        self._consecutive_errors = 0

                # 2) Периодический принудительный снапшот (страховка от протухшего vtag)
                elif now - self._last_snapshot_at > self.SNAPSHOT_INTERVAL:
                    logger.info(f"[{self.bk_id}] 🔄 плановый снапшот (прошло {now - self._last_snapshot_at:.0f}с)")
                    await self._fetch_initial_snapshot()

                # 3) Обычный delta-polling
                else:
                    await self._poll_changes()

                await self._try_send_matches()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._consecutive_errors += 1
                logger.error(f"[{self.bk_id}] Ошибка опроса: {e}", exc_info=True)
                await asyncio.sleep(3)
                continue

            elapsed = time.monotonic() - t0
            sleep_time = max(0, self.POLL_INTERVAL - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    # ============================================================
    # Sports dict
    # ============================================================
    async def _load_sports_dict(self):
        try:
            r = await self._session.get(self.SPORTS_URL, headers=self.HEADERS)
            if r.status_code != 200:
                logger.warning(f"[{self.bk_id}] sports HTTP {r.status_code}")
                return
            data = r.json()
        except Exception as e:
            logger.warning(f"[{self.bk_id}] sports error: {e}")
            return

        # Leon отдаёт массив (не dict) — но код поддерживает оба варианта
        sports = data.get("sports") if isinstance(data, dict) else data
        if not isinstance(sports, list):
            return
        for sp in sports:
            if not isinstance(sp, dict):
                continue
            sid = sp.get("id")
            family = sp.get("family")
            if sid is not None and family:
                self._sport_id_to_family[str(sid)] = family
        logger.info(f"[{self.bk_id}] 📖 Справочник sports: {len(self._sport_id_to_family)} видов")

    # ============================================================
    # Снапшот + delta
    # ============================================================
    async def _fetch_initial_snapshot(self):
        try:
            r = await self._session.get(self.EVENTS_URL, headers=self.HEADERS)
        except Exception as e:
            self._consecutive_errors += 1
            logger.warning(f"[{self.bk_id}] snapshot network error: {e}")
            return

        if r.status_code in (401, 403):
            self._consecutive_errors += 1
            logger.warning(f"[{self.bk_id}] snapshot HTTP {r.status_code} → refresh cookies")
            await self._refresh_cookies_via_playwright()
            return

        if r.status_code == 500:
            # 500 от Leon обычно означает "неправильные параметры URL".
            # Проверь актуальные ctag и flags через DevTools (см. докстринг).
            self._consecutive_errors += 1
            body = ""
            try:
                body = r.text[:200]
            except Exception:
                pass
            logger.warning(
                f"[{self.bk_id}] snapshot HTTP 500 — проверь ctag/flags. "
                f"Сейчас: CTAG={self.CTAG}, FLAGS={self.FLAGS}. "
                f"Body: {body}"
            )
            return

        if r.status_code != 200:
            self._consecutive_errors += 1
            logger.warning(f"[{self.bk_id}] snapshot HTTP {r.status_code}")
            return

        try:
            data = r.json()
        except Exception:
            self._consecutive_errors += 1
            return

        # Успех!
        self._consecutive_errors = 0
        self._last_successful_response_at = time.time()
        self._last_snapshot_at = time.time()

        events = data.get("events", [])
        vtag = data.get("vtag")
        for ev in events:
            league = ev.get("league") or {}
            sp = league.get("sport") or {}
            sid = sp.get("id")
            fam = sp.get("family")
            if sid is not None and fam:
                self._sport_id_to_family.setdefault(str(sid), fam)

        if vtag:
            self._vtag = vtag
        logger.info(
            f"[{self.bk_id}] 📸 Снапшот: {len(events)} событий, vtag={vtag}"
        )
        self._process_events(events)

    async def _poll_changes(self):
        if not self._vtag:
            await self._fetch_initial_snapshot()
            return

        url = self.CHANGES_TPL.format(vtag=self._vtag)
        try:
            r = await self._session.get(url, headers=self.HEADERS)
        except Exception as e:
            self._consecutive_errors += 1
            logger.warning(f"[{self.bk_id}] changes network error: {e}")
            return

        if r.status_code == 400:
            logger.warning(f"[{self.bk_id}] vtag устарел (400) → снапшот")
            await self._fetch_initial_snapshot()
            return

        if r.status_code in (401, 403):
            self._consecutive_errors += 1
            logger.warning(f"[{self.bk_id}] changes HTTP {r.status_code} → refresh cookies")
            await self._refresh_cookies_via_playwright()
            return

        if r.status_code == 500:
            self._consecutive_errors += 1
            logger.warning(
                f"[{self.bk_id}] changes HTTP 500 — вероятно vtag/ctag устарел. "
                f"Перезапрос снапшота."
            )
            await self._fetch_initial_snapshot()
            return

        if r.status_code != 200:
            self._consecutive_errors += 1
            logger.warning(f"[{self.bk_id}] changes HTTP {r.status_code}")
            return

        try:
            data = r.json()
        except Exception:
            self._consecutive_errors += 1
            return

        # Успешный HTTP-ответ
        self._consecutive_errors = 0
        self._last_successful_response_at = time.time()
        if hasattr(self, "_empty_vtag_strikes"):
            self._empty_vtag_strikes = 0

        new_vtag = data.get("vtag")
        if new_vtag:
            self._vtag = new_vtag

        # ⚠️ Leon отдаёт данные под ключом "data", а не "events".
        events = data.get("data") or data.get("events") or []
        if events:
            self._process_events(events)

    # ============================================================
    # Обработка
    # ============================================================
    def _process_events(self, events: list):
        for ev in events:
            if not isinstance(ev, dict):
                continue
            self._process_event(ev)

    def _process_event(self, m: dict):
        match_id = str(m.get("id", ""))
        if not match_id:
            return

        league = m.get("league") or {}
        sport = league.get("sport") or {}
        sid = sport.get("id")

        family = sport.get("family")
        if not family and sid is not None:
            family = self._sport_id_to_family.get(str(sid))
        if not family:
            return

        sport_key = FAMILY_TO_SPORT.get(family)
        if not sport_key:
            return

        region = league.get("region") or {}
        if sport_key == BASKETBALL and region.get("family") == "ELECTRONIC_LEAGUES":
            sport_key = CYBER_BASKETBALL

        if sport_key not in self.enabled_sports:
            return

        if match_id not in self._matches_cache:
            self._matches_cache[match_id] = {"_last_sent": None}
            self._first_seen[match_id] = time.time()

        cache = self._matches_cache[match_id]
        cache["sport"] = sport_key

        tournament = league.get("name", "Неизвестно")
        region_name = region.get("name", "international")

        p1 = p2 = "Неизвестно"
        for comp in m.get("competitors", []):
            if comp.get("homeAway") == "HOME":
                p1 = comp.get("name", "Неизвестно")
            elif comp.get("homeAway") == "AWAY":
                p2 = comp.get("name", "Неизвестно")

        live = m.get("liveStatus") or {}
        score_str = (live.get("score") or "0:0").replace("*", "").strip()
        if score_str in ("-:-", ""):
            score1 = score2 = 0
        else:
            try:
                score1, score2 = map(int, score_str.split(":"))
            except ValueError:
                score1 = score2 = 0

        set_scores_str = live.get("setScores") or ""
        sub1 = sub2 = 0
        if set_scores_str:
            parts = [s.strip() for s in set_scores_str.split(";") if s.strip()]
            if parts:
                try:
                    sub1, sub2 = map(int, parts[-1].split(":"))
                except ValueError:
                    pass

        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0

        for market in m.get("markets", []):
            m_name = market.get("name", "")
            runners = market.get("runners", [])
            if m_name == "Победитель":
                for r in runners:
                    if r.get("name") == "1":
                        odds1 = float(r.get("price", 0) or 0)
                    elif r.get("name") == "2":
                        odds2 = float(r.get("price", 0) or 0)
            elif m_name in ("Тотал очков", "Тотал"):
                total_line = float(market.get("handicap", 0) or 0)
                for r in runners:
                    r_name = r.get("name", "")
                    price = float(r.get("price", 0) or 0)
                    if "Меньше" in r_name:
                        total_under = price
                    elif "Больше" in r_name:
                        total_over = price
            elif m_name == "Фора":
                for r in runners:
                    r_name = r.get("name", "")
                    price = float(r.get("price", 0) or 0)
                    match_re = re.search(r'([12])\s*\(([+-]?\d+\.?\d*)\)', r_name)
                    if match_re:
                        player = match_re.group(1)
                        h_line = float(match_re.group(2))
                        if player == "1" and h1 == 0.0 and h_o1 == 0.0:
                            h1, h_o1 = h_line, price
                        elif player == "2" and h2 == 0.0 and h_o2 == 0.0:
                            h2, h_o2 = h_line, price

        stage_str = live.get("stage", "") or ""
        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            mm = re.search(r'(\d+)', stage_str)
            if mm:
                phase_num = int(mm.group(1))
            else:
                parts_count = len(
                    [x for x in set_scores_str.split(";") if x.strip()]
                ) if set_scores_str else 0
                phase_num = parts_count if parts_count > 0 else 1
            if re.search(r'\bОТ\b|\bOT\b', stage_str, re.IGNORECASE):
                phase_num = 5
        else:
            phase_num = score1 + score2 + 1

        cache.update({
            "player1": p1, "player2": p2,
            "score1": score1, "score2": score2,
            "sub1": sub1, "sub2": sub2,
            "phase_num": phase_num,
            "tournament": tournament,
            "region": region_name,
            "odds1": odds1, "odds2": odds2,
            "total_line": total_line, "total_over": total_over, "total_under": total_under,
            "handicap1": h1, "handicap2": h2,
            "handicap_odds1": h_o1, "handicap_odds2": h_o2,
            "status": stage_str,
        })
        self._last_update_time[match_id] = time.time()

    # ============================================================
    # Отправка
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        now = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get("player1") or not m.get("player2") or m["player1"] == "Неизвестно":
                continue

            sport_key = m.get("sport", TABLE_TENNIS)

            first_seen = self._first_seen.get(match_id, now)
            if m.get("odds1", 0) == 0 and m.get("odds2", 0) == 0:
                if now - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if now - last_sent < 1.0:
                continue

            current_state = (
                m.get("score1", 0), m.get("score2", 0),
                m.get("sub1", 0), m.get("sub2", 0),
                m.get("phase_num", 0),
                m.get("odds1", 0.0), m.get("odds2", 0.0),
                m.get("total_line", 0.0), m.get("total_over", 0.0),
                m.get("total_under", 0.0),
                m.get("handicap1", 0.0), m.get("handicap2", 0.0),
                m.get("handicap_odds1", 0.0), m.get("handicap_odds2", 0.0),
            )
            if m.get("_last_sent") == current_state:
                continue

            score1 = m.get("score1", 0)
            score2 = m.get("score2", 0)
            phase_num = m.get("phase_num", score1 + score2 + 1)
            phase_name = format_phase(sport_key, phase_num)

            # Leon принимает произвольный регион/турнир.
            # Используем /x/x/ чтобы не зависеть от кириллицы и транслита.
            # Проверено: работает для НТ/волейбола/баскетбола.
            slug = get_url_slug(self.bk_id, sport_key) or "table-tennis"
            p1_slug = _slug(m.get("player1", ""))
            p2_slug = _slug(m.get("player2", ""))
            match_url = (
                f"https://leon.ru/bets/{slug}/x/x/"
                f"{match_id}-{p1_slug}-{p2_slug}"
            )

            match = Match(
                bk_id=self.bk_id,
                match_id=match_id,
                player1=m["player1"],
                player2=m["player2"],
                score1=score1, score2=score2,
                sub_score1=m.get("sub1", 0),
                sub_score2=m.get("sub2", 0),
                tournament=m.get("tournament", ""),
                odds1=m.get("odds1", 0.0),
                odds2=m.get("odds2", 0.0),
                total_line=m.get("total_line", 0.0),
                total_over=m.get("total_over", 0.0),
                total_under=m.get("total_under", 0.0),
                handicap1=m.get("handicap1", 0.0),
                handicap2=m.get("handicap2", 0.0),
                handicap_odds1=m.get("handicap_odds1", 0.0),
                handicap_odds2=m.get("handicap_odds2", 0.0),
                timestamp=now,
                raw_time=phase_name,
                sport=sport_key,
                match_url=match_url,
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            m["_last_sent"] = current_state
            self._last_sent_time[match_id] = now
            sent += 1

        if sent:
            logger.info(
                f"[{self.bk_id}] ✅ Отправлено: {sent} "
                f"(в кеше: {len(self._matches_cache)})"
            )

    async def parse(self) -> List[Match]:
        return []