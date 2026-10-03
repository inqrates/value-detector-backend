# parsers/betcity_api.py
"""
Betcity — HTTP-парсер через curl_cffi (без Playwright в горячем цикле).

Особенности:
  - Единственный эндпоинт on_air/bets отдаёт ВСЕ виды спорта одним JSON:
      sports["46"] — Настольный теннис
      sports["12"] — Волейбол
      sports["3"]  — Баскетбол (+ кибер по chmp.is_cyber==1)
  - Требует cookies (cfidsgib-w-betcity, bh и др.), которые снимаются
    Playwright-ом ОДИН РАЗ в cookies/betcity.json.
  - При протухании cookies (reply.error) автоматически переснимаем их
    через Playwright.

URL и заголовки — 1-в-1 как у браузера (см. cookies/betcity_request.json).
"""
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Dict, List

from curl_cffi.requests import AsyncSession

from core.models import Match
from core.sport_map import (
    SPORT_MAP, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)

logger = logging.getLogger(__name__)

COOKIES_DIR = Path("cookies")
COOKIES_PATH = COOKIES_DIR / "betcity.json"


class BetcityApiParser:
    bk_id = "betcity"

    URL = (
        "https://ad.betcity.ru/d/on_air/bets"
        "?rev=8&add=dep_events&ver=88&csn=ooca9s&lng=0"
    )

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "referer": "https://betcity.ru/",
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua-mobile": "?0",
    }

    IMPERSONATE = "chrome150"
    POLL_INTERVAL = 0.5

    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        self.detector = detector
        self.aggregator = aggregator
        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]
        self.is_running = False

        # ---- Кэши (те же имена, что ждёт global_cache_cleaner) ----
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self._last_update_time: Dict[str, float] = {}

        # sport_id ("46"/"12"/"3") → sport_key
        self._sport_ids: Dict[str, str] = {}
        for sk in self.enabled_sports:
            cfg = SPORT_MAP.get(self.bk_id, {}).get(sk, {})
            for sid in cfg.get("ids", []):
                self._sport_ids.setdefault(str(sid), sk)
        logger.info(f"[{self.bk_id}] sport_ids: {self._sport_ids}")

        self._session: AsyncSession = None
        self._refresh_lock = asyncio.Lock()

    # ============================================================
    # Cookies: load / refresh
    # ============================================================
    def _load_cookies(self):
        if not COOKIES_PATH.exists():
            return []
        try:
            return json.loads(
                COOKIES_PATH.read_text(encoding="utf-8")
            ).get("cookies", [])
        except Exception as e:
            logger.warning(f"[{self.bk_id}] load cookies: {e}")
            return []

    async def _refresh_cookies_via_playwright(self):
        """Один раз открыть Chromium, снять свежие cookies, сохранить."""
        async with self._refresh_lock:
            logger.warning(f"[{self.bk_id}] 🔄 Переснимаем cookies через Playwright")
            try:
                from core.browser_manager import browser_manager
                page = await browser_manager.new_page()
                try:
                    await page.goto(
                        "https://betcity.ru/ru/live",
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )
                    await page.wait_for_timeout(6000)
                    try:
                        await page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight)"
                        )
                        await page.wait_for_timeout(1500)
                    except Exception:
                        pass
                    cookies = await page.context.cookies()
                    COOKIES_DIR.mkdir(exist_ok=True)
                    COOKIES_PATH.write_text(
                        json.dumps({"cookies": cookies}, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    logger.info(
                        f"[{self.bk_id}] ✅ Cookies обновлены ({len(cookies)} шт.)"
                    )
                    return cookies
                finally:
                    try:
                        await page.close()
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"[{self.bk_id}] Не удалось обновить cookies: {e}")
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
        if not cookies:
            cookies = await self._refresh_cookies_via_playwright()
        for c in cookies:
            try:
                self._session.cookies.set(
                    c["name"], c["value"],
                    domain=c.get("domain"),
                    path=c.get("path", "/"),
                )
            except Exception:
                pass
        logger.info(f"[{self.bk_id}] 🍪 Загружено {len(cookies)} cookies")

    def _reload_cookies_into_session(self, cookies):
        if self._session is None:
            return
        for c in cookies:
            try:
                self._session.cookies.set(
                    c["name"], c["value"],
                    domain=c.get("domain"),
                    path=c.get("path", "/"),
                )
            except Exception:
                pass

    # ============================================================
    # Жизненный цикл
    # ============================================================
    async def start(self):
        await self._ensure_session()
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
        logger.info(f"[{self.bk_id}] 🚀 HTTP-парсер Betcity запущен (без Playwright)")

        while self.is_running:
            t0 = time.monotonic()
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка опроса: {e}", exc_info=True)
                await asyncio.sleep(3)
                continue

            elapsed = time.monotonic() - t0
            sleep_time = max(0, self.POLL_INTERVAL - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    # ============================================================
    # Один опрос API
    # ============================================================
    async def _poll_once(self):
        r = await self._session.get(self.URL, headers=self.HEADERS)
        if r.status_code != 200:
            logger.warning(f"[{self.bk_id}] HTTP {r.status_code}")
            return

        try:
            data = r.json()
        except Exception as e:
            logger.warning(f"[{self.bk_id}] JSON error: {e}")
            return

        reply = data.get("reply", {})
        sports = reply.get("sports")

        # cookies протухли — переснять и продолжить
        if not sports:
            err = reply.get("error")
            logger.warning(f"[{self.bk_id}] reply.error={err} → обновляем cookies")
            cookies = await self._refresh_cookies_via_playwright()
            if cookies:
                self._reload_cookies_into_session(cookies)
            return

        for sid, sport_data in sports.items():
            sport_key = self._sport_ids.get(str(sid))
            if not sport_key:
                continue
            self._process_sport(sport_data, sport_key)

        await self._try_send_matches()

    # ============================================================
    # Разбор payload (логика из Playwright-версии)
    # ============================================================
    def _process_sport(self, sport_data: dict, sport_key: str):
        championships = sport_data.get("chmps", {}) or {}
        for chmp_id, chmp_data in championships.items():
            tournament_name = chmp_data.get("name_ch", "Неизвестно")
            is_cyber = chmp_data.get("is_cyber", 0)
            events = chmp_data.get("evts", {}) or {}

            effective_sport = sport_key
            if sport_key == BASKETBALL and is_cyber == 1:
                effective_sport = CYBER_BASKETBALL

            for ev_id, ev_data in events.items():
                if ev_data.get("team_type_f", 0) != 0:
                    continue
                if ev_data.get("is_dep", 0) == 1:
                    continue
                self._process_event(
                    str(ev_id), ev_data, tournament_name,
                    str(chmp_id), effective_sport,
                )

    def _process_event(self, match_id: str, ev_data: dict,
                       tournament_name: str, champ_id: str, sport_key: str):
        if match_id not in self._matches_cache:
            self._matches_cache[match_id] = {"_last_sent": None}
            self._first_seen[match_id] = time.time()

        cache = self._matches_cache[match_id]
        cache["tournament"] = tournament_name
        cache["champ_id"] = champ_id
        cache["sport"] = sport_key
        cache["player1"] = ev_data.get("name_ht", "Неизвестно")
        cache["player2"] = ev_data.get("name_at", "Неизвестно")
        cache["time_name"] = ev_data.get("time_name", "") or ""

        # score из sc_ev
        score_str = ev_data.get("sc_ev", "0:0") or "0:0"
        try:
            s1, s2 = map(int, score_str.split(":"))
        except ValueError:
            s1, s2 = 0, 0
        cache["score1"] = s1
        cache["score2"] = s2

        # sc_inter → список пар (a,b)
        pairs = []
        sets_str = ev_data.get("sc_inter", "") or ""
        if sets_str:
            for part in sets_str.split(","):
                part = part.strip()
                if ":" not in part:
                    continue
                try:
                    a, b = part.split(":", 1)
                    pairs.append((int(a), int(b)))
                except ValueError:
                    continue

        time_lower = cache["time_name"].lower()
        is_break = "перерыв" in time_lower

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            if pairs:
                if is_break:
                    sub1 = sub2 = 0
                    phase_num = len(pairs) + 1
                else:
                    sub1, sub2 = pairs[-1]
                    phase_num = len(pairs)
            else:
                sub1 = sub2 = 0
                phase_num = 1
        else:
            if pairs:
                sub1, sub2 = pairs[-1]
            else:
                sub1 = sub2 = 0
            phase_num = s1 + s2 + 1

        cache["sub1"] = sub1
        cache["sub2"] = sub2
        cache["phase_num"] = phase_num

        # Кэфы
        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0
        main_markets = ev_data.get("main", {}) or {}

        if "69" in main_markets:
            wm = (main_markets["69"].get("data", {})
                  .get(match_id, {})
                  .get("blocks", {})
                  .get("Wm", {}))
            odds1 = float(wm.get("P1", {}).get("kf", 0) or 0)
            odds2 = float(wm.get("P2", {}).get("kf", 0) or 0)

        if "72" in main_markets:
            t1m = (main_markets["72"].get("data", {})
                   .get(match_id, {})
                   .get("blocks", {})
                   .get("T1m", {}))
            total_line = float(t1m.get("Tot", 0) or 0)
            total_under = float(t1m.get("Tm", {}).get("kf", 0) or 0)
            total_over = float(t1m.get("Tb", {}).get("kf", 0) or 0)

        if "71" in main_markets:
            f1m = (main_markets["71"].get("data", {})
                   .get(match_id, {})
                   .get("blocks", {})
                   .get("F1m", {}))
            h1 = float(f1m.get("F1", 0) or 0)
            h_o1 = float(f1m.get("Kf_F1", {}).get("kf", 0) or 0)
            h2 = float(f1m.get("F2", 0) or 0)
            h_o2 = float(f1m.get("Kf_F2", {}).get("kf", 0) or 0)

        cache["odds1"] = odds1
        cache["odds2"] = odds2
        cache["total_line"] = total_line
        cache["total_over"] = total_over
        cache["total_under"] = total_under
        cache["handicap1"] = h1
        cache["handicap2"] = h2
        cache["handicap_odds1"] = h_o1
        cache["handicap_odds2"] = h_o2

        self._last_update_time[match_id] = time.time()

    # ============================================================
    # Отправка
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        now = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get("player1") or m["player1"] == "Неизвестно":
                continue

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

            sport_key = m.get("sport", TABLE_TENNIS)
            slug = get_url_slug(self.bk_id, sport_key) or "table-tennis"
            if not slug:
                slug = "basketball" if sport_key == CYBER_BASKETBALL else "table-tennis"

            champ_id = m.get("champ_id")
            if champ_id:
                match_url = f"https://betcity.ru/ru/live/{slug}/{champ_id}/{match_id}"
            else:
                match_url = f"https://betcity.ru/ru/live/{slug}/{match_id}"

            phase_name = format_phase(sport_key, m.get("phase_num", 1))

            match = Match(
                bk_id=self.bk_id,
                match_id=match_id,
                player1=m["player1"],
                player2=m["player2"],
                score1=m.get("score1", 0),
                score2=m.get("score2", 0),
                sub_score1=m.get("sub1", 0),
                sub_score2=m.get("sub2", 0),
                tournament=m.get("tournament", "Неизвестно"),
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

            logger.info(
                f"[{self.bk_id}] 🟢 [{sport_key}] {match.player1} vs {match.player2} | "
                f"матч {match.score1}:{match.score2} | "
                f"{phase_name} {match.sub_score1}:{match.sub_score2} | "
                f"К: {match.odds1}/{match.odds2}"
            )

        if sent:
            logger.info(
                f"[{self.bk_id}] ✅ Отправлено: {sent} "
                f"(в кеше: {len(self._matches_cache)})"
            )

    async def parse(self) -> List[Match]:
        return []