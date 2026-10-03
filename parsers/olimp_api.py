# parsers/olimp_api.py
"""
Olimp — HTTP-парсер через curl_cffi (без Playwright).

API отдаёт JSON на /api/v4/0/live/sports-with-competitions-with-events
без cookies, только с правильными headers (X-Cupis, X-Olimp, Origin, Referer).
Проверено: HTTP 200, ~3.6 МБ JSON, 23 элемента массива (по одному на вид спорта).

Класс сохранён как OlimpApiParser и имеет тот же интерфейс, что у старой
Playwright-версии, чтобы main.py не требовал правок.
"""
import asyncio
import logging
import time
from typing import Dict, List

from curl_cffi.requests import AsyncSession

from core.models import Match
from core.sport_map import (
    SPORT_MAP, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)

logger = logging.getLogger(__name__)


class OlimpApiParser:
    bk_id = "olimp"

    URL = "https://www.olimp.bet/api/v4/0/live/sports-with-competitions-with-events"

    HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9",
        "X-Cupis": "1",
        "X-Olimp": "cupis-desktop",
        "Origin": "https://www.olimp.bet",
        "Referer": "https://www.olimp.bet/live",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
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

        # ---- Кэши (эти же имена ищет global_cache_cleaner в main.py) ----
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self._last_update_time: Dict[str, float] = {}

        # sport_id (str) → sport_key
        self._sport_ids: Dict[str, str] = {}
        for sk in self.enabled_sports:
            cfg = SPORT_MAP.get(self.bk_id, {}).get(sk, {})
            for sid in cfg.get("ids", []):
                self._sport_ids[str(sid)] = sk
        logger.info(f"[{self.bk_id}] sport_ids: {self._sport_ids}")

        self._session: AsyncSession = None

    # ============================================================
    # Жизненный цикл
    # ============================================================
    async def start(self):
        if self._session is None:
            self._session = AsyncSession(
                impersonate=self.IMPERSONATE,
                timeout=30,
                headers=self.HEADERS,
            )
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
        logger.info(f"[{self.bk_id}] 🚀 HTTP-парсер Olimp запущен (без Playwright)")

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

        data = r.json()
        items = data if isinstance(data, list) else [data]

        for item in items:
            if not isinstance(item, dict):
                continue
            payload = item.get("payload")
            if not isinstance(payload, dict):
                continue
            comps = payload.get("competitionsWithEvents")
            if not isinstance(comps, list) or not comps:
                continue
            self._process_payload(payload)

        await self._try_send_matches()

    # ============================================================
    # Разбор payload (логика из старой Playwright-версии)
    # ============================================================
    def _process_payload(self, payload: dict):
        comps = payload.get("competitionsWithEvents", [])
        if not isinstance(comps, list):
            return

        for block in comps:
            if not isinstance(block, dict):
                continue
            comp = block.get("competition", {}) or {}
            tournament = comp.get("name", "Неизвестно")
            tournament_id = comp.get("id")           # ← НОВОЕ
            for event in block.get("events", []) or []:
                if not isinstance(event, dict):
                    continue
                sport_id_str = str(event.get("sportId") or "")
                sport_key = self._sport_ids.get(sport_id_str)
                if not sport_key:
                    continue
                # ← передаём tournament_id
                self._process_event(event, tournament, sport_key, tournament_id)

    def _process_event(self, event: dict, tournament: str, sport_key: str,
                       tournament_id=None):
        if event.get("state") == "FINISHED":
            return

        match_id = str(event.get("id") or "")
        if not match_id:
            return

        if match_id not in self._matches_cache:
            self._matches_cache[match_id] = {"_last_sent": None}
            self._first_seen[match_id] = time.time()

        cache = self._matches_cache[match_id]
        cache["tournament"] = tournament
        cache["tournament_id"] = tournament_id      # ← НОВОЕ
        cache["sport"] = sport_key
        cache["player1"] = event.get("team1Name", "Неизвестно")
        cache["player2"] = event.get("team2Name", "Неизвестно")

        # ---- score ----
        try:
            s1, s2 = map(int, (event.get("score") or "0:0").split(":"))
        except (ValueError, TypeError):
            s1, s2 = 0, 0

        # ---- sub_score из mapsScore ----
        pairs = []
        for m in event.get("mapsScore") or []:
            if isinstance(m, dict):
                try:
                    pairs.append((int(m.get("team1", 0)), int(m.get("team2", 0))))
                except (ValueError, TypeError):
                    pairs.append((0, 0))

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            if pairs:
                sub1, sub2 = pairs[-1]
                phase_num = len(pairs)
                if s1 == 0 and s2 == 0:
                    t1 = sum(p[0] for p in pairs)
                    t2 = sum(p[1] for p in pairs)
                    if t1 > 0 or t2 > 0:
                        s1, s2 = t1, t2
            else:
                sub1 = sub2 = 0
                phase_num = 1
        else:
            if pairs:
                sub1, sub2 = pairs[-1]
            else:
                sub1 = sub2 = 0
            phase_num = s1 + s2 + 1

        cache["score1"] = s1
        cache["score2"] = s2
        cache["sub1"] = sub1
        cache["sub2"] = sub2
        cache["phase_num"] = phase_num

        # ---- Кэфы ----
        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0

        for out in event.get("outcomes", []) or []:
            if not isinstance(out, dict):
                continue
            table_type = out.get("tableType", "")
            categories = out.get("categories", []) or []
            short_name = out.get("shortName", "")

            try:
                prob = float(str(out.get("probability", "0")).replace(",", "."))
            except (ValueError, TypeError):
                prob = 0.0
            try:
                param = float(str(out.get("param", "0")).replace(",", "."))
            except (ValueError, TypeError):
                param = 0.0

            is_result = (table_type == "RESULT") or ("RESULT" in categories)

            if is_result and short_name == "П1":
                odds1 = prob
            elif is_result and short_name == "П2":
                odds2 = prob
            elif table_type == "HANDICAP" and short_name == "Фора 1":
                h1, h_o1 = param, prob
            elif table_type == "HANDICAP" and short_name == "Фора 2":
                h2, h_o2 = param, prob
            elif table_type == "TOTAL" and short_name == "ТотМ":
                total_under = prob
                total_line = param
            elif table_type == "TOTAL" and short_name == "ТотБ":
                total_over = prob
                total_line = param

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
    # Отправка в detector/aggregator
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
            slug = get_url_slug(self.bk_id, sport_key) or "nastolnyy-tennis-40"
            tour_id = m.get("tournament_id")

            if tour_id:
                # Проверенный формат: /live/{sport}/{tour_id}/{match_id}
                # Работает для НТ / волейбола / баскетбола (проверено на живых матчах).
                match_url = (
                    f"https://www.olimp.bet/live/{slug}/{tour_id}/{match_id}"
                )
            else:
                # Без tournament_id URL не работает → пусто, фронт ищет кликом
                match_url = ""
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