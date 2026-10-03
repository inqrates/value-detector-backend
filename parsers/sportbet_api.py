# parsers/sportbet_api.py
"""
Sportbet — HTTP-парсер через curl_cffi (без Playwright).
Логика разбора скопирована из старой Playwright-версии.

Cookies не нужны, авторизация открытая.

HTTP: GET /events.table?status=live&lang=ru&isTime=true — снапшот всех live.
Структура: data.sports[].tournaments[].events[]
Кэфы: markets[].id — 186/238/237 (НТ, волейбол), 219/225/223 (баскет).
"""
import asyncio
import logging
import re
import time
from typing import Dict, List, Optional

from curl_cffi.requests import AsyncSession

from core.models import Match
from core.sport_map import (
    get_sport_config, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL, BEACH_VOLLEYBALL,
)

logger = logging.getLogger(__name__)


# Коды маркетов по видам спорта (на основе дампа Sportbet)
MARKET_CODES = {
    TABLE_TENNIS:       {"win": 186, "total": 238, "handicap": 237},
    VOLLEYBALL:         {"win": 186, "total": 238, "handicap": 237},
    BEACH_VOLLEYBALL:   {"win": 186, "total": 238, "handicap": 237},
    BASKETBALL:         {"win": 219, "total": 225, "handicap": 223},
    CYBER_BASKETBALL:   {"win": 219, "total": 225, "handicap": 223},
}


def _slug(text: str) -> str:
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class SportbetApiParser:
    bk_id = "sportbet"

    URL = "https://bthm-server.sportbet.ru/events.table?status=live&lang=ru&isTime=true"

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ru-RU,ru;q=0.9",
        "origin": "https://sportbet.ru",
        "referer": "https://sportbet.ru/",
        "sec-fetch-site": "same-site",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
    }

    IMPERSONATE = "chrome150"
    POLL_INTERVAL = 0.5

    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        self.detector = detector
        self.aggregator = aggregator
        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL,
        ]
        self.is_running = False

        # Кэши (те же имена, что ждёт global_cache_cleaner)
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self._last_update_time: Dict[str, float] = {}

        self._session: Optional[AsyncSession] = None

    # ============================================================
    # Session
    # ============================================================
    async def _ensure_session(self):
        if self._session is not None:
            return
        self._session = AsyncSession(
            impersonate=self.IMPERSONATE,
            timeout=30,
            headers=self.HEADERS,
        )

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
        logger.info(f"[{self.bk_id}] 🚀 HTTP-парсер Sportbet запущен (без Playwright)")

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
    # Один опрос HTTP
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

        self._process_http(data)
        await self._try_send_matches()

    def _process_http(self, data: dict):
        inner = data.get("data") if isinstance(data, dict) else None
        if not isinstance(inner, dict):
            return

        sports = inner.get("sports", [])
        if not isinstance(sports, list):
            return

        for sport in sports:
            if not isinstance(sport, dict):
                continue
            sport_key = self._resolve_sport_key_from_sport(sport)
            if not sport_key:
                continue

            for tournament in sport.get("tournaments", []):
                if not isinstance(tournament, dict):
                    continue
                tournament_name = tournament.get("name", "Неизвестно")
                category = tournament.get("category", {}) or {}
                category_name = category.get("name", "")
                full_tournament = f"{category_name}. {tournament_name}".strip(". ")

                for event in tournament.get("events", []):
                    if not isinstance(event, dict):
                        continue
                    self._process_event(
                        event, sport_key, full_tournament,
                        category_name, tournament_name,
                    )

    def _resolve_sport_key_from_sport(self, sport: dict) -> Optional[str]:
        sid = sport.get("id")
        slug = sport.get("slug", "")

        for key in self.enabled_sports:
            cfg = get_sport_config(self.bk_id, key)
            if not cfg:
                continue
            if sid in cfg.get("ids", []):
                return key
            if slug and (slug == cfg.get("url_slug") or slug in cfg.get("aliases", [])):
                return key
        return None

    def _process_event(self, event: dict, sport_key: str,
                       tournament: str, category_name: str,
                       tournament_short: str):
        event_id = str(event.get("id", ""))
        if not event_id:
            return

        teams = event.get("teams", {}) or {}
        team1 = teams.get("team1", {}).get("name", "Неизвестно")
        team2 = teams.get("team2", {}).get("name", "Неизвестно")

        if event_id not in self._matches_cache:
            self._matches_cache[event_id] = {"_last_sent": None}
            self._first_seen[event_id] = time.time()

        cache = self._matches_cache[event_id]
        cache["player1"] = team1
        cache["player2"] = team2
        cache["tournament"] = tournament
        cache["country"] = category_name
        cache["league"] = tournament_short
        cache["sport"] = sport_key
        cache["match_status"] = event.get("matchStatus", "") or ""

        self._parse_score(event_id, event.get("score", ""),
                          event.get("scores", ""), sport_key)

        markets = event.get("markets", [])
        if markets:
            self._parse_odds_from_markets(event_id, markets, sport_key)

        self._last_update_time[event_id] = time.time()

    # ============================================================
    # Score / odds (копия из старого парсера)
    # ============================================================
    def _parse_score(self, event_id: str, score_str: str,
                     scores_str: str, sport_key: str):
        cache = self._matches_cache.get(event_id)
        if not cache:
            return

        if score_str:
            try:
                s1, s2 = map(int, str(score_str).split(":"))
                cache["score1"] = s1
                cache["score2"] = s2
            except ValueError:
                pass

        sub1 = sub2 = 0
        phase_num = 1
        if scores_str:
            parts = str(scores_str).split()
            if parts:
                last = parts[-1]
                if ":" in last:
                    try:
                        sub1, sub2 = map(int, last.split(":"))
                    except ValueError:
                        pass
                phase_num = len(parts)

        cache["sub1"] = sub1
        cache["sub2"] = sub2

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            cache["phase_num"] = phase_num
        else:
            s1 = cache.get("score1", 0) or 0
            s2 = cache.get("score2", 0) or 0
            cache["phase_num"] = s1 + s2 + 1

    def _parse_odds_from_markets(self, event_id: str, markets: list, sport_key: str):
        cache = self._matches_cache.get(event_id)
        if not cache:
            return

        codes = MARKET_CODES.get(sport_key, {})
        win_id = codes.get("win")
        total_id = codes.get("total")
        handicap_id = codes.get("handicap")

        for market in markets:
            if market.get("status") != "active":
                continue
            market_id = market.get("id")
            outcomes = market.get("outcomes", []) or []

            if market_id == win_id:
                for out in outcomes:
                    if out.get("active") is False:
                        continue
                    name = out.get("name", "")
                    odd = out.get("odd", 0.0)
                    if name == "Поб 1":
                        cache["odds1"] = odd
                    elif name == "Поб 2":
                        cache["odds2"] = odd

            elif market_id == total_id:
                for out in outcomes:
                    if out.get("active") is False:
                        continue
                    odd = out.get("odd", 0.0)
                    spec = out.get("specifiers", "")
                    if spec and "total=" in spec:
                        try:
                            line_str = spec.split("total=")[1].split("&")[0]
                            cache["total_line"] = float(line_str)
                        except (ValueError, IndexError):
                            pass
                    name = out.get("name", "")
                    if "ТБ" in name or "Больше" in name:
                        cache["total_over"] = odd
                    elif "ТМ" in name or "Меньше" in name:
                        cache["total_under"] = odd

            elif market_id == handicap_id:
                for out in outcomes:
                    if out.get("active") is False:
                        continue
                    odd = out.get("odd", 0.0)
                    full_name = out.get("fullName", "")
                    m = re.search(r"\(([+-]?\d+\.?\d*)\)", full_name)
                    if not m:
                        continue
                    try:
                        line = float(m.group(1))
                    except ValueError:
                        continue
                    name = out.get("name", "")
                    if "Фора 1" in name:
                        cache["handicap1"] = line
                        cache["handicap_odds1"] = odd
                    elif "Фора 2" in name:
                        cache["handicap2"] = line
                        cache["handicap_odds2"] = odd

    # ============================================================
    # Отправка
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get("player1") or m["player1"] == "Неизвестно":
                continue
            if not m.get("player2") or m["player2"] == "Неизвестно":
                continue

            sport_key = m.get("sport", TABLE_TENNIS)

            first_seen = self._first_seen.get(match_id, current_time)
            if m.get("odds1", 0) == 0 and m.get("odds2", 0) == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
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

            phase_num = m.get("phase_num", 1) or 1
            phase_name = format_phase(sport_key, phase_num)

            slug = get_url_slug(self.bk_id, sport_key) or "table-tennis"
            country_slug = _slug(m.get("country", "")) or "x"
            league_slug = _slug(m.get("league", "")) or "x"
            p1_slug = _slug(m.get("player1", ""))
            p2_slug = _slug(m.get("player2", ""))
            match_url = (
                f"https://sportbet.ru/live/{slug}/"
                f"{country_slug}--{league_slug}/"
                f"{p1_slug}-vs-{p2_slug}--{match_id}"
                f"?isTime=1&h=all&page=main"
            )

            match = Match(
                bk_id=self.bk_id,
                match_id=match_id,
                player1=m["player1"],
                player2=m["player2"],
                score1=m.get("score1", 0),
                score2=m.get("score2", 0),
                sub_score1=m.get("sub1", 0),
                sub_score2=m.get("sub2", 0),
                tournament=m.get("tournament", "Sportbet"),
                odds1=m.get("odds1", 0.0),
                odds2=m.get("odds2", 0.0),
                total_line=m.get("total_line", 0.0),
                total_over=m.get("total_over", 0.0),
                total_under=m.get("total_under", 0.0),
                handicap1=m.get("handicap1", 0.0),
                handicap2=m.get("handicap2", 0.0),
                handicap_odds1=m.get("handicap_odds1", 0.0),
                handicap_odds2=m.get("handicap_odds2", 0.0),
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