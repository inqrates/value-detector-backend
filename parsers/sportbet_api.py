# parsers/sportbet_api.py
"""
Sportbet API-парсер с поддержкой мультиспорта (НТ / волейбол / баскетбол).

Перехват: HTTP `events.table` + WS `table:update`.
HTTP-снапшот `events.table?status=live&lang=ru&isTime=true` содержит ВСЕ виды спорта:
  - sport.id = 20 / slug = "table-tennis"
  - sport.id = 23 / slug = "volleyball"
  - sport.id = 2  / slug = "basketball"

Формат события:
  event.score   — счёт партий (НТ, волейбол) или общий счёт (баскетбол)
  event.scores  — очки по партиям/сетам/четвертям, последняя = активная фаза
  event.matchStatus — строка фазы ("4-й сет", "2-я четверть", "Перерыв")
                      Используем только как индикатор перерыва (см. _parse_score).

Маркеты:
  НТ/волейбол:  186=Победитель, 238=Тотал, 237=Фора
  Баскетбол:    219=Победитель, 225=Тотал, 223=Фора
"""
import asyncio
import time
import json
import re
import logging
from typing import Dict, List, Optional
from playwright.async_api import Response, WebSocket
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME, SPORT_URLS
from core.sport_map import (
    SPORT_MAP, get_sport_config, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL, BEACH_VOLLEYBALL,
)

logger = logging.getLogger(__name__)


# ============================================================
# Коды маркетов по видам спорта (на основе дампа Sportbet)
# ============================================================
MARKET_CODES = {
    TABLE_TENNIS:       {"win": 186, "total": 238, "handicap": 237},
    VOLLEYBALL:         {"win": 186, "total": 238, "handicap": 237},
    BEACH_VOLLEYBALL:   {"win": 186, "total": 238, "handicap": 237},
    BASKETBALL:         {"win": 219, "total": 225, "handicap": 223},
    CYBER_BASKETBALL:   {"win": 219, "total": 225, "handicap": 223},
}


# ============================================================
# Хелпер слагификации
# ============================================================
def _slug(text: str) -> str:
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class SportbetApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('sportbet', detector=detector, aggregator=aggregator)

        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL,
        ]

        # Sportbet отдаёт все виды с одного URL — открываем общий лайв
        self.url = SPORT_URLS.get("_all", {}).get("sportbet", self.url)
        logger.info(f"[{self.bk_id}] Используем общий лайв: {self.url}")

        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False

    # ============================================================
    # Запуск и перехват
    # ============================================================
    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            self.page.on("response", self._handle_response)
            self.page.on("websocket", self._handle_websocket)

            logger.info(f"[{self.bk_id}] Загрузка страницы {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded',
                                 timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)

            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            await self.page.evaluate("window.scrollTo(0, 0)")

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехватчики активны")
            asyncio.create_task(self._process_queues())

    async def _handle_response(self, response: Response):
        url = response.url
        try:
            if 'events.sidebar' in url or 'events.table' in url:
                data = await response.json()
                await self._data_queue.put(('http', data))
                logger.debug(f"[{self.bk_id}] 📥 Перехвачен {url.split('?')[0]}")
        except Exception as e:
            logger.warning(f"[{self.bk_id}] Ошибка парсинга HTTP {url}: {e}")

    async def _handle_websocket(self, ws: WebSocket):
        if 'bthm-server.sportbet.ru' in ws.url or 'wss' in ws.url:
            logger.info(f"[{self.bk_id}] 🔌 WebSocket подключён")
            ws.on("framereceived", self._on_ws_frame)

    def _on_ws_frame(self, payload):
        try:
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            if not payload or len(payload) < 20:
                return

            if payload.startswith('42') and ',' in payload:
                comma_pos = payload.find(',')
                if comma_pos != -1:
                    json_part = payload[comma_pos + 1:].strip()
                    if json_part.startswith('['):
                        data = json.loads(json_part)
                        if isinstance(data, list) and len(data) == 2 and data[0] == "table:update":
                            asyncio.create_task(self._data_queue.put(('ws', data[1])))
                            return
            try:
                json_part = payload.split(',', 1)[1] if ',' in payload else payload
                data = json.loads(json_part)
                asyncio.create_task(self._data_queue.put(('ws', data)))
            except (json.JSONDecodeError, IndexError):
                pass
        except Exception as e:
            logger.error(f"[{self.bk_id}] Ошибка WS-фрейма: {e}", exc_info=True)

    async def _process_queues(self):
        while self.is_running:
            try:
                msg_type, data = await asyncio.wait_for(
                    self._data_queue.get(), timeout=1.0
                )
                if msg_type == 'http':
                    self._process_http(data)
                elif msg_type == 'ws':
                    self._process_ws(data)
                await self._try_send_matches()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка обработки очереди: {e}", exc_info=True)

    # ============================================================
    # HTTP-снапшот
    # ============================================================
    def _resolve_sport_key_from_sport(self, sport: dict) -> Optional[str]:
        """Определить вид спорта по sport.id или sport.slug из ответа API."""
        sid = sport.get('id')
        slug = sport.get('slug', '')
        for key in self.enabled_sports:
            cfg = get_sport_config(self.bk_id, key)
            if not cfg:
                continue
            if sid in cfg.get('ids', []):
                return key
            if slug and (slug == cfg.get('url_slug') or slug in cfg.get('aliases', [])):
                return key
        return None

    def _process_http(self, data: dict):
        sports = data.get('data', {}).get('sports', [])
        if not sports:
            return

        for sport in sports:
            sport_key = self._resolve_sport_key_from_sport(sport)
            if not sport_key:
                continue
            for tournament in sport.get('tournaments', []):
                tournament_name = tournament.get('name', 'Неизвестно')
                category = tournament.get('category', {})
                category_name = category.get('name', '')
                full_tournament = f"{category_name}. {tournament_name}".strip('. ')
                for event in tournament.get('events', []):
                    self._process_event(
                        event, sport_key, full_tournament,
                        category_name, tournament_name,
                    )

    def _process_event(self, event: dict, sport_key: str,
                       tournament: str, category_name: str,
                       tournament_short: str):
        event_id = str(event.get('id', ''))
        if not event_id:
            return

        teams = event.get('teams', {})
        team1 = teams.get('team1', {}).get('name', 'Неизвестно')
        team2 = teams.get('team2', {}).get('name', 'Неизвестно')

        if event_id not in self._matches_cache:
            self._matches_cache[event_id] = {'_last_sent': None}
            self._first_seen[event_id] = time.time()

        cache = self._matches_cache[event_id]
        cache['player1'] = team1
        cache['player2'] = team2
        cache['tournament'] = tournament
        cache['country'] = category_name
        cache['league'] = tournament_short
        cache['sport'] = sport_key
        cache['match_status'] = event.get('matchStatus', '') or ''

        self._parse_score(event_id, event.get('score', ''),
                          event.get('scores', ''), sport_key)

        markets = event.get('markets', [])
        if markets:
            self._parse_odds_from_markets(event_id, markets, sport_key)

    # ============================================================
    # Score / odds
    # ============================================================
    def _parse_score(self, event_id: str, score_str: str,
                     scores_str: str, sport_key: str):
        cache = self._matches_cache.get(event_id)
        if not cache:
            return

        # score — счёт партий/сетов (НТ, волейбол) или общий счёт (баскетбол)
        if score_str:
            try:
                s1, s2 = map(int, score_str.split(':'))
                cache['score1'] = s1
                cache['score2'] = s2
            except ValueError:
                pass

        # scores — очки по партиям/сетам/четвертям, последняя часть = активная
        sub1 = sub2 = 0
        phase_num = 1
        if scores_str:
            parts = scores_str.split()
            if parts:
                last = parts[-1]
                if ':' in last:
                    try:
                        sub1, sub2 = map(int, last.split(':'))
                    except ValueError:
                        pass
                phase_num = len(parts)

        cache['sub1'] = sub1
        cache['sub2'] = sub2

        # phase_num:
        # - для НТ/волейбола — сумма партий/сетов + 1 (активная)
        # - для баскетбола — номер активной четверти = len(scores)
        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            cache['phase_num'] = phase_num
        else:
            s1 = cache.get('score1', 0) or 0
            s2 = cache.get('score2', 0) or 0
            cache['phase_num'] = s1 + s2 + 1

    def _parse_odds_from_markets(self, event_id: str, markets: list, sport_key: str):
        cache = self._matches_cache.get(event_id)
        if not cache:
            return

        codes = MARKET_CODES.get(sport_key, {})
        win_id = codes.get('win')
        total_id = codes.get('total')
        handicap_id = codes.get('handicap')

        for market in markets:
            if market.get('status') != 'active':
                continue
            market_id = market.get('id')
            outcomes = market.get('outcomes', [])

            if market_id == win_id:
                for out in outcomes:
                    if out.get('active') is False:
                        continue
                    name = out.get('name', '')
                    odd = out.get('odd', 0.0)
                    if name == 'Поб 1':
                        cache['odds1'] = odd
                    elif name == 'Поб 2':
                        cache['odds2'] = odd

            elif market_id == total_id:
                for out in outcomes:
                    if out.get('active') is False:
                        continue
                    odd = out.get('odd', 0.0)
                    spec = out.get('specifiers', '')
                    if 'total=' in spec:
                        try:
                            line_str = spec.split('total=')[1].split('&')[0]
                            cache['total_line'] = float(line_str)
                        except (ValueError, IndexError):
                            pass
                    name = out.get('name', '')
                    if 'ТБ' in name or 'Больше' in name:
                        cache['total_over'] = odd
                    elif 'ТМ' in name or 'Меньше' in name:
                        cache['total_under'] = odd

            elif market_id == handicap_id:
                for out in outcomes:
                    if out.get('active') is False:
                        continue
                    odd = out.get('odd', 0.0)
                    full_name = out.get('fullName', '')
                    m = re.search(r'\(([+-]?\d+\.?\d*)\)', full_name)
                    if not m:
                        continue
                    try:
                        line = float(m.group(1))
                    except ValueError:
                        continue
                    name = out.get('name', '')
                    if 'Фора 1' in name:
                        cache['handicap1'] = line
                        cache['handicap_odds1'] = odd
                    elif 'Фора 2' in name:
                        cache['handicap2'] = line
                        cache['handicap_odds2'] = odd

    # ============================================================
    # WebSocket — обновляем только уже известные матчи
    # ============================================================
    def _process_ws(self, data):
        if not isinstance(data, dict):
            return
        events = data.get('events', [])
        if not events:
            return

        for event in events:
            event_id = str(event.get('id', ''))
            if not event_id:
                continue
            # Ждём HTTP-снапшот, который проставит sport и team names
            if event_id not in self._matches_cache:
                continue
            cache = self._matches_cache[event_id]
            sport_key = cache.get('sport', TABLE_TENNIS)

            self._parse_score(event_id, event.get('score', ''),
                              event.get('scores', ''), sport_key)
            markets = event.get('markets', [])
            if markets:
                self._parse_odds_from_markets(event_id, markets, sport_key)

            status = event.get('matchStatus', '')
            if status:
                cache['match_status'] = status

    # ============================================================
    # Отправка в detector
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get('player1') or m.get('player1') == 'Неизвестно':
                continue
            if not m.get('player2') or m.get('player2') == 'Неизвестно':
                continue

            sport_key = m.get('sport', TABLE_TENNIS)

            first_seen = self._first_seen.get(match_id, current_time)
            if m.get('odds1', 0) == 0 and m.get('odds2', 0) == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (
                m.get('score1', 0), m.get('score2', 0),
                m.get('sub1', 0), m.get('sub2', 0),
                m.get('phase_num', 0),
                m.get('odds1', 0.0), m.get('odds2', 0.0),
                m.get('total_line', 0.0), m.get('total_over', 0.0),
                m.get('total_under', 0.0),
                m.get('handicap1', 0.0), m.get('handicap2', 0.0),
                m.get('handicap_odds1', 0.0), m.get('handicap_odds2', 0.0),
            )
            if m.get('_last_sent') == current_state:
                continue

            phase_num = m.get('phase_num', 1) or 1
            phase_name = format_phase(sport_key, phase_num)

            # URL страницы матча
            slug = get_url_slug(self.bk_id, sport_key) or 'table-tennis'
            country_slug = _slug(m.get('country', '')) or 'x'
            league_slug = _slug(m.get('league', '')) or 'x'
            p1_slug = _slug(m.get('player1', ''))
            p2_slug = _slug(m.get('player2', ''))
            match_url = (
                f"https://sportbet.ru/live/{slug}/"
                f"{country_slug}--{league_slug}/"
                f"{p1_slug}-vs-{p2_slug}--{match_id}"
                f"?isTime=1&h=all&page=main"
            )

            match = Match(
                bk_id='sportbet',
                match_id=match_id,
                player1=m['player1'],
                player2=m['player2'],
                score1=m.get('score1', 0),
                score2=m.get('score2', 0),
                sub_score1=m.get('sub1', 0),
                sub_score2=m.get('sub2', 0),
                tournament=m.get('tournament', 'Sportbet'),
                odds1=m.get('odds1', 0.0),
                odds2=m.get('odds2', 0.0),
                total_line=m.get('total_line', 0.0),
                total_over=m.get('total_over', 0.0),
                total_under=m.get('total_under', 0.0),
                handicap1=m.get('handicap1', 0.0),
                handicap2=m.get('handicap2', 0.0),
                handicap_odds1=m.get('handicap_odds1', 0.0),
                handicap_odds2=m.get('handicap_odds2', 0.0),
                timestamp=current_time,
                raw_time=phase_name,
                sport=sport_key,
                match_url=match_url,
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            m['_last_sent'] = current_state
            self._last_sent_time[match_id] = current_time
            sent += 1

            logger.info(
                f"[{self.bk_id}] 🟢 [{sport_key}] {match.player1} vs {match.player2} | "
                f"матч {match.score1}:{match.score2} | "
                f"{phase_name} {match.sub_score1}:{match.sub_score2} | "
                f"К: {match.odds1}/{match.odds2}"
            )

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено: {sent} (в кеше: {len(self._matches_cache)})")

    # ============================================================
    # Loop
    # ============================================================
    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Sportbet запущен")
        await self.start()

        while self.is_running:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.bk_id}] Критическая ошибка: {e}", exc_info=True)
                await self.stop()
                await asyncio.sleep(5)

    async def stop(self):
        self.is_running = False
        if self.page and not self.page.is_closed():
            try:
                self.page.remove_listener("response", self._handle_response)
                self.page.remove_listener("websocket", self._handle_websocket)
            except Exception:
                pass
            await browser_manager.close_page(self.page)
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")