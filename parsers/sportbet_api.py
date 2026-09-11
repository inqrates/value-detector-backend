# parsers/sportbet_api.py
import asyncio
import time
import json
import re
import logging
from typing import Dict, List, Optional, Any
from playwright.async_api import Response, WebSocket
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME

logger = logging.getLogger(__name__)


# ============================================================
# Хелпер слагификации (для сборки URL)
# ============================================================
def _slug(text: str) -> str:
    """
    Простой слагификатор. Кириллицу оставляем как есть —
    Playwright сам URL-кодирует её при goto().
    """
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class SportbetApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('sportbet', detector=detector, aggregator=aggregator)
        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False

    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            self.page.on("response", self._handle_response)
            self.page.on("websocket", self._handle_websocket)

            logger.info(f"[{self.bk_id}] Загрузка страницы {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
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
                logger.debug(f"[{self.bk_id}] 📥 Перехвачен {url.split('/')[-1]}")
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
            if not payload:
                return

            if len(payload) < 20:
                logger.debug(f"[{self.bk_id}] ⏩ Пропуск короткого WS: {payload[:50]}")
                return

            logger.debug(f"[{self.bk_id}] 📨 WS-фрейм: {payload[:300]}...")

            if payload.startswith('42'):
                if ',' in payload:
                    comma_pos = payload.find(',')
                    if comma_pos != -1:
                        json_part = payload[comma_pos + 1:].strip()
                        if json_part.startswith('['):
                            data = json.loads(json_part)
                            if isinstance(data, list) and len(data) == 2 and data[0] == "table:update":
                                asyncio.create_task(self._data_queue.put(('ws', data[1])))
                                logger.debug(f"[{self.bk_id}] ✅ Обработан WS: table:update")
                                return
                try:
                    json_part = payload.split(',', 1)[1] if ',' in payload else payload
                    data = json.loads(json_part)
                    asyncio.create_task(self._data_queue.put(('ws', data)))
                    logger.debug(f"[{self.bk_id}] ✅ Обработан WS: {type(data)}")
                except Exception as e:
                    logger.warning(f"[{self.bk_id}] Не удалось распарсить Socket.IO: {e}")
            else:
                try:
                    data = json.loads(payload)
                    asyncio.create_task(self._data_queue.put(('ws', data)))
                except json.JSONDecodeError:
                    logger.debug(f"[{self.bk_id}] WS-фрейм не JSON: {payload[:100]}")
        except Exception as e:
            logger.error(f"[{self.bk_id}] Ошибка WS-фрейма: {e}", exc_info=True)

    async def _process_queues(self):
        while self.is_running:
            try:
                msg_type, data = await asyncio.wait_for(self._data_queue.get(), timeout=1.0)
                if msg_type == 'http':
                    self._process_http(data)
                elif msg_type == 'ws':
                    self._process_ws(data)
                await self._try_send_matches()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка обработки очереди: {e}", exc_info=True)

    def _process_http(self, data: dict):
        try:
            sports = data.get('data', {}).get('sports', [])
        except AttributeError:
            return

        for sport in sports:
            if sport.get('slug') != 'table-tennis' and sport.get('id') != 20:
                continue
            tournaments = sport.get('tournaments', [])
            for tournament in tournaments:
                tournament_name = tournament.get('name', 'Неизвестно')
                category = tournament.get('category', {})
                category_name = category.get('name', '')
                full_tournament = f"{category_name}. {tournament_name}".strip('. ')
                events = tournament.get('events', [])
                for event in events:
                    event_id = str(event.get('id'))
                    if not event_id:
                        continue

                    teams = event.get('teams', {})
                    team1 = teams.get('team1', {}).get('name', 'Неизвестно')
                    team2 = teams.get('team2', {}).get('name', 'Неизвестно')

                    if event_id not in self._matches_cache:
                        self._matches_cache[event_id] = {
                            'player1': team1,
                            'player2': team2,
                            'tournament': full_tournament,
                            # ДОБАВЛЕНО: страна и лига — нужны для URL
                            'country': category_name,
                            'league': tournament_name,
                            # ──────────────────────────────────────
                            'score1': 0, 'score2': 0,
                            'sub1': 0, 'sub2': 0,
                            'odds1': 0.0, 'odds2': 0.0,
                            'total_line': 0.0, 'total_over': 0.0, 'total_under': 0.0,
                            'handicap1': 0.0, 'handicap2': 0.0,
                            'handicap_odds1': 0.0, 'handicap_odds2': 0.0,
                            'finished': False,
                            '_last_sent': None,
                        }
                        self._first_seen[event_id] = time.time()
                        logger.debug(f"[{self.bk_id}] 📝 Новый матч: {team1} vs {team2} (id={event_id})")
                    else:
                        cache = self._matches_cache[event_id]
                        cache['player1'] = team1
                        cache['player2'] = team2
                        cache['tournament'] = full_tournament
                        # ДОБАВЛЕНО: обновляем страну/лигу при каждом апдейте
                        cache['country'] = category_name
                        cache['league'] = tournament_name
                        # ──────────────────────────────────────

                    self._parse_score(event_id, event.get('score', ''), event.get('scores', ''))
                    markets = event.get('markets', [])
                    if markets:
                        self._parse_odds_from_markets(event_id, markets)

    def _parse_score(self, event_id: str, score_str: str, scores_str: str):
        cache = self._matches_cache.get(event_id)
        if not cache:
            return
        if score_str:
            try:
                s1, s2 = map(int, score_str.split(':'))
                cache['score1'] = s1
                cache['score2'] = s2
            except:
                pass
        if scores_str:
            parts = scores_str.split()
            if parts:
                last_set = parts[-1]
                if ':' in last_set:
                    try:
                        sub1, sub2 = map(int, last_set.split(':'))
                        cache['sub1'] = sub1
                        cache['sub2'] = sub2
                    except:
                        pass

    def _parse_odds_from_markets(self, event_id: str, markets: list):
        cache = self._matches_cache.get(event_id)
        if not cache:
            return

        for market in markets:
            market_id = market.get('id')
            outcomes = market.get('outcomes', [])

            if market_id == 186:  # Победитель
                for out in outcomes:
                    if out.get('active') is False:
                        continue
                    name = out.get('name', '')
                    odd = out.get('odd', 0.0)
                    if name == 'Поб 1':
                        cache['odds1'] = odd
                    elif name == 'Поб 2':
                        cache['odds2'] = odd

            elif market_id == 238:  # Тотал очков
                for out in outcomes:
                    if out.get('active') is False:
                        continue
                    odd = out.get('odd', 0.0)
                    spec = out.get('specifiers', '')
                    if 'total=' in spec:
                        line_str = spec.split('total=')[1].split('&')[0]
                        try:
                            line = float(line_str)
                            cache['total_line'] = line
                        except:
                            pass
                    name = out.get('name', '')
                    if 'ТБ' in name or 'Больше' in name:
                        cache['total_over'] = odd
                    elif 'ТМ' in name or 'Меньше' in name:
                        cache['total_under'] = odd

            elif market_id == 237:  # Фора очков
                for out in outcomes:
                    if out.get('active') is False:
                        continue
                    odd = out.get('odd', 0.0)
                    full_name = out.get('fullName', '')
                    match = re.search(r'\(([+-]?\d+\.?\d*)\)', full_name)
                    if match:
                        try:
                            line = float(match.group(1))
                        except:
                            line = 0.0
                    else:
                        continue
                    if 'Фора 1' in out.get('name', '') or 'Фора 1' in full_name:
                        cache['handicap1'] = line
                        cache['handicap_odds1'] = odd
                    elif 'Фора 2' in out.get('name', '') or 'Фора 2' in full_name:
                        cache['handicap2'] = line
                        cache['handicap_odds2'] = odd

    def _process_ws(self, data):
        """Обработка WebSocket-сообщений (обновления матчей)."""
        if not isinstance(data, dict):
            logger.debug(f"[{self.bk_id}] ⏩ Пропуск WS-данных (не dict): {type(data)}")
            return

        events = data.get('events', [])
        if not events:
            logger.debug(f"[{self.bk_id}] В WS нет events")
            return

        updated_count = 0
        for event in events:
            event_id = str(event.get('id'))
            if not event_id:
                continue

            if event_id not in self._matches_cache:
                self._matches_cache[event_id] = {
                    'player1': 'Неизвестно',
                    'player2': 'Неизвестно',
                    'tournament': 'Sportbet',
                    'country': '',                    # <-- ДОБАВЛЕНО
                    'league': '',                     # <-- ДОБАВЛЕНО
                    'score1': 0, 'score2': 0,
                    'sub1': 0, 'sub2': 0,
                    'odds1': 0.0, 'odds2': 0.0,
                    'total_line': 0.0, 'total_over': 0.0, 'total_under': 0.0,
                    'handicap1': 0.0, 'handicap2': 0.0,
                    'handicap_odds1': 0.0, 'handicap_odds2': 0.0,
                    'finished': False,
                    '_last_sent': None,
                }
                self._first_seen[event_id] = time.time()
                logger.debug(f"[{self.bk_id}] 📝 Новый матч из WS: id={event_id}")

            cache = self._matches_cache[event_id]
            tournament = event.get('tournament')
            category = event.get('category')
            if tournament and category:
                cache['tournament'] = f"{category}. {tournament}".strip('. ')
                cache['country'] = category             # <-- ДОБАВЛЕНО
                cache['league'] = tournament            # <-- ДОБАВЛЕНО
            elif tournament:
                cache['tournament'] = tournament
                cache['league'] = tournament            # <-- ДОБАВЛЕНО

            self._parse_score(event_id, event.get('score', ''), event.get('scores', ''))
            markets = event.get('markets', [])
            if markets:
                self._parse_odds_from_markets(event_id, markets)

            match_status = event.get('matchStatus', '')
            if 'заверш' in match_status.lower() or 'finished' in match_status.lower():
                cache['finished'] = True
                logger.debug(f"[{self.bk_id}] 🏁 Матч {event_id} завершён")

            updated_count += 1

        if updated_count:
            logger.debug(f"[{self.bk_id}] ✅ Обновлено {updated_count} матчей через WS")

    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        to_delete = []
        for match_id, m in self._matches_cache.items():
            if m.get('finished', False):
                if current_time - self._first_seen.get(match_id, current_time) > 60:
                    to_delete.append(match_id)
        for mid in to_delete:
            del self._matches_cache[mid]
            self._first_seen.pop(mid, None)
            self._last_sent_time.pop(mid, None)

        for match_id, m in list(self._matches_cache.items()):
            if m.get('finished', False):
                continue
            if not m.get('player1') or not m.get('player2') or m.get('player1') == 'Неизвестно':
                continue

            first_seen = self._first_seen.get(match_id, current_time)
            if m['odds1'] == 0 and m['odds2'] == 0 and current_time - first_seen < 30:
                continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (m['score1'], m['score2'], m['sub1'], m['sub2'],
                             m['odds1'], m['odds2'], m['total_line'], m['total_over'],
                             m['total_under'], m['handicap1'], m['handicap2'],
                             m['handicap_odds1'], m['handicap_odds2'])
            if m.get('_last_sent') == current_state:
                continue

            # ============================================================
            # ДОБАВЛЕНО: собираем URL для страницы матча.
            # Формат: https://sportbet.ru/live/table-tennis/{country}--{league}/{p1}-vs-{p2}--{id}?isTime=1&h=all&page=main
            # Ключ навигации — суффикс "--{id}"; слаги улучшают UX, но
            # даже если они "не совпадут" — сайт всё равно откроет матч по id.
            # ============================================================
            country_slug = _slug(m.get('country', '')) or 'x'
            league_slug = _slug(m.get('league', '')) or 'x'
            p1_slug = _slug(m.get('player1', ''))
            p2_slug = _slug(m.get('player2', ''))
            match_url = (
                f"https://sportbet.ru/live/table-tennis/"
                f"{country_slug}--{league_slug}/"
                f"{p1_slug}-vs-{p2_slug}--{match_id}"
                f"?isTime=1&h=all&page=main"
            )
            # ============================================================

            match = Match(
                bk_id='sportbet',
                match_id=match_id,
                player1=m['player1'],
                player2=m['player2'],
                score1=m['score1'],
                score2=m['score2'],
                sub_score1=m['sub1'],
                sub_score2=m['sub2'],
                tournament=m.get('tournament', 'Sportbet'),
                odds1=m['odds1'],
                odds2=m['odds2'],
                total_line=m['total_line'],
                total_over=m['total_over'],
                total_under=m['total_under'],
                handicap1=m['handicap1'],
                handicap2=m['handicap2'],
                handicap_odds1=m['handicap_odds1'],
                handicap_odds2=m['handicap_odds2'],
                timestamp=current_time,
                raw_time='',
                match_url=match_url,             # <-- ДОБАВЛЕНО
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            m['_last_sent'] = current_state
            self._last_sent_time[match_id] = current_time
            sent += 1

            logger.info(f"[{self.bk_id}] 🟢 Отправлен: {match.player1} vs {match.player2} | "
                        f"{match.score1}:{match.score2} (сет: {match.sub_score1}:{match.sub_score2}) | "
                        f"К: {match.odds1}/{match.odds2} | Т: {match.total_line} | Ф: {match.handicap1}")
            logger.debug(f"[{self.bk_id}] URL: {match_url}")

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено обновлений: {sent}, всего в кеше: {len(self._matches_cache)}")

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
                logger.error(f"[{self.bk_id}] Критическая ошибка, перезапуск: {e}", exc_info=True)
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