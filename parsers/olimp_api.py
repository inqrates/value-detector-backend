# parsers/olimp_api.py
import asyncio
import time
import logging
import re
from typing import Dict, List, Optional
from playwright.async_api import Response
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME

logger = logging.getLogger(__name__)


def _slug(text: str) -> str:
    """Простой слагификатор для URL. Русские буквы Playwright закодирует сам."""
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class OlimpApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('olimp', detector=detector, aggregator=aggregator)
        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False

    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()

            self.page.on("response", self._handle_response)

            logger.info(f"[{self.bk_id}] Загрузка страницы {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)

            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            await self.page.evaluate("window.scrollTo(0, 0)")

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехватчик API активен")
            asyncio.create_task(self._process_queues())

    async def _handle_response(self, response: Response):
        url = response.url
        try:
            if 'api/v4/0/live' in url and 'sports-with-competitions-with-events' in url:
                data = await response.json()
                await self._data_queue.put(('http', data))
                logger.debug(f"[{self.bk_id}] 📥 Перехвачен ответ Olimp API")
        except Exception as e:
            logger.warning(f"[{self.bk_id}] Ошибка парсинга HTTP {url}: {e}")

    def _process_http(self, data: dict):
        if not data or not isinstance(data, list) or not data:
            return

        first = data[0]
        if not isinstance(first, dict):
            return

        payload = first.get('payload')
        if not payload or not isinstance(payload, dict):
            return

        competitions_with_events = payload.get('competitionsWithEvents')
        if not competitions_with_events or not isinstance(competitions_with_events, list):
            return

        for competition_block in competitions_with_events:
            if not isinstance(competition_block, dict):
                continue

            competition = competition_block.get('competition', {})
            tournament = competition.get('name', 'Неизвестно')
            # <-- ДОБАВЛЕНО: попытка вытащить id турнира для URL
            tournament_id = (
                competition.get('id')
                or competition.get('tournamentId')
                or competition.get('competitionId')
            )

            events = competition_block.get('events', [])
            if not isinstance(events, list):
                continue

            for event in events:
                if not isinstance(event, dict):
                    continue

                if event.get('sportId') != '40':
                    continue

                match_id = str(event.get('id', ''))
                if not match_id:
                    continue

                if match_id not in self._matches_cache:
                    self._matches_cache[match_id] = {
                        'player1': 'Неизвестно',
                        'player2': 'Неизвестно',
                        'tournament': tournament,
                        'tournament_id': tournament_id,       # <-- ДОБАВЛЕНО
                        'score1': 0, 'score2': 0,
                        'sub1': 0, 'sub2': 0,
                        'odds1': 0.0, 'odds2': 0.0,
                        'total_line': 0.0, 'total_over': 0.0, 'total_under': 0.0,
                        'handicap1': 0.0, 'handicap2': 0.0,
                        'handicap_odds1': 0.0, 'handicap_odds2': 0.0,
                        '_last_sent': None,
                    }
                    self._first_seen[match_id] = time.time()

                cache = self._matches_cache[match_id]

                cache['player1'] = event.get('team1Name', 'Неизвестно')
                cache['player2'] = event.get('team2Name', 'Неизвестно')
                cache['tournament'] = tournament
                if tournament_id:
                    cache['tournament_id'] = tournament_id   # <-- ДОБАВЛЕНО

                # <-- ДОБАВЛЕНО: дата матча (если есть — используем в URL)
                start_date = (
                    event.get('startDate')
                    or event.get('date')
                    or event.get('startTime')
                )
                if start_date:
                    cache['start_date'] = start_date

                score_str = event.get('score', '0:0')
                try:
                    s1, s2 = map(int, score_str.split(':'))
                except Exception:
                    s1, s2 = 0, 0
                cache['score1'] = s1
                cache['score2'] = s2

                comment = event.get('comment', '')
                cache['comment'] = comment
                sub1 = sub2 = 0
                if comment:
                    matches = re.findall(r'(\d+)[:*](\d+)', comment)
                    if matches:
                        last = matches[-1]
                        try:
                            sub1 = int(last[0])
                            sub2 = int(last[1])
                        except Exception:
                            pass
                cache['sub1'] = sub1
                cache['sub2'] = sub2

                odds1 = odds2 = 0.0
                total_line = total_over = total_under = 0.0
                handicap1 = handicap2 = handicap_odds1 = handicap_odds2 = 0.0

                outcomes = event.get('outcomes', [])
                for out in outcomes:
                    if not isinstance(out, dict):
                        continue

                    table_type = out.get('tableType', '')
                    short_name = out.get('shortName', '')
                    probability_str = out.get('probability', '0')
                    param_str = out.get('param', '0')

                    try:
                        prob = float(probability_str.replace(',', '.'))
                    except Exception:
                        prob = 0.0
                    try:
                        param = float(param_str.replace(',', '.'))
                    except Exception:
                        param = 0.0

                    if table_type == 'RESULT':
                        if short_name == 'П1':
                            odds1 = prob
                        elif short_name == 'П2':
                            odds2 = prob
                    elif table_type == 'HANDICAP':
                        if short_name == 'Фора 1':
                            handicap1 = param
                            handicap_odds1 = prob
                        elif short_name == 'Фора 2':
                            handicap2 = param
                            handicap_odds2 = prob
                    elif table_type == 'TOTAL':
                        if short_name == 'ТотМ':
                            total_under = prob
                            total_line = param
                        elif short_name == 'ТотБ':
                            total_over = prob
                            total_line = param

                cache['odds1'] = odds1
                cache['odds2'] = odds2
                cache['total_line'] = total_line
                cache['total_over'] = total_over
                cache['total_under'] = total_under
                cache['handicap1'] = handicap1
                cache['handicap2'] = handicap2
                cache['handicap_odds1'] = handicap_odds1
                cache['handicap_odds2'] = handicap_odds2
                cache['status'] = event.get('state', '')

    async def _process_queues(self):
        while self.is_running:
            try:
                msg_type, data = await asyncio.wait_for(self._data_queue.get(), timeout=1.0)
                if msg_type == 'http':
                    self._process_http(data)
                await self._try_send_matches()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка обработки очереди: {e}")

    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get('player1') or m.get('player1') == 'Неизвестно':
                continue

            first_seen = self._first_seen.get(match_id, current_time)
            if m.get('odds1', 0) == 0 and m.get('odds2', 0) == 0 and current_time - first_seen < 30:
                continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            # Сравнение с предыдущим состоянием
            current_state = (m.get('score1', 0), m.get('score2', 0),
                             m.get('sub1', 0), m.get('sub2', 0),
                             m.get('odds1', 0.0), m.get('odds2', 0.0),
                             m.get('total_line', 0.0), m.get('total_over', 0.0),
                             m.get('total_under', 0.0), m.get('handicap1', 0.0),
                             m.get('handicap2', 0.0), m.get('handicap_odds1', 0.0),
                             m.get('handicap_odds2', 0.0))
            if m.get('_last_sent') == current_state:
                continue

            # <-- ДОБАВЛЕНО: собираем URL на матч Olimp
            # Структура: /live/nastolnyy-tennis-40/{tournament}-{tournament_id}/{players}-{date}-{match_id}
            # Если tournament_id/date отсутствуют — используем укороченный вариант x/x-{id}
            # Olimp (SPA) сам разберёт match_id из последнего сегмента.
            tour_slug = _slug(m.get('tournament', ''))
            tour_id = m.get('tournament_id', '')
            p1_slug = _slug(m.get('player1', ''))
            p2_slug = _slug(m.get('player2', ''))

            # Нормализуем дату в формат DD-MM-YYYY
            date_slug = ''
            sd = m.get('start_date')
            if sd:
                try:
                    # Пробуем ISO-формат "2026-09-09T..." → "09-09-2026"
                    if isinstance(sd, str) and len(sd) >= 10 and sd[4] == '-':
                        parts = sd[:10].split('-')
                        if len(parts) == 3:
                            date_slug = f"{parts[2]}-{parts[1]}-{parts[0]}"
                except Exception:
                    date_slug = ''

            if tour_slug and tour_id and date_slug and p1_slug and p2_slug:
                match_url = (
                    f"https://www.olimp.bet/live/nastolnyy-tennis-40/"
                    f"{tour_slug}-{tour_id}/{p1_slug}-{p2_slug}-{date_slug}-{match_id}"
                )
            else:
                # Фолбэк: Olimp редиректит по ID из хвоста URL
                match_url = (
                    f"https://www.olimp.bet/live/nastolnyy-tennis-40/x/x-{match_id}"
                )

            match = Match(
                bk_id='olimp',
                match_id=match_id,
                player1=m['player1'],
                player2=m['player2'],
                score1=m['score1'],
                score2=m['score2'],
                sub_score1=m['sub1'],
                sub_score2=m['sub2'],
                tournament=m['tournament'],
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
                raw_time=m.get('status', ''),
                match_url=match_url,                        # <-- ДОБАВЛЕНО
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
                        f"К: {match.odds1}/{match.odds2} | Т: {match.total_line} | Ф: {match.handicap1} | "
                        f"URL: {match.match_url}")

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено обновлений: {sent}, всего в кеше: {len(self._matches_cache)}")

    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Olimp запущен")
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
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")