import asyncio
import time
import logging
from typing import Dict, List
from playwright.async_api import Response
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME

logger = logging.getLogger(__name__)

class BetcityApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('betcity', detector=detector, aggregator=aggregator)
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
            if 'ad.betcity.ru' in url and 'on_air/bets' in url:
                data = await response.json()
                await self._data_queue.put(('http', data))
                logger.debug(f"[{self.bk_id}] 📥 Перехвачен ответ Betcity API")
        except Exception as e:
            logger.warning(f"[{self.bk_id}] Ошибка парсинга HTTP {url}: {e}")

    def _process_http(self, data: dict):
        reply = data.get('reply', {})
        sports = reply.get('sports', {})

        tt_sport = sports.get('46')
        if not tt_sport:
            return

        championships = tt_sport.get('chmps', {})

        for chmp_id, chmp_data in championships.items():
            tournament_name = chmp_data.get('name_ch', 'Неизвестно')
            events = chmp_data.get('evts', {})

            for ev_id, ev_data in events.items():
                match_id = str(ev_id)

                if match_id not in self._matches_cache:
                    self._matches_cache[match_id] = {
                        'tournament': tournament_name,
                        'champ_id': str(chmp_id),                    # <-- ДОБАВЛЕНО
                        'player1': 'Неизвестно',
                        'player2': 'Неизвестно',
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
                cache['tournament'] = tournament_name
                cache['champ_id'] = str(chmp_id)                    # <-- ДОБАВЛЕНО
                                                                    # (на случай смены чемпионата)

                cache['player1'] = ev_data.get('name_ht', 'Неизвестно')
                cache['player2'] = ev_data.get('name_at', 'Неизвестно')

                score_str = ev_data.get('sc_ev', '0:0')
                try:
                    s1, s2 = map(int, score_str.split(':'))
                except ValueError:
                    s1, s2 = 0, 0
                cache['score1'] = s1
                cache['score2'] = s2

                sets_str = ev_data.get('sc_inter', '0:0')
                sub1, sub2 = 0, 0
                if sets_str:
                    last_set = sets_str.split(',')[-1].strip()
                    try:
                        sub1, sub2 = map(int, last_set.split(':'))
                    except ValueError:
                        pass
                cache['sub1'] = sub1
                cache['sub2'] = sub2

                odds1 = odds2 = total_line = total_over = total_under = 0.0
                h1 = h2 = h_o1 = h_o2 = 0.0

                main_markets = ev_data.get('main', {})

                if '69' in main_markets:
                    wm = main_markets['69'].get('data', {}).get(match_id, {}).get('blocks', {}).get('Wm', {})
                    odds1 = float(wm.get('P1', {}).get('kf', 0) or 0)
                    odds2 = float(wm.get('P2', {}).get('kf', 0) or 0)

                if '72' in main_markets:
                    t1m = main_markets['72'].get('data', {}).get(match_id, {}).get('blocks', {}).get('T1m', {})
                    total_line = float(t1m.get('Tot', 0) or 0)
                    total_under = float(t1m.get('Tm', {}).get('kf', 0) or 0)
                    total_over = float(t1m.get('Tb', {}).get('kf', 0) or 0)

                if '71' in main_markets:
                    f1m = main_markets['71'].get('data', {}).get(match_id, {}).get('blocks', {}).get('F1m', {})
                    h1 = float(f1m.get('F1', 0) or 0)
                    h_o1 = float(f1m.get('Kf_F1', {}).get('kf', 0) or 0)
                    h2 = float(f1m.get('F2', 0) or 0)
                    h_o2 = float(f1m.get('Kf_F2', {}).get('kf', 0) or 0)

                cache['odds1'] = odds1
                cache['odds2'] = odds2
                cache['total_line'] = total_line
                cache['total_over'] = total_over
                cache['total_under'] = total_under
                cache['handicap1'] = h1
                cache['handicap2'] = h2
                cache['handicap_odds1'] = h_o1
                cache['handicap_odds2'] = h_o2

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

            if m.get('odds1', 0) == 0 or m.get('odds2', 0) == 0:
                continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (m.get('score1', 0), m.get('score2', 0),
                             m.get('sub1', 0), m.get('sub2', 0),
                             m.get('odds1', 0.0), m.get('odds2', 0.0),
                             m.get('total_line', 0.0), m.get('total_over', 0.0),
                             m.get('total_under', 0.0), m.get('handicap1', 0.0),
                             m.get('handicap2', 0.0), m.get('handicap_odds1', 0.0),
                             m.get('handicap_odds2', 0.0))
            if m.get('_last_sent') == current_state:
                continue

            # ----- Формируем URL на матч -----  # <-- ДОБАВЛЕНО
            champ_id = m.get('champ_id')
            if champ_id:
                match_url = f"https://betcity.ru/ru/live/table-tennis/{champ_id}/{match_id}"
            else:
                # Fallback: короткий URL, betcity сам найдёт по id
                match_url = f"https://betcity.ru/ru/live/table-tennis/{match_id}"

            match = Match(
                bk_id='betcity',
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
                raw_time='',
                match_url=match_url,                               # <-- ДОБАВЛЕНО
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
                        f"URL: {match_url}")                       # <-- лог с URL

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено обновлений: {sent}, всего в кеше: {len(self._matches_cache)}")

    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Betcity запущен")
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