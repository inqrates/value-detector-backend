# parsers/leon_api.py
import asyncio
import time
import json
import re
import logging
from typing import Dict, List, Optional
from playwright.async_api import Response
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME

logger = logging.getLogger(__name__)


def _slug(text: str) -> str:
    """Простой слагификатор для URL Leon."""
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class LeonApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('leon', detector=detector, aggregator=aggregator)
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

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехватчик активен")
            asyncio.create_task(self._process_queues())

    async def _handle_response(self, response: Response):
        url = response.url
        try:
            if 'api-2/betline/changes/inplayupcoming' in url and 'tabletennis' in url:
                data = await response.json()
                await self._data_queue.put(('http', data))
                logger.debug(f"[{self.bk_id}] 📥 Перехвачен ответ Leon API")
        except Exception as e:
            logger.warning(f"[{self.bk_id}] Ошибка парсинга HTTP {url}: {e}")

    def _process_http(self, data: dict):
        if not data or 'data' not in data or not isinstance(data['data'], list):
            return

        for m in data['data']:
            match_id = str(m.get('id', ''))
            if not match_id:
                continue

            if match_id not in self._matches_cache:
                self._matches_cache[match_id] = {
                    '_last_sent': None,
                }
                self._first_seen[match_id] = time.time()

            tournament = m.get('league', {}).get('name', 'Неизвестно')
            country = m.get('league', {}).get('country', {}).get('name', 'international')  # <-- ДОБАВИТЬ

            p1 = p2 = "Неизвестно"
            for comp in m.get('competitors', []):
                if comp.get('homeAway') == 'HOME':
                    p1 = comp.get('name', 'Неизвестно')
                elif comp.get('homeAway') == 'AWAY':
                    p2 = comp.get('name', 'Неизвестно')

            score_str = m.get('liveStatus', {}).get('score', '0:0').replace('*', '').strip()
            if score_str == '-:-' or not score_str:
                score1 = score2 = 0
            else:
                try:
                    score1, score2 = map(int, score_str.split(':'))
                except:
                    score1 = score2 = 0

            set_scores_str = m.get('liveStatus', {}).get('setScores', '')
            sub1 = sub2 = 0
            if set_scores_str:
                sets = [s.strip() for s in set_scores_str.split(';') if s.strip()]
                if sets:
                    last_set = sets[-1]
                    try:
                        sub1, sub2 = map(int, last_set.split(':'))
                    except:
                        pass

            odds1 = odds2 = total_line = total_over = total_under = 0.0
            handicap1 = handicap2 = handicap_odds1 = handicap_odds2 = 0.0

            for market in m.get('markets', []):
                m_name = market.get('name', '')
                runners = market.get('runners', [])

                if m_name == 'Победитель':
                    for r in runners:
                        if r.get('name') == '1':
                            odds1 = float(r.get('price', 0) or 0)
                        elif r.get('name') == '2':
                            odds2 = float(r.get('price', 0) or 0)

                elif m_name == 'Тотал очков':
                    total_line = float(market.get('handicap', 0) or 0)
                    for r in runners:
                        r_name = r.get('name', '')
                        price = float(r.get('price', 0) or 0)
                        if 'Меньше' in r_name:
                            total_under = price
                        elif 'Больше' in r_name:
                            total_over = price

                elif m_name == 'Фора':
                    for r in runners:
                        r_name = r.get('name', '')
                        price = float(r.get('price', 0) or 0)
                        match_re = re.search(r'([12])\s*\(([+-]?\d+\.?\d*)\)', r_name)
                        if match_re:
                            player = match_re.group(1)
                            h_line = float(match_re.group(2))
                            if player == '1':
                                handicap1 = h_line
                                handicap_odds1 = price
                            elif player == '2':
                                handicap2 = h_line
                                handicap_odds2 = price

            self._matches_cache[match_id].update({
                'match_id': match_id,
                'tournament': tournament,
                'country': country,                                   # <-- ДОБАВИТЬ
                'player1': p1,
                'player2': p2,
                'score1': score1,
                'score2': score2,
                'sub1': sub1,
                'sub2': sub2,
                'odds1': odds1,
                'odds2': odds2,
                'total_line': total_line,
                'total_over': total_over,
                'total_under': total_under,
                'handicap1': handicap1,
                'handicap2': handicap2,
                'handicap_odds1': handicap_odds1,
                'handicap_odds2': handicap_odds2,
                'status': m.get('liveStatus', {}).get('stage', '')
            })

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
            if not m.get('player1') or not m.get('player2') or m.get('player1') == 'Неизвестно':
                continue

            first_seen = self._first_seen.get(match_id, current_time)
            if m['odds1'] == 0 and m['odds2'] == 0 and current_time - first_seen < 30:
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

            # ---------- Сборка URL матча ----------  <-- ДОБАВИТЬ блок
            country_slug = _slug(m.get('country', 'international'))
            tour_slug = _slug(m.get('tournament', ''))
            p1_slug = _slug(m.get('player1', ''))
            p2_slug = _slug(m.get('player2', ''))
            match_url = (
                f"https://leon.ru/bets/table-tennis/{country_slug}/"
                f"{tour_slug}/{match_id}-{p1_slug}-{p2_slug}"
            )

            match = Match(
                bk_id='leon',
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
                match_url=match_url,                                  # <-- ДОБАВИТЬ
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

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено обновлений: {sent}, всего в кеше: {len(self._matches_cache)}")

    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Leon запущен")
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