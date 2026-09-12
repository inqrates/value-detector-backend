# parsers/leon_api.py
"""
Leon API-парсер с поддержкой мультиспорта (НТ / волейбол / баскетбол / кибербаскет).

Особенности:
  - Один общий эндпоинт /api-2/betline/changes/inplayupcoming на /live.
  - Вид спорта определяется по полю m.league.sport.family:
      TableTennis -> table_tennis
      Volleyball  -> volleyball
      Basketball  -> basketball
  - Кибербаскетбол: family тоже Basketball, но region.family == 'ELECTRONIC_LEAGUES'.
    Определяем по этому признаку.
  - Счёт и кэфы читаются по той же логике, что и в старом парсере для НТ.

Структура (из дампа):
  liveStatus.score      = "2:1*"          — счёт партий/сетов/общий
  liveStatus.setScores  = "22:25; 20:19"  — очки по фазам, последняя = активная
  liveStatus.stage      = "2-я четверть"   — словесная фаза
  markets[].name        = "Победитель" / "Тотал очков" / "Фора"
  runners[].name        = "1" / "2" / "Меньше (...)" / "Больше (...)" / "1 (+3.5)"
"""
import asyncio
import time
import logging
import re
from typing import Dict, List, Optional
from playwright.async_api import Response
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME, SPORT_URLS
from core.sport_map import (
    SPORT_MAP, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)

logger = logging.getLogger(__name__)


# league.sport.family → sport_key
FAMILY_TO_SPORT = {
    "TableTennis": TABLE_TENNIS,
    "Volleyball":  VOLLEYBALL,
    "Basketball":  BASKETBALL,
}


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
    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('leon', detector=detector, aggregator=aggregator)

        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]

        # Общий лайв Leon
        self.url = SPORT_URLS.get("_all", {}).get("leon", self.url)
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

            logger.info(f"[{self.bk_id}] Загрузка страницы {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded',
                                 timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)

            # растягиваем body — trigger lazy load
            try:
                await self.page.evaluate(
                    "() => { document.body.style.minHeight = '10000px'; }"
                )
            except Exception:
                pass
            await asyncio.sleep(2)

            # короткий скролл
            try:
                await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1.2)
                await self.page.evaluate("window.scrollTo(0, 0)")
            except Exception:
                pass

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехватчик активен")
            asyncio.create_task(self._process_queues())

    async def _handle_response(self, response: Response):
        url = response.url
        if 'api-2/betline/changes/inplayupcoming' not in url:
            return
        try:
            data = await response.json()
            items = data.get('data') if isinstance(data, dict) else None
            if isinstance(items, list):
                logger.debug(f"[{self.bk_id}] 📥 inplayupcoming: {len(items)} events")
            await self._data_queue.put(('http', data))
        except Exception as e:
            logger.warning(f"[{self.bk_id}] Ошибка парсинга HTTP: {e}")

    async def _process_queues(self):
        while self.is_running:
            try:
                msg_type, data = await asyncio.wait_for(
                    self._data_queue.get(), timeout=1.0
                )
                if msg_type == 'http':
                    self._process_http(data)
                await self._try_send_matches()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка обработки очереди: {e}", exc_info=True)

    # ============================================================
    # Парсинг одного ответа
    # ============================================================
    def _process_http(self, data: dict):
        if not data or 'data' not in data or not isinstance(data['data'], list):
            return

        for m in data['data']:
            if not isinstance(m, dict):
                continue

            match_id = str(m.get('id', ''))
            if not match_id:
                continue

            league = m.get('league') or {}
            sport = league.get('sport') or {}
            family = sport.get('family')
            sport_key = FAMILY_TO_SPORT.get(family)
            if not sport_key:
                continue

            # Кибербаскетбол: family='Basketball', но region.family='ELECTRONIC_LEAGUES'
            region = league.get('region') or {}
            if sport_key == BASKETBALL and region.get('family') == 'ELECTRONIC_LEAGUES':
                sport_key = CYBER_BASKETBALL

            if sport_key not in self.enabled_sports:
                continue

            if match_id not in self._matches_cache:
                self._matches_cache[match_id] = {'_last_sent': None}
                self._first_seen[match_id] = time.time()

            cache = self._matches_cache[match_id]
            cache['sport'] = sport_key

            tournament = league.get('name', 'Неизвестно')
            region_name = region.get('name', 'international')

            p1 = p2 = "Неизвестно"
            for comp in m.get('competitors', []):
                if comp.get('homeAway') == 'HOME':
                    p1 = comp.get('name', 'Неизвестно')
                elif comp.get('homeAway') == 'AWAY':
                    p2 = comp.get('name', 'Неизвестно')

            live = m.get('liveStatus') or {}

            # score — партии/сеты/общий счёт
            score_str = (live.get('score') or '0:0').replace('*', '').strip()
            if score_str == '-:-' or not score_str:
                score1 = score2 = 0
            else:
                try:
                    score1, score2 = map(int, score_str.split(':'))
                except ValueError:
                    score1 = score2 = 0

            # setScores — очки по фазам, последняя = активная
            set_scores_str = live.get('setScores') or ''
            sub1 = sub2 = 0
            if set_scores_str:
                parts = [s.strip() for s in set_scores_str.split(';') if s.strip()]
                if parts:
                    try:
                        sub1, sub2 = map(int, parts[-1].split(':'))
                    except ValueError:
                        pass

            # ── Кэфы (как в старой рабочей версии) ──
            odds1 = odds2 = 0.0
            total_line = total_over = total_under = 0.0
            h1 = h2 = h_o1 = h_o2 = 0.0

            for market in m.get('markets', []):
                m_name = market.get('name', '')
                runners = market.get('runners', [])

                if m_name == 'Победитель':
                    for r in runners:
                        if r.get('name') == '1':
                            odds1 = float(r.get('price', 0) or 0)
                        elif r.get('name') == '2':
                            odds2 = float(r.get('price', 0) or 0)

                elif m_name in ('Тотал очков', 'Тотал'):
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
                            if player == '1' and h1 == 0.0 and h_o1 == 0.0:
                                handicap1 = h_line
                                handicap_odds1 = price
                            elif player == '2' and h2 == 0.0 and h_o2 == 0.0:
                                handicap2 = h_line
                                handicap_odds2 = price

            # ── Фаза: для баскетбола/кибербаскета — номер четверти из stage/setScores,
            #          для остальных — сумма партий/сетов + 1 ──
            stage_str = live.get('stage', '') or ''

            if sport_key in (BASKETBALL, CYBER_BASKETBALL):
                mm = re.search(r'(\d+)', stage_str)
                if mm:
                    phase_num = int(mm.group(1))
                else:
                    # Fallback: количество частей в setScores
                    parts_count = len(
                        [x for x in set_scores_str.split(';') if x.strip()]
                    ) if set_scores_str else 0
                    phase_num = parts_count if parts_count > 0 else 1

                # OT (овертайм) — если stage содержит "ОТ" или "OT"
                if re.search(r'\bОТ\b|\bOT\b', stage_str, re.IGNORECASE):
                    phase_num = 5  # format_phase превратит 5 → ОТ1
            else:
                phase_num = score1 + score2 + 1

            phase_name = format_phase(sport_key, phase_num)

            cache.update({
                'player1': p1,
                'player2': p2,
                'score1': score1, 'score2': score2,
                'sub1': sub1, 'sub2': sub2,
                'phase_num': phase_num,
                'phase_name': phase_name,
                'tournament': tournament,
                'region': region_name,
                'odds1': odds1, 'odds2': odds2,
                'total_line': total_line, 'total_over': total_over, 'total_under': total_under,
                'handicap1': h1, 'handicap2': h2,
                'handicap_odds1': h_o1, 'handicap_odds2': h_o2,
                'status': stage_str
            })

    # ============================================================
    # Отправка в detector
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get('player1') or not m.get('player2') or m.get('player1') == 'Неизвестно':
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
                m.get('odds1', 0.0), m.get('odds2', 0.0),
                m.get('total_line', 0.0), m.get('total_over', 0.0),
                m.get('total_under', 0.0),
                m.get('handicap1', 0.0), m.get('handicap2', 0.0),
                m.get('handicap_odds1', 0.0), m.get('handicap_odds2', 0.0),
            )
            if m.get('_last_sent') == current_state:
                continue

            # Фаза (уже посчитана в _process_http)
            score1 = m.get('score1', 0)
            score2 = m.get('score2', 0)
            phase_num = m.get('phase_num', score1 + score2 + 1)
            phase_name = m.get('phase_name') or format_phase(sport_key, phase_num)

            # URL матча
            slug = get_url_slug('leon', sport_key) or 'table-tennis'
            region_slug = _slug(m.get('region', 'international'))
            tour_slug = _slug(m.get('tournament', ''))
            p1_slug = _slug(m.get('player1', ''))
            p2_slug = _slug(m.get('player2', ''))
            match_url = (
                f"https://leon.ru/bets/{slug}/{region_slug}/"
                f"{tour_slug}/{match_id}-{p1_slug}-{p2_slug}"
            )

            match = Match(
                bk_id='leon',
                match_id=match_id,
                player1=m['player1'],
                player2=m['player2'],
                score1=score1,
                score2=score2,
                sub_score1=m.get('sub1', 0),
                sub_score2=m.get('sub2', 0),
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
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Leon запущен")
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
            except Exception:
                pass
            await browser_manager.close_page(self.page)
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")