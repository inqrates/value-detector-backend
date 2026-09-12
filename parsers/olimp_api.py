# parsers/olimp_api.py
"""
Olimp API-парсер с поддержкой мультиспорта (НТ / волейбол / баскетбол / кибербаскет).

Особенности Olimp:
  - На /live прилетает МАССИВ [dict, dict, ...] — по одному элементу
    на каждый вид спорта. data[0] всегда футбол (id=1), поэтому нельзя
    брать только первый элемент — нужно итерировать весь массив.
  - Каждый элемент массива: {operationId, version, payload}, где
    payload.id = sportId ("1"=футбол, "5"=баскет, "10"=волей, "40"=НТ, "140"=кибер, ...).
  - Внутри payload.competitionsWithEvents[].events[] лежат события.
  - Вид спорта для каждого события берём из event.sportId (а не из payload.id).
  - Счёт по фазам — из event.mapsScore[]:
      НТ/волейбол: последняя пара = очки текущей партии/сета
      Баскетбол/кибер: последняя пара = очки текущей четверти
  - Кэфы из event.outcomes[]:
      RESULT  — П1/П2 (у баскетбола tableType="OTHER", но categories=["RESULT"])
      HANDICAP — "Фора 1"/"Фора 2" (главные, без дФ*К-дублей)
      TOTAL   — "ТотМ"/"ТотБ"      (главные, без ТотNТотN*)
"""
import asyncio
import time
import logging
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


class OlimpApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('olimp', detector=detector, aggregator=aggregator)

        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]

        # Общий лайв Olimp — на нём приходят все виды одним массивом
        self.url = SPORT_URLS.get("_all", {}).get("olimp", self.url)
        logger.info(f"[{self.bk_id}] Стартовый URL: {self.url}")

        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False

        # sport_id → sport_key
        self._sport_ids: Dict[str, str] = {}
        for sport_key in self.enabled_sports:
            cfg = SPORT_MAP.get('olimp', {}).get(sport_key, {})
            for sid in cfg.get('ids', []):
                self._sport_ids[str(sid)] = sport_key
        logger.info(f"[{self.bk_id}] sport_ids: {self._sport_ids}")

    # ============================================================
    # Запуск
    # ============================================================
    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            self.page.on("response", self._handle_response)

            logger.info(f"[{self.bk_id}] Загрузка страницы {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded',
                                 timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME + 2000)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехватчик API активен")
            asyncio.create_task(self._process_queues())

    # ============================================================
    # Перехват ответов
    # ============================================================
    async def _handle_response(self, response: Response):
        url = response.url
        # Только live-эндпоинты с событиями. Дерево
        # (sports-with-categories-with-competitions), line/ и broadcast/ — не нужны.
        if 'api/v4/0/live' not in url:
            return
        if 'sports-with-competitions-with-events' not in url:
            return

        try:
            data = await response.json()
        except Exception as e:
            logger.debug(f"[{self.bk_id}] JSON error {url[:100]}: {e}")
            return

        # Ответ — массив [dict, dict, ...], по одному dict на вид спорта.
        # data[0] — всегда футбол, поэтому нельзя брать только его.
        if isinstance(data, list):
            items = [x for x in data if isinstance(x, dict)]
        elif isinstance(data, dict):
            items = [data]
        else:
            return

        for item in items:
            payload = item.get('payload')
            if not isinstance(payload, dict):
                continue
            comps = payload.get('competitionsWithEvents')
            if not isinstance(comps, list) or not comps:
                continue
            await self._data_queue.put(('http', item))

    # ============================================================
    # Обработка
    # ============================================================
    def _process_http(self, data: dict):
        payload = data.get('payload', {})
        if not isinstance(payload, dict):
            return

        comps = payload.get('competitionsWithEvents', [])
        if not isinstance(comps, list):
            return

        for block in comps:
            if not isinstance(block, dict):
                continue
            competition = block.get('competition', {}) or {}
            tournament_name = competition.get('name', 'Неизвестно')
            events = block.get('events', []) or []
            for event in events:
                if not isinstance(event, dict):
                    continue

                # Вид спорта определяем по event.sportId.
                sport_id_str = str(event.get('sportId') or '')
                sport_key = self._sport_ids.get(sport_id_str)
                if not sport_key:
                    continue

                self._process_event(event, tournament_name, sport_key)

    def _process_event(self, event: dict, tournament_name: str, sport_key: str):
        state = event.get('state', '')
        if state == 'FINISHED':
            return

        match_id = str(event.get('id') or '')
        if not match_id:
            return

        if match_id not in self._matches_cache:
            self._matches_cache[match_id] = {'_last_sent': None}
            self._first_seen[match_id] = time.time()

        cache = self._matches_cache[match_id]
        cache['tournament'] = tournament_name
        cache['sport'] = sport_key

        cache['player1'] = event.get('team1Name', 'Неизвестно')
        cache['player2'] = event.get('team2Name', 'Неизвестно')

        # ─── score ───
        score_str = event.get('score', '') or '0:0'
        try:
            s1, s2 = map(int, score_str.split(':'))
        except (ValueError, TypeError):
            s1, s2 = 0, 0
        cache['score1'] = s1
        cache['score2'] = s2

        # ─── sub_score из mapsScore ───
        maps = event.get('mapsScore', []) or []
        pairs = []
        for m in maps:
            if isinstance(m, dict):
                try:
                    pairs.append((int(m.get('team1', 0)), int(m.get('team2', 0))))
                except (ValueError, TypeError):
                    pairs.append((0, 0))

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            if pairs:
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

        cache['sub1'] = sub1
        cache['sub2'] = sub2
        cache['phase_num'] = phase_num

        # ─── Кэфы ───
        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0

        outcomes = event.get('outcomes', []) or []
        for out in outcomes:
            if not isinstance(out, dict):
                continue

            table_type = out.get('tableType', '')
            categories = out.get('categories', []) or []
            short_name = out.get('shortName', '')
            prob_str = out.get('probability', '0')
            param_str = out.get('param', '0')

            try:
                prob = float(str(prob_str).replace(',', '.'))
            except (ValueError, TypeError):
                prob = 0.0
            try:
                param = float(str(param_str).replace(',', '.'))
            except (ValueError, TypeError):
                param = 0.0

            is_result = (table_type == 'RESULT') or ('RESULT' in categories)

            if is_result and short_name == 'П1':
                odds1 = prob
            elif is_result and short_name == 'П2':
                odds2 = prob
            elif table_type == 'HANDICAP' and short_name == 'Фора 1':
                h1 = param
                h_o1 = prob
            elif table_type == 'HANDICAP' and short_name == 'Фора 2':
                h2 = param
                h_o2 = prob
            elif table_type == 'TOTAL' and short_name == 'ТотМ':
                total_under = prob
                total_line = param
            elif table_type == 'TOTAL' and short_name == 'ТотБ':
                total_over = prob
                total_line = param

        cache['odds1'] = odds1
        cache['odds2'] = odds2
        cache['total_line'] = total_line
        cache['total_over'] = total_over
        cache['total_under'] = total_under
        cache['handicap1'] = h1
        cache['handicap2'] = h2
        cache['handicap_odds1'] = h_o1
        cache['handicap_odds2'] = h_o2

    # ============================================================
    # Очередь / отправка
    # ============================================================
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
                logger.error(f"[{self.bk_id}] Ошибка очереди: {e}", exc_info=True)

    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get('player1') or m.get('player1') == 'Неизвестно':
                continue

            first_seen = self._first_seen.get(match_id, current_time)
            if m.get('odds1', 0) == 0 and m.get('odds2', 0) == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            sport_key = m.get('sport', TABLE_TENNIS)

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

            slug = get_url_slug('olimp', sport_key) or 'nastolnyy-tennis-40'
            match_url = f"https://www.olimp.bet/live/{slug}/x/x-{match_id}"

            phase_name = format_phase(sport_key, m.get('phase_num', 1))

            match = Match(
                bk_id='olimp',
                match_id=match_id,
                player1=m['player1'],
                player2=m['player2'],
                score1=m.get('score1', 0),
                score2=m.get('score2', 0),
                sub_score1=m.get('sub1', 0),
                sub_score2=m.get('sub2', 0),
                tournament=m.get('tournament', 'Неизвестно'),
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
                logger.error(f"[{self.bk_id}] Критическая ошибка: {e}", exc_info=True)
                await self.stop()
                await asyncio.sleep(5)

    async def stop(self):
        self.is_running = False
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")