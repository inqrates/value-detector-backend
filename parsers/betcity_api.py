# parsers/betcity_api.py
"""
Betcity API-парсер с поддержкой мультиспорта (НТ / волейбол / баскетбол / кибербаскет).

Перехватывает `ad.betcity.ru` → `on_air/bets` — один ответ содержит ВСЕ виды спорта:
  - sports["46"] — Настольный теннис
  - sports["12"] — Волейбол
  - sports["3"]  — Баскетбол (+ кибербаскетбол, отделяется по chmp.is_cyber==1)

Форматы данных:
  НТ:        sc_ev = счёт партий,      sc_inter = очки по партиям,   last = текущая
  Волейбол:  sc_ev = счёт сетов,       sc_inter = очки по сетам,     last = текущий
  Баскетбол: sc_ev = ОБЩИЙ счёт,       sc_inter = очки по четвертям, last = текущая
             Если time_name == "Перерыв" — все четверти в sc_inter завершены.
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


class BetcityApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('betcity', detector=detector, aggregator=aggregator)

        # Betcity отдаёт ВСЕ виды спорта одним ответом on_air/bets,
        # поэтому открываем общий лайв, а не раздел НТ.
        # Это подтверждено тремя дампами (НТ/волейбол/баскетбол) с https://betcity.ru/ru/live.
        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]
        self.url = SPORT_URLS.get("_all", {}).get("betcity", self.url)
        logger.info(f"[{self.bk_id}] Используем общий лайв: {self.url}")

        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False

        # Карта sport_id ("3"/"12"/"46") → sport_key
        # Кибербаскетбол определяется inline по chmp.is_cyber==1.
        self._sport_ids: Dict[str, str] = {}
        for sport_key in self.enabled_sports:
            cfg = SPORT_MAP.get('betcity', {}).get(sport_key, {})
            for sid in cfg.get('ids', []):
                self._sport_ids.setdefault(str(sid), sport_key)

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

    # ============================================================
    # Обработка одного ответа
    # ============================================================
    def _process_http(self, data: dict):
        reply = data.get('reply', {})
        sports = reply.get('sports', {})
        if not sports:
            return

        for sport_id_str, sport_key in self._sport_ids.items():
            sport_data = sports.get(sport_id_str)
            if not sport_data:
                continue
            self._process_sport(sport_data, sport_key)

    def _process_sport(self, sport_data: dict, sport_key: str):
        championships = sport_data.get('chmps', {})

        for chmp_id, chmp_data in championships.items():
            tournament_name = chmp_data.get('name_ch', 'Неизвестно')
            is_cyber = chmp_data.get('is_cyber', 0)
            events = chmp_data.get('evts', {})

            # Разделяем кибербаскетбол
            effective_sport = sport_key
            if sport_key == BASKETBALL and is_cyber == 1:
                effective_sport = CYBER_BASKETBALL

            for ev_id, ev_data in events.items():
                # Пропускаем деривативные под-маркеты (2-х очк. попадания и т.п.)
                if ev_data.get('team_type_f', 0) != 0:
                    continue
                if ev_data.get('is_dep', 0) == 1:
                    continue

                self._process_event(
                    str(ev_id), ev_data, tournament_name,
                    str(chmp_id), effective_sport,
                )

    def _process_event(self, match_id: str, ev_data: dict,
                       tournament_name: str, champ_id: str, sport_key: str):
        if match_id not in self._matches_cache:
            self._matches_cache[match_id] = {'_last_sent': None}
            self._first_seen[match_id] = time.time()

        cache = self._matches_cache[match_id]
        cache['tournament'] = tournament_name
        cache['champ_id'] = champ_id
        cache['sport'] = sport_key

        cache['player1'] = ev_data.get('name_ht', 'Неизвестно')
        cache['player2'] = ev_data.get('name_at', 'Неизвестно')
        cache['time_name'] = ev_data.get('time_name', '') or ''

        # ─── Score из sc_ev ───
        score_str = ev_data.get('sc_ev', '0:0') or '0:0'
        try:
            s1, s2 = map(int, score_str.split(':'))
        except ValueError:
            s1, s2 = 0, 0
        cache['score1'] = s1
        cache['score2'] = s2

        # ─── sc_inter → список пар (a,b) ───
        pairs: List[tuple] = []
        sets_str = ev_data.get('sc_inter', '') or ''
        if sets_str:
            for part in sets_str.split(','):
                part = part.strip()
                if ':' not in part:
                    continue
                try:
                    a, b = part.split(':', 1)
                    pairs.append((int(a), int(b)))
                except ValueError:
                    continue

        time_lower = cache['time_name'].lower()
        is_break = 'перерыв' in time_lower

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            # score = общий счёт, sub = текущая четверть
            if pairs:
                if is_break:
                    # Все четверти завершены, ждём следующую
                    sub1 = sub2 = 0
                    phase_num = len(pairs) + 1
                else:
                    sub1, sub2 = pairs[-1]
                    phase_num = len(pairs)
            else:
                sub1 = sub2 = 0
                phase_num = 1
        else:
            # НТ / волейбол: score = партии/сеты, sub = очки текущей партии/сета
            if pairs:
                sub1, sub2 = pairs[-1]
            else:
                sub1 = sub2 = 0
            phase_num = s1 + s2 + 1

        cache['sub1'] = sub1
        cache['sub2'] = sub2
        cache['phase_num'] = phase_num

        # ─── Кэфы (общая логика для всех видов Betcity) ───
        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0

        main_markets = ev_data.get('main', {}) or {}

        if '69' in main_markets:
            wm = (main_markets['69'].get('data', {})
                  .get(match_id, {})
                  .get('blocks', {})
                  .get('Wm', {}))
            odds1 = float(wm.get('P1', {}).get('kf', 0) or 0)
            odds2 = float(wm.get('P2', {}).get('kf', 0) or 0)

        if '72' in main_markets:
            t1m = (main_markets['72'].get('data', {})
                   .get(match_id, {})
                   .get('blocks', {})
                   .get('T1m', {}))
            total_line = float(t1m.get('Tot', 0) or 0)
            total_under = float(t1m.get('Tm', {}).get('kf', 0) or 0)
            total_over = float(t1m.get('Tb', {}).get('kf', 0) or 0)

        if '71' in main_markets:
            f1m = (main_markets['71'].get('data', {})
                   .get(match_id, {})
                   .get('blocks', {})
                   .get('F1m', {}))
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

    # ============================================================
    # Очередь и отправка
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
                logger.error(f"[{self.bk_id}] Ошибка обработки очереди: {e}")

    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get('player1') or m.get('player1') == 'Неизвестно':
                continue

            # Правило проекта: не фильтровать по наличию кэфов.
            # Ждём 15с, чтобы детектор увидел свежий матч, дальше шлём даже без кэфов
            # (линия может быть временно закрыта, но матч живой).
            first_seen = self._first_seen.get(match_id, current_time)
            if m.get('odds1', 0) == 0 and m.get('odds2', 0) == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            sport_key = m.get('sport', 'table_tennis')

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

            # ─── URL матча ───
            slug = get_url_slug('betcity', sport_key)
            if not slug:
                slug = 'basketball' if sport_key == CYBER_BASKETBALL else 'table-tennis'

            champ_id = m.get('champ_id')
            if champ_id:
                match_url = f"https://betcity.ru/ru/live/{slug}/{champ_id}/{match_id}"
            else:
                match_url = f"https://betcity.ru/ru/live/{slug}/{match_id}"

            phase_name = format_phase(sport_key, m.get('phase_num', 1))

            match = Match(
                bk_id='betcity',
                match_id=match_id,
                player1=m['player1'],
                player2=m['player2'],
                score1=m.get('score1', 0),
                score2=m.get('score2', 0),
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
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Betcity запущен")
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