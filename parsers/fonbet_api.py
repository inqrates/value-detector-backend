import asyncio
import time
import logging
import re
from typing import Dict, Set, List
from playwright.async_api import Page, Response
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME

logger = logging.getLogger(__name__)

class FonbetApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('fonbet', detector=detector, aggregator=aggregator)
        self._tt_sport_ids: Set[int] = set()
        self._sport_aliases: Dict[int, str] = {}   # <-- ДОБАВИТЬ
        self._events_cache: Dict[int, dict] = {}
        self._factors_cache: Dict[int, list] = {}
        self._live_cache: Dict[int, dict] = {}
        self._first_seen: Dict[int, float] = {}
        self._last_sent_time: Dict[int, float] = {}
        self._data_queue = asyncio.Queue()
        self.is_running = False

    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            self.page.on("response", self._handle_response)

            logger.info(f"🌐 Загрузка страницы: {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME + 2000)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)
            logger.info("✅ Страница загружена, сетевой перехватчик АКТИВЕН")

    async def _handle_response(self, response: Response):
        url = response.url
        if 'events/list' in url or 'live' in url.lower():
            logger.debug(f"🔍 СЕТЕВОЙ ОТВЕТ: {url}")

        try:
            if 'events/list' in url:
                logger.info("📥 [СЕТЬ] Перехвачен ответ: events/list")
                data = await response.json()
                await self._data_queue.put(('events', data))
            elif 'liveEvents' in url:
                logger.info("📥 [СЕТЬ] Перехвачен ответ: liveEvents")
                data = await response.json()
                await self._data_queue.put(('live', data))
        except Exception as e:
            pass

    async def _process_queues(self):
        while self.is_running:
            try:
                msg_type, data = await asyncio.wait_for(self._data_queue.get(), timeout=1.0)
                logger.debug(f"⚙️ Обработка очереди: тип={msg_type}, размер данных: {len(str(data))} символов")

                if msg_type == 'live':
                    self._process_live(data)
                elif msg_type == 'events':
                    self._process_events(data)

                await self._try_send_matches()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"❌ Ошибка обработки очереди: {e}", exc_info=True)

    def _process_events(self, data: dict):
        logger.info(f"🔍 ОТЛАДКА: Ключи верхнего уровня в data: {list(data.keys())}")

        sports = data.get('sports', [])
                # Кэшируем alias для каждого sportId (нужен для URL)
        for s in sports:
            sid = s.get('id')
            alias = s.get('alias')
            if sid and alias:
                self._sport_aliases[sid] = alias
        logger.info(f"🔍 ОТЛАДКА: Найдено элементов в 'sports': {len(sports)}")

        self._tt_sport_ids.clear()
        root = next((s for s in sports if s.get('alias') == 'table-tennis'), None)

        if not root:
            available_aliases = [s.get('alias') for s in sports[:10] if s.get('alias')]
            logger.warning(f"⚠️ ОТЛАДКА: 'table-tennis' НЕ НАЙДЕН! Доступные alias в ответе: {available_aliases}")
            return

        logger.info(f"✅ ОТЛАДКА: Найден root настольного тенниса: id={root['id']}")
        self._tt_sport_ids.add(root['id'])

        changed = True
        while changed:
            changed = False
            for s in sports:
                parent = s.get('parentId')
                if parent and parent in self._tt_sport_ids and s['id'] not in self._tt_sport_ids:
                    self._tt_sport_ids.add(s['id'])
                    changed = True

        logger.debug(f"🏓 Итоговые sportId для настольного тенниса: {self._tt_sport_ids}")

        events = data.get('events', [])
        logger.info(f"🔍 ОТЛАДКА: Найдено элементов в 'events': {len(events)}")

        tt_events_count = 0
        live_found_count = 0

        for ev in events:
            if ev.get('sportId') in self._tt_sport_ids:
                eid = ev['id']
                tt_events_count += 1

                if eid not in self._events_cache:
                    self._events_cache[eid] = {
                        '_last_sent': None,
                    }
                    self._first_seen[eid] = time.time()

                self._events_cache[eid]['player1'] = ev.get('team1', '') or ev.get('participant1', '')
                self._events_cache[eid]['player2'] = ev.get('team2', '') or ev.get('participant2', '')
                self._events_cache[eid]['tournament'] = ev.get('tournamentName', '')
                self._events_cache[eid]['sport_id'] = ev.get('sportId')   # <-- ДОБАВИТЬ

                if 'score1' in ev or 'score2' in ev or 'scoreStr' in ev or ev.get('isLive'):
                    if eid not in self._live_cache:
                        self._live_cache[eid] = {}
                        self._first_seen[eid] = time.time()
                        live_found_count += 1

                    self._live_cache[eid]['score1'] = ev.get('score1', 0)
                    self._live_cache[eid]['score2'] = ev.get('score2', 0)
                    self._live_cache[eid]['comment'] = ev.get('scoreStr', ev.get('comment', ''))
                    self._live_cache[eid]['liveDelay'] = ev.get('liveDelay', 0)

        if tt_events_count > 0:
            logger.info(f"🔥 УСПЕХ: Найдено {tt_events_count} событий настольного тенниса!")
        if live_found_count > 0:
            logger.info(f"🔥 УСПЕХ: Из них LIVE-событий: {live_found_count}")

        factors = data.get('customFactors', [])
        logger.debug(f"🔍 ОТЛАДКА: Найдено элементов в 'customFactors': {len(factors)}")

        for item in factors:
            eid = item.get('e')
            if eid in self._events_cache or eid in self._live_cache:
                self._factors_cache[eid] = item.get('factors', [])

        if 'eventMiscs' in data:
            logger.info(f"📥 [LIVE] Обнаружены eventMiscs в ответе events/list, записей: {len(data['eventMiscs'])}")
            self._process_live({'eventMiscs': data['eventMiscs']})

    def _process_live(self, data: dict):
        miscs = data.get('eventMiscs', [])
        for m in miscs:
            eid = m.get('id')
            if not eid: continue
            if eid not in self._live_cache:
                self._live_cache[eid] = {}
                self._first_seen[eid] = time.time()
            self._live_cache[eid]['score1'] = m.get('score1', 0)
            self._live_cache[eid]['score2'] = m.get('score2', 0)
            self._live_cache[eid]['comment'] = m.get('comment', '')

    def _parse_factors(self, factors: list) -> dict:
        result = {
            'odds1': 0.0, 'odds2': 0.0, 'totalLine': 0.0, 'totalOver': 0.0, 'totalUnder': 0.0,
            'handicap1': 0.0, 'handicap2': 0.0, 'handicapOdds1': 0.0, 'handicapOdds2': 0.0
        }
        for f in factors:
            if f.get('f') == 921: result['odds1'] = f.get('v', 0.0)
            elif f.get('f') == 923: result['odds2'] = f.get('v', 0.0)

        total_lines = {}
        handicap_pairs = []
        for f in factors:
            pt = str(f.get('pt', '')).strip()
            val = f.get('v', 0.0)
            if re.match(r'^\d+\.?\d*$', pt):
                line = float(pt)
                if line not in total_lines: total_lines[line] = [None, None]
                if total_lines[line][0] is None: total_lines[line][0] = val
                elif total_lines[line][1] is None: total_lines[line][1] = val
            elif re.match(r'^[+-]\d+\.?\d*$', pt):
                handicap_pairs.append((float(pt), val))

        if total_lines:
            first_line = next(iter(total_lines))
            result['totalLine'] = first_line
            result['totalOver'] = total_lines[first_line][0] or 0.0
            result['totalUnder'] = total_lines[first_line][1] or 0.0

        if len(handicap_pairs) >= 2:
            handicap_pairs.sort(key=lambda x: x[0])
            result['handicap1'], result['handicapOdds1'] = handicap_pairs[0]
            result['handicap2'], result['handicapOdds2'] = handicap_pairs[1]
        elif len(handicap_pairs) == 1:
            result['handicap1'], result['handicapOdds1'] = handicap_pairs[0]

        if result['totalLine'] == 0:
            for f in factors:
                if f.get('f') == 930: result['totalOver'], result['totalLine'] = f.get('v', 0.0), f.get('p', 0) / 100.0
                elif f.get('f') == 931: result['totalUnder'], result['totalLine'] = f.get('v', 0.0), f.get('p', 0) / 100.0
        if result['handicap1'] == 0 and result['handicapOdds1'] == 0:
            for f in factors:
                if f.get('f') == 927: result['handicap1'], result['handicapOdds1'] = f.get('p', 0) / 100.0, f.get('v', 0.0)
                elif f.get('f') == 928: result['handicap2'], result['handicapOdds2'] = f.get('p', 0) / 100.0, f.get('v', 0.0)
        return result

    async def _try_send_matches(self):
        current_time = time.time()
        sent = 0

        for eid, live in list(self._live_cache.items()):
            event = self._events_cache.get(eid, {})
            if not event.get('player1') or not event.get('player2'):
                continue

            factors = self._factors_cache.get(eid, [])
            parsed = self._parse_factors(factors)

            first_seen = self._first_seen.get(eid, current_time)
            if parsed['odds1'] == 0 and parsed['odds2'] == 0 and current_time - first_seen < 15:
                continue

            # Cooldown
            last_sent = self._last_sent_time.get(eid, 0)
            if current_time - last_sent < 1.0:
                continue

            sub1, sub2 = 0, 0
            comment = live.get('comment', '')
            if comment:
                sets = re.findall(r'\d+[*]?-\d+[*]?', comment)
                if sets:
                    last = sets[-1].replace('*', '')
                    parts = last.split('-')
                    if len(parts) == 2:
                        sub1 = int(parts[0]) if parts[0].isdigit() else 0
                        sub2 = int(parts[1]) if parts[1].isdigit() else 0

            current_state = (live.get('score1', 0), live.get('score2', 0),
                             sub1, sub2,
                             parsed['odds1'], parsed['odds2'],
                             parsed['totalLine'], parsed['totalOver'],
                             parsed['totalUnder'], parsed['handicap1'],
                             parsed['handicap2'], parsed['handicapOdds1'],
                             parsed['handicapOdds2'])
            if event.get('_last_sent') == current_state:
                continue

            sport_id = event.get('sport_id')
            alias = self._sport_aliases.get(sport_id) if sport_id else None
            if alias and sport_id:
                match_url = f"https://fon.bet/live/table-tennis/category/{alias}/{sport_id}/{eid}"
            else:
                match_url = f"https://fon.bet/live/table-tennis/{eid}"   

            match = Match(
                bk_id='fonbet', match_id=str(eid),
                player1=event['player1'], player2=event['player2'],
                score1=live.get('score1', 0), score2=live.get('score2', 0),
                sub_score1=sub1, sub_score2=sub2,
                tournament=event.get('tournament', ''),
                odds1=parsed['odds1'], odds2=parsed['odds2'],
                total_line=parsed['totalLine'], total_over=parsed['totalOver'], total_under=parsed['totalUnder'],
                handicap1=parsed['handicap1'], handicap2=parsed['handicap2'],
                handicap_odds1=parsed['handicapOdds1'], handicap_odds2=parsed['handicapOdds2'],
                timestamp=current_time, raw_time=comment,
                match_url=match_url,                                  
            )

            logger.info("=" * 70)
            logger.info(f"🎾 МАТЧ: {match.player1} vs {match.player2}")
            logger.info(f"📊 Счёт: {match.score1}:{match.score2} | Сеты: {match.sub_score1}:{match.sub_score2}")
            logger.info(f"💰 Коэф: П1={match.odds1} | П2={match.odds2} | Тотал {match.total_line} | Фора {match.handicap1}")
            logger.info("=" * 70)

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            event['_last_sent'] = current_state
            self._last_sent_time[eid] = current_time
            sent += 1

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено обновлений: {sent}, всего в кеше: {len(self._live_cache)}")

    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(" API-парсер Fonbet запущен")
        asyncio.create_task(self._process_queues())
        while self.is_running:
            try:
                await self.start()
                while self.is_running:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
                await self.stop()
                await asyncio.sleep(5)

    async def stop(self):
        self.is_running = False
        logger.info("🛑 Парсер остановлен")