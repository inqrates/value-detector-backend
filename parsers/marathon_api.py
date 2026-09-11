import asyncio
import time
import re
import json
import logging
from typing import Dict, List
import httpx

from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME

logger = logging.getLogger(__name__)


# <-- НОВОЕ: slug-хелпер для URL
def _slug(text: str) -> str:
    """Слагификатор для URL. Кириллицу Playwright закодирует сам."""
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class MarathonApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('marathon', detector=detector, aggregator=aggregator)
        self._data_queue = asyncio.Queue(maxsize=10000)
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False
        self._client: httpx.AsyncClient = None
        self._full_snapshot_interval = 10 * 60

    def _process_item_map(self, item_map: dict):
        for tournament_id, tournament_data in item_map.items():
            if tournament_data.get('sportCode') != 'TableTennis':
                continue
            tournament_name = tournament_data.get('name', 'Неизвестно')
            for event in tournament_data.get('liveEvents', []):
                self._process_event(event, tournament_name)

    async def _get_session_data(self):
        logger.info(f"[{self.bk_id}] Получение сессионных данных через Playwright")
        page = await browser_manager.new_page()
        try:
            await page.goto(self.url, wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
            await page.wait_for_timeout(3000)
            cookies = await page.context.cookies()
            cookies_dict = {c['name']: c['value'] for c in cookies}
            user_agent = await page.evaluate("navigator.userAgent")
            logger.info(f"[{self.bk_id}] Сессия получена, кук: {len(cookies_dict)}")
            return cookies_dict, user_agent
        finally:
            await page.close()

    async def _init_client(self):
        if self._client is None:
            cookies, user_agent = await self._get_session_data()
            headers = {
                "Accept": "text/event-stream",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": self.url,
                "Origin": "https://new.marathonbet.ru",
                "User-Agent": user_agent,
                "x-pan-source": "REDESIGN_WEB",
                "x-pan-target": "BROWSER",
                "x-pan-version": "MOBILE-SSR-2.6.5",
            }
            self._client = httpx.AsyncClient(
                cookies=cookies,
                headers=headers,
                timeout=httpx.Timeout(300.0, connect=10.0),
                follow_redirects=True
            )
            logger.info(f"[{self.bk_id}] HTTP-клиент инициализирован")

    async def start(self):
        if self._client is None:
            await self._init_client()
        logger.info(f"[{self.bk_id}] Запуск SSE-потока и фоновых задач")
        asyncio.create_task(self._process_queues())
        asyncio.create_task(self._sse_loop())
        asyncio.create_task(self._periodic_full_snapshot())

    async def _sse_loop(self):
        url = "https://new.marathonbet.ru/eag/event-line/api/v1/sports/382549/all-tournaments/live"
        retry_delay = 1
        max_retry_delay = 60
        retry_count = 0

        while self.is_running:
            try:
                if self._client is None:
                    await asyncio.sleep(1)
                    continue

                logger.info(f"[{self.bk_id}] Подключение к SSE: {url}")
                async with self._client.stream("GET", url) as response:
                    if response.status_code != 200:
                        logger.error(f"[{self.bk_id}] SSE ошибка статус: {response.status_code}")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, max_retry_delay)
                        retry_count += 1
                        if retry_count > 5:
                            logger.warning(f"[{self.bk_id}] Множественные ошибки, переинициализация клиента...")
                            self._client = None
                            retry_count = 0
                        continue

                    retry_delay = 1
                    retry_count = 0
                    logger.info(f"[{self.bk_id}] SSE-соединение установлено")
                    buffer = ""
                    async for chunk in response.aiter_bytes():
                        if not chunk:
                            break
                        try:
                            buffer += chunk.decode('utf-8')
                        except UnicodeDecodeError:
                            continue

                        while '\n\n' in buffer:
                            part, buffer = buffer.split('\n\n', 1)
                            if part.strip():
                                await self._process_sse_part(part)

                    logger.warning(f"[{self.bk_id}] SSE-поток завершён, переподключение через {retry_delay} сек")
                    await asyncio.sleep(retry_delay)

            except asyncio.CancelledError:
                logger.info(f"[{self.bk_id}] SSE-задача отменена")
                break
            except Exception as e:
                logger.error(f"[{self.bk_id}] SSE ошибка: {e}", exc_info=True)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
                retry_count += 1
                if retry_count > 5:
                    self._client = None
                    retry_count = 0

    async def _process_sse_part(self, part: str):
        lines = part.split('\n')
        event_type = 'message'
        data_line = ''
        for line in lines:
            line = line.strip()
            if line.startswith('event:'):
                event_type = line[6:].strip()
            elif line.startswith('data:'):
                if data_line:
                    data_line += '\n'
                data_line += line[5:]

        if data_line:
            try:
                data = json.loads(data_line)
                await self._data_queue.put((event_type, data))
            except json.JSONDecodeError as e:
                logger.error(f"[{self.bk_id}] Ошибка парсинга JSON: {e}")

    async def _periodic_full_snapshot(self):
        url = "https://new.marathonbet.ru/eag/event-line/api/v1/sports/382549/all-tournaments/live?fullSnapshot=true"
        while self.is_running:
            await asyncio.sleep(self._full_snapshot_interval)
            if self._client is None:
                continue
            try:
                response = await self._client.get(url)
                if response.status_code == 200:
                    data = response.json()
                    await self._data_queue.put(('snapshot', data))
                    logger.info(f"[{self.bk_id}] Запрошен fullSnapshot")
                else:
                    logger.warning(f"[{self.bk_id}] fullSnapshot ошибка: {response.status_code}")
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка fullSnapshot: {e}")

    def _convert_fraction_to_decimal(self, n: int, d: int) -> float:
        if d == 0:
            return 0.0
        return (n + d) / d

    def _process_event(self, event: dict, tournament_name: str):
        match_id = str(event.get('treeId', ''))
        if not match_id:
            return

        if match_id not in self._matches_cache:
            self._matches_cache[match_id] = {}
            self._first_seen[match_id] = time.time()

        cache = self._matches_cache[match_id]
        cache['phase_type'] = event.get('phase', {}).get('phaseType', 'Started')

        name = event.get('name', '')
        if ' - ' in name:
            parts = name.split(' - ', 1)
            cache['player1'] = parts[0].strip()
            cache['player2'] = parts[1].strip()
        else:
            home_members = event.get('homeTeam', {}).get('members', [])
            away_members = event.get('awayTeam', {}).get('members', [])
            if home_members and away_members:
                cache['player1'] = home_members[0].get('name', 'Неизвестно')
                cache['player2'] = away_members[0].get('name', 'Неизвестно')

        if tournament_name and tournament_name != 'Неизвестно':
            cache['tournament'] = tournament_name

        match_score = event.get('matchScore', {})
        if match_score:
            main_score = match_score.get('main', {})
            try:
                cache['score1'] = int(main_score.get('home', 0))
                cache['score2'] = int(main_score.get('away', 0))
            except (ValueError, TypeError):
                pass

            parts = match_score.get('parts', [])
            phase = event.get('phase', {})
            part_number = phase.get('partNumber', 1)
            if parts and len(parts) >= part_number:
                current_part = parts[part_number - 1]
                try:
                    cache['sub1'] = int(current_part.get('home', 0))
                    cache['sub2'] = int(current_part.get('away', 0))
                except (ValueError, TypeError):
                    pass

        markets = event.get('markets', {})
        if not markets:
            return

        player1_name = cache.get('player1', '')
        player2_name = cache.get('player2', '')

        odds1 = cache.get('odds1', 0.0)
        odds2 = cache.get('odds2', 0.0)
        total_line = cache.get('total_line', 0.0)
        total_over = cache.get('total_over', 0.0)
        total_under = cache.get('total_under', 0.0)
        handicap1 = cache.get('handicap1', 0.0)
        handicap2 = cache.get('handicap2', 0.0)
        handicap_odds1 = cache.get('handicap_odds1', 0.0)
        handicap_odds2 = cache.get('handicap_odds2', 0.0)

        for market_id, market_data in markets.items():
            market_name = market_data.get('name', '')
            selections = market_data.get('selections', {})

            if market_name == 'Победа в матче':
                for sel_id, sel_data in selections.items():
                    sel_name = sel_data.get('name', '')
                    coeff = sel_data.get('coeff', {})
                    price = coeff.get('price', {})
                    n = price.get('n', 0)
                    d = price.get('d', 1)
                    decimal_coeff = self._convert_fraction_to_decimal(n, d)
                    if player1_name and player1_name in sel_name:
                        odds1 = decimal_coeff
                    elif player2_name and player2_name in sel_name:
                        odds2 = decimal_coeff

            elif 'Тотал' in market_name and 'очкам' in market_name:
                for sel_id, sel_data in selections.items():
                    sel_name = sel_data.get('name', '')
                    coeff = sel_data.get('coeff', {})
                    price = coeff.get('price', {})
                    n = price.get('n', 0)
                    d = price.get('d', 1)
                    decimal_coeff = self._convert_fraction_to_decimal(n, d)
                    match_line = re.search(r'([\d.]+)', sel_name)
                    if match_line:
                        total_line = float(match_line.group())
                    if 'Меньше' in sel_name:
                        total_under = decimal_coeff
                    elif 'Больше' in sel_name:
                        total_over = decimal_coeff

            elif 'Фора' in market_name or 'Handicap' in market_name:
                for sel_id, sel_data in selections.items():
                    sel_name = sel_data.get('name', '')
                    coeff = sel_data.get('coeff', {})
                    price = coeff.get('price', {})
                    n = price.get('n', 0)
                    d = price.get('d', 1)
                    decimal_coeff = self._convert_fraction_to_decimal(n, d)
                    match_handicap = re.search(r'\(([+-]?[\d.]+)\)', sel_name)
                    if match_handicap:
                        line = float(match_handicap.group(1))
                        name_part = re.sub(r'\s*\([+-]?[\d.]+\)\s*$', '', sel_name).strip()
                        if player1_name and player1_name in name_part:
                            handicap1 = line
                            handicap_odds1 = decimal_coeff
                        elif player2_name and player2_name in name_part:
                            handicap2 = line
                            handicap_odds2 = decimal_coeff

        cache['odds1'] = odds1
        cache['odds2'] = odds2
        cache['total_line'] = total_line
        cache['total_over'] = total_over
        cache['total_under'] = total_under
        cache['handicap1'] = handicap1
        cache['handicap2'] = handicap2
        cache['handicap_odds1'] = handicap_odds1
        cache['handicap_odds2'] = handicap_odds2

    async def _process_queues(self):
        while self.is_running:
            try:
                event_type, data = await asyncio.wait_for(self._data_queue.get(), timeout=1.0)
                if event_type in ('json', 'snapshot'):
                    if isinstance(data, dict):
                        item_map = data.get('itemMap')
                        if item_map:
                            self._process_item_map(item_map)
                elif event_type == 'update':
                    if isinstance(data, list):
                        for change in data:
                            value = change.get('value')
                            if isinstance(value, dict):
                                if 'liveEvents' in value:
                                    tournament_name = value.get('name', 'Неизвестно')
                                    for event in value.get('liveEvents', []):
                                        self._process_event(event, tournament_name)
                                elif 'name' in value and 'matchScore' in value:
                                    tournament_name = 'Неизвестно'
                                    self._process_event(value, tournament_name)
                                else:
                                    match_id_from_path = None
                                    for p in change.get('path', []):
                                        if p.get('key') == 'treeId' and p.get('inList') == 'liveEvents':
                                            match_id_from_path = str(p.get('val'))
                                            break

                                    if not match_id_from_path or match_id_from_path not in self._matches_cache:
                                        continue

                                    cache = self._matches_cache[match_id_from_path]

                                    if 'main' in value:
                                        try:
                                            cache['score1'] = int(value['main'].get('home', cache.get('score1', 0)))
                                            cache['score2'] = int(value['main'].get('away', cache.get('score2', 0)))
                                        except (ValueError, TypeError):
                                            pass

                                        if 'parts' in value and isinstance(value['parts'], list) and len(value['parts']) > 0:
                                            try:
                                                part_idx = cache.get('part_number', 1) - 1
                                                if len(value['parts']) == 1 and part_idx >= 0:
                                                    cache['sub1'] = int(value['parts'][0].get('home', cache.get('sub1', 0)))
                                                    cache['sub2'] = int(value['parts'][0].get('away', cache.get('sub2', 0)))
                                                elif len(value['parts']) > 1:
                                                    cache['sub1'] = int(value['parts'][-1].get('home', cache.get('sub1', 0)))
                                                    cache['sub2'] = int(value['parts'][-1].get('away', cache.get('sub2', 0)))
                                            except (ValueError, TypeError, IndexError):
                                                pass

                                    if 'partNumber' in value:
                                        try:
                                            cache['part_number'] = int(value['partNumber'])
                                        except (ValueError, TypeError):
                                            pass

                                    if 'phaseType' in value:
                                        cache['phase_type'] = value['phaseType']

                                    if 'coeff' in value and 'name' in value:
                                        sel_name = value.get('name', '')
                                        coeff = value.get('coeff', {})
                                        price = coeff.get('price', {})
                                        n = price.get('n', 0)
                                        d = price.get('d', 1)
                                        decimal_coeff = self._convert_fraction_to_decimal(n, d)

                                        player1_name = cache.get('player1', '')
                                        player2_name = cache.get('player2', '')

                                        match_handicap = re.search(r'\(([+-]?[\d.]+)\)', sel_name)
                                        match_total = re.search(r'(Меньше|Больше)\s*([\d.]+)', sel_name)

                                        if match_handicap:
                                            line = float(match_handicap.group(1))
                                            name_part = re.sub(r'\s*\([+-]?[\d.]+\)\s*$', '', sel_name).strip()
                                            if player1_name and player1_name in name_part:
                                                cache['handicap1'] = line
                                                cache['handicap_odds1'] = decimal_coeff
                                            elif player2_name and player2_name in name_part:
                                                cache['handicap2'] = line
                                                cache['handicap_odds2'] = decimal_coeff
                                        elif match_total:
                                            line = float(match_total.group(2))
                                            cache['total_line'] = line
                                            if 'Меньше' in sel_name:
                                                cache['total_under'] = decimal_coeff
                                            elif 'Больше' in sel_name:
                                                cache['total_over'] = decimal_coeff
                                        else:
                                            if player1_name and player1_name in sel_name:
                                                cache['odds1'] = decimal_coeff
                                            elif player2_name and player2_name in sel_name:
                                                cache['odds2'] = decimal_coeff

                await self._try_send_matches()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка обработки очереди: {e}", exc_info=True)

    async def _try_send_matches(self, force_match_id: str = None):
        sent = 0
        current_time = time.time()
        matches_to_check = [force_match_id] if force_match_id else list(self._matches_cache.keys())

        for match_id in matches_to_check:
            if match_id not in self._matches_cache:
                continue
            m = self._matches_cache[match_id]

            if m.get('phase_type') == 'Finished':
                continue

            if not m.get('player1') or m.get('player1') == 'Неизвестно':
                continue

            if m.get('odds1', 0) == 0 and m.get('odds2', 0) == 0:
                continue

            # Cooldown
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

            # <-- НОВОЕ: собираем URL для матча
            tour_slug = _slug(m.get('tournament', ''))
            p1_slug = _slug(m.get('player1', ''))
            p2_slug = _slug(m.get('player2', ''))
            match_url = (
                f"https://new.marathonbet.ru/su/betting/event/"
                f"table-tennis/{tour_slug}/{p1_slug}-vs-{p2_slug}"
            )

            match = Match(
                bk_id='marathon',
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
                raw_time='',
                match_url=match_url,      # <-- НОВОЕ
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            m['_last_sent'] = current_state
            self._last_sent_time[match_id] = current_time
            sent += 1

            if force_match_id:
                logger.debug(f"[{self.bk_id}] 🔄 Обновление: {match.player1} vs {match.player2} | "
                             f"Счет: {match.score1}:{match.score2} | Ф: {match.handicap1}/{match.handicap2} | Т: {match.total_line}")
            else:
                logger.info(f"[{self.bk_id}] 🟢 Отправлен: {match.player1} vs {match.player2} | "
                            f"{match.score1}:{match.score2} (сет: {match.sub_score1}:{match.sub_score2}) | "
                            f"К: {match.odds1}/{match.odds2} | Т: {match.total_line} | Ф: {match.handicap1}")

        if sent and not force_match_id:
            logger.info(f"[{self.bk_id}] ✅ Отправлено обновлений: {sent}, всего в кеше: {len(self._matches_cache)}")

    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Marathon запущен (реальное время)")
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
        if self._client:
            await self._client.aclose()
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")