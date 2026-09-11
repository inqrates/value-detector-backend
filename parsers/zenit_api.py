# parsers/zenit_api.py
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
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME

logger = logging.getLogger(__name__)


# ============================================================
# ДОБАВЛЕНО: хелпер слагификации (для сборки URL)
# ============================================================
def _slug(text: str) -> str:
    """Простой слагификатор. Кириллицу оставляем — Playwright сам закодирует."""
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"
# ============================================================


class ZenitApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('zenit', detector=detector, aggregator=aggregator)
        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False
        self._tt_sport_id = 134

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
            if '/ajax/live/video/get_list' in url:
                data = await response.json()
                await self._data_queue.put(('http', data))
                logger.debug(f"[{self.bk_id}] 📥 Перехвачен список матчей")
        except Exception as e:
            logger.warning(f"[{self.bk_id}] Ошибка парсинга HTTP {url}: {e}")

    async def _handle_websocket(self, ws: WebSocket):
        if 'wss://zenit.win/wss' in ws.url:
            logger.info(f"[{self.bk_id}] 🔌 WebSocket подключён")
            ws.on("framereceived", self._on_ws_frame)

    def _on_ws_frame(self, payload):
        try:
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            if not payload:
                return
            logger.debug(f"[{self.bk_id}] 📨 WS-фрейм: {payload[:300]}...")
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
            games = data.get('result', {}).get('games', [])
        except AttributeError:
            return

        for game in games:
            sid = game.get('sid')
            if sid != self._tt_sport_id:
                continue
            gid = str(game.get('gid'))
            if not gid:
                continue

            name = game.get('name', '')
            if ' - ' in name:
                player1, player2 = name.split(' - ', 1)
            elif ' vs ' in name:
                player1, player2 = name.split(' vs ', 1)
            else:
                player1, player2 = name, ''

            if gid not in self._matches_cache:
                self._matches_cache[gid] = {
                    'player1': player1.strip(),
                    'player2': player2.strip(),
                    'tournament': 'Zenit Live',
                    'score1': 0, 'score2': 0,
                    'sub1': 0, 'sub2': 0,
                    'odds1': 0.0, 'odds2': 0.0,
                    'total_line': 0.0, 'total_over': 0.0, 'total_under': 0.0,
                    'handicap1': 0.0, 'handicap2': 0.0,
                    'handicap_odds1': 0.0, 'handicap_odds2': 0.0,
                    'finished': False,
                    '_last_sent': None,
                }
                self._first_seen[gid] = time.time()
                logger.debug(f"[{self.bk_id}] 📝 Новый матч: {player1} vs {player2} (gid={gid})")
            else:
                self._matches_cache[gid]['player1'] = player1.strip()
                self._matches_cache[gid]['player2'] = player2.strip()

    def _parse_odds(self, cache: dict, odds_data: dict, main_line: list):
        cache['odds1'] = 0.0
        cache['odds2'] = 0.0
        cache['handicap1'] = 0.0
        cache['handicap2'] = 0.0
        cache['handicap_odds1'] = 0.0
        cache['handicap_odds2'] = 0.0
        cache['total_line'] = 0.0
        cache['total_over'] = 0.0
        cache['total_under'] = 0.0

        odd_1 = odds_data.get('1', {}).get('cf', 0.0)
        odd_3 = odds_data.get('3', {}).get('cf', 0.0)
        cache['odds1'] = odd_1
        cache['odds2'] = odd_3

        odd_7 = odds_data.get('7', {})
        if odd_7:
            cf = odd_7.get('cf', 0.0)
            odd_key = odd_7.get('oddKey', '')
            parts = odd_key.split('|')
            if len(parts) >= 3:
                try:
                    handicap_val = float(parts[2])
                    cache['handicap1'] = handicap_val
                    cache['handicap_odds1'] = cf
                except ValueError:
                    pass

        odd_8 = odds_data.get('8', {})
        if odd_8:
            cf = odd_8.get('cf', 0.0)
            odd_key = odd_8.get('oddKey', '')
            parts = odd_key.split('|')
            if len(parts) >= 3:
                try:
                    handicap_val = float(parts[2])
                    cache['handicap2'] = handicap_val
                    cache['handicap_odds2'] = cf
                except ValueError:
                    pass

        odd_9 = odds_data.get('9', {})
        if odd_9:
            cf = odd_9.get('cf', 0.0)
            cache['total_under'] = cf
            odd_key = odd_9.get('oddKey', '')
            parts = odd_key.split('|')
            if len(parts) >= 3:
                try:
                    total_line = float(parts[2])
                    cache['total_line'] = total_line
                except ValueError:
                    pass

        odd_10 = odds_data.get('10', {})
        if odd_10:
            cf = odd_10.get('cf', 0.0)
            cache['total_over'] = cf
            if cache['total_line'] == 0.0:
                odd_key = odd_10.get('oddKey', '')
                parts = odd_key.split('|')
                if len(parts) >= 3:
                    try:
                        total_line = float(parts[2])
                        cache['total_line'] = total_line
                    except ValueError:
                        pass

        if cache['total_line'] == 0.0 and main_line:
            for item in main_line:
                if item.get('bet') == 9:
                    txt = item.get('txt')
                    if txt:
                        try:
                            cache['total_line'] = float(txt)
                        except ValueError:
                            pass
                    break

    def _process_ws(self, data: dict):
        if data.get('t') != 21:
            return

        d = data.get('d', {})
        matches_data = d.get('matches', {})
        if not matches_data:
            return

        updated_count = 0
        for gid_str, match_info in matches_data.items():
            if gid_str not in self._matches_cache:
                self._matches_cache[gid_str] = {
                    'player1': '',
                    'player2': '',
                    'tournament': 'Zenit Live',
                    'score1': 0, 'score2': 0,
                    'sub1': 0, 'sub2': 0,
                    'odds1': 0.0, 'odds2': 0.0,
                    'total_line': 0.0, 'total_over': 0.0, 'total_under': 0.0,
                    'handicap1': 0.0, 'handicap2': 0.0,
                    'handicap_odds1': 0.0, 'handicap_odds2': 0.0,
                    'finished': False,
                    '_last_sent': None,
                }
                self._first_seen[gid_str] = time.time()

            cache = self._matches_cache[gid_str]
            if 'team1' in match_info:
                cache['player1'] = match_info['team1']
            if 'team2' in match_info:
                cache['player2'] = match_info['team2']

            score_str = match_info.get('score', '')
            if score_str:
                match_total = re.search(r'^[*]?(\d+):(\d+)', score_str)
                if match_total:
                    cache['score1'] = int(match_total.group(1))
                    cache['score2'] = int(match_total.group(2))

                match_sets = re.search(r'\((.*?)\)', score_str)
                if match_sets:
                    sets_str = match_sets.group(1)
                    sets = [s.strip() for s in sets_str.split(',') if s.strip()]
                    if sets:
                        last_set = sets[-1]
                        if ':' in last_set:
                            try:
                                sub1, sub2 = map(int, last_set.split(':'))
                                cache['sub1'] = sub1
                                cache['sub2'] = sub2
                            except ValueError:
                                pass

            sscore = match_info.get('sScore', {})
            sscore_data = sscore.get('sScoreData', {})
            if sscore_data.get('sd') == 'Конец матча':
                cache['finished'] = True
                logger.debug(f"[{self.bk_id}] 🏁 Матч {gid_str} завершён")
                continue

            odds_data = match_info.get('odds')
            main_line = match_info.get('mainLine', [])
            if odds_data:
                self._parse_odds(cache, odds_data, main_line)

            updated_count += 1

        if updated_count:
            logger.info(f"[{self.bk_id}] ✅ Обновлено {updated_count} матчей через WS")

    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        # Удаляем старые завершённые матчи
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
            if not m.get('player1') or not m.get('player2'):
                continue

            first_seen = self._first_seen.get(match_id, current_time)
            if (m['odds1'] == 0 and m['odds2'] == 0 and
                m['score1'] == 0 and m['score2'] == 0 and
                current_time - first_seen < 30):
                continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            # Сравниваем с предыдущим отправленным состоянием
            current_state = (m['score1'], m['score2'], m['sub1'], m['sub2'],
                             m['odds1'], m['odds2'], m['total_line'], m['total_over'],
                             m['total_under'], m['handicap1'], m['handicap2'],
                             m['handicap_odds1'], m['handicap_odds2'])
            if m.get('_last_sent') == current_state:
                continue

            # ============================================================
            # ДОБАВЛЕНО: собираем URL страницы матча.
            # Пример: https://zenit.win/live/134  (базовый лайв-раздел)
            # Матч открывается как https://zenit.win/live/134/{gid}
            # — Zenit как SPA читает gid из последнего сегмента.
            # ============================================================
            match_url = f"https://zenit.win/live/134/{match_id}"
            # ============================================================

            match = Match(
                bk_id='zenit',
                match_id=match_id,
                player1=m['player1'],
                player2=m['player2'],
                score1=m['score1'],
                score2=m['score2'],
                sub_score1=m['sub1'],
                sub_score2=m['sub2'],
                tournament=m.get('tournament', 'Zenit Live'),
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
                match_url=match_url,                      # <-- ДОБАВЛЕНО
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
            logger.debug(f"[{self.bk_id}] URL: {match_url}")  # <-- ДОБАВЛЕНО

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено обновлений: {sent}, всего в кеше: {len(self._matches_cache)}")

    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Zenit запущен")
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