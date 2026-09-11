import asyncio
import time
import json
import re                                # <-- ДОБАВЛЕНО (нужно для slug)
import logging
from typing import Dict, List, Optional
from playwright.async_api import Response, WebSocket
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------
# Вспомогательная функция для построения slug-части URL.      # <-- ДОБАВЛЕНО
# Русские буквы Playwright сам URL-энкодит, поэтому здесь
# делаем минимальную очистку.
# ---------------------------------------------------------------
def _slug(text: str) -> str:
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class LigaStavokApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('ligastavok', detector=detector, aggregator=aggregator)
        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._finished: set = set()
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False
        self._game_id = 1246

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

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехватчики (HTTP + WS) активны")

    async def _handle_response(self, response: Response):
        url = response.url
        try:
            if '/rest/events/v8/eventsList' in url:
                data = await response.json()
                await self._data_queue.put(('http', data))
                logger.debug(f"[{self.bk_id}] 📥 Перехвачен eventsList")
        except Exception as e:
            logger.warning(f"[{self.bk_id}] Ошибка парсинга HTTP {url}: {e}")

    async def _handle_websocket(self, ws: WebSocket):
        if 'lds-api-sites.ligastavok.ru/ws' in ws.url:
            logger.info(f"[{self.bk_id}] 🔌 WebSocket подключён")
            ws.on("framereceived", self._process_ws_frame)

    def _process_ws_frame(self, frame):
        try:
            payload_str = frame if isinstance(frame, str) else (frame.payload if hasattr(frame, 'payload') else str(frame))
            data = json.loads(payload_str)

            if not isinstance(data, dict): return
            if data.get("id") is not None: return

            result = data.get("result")
            if not isinstance(result, dict): return

            payload = result.get("payload")
            if not isinstance(payload, list): return

            for item in payload:
                if not isinstance(item, dict): continue

                match_id = str(item.get("id", ""))
                if not match_id or match_id in self._finished:
                    continue

                if match_id not in self._matches_cache:
                    self._matches_cache[match_id] = {
                        'event': {},
                        'scores': {'total': {}, 'current': {}, 'all': []},
                        'outcomes': {},
                        'player1': '',
                        'player2': '',
                        'tournament': '',
                        'gameId': None,
                        '_last_sent': None,
                    }
                    self._first_seen[match_id] = time.time()

                cache = self._matches_cache[match_id]
                ws_data = item.get("data") or {}
                if not isinstance(ws_data, dict): ws_data = {}

                changed = False

                headers = ws_data.get("headers") or []
                if not isinstance(headers, list): headers = []

                for h in headers:
                    if not isinstance(h, dict): continue
                    path = h.get("path", "")
                    val = h.get("value")
                    if path == "/scores/current/ScoreTeam1" and val is not None:
                        cache['scores']['current']['ScoreTeam1'] = int(val)
                        changed = True
                    elif path == "/scores/current/ScoreTeam2" and val is not None:
                        cache['scores']['current']['ScoreTeam2'] = int(val)
                        changed = True
                    elif path == "/scores/total/ScoreTeam1" and val is not None:
                        cache['scores']['total']['ScoreTeam1'] = int(val)
                        changed = True
                    elif path == "/scores/total/ScoreTeam2" and val is not None:
                        cache['scores']['total']['ScoreTeam2'] = int(val)
                        changed = True
                    elif path == "/scores/all" and val is not None:
                        cache['scores']['all'] = val
                        changed = True
                    elif path == "/event/statusTranslated" and val is not None:
                        cache['event']['statusTranslated'] = val
                        changed = True

                outcomes = ws_data.get("outcomes") or []
                if not isinstance(outcomes, list): outcomes = []

                for op in outcomes:
                    if not isinstance(op, dict): continue

                    op_type = op.get("op")
                    path = op.get("path", "")
                    val = op.get("value")

                    if op_type in ("add", "replace"):
                        if isinstance(val, dict):
                            out_id = f"_{val.get('id', '')}"
                            if out_id == "_" and len(path.split("/")) >= 3 and path.split("/")[1] == "outcomes":
                                out_id = path.split("/")[2]

                            cache['outcomes'][out_id] = {
                                'outcomeKey': val.get('outcomeKey'),
                                'value': float(val.get('value', 0) or 0),
                                'adValue': float(val.get('adValue', 0) or 0)
                            }
                            changed = True
                        elif val is not None:
                            parts = path.split("/")
                            if len(parts) >= 4 and parts[1] == "outcomes":
                                out_id = parts[2]
                                field = parts[3]
                                if out_id not in cache['outcomes']:
                                    cache['outcomes'][out_id] = {'outcomeKey': None, 'value': 0.0, 'adValue': 0.0}

                                if field == "value":
                                    cache['outcomes'][out_id]['value'] = float(val or 0)
                                    changed = True
                                elif field == "adValue":
                                    cache['outcomes'][out_id]['adValue'] = float(val or 0)
                                    changed = True
                                elif field == "outcomeKey":
                                    cache['outcomes'][out_id]['outcomeKey'] = val
                                    changed = True

                    elif op_type == "remove" and path:
                        parts = path.split("/")
                        if len(parts) >= 3 and parts[1] == "outcomes":
                            cache['outcomes'].pop(parts[2], None)
                            changed = True

                if changed:
                    asyncio.create_task(self._try_send_matches())

                if cache.get('event', {}).get('statusTranslated') == 'Матч завершен':
                    self._finished.add(match_id)
                    logger.info(f"[{self.bk_id}] 🏁 Матч {match_id} завершён (из WS)")

        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.error(f"[{self.bk_id}] Ошибка обработки WS-кадра: {e}", exc_info=True)

    def _process_http(self, data: dict):
        if not data or 'result' not in data or 'data' not in data['result']:
            return

        for m in data['result']['data']:
            if m.get('gameId') != self._game_id:
                continue

            eid = str(m['id'])
            if eid in self._finished:
                continue

            if eid not in self._matches_cache:
                self._matches_cache[eid] = {
                    'event': {},
                    'scores': {'total': {}, 'current': {}, 'all': []},
                    'outcomes': {},
                    'player1': '',
                    'player2': '',
                    'tournament': '',
                    'gameId': None,
                    '_last_sent': None,
                }
                self._first_seen[eid] = time.time()

            cache = self._matches_cache[eid]
            event = m.get('event') or {}

            if not cache.get('player1'):
                competitors = event.get('competitors', [])
                if len(competitors) >= 2:
                    cache['player1'] = competitors[0].get('name', '')
                    cache['player2'] = competitors[1].get('name', '')

                    # --- ДОБАВЛЕНО: сохраняем p_id (id первого участника) ---
                    # В разных версиях API ключ может называться по-разному.
                    # Проверяем несколько вариантов.
                    p_id_candidates = [
                        event.get('pId'),
                        event.get('participantId'),
                        event.get('serviceId'),
                        competitors[0].get('id') if isinstance(competitors[0], dict) else None,
                        competitors[0].get('participantId') if isinstance(competitors[0], dict) else None,
                    ]
                    cache['p_id'] = next((str(x) for x in p_id_candidates if x), '')
                    # --- / ДОБАВЛЕНО ---

                    cache['tournament'] = event.get('tournamentTitle', '')
                    cache['event'] = event
                    logger.debug(
                        f"[{self.bk_id}] 📝 Имена: {cache['player1']} vs {cache['player2']} "
                        f"(p_id={cache.get('p_id')})"
                    )

            if 'scores' in m and m['scores']:
                for key in ['total', 'current', 'all']:
                    if key in m['scores'] and m['scores'][key] is not None:
                        cache['scores'][key] = m['scores'][key]

            if 'outcomes' in m and m['outcomes']:
                for out_key, out_val in m['outcomes'].items():
                    cache['outcomes'][out_key] = {
                        'outcomeKey': out_val.get('outcomeKey'),
                        'value': float(out_val.get('value', 0) or 0),
                        'adValue': float(out_val.get('adValue', 0) or 0)
                    }

            if event.get('statusTranslated') == 'Матч завершен':
                self._finished.add(eid)

    async def _process_queues(self):
        while self.is_running:
            try:
                msg_type, data = await asyncio.wait_for(self._data_queue.get(), timeout=1.0)
                if msg_type == 'http':
                    self._process_http(data)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка очереди: {e}")

    def _parse_match_data(self, eid: str) -> Optional[dict]:
        m = self._matches_cache.get(eid)
        if not m:
            return None

        player1 = m.get('player1') or ''
        player2 = m.get('player2') or ''
        if not player1 or not player2:
            return None

        scores = m.get('scores') or {}
        total_score = scores.get('total') or {}
        score1 = int(total_score.get('ScoreTeam1', 0) or 0)
        score2 = int(total_score.get('ScoreTeam2', 0) or 0)

        current_score = scores.get('current') or {}
        sub1 = int(current_score.get('ScoreTeam1', 0) or 0)
        sub2 = int(current_score.get('ScoreTeam2', 0) or 0)

        if sub1 == 0 and sub2 == 0:
            all_sets = scores.get('all') or []
            if all_sets and len(all_sets) > 0:
                last_set = all_sets[-1] or {}
                sub1 = int(last_set.get('ScoreTeam1', 0) or 0)
                sub2 = int(last_set.get('ScoreTeam2', 0) or 0)

        outcomes = m.get('outcomes') or {}
        odds1 = odds2 = total_line = total_over = total_under = 0.0
        handicap1 = handicap2 = handicap_odds1 = handicap_odds2 = 0.0

        for out in outcomes.values():
            if not isinstance(out, dict):
                continue

            key = out.get('outcomeKey')
            val = float(out.get('value', 0) or 0)
            ad = float(out.get('adValue', 0) or 0)

            if key == '_1': odds1 = val
            elif key == '_2': odds2 = val
            elif key == 'gross':
                total_over = val
                total_line = ad
            elif key == 'less':
                total_under = val
                total_line = ad
            elif key == '1':
                handicap1 = ad
                handicap_odds1 = val
            elif key == '2':
                handicap2 = ad
                handicap_odds2 = val

        event_data = m.get('event') or {}
        return {
            'player1': player1, 'player2': player2,
            'score1': score1, 'score2': score2,
            'sub1': sub1, 'sub2': sub2,
            'comment': event_data.get('statusTranslated', ''),
            'tournament': m.get('tournament', ''),
            'odds1': odds1, 'odds2': odds2,
            'total_line': total_line, 'total_over': total_over, 'total_under': total_under,
            'handicap1': handicap1, 'handicap2': handicap2,
            'handicap_odds1': handicap_odds1, 'handicap_odds2': handicap_odds2,
            'p_id': m.get('p_id', ''),                # <-- ДОБАВЛЕНО (пробрасываем p_id)
        }

    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for eid, m in list(self._matches_cache.items()):
            if eid in self._finished:
                continue

            parsed = self._parse_match_data(eid)
            if not parsed:
                continue

            first_seen = self._first_seen.get(eid, current_time)
            if parsed['odds1'] == 0 and parsed['odds2'] == 0 and current_time - first_seen < 30:
                continue

            last_sent = self._last_sent_time.get(eid, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (parsed['score1'], parsed['score2'],
                             parsed['sub1'], parsed['sub2'],
                             parsed['odds1'], parsed['odds2'],
                             parsed['total_line'], parsed['total_over'],
                             parsed['total_under'], parsed['handicap1'],
                             parsed['handicap2'], parsed['handicap_odds1'],
                             parsed['handicap_odds2'])
            if m.get('_last_sent') == current_state:
                continue

            # ------------------------------------------------
            #  ДОБАВЛЕНО: собираем URL матча
            #  Формат (по вашему примеру):
            #  https://www.ligastavok.ru/sports/table-tennis/
            #     vlasenko-e-skrobot-p-id-23587159-service-id-27-ext-id-1187530
            # ------------------------------------------------
            slug = _slug(f"{parsed['player1']}-{parsed['player2']}")
            p_id = parsed.get('p_id') or '0'
            match_url = (
                f"https://www.ligastavok.ru/sports/table-tennis/"
                f"{slug}-p-id-{p_id}-service-id-27-ext-id-{eid}"
            )
            # ------------------------------------------------

            match = Match(
                bk_id='ligastavok', match_id=eid,
                player1=parsed['player1'], player2=parsed['player2'],
                score1=parsed['score1'], score2=parsed['score2'],
                sub_score1=parsed['sub1'], sub_score2=parsed['sub2'],
                tournament=parsed['tournament'],
                odds1=parsed['odds1'], odds2=parsed['odds2'],
                total_line=parsed['total_line'], total_over=parsed['total_over'], total_under=parsed['total_under'],
                handicap1=parsed['handicap1'], handicap2=parsed['handicap2'],
                handicap_odds1=parsed['handicap_odds1'], handicap_odds2=parsed['handicap_odds2'],
                timestamp=current_time, raw_time=parsed['comment'],
                match_url=match_url,                     # <-- ДОБАВЛЕНО
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            m['_last_sent'] = current_state
            self._last_sent_time[eid] = current_time
            sent += 1

            is_update = " 🔄 LIVE" if eid in self._first_seen and (current_time - self._first_seen[eid]) > 5 else ""

            logger.info(f"[{self.bk_id}] 🟢 Отправлен{is_update}: {match.player1} vs {match.player2} | "
                        f"{match.score1}:{match.score2} (сет: {match.sub_score1}:{match.sub_score2}) | "
                        f"К: {match.odds1}/{match.odds2} | Т: {match.total_line} | Ф: {match.handicap1} | "
                        f"URL: {match_url}")

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено обновлений: {sent}, всего матчей в кеше: {len(self._matches_cache)}")

    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер (HTTP + WS) запущен")
        await self.start()
        asyncio.create_task(self._process_queues())

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