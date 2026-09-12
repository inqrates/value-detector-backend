# parsers/ligastavok_api.py
"""
LigaStavok API-парсер с поддержкой мультиспорта (НТ / волейбол / баскетбол / кибербаскет).

Перехват: HTTP `/rest/events/v8/eventsList` + WS `lds-api-sites.ligastavok.ru/ws`.

Особенности:
  - HTTP отдаёт ПОРЦИЯМИ по ~40 событий. Разные виды приходят в разных ответах.
    Кэш накапливается, матчи не теряются.
  - Вид спорта — по item.categorySeoName (ВЕРХНИЙ уровень item, не внутри item.event):
      nastolnyi-tennis → table_tennis
      voleibol         → volleyball
      basketbol        → basketball
      kiberbasketbol   → cyber_basketball
    Fallback: по item.gameId (1246/128/25/23139).

Счёт (единая структура для всех видов):
  scores.total.ScoreTeam1/ScoreTeam2   — общий счёт (партии/сеты/очки)
  scores.current.ScoreTeam1/ScoreTeam2 — очки активной фазы
  scores.all[]                          — массив всех фаз
  event.statusTranslated                — словесная фаза («2-я четверть»)

Фазы:
  НТ / волейбол: phase_num = total.ScoreTeam1 + total.ScoreTeam2 + 1
  Баскетбол:     phase_num = число из event.statusTranslated

Кэфы — по outcomeKey (проверено дампом):
  '_1'    → odds1 (П1)
  '_2'    → odds2 (П2)
  'gross' → total_over,  total_line = adValue
  'less'  → total_under, total_line = adValue
  '1'     → handicap1,   handicap_odds1 = value, линия = adValue
  '2'     → handicap2,   handicap_odds2 = value, линия = adValue
"""
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
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME, SPORT_URLS
from core.sport_map import (
    SPORT_MAP, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)

logger = logging.getLogger(__name__)


# categorySeoName (верхний уровень item) → sport_key
SEO_TO_SPORT = {
    "nastolnyi-tennis": TABLE_TENNIS,
    "voleibol":         VOLLEYBALL,
    "basketbol":        BASKETBALL,
    "kiberbasketbol":   CYBER_BASKETBALL,
}


def _slug(text: str) -> str:
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


class LigaStavokApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('ligastavok', detector=detector, aggregator=aggregator)

        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]

        self.url = SPORT_URLS.get("_all", {}).get("ligastavok", self.url)
        logger.info(f"[{self.bk_id}] Используем общий лайв: {self.url}")

        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._finished: set = set()
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False

        # gameId → sport_key (fallback)
        self._sport_ids: Dict[str, str] = {}
        for sport_key in self.enabled_sports:
            cfg = SPORT_MAP.get('ligastavok', {}).get(sport_key, {})
            for sid in cfg.get('ids', []):
                self._sport_ids[str(sid)] = sport_key
        logger.info(f"[{self.bk_id}] sport_ids: {self._sport_ids}")

    # ============================================================
    # Запуск и перехват
    # ============================================================
    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            self.page.on("response", self._handle_response)
            self.page.on("websocket", self._handle_websocket)

            logger.info(f"[{self.bk_id}] Загрузка страницы {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded',
                                 timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)

            # Программно растягиваем body — trigger для lazy load всех секций сразу
            try:
                await self.page.evaluate(
                    "() => { document.body.style.minHeight = '10000px'; }"
                )
            except Exception:
                pass
            await asyncio.sleep(3)

            # Короткий скролл для триггера scroll-событий
            try:
                await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(1.2)
                await self.page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(0.5)
            except Exception:
                pass

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехватчики (HTTP + WS) активны")
            asyncio.create_task(self._process_queues())

    async def _handle_response(self, response: Response):
        url = response.url
        if '/rest/events/v8/eventsList' not in url:
            return
        try:
            data = await response.json()
            items = (data.get('result') or {}).get('data') or []
            logger.debug(f"[{self.bk_id}] 📥 eventsList: {len(items)} событий")
            await self._data_queue.put(('http', data))
        except Exception as e:
            logger.warning(f"[{self.bk_id}] Ошибка парсинга HTTP: {e}")

    async def _handle_websocket(self, ws: WebSocket):
        if 'lds-api-sites.ligastavok.ru/ws' in ws.url:
            logger.info(f"[{self.bk_id}] 🔌 WebSocket подключён")
            ws.on("framereceived", self._process_ws_frame)

    def _process_ws_frame(self, frame):
        try:
            payload_str = frame if isinstance(frame, str) else (
                frame.payload if hasattr(frame, 'payload') else str(frame)
            )
            data = json.loads(payload_str)

            if not isinstance(data, dict):
                return
            if data.get("id") is not None:
                return

            result = data.get("result")
            if not isinstance(result, dict):
                return

            payload = result.get("payload")
            if not isinstance(payload, list):
                return

            for item in payload:
                if not isinstance(item, dict):
                    continue

                match_id = str(item.get("id", ""))
                if not match_id or match_id in self._finished:
                    continue

                if match_id not in self._matches_cache:
                    continue

                cache = self._matches_cache[match_id]
                sport_key = cache.get('sport', TABLE_TENNIS)

                ws_data = item.get("data") or {}
                if not isinstance(ws_data, dict):
                    ws_data = {}

                self._apply_ws_data(cache, ws_data, sport_key)

                if cache.get('event', {}).get('statusTranslated') == 'Матч завершен':
                    self._finished.add(match_id)
                    logger.info(f"[{self.bk_id}] 🏁 Матч {match_id} завершён (из WS)")

        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.error(f"[{self.bk_id}] Ошибка обработки WS-кадра: {e}", exc_info=True)

    def _apply_ws_data(self, cache: dict, ws_data: dict, sport_key: str):
        headers = ws_data.get("headers") or []
        if isinstance(headers, list):
            for h in headers:
                if not isinstance(h, dict):
                    continue
                path = h.get("path", "")
                val = h.get("value")
                if val is None:
                    continue

                scores = cache.setdefault('scores', {})
                if path == "/scores/total/ScoreTeam1":
                    scores.setdefault('total', {})['ScoreTeam1'] = str(val)
                elif path == "/scores/total/ScoreTeam2":
                    scores.setdefault('total', {})['ScoreTeam2'] = str(val)
                elif path == "/scores/current/ScoreTeam1":
                    scores.setdefault('current', {})['ScoreTeam1'] = str(val)
                elif path == "/scores/current/ScoreTeam2":
                    scores.setdefault('current', {})['ScoreTeam2'] = str(val)
                elif path == "/event/statusTranslated":
                    cache.setdefault('event', {})['statusTranslated'] = val

        outcomes_upd = ws_data.get("outcomes") or []
        if isinstance(outcomes_upd, list):
            for op in outcomes_upd:
                if not isinstance(op, dict):
                    continue
                op_type = op.get("op")
                path = op.get("path", "")
                val = op.get("value")
                if op_type not in ("add", "replace"):
                    continue
                parts = path.split("/")
                if len(parts) >= 3 and parts[1] == "outcomes":
                    out_id = parts[2]
                    field = parts[3] if len(parts) >= 4 else None
                    outcomes_cache = cache.setdefault('outcomes', {})
                    if isinstance(val, dict):
                        outcomes_cache[out_id] = {
                            'outcomeKey': val.get('outcomeKey'),
                            'value': float(val.get('value', 0) or 0),
                            'adValue': val.get('adValue'),
                            'marketId': val.get('marketId'),
                        }
                    elif field == "value":
                        outcomes_cache.setdefault(out_id, {})['value'] = float(val or 0)
                    elif field == "adValue":
                        outcomes_cache.setdefault(out_id, {})['adValue'] = val

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

    # ============================================================
    # HTTP
    # ============================================================
    def _resolve_sport_key(self, m: dict) -> Optional[str]:
        seo = (m.get("categorySeoName") or "").strip()
        sport = SEO_TO_SPORT.get(seo)
        if sport and sport in self.enabled_sports:
            return sport

        gid = str(m.get("gameId", ""))
        sport = self._sport_ids.get(gid)
        if sport and sport in self.enabled_sports:
            return sport
        return None

    def _process_http(self, data: dict):
        if not data or 'result' not in data or 'data' not in data['result']:
            return

        items = data['result']['data']
        if not isinstance(items, list):
            return

        for m in items:
            if not isinstance(m, dict):
                continue

            match_id = str(m.get('id', ''))
            if not match_id or match_id in self._finished:
                continue

            sport_key = self._resolve_sport_key(m)
            if not sport_key:
                continue

            if match_id not in self._matches_cache:
                self._matches_cache[match_id] = {
                    'event': {}, 'scores': {}, 'outcomes': {}, 'markets': {},
                    'player1': '', 'player2': '', 'tournament': '',
                    'sport': sport_key, '_last_sent': None,
                }
                self._first_seen[match_id] = time.time()

            cache = self._matches_cache[match_id]
            cache['sport'] = sport_key

            event = m.get('event')
            if isinstance(event, dict) and event:
                cache.setdefault('event', {})
                cache['event'].update(event)

                p1 = event.get('team1')
                p2 = event.get('team2')
                if p1:
                    cache['player1'] = p1
                if p2:
                    cache['player2'] = p2

                t = event.get('tournamentTitle') or event.get('topicTitle')
                if t:
                    cache['tournament'] = t

            if m.get('scores'):
                cache['scores'] = m['scores']
            if m.get('markets'):
                cache['markets'] = m['markets']
            if m.get('outcomes'):
                cache['outcomes'] = m['outcomes']

            if not cache.get('tournament'):
                cache['tournament'] = 'LigaStavok'

            if cache.get('event', {}).get('statusTranslated') == 'Матч завершен':
                self._finished.add(match_id)

    # ============================================================
    # Сборка Match из кэша
    # ============================================================
    def _parse_match_data(self, match_id: str) -> Optional[dict]:
        m = self._matches_cache.get(match_id)
        if not m:
            return None

        player1 = m.get('player1') or ''
        player2 = m.get('player2') or ''
        if not player1 or not player2:
            return None

        sport_key = m.get('sport', TABLE_TENNIS)

        scores = m.get('scores') or {}
        total = scores.get('total') or {}
        current = scores.get('current') or {}
        all_scores = scores.get('all') or []

        try:
            score1 = int(total.get('ScoreTeam1', 0) or 0)
            score2 = int(total.get('ScoreTeam2', 0) or 0)
        except (ValueError, TypeError):
            score1 = score2 = 0

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            st = ((m.get('event') or {}).get('statusTranslated') or '').strip()

            if 'перерыв' in st.lower():
                phase_num = max(1, len(all_scores))
                sub1 = sub2 = 0
            else:
                mm = re.search(r'(\d+)', st)
                if mm:
                    phase_num = int(mm.group(1))
                else:
                    phase_num = max(1, len(all_scores))

                if 0 < phase_num <= len(all_scores):
                    cur = all_scores[phase_num - 1] or {}
                    try:
                        sub1 = int(cur.get('ScoreTeam1', 0) or 0)
                        sub2 = int(cur.get('ScoreTeam2', 0) or 0)
                    except (ValueError, TypeError):
                        sub1 = sub2 = 0
                else:
                    try:
                        sub1 = int(current.get('ScoreTeam1', 0) or 0)
                        sub2 = int(current.get('ScoreTeam2', 0) or 0)
                    except (ValueError, TypeError):
                        sub1 = sub2 = 0
        else:
            phase_num = score1 + score2 + 1
            try:
                sub1 = int(current.get('ScoreTeam1', 0) or 0)
                sub2 = int(current.get('ScoreTeam2', 0) or 0)
            except (ValueError, TypeError):
                sub1 = sub2 = 0

        # ── Кэфы по outcomeKey (как в старой рабочей версии) ──
        outcomes = m.get('outcomes') or {}

        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0

        for out in outcomes.values():
            if not isinstance(out, dict):
                continue

            key = out.get('outcomeKey')

            try:
                val = float(out.get('value', 0) or 0)
            except (ValueError, TypeError):
                val = 0.0

            adval_raw = out.get('adValue')
            try:
                adval = float(adval_raw) if adval_raw is not None else 0.0
            except (ValueError, TypeError):
                adval = 0.0

            if key == '_1':
                odds1 = val
            elif key == '_2':
                odds2 = val
            elif key == 'gross':
                total_over = val
                total_line = adval
            elif key == 'less':
                total_under = val
                total_line = adval
            elif key == '1':
                # Форы могут быть несколько — берём первую попавшуюся ненулевую
                if h1 == 0.0 and h_o1 == 0.0:
                    h1 = adval
                    h_o1 = val
            elif key == '2':
                if h2 == 0.0 and h_o2 == 0.0:
                    h2 = adval
                    h_o2 = val

        event = m.get('event') or {}
        return {
            'sport': sport_key,
            'player1': player1, 'player2': player2,
            'score1': score1, 'score2': score2,
            'sub1': sub1, 'sub2': sub2,
            'phase_num': phase_num,
            'tournament': m.get('tournament', ''),
            'odds1': odds1, 'odds2': odds2,
            'total_line': total_line, 'total_over': total_over, 'total_under': total_under,
            'handicap1': h1, 'handicap2': h2,
            'handicap_odds1': h_o1, 'handicap_odds2': h_o2,
            'p_id': self._extract_p_id(event),
        }

    def _extract_p_id(self, event: dict) -> str:
        competitors = event.get('competitors') or []
        if isinstance(competitors, list) and competitors:
            first = competitors[0]
            if isinstance(first, dict):
                pid = first.get('id')
                if pid:
                    return str(pid)
        return '0'

    # ============================================================
    # Отправка
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if match_id in self._finished:
                continue

            parsed = self._parse_match_data(match_id)
            if not parsed:
                continue

            sport_key = parsed['sport']

            first_seen = self._first_seen.get(match_id, current_time)
            if parsed['odds1'] == 0 and parsed['odds2'] == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (
                parsed['score1'], parsed['score2'],
                parsed['sub1'], parsed['sub2'],
                parsed['phase_num'],
                parsed['odds1'], parsed['odds2'],
                parsed['total_line'], parsed['total_over'], parsed['total_under'],
                parsed['handicap1'], parsed['handicap2'],
                parsed['handicap_odds1'], parsed['handicap_odds2'],
            )
            if m.get('_last_sent') == current_state:
                continue

            phase_name = format_phase(sport_key, parsed['phase_num'])

            slug = get_url_slug('ligastavok', sport_key) or 'table-tennis'
            p_id = parsed.get('p_id') or '0'
            player_slug = _slug(f"{parsed['player1']}-{parsed['player2']}")
            match_url = (
                f"https://www.ligastavok.ru/sports/{slug}/"
                f"{player_slug}-p-id-{p_id}-service-id-27-ext-id-{match_id}"
            )

            match = Match(
                bk_id='ligastavok',
                match_id=match_id,
                player1=parsed['player1'],
                player2=parsed['player2'],
                score1=parsed['score1'],
                score2=parsed['score2'],
                sub_score1=parsed['sub1'],
                sub_score2=parsed['sub2'],
                tournament=parsed['tournament'],
                odds1=parsed['odds1'],
                odds2=parsed['odds2'],
                total_line=parsed['total_line'],
                total_over=parsed['total_over'],
                total_under=parsed['total_under'],
                handicap1=parsed['handicap1'],
                handicap2=parsed['handicap2'],
                handicap_odds1=parsed['handicap_odds1'],
                handicap_odds2=parsed['handicap_odds2'],
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
        logger.info(f"[{self.bk_id}] 🚀 API-парсер LigaStavok запущен")
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