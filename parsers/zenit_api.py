# parsers/zenit_api.py
"""
Zenit API-парсер с поддержкой мультиспорта (НТ / волейбол / баскетбол / кибербаскет).

Архитектура:
  - HTTP `/ajax/live/video/get_list` — снапшот списка матчей:
      result.games[] = {gid, name, sid, type, stats}
      Даёт gid+name+sid (для новых матчей).
  - WS `wss://zenit.win/wss` — три типа фреймов:
      t=20 (основной поток, ~1.5/сек): плоский снапшот изменений.
          d.sports         — dict {"533": {enSport, name, ...}}
          d.championships  — dict {"460051": {sportId, sportChampionship, ...}}
          d.matches        — dict {"gid": {championshipId, team1, team2, score, sScore, odds, bl, ...}}
      t=21 (редко): та же структура, что t=20.
      t=7: {removedIds: [...]} — удаления. Иногда полный снапшот дерева (sports list).

Sport матча — через championshipId → championships[cid].sportId → sport_key.

Score — через sScore.sScoreData:
  scs[0]  = общий счёт
  scs[1+] = партии / сеты / четверти / таймы
  scs[-1] = активная фаза
  sd      = название фазы ("3 четверть", "2 партия", "Перерыв")

Odds (одинаковые для всех видов):
  "1" = П1,  "3" = П2
  "7" = Фора 1 (oddKey "...|9|<line>"),  "8" = Фора 2 (oddKey "...|10|<line>")
  "9" = Тотал М (oddKey "...|11|<line>"), "10" = Тотал Б (oddKey "...|12|<line>")
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


class ZenitApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('zenit', detector=detector, aggregator=aggregator)

        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]

        self.url = SPORT_URLS.get("_all", {}).get("zenit", self.url)
        logger.info(f"[{self.bk_id}] Стартовый URL: {self.url}")

        self._data_queue = asyncio.Queue()
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False

        # sid (str) → sport_key
        self._sport_ids: Dict[str, str] = {}
        for sport_key in self.enabled_sports:
            cfg = SPORT_MAP.get('zenit', {}).get(sport_key, {})
            for sid in cfg.get('ids', []):
                self._sport_ids[str(sid)] = sport_key
        logger.info(f"[{self.bk_id}] sport_ids: {self._sport_ids}")

        # championshipId (int) → sportId (int)
        self._championship_sport: Dict[int, int] = {}

        # счётчики для дебага
        self._ws_frame_count = 0
        self._ws_t20_count = 0
        self._ws_t7_count = 0
        self._ws_other_count = 0
        self._last_stats_log = 0.0

    # ============================================================
    # Запуск
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
                logger.debug(f"[{self.bk_id}] 📥 Перехвачен get_list")
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
            data = json.loads(payload)
            t = data.get('t')
            if t not in (7, 20, 21):
                return
            asyncio.create_task(self._data_queue.put(('ws', data)))
        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.error(f"[{self.bk_id}] Ошибка WS-фрейма: {e}", exc_info=True)

    async def _process_queues(self):
        while self.is_running:
            try:
                msg_type, data = await asyncio.wait_for(
                    self._data_queue.get(), timeout=1.0
                )
                if msg_type == 'http':
                    self._process_http(data)
                elif msg_type == 'ws':
                    self._process_ws(data)
                await self._try_send_matches()
                self._log_stats_periodically()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка очереди: {e}", exc_info=True)

    def _log_stats_periodically(self):
        now = time.time()
        if now - self._last_stats_log < 30:
            return
        self._last_stats_log = now
        with_sport = sum(1 for m in self._matches_cache.values() if m.get('sport'))
        with_odds = sum(1 for m in self._matches_cache.values()
                        if m.get('odds1', 0) > 0 or m.get('odds2', 0) > 0)
        with_score = sum(1 for m in self._matches_cache.values()
                         if m.get('score1', 0) > 0 or m.get('score2', 0) > 0
                         or m.get('sub1', 0) > 0 or m.get('sub2', 0) > 0)
        logger.info(
            f"[{self.bk_id}] 📊 WS: t20={self._ws_t20_count}, t7={self._ws_t7_count}, "
            f"other={self._ws_other_count} | cache={len(self._matches_cache)} "
            f"(sport={with_sport}, score={with_score}, odds={with_odds})"
        )

    # ============================================================
    # HTTP get_list
    # ============================================================
    def _process_http(self, data: dict):
        try:
            games = data.get('result', {}).get('games', [])
        except AttributeError:
            return

        for game in games:
            sid = str(game.get('sid', ''))
            sport_key = self._sport_ids.get(sid)
            if not sport_key:
                continue

            gid = str(game.get('gid', ''))
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
                self._matches_cache[gid] = {'_last_sent': None}
                self._first_seen[gid] = time.time()

            cache = self._matches_cache[gid]
            cache.setdefault('sport', sport_key)
            if player1.strip() and not cache.get('player1'):
                cache['player1'] = player1.strip()
            if player2.strip() and not cache.get('player2'):
                cache['player2'] = player2.strip()
            cache.setdefault('tournament', 'Zenit Live')

    # ============================================================
    # WS диспетчер
    # ============================================================
    def _process_ws(self, data: dict):
        t = data.get('t')
        d = data.get('d', {})
        if not isinstance(d, dict):
            return

        if t in (20, 21):
            self._ws_t20_count += 1
            self._process_ws_full(d)
        elif t == 7:
            self._ws_t7_count += 1
            self._process_ws_t7(d)
        else:
            self._ws_other_count += 1

    def _process_ws_full(self, d: dict):
        """
        Основной поток (t=20/21). Плоский снапшот:
          d.sports (dict), d.championships (dict), d.matches (dict), d.removedIds
        """
        # 1) sports
        sports = d.get('sports')
        if isinstance(sports, dict):
            for sid_str, meta in sports.items():
                if not isinstance(meta, dict):
                    continue
                try:
                    int(sid_str)
                except (ValueError, TypeError):
                    pass

        # 2) championships
        championships = d.get('championships')
        if isinstance(championships, dict):
            for cid_str, ch in championships.items():
                if not isinstance(ch, dict):
                    continue
                try:
                    cid = int(cid_str)
                    sid = int(ch.get('sportId'))
                except (ValueError, TypeError):
                    continue
                self._championship_sport[cid] = sid

        # 3) matches
        matches = d.get('matches')
        if isinstance(matches, dict):
            for gid_str, match_info in matches.items():
                self._process_match(gid_str, match_info)

        # 4) removedIds
        removed = d.get('removedIds')
        if isinstance(removed, list):
            for gid in removed:
                self._matches_cache.pop(str(gid), None)

    def _process_ws_t7(self, d: dict):
        """
        t=7 бывает двух видов:
          - {"removedIds": [...]} — удаления
          - {sports: [ {championships: [{matches: [...]}]} ]} — полный снапшот дерева
        """
        removed = d.get('removedIds')
        if isinstance(removed, list):
            for gid in removed:
                self._matches_cache.pop(str(gid), None)

        sports = d.get('sports')
        if not isinstance(sports, list):
            return

        for sport in sports:
            if not isinstance(sport, dict):
                continue
            sid = sport.get('id')
            if sid is None:
                continue
            try:
                sid_int = int(sid)
            except (ValueError, TypeError):
                continue

            sport_key = self._sport_ids.get(str(sid_int))
            if not sport_key:
                continue

            for ch in sport.get('championships', []):
                if not isinstance(ch, dict):
                    continue
                cid = ch.get('id')
                if cid is not None:
                    try:
                        self._championship_sport[int(cid)] = sid_int
                    except (ValueError, TypeError):
                        pass

                champ_name = ch.get('name', '')
                for m in ch.get('matches', []):
                    if not isinstance(m, dict):
                        continue
                    gid = str(m.get('id', ''))
                    if not gid:
                        continue
                    if gid not in self._matches_cache:
                        self._matches_cache[gid] = {'_last_sent': None}
                        self._first_seen[gid] = time.time()
                    c = self._matches_cache[gid]
                    c.setdefault('sport', sport_key)
                    c.setdefault('tournament', champ_name or 'Zenit Live')
                    if m.get('team1'):
                        c['player1'] = m['team1']
                    if m.get('team2'):
                        c['player2'] = m['team2']

    # ============================================================
    # Обработка матча из t=20/21
    # ============================================================
    def _process_match(self, gid_str: str, match_info):
        if not isinstance(match_info, dict):
            return
        if match_info.get('bl') == 1:
            return  # заблокирован

        gid = str(gid_str)

        if gid not in self._matches_cache:
            sport_key = self._resolve_sport_key(match_info.get('championshipId'))
            if not sport_key:
                return  # неизвестный спорт — пропускаем
            self._matches_cache[gid] = {'_last_sent': None}
            self._first_seen[gid] = time.time()
            cache = self._matches_cache[gid]
            cache['sport'] = sport_key
            cache.setdefault('tournament', 'Zenit Live')
        else:
            cache = self._matches_cache[gid]
            if not cache.get('sport'):
                sport_key = self._resolve_sport_key(match_info.get('championshipId'))
                if sport_key:
                    cache['sport'] = sport_key

        if match_info.get('team1'):
            cache['player1'] = match_info['team1']
        if match_info.get('team2'):
            cache['player2'] = match_info['team2']

        sport_key = cache.get('sport')
        if not sport_key:
            return

        if 'sScore' in match_info or 'score' in match_info:
            self._parse_ws_score(cache, match_info, sport_key)

        odds_data = match_info.get('odds')
        if isinstance(odds_data, dict) and odds_data:
            self._parse_ws_odds(cache, odds_data, match_info.get('mainLine', []))

    def _resolve_sport_key(self, championship_id) -> Optional[str]:
        if championship_id is None:
            return None
        try:
            cid = int(championship_id)
        except (ValueError, TypeError):
            return None
        sid = self._championship_sport.get(cid)
        if sid is None:
            return None
        return self._sport_ids.get(str(sid))

    # ============================================================
    # Парсинг score через sScore
    # ============================================================
    def _parse_ws_score(self, cache: dict, match_info: dict, sport_key: str):
        sd_data = (match_info.get('sScore') or {}).get('sScoreData') or {}
        scs = sd_data.get('scs') or []
        sd = (sd_data.get('sd') or '').strip()

        # общий счёт = scs[0]
        if scs and isinstance(scs[0], dict):
            cur = (scs[0].get('scv') or {}).get('cur') or {}
            try:
                cache['score1'] = int(cur.get('t1', 0))
                cache['score2'] = int(cur.get('t2', 0))
            except (ValueError, TypeError):
                pass

        # sub_score = scs[-1]
        sub1 = sub2 = 0
        if scs and isinstance(scs[-1], dict):
            cur = (scs[-1].get('scv') or {}).get('cur') or {}
            try:
                sub1 = int(cur.get('t1', 0))
                sub2 = int(cur.get('t2', 0))
            except (ValueError, TypeError):
                pass
        cache['sub1'] = sub1
        cache['sub2'] = sub2

        # phase_num
        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            if 'Перерыв' in sd:
                # последняя четверть в scs — ещё не началась
                phase_num = max(1, len(scs) - 1)
                cache['sub1'] = cache['sub2'] = 0
            else:
                m = re.search(r'(\d+)', sd)
                if m:
                    phase_num = int(m.group(1))
                else:
                    phase_num = max(1, len(scs) - 1)
        else:
            # НТ / волейбол: фаза = сумма партий/сетов + 1
            s1 = cache.get('score1', 0) or 0
            s2 = cache.get('score2', 0) or 0
            phase_num = s1 + s2 + 1

        cache['phase_num'] = phase_num

    # ============================================================
    # Парсинг odds
    # ============================================================
    def _parse_ws_odds(self, cache: dict, odds_data: dict, main_line: list):
        def _cf(key):
            v = odds_data.get(key)
            if isinstance(v, dict):
                try:
                    return float(v.get('cf', 0) or 0)
                except (ValueError, TypeError):
                    return 0.0
            return 0.0

        def _line(key):
            v = odds_data.get(key)
            if isinstance(v, dict):
                odd_key = v.get('oddKey', '')
                parts = odd_key.split('|')
                if len(parts) >= 3:
                    try:
                        return float(parts[2])
                    except ValueError:
                        return 0.0
            return 0.0

        cache['odds1'] = _cf('1')
        cache['odds2'] = _cf('3')
        cache['handicap1'] = _line('7')
        cache['handicap_odds1'] = _cf('7')
        cache['handicap2'] = _line('8')
        cache['handicap_odds2'] = _cf('8')
        cache['total_under'] = _cf('9')
        cache['total_over'] = _cf('10')
        cache['total_line'] = _line('9') or _line('10')

    # ============================================================
    # Отправка в detector
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get('player1') or not m.get('player2'):
                continue
            if not m.get('sport'):
                continue

            sport_key = m['sport']

            first_seen = self._first_seen.get(match_id, current_time)
            has_odds = (m.get('odds1', 0) > 0 or m.get('odds2', 0) > 0)
            has_score = (
                m.get('score1', 0) > 0 or m.get('score2', 0) > 0
                or m.get('sub1', 0) > 0 or m.get('sub2', 0) > 0
            )
            if not has_odds and not has_score:
                continue
            if not has_odds and current_time - first_seen < 15:
                continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

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

            phase_name = format_phase(sport_key, m.get('phase_num', 1))

            sid = None
            cfg = SPORT_MAP.get('zenit', {}).get(sport_key, {})
            ids = cfg.get('ids', [])
            if ids:
                sid = ids[0]
            match_url = f"https://zenit.win/live/{sid}/{match_id}" if sid \
                else f"https://zenit.win/live/{match_id}"

            match = Match(
                bk_id='zenit',
                match_id=match_id,
                player1=m['player1'],
                player2=m['player2'],
                score1=m.get('score1', 0),
                score2=m.get('score2', 0),
                sub_score1=m.get('sub1', 0),
                sub_score2=m.get('sub2', 0),
                tournament=m.get('tournament', 'Zenit Live'),
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
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Zenit запущен")
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
                self.page.remove_listener("websocket", self._handle_websocket)
            except Exception:
                pass
            await browser_manager.close_page(self.page)
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")