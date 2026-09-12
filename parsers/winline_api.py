# parsers/winline_api.py
"""
Winline API-парсер с поддержкой мультиспорта (НТ / волейбол / баскетбол / кибербаскет).

Особенности:
  - Единый WebSocket wss.winline.ru/data_ng, бинарные фреймы.
  - Декодирование через parsers.decoder.DataNgDecoder.
  - Вид спорта — по event.sportId:
      20  → table_tennis
      23  → volleyball
      2   → basketball
      193 → cyber_basketball  (Кибер NBA)
      153 → cyber_basketball  (ESport NBA2K, страховка)

Формат данных:
  event.score      = "1:0"                    — партии/сеты/общий счёт
  event.setScores  = "11:9 - 4:3"             — части через " - ", последняя = активная
  event.time       = "2сет" / "1Ч 9:59"       — фаза (для баскетбола "NЧ")
  event.state      = 1 (идёт), 2, 3 (завершено)

Кэфы (market → значения):
  market='1' type=1        → П1/П2 (values=[П1, П2])
  market='Больше' type=4   → тотал матча (values=[ТБ, ТМ], coeff=линия)
  market='Больше' type=71  → тотал партии/четверти
  market='1' type=3        → фора матча (values=[Ф1, Ф2], coeff=линия)
"""
import asyncio
import logging
import re
import time
from typing import Dict, Optional, List
from playwright.async_api import WebSocket
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME, ZOOM, SPORT_URLS
from core.sport_map import (
    get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)
from parsers.decoder import DataNgDecoder, WebSocketDecodeError

logger = logging.getLogger(__name__)


class WinlineApiParser(BaseParser):
    # sportId (Winline) → sport_key
    SPORT_IDS = {
        20:  TABLE_TENNIS,
        23:  VOLLEYBALL,
        2:   BASKETBALL,
        193: CYBER_BASKETBALL,    # Кибер NBA
        153: CYBER_BASKETBALL,    # ESport NBA2K
    }

    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('winline', detector=detector, aggregator=aggregator)

        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]

        # Общий лайв Winline (одна страница на все виды)
        self.url = SPORT_URLS.get("_all", {}).get("winline", self.url)
        logger.info(f"[{self.bk_id}] Используем общий лайв: {self.url}")

        self._decoder = DataNgDecoder()

        # Кэши
        self._events_cache: Dict[int, dict] = {}         # event_id → merged event
        self._lines_cache: Dict[int, Dict[int, dict]] = {}  # event_id → {line_id: line}
        self._first_seen: Dict[int, float] = {}
        self._last_sent_time: Dict[int, float] = {}

        self.is_running = False
        self._frame_count = 0

    # ============================================================
    # Запуск
    # ============================================================
    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            self.page.on("websocket", self._handle_websocket)

            logger.info(f"[{self.bk_id}] Загрузка страницы {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded',
                                 timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехват WS активен")

    def _handle_websocket(self, ws: WebSocket):
        logger.info(f"[{self.bk_id}] 🔌 WebSocket: {ws.url}")
        if 'wss.winline.ru/data_ng' in ws.url:
            ws.on("framereceived", self._on_frame)

    def _on_frame(self, frame):
        self._frame_count += 1
        try:
            payload = frame if isinstance(frame, (bytes, bytearray)) else (
                frame.payload if hasattr(frame, 'payload') else None
            )
            if not payload:
                return

            decoded = self._decoder.decode(payload)
            if decoded is None:
                return  # menu (step 16)

            items = decoded if isinstance(decoded, list) else [decoded]
            for item in items:
                if not isinstance(item, dict):
                    continue
                t = item.get("type")
                if t in ("prematch", "live"):
                    self._apply_events(item.get("events") or [])
                    self._apply_lines(item.get("lines") or [])

        except WebSocketDecodeError:
            pass
        except Exception as e:
            logger.debug(f"[{self.bk_id}] кадр: {e}")

    # ============================================================
    # События
    # ============================================================
    def _apply_events(self, events: list):
        for ev in events:
            if not isinstance(ev, dict):
                continue
            eid = ev.get("id")
            if eid is None:
                continue

            if ev.get("deleted"):
                self._events_cache.pop(eid, None)
                self._lines_cache.pop(eid, None)
                self._first_seen.pop(eid, None)
                self._last_sent_time.pop(eid, None)
                continue

            old = self._events_cache.get(eid)

            # ── Фильтр prematch ──
            # Не храним события без признаков live: state=None,
            # score пустой/'-:-', time пустой. Их тысячи — память и CPU.
            # Когда матч начнётся, придёт новое событие со state != None.
            state = ev.get("state")
            score_raw = (ev.get("score") or "").strip()
            time_raw = (ev.get("time") or "").strip()

            has_live_data = (
                state is not None
                or (score_raw and score_raw not in ("-:-", "-"))
                or bool(time_raw)
            )

            if not has_live_data and old is None:
                continue  # чистый prematch, ещё не видели как live — не храним

            merged = {**(old or {}), **ev}
            self._events_cache[eid] = merged

            if eid not in self._first_seen:
                self._first_seen[eid] = time.time()

    def _apply_lines(self, lines: list):
        for ln in lines:
            if not isinstance(ln, dict):
                continue
            lid = ln.get("id")
            eid = ln.get("eventId")
            if lid is None or eid is None:
                continue

            bucket = self._lines_cache.setdefault(eid, {})
            if ln.get("deleted"):
                bucket.pop(lid, None)
                continue

            old = bucket.get(lid, {})
            bucket[lid] = {**old, **ln}

    # ============================================================
    # Парсинг события → Match
    # ============================================================
    def _parse_event(self, eid: int, ev: dict) -> Optional[dict]:
                # Пропускаем prematch: у него нет ни score, ни time
        score_raw = (ev.get("score") or "").strip()
        time_raw = (ev.get("time") or "").strip()
        state = ev.get("state")

        # state=None → точно prematch
        if state is None:
            return None

        # score пустой или '-:-' И time пустое → ещё не начался
        if (score_raw in ("", "-:-") and not time_raw):
            return None
        sport_id = ev.get("sportId")
        sport_key = self.SPORT_IDS.get(sport_id)
        if not sport_key or sport_key not in self.enabled_sports:
            return None

        participants = ev.get("participants") or []
        if not isinstance(participants, list) or len(participants) < 2:
            return None
        p1, p2 = participants[0], participants[1]
        if not p1 or not p2:
            return None

                # score "X:Y" (уже отфильтровали пустые выше)
        try:
            a, b = score_raw.split(":", 1)
            score1, score2 = int(a), int(b)
        except ValueError:
            score1 = score2 = 0

        # setScores "A:B - C:D - E:F"
        set_scores_str = ev.get("setScores") or ""
        sub1 = sub2 = 0
        parts = []
        if set_scores_str:
            parts = [p.strip() for p in set_scores_str.split(" - ") if p.strip()]
            if parts:
                try:
                    a, b = parts[-1].split(":", 1)
                    sub1, sub2 = int(a), int(b)
                except ValueError:
                    pass

        # phase_num
        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            time_str = ev.get("time") or ""
            m = re.search(r'(\d+)\s*Ч', time_str)
            if m:
                phase_num = int(m.group(1))
            else:
                # fallback: количество частей в setScores
                phase_num = len(parts) if parts else 1
        else:
            # НТ / волейбол: фаза = сумма партий/сетов + 1
            phase_num = score1 + score2 + 1

        # Кэфы из lines
        lines = self._lines_cache.get(eid, {})
        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0

        for ln in lines.values():
            market = ln.get("market") or ""
            mtype = ln.get("marketType")
            values = ln.get("values") or []
            coeff = ln.get("coefficient")
            if not isinstance(values, list) or len(values) < 2:
                continue

            # П1/П2 — market='1', type=1 (основной)
            if market == '1' and mtype == 1 and len(values) >= 2:
                try:
                    odds1 = float(values[0])
                    odds2 = float(values[1])
                except (ValueError, TypeError):
                    pass

            # Тотал матча — market='Больше', type=4
            elif market == 'Больше' and mtype == 4:
                try:
                    total_over = float(values[0])
                    total_under = float(values[1])
                    if coeff:
                        total_line = float(str(coeff).replace(',', '.'))
                except (ValueError, TypeError):
                    pass

            # Фора матча — market='1', type=3 (best effort)
            elif market == '1' and mtype == 3 and h1 == 0.0 and h_o1 == 0.0:
                try:
                    line_val = float(str(coeff).replace(',', '.'))
                    h1 = line_val
                    h_o1 = float(values[0])
                    h2 = -line_val
                    h_o2 = float(values[1])
                except (ValueError, TypeError):
                    pass

        return {
            'sport': sport_key,
            'player1': p1,
            'player2': p2,
            'score1': score1, 'score2': score2,
            'sub1': sub1, 'sub2': sub2,
            'phase_num': phase_num,
            'tournament': ev.get('championship') or 'Winline',
            'odds1': odds1, 'odds2': odds2,
            'total_line': total_line,
            'total_over': total_over,
            'total_under': total_under,
            'handicap1': h1, 'handicap2': h2,
            'handicap_odds1': h_o1, 'handicap_odds2': h_o2,
        }

    # ============================================================
    # Отправка в detector
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for eid, ev in list(self._events_cache.items()):
            # Пропускаем завершённые
            if ev.get("state") in (3, 4):
                continue

            parsed = self._parse_event(eid, ev)
            if not parsed:
                continue

            sport_key = parsed['sport']

            first_seen = self._first_seen.get(eid, current_time)
            # Правило проекта: не фильтровать по кэфам, но дать 15с grace
            if parsed['odds1'] == 0 and parsed['odds2'] == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(eid, 0)
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
            if ev.get('_last_sent') == current_state:
                continue

            phase_name = format_phase(sport_key, parsed['phase_num'])

            # URL матча — короткий ID-only (Winline SPA редиректит)
            slug = get_url_slug('winline', sport_key) or 'nastolijnyj_tennis'
            match_url = f"https://winline.ru/live/sport/{slug}/{eid}"

            match = Match(
                bk_id='winline',
                match_id=str(eid),
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

            ev['_last_sent'] = current_state
            self._last_sent_time[eid] = current_time
            sent += 1

            logger.info(
                f"[{self.bk_id}] 🟢 [{sport_key}] {match.player1} vs {match.player2} | "
                f"матч {match.score1}:{match.score2} | "
                f"{phase_name} {match.sub_score1}:{match.sub_score2} | "
                f"К: {match.odds1}/{match.odds2}"
            )

        # Чистка старых events (не видели >60с)
        ttl = 60.0
        for eid in list(self._events_cache.keys()):
            if current_time - self._first_seen.get(eid, current_time) > ttl:
                if current_time - self._last_sent_time.get(eid, 0) > ttl:
                    self._events_cache.pop(eid, None)
                    self._lines_cache.pop(eid, None)
                    self._first_seen.pop(eid, None)
                    self._last_sent_time.pop(eid, None)

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено: {sent} (в кеше: {len(self._events_cache)})")

    # ============================================================
    # Loop
    # ============================================================
    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Winline (WebSocket) запущен")
        await self.start()

        while self.is_running:
            try:
                await asyncio.sleep(1)
                await self._try_send_matches()
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
                self.page.remove_listener("websocket", self._handle_websocket)
            except Exception:
                pass
            await browser_manager.close_page(self.page)
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")