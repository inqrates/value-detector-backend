# parsers/winline_api.py
import asyncio
import time
import logging
import gzip
import json
import re                                                  # <-- ДОБАВЛЕНО
import struct
from typing import Dict, List, Optional, Any

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
# Декодер бинарного протокола Winline (без изменений)
# ============================================================

class WebSocketDecodeError(ValueError):
    pass


class BinaryReader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.position = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.position

    def read(self, size: int) -> bytes:
        end = self.position + size
        if end > len(self.data):
            raise WebSocketDecodeError(f"Недостаточно данных в позиции {self.position}")
        value = self.data[self.position:end]
        self.position = end
        return value

    def uint8(self) -> int:
        return self.read(1)[0]

    def uint16(self) -> int:
        return int.from_bytes(self.read(2), "little")

    def uint32(self) -> int:
        return int.from_bytes(self.read(4), "little")

    def int32(self) -> int:
        return int.from_bytes(self.read(4), "little", signed=True)

    def string(self) -> str:
        size = self.uint16()
        value = self.read(size).split(b"\x1b", 1)[0]
        return value.decode("utf-8", errors="replace")


class DataNgDecoder:
    def __init__(self) -> None:
        self.sports: dict[int, str] = {}
        self.markets: dict[int, tuple[int, str]] = {}
        self.championships: dict[int, dict[str, Any]] = {}
        self.live_events: dict[int, dict[str, Any]] = {}

    def decode(self, message: bytes) -> dict[str, Any] | list[dict[str, Any]] | None:
        if message.startswith(b"\x1f\x8b"):
            message = gzip.decompress(message)

        if len(message) < 2:
            raise WebSocketDecodeError("Сообщение data_ng слишком короткое")

        step = int.from_bytes(message[:2], "little")
        reader = BinaryReader(message[2:])

        if step == 20_000:
            result: list[dict[str, Any]] = []
            while reader.remaining:
                size = reader.uint32()
                decoded = self.decode(reader.read(size))
                if isinstance(decoded, list):
                    result.extend(decoded)
                elif decoded is not None:
                    result.append(decoded)
            return result or None

        if step == 16:
            sport_count = reader.uint32()
            for _ in range(sport_count):
                sport_id = reader.int32()
                reader.int32()
                self.sports[sport_id] = reader.string()
                for _ in range(9):
                    reader.string()

            market_count = reader.uint32()
            for _ in range(market_count):
                market_id = reader.int32()
                reader.string()
                reader.int32()
                reader.int32()
                market_type = reader.int32()
                reader.string()
                labels = [reader.string() for _ in range(30)]
                self.markets[market_id] = (market_type, labels[0])
            return None

        if step == 3:
            reader.read(16)
            events: list[dict[str, Any]] = []
            lines: list[dict[str, Any]] = []
            current_championship_id: int | None = None

            while reader.remaining:
                record_type = reader.uint8()

                if record_type == 1:
                    reader.int32()
                    reader.int32()
                    reader.string()
                    reader.int32()
                    reader.uint8()
                    reader.uint16()
                    reader.uint16()
                    reader.uint8()

                elif record_type == 2:
                    championship_id = reader.uint32()
                    sport_id = reader.uint32()
                    country_id = reader.uint32()
                    championship_name = reader.string()
                    reader.int32()
                    reader.int32()
                    reader.int32()
                    reader.uint8()
                    reader.int32()
                    reader.int32()

                    self.championships[championship_id] = {
                        "id": championship_id,
                        "name": championship_name,
                        "sportId": sport_id,
                        "sport": self.sports.get(sport_id),
                        "countryId": country_id,
                    }
                    current_championship_id = championship_id

                elif record_type in (3, 34):
                    event = {
                        "id": reader.int32(),
                        "radarId": reader.int32(),
                        "nativeId": reader.int32(),
                        "provider": reader.uint8(),
                        "category": reader.uint8(),
                        "liveId": reader.uint8(),
                        "linesAvailability": reader.uint8(),
                    }
                    reader.read(2)
                    widget_size = reader.uint16()
                    reader.read(widget_size)

                    if record_type == 34:
                        current_championship_id = reader.int32()

                    championship = self.championships.get(current_championship_id or -1, {})
                    event.update(
                        championshipId=current_championship_id,
                        championship=championship.get("name"),
                        sportId=championship.get("sportId"),
                        sport=championship.get("sport"),
                        timestamp=reader.int32(),
                        additionalLines=reader.uint8(),
                        isOD=reader.uint8(),
                        participants=[reader.string(), reader.string()],
                    )
                    events.append(event)

                elif record_type in (4, 43):
                    line_id = reader.uint32()
                    event_id = reader.uint32() if record_type == 43 else events[-1]["id"]
                    market_id = reader.uint16()
                    margin_cash = reader.uint16() / 10_000
                    margin = reader.uint16() / 10_000

                    if market_id not in self.markets:
                        raise WebSocketDecodeError(f"Нет marketId={market_id}")

                    market_type, market_name = self.markets[market_id]
                    line = {
                        "id": line_id,
                        "eventId": event_id,
                        "marketId": market_id,
                        "market": market_name,
                        "marketType": market_type,
                        "marginCash": margin_cash,
                        "margin": margin,
                    }

                    if market_type in (3, 6):
                        line["favorite"] = reader.uint8()
                        line["coefficient"] = reader.uint16() / 100
                    elif market_type in (4, 7):
                        line["coefficient"] = reader.uint16() / 100
                    elif market_type in (51, 151):
                        line["coefficient"] = 1
                    elif market_type == 61:
                        line["favorite"] = reader.uint8()
                        line["coefficient"] = f"1/{reader.uint16() / 100:g}"
                    elif market_type == 71:
                        line["coefficient"] = f"1/{reader.uint16() / 100:g}"

                    value_count = 2
                    if market_type in (2, 5, 51):
                        value_count = 3
                    elif market_type == 9:
                        value_count = 4

                    line["values"] = [reader.uint16() / 100 for _ in range(value_count)]
                    lines.append(line)

                elif record_type == 31:
                    events.append({
                        "id": reader.uint32(),
                        "timestamp": reader.uint32(),
                        "update": True,
                    })

                elif record_type in (32, 42):
                    target = events if record_type == 32 else lines
                    target.append({"id": reader.uint32(), "deleted": True})

                elif record_type == 33:
                    events.append({
                        "id": reader.uint32(),
                        "additionalLines": reader.uint8(),
                        "isOD": reader.uint8(),
                        "update": True,
                    })

                else:
                    raise WebSocketDecodeError(f"Неизвестный prematch recordType={record_type}")

            return {"type": "prematch", "events": events, "lines": lines}

        if step == 4:
            reader.read(12)
            events: list[dict[str, Any]] = []
            lines: list[dict[str, Any]] = []

            while reader.remaining:
                record_type = reader.uint8()

                if record_type == 2:
                    championship_id = reader.uint32()
                    sport_id = reader.uint32()
                    reader.int32()
                    country_id = reader.uint32()
                    reader.uint8()
                    championship_name = reader.string()
                    reader.uint8()
                    reader.uint32()
                    reader.uint32()

                    self.championships[championship_id] = {
                        "id": championship_id,
                        "name": championship_name,
                        "sportId": sport_id,
                        "sport": self.sports.get(sport_id),
                        "countryId": country_id,
                    }

                elif record_type in (3, 4):
                    is_update = record_type == 4
                    event = {"id": reader.int32()}

                    if not is_update:
                        event.update(
                            radarId=reader.int32(),
                            nativeId=reader.int32(),
                            provider=reader.uint8(),
                            category=reader.uint8(),
                        )

                    widget_size = reader.uint16()
                    reader.read(widget_size)
                    event["isOD"] = reader.uint8()
                    reader.uint8()
                    event["duration"] = reader.uint8()
                    event["state"] = reader.int32()

                    if event["state"] > 3:
                        self.live_events.pop(event["id"], None)
                        events.append({"id": event["id"], "deleted": True})
                        continue

                    if not is_update:
                        championship_id = reader.uint32()
                        championship = self.championships.get(championship_id, {})
                        event.update(
                            championshipId=championship_id,
                            championship=championship.get("name"),
                            sportId=championship.get("sportId"),
                            sport=championship.get("sport"),
                            participants=[reader.string(), reader.string()],
                            timestamp=reader.uint32(),
                        )

                    event["time"] = reader.string()
                    event["cards"] = [reader.uint8() for _ in range(4)]
                    reader.uint8()
                    event["score"] = reader.string()
                    event["setScores"] = reader.string()
                    event["addInfo"] = reader.string()
                    event["lineCount"] = reader.uint8()

                    if is_update:
                        previous = self.live_events.get(event["id"], {})
                        event = {**previous, **event, "update": True}

                    self.live_events[event["id"]] = event
                    events.append(event)

                elif record_type == 5:
                    line_id = reader.uint32()
                    state = reader.uint8()

                    if state == 5:
                        lines.append({"id": line_id, "deleted": True})
                        continue

                    event_id = reader.uint32()
                    value_count = reader.uint8()
                    values = [reader.uint16() / 100 for _ in range(min(value_count, 31))]
                    market_id = reader.uint16()

                    line = {
                        "id": line_id,
                        "eventId": event_id,
                        "state": state,
                        "marketId": market_id,
                        "values": values,
                        "coefficient": reader.string(),
                        "favorite": reader.uint8(),
                    }

                    if market_id in self.markets:
                        market_type, market_name = self.markets[market_id]
                        line.update(market=market_name, marketType=market_type)

                    lines.append(line)

                elif record_type == 6:
                    reader.uint32()
                    reader.uint8()

                else:
                    raise WebSocketDecodeError(f"Неизвестный live recordType={record_type}")

            return {"type": "live", "events": events, "lines": lines}

        return None


# ============================================================
# Парсер Winline
# ============================================================

class WinlineApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None):
        super().__init__('winline', detector=detector, aggregator=aggregator)
        self._decoder = DataNgDecoder()
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self._last_sent_state: Dict[str, tuple] = {}
        self.is_running = False
        self._data_queue = asyncio.Queue()

    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            logger.info(f"[{self.bk_id}] Открыта новая вкладка")

            self.page.on("websocket", self._handle_websocket)

            await self.page.goto(self.url, wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)

            logger.info(f"[{self.bk_id}] ✅ Страница загружена, перехват WebSocket активен")
            asyncio.create_task(self._process_queues())

    def _handle_websocket(self, ws):
        logger.info(f"[{self.bk_id}] 🔌 Событие WebSocket: {ws.url}")
        if 'wss.winline.ru/data_ng' in ws.url:
            logger.info(f"[{self.bk_id}] 🔌 WebSocket подключён: {ws.url}")
            ws.on("framereceived", self._on_frame)

    def _on_frame(self, payload: bytes):
        logger.debug(f"[{self.bk_id}] 📨 Получен фрейм, размер: {len(payload)} байт")
        try:
            decoded = self._decoder.decode(payload)
            if decoded is None:
                return
            self._data_queue.put_nowait(decoded)
            logger.debug(f"[{self.bk_id}] 📥 Данные помещены в очередь")
        except Exception as e:
            logger.error(f"[{self.bk_id}] Ошибка декодирования: {e}", exc_info=True)

    async def _process_queues(self):
        while self.is_running:
            try:
                decoded = await asyncio.wait_for(self._data_queue.get(), timeout=1.0)
                await self._process_decoded(decoded)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка обработки очереди: {e}", exc_info=True)

    async def _process_decoded(self, decoded):
        items = decoded if isinstance(decoded, list) else [decoded]
        for item in items:
            if not item:
                continue
            if item.get("type") in ("prematch", "live"):
                for ev in item.get("events", []):
                    event_id = ev.get("id")
                    if not event_id:
                        continue
                    if ev.get("deleted"):
                        self._matches_cache.pop(str(event_id), None)
                        continue
                    sport_id = ev.get("sportId")
                    if sport_id is not None and sport_id != 20:
                        logger.debug(f"[{self.bk_id}] ⏩ Пропуск события {event_id}, sportId={sport_id}")
                        continue
                    await self._update_event(str(event_id), ev)
                    await self._send_match(str(event_id))

                for ln in item.get("lines", []):
                    event_id = ln.get("eventId")
                    if not event_id:
                        continue
                    if str(event_id) not in self._matches_cache:
                        continue
                    self._matches_cache[str(event_id)].setdefault('lines', []).append(ln)
                    await self._send_match(str(event_id))

    async def _update_event(self, match_id: str, ev: dict):
        cache = self._matches_cache.get(match_id, {})
        participants = ev.get('participants', ['', ''])
        cache['participants'] = participants

        score_str = ev.get('score', '0:0')
        try:
            s1, s2 = map(int, score_str.split(':'))
        except:
            s1, s2 = 0, 0
        cache['score'] = score_str
        cache['score1'] = s1
        cache['score2'] = s2

        set_scores = ev.get('setScores', '')
        cache['setScores'] = set_scores
        sub1 = sub2 = 0
        if set_scores:
            sets = [s.strip() for s in set_scores.split('-') if s.strip()]
            if sets:
                last_set = sets[-1]
                try:
                    sub1, sub2 = map(int, last_set.split(':'))
                except:
                    pass
        cache['sub1'] = sub1
        cache['sub2'] = sub2

        cache['state'] = ev.get('state', 0)
        cache['championship'] = ev.get('championship', 'Неизвестно')
        cache['sport'] = ev.get('sport', '')
        cache['update'] = ev.get('update', False)
        cache['lines'] = cache.get('lines', [])

        # ДОБАВЛЕНО: сохраняем championshipId — может понадобиться
        # в будущем для более точной сборки URL.
        if 'championshipId' in ev:
            cache['championshipId'] = ev['championshipId']
        # ────────────────────────────────────────────────

        self._matches_cache[match_id] = cache
        if match_id not in self._first_seen:
            self._first_seen[match_id] = time.time()

    async def _send_match(self, match_id: str):
        m = self._matches_cache.get(match_id)
        if not m:
            return

        participants = m.get('participants', ['', ''])
        if not participants[0] or not participants[1]:
            return

        score1 = m.get('score1', 0)
        score2 = m.get('score2', 0)
        sub1 = m.get('sub1', 0)
        sub2 = m.get('sub2', 0)

        lines = m.get('lines', [])
        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        handicap1 = handicap2 = handicap_odds1 = handicap_odds2 = 0.0

        for line in lines:
            market = line.get('market', '')
            coeff_str = line.get('coefficient')
            values = line.get('values', [])

            if market == '1':
                if len(values) >= 2 and values[0] > 0 and values[1] > 0:
                    odds1 = values[0]
                    odds2 = values[1]
                else:
                    try:
                        coeff = float(coeff_str) if coeff_str is not None else 0.0
                        if coeff > 0:
                            odds1 = coeff
                    except:
                        pass

            elif market == '2':
                try:
                    coeff = float(coeff_str) if coeff_str is not None else 0.0
                    if coeff > 0:
                        odds2 = coeff
                except:
                    pass

            if market == 'Больше':
                if isinstance(coeff_str, str) and '/' not in coeff_str:
                    try:
                        total_line = float(coeff_str)
                    except:
                        pass
                if len(values) >= 2 and values[0] > 0 and values[1] > 0:
                    total_over = values[0]
                    total_under = values[1]
                elif len(values) >= 1 and values[0] > 0:
                    total_over = values[0]

            elif market == 'Меньше':
                if total_line == 0 and isinstance(coeff_str, str) and '/' not in coeff_str:
                    try:
                        total_line = float(coeff_str)
                    except:
                        pass
                if len(values) >= 2 and values[0] > 0 and values[1] > 0:
                    total_under = values[0]
                    total_over = values[1]
                elif len(values) >= 1 and values[0] > 0:
                    total_under = values[0]

            if market in ('1', '2') and isinstance(coeff_str, str) and '/' in coeff_str:
                parts = coeff_str.split('/')
                if len(parts) == 2:
                    try:
                        line_val = float(parts[1])
                    except:
                        line_val = 0.0

                    if len(values) >= 2 and values[0] > 0 and values[1] > 0:
                        if market == '1':
                            handicap1 = line_val
                            handicap_odds1 = values[0]
                            handicap2 = -line_val if line_val != 0 else 0.0
                            handicap_odds2 = values[1]
                        elif market == '2':
                            handicap2 = line_val
                            handicap_odds2 = values[0]
                            handicap1 = -line_val if line_val != 0 else 0.0
                            handicap_odds1 = values[1]
                    else:
                        try:
                            coef = float(parts[0])
                        except:
                            coef = 0.0
                        if market == '1':
                            handicap1 = line_val
                            handicap_odds1 = coef
                        elif market == '2':
                            handicap2 = line_val
                            handicap_odds2 = coef

        # ============================================================
        # ДОБАВЛЕНО: собираем URL страницы матча.
        # Пример: https://winline.ru/live/sport/nastolijnyj_tennis/mezhdunarodnye/setka_cup__muzhchiny/16665006
        # Из WS есть только championship (например "setka-cup")
        # и event_id. Полный URL содержит ещё регион и суффикс пола,
        # поэтому собираем упрощённо. Winline как SPA редиректит
        # по последнему сегменту (event_id) — этого достаточно.
        # ============================================================
        champ_raw = m.get('championship') or ''
        champ_slug = _slug(champ_raw)
        match_url = (
            f"https://winline.ru/live/sport/nastolijnyj_tennis/"
            f"x/{champ_slug}/{match_id}"
        )
        # ============================================================

        match = Match(
            bk_id='winline',
            match_id=match_id,
            player1=participants[0],
            player2=participants[1],
            score1=score1,
            score2=score2,
            sub_score1=sub1,
            sub_score2=sub2,
            tournament=m.get('championship', 'Неизвестно'),
            odds1=odds1,
            odds2=odds2,
            total_line=total_line,
            total_over=total_over,
            total_under=total_under,
            handicap1=handicap1,
            handicap2=handicap2,
            handicap_odds1=handicap_odds1,
            handicap_odds2=handicap_odds2,
            timestamp=time.time(),
            raw_time=str(m.get('state', 0)),
            match_url=match_url,                            # <-- ДОБАВЛЕНО
        )

        current_state = (
            match.score1, match.score2,
            match.sub_score1, match.sub_score2,
            round(match.odds1, 2), round(match.odds2, 2),
            round(match.total_line, 1),
            round(match.total_over, 2), round(match.total_under, 2),
            round(match.handicap1, 1), round(match.handicap2, 1),
            round(match.handicap_odds1, 2), round(match.handicap_odds2, 2),
        )
        if self._last_sent_state.get(match_id) == current_state:
            return

        if self.detector:
            await self.detector.process(match)
        if self.aggregator:
            self.aggregator.update(match)

        self._last_sent_state[match_id] = current_state
        self._last_sent_time[match_id] = time.time()

        logger.info(f"[{self.bk_id}] 🟢 Отправлен: {match.player1} vs {match.player2} | "
                    f"{match.score1}:{match.score2} (сет: {match.sub_score1}:{match.sub_score2}) | "
                    f"К: {match.odds1}/{match.odds2} | Т: {match.total_line} | Ф: {match.handicap1}")
        logger.debug(f"[{self.bk_id}] URL: {match_url}")     # <-- ДОБАВЛЕНО

    async def parse(self):
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Winline (WebSocket) запущен")
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
                self.page.remove_listener("websocket", self._handle_websocket)
            except Exception:
                pass
            await browser_manager.close_page(self.page)
        logger.info(f"[{self.bk_id}] 🛑 Остановка парсера...")