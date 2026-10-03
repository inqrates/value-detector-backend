# parsers/winline_api.py
"""
Winline — WS-парсер через websockets (без Playwright).

Handshake: 5 текстовых команд сразу после connect:
  "lang", "AA==", "data", "WINLINE", "getdate"

Данные: бинарные gzip-кадры → DataNgDecoder (уже есть).

Heartbeat: Winline НЕ отвечает на стандартный WS-ping (websockets шлёт его
автоматически каждые 20 сек), из-за чего сервер рвёт соединение с 1011.
Решение — отключить встроенный ping и слать свой heartbeat через "getdate"
каждые 20 секунд. Winline на getdate отвечает всегда.
"""
import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

import websockets

from core.models import Match
from core.sport_map import (
    get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)
from parsers.decoder import DataNgDecoder, WebSocketDecodeError

logger = logging.getLogger(__name__)

COOKIES_PATH = Path("cookies/winline.json")


class WinlineApiParser:
    bk_id = "winline"

    WS_URL = "wss://wss.winline.ru/data_ng?client=newsite&nb=true"

    HANDSHAKE_FRAMES = ["lang", "AA==", "data", "WINLINE", "getdate"]

    # sportId → sport_key
    SPORT_IDS = {
        20:  TABLE_TENNIS,
        23:  VOLLEYBALL,
        2:   BASKETBALL,
        193: CYBER_BASKETBALL,
        153: CYBER_BASKETBALL,
    }

    IMPERSONATE_UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/150.0.0.0 Safari/537.36"
    )

    WS_RECONNECT_DELAY = 2.0       # после штатного закрытия — быстро
    WS_RECONNECT_DELAY_MAX = 30.0  # после серии ошибок
    CLEANUP_TTL = 60.0
    HEARTBEAT_INTERVAL = 20.0      # getdate каждые 20 сек

    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        self.detector = detector
        self.aggregator = aggregator
        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]
        self.is_running = False

        self._decoder = DataNgDecoder()

        # Кэши
        self._events_cache: Dict[int, dict] = {}
        self._lines_cache: Dict[int, Dict[int, dict]] = {}
        self._first_seen: Dict[int, float] = {}
        self._last_sent_time: Dict[int, float] = {}
        self._last_update_time: Dict[int, float] = {}

        self._ws = None
        self._cookie_header = ""

        # Статистика реконнектов (для дебага)
        self._reconnects_total = 0
        self._reconnects_fast = 0      # штатные close → быстрый reconnect
        self._reconnects_error = 0     # аномальные → медленный reconnect

    # ============================================================
    # Cookies
    # ============================================================
    def _load_cookies_header(self) -> str:
        if not COOKIES_PATH.exists():
            return ""
        try:
            data = json.loads(COOKIES_PATH.read_text(encoding="utf-8"))
            cookies = data.get("cookies", [])
            return "; ".join(
                f"{c['name']}={c['value']}" for c in cookies
                if c.get("name") and c.get("value")
            )
        except Exception:
            return ""

    # ============================================================
    # Жизненный цикл
    # ============================================================
    async def start(self):
        self._cookie_header = self._load_cookies_header()
        logger.info(f"[{self.bk_id}] 🍪 Cookies: {'да' if self._cookie_header else 'нет'}")

    async def stop(self):
        self.is_running = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        logger.info(f"[{self.bk_id}] 🛑 Остановка WS-парсера...")

    async def run(self):
        self.is_running = True
        await self.start()
        logger.info(f"[{self.bk_id}] 🚀 WS-парсер Winline запущен (без Playwright)")

        ws_task = asyncio.create_task(self._ws_loop())
        send_task = asyncio.create_task(self._send_loop())

        try:
            await asyncio.gather(ws_task, send_task)
        except asyncio.CancelledError:
            pass
        finally:
            ws_task.cancel()
            send_task.cancel()
            try:
                await asyncio.gather(ws_task, send_task, return_exceptions=True)
            except Exception:
                pass

    # ============================================================
    # WS-цикл с реконнектом
    # ============================================================
    async def _ws_loop(self):
        delay = self.WS_RECONNECT_DELAY
        while self.is_running:
            try:
                closed_cleanly = await self._ws_connect_and_listen()

                if not self.is_running:
                    break

                if closed_cleanly:
                    # Штатное закрытие (сервер, keepalive, EOF) — быстрый reconnect
                    self._reconnects_fast += 1
                    delay = self.WS_RECONNECT_DELAY
                    logger.info(
                        f"[{self.bk_id}] WS закрыт → реконнект через {delay:.1f}с "
                        f"(штатных: {self._reconnects_fast}, "
                        f"ошибок: {self._reconnects_error})"
                    )
                else:
                    # Аномалия — exponential backoff
                    self._reconnects_error += 1
                    delay = min(delay * 2, self.WS_RECONNECT_DELAY_MAX)
                    logger.warning(
                        f"[{self.bk_id}] WS аномально закрыт → "
                        f"реконнект через {delay:.1f}с"
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self.is_running:
                    break
                self._reconnects_error += 1
                delay = min(delay * 2, self.WS_RECONNECT_DELAY_MAX)
                logger.warning(
                    f"[{self.bk_id}] WS ошибка ({type(e).__name__}): {e} → "
                    f"реконнект через {delay:.1f}с"
                )

            if not self.is_running:
                break
            await asyncio.sleep(delay)

    async def _ws_connect_and_listen(self) -> bool:
        """
        Возвращает True если закрытие штатное (сервер/EOF/сеть),
        False если аномалия (ошибка протокола).
        """
        headers = [
            ("User-Agent", self.IMPERSONATE_UA),
            ("Origin", "https://winline.ru"),
        ]
        if self._cookie_header:
            headers.append(("Cookie", self._cookie_header))

        logger.info(f"[{self.bk_id}] 🔌 WS подключаемся...")

        # ВАЖНО: ping_interval=None — отключаем стандартный WS-ping.
        # Winline на него не отвечает, из-за этого прилетает 1011.
        # Вместо этого — свой heartbeat через "getdate".
        async with websockets.connect(
            self.WS_URL,
            additional_headers=headers,
            max_size=50 * 1024 * 1024,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
        ) as ws:
            self._ws = ws
            logger.info(f"[{self.bk_id}] ✅ WS подключён")

            # Handshake
            for cmd in self.HANDSHAKE_FRAMES:
                try:
                    await ws.send(cmd)
                    await asyncio.sleep(0.05)
                except Exception as e:
                    logger.warning(f"[{self.bk_id}] handshake {cmd!r}: {e}")
                    return False

            logger.info(f"[{self.bk_id}] 📤 Handshake отправлен")

            # Запускаем heartbeat (getdate каждые 20 сек)
            hb_task = asyncio.create_task(self._heartbeat_loop(ws))

            try:
                async for frame in ws:
                    if not self.is_running:
                        break
                    try:
                        self._on_frame(frame)
                    except Exception as e:
                        logger.debug(f"[{self.bk_id}] frame: {e}")
            except websockets.ConnectionClosed as e:
                # 1011, 1000, 1006 — все штатные варианты после handshake
                # Логируем на INFO, без стека
                logger.info(
                    f"[{self.bk_id}] WS закрыт сервером: "
                    f"code={e.code}, reason={e.reason!r}"
                )
                return True
            except websockets.ConnectionClosedError as e:
                # Аномальное закрытие — логируем но не крашим
                logger.info(
                    f"[{self.bk_id}] WS аномально закрыт: "
                    f"code={e.code}, reason={e.reason!r}"
                )
                return True   # всё равно переподключаемся быстро — это не сеть
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                logger.info(f"[{self.bk_id}] WS сетевой сбой: {type(e).__name__}")
                return True
            finally:
                hb_task.cancel()
                try:
                    await hb_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

        self._ws = None
        return True

    async def _heartbeat_loop(self, ws):
        """
        Раз в HEARTBEAT_INTERVAL шлём "getdate".
        Winline всегда отвечает на это — соединение остаётся живым.
        """
        while True:
            try:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                if ws.closed:
                    break
                await ws.send("getdate")
                logger.debug(f"[{self.bk_id}] heartbeat sent")
            except asyncio.CancelledError:
                break
            except websockets.ConnectionClosed:
                break
            except Exception as e:
                logger.debug(f"[{self.bk_id}] heartbeat: {e}")
                break

    # ============================================================
    # Обработка кадра
    # ============================================================
    def _on_frame(self, payload):
        if isinstance(payload, str):
            return  # text-фреймов после handshake не ожидаем
        try:
            data = payload if isinstance(payload, (bytes, bytearray)) else (
                payload.payload if hasattr(payload, "payload") else None
            )
            if not data:
                return

            decoded = self._decoder.decode(bytes(data))
            if decoded is None:
                return  # menu

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
            logger.debug(f"[{self.bk_id}] decode: {e}")

    # ============================================================
    # Применение events / lines
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

            state = ev.get("state")
            score_raw = (ev.get("score") or "").strip()
            time_raw = (ev.get("time") or "").strip()

            has_live_data = (
                state is not None
                or (score_raw and score_raw not in ("-:-", "-"))
                or bool(time_raw)
            )

            if not has_live_data and old is None:
                continue

            merged = {**(old or {}), **ev}
            self._events_cache[eid] = merged

            if eid not in self._first_seen:
                self._first_seen[eid] = time.time()

            self._last_update_time[eid] = time.time()

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
            self._last_update_time[eid] = time.time()

    # ============================================================
    # Парсинг event → Match
    # ============================================================
    def _parse_event(self, eid: int, ev: dict) -> Optional[dict]:
        score_raw = (ev.get("score") or "").strip()
        time_raw = (ev.get("time") or "").strip()
        state = ev.get("state")

        if state is None:
            return None
        if score_raw in ("", "-:-") and not time_raw:
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

        try:
            a, b = score_raw.split(":", 1)
            score1, score2 = int(a), int(b)
        except ValueError:
            score1 = score2 = 0

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

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            time_str = ev.get("time") or ""
            m = re.search(r"(\d+)\s*Ч", time_str)
            if m:
                phase_num = int(m.group(1))
            else:
                phase_num = len(parts) if parts else 1
        else:
            phase_num = score1 + score2 + 1

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

            if market == "1" and mtype == 1 and len(values) >= 2:
                try:
                    odds1 = float(values[0])
                    odds2 = float(values[1])
                except (ValueError, TypeError):
                    pass

            elif market == "Больше" and mtype == 4:
                try:
                    total_over = float(values[0])
                    total_under = float(values[1])
                    if coeff:
                        total_line = float(str(coeff).replace(",", "."))
                except (ValueError, TypeError):
                    pass

            elif market == "1" and mtype == 3 and h1 == 0.0 and h_o1 == 0.0:
                try:
                    line_val = float(str(coeff).replace(",", "."))
                    h1 = line_val
                    h_o1 = float(values[0])
                    h2 = -line_val
                    h_o2 = float(values[1])
                except (ValueError, TypeError):
                    pass

        return {
            "sport": sport_key,
            "player1": p1,
            "player2": p2,
            "score1": score1, "score2": score2,
            "sub1": sub1, "sub2": sub2,
            "phase_num": phase_num,
            "tournament": ev.get("championship") or "Winline",
            "odds1": odds1, "odds2": odds2,
            "total_line": total_line,
            "total_over": total_over,
            "total_under": total_under,
            "handicap1": h1, "handicap2": h2,
            "handicap_odds1": h_o1, "handicap_odds2": h_o2,
        }

    # ============================================================
    # Основной цикл отправки
    # ============================================================
    async def _send_loop(self):
        while self.is_running:
            try:
                await self._try_send_matches()
                await self._cleanup_old()
            except Exception as e:
                logger.error(f"[{self.bk_id}] send loop: {e}", exc_info=True)
            await asyncio.sleep(1.0)

    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for eid, ev in list(self._events_cache.items()):
            if ev.get("state") in (3, 4):
                continue

            parsed = self._parse_event(eid, ev)
            if not parsed:
                continue

            sport_key = parsed["sport"]

            first_seen = self._first_seen.get(eid, current_time)
            if parsed["odds1"] == 0 and parsed["odds2"] == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(eid, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (
                parsed["score1"], parsed["score2"],
                parsed["sub1"], parsed["sub2"],
                parsed["phase_num"],
                parsed["odds1"], parsed["odds2"],
                parsed["total_line"], parsed["total_over"], parsed["total_under"],
                parsed["handicap1"], parsed["handicap2"],
                parsed["handicap_odds1"], parsed["handicap_odds2"],
            )
            if ev.get("_last_sent") == current_state:
                continue

            phase_name = format_phase(sport_key, parsed["phase_num"])

            slug = get_url_slug(self.bk_id, sport_key) or "nastolijnyj_tennis"
            match_url = f"https://winline.ru/live/sport/{slug}/{eid}"

            match = Match(
                bk_id=self.bk_id,
                match_id=str(eid),
                player1=parsed["player1"],
                player2=parsed["player2"],
                score1=parsed["score1"],
                score2=parsed["score2"],
                sub_score1=parsed["sub1"],
                sub_score2=parsed["sub2"],
                tournament=parsed["tournament"],
                odds1=parsed["odds1"],
                odds2=parsed["odds2"],
                total_line=parsed["total_line"],
                total_over=parsed["total_over"],
                total_under=parsed["total_under"],
                handicap1=parsed["handicap1"],
                handicap2=parsed["handicap2"],
                handicap_odds1=parsed["handicap_odds1"],
                handicap_odds2=parsed["handicap_odds2"],
                timestamp=current_time,
                raw_time=phase_name,
                sport=sport_key,
                match_url=match_url,
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            ev["_last_sent"] = current_state
            self._last_sent_time[eid] = current_time
            sent += 1

            logger.info(
                f"[{self.bk_id}] 🟢 [{sport_key}] {match.player1} vs {match.player2} | "
                f"матч {match.score1}:{match.score2} | "
                f"{phase_name} {match.sub_score1}:{match.sub_score2} | "
                f"К: {match.odds1}/{match.odds2}"
            )

        if sent:
            logger.info(
                f"[{self.bk_id}] ✅ Отправлено: {sent} "
                f"(в кеше events: {len(self._events_cache)})"
            )

    async def _cleanup_old(self):
        now = time.time()
        for eid in list(self._events_cache.keys()):
            if now - self._first_seen.get(eid, now) > self.CLEANUP_TTL:
                if now - self._last_sent_time.get(eid, 0) > self.CLEANUP_TTL:
                    self._events_cache.pop(eid, None)
                    self._lines_cache.pop(eid, None)
                    self._first_seen.pop(eid, None)
                    self._last_sent_time.pop(eid, None)

    async def parse(self) -> List[Match]:
        return []