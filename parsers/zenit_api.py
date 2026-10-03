# parsers/zenit_api.py
"""
Zenit — HTTP + WS парсер через curl_cffi + websockets (без Playwright).

Архитектура:
  1. HTTP GET /ajax/live/video/get_list — снапшот матчей (gid, name, sid).
  2. WS wss://zenit.win/wss — 3 типа фреймов:
       t=20/21 — matches с score, sScore, odds
       t=7     — снапшот дерева (sports → championships → matches) или removedIds
       t=60    — серверное время
  3. Логика разбора скопирована из старого Playwright-парсера.
"""
import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

import websockets
import ssl
try:
    import certifi
    _SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    _SSL_CTX = ssl.create_default_context()
from curl_cffi.requests import AsyncSession

from core.models import Match
from core.sport_map import (
    SPORT_MAP, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)

logger = logging.getLogger(__name__)

COOKIES_PATH = Path("cookies/zenit.json")


class ZenitApiParser:
    bk_id = "zenit"

    BASE = "https://zenit.win"
    GET_LIST_URL = f"{BASE}/ajax/live/video/get_list"
    WS_URL = "wss://zenit.win/wss"

    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ru-RU,ru;q=0.9",
        "referer": f"{BASE}/live",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "x-requested-with": "XMLHttpRequest",
    }

    IMPERSONATE = "chrome150"
    SNAPSHOT_INTERVAL = 120.0
    WS_RECONNECT_DELAY = 3.0

    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        self.detector = detector
        self.aggregator = aggregator
        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]
        self.is_running = False

        # Кэши
        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self._last_update_time: Dict[str, float] = {}

        # sid → sport_key
        self._sport_ids: Dict[str, str] = {}
        for sport_key in self.enabled_sports:
            cfg = SPORT_MAP.get(self.bk_id, {}).get(sport_key, {})
            for sid in cfg.get("ids", []):
                self._sport_ids[str(sid)] = sport_key
        logger.info(f"[{self.bk_id}] sport_ids: {self._sport_ids}")

        # championshipId → sportId
        self._championship_sport: Dict[int, int] = {}

        self._session: Optional[AsyncSession] = None
        self._ws = None
        self._last_snapshot_at: float = 0.0
        self._cookie_header: str = ""

    # ============================================================
    # Cookies / session
    # ============================================================
    def _load_cookies(self) -> list:
        if not COOKIES_PATH.exists():
            return []
        try:
            return json.loads(COOKIES_PATH.read_text(encoding="utf-8")).get("cookies", [])
        except Exception:
            return []

    async def _ensure_session(self):
        if self._session is not None:
            return
        self._session = AsyncSession(
            impersonate=self.IMPERSONATE,
            timeout=30,
            headers=self.HEADERS,
        )
        cookies = self._load_cookies()
        for c in cookies:
            try:
                self._session.cookies.set(
                    c["name"], c["value"],
                    domain=c.get("domain"), path=c.get("path", "/"),
                )
            except Exception:
                pass
        self._cookie_header = "; ".join(
            f"{c['name']}={c['value']}" for c in cookies
            if c.get("name") and c.get("value")
        )
        logger.info(f"[{self.bk_id}] 🍪 Загружено {len(cookies)} cookies")

    # ============================================================
    # Жизненный цикл
    # ============================================================
    async def start(self):
        await self._ensure_session()

    async def stop(self):
        self.is_running = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
        logger.info(f"[{self.bk_id}] 🛑 Остановка HTTP+WS парсера...")

    async def run(self):
        self.is_running = True
        await self.start()
        logger.info(f"[{self.bk_id}] 🚀 HTTP+WS парсер Zenit запущен (без Playwright)")

                # Zenit WS отдаёт полный снапшот через t=7, HTTP-снапшот не нужен.
        # Оставлен как fallback — пробуем один раз, при ошибке больше не трогаем.
        try:
            await self._fetch_snapshot()
        except Exception as e:
            logger.info(f"[{self.bk_id}] HTTP-снапшот недоступен ({e}), работаем только через WS")
        self._last_snapshot_at = time.time() + 99999   # отключаем повторные попытки

        ws_task = asyncio.create_task(self._ws_loop())

        try:
            while self.is_running:
                t0 = time.monotonic()
                try:
                    now = time.time()
                    # HTTP-снапшот отключён — WS полностью покрывает задачу
                    if now - self._last_snapshot_at >= self.SNAPSHOT_INTERVAL:
                        await self._fetch_snapshot()
                    await self._try_send_matches()
                except Exception as e:
                    logger.error(f"[{self.bk_id}] цикл: {e}", exc_info=True)
                    await asyncio.sleep(3)
                    continue

                elapsed = time.monotonic() - t0
                await asyncio.sleep(max(0, 0.5 - elapsed))
        finally:
            self.is_running = False
            ws_task.cancel()
            try:
                await ws_task
            except (asyncio.CancelledError, Exception):
                pass

    # ============================================================
    # HTTP: снапшот списка матчей
    # ============================================================
    async def _fetch_snapshot(self):
        try:
            r = await self._session.get(self.GET_LIST_URL, headers=self.HEADERS)
        except Exception as e:
            logger.warning(f"[{self.bk_id}] snapshot HTTP error: {e}")
            return

        if r.status_code != 200:
            logger.debug(f"[{self.bk_id}] snapshot HTTP {r.status_code}")
            return

        try:
            data = r.json()
        except Exception:
            return

        games = data.get("result", {}).get("games", []) if isinstance(data, dict) else []
        if not isinstance(games, list):
            games = []

        for game in games:
            if not isinstance(game, dict):
                continue
            sid = str(game.get("sid", ""))
            sport_key = self._sport_ids.get(sid)
            if not sport_key:
                continue

            gid = str(game.get("gid", ""))
            if not gid:
                continue

            name = game.get("name", "")
            if " - " in name:
                player1, player2 = name.split(" - ", 1)
            elif " vs " in name:
                player1, player2 = name.split(" vs ", 1)
            else:
                player1, player2 = name, ""

            if gid not in self._matches_cache:
                self._matches_cache[gid] = {"_last_sent": None}
                self._first_seen[gid] = time.time()

            cache = self._matches_cache[gid]
            cache.setdefault("sport", sport_key)
            if player1.strip() and not cache.get("player1"):
                cache["player1"] = player1.strip()
            if player2.strip() and not cache.get("player2"):
                cache["player2"] = player2.strip()
            cache.setdefault("tournament", "Zenit Live")

        self._last_snapshot_at = time.time()
        logger.info(
            f"[{self.bk_id}] 📸 Снапшот: {len(games)} игр, "
            f"в кеше: {len(self._matches_cache)}"
        )

    # ============================================================
    # WS: подключение + подписки
    # ============================================================
    async def _ws_loop(self):
        while self.is_running:
            try:
                await self._ws_connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[{self.bk_id}] WS ошибка: {e}")
            if not self.is_running:
                break
            logger.info(f"[{self.bk_id}] WS переподключение через "
                        f"{self.WS_RECONNECT_DELAY:.0f}с")
            await asyncio.sleep(self.WS_RECONNECT_DELAY)

    async def _ws_connect_and_listen(self):
        headers = [
            ("Origin", "https://zenit.win"),
            ("User-Agent", (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Safari/537.36"
            )),
        ]
        if self._cookie_header:
            headers.append(("Cookie", self._cookie_header))

        logger.info(f"[{self.bk_id}] 🔌 WS подключаемся...")

        async with websockets.connect(
            self.WS_URL,
            additional_headers=headers,
            max_size=20 * 1024 * 1024,
            ping_interval=None,   # Zenit сам шлёт пинги через t=60
            ping_timeout=None,
	    ssl=_SSL_CTX,
        ) as ws:
            self._ws = ws
            logger.info(f"[{self.bk_id}] ✅ WS подключён")

            # Handshake — точная реплика из sniff
            init_frames = [
                {"d": {"timezone": 0, "lng": 0, "t": 60, "d": 0}, "t": 60, "op": 0},
                {"d": 2, "t": 67, "op": 0},
                {"d": {"timezone": 5, "site": 1, "lng": 1049, "sort": 2, "subscriptionID": 0},
                 "t": 20, "op": 0, "s_id": 0},
                {"d": {"site": 1, "lng": 1049, "sort": 2, "withoutStatsMatches": False},
                 "t": 7, "op": 0},
            ]
            for f in init_frames:
                await ws.send(json.dumps(f))
                await asyncio.sleep(0.1)

            async for frame in ws:
                if not self.is_running:
                    break
                try:
                    self._process_ws_frame(frame)
                except Exception as e:
                    logger.debug(f"[{self.bk_id}] ws frame: {e}")

        self._ws = None

    # ---------- Обработка WS-кадров (копия из старого парсера) ----------
    def _process_ws_frame(self, payload):
        try:
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            if not payload:
                return
            data = json.loads(payload)
            t = data.get("t")
            if t not in (7, 20, 21):
                return
            d = data.get("d", {})
            if not isinstance(d, dict):
                return

            if t in (20, 21):
                self._process_ws_full(d)
            elif t == 7:
                self._process_ws_t7(d)
        except json.JSONDecodeError:
            pass

    def _process_ws_full(self, d: dict):
        # 1) sports (справочник)
        # (пока пропускаем — не критично)

        # 2) championships
        championships = d.get("championships")
        if isinstance(championships, dict):
            for cid_str, ch in championships.items():
                if not isinstance(ch, dict):
                    continue
                try:
                    cid = int(cid_str)
                    sid = int(ch.get("sportId"))
                except (ValueError, TypeError):
                    continue
                self._championship_sport[cid] = sid

        # 3) matches
        matches = d.get("matches")
        if isinstance(matches, dict):
            for gid_str, match_info in matches.items():
                self._process_match(gid_str, match_info)

        # 4) removedIds
        removed = d.get("removedIds")
        if isinstance(removed, list):
            for gid in removed:
                self._matches_cache.pop(str(gid), None)

    def _process_ws_t7(self, d: dict):
        removed = d.get("removedIds")
        if isinstance(removed, list):
            for gid in removed:
                self._matches_cache.pop(str(gid), None)

        sports = d.get("sports")
        if not isinstance(sports, list):
            return

        for sport in sports:
            if not isinstance(sport, dict):
                continue
            sid = sport.get("id")
            if sid is None:
                continue
            try:
                sid_int = int(sid)
            except (ValueError, TypeError):
                continue

            sport_key = self._sport_ids.get(str(sid_int))
            if not sport_key:
                continue

            for ch in sport.get("championships", []):
                if not isinstance(ch, dict):
                    continue
                cid = ch.get("id")
                if cid is not None:
                    try:
                        self._championship_sport[int(cid)] = sid_int
                    except (ValueError, TypeError):
                        pass

                champ_name = ch.get("name", "")
                for m in ch.get("matches", []):
                    if not isinstance(m, dict):
                        continue
                    gid = str(m.get("id", ""))
                    if not gid:
                        continue
                    if gid not in self._matches_cache:
                        self._matches_cache[gid] = {"_last_sent": None}
                        self._first_seen[gid] = time.time()
                    c = self._matches_cache[gid]
                    c.setdefault("sport", sport_key)
                    c.setdefault("tournament", champ_name or "Zenit Live")
                    if m.get("team1"):
                        c["player1"] = m["team1"]
                    if m.get("team2"):
                        c["player2"] = m["team2"]

    def _process_match(self, gid_str: str, match_info):
        if not isinstance(match_info, dict):
            return
        if match_info.get("bl") == 1:
            return

        gid = str(gid_str)

        if gid not in self._matches_cache:
            sport_key = self._resolve_sport_key(match_info.get("championshipId"))
            if not sport_key:
                return
            self._matches_cache[gid] = {"_last_sent": None}
            self._first_seen[gid] = time.time()
            cache = self._matches_cache[gid]
            cache["sport"] = sport_key
            cache.setdefault("tournament", "Zenit Live")
        else:
            cache = self._matches_cache[gid]
            if not cache.get("sport"):
                sport_key = self._resolve_sport_key(match_info.get("championshipId"))
                if sport_key:
                    cache["sport"] = sport_key

        if match_info.get("team1"):
            cache["player1"] = match_info["team1"]
        if match_info.get("team2"):
            cache["player2"] = match_info["team2"]

        sport_key = cache.get("sport")
        if not sport_key:
            return

        if "sScore" in match_info or "score" in match_info:
            self._parse_ws_score(cache, match_info, sport_key)

        odds_data = match_info.get("odds")
        if isinstance(odds_data, dict) and odds_data:
            self._parse_ws_odds(cache, odds_data, match_info.get("mainLine", []))

        self._last_update_time[gid] = time.time()

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

    # ---------- Score (копия) ----------
    def _parse_ws_score(self, cache: dict, match_info: dict, sport_key: str):
        sd_data = (match_info.get("sScore") or {}).get("sScoreData") or {}
        scs = sd_data.get("scs") or []
        sd = (sd_data.get("sd") or "").strip()

        if scs and isinstance(scs[0], dict):
            cur = (scs[0].get("scv") or {}).get("cur") or {}
            try:
                cache["score1"] = int(cur.get("t1", 0))
                cache["score2"] = int(cur.get("t2", 0))
            except (ValueError, TypeError):
                pass

        sub1 = sub2 = 0
        if scs and isinstance(scs[-1], dict):
            cur = (scs[-1].get("scv") or {}).get("cur") or {}
            try:
                sub1 = int(cur.get("t1", 0))
                sub2 = int(cur.get("t2", 0))
            except (ValueError, TypeError):
                pass
        cache["sub1"] = sub1
        cache["sub2"] = sub2

        if sport_key in (BASKETBALL, CYBER_BASKETBALL):
            if "Перерыв" in sd:
                phase_num = max(1, len(scs) - 1)
                cache["sub1"] = cache["sub2"] = 0
            else:
                m = re.search(r"(\d+)", sd)
                if m:
                    phase_num = int(m.group(1))
                else:
                    phase_num = max(1, len(scs) - 1)
        else:
            s1 = cache.get("score1", 0) or 0
            s2 = cache.get("score2", 0) or 0
            phase_num = s1 + s2 + 1

        cache["phase_num"] = phase_num

    # ---------- Odds (копия) ----------
    def _parse_ws_odds(self, cache: dict, odds_data: dict, main_line: list):
        def _cf(key):
            v = odds_data.get(key)
            if isinstance(v, dict):
                try:
                    return float(v.get("cf", 0) or 0)
                except (ValueError, TypeError):
                    return 0.0
            return 0.0

        def _line(key):
            v = odds_data.get(key)
            if isinstance(v, dict):
                odd_key = v.get("oddKey", "")
                parts = odd_key.split("|")
                if len(parts) >= 3:
                    try:
                        return float(parts[2])
                    except ValueError:
                        return 0.0
            return 0.0

        cache["odds1"] = _cf("1")
        cache["odds2"] = _cf("3")
        cache["handicap1"] = _line("7")
        cache["handicap_odds1"] = _cf("7")
        cache["handicap2"] = _line("8")
        cache["handicap_odds2"] = _cf("8")
        cache["total_under"] = _cf("9")
        cache["total_over"] = _cf("10")
        cache["total_line"] = _line("9") or _line("10")

    # ============================================================
    # Отправка (копия)
    # ============================================================
    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get("player1") or not m.get("player2"):
                continue
            if not m.get("sport"):
                continue

            sport_key = m["sport"]

            first_seen = self._first_seen.get(match_id, current_time)
            has_odds = (m.get("odds1", 0) > 0 or m.get("odds2", 0) > 0)
            has_score = (
                m.get("score1", 0) > 0 or m.get("score2", 0) > 0
                or m.get("sub1", 0) > 0 or m.get("sub2", 0) > 0
            )
            if not has_odds and not has_score:
                continue
            if not has_odds and current_time - first_seen < 15:
                continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (
                m.get("score1", 0), m.get("score2", 0),
                m.get("sub1", 0), m.get("sub2", 0),
                m.get("phase_num", 0),
                m.get("odds1", 0.0), m.get("odds2", 0.0),
                m.get("total_line", 0.0), m.get("total_over", 0.0),
                m.get("total_under", 0.0),
                m.get("handicap1", 0.0), m.get("handicap2", 0.0),
                m.get("handicap_odds1", 0.0), m.get("handicap_odds2", 0.0),
            )
            if m.get("_last_sent") == current_state:
                continue

            phase_name = format_phase(sport_key, m.get("phase_num", 1))

            sid = None
            cfg = SPORT_MAP.get(self.bk_id, {}).get(sport_key, {})
            ids = cfg.get("ids", [])
            if ids:
                sid = ids[0]
            match_url = f"https://zenit.win/live/{sid}/{match_id}" if sid \
                else f"https://zenit.win/live/{match_id}"

            match = Match(
                bk_id=self.bk_id,
                match_id=match_id,
                player1=m["player1"],
                player2=m["player2"],
                score1=m.get("score1", 0),
                score2=m.get("score2", 0),
                sub_score1=m.get("sub1", 0),
                sub_score2=m.get("sub2", 0),
                tournament=m.get("tournament", "Zenit Live"),
                odds1=m.get("odds1", 0.0),
                odds2=m.get("odds2", 0.0),
                total_line=m.get("total_line", 0.0),
                total_over=m.get("total_over", 0.0),
                total_under=m.get("total_under", 0.0),
                handicap1=m.get("handicap1", 0.0),
                handicap2=m.get("handicap2", 0.0),
                handicap_odds1=m.get("handicap_odds1", 0.0),
                handicap_odds2=m.get("handicap_odds2", 0.0),
                timestamp=current_time,
                raw_time=phase_name,
                sport=sport_key,
                match_url=match_url,
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            m["_last_sent"] = current_state
            self._last_sent_time[match_id] = current_time
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
                f"(в кеше: {len(self._matches_cache)})"
            )

    async def parse(self) -> List[Match]:
        return []