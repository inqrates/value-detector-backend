# parsers/marathon_api.py
"""
Marathon API-парсер с поддержкой мультиспорта (НТ / волейбол / баскетбол).

Особенности:
  - Новый API: /eag/event-line/api/v1/sports/by-slug/all-tournaments/live?sportSlug=...
  - Три параллельных SSE-стрима (table-tennis, volleyball, basketball).
  - Формат данных: JSON-массив изменений {path, value}.
  - Вид спорта определяется по sportSlug в URL и проверяется через value.sportCode.

Парсинг коэфов (из markets):
  model MTCH_DNB / MTCH_R → Победитель (odds1/odds2)
  model MTCH_TTLG / MTCH_TTLP → Тотал (total_line/total_over/total_under)
  model MTCH_HB / MTCH_HBP → Фора (handicap1/2 + odds)
  Коэффициенты в формате {n, d} → (n+d)/d.
"""
import asyncio
import time
import json
import re
import logging
from typing import Dict, List, Optional
import httpx

from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import PAGE_LOAD_TIMEOUT, SPORT_URLS
from core.sport_map import (
    SPORT_MAP, get_url_slug, format_phase,
    TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
)

logger = logging.getLogger(__name__)

SPORT_CODE_TO_KEY = {
    "TableTennis": TABLE_TENNIS,
    "Volleyball": VOLLEYBALL,
    "Basketball": BASKETBALL,
    "e-Sports": CYBER_BASKETBALL,
}

def _slug(text: str) -> str:
    if not text:
        return "x"
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text or "x"


def _fraction_to_decimal(n: int, d: int) -> float:
    if d == 0:
        return 0.0
    return round((n + d) / d, 3)


class MarathonApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('marathon', detector=detector, aggregator=aggregator)

        self.enabled_sports = enabled_sports or [
            TABLE_TENNIS, VOLLEYBALL, BASKETBALL, CYBER_BASKETBALL,
        ]

        self._matches_cache: Dict[str, dict] = {}
        self._first_seen: Dict[str, float] = {}
        self._last_sent_time: Dict[str, float] = {}
        self.is_running = False
        self._client: httpx.AsyncClient = None

    # ============================================================
    # Запуск
    # ============================================================
    async def start(self):
        if self._client is None:
            await self._init_client()
        for sport_key in self.enabled_sports:
            url = self._build_sse_url(sport_key)
            asyncio.create_task(self._sse_loop(sport_key, url))
            logger.info(f"[{self.bk_id}] SSE запущен для {sport_key}: {url}")
        asyncio.create_task(self._process_queues())

    def _build_sse_url(self, sport_key: str) -> str:
        slug = get_url_slug(self.bk_id, sport_key)
        if not slug:
            slug = sport_key.replace('_', '-')
        return (f"https://new.marathonbet.ru/eag/event-line/api/v1/"
                f"sports/by-slug/all-tournaments/live?sportSlug={slug}")

    async def _init_client(self):
        page = await browser_manager.new_page()
        try:
            await page.goto("https://new.marathonbet.ru/su/live/table-tennis",
                            wait_until='domcontentloaded', timeout=PAGE_LOAD_TIMEOUT)
            await page.wait_for_timeout(3000)
            cookies = await page.context.cookies()
            cookies_dict = {c['name']: c['value'] for c in cookies}
            user_agent = await page.evaluate("navigator.userAgent")
        finally:
            await page.close()

        headers = {
            "Accept": "text/event-stream",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://new.marathonbet.ru/su/live/table-tennis",
            "Origin": "https://new.marathonbet.ru",
            "User-Agent": user_agent,
            "x-pan-source": "REDESIGN_WEB",
            "x-pan-target": "BROWSER",
            "x-pan-version": "MOBILE-SSR-2.6.5",
        }
        self._client = httpx.AsyncClient(
            cookies=cookies_dict,
            headers=headers,
            timeout=httpx.Timeout(300.0, connect=10.0),
            follow_redirects=True,
        )

    # ============================================================
    # SSE-стримы
    # ============================================================
    async def _sse_loop(self, sport_key: str, url: str):
        retry_delay = 1
        max_retry_delay = 60

        while self.is_running:
            try:
                if self._client is None:
                    await asyncio.sleep(1)
                    continue

                logger.info(f"[{self.bk_id}] [{sport_key}] Подключаемся к SSE...")
                async with self._client.stream("GET", url) as response:
                    if response.status_code != 200:
                        logger.error(f"[{self.bk_id}] [{sport_key}] SSE ошибка: {response.status_code}")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, max_retry_delay)
                        continue

                    retry_delay = 1
                    logger.info(f"[{self.bk_id}] [{sport_key}] SSE подключён")
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
                                await self._process_sse_part(part, sport_key)

                    logger.warning(f"[{self.bk_id}] [{sport_key}] SSE завершён, переподключение")
                    await asyncio.sleep(retry_delay)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.bk_id}] [{sport_key}] SSE ошибка: {e}", exc_info=True)
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)

    async def _process_sse_part(self, part: str, sport_key: str):
        data_line = ''
        for line in part.split('\n'):
            line = line.strip()
            if line.startswith('data:'):
                if data_line:
                    data_line += '\n'
                data_line += line[5:]
        if not data_line:
            return
        try:
            data = json.loads(data_line)
        except json.JSONDecodeError:
            return
        if not isinstance(data, list):
            return
        for change in data:
            if isinstance(change, dict):
                self._apply_change(change, sport_key)

    # ============================================================
    # Обработка одного изменения
    # ============================================================
    def _apply_change(self, change: dict, sport_key: str):
        path = change.get("path") or []
        value = change.get("value")
        if value is None:
            return

        # 1) Полное событие
        if isinstance(value, dict) and "treeId" in value and "sportCode" in value:
            self._upsert_event(value, sport_key)
            return

        # 2) Обновление matchScore
        if any(p.get("inObj") == "matchScore" for p in path if isinstance(p, dict)):
            match_id = self._match_id_from_path(path)
            if match_id and isinstance(value, dict):
                self._update_match_score(match_id, value)
            return

        # 3) Обновление коэфа
        if isinstance(value, dict) and "selId" in value and "coeff" in value:
            match_id = self._match_id_from_path(path)
            if match_id:
                self._update_selection(match_id, value)

    def _match_id_from_path(self, path: list) -> Optional[str]:
        for p in path:
            if (isinstance(p, dict)
                    and p.get("inList") == "liveEvents"
                    and p.get("key") == "treeId"):
                return str(p.get("val"))
        return None

    def _upsert_event(self, event: dict, sport_key: str):
        match_id = str(event.get("treeId", ""))
        if not match_id:
            return
        if SPORT_CODE_TO_KEY.get(event.get("sportCode")) != sport_key:
            return

        if match_id not in self._matches_cache:
            self._matches_cache[match_id] = {"_last_sent": None}
            self._first_seen[match_id] = time.time()

        cache = self._matches_cache[match_id]
        cache["sport"] = sport_key
        cache["player1"] = (event.get("homeTeam", {}).get("members", [{}])[0]
                            .get("name", ""))
        cache["player2"] = (event.get("awayTeam", {}).get("members", [{}])[0]
                            .get("name", ""))
        cache["tournament"] = event.get("header", "")

        phase = event.get("phase") or {}
        cache["phase_num"] = phase.get("partNumber", 1)

        ms = event.get("matchScore") or {}
        main = ms.get("main") or {}
        cache["score1"] = int(main.get("home", 0) or 0)
        cache["score2"] = int(main.get("away", 0) or 0)

        parts = ms.get("parts") or []
        if parts:
            last = parts[-1]
            cache["sub1"] = int(last.get("home", 0) or 0)
            cache["sub2"] = int(last.get("away", 0) or 0)
        else:
            cache["sub1"] = 0
            cache["sub2"] = 0

        cache["markets"] = event.get("markets") or {}
        self._build_sel_index(cache)
        self._recalc_odds_from_markets(cache)

    def _update_match_score(self, match_id: str, ms: dict):
        cache = self._matches_cache.get(match_id)
        if not cache:
            return
        main = ms.get("main") or {}
        cache["score1"] = int(main.get("home", 0) or 0)
        cache["score2"] = int(main.get("away", 0) or 0)
        parts = ms.get("parts") or []
        if parts:
            last = parts[-1]
            cache["sub1"] = int(last.get("home", 0) or 0)
            cache["sub2"] = int(last.get("away", 0) or 0)

    def _build_sel_index(self, cache: dict):
        index = {}
        for mid, mkt in (cache.get("markets") or {}).items():
            for sid, sel in (mkt.get("selections") or {}).items():
                index[sid] = (mid, sel)
        cache["sel_index"] = index

    def _update_selection(self, match_id: str, new_val: dict):
        cache = self._matches_cache.get(match_id)
        if not cache:
            return
        sel_id = new_val.get("selId")
        index = cache.get("sel_index") or {}
        if sel_id not in index:
            return
        mid, sel = index[sel_id]
        sel["coeff"] = new_val.get("coeff", sel.get("coeff"))
        self._recalc_odds_from_markets(cache)

    # ============================================================
    # Пересчёт коэфов из markets
    # ============================================================
    def _recalc_odds_from_markets(self, cache: dict):
        odds1 = odds2 = 0.0
        total_line = total_over = total_under = 0.0
        h1 = h2 = h_o1 = h_o2 = 0.0

        p1 = cache.get("player1", "")
        p2 = cache.get("player2", "")

        for mkt in (cache.get("markets") or {}).values():
            model = mkt.get("model", "")
            selections = mkt.get("selections", {})
            if not selections:
                continue

            for sel in selections.values():
                name = sel.get("name", "")
                price = sel.get("coeff", {}).get("price", {})
                n = price.get("n", 0)
                d = price.get("d", 1)
                dec = _fraction_to_decimal(n, d)

                if model in ("MTCH_DNB", "MTCH_R"):
                    if name == p1:
                        odds1 = dec
                    elif name == p2:
                        odds2 = dec

                elif model in ("MTCH_TTLG", "MTCH_TTLP"):
                    m = re.search(r'(\d+\.?\d*)', name)
                    if m:
                        total_line = float(m.group(1))
                    if "Больше" in name:
                        total_over = dec
                    elif "Меньше" in name:
                        total_under = dec

                elif model in ("MTCH_HB", "MTCH_HBP"):
                    m = re.search(r'([+-]?\d+\.?\d*)', name)
                    if m:
                        line = float(m.group(1))
                        if name.startswith(p1):
                            h1 = line
                            h_o1 = dec
                        elif name.startswith(p2):
                            h2 = line
                            h_o2 = dec

        cache["odds1"] = odds1
        cache["odds2"] = odds2
        cache["total_line"] = total_line
        cache["total_over"] = total_over
        cache["total_under"] = total_under
        cache["handicap1"] = h1
        cache["handicap2"] = h2
        cache["handicap_odds1"] = h_o1
        cache["handicap_odds2"] = h_o2

    # ============================================================
    # Отправка
    # ============================================================
    async def _process_queues(self):
        while self.is_running:
            try:
                await asyncio.sleep(1)
                await self._try_send_matches()
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка очереди: {e}", exc_info=True)

    async def _try_send_matches(self):
        sent = 0
        current_time = time.time()

        for match_id, m in list(self._matches_cache.items()):
            if not m.get("player1") or not m.get("player2"):
                continue

            sport_key = m.get("sport", TABLE_TENNIS)

            first_seen = self._first_seen.get(match_id, current_time)
            if m.get("odds1", 0) == 0 and m.get("odds2", 0) == 0:
                if current_time - first_seen < 15:
                    continue

            last_sent = self._last_sent_time.get(match_id, 0)
            if current_time - last_sent < 1.0:
                continue

            current_state = (
                m.get("score1", 0), m.get("score2", 0),
                m.get("sub1", 0), m.get("sub2", 0),
                m.get("phase_num", 1),
                m.get("odds1", 0.0), m.get("odds2", 0.0),
                m.get("total_line", 0.0), m.get("total_over", 0.0),
                m.get("total_under", 0.0),
                m.get("handicap1", 0.0), m.get("handicap2", 0.0),
                m.get("handicap_odds1", 0.0), m.get("handicap_odds2", 0.0),
            )
            if m.get("_last_sent") == current_state:
                continue

            phase_num = m.get("phase_num", 1)
            phase_name = format_phase(sport_key, phase_num)

            slug = get_url_slug(self.bk_id, sport_key) or 'table-tennis'
            tour_slug = _slug(m.get("tournament", ""))
            p1_slug = _slug(m.get("player1", ""))
            p2_slug = _slug(m.get("player2", ""))
            match_url = (
                f"https://new.marathonbet.ru/su/betting/event/"
                f"{slug}/{tour_slug}/{p1_slug}-vs-{p2_slug}"
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
        logger.info(f"[{self.bk_id}] 🚀 API-парсер Marathon запущен")
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