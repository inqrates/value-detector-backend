# parsers/fonbet_api.py
"""
Fonbet API-парсер с поддержкой мультиспорта.

Виды:
  - Настольный теннис (table_tennis)     — comment "(2-1) 10:8",   счёт партий из misc
  - Волейбол (volleyball)                — comment "(25-18 16-18*)", счёт сетов из misc
  - Баскетбол (basketball)               — comment "(35-19 19-26 9-3)", счёт из суммы
  - Кибербаскет (cyber_basketball)       — как баскетбол

Коды факторов (одинаковые для НТ/волейбола/баскетбола):
  921/923 — П1/П2 (партии/сета/четверти)
  930/931 — тотал
  927/928 — фора
  910/912, 989/991, 1569/1572 — форы матча
  1696/1697, 1727/1728, 1730/1731 — тоталы матча
"""
import asyncio
import time
import logging
import re
from typing import Dict, Set, List, Optional
from playwright.async_api import Page, Response
from core.models import Match
from parsers.base import BaseParser
from core.browser_manager import browser_manager
from config import ZOOM, PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME, SPORT_URLS
from core.sport_map import get_sport_config, get_url_slug, format_phase

logger = logging.getLogger(__name__)


class FonbetApiParser(BaseParser):
    def __init__(self, detector=None, aggregator=None, enabled_sports=None):
        super().__init__('fonbet', detector=detector, aggregator=aggregator)

        self.enabled_sports = enabled_sports or ["table_tennis"]

        # ── URL: общий лайв при мультиспорте, иначе раздел одного вида ──
        if len(self.enabled_sports) > 1:
            self.url = SPORT_URLS["_all"].get("fonbet", self.url)
            logger.info(f"[fonbet] Мультиспорт: открываем общий лайв {self.url}")
        else:
            self.url = SPORT_URLS.get(self.enabled_sports[0], {}).get("fonbet", self.url)
            logger.info(f"[fonbet] Открываем раздел {self.enabled_sports[0]}: {self.url}")

        # ── Кэши ──
        self._events_cache: Dict[int, dict] = {}
        self._factors_cache: Dict[int, list] = {}
        self._live_cache: Dict[int, dict] = {}
        self._first_seen: Dict[int, float] = {}
        self._last_sent_time: Dict[int, float] = {}
        self._data_queue = asyncio.Queue()
        self.is_running = False

        # ── Мультиспорт: аккумулятор sports[] ──
        self._all_sports: Dict[int, dict] = {}
        self._tree_by_root: Dict[int, Set[int]] = {}
        self._sport_aliases: Dict[int, str] = {}
        self._sport_category_by_id: Dict[int, Optional[int]] = {}

    # ============================================================
    # Запуск и перехват
    # ============================================================
    async def start(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            self.page.on("response", self._handle_response)

            logger.info(f"🌐 [{self.bk_id}] Загрузка страницы: {self.url}")
            await self.page.goto(self.url, wait_until='domcontentloaded',
                                 timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME + 2000)
            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)
            logger.info(f"✅ [{self.bk_id}] Страница загружена, перехват активен")

    async def _handle_response(self, response: Response):
        url = response.url
        try:
            if 'events/list' in url:
                data = await response.json()
                await self._data_queue.put(('events', data))
            elif 'liveEvents' in url:
                data = await response.json()
                await self._data_queue.put(('live', data))
        except Exception:
            pass

    async def _process_queues(self):
        while self.is_running:
            try:
                msg_type, data = await asyncio.wait_for(
                    self._data_queue.get(), timeout=1.0
                )
                if msg_type == 'live':
                    self._process_live(data)
                elif msg_type == 'events':
                    self._process_events(data)
                await self._try_send_matches()
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[{self.bk_id}] Ошибка очереди: {e}", exc_info=True)

    # ============================================================
    # Аккумулятор sports[]
    # ============================================================
    def _accumulate_sports(self, sports: list) -> bool:
        changed = False
        for s in sports:
            sid = s.get('id')
            if not sid:
                continue
            if sid not in self._all_sports:
                changed = True
            self._all_sports[sid] = {
                'parentId': s.get('parentId'),
                'alias': s.get('alias', ''),
                'sportCategoryId': s.get('sportCategoryId'),
                'name': s.get('name', ''),
            }
            if s.get('alias'):
                self._sport_aliases[sid] = s['alias']
            if s.get('sportCategoryId') is not None:
                self._sport_category_by_id[sid] = s['sportCategoryId']
        return changed

    def _rebuild_tree(self):
        children: Dict[int, list] = {}
        for sid, meta in self._all_sports.items():
            pid = meta.get('parentId')
            if pid:
                children.setdefault(pid, []).append(sid)

        all_roots: Set[int] = set()
        for sport_key in self.enabled_sports:
            cfg = get_sport_config(self.bk_id, sport_key)
            for root_id in cfg.get('ids', []):
                all_roots.add(root_id)

        tree_by_root: Dict[int, Set[int]] = {}
        for root_id in all_roots:
            desc = {root_id}
            stack = [root_id]
            while stack:
                cur = stack.pop()
                for child in children.get(cur, []):
                    if child not in desc:
                        desc.add(child)
                        stack.append(child)
            tree_by_root[root_id] = desc

        self._tree_by_root = tree_by_root

        total = sum(len(s) for s in tree_by_root.values())
        roots_info = ", ".join(f"root={r}(ids={len(s)})" for r, s in tree_by_root.items())
        logger.info(
            f"[{self.bk_id}] Дерево: enabled={self.enabled_sports}, "
            f"roots: {roots_info}, total_seen={len(self._all_sports)}, tree_total={total}"
        )

    # ============================================================
    # events/list
    # ============================================================
    def _process_events(self, data: dict):
        sports = data.get('sports', [])
        if sports:
            changed = self._accumulate_sports(sports)
            if changed:
                self._rebuild_tree()

        if not self._tree_by_root:
            return

        enabled_ids = set()
        for s in self._tree_by_root.values():
            enabled_ids |= s

        miscs_by_eid = {
            m.get('id'): m for m in data.get('eventMiscs', []) if m.get('id')
        }

        events = data.get('events', [])
        for ev in events:
            sid = ev.get('sportId')
            if sid not in enabled_ids:
                continue
            eid = ev['id']
            if eid not in self._events_cache:
                self._events_cache[eid] = {'_last_sent': None}
                self._first_seen[eid] = time.time()

            self._events_cache[eid]['player1'] = ev.get('team1', '') or ev.get('participant1', '')
            self._events_cache[eid]['player2'] = ev.get('team2', '') or ev.get('participant2', '')
            self._events_cache[eid]['tournament'] = ev.get('tournamentName', '')
            self._events_cache[eid]['sport_id'] = sid
            self._events_cache[eid]['sport_category_id'] = self._sport_category_by_id.get(sid)
            self._events_cache[eid]['parent_id'] = ev.get('parentId')
            self._events_cache[eid]['kind'] = ev.get('kind')
            self._events_cache[eid]['level'] = ev.get('level')

            live_misc = miscs_by_eid.get(eid, {})
            s1 = live_misc.get('score1')
            s2 = live_misc.get('score2')
            if s1 is None:
                s1 = ev.get('score1')
            if s2 is None:
                s2 = ev.get('score2')
            comment_val = (
                live_misc.get('comment')
                or ev.get('comment')
                or ev.get('scoreStr')
                or ''
            )

            has_data = (s1 is not None) or (s2 is not None) or bool(comment_val)
            is_live = ev.get('isLive') or has_data
            if is_live:
                if eid not in self._live_cache:
                    self._live_cache[eid] = {}
                    self._first_seen[eid] = time.time()
                self._live_cache[eid]['score1'] = s1
                self._live_cache[eid]['score2'] = s2
                self._live_cache[eid]['comment'] = comment_val
                self._live_cache[eid]['liveDelay'] = (
                    live_misc.get('liveDelay', ev.get('liveDelay', 0)) or 0
                )

        for item in data.get('customFactors', []):
            eid = item.get('e')
            if eid:
                self._factors_cache[eid] = item.get('factors', [])

        if 'eventMiscs' in data:
            self._process_live({'eventMiscs': data['eventMiscs']})

    def _process_live(self, data: dict):
        miscs = data.get('eventMiscs', [])
        for m in miscs:
            eid = m.get('id')
            if not eid:
                continue
            if eid not in self._live_cache:
                self._live_cache[eid] = {}
                self._first_seen[eid] = time.time()
            s1 = m.get('score1')
            s2 = m.get('score2')
            if s1 is not None:
                self._live_cache[eid]['score1'] = s1
            if s2 is not None:
                self._live_cache[eid]['score2'] = s2
            if m.get('comment'):
                self._live_cache[eid]['comment'] = m['comment']

    # ============================================================
    # sport_key события
    # ============================================================
    def _resolve_sport_key(self, sport_id, sport_category_id) -> Optional[str]:
        if sport_id is None:
            return None
        for sport_key in self.enabled_sports:
            cfg = get_sport_config(self.bk_id, sport_key)
            if not cfg:
                continue
            in_tree = False
            for root_id in cfg.get('ids', []):
                if sport_id in self._tree_by_root.get(root_id, set()):
                    in_tree = True
                    break
            if not in_tree:
                continue
            exclude = cfg.get('exclude_category_ids') or []
            if exclude and sport_category_id in exclude:
                continue
            include = cfg.get('category_ids')
            if include and sport_category_id not in include:
                continue
            return sport_key
        return None

    # ============================================================
    # parse_comment — общая логика для всех видов
    # ============================================================
    def _parse_comment(self, comment: str, sport_key: str,
                       children_miscs: list = None) -> dict:
        result = {
            "sub1": 0, "sub2": 0,
            "total1": 0, "total2": 0,
            "phase_num": 0, "phase_name": "",
        }
        if not comment:
            comment = ""

        # ── НТ: "(2-1) 10:8" ──
        if sport_key == "table_tennis":
            if comment:
                # Очки в партии: "10:8" после скобок
                after = re.sub(r'^\([^)]*\)\s*', '', comment)
                colon = re.findall(r'(\d+):(\d+)', after)
                if colon:
                    result["sub1"] = int(colon[0][0])
                    result["sub2"] = int(colon[0][1])
                # Счёт партий: "(2-1)"
                first_bracket = re.search(r'\(([^)]+)\)', comment)
                s1 = s2 = 0
                if first_bracket:
                    m = re.match(r'[*]?(\d+)[*]?-(\d+)', first_bracket.group(1))
                    if m:
                        s1, s2 = int(m.group(1)), int(m.group(2))
                result["total1"] = s1
                result["total2"] = s2
                pn = s1 + s2 + 1
                result["phase_num"] = pn
                result["phase_name"] = format_phase(sport_key, pn)
            return result

        # ── Волейбол: "(25-18 16-18*)" — как баскетбол, но счёт сетов из misc ──
        if sport_key in ("volleyball", "beach_volleyball"):
            if comment:
                first_bracket = re.search(r'\(([^)]+)\)', comment)
                inner = first_bracket.group(1) if first_bracket else comment
                # Убираем звёздочки (кто подаёт)
                inner = inner.replace('*', '')
                pairs = re.findall(r'(\d+)-(\d+)', inner)
                phase_num = 0
                sub1 = sub2 = 0
                for i, (h, a) in enumerate(pairs):
                    h, a = int(h), int(a)
                    if h > 0 or a > 0:
                        phase_num = i + 1
                        sub1, sub2 = h, a
                if phase_num == 0:
                    phase_num = 1
                result["sub1"] = sub1
                result["sub2"] = sub2
                result["phase_num"] = phase_num
                result["phase_name"] = format_phase(sport_key, phase_num)
                # ВАЖНО: total1/total2 = 0 — счёт сетов берётся из misc.score1/2
                return result

            # Fallback: comment пустой — из активного ребёнка
            if children_miscs:
                active_idx = 0
                active_s1 = active_s2 = 0
                for i, cm in enumerate(children_miscs):
                    s1 = cm.get('score1') or 0
                    s2 = cm.get('score2') or 0
                    if s1 > 0 or s2 > 0:
                        active_idx = i + 1
                        active_s1 = s1
                        active_s2 = s2
                if active_idx == 0:
                    active_idx = 1
                result["sub1"] = active_s1
                result["sub2"] = active_s2
                result["phase_num"] = active_idx
                result["phase_name"] = format_phase(sport_key, active_idx)
            return result

        # ── Баскетбол: "(35-19 19-26 9-3)" — сумма четвертей ──
        if sport_key in ("basketball", "cyber_basketball"):
            if comment:
                first_bracket = re.search(r'\(([^)]+)\)', comment)
                inner = first_bracket.group(1) if first_bracket else comment
                pairs = re.findall(r'(\d+)-(\d+)', inner)
                phase_num = 0
                sub1 = sub2 = 0
                total1 = total2 = 0
                for i, (h, a) in enumerate(pairs):
                    h, a = int(h), int(a)
                    total1 += h
                    total2 += a
                    if h > 0 or a > 0:
                        phase_num = i + 1
                        sub1, sub2 = h, a
                if phase_num == 0:
                    phase_num = 1
                result["sub1"] = sub1
                result["sub2"] = sub2
                result["phase_num"] = phase_num
                result["phase_name"] = format_phase(sport_key, phase_num)
                result["total1"] = total1
                result["total2"] = total2
                return result

            if children_miscs:
                active_idx = 0
                active_s1 = active_s2 = 0
                for i, cm in enumerate(children_miscs):
                    s1 = cm.get('score1') or 0
                    s2 = cm.get('score2') or 0
                    if s1 > 0 or s2 > 0:
                        active_idx = i + 1
                        active_s1 = s1
                        active_s2 = s2
                if active_idx == 0:
                    active_idx = 1
                result["sub1"] = active_s1
                result["sub2"] = active_s2
                result["phase_num"] = active_idx
                result["phase_name"] = format_phase(sport_key, active_idx)
            return result

        return result

    # ============================================================
    # _parse_factors
    # ============================================================
    def _parse_factors(self, factors: list) -> dict:
        result = {
            'odds1': 0.0, 'odds2': 0.0,
            'totalLine': 0.0, 'totalOver': 0.0, 'totalUnder': 0.0,
            'handicap1': 0.0, 'handicap2': 0.0,
            'handicapOdds1': 0.0, 'handicapOdds2': 0.0,
        }
        for f in factors:
            if f.get('f') == 921:
                result['odds1'] = f.get('v', 0.0)
            elif f.get('f') == 923:
                result['odds2'] = f.get('v', 0.0)

        total_lines = {}
        handicap_pairs = []
        for f in factors:
            pt = str(f.get('pt', '')).strip()
            val = f.get('v', 0.0)
            if re.match(r'^\d+\.?\d*$', pt):
                line = float(pt)
                total_lines.setdefault(line, [None, None])
                if total_lines[line][0] is None:
                    total_lines[line][0] = val
                elif total_lines[line][1] is None:
                    total_lines[line][1] = val
            elif re.match(r'^[+-]\d+\.?\d*$', pt):
                handicap_pairs.append((float(pt), val))

        if total_lines:
            first_line = next(iter(total_lines))
            result['totalLine'] = first_line
            result['totalOver'] = total_lines[first_line][0] or 0.0
            result['totalUnder'] = total_lines[first_line][1] or 0.0

        if len(handicap_pairs) >= 2:
            handicap_pairs.sort(key=lambda x: x[0])
            result['handicap1'], result['handicapOdds1'] = handicap_pairs[0]
            result['handicap2'], result['handicapOdds2'] = handicap_pairs[1]
        elif len(handicap_pairs) == 1:
            result['handicap1'], result['handicapOdds1'] = handicap_pairs[0]

        if result['totalLine'] == 0:
            for f in factors:
                if f.get('f') == 930:
                    result['totalOver'] = f.get('v', 0.0)
                    result['totalLine'] = f.get('p', 0) / 100.0
                elif f.get('f') == 931:
                    result['totalUnder'] = f.get('v', 0.0)
                    result['totalLine'] = f.get('p', 0) / 100.0

        if result['handicap1'] == 0 and result['handicapOdds1'] == 0:
            for f in factors:
                if f.get('f') == 927:
                    result['handicap1'] = f.get('p', 0) / 100.0
                    result['handicapOdds1'] = f.get('v', 0.0)
                elif f.get('f') == 928:
                    result['handicap2'] = f.get('p', 0) / 100.0
                    result['handicapOdds2'] = f.get('v', 0.0)

        return result

    # ============================================================
    # Отправка в detector
    # ============================================================
    async def _try_send_matches(self):
        current_time = time.time()
        sent = 0

        for eid, live in list(self._live_cache.items()):
            event = self._events_cache.get(eid, {})
            if not event.get('player1') or not event.get('player2'):
                continue

            sport_id = event.get('sport_id')
            sport_category_id = event.get('sport_category_id')
            sport_key = self._resolve_sport_key(sport_id, sport_category_id)
            if not sport_key:
                continue

            s1 = live.get('score1')
            s2 = live.get('score2')
            comment = live.get('comment', '') or ''
            has_live_data = (s1 is not None) or (s2 is not None) or bool(comment)
            if not has_live_data:
                continue

            # Кэфы из root события
            factors = self._factors_cache.get(eid, [])
            parsed = self._parse_factors(factors)

            # Для баскетбола: если в root нет кэфов, берём из активной четверти
            if sport_key in ("basketball", "cyber_basketball"):
                if not parsed['odds1'] and not parsed['odds2']:
                    children = [
                        (ceid, cev) for ceid, cev in self._events_cache.items()
                        if cev.get('parent_id') == eid
                    ]
                    if children:
                        children.sort(key=lambda x: x[1].get('kind', 0))
                        for ceid, cev in reversed(children):
                            cf = self._factors_cache.get(ceid, [])
                            cp = self._parse_factors(cf)
                            if cp['odds1'] or cp['odds2']:
                                parsed.update(cp)
                                break

            first_seen = self._first_seen.get(eid, current_time)
            if parsed['odds1'] == 0 and parsed['odds2'] == 0 and current_time - first_seen < 15:
                continue

            last_sent = self._last_sent_time.get(eid, 0)
            if current_time - last_sent < 1.0:
                continue

            # Собираем miscs детей (для fallback по всем видам)
            children_miscs = []
            if sport_key in ("basketball", "cyber_basketball", "volleyball", "beach_volleyball"):
                children_eids = [
                    ceid for ceid, cev in self._events_cache.items()
                    if cev.get('parent_id') == eid
                ]
                children_eids.sort(key=lambda c: self._events_cache.get(c, {}).get('kind', 0))
                for ceid in children_eids:
                    cm = dict(self._live_cache.get(ceid, {}) or {})
                    children_miscs.append(cm)

            # Парсим comment
            score_info = self._parse_comment(comment, sport_key, children_miscs=children_miscs)
            sub1 = score_info["sub1"]
            sub2 = score_info["sub2"]
            phase_name = score_info["phase_name"]

            # Итоговый score1/score2
            if sport_key in ("basketball", "cyber_basketball"):
                # Баскетбол — сумма очков из comment
                if score_info["total1"] or score_info["total2"]:
                    final_score1 = score_info["total1"]
                    final_score2 = score_info["total2"]
                else:
                    final_score1 = s1 or 0
                    final_score2 = s2 or 0
            else:
                # НТ, волейбол, остальные — счёт партий/сетов из misc
                final_score1 = s1 or 0
                final_score2 = s2 or 0

            current_state = (
                final_score1, final_score2, sub1, sub2,
                parsed['odds1'], parsed['odds2'],
                parsed['totalLine'], parsed['totalOver'], parsed['totalUnder'],
                parsed['handicap1'], parsed['handicap2'],
                parsed['handicapOdds1'], parsed['handicapOdds2'],
            )
            if event.get('_last_sent') == current_state:
                continue

            # URL
            url_slug = get_url_slug(self.bk_id, sport_key) or 'table-tennis'
            alias = self._sport_aliases.get(sport_id)
            if alias and sport_id:
                match_url = f"https://fon.bet/live/{url_slug}/category/{alias}/{sport_id}/{eid}"
            else:
                match_url = f"https://fon.bet/live/{url_slug}/{eid}"

            match = Match(
                bk_id='fonbet',
                match_id=str(eid),
                player1=event['player1'],
                player2=event['player2'],
                score1=final_score1,
                score2=final_score2,
                sub_score1=sub1,
                sub_score2=sub2,
                tournament=event.get('tournament', ''),
                odds1=parsed['odds1'],
                odds2=parsed['odds2'],
                total_line=parsed['totalLine'],
                total_over=parsed['totalOver'],
                total_under=parsed['totalUnder'],
                handicap1=parsed['handicap1'],
                handicap2=parsed['handicap2'],
                handicap_odds1=parsed['handicapOdds1'],
                handicap_odds2=parsed['handicapOdds2'],
                timestamp=current_time,
                raw_time=phase_name or comment,
                sport=sport_key,
                match_url=match_url,
            )

            # ── Лог с правильным названием фазы ──
            logger.info(
                f"[{self.bk_id}] 🟢 [{sport_key}] {match.player1} vs {match.player2} | "
                f"матч {match.score1}:{match.score2} | "
                f"{phase_name} {match.sub_score1}:{match.sub_score2} | "
                f"К: {match.odds1}/{match.odds2}"
            )

            if self.detector:
                await self.detector.process(match)
            if self.aggregator:
                self.aggregator.update(match)

            event['_last_sent'] = current_state
            self._last_sent_time[eid] = current_time
            sent += 1

        if sent:
            logger.info(f"[{self.bk_id}] ✅ Отправлено: {sent} (в кеше: {len(self._live_cache)})")

    # ============================================================
    # Loop
    # ============================================================
    async def parse(self) -> List[Match]:
        return []

    async def run(self):
        self.is_running = True
        logger.info(f"[{self.bk_id}] 🚀 Fonbet API-парсер запущен, виды: {self.enabled_sports}")
        asyncio.create_task(self._process_queues())
        while self.is_running:
            try:
                await self.start()
                while self.is_running:
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"[{self.bk_id}] Критическая ошибка: {e}", exc_info=True)
                await self.stop()
                await asyncio.sleep(5)

    async def stop(self):
        self.is_running = False
        logger.info(f"[{self.bk_id}] 🛑 Парсер остановлен")