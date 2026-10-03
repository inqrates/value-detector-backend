# core/detector.py
import asyncio
import time
import logging
from collections import defaultdict
from core.models import Match
from core.normalizer import normalizer
from core.sport_map import format_phase

logger = logging.getLogger(__name__)


# ── Порог «fast уже много набрал, а slow молчит» ──
# Реальная задержка послегола — обычно +1…+3 очка. Если fast набрал
# >= N очков в текущей партии, а slow показывает 0:0 — это потеря
# данных, а не задержка. Защищает от ложных сигналов (LigaStavok 0:0).
ZERO_SUB_FAST_THRESHOLD = 4


class Detector:
    def __init__(self, broadcast_callback=None):
        self.broadcast_callback = broadcast_callback
        self.states = defaultdict(dict)
        self.consensus_time = {}
        self.locks = defaultdict(asyncio.Lock)

        self.known_matches = defaultdict(set)

        self.last_signal_context = {}
        self.last_signal_time = {}
        self.last_global_update = {}

        self._phase_history = defaultdict(dict)

        self.STALE_TIMEOUT = 25.0
        self.MIN_CONSENSUS_BKS = 2
        self.DELAY_THRESHOLD = 1.5
        self.REPEAT_INTERVAL = 5.0
        self.MIN_SCORE_DIFF = 2

    async def process(self, match: 'Match'):
        sport = getattr(match, "sport", "table_tennis") or "table_tennis"
        key = self._get_match_key(match.player1, match.player2, sport=sport)

        async with self.locks[key]:
            is_new_for_bk = match.bk_id not in self.known_matches[key]
            if is_new_for_bk:
                self.known_matches[key].add(match.bk_id)
                if len(self.known_matches[key]) == 2:
                    logger.info(
                        f"🤝 Матчинг: '{match.player1}' vs '{match.player2}' "
                        f"→ {sorted(self.known_matches[key])}"
                    )

            old = self.states[key].get(match.bk_id)
            if old is not None:
                old_phase = (getattr(old, "raw_time", "") or "").strip()
                new_phase = (getattr(match, "raw_time", "") or "").strip()
                if old_phase and new_phase and old_phase != new_phase:
                    self._phase_history[key][match.bk_id] = {
                        'phase_name': old_phase,
                        'score1': old.score1,
                        'score2': old.score2,
                        'sub1': old.sub_score1,
                        'sub2': old.sub_score2,
                        'odds1': old.odds1,
                        'odds2': old.odds2,
                        'ts': old.timestamp,
                    }

            self.states[key][match.bk_id] = match
            await self._analyze_match(key)

    async def _analyze_match(self, key: str):
        bk_states = self.states.get(key, {})
        if not bk_states:
            return

        now = time.time()

        expired = [bk for bk, m in bk_states.items() if now - m.timestamp > 30]
        for bk in expired:
            del bk_states[bk]
        if not bk_states:
            return

        score_groups = defaultdict(list)
        for bk_id, m in bk_states.items():
            is_active = (m.odds1 > 0 and m.odds2 > 0)
            if is_active:
                score = (m.score1, m.score2, m.sub_score1, m.sub_score2)
                score_groups[score].append(bk_id)

        if not score_groups:
            return

        sorted_groups = sorted(score_groups.items(), key=lambda x: len(x[1]), reverse=True)
        majority_score, majority_bks = sorted_groups[0]

        if len(majority_bks) < self.MIN_CONSENSUS_BKS and len(bk_states) >= 3:
            return

        lagging_bks = []
        for score, bks in sorted_groups[1:]:
            if not bks:
                continue
            if self._is_score_newer(majority_score, score):
                lagging_bks.extend(bks)

        if lagging_bks:
            self.last_global_update[key] = now
        else:
            last_update = self.last_global_update.get(key, now)
            if now - last_update > self.STALE_TIMEOUT:
                self.consensus_time.pop(key, None)
                self.last_signal_context.pop(key, None)
                self.last_signal_time.pop(key, None)
                return

        if not lagging_bks:
            self.consensus_time.pop(key, None)
            self.last_signal_context.pop(key, None)
            self.last_signal_time.pop(key, None)
            return

        if self._has_parse_bug(majority_score, bk_states, lagging_bks):
            return

        if key not in self.consensus_time or self.consensus_time[key]['score'] != majority_score:
            self.consensus_time[key] = {'score': majority_score, 'time': now}

        delay_since_detection = now - self.consensus_time[key]['time']

        if delay_since_detection >= self.DELAY_THRESHOLD:
            current_context = (majority_score, tuple(sorted(lagging_bks)))
            last_context = self.last_signal_context.get(key)
            last_signal_time = self.last_signal_time.get(key, 0)

            is_first_signal = (current_context != last_context)
            is_repeat_time = (now - last_signal_time >= self.REPEAT_INTERVAL)

            if is_first_signal or is_repeat_time:
                active_lagging = [
                    bk for bk in lagging_bks
                    if bk_states[bk].odds1 > 0 and bk_states[bk].odds2 > 0
                ]

                if active_lagging:
                    max_delay = 0.0
                    for bk_id in active_lagging:
                        m = bk_states[bk_id]
                        if m.timestamp > 0:
                            delay = now - m.timestamp
                            if delay > max_delay:
                                max_delay = delay

                    if max_delay > 60:
                        max_delay = 60.0

                    if max_delay < 0.1:
                        max_delay = delay_since_detection

                    is_significant = self._is_significant_gap(majority_score, bk_states, active_lagging)

                    if is_significant:
                        await self._emit_signal(
                            key, majority_bks, active_lagging,
                            majority_score, max_delay, is_first_signal
                        )
                        self.last_signal_context[key] = current_context
                        self.last_signal_time[key] = now

    @staticmethod
    def _set_number(m) -> int:
        return m.score1 + m.score2 + 1

    @staticmethod
    def _is_reverse_order(fast_match, slow_match) -> bool:
        if not fast_match or not slow_match:
            return False
        try:
            fn1 = normalizer.normalize_name(fast_match.player1 or "")
            fn2 = normalizer.normalize_name(fast_match.player2 or "")
            sn1 = normalizer.normalize_name(slow_match.player1 or "")
            sn2 = normalizer.normalize_name(slow_match.player2 or "")
        except Exception:
            return False
        if not (fn1 and fn2 and sn1 and sn2):
            return False
        return fn1 == sn2 and fn2 == sn1

    def _phase_label(self, m, consensus_score, sport):
        raw = (getattr(m, "raw_time", "") or "").strip()

        expected_word = {
            "table_tennis":     "партия",
            "volleyball":       "сет",
            "beach_volleyball": "сет",
            "basketball":       "четверть",
            "cyber_basketball": "четверть",
            "football":         "тайм",
            "futsal":           "тайм",
            "hockey":           "период",
            "tennis":           "сет",
            "handball":         "тайм",
            "cricket":          "иннинг",
            "cybersport":       "карта",
        }.get(sport, "фаза")

        if expected_word in raw:
            return raw

        if consensus_score is not None:
            n = consensus_score[0] + consensus_score[1] + 1
        else:
            n = self._set_number(m)
        return format_phase(sport, n)

    def _has_parse_bug(self, majority_score, bk_states, lagging_bks):
        """
        Защита от ложных сигналов при «битых» данных у одной из БК.

        Пример: fast sub 6:10, slow sub 0:0 (LigaStavok теряет данные).
        Это не задержка, а рассинхрон. Пропускаем.

        Раньше был ранний `return False` при score1=score2=0, из-за чего
        защита не работала в 1-й партии. Теперь смотрим ещё и на sub.
        """
        m_s1, m_s2, m_sub1, m_sub2 = majority_score
        majority_sub_sum = (m_sub1 or 0) + (m_sub2 or 0)

        if m_s1 == 0 and m_s2 == 0 and majority_sub_sum == 0:
            return False

        majority_sub_is_zero = (m_sub1 == 0 and m_sub2 == 0)

        for bk_id in lagging_bks:
            m = bk_states[bk_id]
            lag_sub_is_zero = (m.sub_score1 == 0 and m.sub_score2 == 0)
            if majority_sub_is_zero != lag_sub_is_zero:
                logger.debug(
                    f"[detector] parse_bug: majority_sub="
                    f"{m_sub1}:{m_sub2} lag_sub={m.sub_score1}:{m.sub_score2} "
                    f"bk={bk_id}"
                )
                return True
        return False

    def _is_score_newer(self, new_score: tuple, old_score: tuple) -> bool:
        n_s1, n_s2, n_sub1, n_sub2 = new_score
        o_s1, o_s2, o_sub1, o_sub2 = old_score
        if n_s1 > o_s1 or n_s2 > o_s2:
            return True
        if n_s1 < o_s1 or n_s2 < o_s2:
            return False
        if n_sub1 > o_sub1 or n_sub2 > o_sub2:
            return True
        return False

    def _is_significant_gap(self, majority_score, bk_states, lagging_bks):
        m_s1, m_s2, m_sub1, m_sub2 = majority_score
        maj_set = m_s1 + m_s2 + 1

        fast_max_sub = max(m_sub1 or 0, m_sub2 or 0)

        for bk_id in lagging_bks:
            m = bk_states[bk_id]
            lag_set = self._set_number(m)

            # ── СТРАХОВКА: slow потерял sub (0:0), а fast уже много набрал ──
            slow_sub_sum = (m.sub_score1 or 0) + (m.sub_score2 or 0)
            if slow_sub_sum == 0 and fast_max_sub >= ZERO_SUB_FAST_THRESHOLD:
                logger.debug(
                    f"[detector] skip zero-slow-sub bk={bk_id} "
                    f"fast_sub={m_sub1}:{m_sub2} slow_sub=0:0"
                )
                continue

            if lag_set < maj_set:
                if max(m.sub_score1, m.sub_score2) >= 11:
                    return True
                continue

            if lag_set > maj_set:
                continue

            if m.sub_score1 > m_sub1 or m.sub_score2 > m_sub2:
                continue

            diff = max(m_sub1 - m.sub_score1, m_sub2 - m.sub_score2)
            if diff >= self.MIN_SCORE_DIFF:
                return True

        return False

    async def _emit_signal(self, key, fast_bks, slow_bks, consensus_score, delay, is_first):
        fast_match = self.states[key].get(fast_bks[0])
        if not fast_match:
            return

        sport = getattr(fast_match, "sport", "table_tennis") or "table_tennis"
        fast_phase = self._phase_label(fast_match, consensus_score, sport)

        prev = self._phase_history.get(key, {}).get(fast_bks[0]) or {}
        fast_prev_phase = prev.get('phase_name', '') or ''
        fast_prev_score = [prev.get('score1', 0), prev.get('score2', 0)]
        fast_prev_sub = [prev.get('sub1', 0), prev.get('sub2', 0)]
        fast_prev_odds = [prev.get('odds1', 0.0), prev.get('odds2', 0.0)]

        slow_bk = slow_bks[0] if slow_bks else fast_bks[0]
        slow_match = self.states[key].get(slow_bk)

        reverse = self._is_reverse_order(fast_match, slow_match)

        if slow_match:
            if reverse:
                slow_score_pair = [slow_match.score2, slow_match.score1]
                slow_sub_pair = [slow_match.sub_score2, slow_match.sub_score1]
                slow_odds_pair = [slow_match.odds2, slow_match.odds1]
            else:
                slow_score_pair = [slow_match.score1, slow_match.score2]
                slow_sub_pair = [slow_match.sub_score1, slow_match.sub_score2]
                slow_odds_pair = [slow_match.odds1, slow_match.odds2]

            match_id_for_slow = slow_match.match_id
            match_url = getattr(slow_match, 'match_url', '') or ''
            if not match_url:
                match_url = self._get_match_url(slow_bk, match_id_for_slow)
        else:
            slow_score_pair = [0, 0]
            slow_sub_pair = [0, 0]
            slow_odds_pair = [0.0, 0.0]
            match_id_for_slow = fast_match.match_id
            match_url = getattr(fast_match, 'match_url', '') or \
                        self._get_match_url(fast_bks[0], match_id_for_slow)

        slow_info = []
        for bk_id in slow_bks:
            m = self.states[key].get(bk_id)
            if not m:
                continue
            rev = self._is_reverse_order(fast_match, m)
            if rev:
                ss1, ss2 = m.sub_score2, m.sub_score1
                o1, o2 = m.odds2, m.odds1
            else:
                ss1, ss2 = m.sub_score1, m.sub_score2
                o1, o2 = m.odds1, m.odds2
            slow_phase = self._phase_label(m, None, sport)
            slow_info.append(
                f"{bk_id} ({slow_phase}) {ss1}:{ss2} (кэфы {o1:.2f}/{o2:.2f})"
            )

        tag = "🚨 НОВЫЙ " if is_first else "⏳ Длится"
        rev_note = " [порядок команд инвертирован]" if reverse else ""

        logger.warning(
            f"{tag} | {delay:.1f}с | [{sport}] {fast_match.player1} vs {fast_match.player2}{rev_note}\n"
            f"   ⚡ {', '.join(fast_bks)} ({fast_phase}): {consensus_score[2]}:{consensus_score[3]}\n"
            f"   🐢 " + " | ".join(slow_info)
        )

        if self.broadcast_callback:
            signal_data = {
                "match_teams": [fast_match.player1, fast_match.player2],
                "sport": sport,
                "fast_phase": fast_phase,
                "score": [consensus_score[0], consensus_score[1]],
                "sub_score": [consensus_score[2], consensus_score[3]],
                "fast_bk": fast_bks[0],
                "fast_score": [fast_match.score1, fast_match.score2],
                "fast_sub_score": [fast_match.sub_score1, fast_match.sub_score2],
                "fast_odds": [fast_match.odds1, fast_match.odds2],
                "slow_bk": slow_bk,
                "slow_score": slow_score_pair,
                "slow_sub_score": slow_sub_pair,
                "slow_odds": slow_odds_pair,
                "delay": round(delay, 1),
                "match_id": match_id_for_slow,
                "match_url": match_url,
                "is_new": is_first,
                "tournament": fast_match.tournament,
                "fast_prev_phase": fast_prev_phase,
                "fast_prev_score": fast_prev_score,
                "fast_prev_sub_score": fast_prev_sub,
                "fast_prev_odds": fast_prev_odds,
            }
            await self.broadcast_callback({"type": "signal", "payload": signal_data})

    def _get_match_url(self, bk_id: str, match_id: str) -> str:
        templates = {
            "fonbet":     "https://fon.bet/live/table-tennis/category/x/x/{id}",
            "winline":    "https://winline.ru/live/sport/nastolijnyj_tennis/event/{id}",
            "ligastavok": "https://www.ligastavok.ru/sports/table-tennis/x-id-{id}-service-id-27-ext-id-{id}",
            "leon":       "https://leon.ru/bets/table-tennis/{id}",
            "olimp":      "https://www.olimp.bet/live/nastolnyy-tennis-40/x/x-{id}",
            "betcity":    "https://betcity.ru/ru/live/table-tennis/{id}",
            "marathon":   "https://new.marathonbet.ru/su/betting/event/table-tennis/x/x-vs-x",
            "zenit":      "https://zenit.win/live/134/{id}",
            "sportbet":   "https://sportbet.ru/live/table-tennis/x--x/x-vs-x--{id}?isTime=1&h=all&page=main",
            "baltbet":    "https://baltbet.ru/event/{id}",
        }
        tpl = templates.get(bk_id)
        return tpl.format(id=match_id) if tpl else ""

    def _get_match_key(self, p1, p2, sport="table_tennis", tournament=None):
        n1 = normalizer.normalize_name(p1)
        n2 = normalizer.normalize_name(p2)
        if n1 > n2:
            n1, n2 = n2, n1
        sport = sport or "table_tennis"
        return f"{sport}::{n1}||{n2}"