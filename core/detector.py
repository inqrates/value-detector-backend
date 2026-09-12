# core/detector.py
import asyncio
import time
import logging
from collections import defaultdict
from core.models import Match
from core.normalizer import normalizer
from core.sport_map import format_phase          # <-- ПАТЧ 2a

logger = logging.getLogger(__name__)


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

            self.states[key][match.bk_id] = match
            await self._analyze_match(key)

    async def _analyze_match(self, key: str):
        bk_states = self.states.get(key, {})
        if not bk_states:
            return

        now = time.time()

        # Удаляем записи старше 30 секунд (предотвращает бесконечный рост задержки)
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

        # Таймер консенсуса
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
                    # Вычисляем реальную задержку (максимальное время с последнего обновления)
                    max_delay = 0.0
                    for bk_id in active_lagging:
                        m = bk_states[bk_id]
                        if m.timestamp > 0:
                            delay = now - m.timestamp
                            if delay > max_delay:
                                max_delay = delay

                    # Ограничиваем максимум 60 секунд
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

    # ---------- Хелперы ----------
    @staticmethod
    def _set_number(m) -> int:
        return m.score1 + m.score2 + 1

        # <-- ПАТЧ 2b: хелпер метки фазы
    def _phase_label(self, m, consensus_score, sport):
        """
        Возвращает готовую метку фазы ('4-я партия', '2-й сет', '3-я четверть').

        Приоритет:
          1) raw_time — если в нём сформированная фаза, И слово-маркер
             соответствует виду спорта. Это защищает от случаев, когда
             БК кладёт в raw_time чужую формулировку (например, ligastavok
             пишет 'сет' для настольного тенниса — НЕ примем, уйдём в fallback).
          2) Вычисление через format_phase(sport, N), где N = сумма партий/сетов + 1.
        """
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
    # -- / ПАТЧ 2b -->

    def _has_parse_bug(self, majority_score, bk_states, lagging_bks):
        m_s1, m_s2, m_sub1, m_sub2 = majority_score
        if m_s1 == 0 and m_s2 == 0:
            return False
        majority_sub_is_zero = (m_sub1 == 0 and m_sub2 == 0)
        for bk_id in lagging_bks:
            m = bk_states[bk_id]
            lag_sub_is_zero = (m.sub_score1 == 0 and m.sub_score2 == 0)
            if majority_sub_is_zero != lag_sub_is_zero:
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

        for bk_id in lagging_bks:
            m = bk_states[bk_id]
            lag_set = self._set_number(m)

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

    # <-- ПАТЧ 2c: _emit_signal с правильной фазой
    async def _emit_signal(self, key, fast_bks, slow_bks, consensus_score, delay, is_first):
        fast_match = self.states[key].get(fast_bks[0])
        if not fast_match:
            return

        sport = getattr(fast_match, "sport", "table_tennis") or "table_tennis"

        fast_phase = self._phase_label(fast_match, consensus_score, sport)

        slow_info = []
        for bk_id in slow_bks:
            m = self.states[key].get(bk_id)
            if m:
                slow_phase = self._phase_label(m, None, sport)
                slow_info.append(
                    f"{bk_id} ({slow_phase}) {m.sub_score1}:{m.sub_score2} "
                    f"(кэфы {m.odds1:.2f}/{m.odds2:.2f})"
                )

        tag = "🚨 НОВЫЙ " if is_first else "⏳ Длится"

        logger.warning(
            f"{tag} | {delay:.1f}с | [{sport}] {fast_match.player1} vs {fast_match.player2}\n"
            f"   ⚡ {', '.join(fast_bks)} ({fast_phase}): {consensus_score[2]}:{consensus_score[3]}\n"
            f"   🐢 " + " | ".join(slow_info)
        )

        if self.broadcast_callback:
            slow_bk = slow_bks[0] if slow_bks else fast_bks[0]
            slow_match = self.states[key].get(slow_bk)
            if slow_match:
                match_id_for_slow = slow_match.match_id
                # 1) сначала пробуем готовый URL, который собрал парсер
                match_url = getattr(slow_match, 'match_url', '') or ''
                # 2) фолбэк — короткий ID-only URL
                if not match_url:
                    match_url = self._get_match_url(slow_bk, match_id_for_slow)
            else:
                match_id_for_slow = fast_match.match_id  # fallback
                match_url = getattr(fast_match, 'match_url', '') or \
                            self._get_match_url(fast_bks[0], match_id_for_slow)

            slow_match = self.states[key].get(slow_bk)

            signal_data = {
                "match_teams": [fast_match.player1, fast_match.player2],
                "sport": sport,                       # <-- "table_tennis" / "volleyball" / "basketball" / "cyber_basketball"
                "fast_phase": fast_phase,             # <-- "2-я партия" / "3-й сет" / "4-я четверть"
                "score": [consensus_score[0], consensus_score[1]],
                "sub_score": [consensus_score[2], consensus_score[3]],
                "fast_bk": fast_bks[0],
                "fast_score": [fast_match.score1, fast_match.score2],
                "fast_sub_score": [fast_match.sub_score1, fast_match.sub_score2],
                "fast_odds": [fast_match.odds1, fast_match.odds2],
                "slow_bk": slow_bk,
                "slow_score": [slow_match.score1, slow_match.score2] if slow_match else [0, 0],
                "slow_sub_score": [slow_match.sub_score1, slow_match.sub_score2] if slow_match else [0, 0],
                "slow_odds": [slow_match.odds1, slow_match.odds2] if slow_match else [0, 0],
                "delay": round(delay, 1),
                "match_id": match_id_for_slow,
                "match_url": match_url,
                "is_new": is_first,
                "tournament": fast_match.tournament,
            }
            await self.broadcast_callback({"type": "signal", "payload": signal_data})
    # -- / ПАТЧ 2c -->

    def _get_match_url(self, bk_id: str, match_id: str) -> str:
        """
        Фолбэк-шаблоны: короткие ID-only URL.
        Большинство сайтов (SPA) сами редиректят на полный URL,
        прочитав ID из последнего сегмента.
        """
        templates = {
            "fonbet":     "https://fon.bet/live/table-tennis/{id}",
            "winline":    "https://winline.ru/live/sport/nastolijnyj_tennis/{id}",
            "ligastavok": "https://www.ligastavok.ru/sports/table-tennis/x-p-id-0-service-id-27-ext-id-{id}",
            "leon":       "https://leon.ru/bets/table-tennis/{id}",
            "olimp":      "https://www.olimp.bet/live/nastolnyy-tennis-40/x/x-{id}",
            "betcity":    "https://betcity.ru/ru/live/table-tennis/{id}",
            "marathon":   "https://new.marathonbet.ru/su/betting/event/table-tennis/x/{id}",
            "zenit":      "https://zenit.win/live/134/{id}",
            "sportbet":   "https://sportbet.ru/live/table-tennis/x--x/x-vs-x--{id}?isTime=1&h=all&page=main",
            "baltbet":    "https://baltbet.ru/event/{id}",
        }
        tpl = templates.get(bk_id)
        return tpl.format(id=match_id) if tpl else ""

    def _get_match_key(self, p1, p2, sport="table_tennis", tournament=None):
        """Возвращает строковый ключ (совместим с aggregator._get_key)."""
        n1 = normalizer.normalize_name(p1)
        n2 = normalizer.normalize_name(p2)
        if n1 > n2:
            n1, n2 = n2, n1
        sport = sport or "table_tennis"
        return f"{sport}::{n1}||{n2}"