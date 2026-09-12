# core/aggregator.py
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from core.models import Match
from core.normalizer import normalizer

class OddsAggregator:
    def __init__(self, ttl: int = 10):
        self._matches: Dict[str, Dict[str, Match]] = {}
        self._last_update: Dict[str, float] = {}
        self._ttl = ttl
        self._bk_stats: Dict[str, Dict] = defaultdict(lambda: {
            "matches": 0,
            "total_delay": 0.0,
            "signals": 0,
            "slow": 0,
            "fast": 0,
            "last_update": 0.0
        })

    def _get_key(self, match: Match) -> str:
        p1 = normalizer.normalize_name(match.player1)
        p2 = normalizer.normalize_name(match.player2)
        if p1 > p2:
            p1, p2 = p2, p1
        # Добавляем вид спорта в ключ, чтобы матчи разных видов не смешивались
        sport = getattr(match, "sport", "table_tennis") or "table_tennis"
        return f"{sport}::{p1}||{p2}"

    def update(self, match: Match):
        key = self._get_key(match)
        if key not in self._matches:
            self._matches[key] = {}
        self._matches[key][match.bk_id] = match
        self._last_update[key] = time.time()
        self._bk_stats[match.bk_id]["matches"] += 1
        self._bk_stats[match.bk_id]["last_update"] = time.time()

    def _clean_expired(self):
        now = time.time()
        for key in list(self._matches.keys()):
            if now - self._last_update.get(key, 0) > self._ttl:
                del self._matches[key]

    def get_all_matches(self) -> List[Dict]:
        self._clean_expired()
        result = []
        for key, bk_data in self._matches.items():
            sample = next(iter(bk_data.values()))
            item = {
                'key': key,
                'player1': sample.player1,
                'player2': sample.player2,
                'tournament': sample.tournament,
                'bks': {}
            }
            for bk_id, m in bk_data.items():
                item['bks'][bk_id] = {
                    'match_id': m.match_id,
                    'match_url': getattr(m, 'match_url', ''),
                    'odds1': m.odds1,
                    'odds2': m.odds2,
                    'oddsX': m.oddsX,
                    'score1': m.score1,
                    'score2': m.score2,
                    'sub_score1': m.sub_score1,
                    'sub_score2': m.sub_score2,
                    'total_line': m.total_line,
                    'total_over': m.total_over,
                    'total_under': m.total_under,
                    'handicap1': m.handicap1,
                    'handicap2': m.handicap2,
                    'handicap_odds1': m.handicap_odds1,
                    'handicap_odds2': m.handicap_odds2,
                    'timestamp': m.timestamp
                }
            result.append(item)
        return result

    # ---------- Арбитраж ----------
    def find_arbitrage(self, min_profit: float = 0.5) -> List[Dict]:
        self._clean_expired()
        results = []
        for key, bk_data in self._matches.items():
            odds1 = [(bk, m.odds1, m.match_id) for bk, m in bk_data.items() if m.odds1 > 0]
            odds2 = [(bk, m.odds2, m.match_id) for bk, m in bk_data.items() if m.odds2 > 0]
            if not odds1 or not odds2:
                continue
            max1_bk, max1, id1 = max(odds1, key=lambda x: x[1])
            max2_bk, max2, id2 = max(odds2, key=lambda x: x[1])
            inv_sum = 1/max1 + 1/max2
            if inv_sum < 1:
                profit = (1 - inv_sum) * 100
                if profit >= min_profit:
                    sample = next(iter(bk_data.values()))
                    results.append({
                        'key': key,
                        'player1': sample.player1,
                        'player2': sample.player2,
                        'tournament': sample.tournament,
                        'best_p1': max1,
                        'best_p2': max2,
                        'bk_p1': max1_bk,
                        'bk_p2': max2_bk,
                        'match_id_p1': id1,
                        'match_id_p2': id2,
                        'profit_percent': profit,
                        'all': {bk: {'p1': m.odds1, 'p2': m.odds2, 'match_id': m.match_id} for bk, m in bk_data.items()}
                    })
        return results

    # ---------- Коридоры ----------
    def find_corridors(self) -> List[Dict]:
        self._clean_expired()
        results = []
        for key, bk_data in self._matches.items():
            handicaps = []
            for bk, m in bk_data.items():
                if m.handicap1 != 0 and m.handicap_odds1 > 0:
                    handicaps.append((bk, '1', m.handicap1, m.handicap_odds1, m.match_id))
                if m.handicap2 != 0 and m.handicap_odds2 > 0:
                    handicaps.append((bk, '2', m.handicap2, m.handicap_odds2, m.match_id))
            if len(handicaps) < 2:
                continue
            sample = next(iter(bk_data.values()))
            for i, (bk1, side1, line1, odd1, id1) in enumerate(handicaps):
                for bk2, side2, line2, odd2, id2 in handicaps[i+1:]:
                    if bk1 == bk2:
                        continue
                    if line1 < line2:
                        corridor_line = (line1, line2)
                        profit = (odd1 * odd2) / (odd1 + odd2)
                        results.append({
                            'key': key,
                            'player1': sample.player1,
                            'player2': sample.player2,
                            'tournament': sample.tournament,
                            'bk1': bk1, 'side1': side1, 'line1': line1, 'odd1': odd1,
                            'bk2': bk2, 'side2': side2, 'line2': line2, 'odd2': odd2,
                            'match_id1': id1,
                            'match_id2': id2,
                            'corridor': corridor_line,
                            'profit': profit
                        })
        return results

    # ---------- Валуи ----------
    def find_value_bets(self, threshold: float = 1.05) -> List[Dict]:
        self._clean_expired()
        results = []
        for key, bk_data in self._matches.items():
            p1s = [(m.odds1, m.match_id) for m in bk_data.values() if m.odds1 > 0]
            p2s = [(m.odds2, m.match_id) for m in bk_data.values() if m.odds2 > 0]
            if not p1s or not p2s:
                continue
            avg1 = sum(o for o, _ in p1s) / len(p1s)
            avg2 = sum(o for o, _ in p2s) / len(p2s)
            sample = next(iter(bk_data.values()))
            for bk, m in bk_data.items():
                if m.odds1 > avg1 * threshold:
                    results.append({
                        'key': key,
                        'player1': sample.player1,
                        'player2': sample.player2,
                        'tournament': sample.tournament,
                        'bk': bk,
                        'outcome': 'П1',
                        'odd': m.odds1,
                        'avg': avg1,
                        'ratio': m.odds1 / avg1,
                        'match_id': m.match_id,
                    })
                if m.odds2 > avg2 * threshold:
                    results.append({
                        'key': key,
                        'player1': sample.player1,
                        'player2': sample.player2,
                        'tournament': sample.tournament,
                        'bk': bk,
                        'outcome': 'П2',
                        'odd': m.odds2,
                        'avg': avg2,
                        'ratio': m.odds2 / avg2,
                        'match_id': m.match_id,
                    })
        return results

    # ---------- Статистика для советника ----------
    def get_advisor_stats(self) -> Dict:
        now = time.time()
        active_bks = {}
        for bk, stats in self._bk_stats.items():
            if now - stats["last_update"] < 30:
                active_bks[bk] = {
                    "matches": stats["matches"],
                    "signals": stats["signals"],
                    "slow": stats["slow"],
                    "fast": stats["fast"],
                    "avg_delay": stats["total_delay"] / stats["signals"] if stats["signals"] > 0 else 0.0,
                }
        # Убираем логирование – только для отладки можно оставить DEBUG
        # logger.debug(f"get_advisor_stats: {active_bks}")
        return active_bks

    def record_signal(self, fast_bk: str, slow_bk: str, delay: float):
        self._bk_stats[fast_bk]["fast"] += 1
        self._bk_stats[slow_bk]["slow"] += 1
        self._bk_stats[slow_bk]["signals"] += 1
        self._bk_stats[slow_bk]["total_delay"] += delay