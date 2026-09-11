# core/strategy_engine.py
from typing import List, Dict, Optional, Any
from core.models import Match
import logging

logger = logging.getLogger(__name__)

class Recommendation:
    def __init__(self, match: Match, bk_id: str, market_type: str, line: float,
                 side: str, odd: float, event_id: str, factor: int, amount: float):
        self.match = match
        self.bk_id = bk_id
        self.market_type = market_type   # 'winner', 'total', 'handicap'
        self.line = line
        self.side = side                 # '1','2' for winner; 'over','under' for total; '1','2' for handicap
        self.odd = odd
        self.event_id = event_id         # ID события в БК (для API)
        self.factor = factor             # код исхода (если нужно)
        self.amount = amount

    def to_dict(self):
        return {
            "match_id": self.match.match_id,
            "player1": self.match.player1,
            "player2": self.match.player2,
            "bk": self.bk_id,
            "market_type": self.market_type,
            "line": self.line,
            "side": self.side,
            "odd": self.odd,
            "amount": self.amount,
            "event_id": self.event_id,
            "factor": self.factor,
        }


class StrategyEngine:
    @staticmethod
    def get_recommendations(match: Match, strategies: List[dict]) -> List[Recommendation]:
        if not strategies:
            return []

        current_set = match.score1 + match.score2 + 1
        if not match.set_markets or current_set not in match.set_markets:
            logger.debug(f"[StrategyEngine] Нет set_markets для сета {current_set} у матча {match.match_id}")
            return []

        set_markets = match.set_markets[current_set]
        recs = []

        for strat in strategies:
            if not strat.get('enabled', False):
                continue
            if strat.get('type', '').lower() != 'after-goal':
                continue
            if strat.get('bk_slow', '').lower() != match.bk_id:
                continue

            market_type = strat.get('market_type', 'auto').lower()
            min_odds = strat.get('min_odds', 1.3)
            max_odds = strat.get('max_odds', 5.0)
            bet_size = strat.get('bet_size', 100)

            candidates = []

            if market_type in ('auto', 'winner'):
                if 'winner' in set_markets:
                    for side, odd in set_markets['winner'].items():
                        if min_odds <= odd <= max_odds:
                            candidates.append(('winner', 0.0, side, odd))

            if market_type in ('auto', 'total'):
                if 'total' in set_markets:
                    total = set_markets['total']
                    line = total.get('line', 0.0)
                    for side in ['over', 'under']:
                        odd = total.get(side, 0.0)
                        if min_odds <= odd <= max_odds:
                            candidates.append(('total', line, side, odd))

            if market_type in ('auto', 'handicap'):
                if 'handicap' in set_markets:
                    h_data = set_markets['handicap']
                    for side in ['1', '2']:
                        if side in h_data:
                            line = h_data[side].get('line', 0.0)
                            odd = h_data[side].get('odd', 0.0)
                            if min_odds <= odd <= max_odds:
                                candidates.append(('handicap', line, side, odd))

            if candidates:
                candidates.sort(key=lambda x: x[3], reverse=True)
                best = candidates[0]
                event_id = match.match_id
                factor = 0
                rec = Recommendation(
                    match=match,
                    bk_id=match.bk_id,
                    market_type=best[0],
                    line=best[1],
                    side=best[2],
                    odd=best[3],
                    event_id=event_id,
                    factor=factor,
                    amount=bet_size
                )
                recs.append(rec)
                logger.info(f"[StrategyEngine] Рекомендация для {match.player1} vs {match.player2}: "
                            f"{best[0]} {best[2]} @ {best[3]:.2f} (линия {best[1]})")

        return recs