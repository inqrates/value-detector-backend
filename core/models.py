# core/models.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class Match:
    bk_id: str
    match_id: str
    player1: str
    player2: str
    score1: int
    score2: int
    sub_score1: int
    sub_score2: int
    tournament: str
    odds1: float
    odds2: float
    oddsX: float = 0.0
    total_line: float = 0.0
    total_over: float = 0.0
    total_under: float = 0.0
    handicap1: float = 0.0
    handicap2: float = 0.0
    handicap_odds1: float = 0.0
    handicap_odds2: float = 0.0
    timestamp: float = 0.0
    raw_time: str = ""
    sport: str = "table_tennis"        
    set_markets: dict = None
    match_url: str = ""
    extra: dict = None