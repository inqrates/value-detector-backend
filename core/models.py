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
    total_line: float = 0.0          # линия тотала
    total_over: float = 0.0           # кэф на больше
    total_under: float = 0.0          # кэф на меньше
    handicap1: float = 0.0            # фора на первого игрока (число)
    handicap2: float = 0.0            # фора на второго игрока
    handicap_odds1: float = 0.0       # кэф на фору 1
    handicap_odds2: float = 0.0       # кэф на фору 2
    timestamp: float = 0.0
    raw_time: str = ""
    sport: str = "table_tennis"
    set_markets: dict = None 
    match_url: str = ""          # <-- ДОБАВИТЬ: прямая ссылка на матч
    extra: dict = None           # <-- ДОБАВИТЬ: сырые поля для фолбэка/отладки