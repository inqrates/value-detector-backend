# core/normalizer.py
"""
Тонкая обёртка над core/names.py.

Раньше здесь была отдельная реализация нормализации. Теперь она
одна — в core/names.py, и используется и сервером, и клиентом.
Этот модуль сохранён ради совместимости: парсеры и detector
импортируют `normalizer` и вызывают `normalizer.normalize_name(...)`.
"""
import re

from core import names


class TableTennisNormalizer:
    """
    Совместимый интерфейс. Вся логика — в core.names.
    Кэширование тоже там (общее на процесс).
    """

    MAX_CACHE = names.MAX_CACHE

    # Оставлены как атрибуты: вдруг где-то читают напрямую
    known_aliases = names.ALIASES
    _TRANSLIT = names.TRANSLIT

    def normalize_name(self, name: str) -> str:
        return names.normalize_name(name)

    def normalize_tournament(self, tournament: str) -> str:
        """Нормализация турнира (для отображения, не для ключа)."""
        if not tournament:
            return ""
        t = tournament.lower()
        t = re.sub(r'настольный теннис|международные|лига|зал|кубок', '', t)
        t = re.sub(r'[^\w]', '', t)
        return t

    def similarity(self, name1: str, name2: str) -> float:
        return names.similarity(name1, name2)

    def match_players(self, p1_a, p2_a, p1_b, p2_b, threshold=0.85):
        return names.match_players(p1_a, p2_a, p1_b, p2_b, threshold=threshold)


normalizer = TableTennisNormalizer()
