# core/normalizer.py
import re
from typing import Dict, Tuple
from difflib import SequenceMatcher

class TableTennisNormalizer:
    def __init__(self):
        self._cache: Dict[str, str] = {}
        self._similarity_cache: Dict[Tuple[str, str], float] = {}
        self.known_aliases = {
            "адам": "лукас",
            "лукас": "адам",
            "хецко": "гечко",
            "гечко": "хецко",
        }

    def normalize_name(self, name: str) -> str:
        """
        Извлекает чистую фамилию из имени игрока.
        Убирает инициалы, скобки, цифры, дефисы, точки.
        """
        if not name:
            return ""

        cache_key = name.strip()
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 1. Убираем всё в скобках: (мл.), (огран. возм-ти), (1964)
        text = re.sub(r'\(.*?\)', '', name)
        # 2. Убираем цифры: 1964, 1985
        text = re.sub(r'\d+', '', text)
        # 3. Убираем всю пунктуацию: точки, дефисы, запятые
        text = re.sub(r'[^\w\s]', ' ', text)
        # 4. Приводим к нижнему регистру
        text = text.lower()
        # 5. Разбиваем на слова и убираем одиночные буквы (инициалы)
        words = [w for w in text.split() if len(w) > 1]

        if not words:
            # Если остались только одиночные буквы, берём как есть
            words = text.split()

        # 6. Фамилия — самое длинное слово
        lastname = max(words, key=len) if words else ""

        # 7. Проверка по словарю алиасов
        lastname = self.known_aliases.get(lastname, lastname)

        self._cache[cache_key] = lastname
        return lastname

    def normalize_tournament(self, tournament: str) -> str:
        """Нормализация турнира (для отображения, не для ключа)."""
        if not tournament:
            return ""
        t = tournament.lower()
        t = re.sub(r'настольный теннис|международные|лига|зал|кубок', '', t)
        t = re.sub(r'[^\w]', '', t)
        return t

    def similarity(self, name1: str, name2: str) -> float:
        if not name1 or not name2:
            return 0.0
        cache_key = tuple(sorted([name1, name2]))
        if cache_key in self._similarity_cache:
            return self._similarity_cache[cache_key]

        ln1 = self.normalize_name(name1)
        ln2 = self.normalize_name(name2)
        if not ln1 or not ln2:
            score = 0.0
        elif ln1 == ln2:
            score = 1.0
        elif ln1 in ln2 or ln2 in ln1:
            score = 0.95
        else:
            score = SequenceMatcher(None, ln1, ln2).ratio()
            if len(ln1) <= 6 and len(ln2) <= 6 and score >= 0.75:
                score = 0.85

        self._similarity_cache[cache_key] = score
        return score

    def match_players(self, p1_a, p2_a, p1_b, p2_b, threshold=0.85):
        s1 = self.similarity(p1_a, p1_b)
        s2 = self.similarity(p2_a, p2_b)
        direct = (s1 + s2) / 2
        cs1 = self.similarity(p1_a, p2_b)
        cs2 = self.similarity(p2_a, p1_b)
        cross = (cs1 + cs2) / 2
        best = max(direct, cross)
        return best >= threshold, best


normalizer = TableTennisNormalizer()