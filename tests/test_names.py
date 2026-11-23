# tests/test_names.py
"""
Тесты единой нормализации имён (core/names.py).

Файл СИНХРОНИЗИРУЕТСЯ между проектами:
    PGFrontPatch/tests/test_names.py  ←→  ten_ten/tests/test_names.py

Запуск:
    python -m unittest discover -s tests -v

Что защищаем: если нормализация «поедет», матчи между БК перестанут
находиться — и это будет выглядеть как «БК не отвечает». Тесты
фиксируют ключевые свойства, на которых держится сопоставление.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import names


class TestNormalizeName(unittest.TestCase):
    def setUp(self):
        names.clear_caches()

    def test_cyrillic_lastname(self):
        self.assertEqual(names.normalize_name("Иван Петров"), "петров")

    def test_reversed_order(self):
        self.assertEqual(names.normalize_name("Петров Иван"), "петров")

    def test_initial_removed(self):
        self.assertEqual(names.normalize_name("Петров И."), "петров")

    def test_parentheses_removed(self):
        self.assertEqual(names.normalize_name("Петров (мл.)"), "петров")

    def test_digits_removed(self):
        self.assertEqual(names.normalize_name("Петров (1964)"), "петров")

    def test_latin_transliterated(self):
        self.assertEqual(names.normalize_name("Ivan Petrov"), "петров")

    def test_latin_matches_cyrillic(self):
        self.assertEqual(
            names.normalize_name("Ivan Petrov"),
            names.normalize_name("Петров Иван"),
        )

    def test_empty(self):
        self.assertEqual(names.normalize_name(""), "")
        self.assertEqual(names.normalize_name("   "), "")

    def test_alias_applied(self):
        self.assertEqual(names.normalize_name("Lukas Adam"), "адам")

    def test_alias_gecko(self):
        """
        Алиас работает для кириллицы. Латиница «Hecko» не даёт
        «гечко»: в таблице транслита нет одиночной 'c'.
        """
        self.assertEqual(names.normalize_name("Хецко"), "гечко")

    def test_latin_c_not_transliterated(self):
        """
        ИЗВЕСТНОЕ ОГРАНИЧЕНИЕ: одиночная латинская 'c' не переводится
        (в таблице есть только 'ch', 'kh', 'ts'). Поэтому «Hecko»
        остаётся с латинской 'c' и алиас не срабатывает.
        """
        self.assertEqual(names.normalize_name("Hecko"), "хеcко")

    def test_longest_word_wins(self):
        """
        Берётся САМОЕ ДЛИННОЕ слово, а не последнее. Если имя длиннее
        фамилии — вернётся имя. Это текущее поведение.
        """
        self.assertEqual(
            names.normalize_name("Пётр Кузнецов"), "кузнецов"
        )

    def test_long_name_beats_short_lastname(self):
        """ИЗВЕСТНОЕ ОГРАНИЧЕНИЕ: «Александр» (9) длиннее «Кузнецов» (8)."""
        self.assertEqual(
            names.normalize_name("Александр Кузнецов"), "александр"
        )

    def test_tie_prefers_last(self):
        """При равной длине слов берётся последнее (Имя Фамилия)."""
        self.assertEqual(names.normalize_name("Ан Ли"), "ли")

    def test_fast_mode_tie_prefers_first(self):
        """Совместимость со старым fast_client: первое при равенстве."""
        self.assertEqual(
            names.normalize_name("Ан Ли", transliterate_=False,
                                 aliases=False, prefer_last_on_tie=False,
                                 fallback_to_raw=True),
            "ан",
        )

    def test_fast_mode_no_transliteration(self):
        self.assertEqual(
            names.normalize_name("Ivan Petrov", transliterate_=False,
                                 aliases=False, prefer_last_on_tie=False,
                                 fallback_to_raw=True),
            "petrov",
        )

    def test_fast_mode_fallback_to_raw(self):
        """Односимвольные слова — возвращается строка целиком."""
        self.assertEqual(
            names.normalize_name("И П", transliterate_=False,
                                 aliases=False, prefer_last_on_tie=False,
                                 fallback_to_raw=True),
            "и п",
        )


class TestNormalizeBk(unittest.TestCase):
    def test_lowercase(self):
        self.assertEqual(names.normalize_bk("Fonbet"), "fonbet")

    def test_spaces_removed(self):
        self.assertEqual(names.normalize_bk("Liga Stavok"), "ligastavok")

    def test_hyphen_removed(self):
        self.assertEqual(names.normalize_bk("liga-stavok"), "ligastavok")

    def test_underscore_removed(self):
        self.assertEqual(names.normalize_bk("liga_stavok"), "ligastavok")

    def test_empty(self):
        self.assertEqual(names.normalize_bk(""), "")


class TestSimilarity(unittest.TestCase):
    def setUp(self):
        names.clear_caches()

    def test_identical(self):
        self.assertEqual(names.similarity("Петров", "Петров"), 1.0)

    def test_substring(self):
        self.assertEqual(names.similarity("Петров", "Петров И."), 1.0)

    def test_different(self):
        self.assertLess(names.similarity("Петров", "Кузнецов"), 0.75)

    def test_empty(self):
        self.assertEqual(names.similarity("", "Петров"), 0.0)


class TestMatchPlayers(unittest.TestCase):
    def setUp(self):
        names.clear_caches()

    def test_direct_match(self):
        ok, score = names.match_players(
            "Иван Петров", "Алексей Сидоров",
            "Петров И.", "Сидоров А.",
        )
        self.assertTrue(ok)
        self.assertGreaterEqual(score, 0.85)

    def test_reverse_match(self):
        ok, _ = names.match_players(
            "Иван Петров", "Алексей Сидоров",
            "Сидоров А.", "Петров И.",
        )
        self.assertTrue(ok)

    def test_no_match(self):
        ok, _ = names.match_players(
            "Петров", "Сидоров", "Кузнецов", "Орлов"
        )
        self.assertFalse(ok)


class TestPlayersMatch(unittest.TestCase):
    """players_match — быстрая проверка (fast-режим, без транслита)."""

    def setUp(self):
        names.clear_caches()

    def test_direct(self):
        self.assertTrue(names.players_match(
            "Ivan Petrov", "Petr Sidorov",
            "Petrov Ivan", "Sidorov Petr",
        ))

    def test_reverse(self):
        self.assertTrue(names.players_match(
            "Ivan Petrov", "Petr Sidorov",
            "Sidorov Petr", "Petrov Ivan",
        ))

    def test_prefix_match(self):
        """Общий префикс из 4 символов считается совпадением."""
        self.assertTrue(names.players_match(
            "Petrov", "Sidorov", "Petrovsky", "Sidorovtsev"
        ))

    def test_no_match(self):
        self.assertFalse(names.players_match(
            "Petrov", "Sidorov", "Kuznetsov", "Orlov"
        ))

    def test_empty_safe(self):
        self.assertFalse(names.players_match("", "", "", ""))


if __name__ == "__main__":
    unittest.main(verbosity=2)
