# tests/test_detector.py
"""
Тесты детектора задержек — на чистой логике, без сети и браузера.

Запуск:
    python -m unittest discover -s tests -v
    python tests/test_detector.py

Зачем: самая хрупкая часть системы — решение «это задержка или
сломанный парсер». Раньше проверить её можно было только на живом
матче, то есть никак. Теперь любая правка ловится здесь.

Тесты фиксируют ТЕКУЩЕЕ поведение. Если тест падает после правки —
значит поведение изменилось, и это надо осознать, а не «поправить тест».
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.detector import Detector, ZERO_SUB_FAST_THRESHOLD
from core.models import Match


def make_match(bk_id="bk1", s1=0, s2=0, sub1=0, sub2=0,
               odds1=2.0, odds2=2.0, p1="Петров", p2="Сидоров"):
    """Матч с заданным счётом. Остальные поля — заглушки."""
    return Match(
        bk_id=bk_id,
        match_id=f"{bk_id}-1",
        player1=p1,
        player2=p2,
        score1=s1,
        score2=s2,
        sub_score1=sub1,
        sub_score2=sub2,
        tournament="test",
        odds1=odds1,
        odds2=odds2,
    )


class TestSetNumber(unittest.TestCase):
    def test_first_set(self):
        self.assertEqual(Detector._set_number(make_match(s1=0, s2=0)), 1)

    def test_third_set(self):
        self.assertEqual(Detector._set_number(make_match(s1=1, s2=1)), 3)


class TestScoreNewer(unittest.TestCase):
    """_is_score_newer: продвинулся ли счёт."""

    def setUp(self):
        self.d = Detector()

    def test_set_win_is_newer(self):
        self.assertTrue(self.d._is_score_newer((1, 0, 0, 0), (0, 0, 0, 0)))

    def test_older_is_not_newer(self):
        self.assertFalse(self.d._is_score_newer((0, 0, 0, 0), (1, 0, 0, 0)))

    def test_points_grow(self):
        self.assertTrue(self.d._is_score_newer((0, 0, 5, 0), (0, 0, 0, 0)))

    def test_equal_is_not_newer(self):
        self.assertFalse(self.d._is_score_newer((1, 1, 3, 2), (1, 1, 3, 2)))

    def test_points_do_not_shrink(self):
        self.assertFalse(self.d._is_score_newer((0, 0, 3, 0), (0, 0, 5, 0)))


class TestParseBug(unittest.TestCase):
    """
    _has_parse_bug: fast видит очки, slow показывает 0:0 —
    это не задержка, а потеря данных у slow.
    """

    def setUp(self):
        self.d = Detector()

    def test_fast_has_points_slow_zero_is_bug(self):
        states = {"slow": make_match("slow", sub1=0, sub2=0)}
        self.assertTrue(
            self.d._has_parse_bug((0, 0, 6, 10), states, ["slow"])
        )

    def test_both_have_points_is_not_bug(self):
        states = {"slow": make_match("slow", sub1=3, sub2=2)}
        self.assertFalse(
            self.d._has_parse_bug((0, 0, 6, 10), states, ["slow"])
        )

    def test_match_start_is_not_bug(self):
        """0:0 по партиям и 0:0 по очкам — матч только начался."""
        states = {"slow": make_match("slow", sub1=0, sub2=0)}
        self.assertFalse(
            self.d._has_parse_bug((0, 0, 0, 0), states, ["slow"])
        )

    def test_majority_zero_returns_early(self):
        """
        Если majority сам 0:0:0:0 — проверка выходит раньше.
        Такая ситуация в норме невозможна (majority — самый
        продвинутый счёт), поэтому код считает её «не багом».
        """
        states = {"slow": make_match("slow", sub1=3, sub2=2)}
        self.assertFalse(
            self.d._has_parse_bug((0, 0, 0, 0), states, ["slow"])
        )


class TestSignificantGap(unittest.TestCase):
    """
    _is_significant_gap: гэп достаточно большой, чтобы ставить.
    """

    def setUp(self):
        self.d = Detector()

    def test_gap_of_two_points_is_significant(self):
        states = {"slow": make_match("slow", sub1=4, sub2=3)}
        self.assertTrue(
            self.d._is_significant_gap((0, 0, 6, 3), states, ["slow"])
        )

    def test_gap_of_one_point_is_not_significant(self):
        states = {"slow": make_match("slow", sub1=5, sub2=3)}
        self.assertFalse(
            self.d._is_significant_gap((0, 0, 6, 3), states, ["slow"])
        )

    def test_slow_zero_sub_with_many_fast_points_is_skipped(self):
        """
        Страховка ZERO_SUB_FAST_THRESHOLD: slow 0:0, fast уже >= 4 —
        это потеря данных, а не задержка. Ставить нельзя.
        """
        self.assertGreaterEqual(6, ZERO_SUB_FAST_THRESHOLD)
        states = {"slow": make_match("slow", sub1=0, sub2=0)}
        self.assertFalse(
            self.d._is_significant_gap((0, 0, 6, 3), states, ["slow"])
        )

    def test_slow_zero_sub_with_few_fast_points_counts(self):
        """
        Но если fast набрал мало (< порога) — это может быть реальная
        задержка в начале партии.
        """
        self.assertLess(2, ZERO_SUB_FAST_THRESHOLD)
        states = {"slow": make_match("slow", sub1=0, sub2=0)}
        self.assertTrue(
            self.d._is_significant_gap((0, 0, 2, 1), states, ["slow"])
        )

    def test_slow_in_previous_set_with_finished_sub(self):
        """slow ещё в предыдущей партии, но та партия уже доиграна."""
        states = {"slow": make_match("slow", s1=0, s2=0, sub1=11, sub2=5)}
        self.assertTrue(
            self.d._is_significant_gap((1, 0, 2, 1), states, ["slow"])
        )

    def test_slow_in_previous_set_unfinished_is_skipped(self):
        """slow в предыдущей партии и она не доиграна — не сигнал."""
        states = {"slow": make_match("slow", s1=0, s2=0, sub1=5, sub2=3)}
        self.assertFalse(
            self.d._is_significant_gap((1, 0, 2, 1), states, ["slow"])
        )

    def test_slow_ahead_is_skipped(self):
        """slow в более поздней партии — не отстаёт, ставить не на чем."""
        states = {"slow": make_match("slow", s1=1, s2=0, sub1=3, sub2=2)}
        self.assertFalse(
            self.d._is_significant_gap((0, 0, 6, 3), states, ["slow"])
        )

    def test_slow_ahead_in_same_set_is_skipped(self):
        """slow в той же партии, но с большим счётом — не отстаёт."""
        states = {"slow": make_match("slow", sub1=7, sub2=3)}
        self.assertFalse(
            self.d._is_significant_gap((0, 0, 6, 3), states, ["slow"])
        )


class TestReverseOrder(unittest.TestCase):
    """_is_reverse_order: у БК разный порядок хозяин/гость."""

    def test_direct_order_is_not_reverse(self):
        f = make_match(p1="Иван Петров", p2="Алексей Сидоров")
        s = make_match(p1="Петров И.", p2="Сидоров А.")
        self.assertFalse(Detector._is_reverse_order(f, s))

    def test_swapped_order_is_reverse(self):
        f = make_match(p1="Иван Петров", p2="Алексей Сидоров")
        s = make_match(p1="Сидоров А.", p2="Петров И.")
        self.assertTrue(Detector._is_reverse_order(f, s))

    def test_none_safe(self):
        self.assertFalse(Detector._is_reverse_order(None, make_match()))
        self.assertFalse(Detector._is_reverse_order(make_match(), None))


class TestMatchKey(unittest.TestCase):
    """_get_match_key: ключ матча не зависит от порядка игроков."""

    def setUp(self):
        self.d = Detector()

    def test_order_independent(self):
        k1 = self.d._get_match_key("Иван Петров", "Алексей Сидоров")
        k2 = self.d._get_match_key("Алексей Сидоров", "Иван Петров")
        self.assertEqual(k1, k2)

    def test_different_sports_differ(self):
        k1 = self.d._get_match_key("Петров", "Сидоров", sport="table_tennis")
        k2 = self.d._get_match_key("Петров", "Сидоров", sport="volleyball")
        self.assertNotEqual(k1, k2)

    def test_different_players_differ(self):
        k1 = self.d._get_match_key("Петров", "Сидоров")
        k2 = self.d._get_match_key("Петров", "Кузнецов")
        self.assertNotEqual(k1, k2)

    def test_latin_and_cyrillic_match(self):
        """
        Транслитерация: латиница и кириллица дают один ключ.
        ВНИМАНИЕ: таблица транслита не покрывает 'x' и часть
        сочетаний — тест берёт имена, где транслит однозначен.
        """
        k1 = self.d._get_match_key("Ivan Petrov", "Petr Sidorov")
        k2 = self.d._get_match_key("Петров Иван", "Сидоров Петр")
        self.assertEqual(k1, k2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
