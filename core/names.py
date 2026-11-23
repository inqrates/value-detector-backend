# core/names.py
"""
Единая нормализация имён игроков и названий БК.

ЗАЧЕМ ЭТОТ ФАЙЛ
───────────────
Раньше нормализация была в трёх местах:
  * core/normalizer.py            (сервер: транслит + алиасы + similarity)
  * core/fast_client.py           (клиент: БЕЗ транслита)
  * ui/after_goal_engine.py       (клиент: копия серверной)

Если реализации расходятся, матч между БК просто не находится —
и это выглядит как «БК не отвечает», а не как ошибка нормализации.
Тихий баг, который ищут часами.

Теперь реализация одна. Файл СИНХРОНИЗИРУЕТСЯ между проектами:
  PGFrontPatch/core/names.py  ←→  ten_ten/core/names.py
Они должны быть побайтово одинаковыми. При правке — правь оба.

ПОВЕДЕНИЕ СОХРАНЕНО
───────────────────
У старых реализаций были реальные различия, которые нельзя терять:
  * fast_client не транслитерировал и не применял алиасы;
  * fast_client при равной длине слов брал ПЕРВОЕ (max), а
    нормализатор — ПОСЛЕДНЕЕ.
Поэтому у normalize_name есть флаги. Дефолт — «полный» режим
(как normalizer.py). Для совместимости со старым fast_client
вызывай normalize_name(..., transliterate_=False, aliases=False,
prefer_last_on_tie=False).
"""
import re
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

MAX_CACHE = 10_000

# ── Транслитерация латиница → кириллица ──
TRANSLIT: Dict[str, str] = {
    'a': 'а', 'b': 'б', 'v': 'в', 'g': 'г', 'd': 'д',
    'e': 'е', 'yo': 'ё', 'zh': 'ж', 'z': 'з', 'i': 'и',
    'j': 'й', 'k': 'к', 'l': 'л', 'm': 'м', 'n': 'н',
    'o': 'о', 'p': 'п', 'r': 'р', 's': 'с', 't': 'т',
    'u': 'у', 'f': 'ф', 'kh': 'х', 'ts': 'ц', 'ch': 'ч',
    'sh': 'ш', 'sch': 'щ', 'y': 'ы', 'yu': 'ю', 'ya': 'я',
    'h': 'х', 'w': 'в',
}

# ── Известные расхождения в написании фамилий между БК ──
ALIASES: Dict[str, str] = {
    "адам": "адам",
    "лукас": "адам",
    "хецко": "гечко",
    "гечко": "гечко",
}

_cache: Dict[tuple, str] = {}
_similarity_cache: Dict[Tuple[str, str], float] = {}


def _trim_cache(cache: dict) -> None:
    """Срезает треть кэша при переполнении (грубая, но дешёвая чистка)."""
    if len(cache) > MAX_CACHE:
        cut = int(MAX_CACHE * 0.3)
        for k in list(cache.keys())[:cut]:
            cache.pop(k, None)


def transliterate(text: str) -> str:
    """Конвертирует латиницу в кириллицу (для сопоставления имён)."""
    if not text or not re.search(r'[a-zA-Z]', text):
        return text
    result = []
    i = 0
    while i < len(text):
        matched = False
        for length in (3, 2, 1):
            chunk = text[i:i + length]
            if chunk in TRANSLIT:
                result.append(TRANSLIT[chunk])
                i += length
                matched = True
                break
        if not matched:
            result.append(text[i])
            i += 1
    return ''.join(result)


def _words(text: str) -> List[str]:
    return text.split()


def normalize_name(
    name: str,
    transliterate_: bool = True,
    aliases: bool = True,
    prefer_last_on_tie: bool = True,
    fallback_to_raw: bool = False,
) -> str:
    """
    Извлекает чистую фамилию из имени игрока.

    Убирает инициалы, скобки, цифры, пунктуацию. Фамилия — самое
    длинное слово; при равной длине берётся последнее
    (prefer_last_on_tie=True, формат «Имя Фамилия»).

    transliterate_ / aliases / prefer_last_on_tie / fallback_to_raw —
    для совместимости со старыми реализациями, см. шапку файла.
    fallback_to_raw=True: если не осталось слов длиннее 1 символа,
    вернуть очищенную строку целиком (поведение старого fast_client).
    """
    if not name:
        return ""

    cache_key = (name.strip(), transliterate_, aliases,
                 prefer_last_on_tie, fallback_to_raw)
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    text = re.sub(r'\(.*?\)', '', name)     # (мл.), (1964), (огран. возм-ти)
    text = re.sub(r'\d+', '', text)         # 1964, 1985
    text = re.sub(r'[^\w\s]', ' ', text)    # точки, дефисы, запятые
    text = text.lower()
    if transliterate_:
        text = transliterate(text)

    words = [w for w in _words(text) if len(w) > 1]
    if not words:
        if fallback_to_raw:
            _cache[cache_key] = text.strip()
            return text.strip()
        words = _words(text)
    if aliases:
        words = [ALIASES.get(w, w) for w in words]
    if not words:
        _cache[cache_key] = ""
        return ""

    if prefer_last_on_tie:
        lastname = words[-1]
        for w in words:
            if len(w) > len(lastname):
                lastname = w
    else:
        lastname = max(words, key=len)

    _cache[cache_key] = lastname
    _trim_cache(_cache)
    return lastname


def normalize_bk(name: str) -> str:
    """Приводит название БК к внутреннему виду: fonbet, ligastavok, …"""
    return (name or "").lower().replace(" ", "").replace("-", "").replace("_", "")


def similarity(name1: str, name2: str, threshold_len: int = 6,
               threshold_ratio: float = 0.75, boost: float = 0.85) -> float:
    """Похожесть двух имён: 0.0…1.0."""
    if not name1 or not name2:
        return 0.0
    cache_key = tuple(sorted([name1, name2]))
    cached = _similarity_cache.get(cache_key)
    if cached is not None:
        return cached

    ln1 = normalize_name(name1)
    ln2 = normalize_name(name2)
    if not ln1 or not ln2:
        score = 0.0
    elif ln1 == ln2:
        score = 1.0
    elif ln1 in ln2 or ln2 in ln1:
        score = 0.95
    else:
        score = SequenceMatcher(None, ln1, ln2).ratio()
        if len(ln1) <= threshold_len and len(ln2) <= threshold_len \
                and score >= threshold_ratio:
            score = boost

    _similarity_cache[cache_key] = score
    _trim_cache(_similarity_cache)
    return score


def match_players(p1_a: str, p2_a: str, p1_b: str, p2_b: str,
                  threshold: float = 0.85) -> Tuple[bool, float]:
    """
    Сравнивает пару игроков с учётом возможного обратного порядка.
    Возвращает (совпало, лучший счёт).
    """
    s1 = similarity(p1_a, p1_b)
    s2 = similarity(p2_a, p2_b)
    direct = (s1 + s2) / 2
    cs1 = similarity(p1_a, p2_b)
    cs2 = similarity(p2_a, p1_b)
    cross = (cs1 + cs2) / 2
    best = max(direct, cross)
    return best >= threshold, best


def close_token(a: str, b: str, prefix_len: int = 4) -> bool:
    """Совпадают ли фамилии (равны или общий префикс)."""
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) >= prefix_len and len(b) >= prefix_len and a[:prefix_len] == b[:prefix_len]:
        return True
    return False


def players_match(p1a: str, p2a: str, p1b: str, p2b: str) -> bool:
    """
    Быстрая проверка совпадения пары игроков (без транслита и алиасов).

    Сохранено поведение старого core/fast_client._players_match:
    поиск по фамилиям с общим префиксом, любой порядок сторон.
    """
    na1 = normalize_name(p1a, transliterate_=False, aliases=False,
                         prefer_last_on_tie=False, fallback_to_raw=True)
    na2 = normalize_name(p2a, transliterate_=False, aliases=False,
                         prefer_last_on_tie=False, fallback_to_raw=True)
    nb1 = normalize_name(p1b, transliterate_=False, aliases=False,
                         prefer_last_on_tie=False, fallback_to_raw=True)
    nb2 = normalize_name(p2b, transliterate_=False, aliases=False,
                         prefer_last_on_tie=False, fallback_to_raw=True)
    if not (na1 and na2 and nb1 and nb2):
        return False

    direct = close_token(na1, nb1) and close_token(na2, nb2)
    cross = close_token(na1, nb2) and close_token(na2, nb1)
    return direct or cross


def clear_caches() -> None:
    """Сброс кэшей (для тестов)."""
    _cache.clear()
    _similarity_cache.clear()
