# core/sport_map.py
"""
Центральная карта видов спорта по букмекерским конторам.

Каждая БК имеет свой формат идентификации видов спорта:
  - Числовой ID (Fonbet, Betcity, Olimp, Sportbet, Zenit, LigaStavok, Winline)
  - Строковый family (Leon)
  - Строковый slug (Marathon)
"""

# ============================================================
# Ключи видов спорта (универсальные)
# ============================================================
TABLE_TENNIS = "table_tennis"
VOLLEYBALL = "volleyball"
BASKETBALL = "basketball"
CYBER_BASKETBALL = "cyber_basketball"
FOOTBALL = "football"
HOCKEY = "hockey"
TENNIS = "tennis"
HANDBALL = "handball"
BEACH_VOLLEYBALL = "beach_volleyball"
FUTSAL = "futsal"
CRICKET = "cricket"
CYBERSPORT = "cybersport"


# ============================================================
# FONBET
# ============================================================
FONBET = {
    TABLE_TENNIS: {
        "ids": [3088],
        "aliases": ["table-tennis"],
        "name": "Настольный теннис",
        "url_slug": "table-tennis",
    },
    VOLLEYBALL: {
        "ids": [9],
        "aliases": ["volleyball"],
        "name": "Волейбол",
        "url_slug": "volleyball",
    },
    BASKETBALL: {
        "ids": [3],
        "aliases": ["basketball"],
        "name": "Баскетбол",
        "url_slug": "basketball",
        "exclude_category_ids": [119],
    },
    CYBER_BASKETBALL: {
        "ids": [3],
        "aliases": ["basketball"],
        "name": "Кибербаскетбол",
        "url_slug": "basketball",
        "category_ids": [119],
    },
    FOOTBALL: {
        "ids": [1],
        "aliases": ["football"],
        "name": "Футбол",
        "url_slug": "football",
        "exclude_category_ids": [118],
    },
    HOCKEY: {
        "ids": [2],
        "aliases": ["hockey"],
        "name": "Хоккей",
        "url_slug": "hockey",
        "exclude_category_ids": [165],
    },
    TENNIS: {
        "ids": [4],
        "aliases": ["tennis"],
        "name": "Теннис",
        "url_slug": "tennis",
    },
    CYBERSPORT: {
        "ids": [29086],
        "aliases": ["esports"],
        "name": "Киберспорт",
        "url_slug": "esports",
    },
}


# ============================================================
# BETCITY
# ============================================================
BETCITY = {
    TABLE_TENNIS: {"ids": [46], "name": "Настольный теннис", "url_slug": "table-tennis"},
    VOLLEYBALL:   {"ids": [12], "name": "Волейбол",         "url_slug": "volleyball"},
    BASKETBALL:   {"ids": [3],  "name": "Баскетбол",        "url_slug": "basketball"},
}


# ============================================================
# OLIMP
# ============================================================
OLIMP = {
    TABLE_TENNIS:     {"ids": ["40"],  "name": "Настольный теннис", "url_slug": "nastolnyy-tennis-40"},
    VOLLEYBALL:       {"ids": ["10"],  "name": "Волейбол",         "url_slug": "voleybol-10"},
    BASKETBALL:       {"ids": ["5"],   "name": "Баскетбол",        "url_slug": "basketbol-5"},
    CYBER_BASKETBALL: {"ids": ["140"], "name": "Кибербаскетбол",   "url_slug": "kiberbasketbol-140"},
    CYBERSPORT:       {"ids": ["112"], "name": "Киберспорт",       "url_slug": "kibersport-112"},
    FOOTBALL:         {"ids": ["1"],   "name": "Футбол",           "url_slug": "futbol-1"},
    HOCKEY:           {"ids": ["2"],   "name": "Хоккей",           "url_slug": "khokkey-2"},
    TENNIS:           {"ids": ["3"],   "name": "Теннис",           "url_slug": "tennis-3"},
}


# ============================================================
# SPORTBET
# ============================================================
SPORTBET = {
    TABLE_TENNIS: {"ids": [20], "name": "Настольный теннис", "url_slug": "table-tennis"},
    VOLLEYBALL:   {"ids": [23], "name": "Волейбол",         "url_slug": "volleyball"},
    BASKETBALL:   {"ids": [2],  "name": "Баскетбол",        "url_slug": "basketball"},
    FOOTBALL:     {"ids": [1],  "name": "Футбол",           "url_slug": "soccer"},
    HOCKEY:       {"ids": [4],  "name": "Хоккей",           "url_slug": "ice-hockey"},
    TENNIS:       {"ids": [5],  "name": "Теннис",           "url_slug": "tennis"},
}


# ============================================================
# ZENIT
# ============================================================
ZENIT = {
    TABLE_TENNIS:     {"ids": [134], "name": "Настольный теннис", "url_slug": "134"},
    VOLLEYBALL:       {"ids": [41],  "name": "Волейбол",         "url_slug": "41"},
    BASKETBALL:       {"ids": [28],  "name": "Баскетбол",        "url_slug": "28"},
    CYBER_BASKETBALL: {"ids": [564], "name": "Кибербаскетбол",   "url_slug": "564"},
    FOOTBALL:         {"ids": [26],  "name": "Футбол",           "url_slug": "26"},
    HOCKEY:           {"ids": [27],  "name": "Хоккей",           "url_slug": "27"},
    TENNIS:           {"ids": [33],  "name": "Теннис",           "url_slug": "33"},
    HANDBALL:         {"ids": [32],  "name": "Гандбол",          "url_slug": "32"},
}


# ============================================================
# LIGASTAVOK
# ============================================================
LIGASTAVOK = {
    TABLE_TENNIS: {"ids": [1246], "name": "Настольный теннис", "url_slug": "table-tennis"},
    VOLLEYBALL:   {"ids": [128],  "name": "Волейбол",         "url_slug": "volleyball"},
    BASKETBALL:   {"ids": [25],   "name": "Баскетбол",        "url_slug": "basketball"},
    CYBERSPORT:   {"ids": [10014],"name": "Киберспорт",       "url_slug": "cybersport"},
    FOOTBALL:     {"ids": [33],   "name": "Футбол",           "url_slug": "soccer"},
    HOCKEY:       {"ids": [31],   "name": "Хоккей",           "url_slug": "ice-hockey"},
    TENNIS:       {"ids": [34],   "name": "Теннис",           "url_slug": "tennis"},
    HANDBALL:     {"ids": [30],   "name": "Гандбол",          "url_slug": "handball"},
}


# ============================================================
# LEON
# ============================================================
LEON = {
    TABLE_TENNIS:     {"aliases": ["TableTennis"], "name": "Настольный теннис", "url_slug": "table-tennis"},
    VOLLEYBALL:       {"aliases": ["Volleyball"],  "name": "Волейбол",         "url_slug": "volleyball"},
    BASKETBALL:       {"aliases": ["Basketball"],  "name": "Баскетбол",        "url_slug": "basketball"},
    CYBER_BASKETBALL: {
        "aliases": ["Basketball"],
        "region_family": "ELECTRONIC_LEAGUES",
        "name": "Кибербаскетбол",
        "url_slug": "basketball",
    },
    FOOTBALL: {"aliases": ["Football"],  "name": "Футбол", "url_slug": "football"},
    HOCKEY:   {"aliases": ["IceHockey"], "name": "Хоккей", "url_slug": "ice-hockey"},
    TENNIS:   {"aliases": ["Tennis"],    "name": "Теннис", "url_slug": "tennis"},
}


# ============================================================
# WINLINE
# ============================================================
WINLINE = {
    TABLE_TENNIS:     {"ids": [20],  "name": "Настольный теннис", "url_slug": "nastolijnyj_tennis"},
    VOLLEYBALL:       {"ids": [23],  "name": "Волейбол",         "url_slug": "volleyball"},
    BASKETBALL:       {"ids": [2],   "name": "Баскетбол",        "url_slug": "basketball"},
    CYBER_BASKETBALL: {"ids": [193], "name": "Кибербаскетбол",   "url_slug": "cyberbasketball"},
}


# ============================================================
# MARATHON
# ============================================================
MARATHON = {
    TABLE_TENNIS: {
        "aliases": ["table-tennis"],
        "sport_code": "TableTennis",
        "name": "Настольный теннис",
        "url_slug": "table-tennis",
    },
    VOLLEYBALL: {
        "aliases": ["volleyball"],
        "sport_code": "Volleyball",
        "name": "Волейбол",
        "url_slug": "volleyball",
    },
    BASKETBALL: {
        "aliases": ["basketball"],
        "sport_code": "Basketball",
        "name": "Баскетбол",
        "url_slug": "basketball",
    },
    CYBER_BASKETBALL: {
        "aliases": ["cyber-basketball"],
        "sport_code": "e-Sports",
        "name": "Кибербаскетбол",
        "url_slug": "cyber-basketball",
    },
}


# ============================================================
# ОБЩАЯ КАРТА ПО БК
# ============================================================
SPORT_MAP = {
    "fonbet": FONBET,
    "betcity": BETCITY,
    "olimp": OLIMP,
    "sportbet": SPORTBET,
    "zenit": ZENIT,
    "ligastavok": LIGASTAVOK,
    "leon": LEON,
    "winline": WINLINE,
    "marathon": MARATHON,
}


# ============================================================
# Какие виды спорта включены по умолчанию
# ============================================================
DEFAULT_ENABLED_SPORTS = [TABLE_TENNIS]


# ============================================================
# Вспомогательные функции
# ============================================================
def get_sport_config(bk: str, sport: str) -> dict:
    """Вернуть конфиг вида спорта для БК или {} если нет."""
    return SPORT_MAP.get(bk, {}).get(sport, {})


def get_ids_for_sport(bk: str, sport: str) -> list:
    """Вернуть список числовых ID для вида спорта."""
    cfg = get_sport_config(bk, sport)
    return cfg.get("ids", [])


def get_aliases_for_sport(bk: str, sport: str) -> list:
    """Вернуть список строковых ID (alias / family / slug)."""
    cfg = get_sport_config(bk, sport)
    if "aliases" in cfg:
        return cfg["aliases"]
    if "url_slug" in cfg:
        return [cfg["url_slug"]]
    return []


def get_all_ids_for_bk(bk: str) -> dict:
    """Собрать маппинг {id: sport_key} для всех видов спорта БК."""
    result = {}
    for sport_key, cfg in SPORT_MAP.get(bk, {}).items():
        for id_val in cfg.get("ids", []):
            result[id_val] = sport_key
    return result


def get_url_slug(bk: str, sport: str) -> str:
    """Вернуть slug для сборки URL матча."""
    cfg = get_sport_config(bk, sport)
    return cfg.get("url_slug", "")


def list_sports_for_bk(bk: str) -> list:
    """Список всех видов спорта, поддержанных для БК."""
    return list(SPORT_MAP.get(bk, {}).keys())


# ============================================================
# КОДЫ ФАКТОРОВ ПО ВИДАМ СПОРТА
# ============================================================
FACTOR_CODES = {
    "fonbet": {
        "table_tennis": {
            "win1": 921, "win2": 923, "draw": None,
            "total_over": 930, "total_under": 931,
            "handicap1": 927, "handicap2": 928,
        },
        "volleyball": {
            "win1": 921, "win2": 923, "draw": None,
            "total_over": 930, "total_under": 931,
            "handicap1": 927, "handicap2": 928,
        },
        "basketball": {
            "win1": 921, "win2": 923, "draw": 922,
            "total_over": 930, "total_under": 931,
            "handicap1": 927, "handicap2": 928,
            "match_total_over": 1736, "match_total_under": 1737,
            "match_handicap1": 910, "match_handicap2": 912,
        },
        "cyber_basketball": {
            "win1": 921, "win2": 923, "draw": 922,
            "total_over": 930, "total_under": 931,
            "handicap1": 927, "handicap2": 928,
            "match_total_over": 1736, "match_total_under": 1737,
            "match_handicap1": 910, "match_handicap2": 912,
        },
    },
}


def get_factor_codes(bk: str, sport: str) -> dict:
    """Вернуть коды факторов для вида спорта."""
    return FACTOR_CODES.get(bk, {}).get(sport, {})


# ============================================================
# НАЗВАНИЯ ФАЗ ПО ВИДАМ СПОРТА
# Кортеж: (существительное, суффикс порядкового)
# ============================================================
PHASE_LABELS = {
    "table_tennis":     ("партия",   "я"),
    "volleyball":       ("сет",      "й"),
    "beach_volleyball": ("сет",      "й"),
    "basketball":       ("четверть", "я"),
    "cyber_basketball": ("четверть", "я"),
    "football":         ("тайм",     "й"),
    "futsal":           ("тайм",     "й"),
    "hockey":           ("период",   "й"),
    "tennis":           ("сет",      "й"),
    "handball":         ("тайм",     "й"),
    "cricket":          ("иннинг",   "й"),
    "cybersport":       ("карта",    "я"),
}


def format_phase(sport: str, n: int) -> str:
    """Возвращает '2-й сет', '3-я партия', '1-я четверть' и т.п."""
    label, sfx = PHASE_LABELS.get(sport, ("фаза", "я"))
    return f"{n}-{sfx} {label}"


def get_phase_label(sport: str) -> str:
    """Deprecated. Используй format_phase()."""
    return PHASE_LABELS.get(sport, ("фаза", "я"))[0]