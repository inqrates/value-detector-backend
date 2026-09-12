# config.py
"""
Конфигурация проекта для настольного тенниса.
"""

# ============================================================
# URL лайв-разделов по видам спорта и БК
# ============================================================
SPORT_URLS = {
    "table_tennis": {
        "fonbet":     "https://fon.bet/live/table-tennis",
        "winline":    "https://winline.ru/live/sport/nastolijnyj_tennis",
        "ligastavok": "https://www.ligastavok.ru/live/table-tennis",
        "leon":       "https://leon.ru/bets/table-tennis",
        "olimp":      "https://www.olimp.bet/live/nastolnyy-tennis-40",
        "betcity":    "https://betcity.ru/ru/live/table-tennis",
        "marathon":   "https://new.marathonbet.ru/su/live/table-tennis",
        "zenit":      "https://zenit.win/live/134",
        "sportbet":   "https://sportbet.ru/live/table-tennis?isTime=1",
    },
    "volleyball": {
        "fonbet":     "https://fon.bet/live/volleyball",
        "winline":    "https://winline.ru/live/sport/volleyball",
        "ligastavok": "https://www.ligastavok.ru/live/volleyball",
        "leon":       "https://leon.ru/bets/volleyball",
        "olimp":      "https://www.olimp.bet/live/voleybol-10",
        "betcity":    "https://betcity.ru/ru/live/volleyball",
        "marathon":   "https://new.marathonbet.ru/su/live/volleyball",
        "zenit":      "https://zenit.win/live/41",
        "sportbet":   "https://sportbet.ru/live/volleyball?isTime=1",
    },
    "basketball": {
        "fonbet":     "https://fon.bet/live/basketball",
        "winline":    "https://winline.ru/live/sport/basketball",
        "ligastavok": "https://www.ligastavok.ru/live/basketball",
        "leon":       "https://leon.ru/bets/basketball",
        "olimp":      "https://www.olimp.bet/live/basketbol-5",
        "betcity":    "https://betcity.ru/ru/live/basketball",
        "marathon":   "https://new.marathonbet.ru/su/live/basketball",
        "zenit":      "https://zenit.win/live/28",
        "sportbet":   "https://sportbet.ru/live/basketball?isTime=1",
    },
}

# ============================================================
# Общий лайв (все виды спорта сразу) — для мультиспорта
# ============================================================
SPORT_URLS["_all"] = {
    "fonbet":     "https://fon.bet/live",
    "winline":    "https://winline.ru/live",
    "ligastavok": "https://www.ligastavok.ru/live",
    "leon":       "https://leon.ru/live",
    "olimp":      "https://www.olimp.bet/live",
    "betcity":    "https://betcity.ru/ru/live",
    "marathon":   "https://new.marathonbet.ru/su/live",
    "zenit":      "https://zenit.win/live",
    "sportbet":   "https://sportbet.ru/live?h=all&isTime=1",
}


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


# Убираем старую функцию get_phase_label, если она была — оставь только format_phase


def get_phase_label(sport: str) -> str:
    """Deprecated. Используй format_phase()."""
    return PHASE_LABELS.get(sport, ("фаза", "я"))[0]

# Какие виды спорта включены по умолчанию
DEFAULT_ENABLED_SPORTS = ["table_tennis"]

# Обратная совместимость: TABLE_TENNIS_URLS остаётся как ссылка на НТ
TABLE_TENNIS_URLS = SPORT_URLS["table_tennis"]

PARSE_INTERVAL = 0.4
PAGE_LOAD_TIMEOUT = 60000      # мс
PAGE_STABILIZE_TIME = 3000     # мс
SIGNAL_COOLDOWN = 10.0
DELAY_THRESHOLD = 1.0
ODDS_DIFF_THRESHOLD = 0.05

HEADLESS = False
VIEWPORT_WIDTH = 1920
VIEWPORT_HEIGHT = 6000
ZOOM = 0.5

# Логирование – меняем на INFO, чтобы не было DEBUG-шума
LOG_LEVEL = "INFO"          # <-- изменено с "DEBUG"
LOG_FILE = "logs/backend.log"

# ---- Антисон для вкладок ----
PAGE_RELOAD_ENABLED = True
PAGE_RELOAD_STAGGER = 60

PAGE_KEEP_FRONT = ['betcity']
PAGE_KEEP_FRONT_INTERVAL = 45

# Индивидуальные пороги для конкретных БК
PAGE_STUCK_TIMEOUT_BY_BK = {
    'betcity': 25,
}
PAGE_STUCK_TIMEOUT_DEFAULT = 45

# Периодический reload — для БК, где список матчей подгружается
# только при загрузке страницы (новые матчи не видны без reload)
PAGE_PERIODIC_RELOAD_BY_BK = {
    'sportbet': 300,   # перезагрузка каждые 5 минут
}