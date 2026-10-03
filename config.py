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
VIEWPORT_HEIGHT = 4000
ZOOM = 0.5

# Логирование
LOG_LEVEL = "INFO"
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

# ============================================================
# Периодический reload — сбрасывает DOM/JS heap у Chromium
# ============================================================
# Дефолт 20 минут — для всех БК, у которых нет индивидуальной настройки.
# Перезагрузка освобождает память: DOM сбрасывается, JS heap чистится.
# ============================================================
# Периодический reload — сбрасывает DOM/JS heap у Chromium
# ============================================================
# ВАЖНО: PAGE_PERIODIC_RELOAD_* применяются ТОЛЬКО к парсерам,
# которые наследуют BaseParser и имеют self.page (Playwright Page).
# На момент рефакторинга это ТОЛЬКО marathon.
#
# Остальные парсеры (fonbet, winline, ligastavok, leon, olimp,
# betcity, zenit, sportbet) работают через curl_cffi / websockets —
# у них нет страницы, reload не применим. Их чистит HealthMonitor
# через FORCED_RESTART_INTERVAL (см. core/health_monitor.py).

PAGE_PERIODIC_RELOAD_DEFAULT = 1200      # 20 мин

# Индивидуальные интервалы для marathon (единственный BaseParser).
# Ключи для остальных БК оставлены для совместимости, но фактически
# игнорируются (парсер не имеет self.page).
PAGE_PERIODIC_RELOAD_BY_BK = {
    'sportbet': 300,     # не используется (нет self.page)
    'betcity':  900,     # не используется
    'winline':  1800,    # не используется
}