# config.py
"""
Конфигурация проекта для настольного тенниса.
"""

TABLE_TENNIS_URLS = {
    'fonbet': 'https://fon.bet/live/table-tennis',
    'winline': 'https://winline.ru/live/sport/nastolijnyj_tennis',
    'ligastavok': 'https://www.ligastavok.ru/live/table-tennis',
    'leon': 'https://leon.ru/live/table-tennis',
    'olimp': 'https://www.olimp.bet/live/nastolnyy-tennis-40',
    'betcity': 'https://betcity.ru/ru/live/table-tennis',
    'marathon': 'https://new.marathonbet.ru/su/sport/live/382549',
    'betboom': 'https://betboom.ru/sport/live/table-tennis',
    'zenit': 'https://zenit.win/live/134',
    'sportbet': 'https://sportbet.ru/live/table-tennis?isTime=1',
}

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

# Логирование – меняем на INFO, чтобы не было DEBUG-шума
LOG_LEVEL = "INFO"          # <-- изменено с "DEBUG"
LOG_FILE = "logs/backend.log"