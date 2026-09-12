# core/sport_map.py
"""
Центральная карта видов спорта по букмекерским конторам.

Каждая БК имеет свой формат идентификации видов спорта:
  - Числовой ID (Fonbet, Betcity, Olimp, Sportbet, Zenit, LigaStavok, Winline)
  - Строковый family (Leon)
  - Строковый slug (Marathon)

Структура:
  SPORT_MAP[bk_id][sport_key] = {
      'ids': [...],           # список числовых или строковых ID
      'aliases': [...],       # альтернативные алиасы (для match по различным полям)
      'name': str,            # человекочитаемое название
      'url_slug': str,        # slug для URL матча
      'cyber_in_real': bool,  # кибер внутри реального (Fonbet, Betcity, Leon)
      'category_ids': [...],  # sportCategoryId для отделения кибера (Fonbet)
  }
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
# НТ:        root 3088, alias 'table-tennis'
# Волейбол:  root 9,    alias 'volleyball'
# Баскетбол: root 3,    alias 'basketball'
# Кибер:     внутри реальных видов по sportCategoryId:
#             118 = FC 26 (киберфутбол)
#             119 = NBA 2K26 (кибербаскет)
#             165 = NHL 26 (киберхоккей)
#            + отдельный root 29086 'esports' (CS, LoL, Dota)
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
        "exclude_category_ids": [119],  # исключаем кибер NBA 2K26
    },
    CYBER_BASKETBALL: {
        "ids": [3],                     # тот же root 3
        "aliases": ["basketball"],
        "name": "Кибербаскетбол",
        "url_slug": "basketball",
        "category_ids": [119],          # только NBA 2K26
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
# Через поле sports[].id_sp в ответе on_air/bets
# Кибер внутри basketball (по аналогии с Fonbet)
# ============================================================
BETCITY = {
    TABLE_TENNIS: {
        "ids": [46],
        "name": "Настольный теннис",
        "url_slug": "table-tennis",
    },
    VOLLEYBALL: {
        "ids": [12],
        "name": "Волейбол",
        "url_slug": "volleyball",
    },
    BASKETBALL: {
        "ids": [3],
        "name": "Баскетбол",
        "url_slug": "basketball",
    },
}


# ============================================================
# OLIMP
# ============================================================
# Через поле sportId в HTTP (строкой)
# Кибер — отдельные ID
# ============================================================
OLIMP = {
    TABLE_TENNIS: {
        "ids": ["40"],
        "name": "Настольный теннис",
        "url_slug": "nastolnyy-tennis-40",
    },
    VOLLEYBALL: {
        "ids": ["10"],
        "name": "Волейбол",
        "url_slug": "voleybol-10",
    },
    BASKETBALL: {
        "ids": ["5"],
        "name": "Баскетбол",
        "url_slug": "basketbol-5",
    },
    CYBER_BASKETBALL: {
        "ids": ["140"],
        "name": "Кибербаскетбол",
        "url_slug": "kiberbasketbol-140",
    },
    CYBERSPORT: {
        "ids": ["112"],
        "name": "Киберспорт",
        "url_slug": "kibersport-112",
    },
    FOOTBALL: {
        "ids": ["1"],
        "name": "Футбол",
        "url_slug": "futbol-1",
    },
    HOCKEY: {
        "ids": ["2"],
        "name": "Хоккей",
        "url_slug": "khokkey-2",
    },
    TENNIS: {
        "ids": ["3"],
        "name": "Теннис",
        "url_slug": "tennis-3",
    },
}


# ============================================================
# SPORTBET
# ============================================================
# Через поле sports[].id в events.table
# ============================================================
SPORTBET = {
    TABLE_TENNIS: {
        "ids": [20],
        "name": "Настольный теннис",
        "url_slug": "table-tennis",
    },
    VOLLEYBALL: {
        "ids": [23],
        "name": "Волейбол",
        "url_slug": "volleyball",
    },
    BASKETBALL: {
        "ids": [2],
        "name": "Баскетбол",
        "url_slug": "basketball",
    },
    FOOTBALL: {
        "ids": [1],
        "name": "Футбол",
        "url_slug": "soccer",
    },
    HOCKEY: {
        "ids": [4],
        "name": "Хоккей",
        "url_slug": "ice-hockey",
    },
    TENNIS: {
        "ids": [5],
        "name": "Теннис",
        "url_slug": "tennis",
    },
}


# ============================================================
# ZENIT
# ============================================================
# Через поле sid в WS (t=21) и HTTP
# Кибер — отдельные ID
# ============================================================
ZENIT = {
    TABLE_TENNIS: {
        "ids": [134],
        "name": "Настольный теннис",
        "url_slug": "134",
    },
    VOLLEYBALL: {
        "ids": [41],
        "name": "Волейбол",
        "url_slug": "41",
    },
    BASKETBALL: {
        "ids": [28],
        "name": "Баскетбол",
        "url_slug": "28",
    },
    CYBER_BASKETBALL: {
        "ids": [564],
        "name": "Кибербаскетбол",
        "url_slug": "564",
    },
    FOOTBALL: {
        "ids": [26],
        "name": "Футбол",
        "url_slug": "26",
    },
    HOCKEY: {
        "ids": [27],
        "name": "Хоккей",
        "url_slug": "27",
    },
    TENNIS: {
        "ids": [33],
        "name": "Теннис",
        "url_slug": "33",
    },
    HANDBALL: {
        "ids": [32],
        "name": "Гандбол",
        "url_slug": "32",
    },
}


# ============================================================
# LIGASTAVOK
# ============================================================
# Через поле gameId в eventsList/tournamentTree
# ============================================================
LIGASTAVOK = {
    TABLE_TENNIS: {
        "ids": [1246],
        "name": "Настольный теннис",
        "url_slug": "table-tennis",
    },
    VOLLEYBALL: {
        "ids": [128],
        "name": "Волейбол",
        "url_slug": "volleyball",
    },
    BASKETBALL: {
        "ids": [25],
        "name": "Баскетбол",
        "url_slug": "basketball",
    },
    CYBERSPORT: {
        "ids": [10014],
        "name": "Киберспорт",
        "url_slug": "cybersport",
    },
    FOOTBALL: {
        "ids": [33],
        "name": "Футбол",
        "url_slug": "soccer",
    },
    HOCKEY: {
        "ids": [31],
        "name": "Хоккей",
        "url_slug": "ice-hockey",
    },
    TENNIS: {
        "ids": [34],
        "name": "Теннис",
        "url_slug": "tennis",
    },
    HANDBALL: {
        "ids": [30],
        "name": "Гандбол",
        "url_slug": "handball",
    },
}


# ============================================================
# LEON
# ============================================================
# Через поле family в ответе API
# Кибербаскет внутри Basketball (region.family == 'ELECTRONIC_LEAGUES')
# ============================================================
LEON = {
    TABLE_TENNIS: {
        "aliases": ["TableTennis"],
        "name": "Настольный теннис",
        "url_slug": "table-tennis",
    },
    VOLLEYBALL: {
        "aliases": ["Volleyball"],
        "name": "Волейбол",
        "url_slug": "volleyball",
    },
    BASKETBALL: {
        "aliases": ["Basketball"],
        "name": "Баскетбол",
        "url_slug": "basketball",
    },
    CYBER_BASKETBALL: {
        "aliases": ["Basketball"],           # тот же вид
        "region_family": "ELECTRONIC_LEAGUES",
        "name": "Кибербаскетбол",
        "url_slug": "basketball",
    },
    FOOTBALL: {
        "aliases": ["Football"],
        "name": "Футбол",
        "url_slug": "football",
    },
    HOCKEY: {
        "aliases": ["IceHockey"],
        "name": "Хоккей",
        "url_slug": "ice-hockey",
    },
    TENNIS: {
        "aliases": ["Tennis"],
        "name": "Теннис",
        "url_slug": "tennis",
    },
}


# ============================================================
# WINLINE
# ============================================================
# Через поле sportId в бинарном WS data_ng
# Кибербаскет — отдельный ID (193)
# ============================================================
WINLINE = {
    TABLE_TENNIS: {
        "ids": [20],
        "name": "Настольный теннис",
        "url_slug": "nastolijnyj_tennis",
    },
    VOLLEYBALL: {
        "ids": [23],
        "name": "Волейбол",
        "url_slug": "volleyball",
    },
    BASKETBALL: {
        "ids": [2],
        "name": "Баскетбол",
        "url_slug": "basketball",
    },
    CYBER_BASKETBALL: {
        "ids": [193],
        "name": "Кибербаскетбол",
        "url_slug": "cyberbasketball",
    },
}


# ============================================================
# MARATHON
# ============================================================
# Через строковый sportSlug в URL SSE
# Новая схема: /sports/by-slug/all-tournaments/live?sportSlug=XXX
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
        "sport_code": "e-Sports",       # + seo.sport.name == "Кибербаскетбол"
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
    """Вернуть список числовых ID для вида спорта (Fonbet, Olimp, ...)."""
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
    """Собрать маппинг {id: sport_key} для всех включённых видов спорта БК."""
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