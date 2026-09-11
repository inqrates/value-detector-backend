# core/match_urls.py

URL_TEMPLATES = {
    "fonbet": "https://fon.bet/event/{match_id}",
    "winline": "https://winline.ru/event/{match_id}",
    "ligastavok": "https://www.ligastavok.ru/event/{match_id}",
    "leon": "https://leon.ru/event/{match_id}",
    "olimp": "https://www.olimp.bet/live/match/{match_id}",
    "baltbet": "https://baltbet.ru/event/{match_id}",
    "betcity": "https://m.betcity.ru/event/{match_id}",
    "marathon": "https://new.marathonbet.ru/event/{match_id}",
    "zenit": "https://zenit.win/event/{match_id}",
    "sportbet": "https://sportbet.ru/event/{match_id}",
}

def get_match_url(bk_id: str, match_id: str) -> str:
    template = URL_TEMPLATES.get(bk_id)
    if not template:
        return ""
    return template.format(match_id=match_id)