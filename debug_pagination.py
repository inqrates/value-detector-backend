# debug_pagination.py
import json
import sys
import time
from pathlib import Path
from collections import Counter

try:
    from curl_cffi.requests import Session
except ImportError:
    print("❌ pip install curl_cffi")
    sys.exit(1)

COOKIES_PATH = Path("cookies/ligastavok.json")
EVENTS_URL = "https://lds-api-sites.ligastavok.ru/rest/events/v8/eventsList"
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9",
    "content-type": "application/json",
    "origin": "https://www.ligastavok.ru",
    "referer": "https://www.ligastavok.ru/",
}
OUR_SPORTS = {"nastolnyi-tennis", "voleibol", "basketbol", "kiberbasketbol"}
LIMIT = 40
MAX_PAGES = 30


def load_cookies():
    if not COOKIES_PATH.exists():
        print(f"❌ Нет {COOKIES_PATH}")
        sys.exit(1)
    return json.loads(COOKIES_PATH.read_text(encoding="utf-8")).get("cookies", [])


def build_session(cookies):
    s = Session(impersonate="chrome150", timeout=30)
    for c in cookies:
        try:
            s.cookies.set(
                c["name"], c["value"],
                domain=c.get("domain"),
                path=c.get("path", "/"),
            )
        except Exception:
            pass
    return s


def dump_what_we_send(session, cookies):
    """Что реально уходит на lds-api-sites.ligastavok.ru."""
    print("\n" + "=" * 70)
    print("ДИАГНОСТИКА: что уходит на lds-api-sites.ligastavok.ru")
    print("=" * 70)

    # IP (проверка смены)
    try:
        ipr = session.get("https://api.ipify.org?format=json", timeout=5)
        ip = ipr.json().get("ip", "?")
        print(f"Твой IP: {ip}")
    except Exception as e:
        print(f"IP: не удалось получить ({e})")

    # Cookie-домены из файла
    print("\nCookies из файла по доменам:")
    domains = Counter()
    for c in cookies:
        domains[c.get("domain", "?")] += 1
    for d, n in domains.most_common():
        print(f"  {d}: {n}")

    # Какие cookie-имена сидят на .ligastavok.ru (уходят на lds-api-sites)
    relevant = [
        c for c in cookies
        if c.get("domain") in (".ligastavok.ru", "ligastavok.ru")
        or (c.get("domain", "").endswith(".ligastavok.ru")
            and not c.get("domain", "").startswith("www."))
    ]
    print(f"\nCookies для lds-api-sites.ligastavok.ru "
          f"(domain=.ligastavok.ru): {len(relevant)}")
    for c in relevant:
        name = c.get("name", "?")
        val = (c.get("value") or "")[:20]
        print(f"  {name} = {val}...")
    has_qrator = any(c.get("name", "").lower() == "qrator_jsr" for c in relevant)
    print(f"\nqrator_jsr на .ligastavok.ru: "
          f"{'✅ ДА (уйдёт в запрос)' if has_qrator else '❌ НЕТ (не уйдёт!)'}")

    # Cookie header, который уйдёт в реальном запросе
    try:
        # тестовый запрос — смотрим какие cookies уходят
        test = session.post(
            EVENTS_URL,
            headers=HEADERS,
            json={"gameId": [], "limit": 1, "ns": ["live"],
                  "topEvents": False, "view": "priority",
                  "widgetVideo": False},
            timeout=15,
        )
        # curl_cffi не отдаёт отправленный cookie header напрямую,
        # но можно проверить через заголовки запроса
        print(f"\nТестовый запрос: HTTP {test.status_code}")
        if test.status_code == 403:
            print("→ 403, куки не приняты. Причина в cookies, не в скрипте.")
    except Exception as e:
        print(f"Тестовый запрос: ошибка {e}")


def fetch_page(session, skip):
    body = {
        "gameId": [], "limit": LIMIT, "ns": ["live"],
        "topEvents": False, "view": "priority", "widgetVideo": False,
    }
    if skip > 0:
        body["skip"] = skip
        body["ts"] = int(time.time() * 1000)
    try:
        r = session.post(EVENTS_URL, headers=HEADERS, json=body, timeout=30)
    except Exception as e:
        print(f"   ❌ network: {e}")
        return [], 0, 0
    if r.status_code != 200:
        return [], 0, r.status_code
    try:
        data = r.json()
    except Exception:
        return [], 0, 200
    result = data.get("result") or {}
    return (result.get("data") or [],
            result.get("total") or result.get("totalCount") or 0,
            200)


def main():
    cookies = load_cookies()
    age_min = (time.time() - COOKIES_PATH.stat().st_mtime) / 60
    names = {(c.get("name") or "").lower() for c in cookies}
    print("=" * 70)
    print(f"Куки: {len(cookies)} шт., возраст {age_min:.1f} мин, "
          f"qrator_jsr={'✅' if 'qrator_jsr' in names else '❌'}")
    print("=" * 70)

    session = build_session(cookies)
    dump_what_we_send(session, cookies)

    seen_ids = set()
    our_seen = set()
    all_sports = Counter()
    our_by_name = Counter()
    total_api = 0
    skip = 0
    page = 0

    print("\n" + "=" * 70)
    print("ПАГИНАЦИЯ")
    print("=" * 70)
    while page < MAX_PAGES:
        items, total_here, status = fetch_page(session, skip)
        if status != 200:
            print(f"стр {page}: ❌ HTTP {status}")
            break
        if total_here:
            total_api = total_here
        if not items:
            print(f"стр {page} (skip={skip}): items=0 — стоп")
            break

        new = []
        for it in items:
            eid = it.get("id")
            if eid and eid not in seen_ids:
                seen_ids.add(eid)
                new.append(it)
                seo = it.get("categorySeoName") or "?"
                all_sports[seo] += 1
                if seo in OUR_SPORTS:
                    our_seen.add(eid)
                    our_by_name[seo] += 1

        print(f"стр {page} (skip={skip:>3}): items={len(items):>2} "
              f"new={len(new):>2} (unique={len(seen_ids):>3}, "
              f"total API={total_api})")

        if len(items) < LIMIT:
            print(f"   → items < {LIMIT}, конец")
            break
        if not new:
            print("   ⚠️ нет новых — сервер отдаёт дубликаты, стоп")
            break
        if total_api and len(seen_ids) >= total_api:
            print(f"   → достигли total ({total_api})")
            break
        skip += LIMIT
        page += 1
        time.sleep(0.3)

    print("\n" + "=" * 70)
    print("ИТОГ")
    print("=" * 70)
    print(f"Всего уникальных: {len(seen_ids)} (total API={total_api})")
    print(f"Наших видов:     {len(our_seen)}")
    for seo, n in our_by_name.most_common():
        print(f"  ★ {seo}: {n}")
    if len(seen_ids) > LIMIT:
        print(f"\n✅ Пагинация работает ({len(seen_ids)} > {LIMIT})")
    elif len(seen_ids) == LIMIT:
        print(f"\n❌ Только {LIMIT} — skip не работает")
    else:
        print(f"\n⚠️ < {LIMIT} — см. диагностику выше")


if __name__ == "__main__":
    main()