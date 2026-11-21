# debug_ligastavok.py
"""
Диагностика LigaStavok: проверяем, что реально отдаёт API
и что лежит в куках. Запускать в корне проекта:

    python debug_ligastavok.py

Скрипт делает:
  1. Проверяет cookies/ligastavok.json — есть ли qrator_jsr.
  2. Дёргает /events/v8/eventsList с РАЗНЫМИ параметрами
     (limit, view, topEvents) и сравнивает, сколько отдаёт.
  3. Если бекенд запущен — показывает, сколько матчей Лиги
     в агрегаторе через /matches.
  4. Пишет итог: в чём проблема — лимит, view или куки.
"""
import json
import sys
import time
from pathlib import Path
from collections import Counter

try:
    from curl_cffi.requests import Session
except ImportError:
    print("❌ Установи: pip install curl_cffi")
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


def load_cookies():
    if not COOKIES_PATH.exists():
        print(f"❌ Нет файла {COOKIES_PATH}")
        sys.exit(1)
    data = json.loads(COOKIES_PATH.read_text(encoding="utf-8"))
    return data.get("cookies", [])


def check_cookies(cookies):
    print("\n" + "=" * 70)
    print("1) КУКИ")
    print("=" * 70)
    age = time.time() - COOKIES_PATH.stat().st_mtime
    print(f"Файл:     {COOKIES_PATH}")
    print(f"Размер:   {COOKIES_PATH.stat().st_size} байт")
    print(f"Обновлён: {age / 60:.1f} мин назад ({age / 3600:.2f} ч)")
    print(f"Cookies:  {len(cookies)}")

    names = {}
    for c in cookies:
        n = (c.get("name") or "").lower()
        names[n] = c.get("value", "")

    qrator_markers = [n for n in names if n.startswith("qrator")]
    print(f"Qrator:   {qrator_markers or '❌ НЕТ'}")

    if "qrator_jsr" not in names:
        print("\n⚠️ qrator_jsr ОТСУТСТВУЕТ — куки битые, "
              "LigaStavok будет отдавать 403 или пустой ответ")

    domains = Counter()
    for c in cookies:
        domains[c.get("domain", "?")] += 1
    print("\nДомены:")
    for d, n in domains.most_common():
        print(f"  {d}: {n}")


def call_events(session, body, label):
    print(f"\n--- {label} ---")
    print(f"body: {json.dumps(body, ensure_ascii=False)}")
    try:
        r = session.post(EVENTS_URL, headers=HEADERS, json=body, timeout=30)
    except Exception as e:
        print(f"❌ network error: {e}")
        return 0
    print(f"HTTP {r.status_code}")
    if r.status_code != 200:
        print(f"body: {r.text[:300]}")
        return 0

    try:
        data = r.json()
    except Exception:
        print(f"❌ not JSON: {r.text[:300]}")
        return 0

    result = data.get("result") or {}
    items = result.get("data") or []
    total = result.get("total") or result.get("totalCount")
    print(f"items:  {len(items)}")
    print(f"total:  {total}")

    if items:
        sports = Counter()
        for it in items:
            sports[it.get("categorySeoName") or "?"] += 1
        print(f"виды:   {dict(sports)}")
        first = items[0]
        print(f"first:  id={first.get('id')} seo={first.get('categorySeoName')}")

    return len(items)


def main():
    cookies = load_cookies()
    check_cookies(cookies)

    session = Session(impersonate="chrome150", timeout=30)
    for c in cookies:
        try:
            session.cookies.set(
                c["name"], c["value"],
                domain=c.get("domain"),
                path=c.get("path", "/"),
            )
        except Exception:
            pass

    print("\n" + "=" * 70)
    print("2) ЗАПРОСЫ eventsList")
    print("=" * 70)

    base = {
        "gameId": [],
        "limit": 40,
        "ns": ["live"],
        "topEvents": False,
        "view": "priority",
        "widgetVideo": False,
    }

    n_default = call_events(session, dict(base), "default (как в парсере)")

    v1 = dict(base); v1["limit"] = 200
    n_limit200 = call_events(session, v1, "limit=200")

    v2 = dict(base); v2["view"] = "all"
    n_all = call_events(session, v2, "view=all")

    v3 = dict(base); v3["topEvents"] = True
    n_top = call_events(session, v3, "topEvents=true")

    v4 = dict(base); v4["view"] = "all"; v4["limit"] = 200
    n_both = call_events(session, v4, "view=all + limit=200")

    # ── Бекенд /matches ──
    print("\n" + "=" * 70)
    print("3) БЕКЕНД /matches")
    print("=" * 70)
    try:
        backend = session.get("http://localhost:8000/matches", timeout=5)
        if backend.status_code == 200:
            data = backend.json()
            liga = [m for m in data if "ligastavok" in (m.get("bks") or {})]
            print(f"Всего матчей: {len(data)}")
            print(f"С Лигой:      {len(liga)}")
        else:
            print(f"HTTP {backend.status_code}")
    except Exception as e:
        print(f"⚠️ бекенд недоступен: {e}")

    # ── Итог ──
    print("\n" + "=" * 70)
    print("ИТОГ")
    print("=" * 70)
    print(f"default (limit=40, view=priority): {n_default}")
    print(f"limit=200:                         {n_limit200}")
    print(f"view=all:                          {n_all}")
    print(f"topEvents=true:                    {n_top}")
    print(f"view=all + limit=200:              {n_both}")

    if n_default <= 40 and n_limit200 > 40:
        print("\n👉 Проблема в лимите. Увеличь self.LIMIT "
              "в ligastavok_api.py до 200.")
    elif n_default <= 40 and n_all > 40:
        print("\n👉 Проблема в view. Замени 'priority' на 'all'.")
    elif n_default <= 40 and n_both > 40:
        print("\n👉 Нужно И view='all', И limit=200.")
    elif n_default > 40:
        print("\n✅ API уже отдаёт всё корректно — проблема не в лимите.")
    else:
        print("\n⚠️ Все варианты вернули <= 40 — проверь куки/Qrator/IP.")


if __name__ == "__main__":
    main()