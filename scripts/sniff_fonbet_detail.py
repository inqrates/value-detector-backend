# scripts/sniff_fonbet_detail.py
"""
Проверки по детальному endpoint Fonbet:
  1. Что реально лежит в ответе (факторы, дочерние события, статистика)
  2. Совпадают ли factor_id с listBase (одна таблица маппинга?)
  3. Работает ли БЕЗ cookies (чистый GET)
  4. Есть ли rate limit (20 запросов подряд)
  5. Что там на других видах спорта (НТ, теннис)

Запуск:
  python scripts/sniff_fonbet_detail.py --event 68519831
  python scripts/sniff_fonbet_detail.py --event 68519831 --rate-limit 20
  python scripts/sniff_fonbet_detail.py --scan-sports
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import Counter, defaultdict
from pathlib import Path

from curl_cffi.requests import AsyncSession


BASE = "https://line-lb54-w.bk6bba-resources.com"
EVENT_URL = f"{BASE}/ma/events/event"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9",
    "origin": "https://fon.bet",
    "referer": "https://fon.bet/",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

IMPERSONATE = "chrome150"
OUT = Path("sniff_out")
OUT.mkdir(exist_ok=True)


async def fetch_event(event_id: int, scope: int = 1600) -> dict:
    async with AsyncSession(impersonate=IMPERSONATE, timeout=30, headers=HEADERS) as s:
        r = await s.get(
            EVENT_URL,
            headers=HEADERS,
            params={"lang": "ru", "version": "0",
                    "eventId": str(event_id), "scopeMarket": str(scope)},
        )
        r.raise_for_status()
        return r.json()


def analyze_event(data: dict, event_id: int):
    events = data.get("events") or []
    custom = data.get("customFactors") or []
    miscs = data.get("eventMiscs") or []
    live_infos = data.get("liveEventInfos") or []

    print(f"\n{'=' * 70}")
    print(f"  СОБЫТИЕ {event_id}")
    print(f"{'=' * 70}")

    # главное событие
    main = next((e for e in events if e.get("id") == event_id), None)
    if main:
        print(f"  {main.get('team1')} vs {main.get('team2')}")
        print(f"  sportId={main.get('sportId')} place={main.get('place')}")

    # дочерние события
    children = [e for e in events if e.get("parentId") == event_id]
    print(f"\n  📋 Дочерних событий (статистика): {len(children)}")
    for ch in children:
        print(f"    id={ch.get('id')} kind={ch.get('kind')} name={ch.get('name')!r}")

    # факторы по каждому событию
    print(f"\n  📊 Factors by event:")
    total_factors = 0
    for cf in custom:
        eid = cf.get("e")
        cnt = cf.get("countAll")
        n = len(cf.get("factors") or [])
        total_factors += n
        label = "MAIN" if eid == event_id else f"child"
        print(f"    e={eid:<12} {label:<6} countAll={cnt:<5} factors={n}")
    print(f"    ВСЕГО факторов: {total_factors}")

    # factor_id Counter
    all_fids = Counter()
    for cf in custom:
        for f in cf.get("factors") or []:
            all_fids[f.get("f")] += 1
    print(f"  Уникальных factor_id в ответе: {len(all_fids)}")

    # сохраняем
    path = OUT / f"event_{event_id}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  💾 {path}")

    return all_fids


async def check_no_cookies(event_id: int):
    """Проверяем что endpoint отдаёт данные без cookies."""
    print(f"\n{'=' * 70}")
    print(f"  ПРОВЕРКА БЕЗ COOKIES")
    print(f"{'=' * 70}")
    async with AsyncSession(impersonate=IMPERSONATE, timeout=30) as s:
        # без наших HEADERS тоже, только голый GET
        r = await s.get(
            EVENT_URL,
            params={"lang": "ru", "version": "0",
                    "eventId": str(event_id), "scopeMarket": "1600"},
        )
        print(f"  HTTP {r.status_code}  size={len(r.content)} B")
        if r.status_code == 200:
            d = r.json()
            n_ev = len(d.get("events") or [])
            n_f = sum(len(cf.get("factors") or []) for cf in d.get("customFactors") or [])
            print(f"  ✅ Работает. events={n_ev}, factors={n_f}")


async def check_rate_limit(event_id: int, n: int = 20):
    """Долбим endpoint N раз подряд, смотрим коды ответов и тайминги."""
    print(f"\n{'=' * 70}")
    print(f"  RATE LIMIT — {n} запросов подряд")
    print(f"{'=' * 70}")
    codes = Counter()
    times = []
    async with AsyncSession(impersonate=IMPERSONATE, timeout=30, headers=HEADERS) as s:
        for i in range(n):
            t0 = time.monotonic()
            try:
                r = await s.get(
                    EVENT_URL,
                    headers=HEADERS,
                    params={"lang": "ru", "version": "0",
                            "eventId": str(event_id), "scopeMarket": "1600"},
                )
                dt = time.monotonic() - t0
                codes[r.status_code] += 1
                times.append(dt)
                mark = "✅" if r.status_code == 200 else "⚠️"
                print(f"  [{i+1:>2}] {mark} {r.status_code} {dt*1000:.0f}ms")
            except Exception as e:
                codes[f"ERR:{type(e).__name__}"] += 1
                print(f"  [{i+1:>2}] 💥 {type(e).__name__}")

    print(f"\n  Итог: {dict(codes)}")
    if times:
        print(f"  Средний отклик: {sum(times)/len(times)*1000:.0f}ms")
        print(f"  Min/Max: {min(times)*1000:.0f} / {max(times)*1000:.0f}ms")


async def scan_sports():
    """
    Берём снапшот listBase, находим по одному топовому событию
    каждого нужного вида и проверяем детальный endpoint.
    """
    print(f"\n{'=' * 70}")
    print(f"  SCAN SPORTS — детальный endpoint по видам спорта")
    print(f"{'=' * 70}")

    from curl_cffi.requests import AsyncSession as S
    async with S(impersonate=IMPERSONATE, timeout=60, headers=HEADERS) as s:
        r = await s.get(
            f"{BASE}/ma/events/listBase",
            headers=HEADERS,
            params={"lang": "ru", "scopeMarket": "1600"},
        )
        data = r.json()

    # группируем events по спорту, берём топ по числу факторов
    custom_by_eid = {cf["e"]: len(cf.get("factors") or [])
                     for cf in data.get("customFactors") or []}
    sports_want = {
        1: "football", 2: "hockey", 4: "tennis",
        3: "basketball", 3088: "table_tennis", 9: "volleyball", 1434: "futsal",
    }

    best = {}
    for ev in data.get("events") or []:
        sid = ev.get("sportId")
        # может быть вложенный — ищем корневой
        root = sid
        # простая эвристика: пробегаем дерево sports
        sports_index = {s["id"]: s for s in data.get("sports") or []}
        cur = sports_index.get(sid)
        depth = 0
        while cur and cur.get("parentId") and depth < 10:
            cur = sports_index.get(cur["parentId"])
            depth += 1
        if cur:
            root = cur.get("id")
        if root not in sports_want:
            continue
        eid = ev.get("id")
        n_f = custom_by_eid.get(eid, 0)
        if n_f == 0:
            continue
        prev = best.get(root)
        if prev is None or n_f > prev[1]:
            best[root] = (eid, n_f, ev.get("team1"), ev.get("team2"))

    print(f"\n  Топ событий по видам спорта:")
    for sid, label in sports_want.items():
        if sid not in best:
            print(f"    {label:<15} — нет данных")
            continue
        eid, n_f, t1, t2 = best[sid]
        print(f"    {label:<15} event={eid}  factors_in_listBase={n_f}  {t1} vs {t2}")

    print(f"\n  Детальный endpoint по каждому:")
    for sid, label in sports_want.items():
        if sid not in best:
            continue
        eid = best[sid][0]
        try:
            d = await fetch_event(eid)
            n_ev = len(d.get("events") or [])
            n_children = sum(1 for e in (d.get("events") or [])
                             if e.get("parentId") == eid)
            n_total = sum(len(cf.get("factors") or [])
                          for cf in d.get("customFactors") or [])
            fids = set()
            for cf in d.get("customFactors") or []:
                for f in cf.get("factors") or []:
                    fids.add(f.get("f"))
            print(f"    {label:<15} event={eid}  children={n_children}  "
                  f"total_factors={n_total}  unique_fids={len(fids)}")
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"    {label:<15} event={eid}  💥 {e}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", type=int)
    ap.add_argument("--scope", type=int, default=1600)
    ap.add_argument("--rate-limit", type=int, default=0)
    ap.add_argument("--no-cookies-check", action="store_true")
    ap.add_argument("--scan-sports", action="store_true")
    args = ap.parse_args()

    if args.scan_sports:
        await scan_sports()
        return

    if not args.event:
        print("Укажи --event <id> или --scan-sports")
        return

    print(f"🌐 Тянем eventId={args.event} scopeMarket={args.scope}")
    data = await fetch_event(args.event, args.scope)
    analyze_event(data, args.event)

    if not args.no_cookies_check:
        await check_no_cookies(args.event)

    if args.rate_limit > 0:
        await check_rate_limit(args.event, args.rate_limit)


if __name__ == "__main__":
    asyncio.run(main())