# scripts/sniff_fonbet.py
"""
Сниффер Fonbet — тянет полный снапшот и показывает, что реально есть в API.

Запуск:
    python scripts/sniff_fonbet.py                       # полный анализ
    python scripts/sniff_fonbet.py --no-fetch            # использовать сохранённый snapshot.json
    python scripts/sniff_fonbet.py --event 12345678      # все факторы конкретного события
    python scripts/sniff_fonbet.py --scope 2000          # другой scopeMarket
    python scripts/sniff_fonbet.py --delta               # послушать дельту 30 сек (live updates)
"""
import asyncio
import json
import argparse
import time
from collections import Counter, defaultdict
from pathlib import Path

from curl_cffi.requests import AsyncSession


CLUSTER = "line-lb54-w.bk6bba-resources.com"
BASE = f"https://{CLUSTER}"
LIST_BASE_URL = f"{BASE}/ma/events/listBase"
LIST_DELTA_URL = f"{BASE}/ma/events/list"

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
OUT_DIR = Path("sniff_out")
OUT_DIR.mkdir(exist_ok=True)


async def fetch_snapshot(scope_market: str) -> dict:
    async with AsyncSession(impersonate=IMPERSONATE, timeout=60, headers=HEADERS) as s:
        r = await s.get(
            LIST_BASE_URL,
            headers=HEADERS,
            params={"lang": "ru", "scopeMarket": scope_market},
        )
        r.raise_for_status()
        return r.json()


def save_raw(data, name: str):
    path = OUT_DIR / name
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    size_mb = path.stat().st_size / 1024 / 1024
    print(f"💾 Сохранено: {path} ({size_mb:.1f} MB)")


# ============================================================
# Аналитика
# ============================================================
def summary(data: dict):
    print(f"\n{'='*70}")
    print("  ОБЩАЯ СВОДКА")
    print(f"{'='*70}")
    for key, v in data.items():
        if isinstance(v, list):
            print(f"  {key:<20} список из {len(v)}")
        elif isinstance(v, dict):
            print(f"  {key:<20} dict с {len(v)} ключами")
        else:
            print(f"  {key:<20} {v}")


def sports_breakdown(data: dict):
    sports = data.get("sports", [])
    print(f"\n{'='*70}")
    print(f"  СПОРТЫ ({len(sports)})")
    print(f"{'='*70}")
    # Только корневые (parentId отсутствует)
    for s in sports:
        if s.get("parentId") is None:
            print(f"  id={s.get('id'):<8} cat={str(s.get('sportCategoryId')):<6} "
                  f"alias={s.get('alias',''):<25} {s.get('name','')}")


def events_breakdown(data: dict):
    events = data.get("events", [])
    live = [e for e in events if e.get("isLive")]
    print(f"\n{'='*70}")
    print(f"  СОБЫТИЯ: всего {len(events)}, live {len(live)}")
    print(f"{'='*70}")

    by_sport = Counter()
    live_by_sport = Counter()
    for ev in events:
        sid = ev.get("sportId")
        by_sport[sid] += 1
        if ev.get("isLive"):
            live_by_sport[sid] += 1

    print(f"  {'sportId':<10} {'всего':<10} {'live':<10}")
    for sid, cnt in by_sport.most_common(30):
        print(f"  {sid:<10} {cnt:<10} {live_by_sport.get(sid,0):<10}")


def factors_breakdown(data: dict):
    custom = data.get("customFactors", [])
    print(f"\n{'='*70}")
    print(f"  CUSTOM FACTORS — {len(custom)} событий с факторами")
    print(f"{'='*70}")
    if not custom:
        print("  ⚠️ Пусто. Возможно, нужен другой scopeMarket.")
        return

    counts = sorted(len(cf.get("factors", [])) for cf in custom)
    n = len(counts)
    print(f"  Факторов на событие:")
    print(f"    min={counts[0]}  p25={counts[n//4]}  median={counts[n//2]}  "
          f"p75={counts[3*n//4]}  p95={counts[int(n*0.95)]}  max={counts[-1]}")
    print(f"  Событий с >50 факторов:   {sum(1 for c in counts if c > 50)}")
    print(f"  Событий с >100 факторов:  {sum(1 for c in counts if c > 100)}")
    print(f"  Событий с >300 факторов:  {sum(1 for c in counts if c > 300)}")
    print(f"  Событий с >1000 факторов: {sum(1 for c in counts if c > 1000)}")

    all_fids = Counter()
    for cf in custom:
        for f in cf.get("factors", []):
            all_fids[f.get("f")] += 1

    print(f"\n  Уникальных factor_id: {len(all_fids)}")
    print(f"\n  Top-40 factor_id по частоте:")
    for fid, cnt in all_fids.most_common(40):
        print(f"    f={fid:<8} → {cnt}")


def top_events_by_size(data: dict, top_n: int = 15):
    custom = data.get("customFactors", [])
    events = {e["id"]: e for e in data.get("events", [])}

    rows = []
    for cf in custom:
        eid = cf.get("e")
        n = len(cf.get("factors", []))
        ev = events.get(eid, {})
        rows.append((n, eid, ev.get("sportId"), ev.get("team1", ""), ev.get("team2", ""),
                     ev.get("tournamentName", ""), ev.get("isLive")))
    rows.sort(reverse=True)

    print(f"\n{'='*70}")
    print(f"  ТОП-{top_n} СОБЫТИЙ ПО ЧИСЛУ ФАКТОРОВ")
    print(f"{'='*70}")
    for n, eid, sid, t1, t2, tour, isl in rows[:top_n]:
        flag = "🔴LIVE" if isl else "     "
        print(f"  {n:>6} | id={eid:<12} sport={sid:<6} {flag} {t1} vs {t2} [{tour[:40]}]")


def deep_event(data: dict, event_id: int):
    events = data.get("events", [])
    custom = data.get("customFactors", [])
    ev = next((e for e in events if e.get("id") == event_id), None)
    cf = next((c for c in custom if c.get("e") == event_id), None)

    print(f"\n{'='*70}")
    print(f"  СОБЫТИЕ {event_id}")
    print(f"{'='*70}")
    if not ev:
        print("  ❌ не найдено")
        return

    print(json.dumps(ev, ensure_ascii=False, indent=2)[:2500])

    if not cf:
        print("  ⚠️ Нет customFactors для этого события")
        return

    factors = cf.get("factors", [])
    print(f"\n  ФАКТОРОВ: {len(factors)}")

    by_pt = defaultdict(list)
    for f in factors:
        by_pt[f.get("pt", "")].append(f)

    print(f"  Уникальных pt (названий линий): {len(by_pt)}")
    print(f"\n  Первые 80 факторов (f=id рынка, pt=параметр, v=кэф, p=число):")
    for f in factors[:80]:
        print(f"    f={f.get('f'):<8} pt={str(f.get('pt','')):<15} "
              f"v={f.get('v',''):<8} p={f.get('p','')}")


async def listen_delta(seconds: int = 30):
    """Слушаем дельту — покажет, какие факторы реально обновляются в live."""
    print(f"\n{'='*70}")
    print(f"  DELTA SNIFFER — слушаем {seconds} сек")
    print(f"{'='*70}")

    async with AsyncSession(impersonate=IMPERSONATE, timeout=60, headers=HEADERS) as s:
        r = await s.get(LIST_BASE_URL, headers=HEADERS,
                        params={"lang": "ru", "scopeMarket": "1600"})
        data = r.json()
        version = data.get("packetVersion")
        print(f"  Начальный packetVersion={version}")

        t0 = time.time()
        updates_by_fid = Counter()
        updates_by_event = Counter()
        poll_count = 0

        while time.time() - t0 < seconds:
            try:
                r = await s.get(LIST_DELTA_URL, headers=HEADERS,
                                params={"lang": "ru", "version": str(version),
                                        "scopeMarket": "1600"})
                if r.status_code != 200:
                    await asyncio.sleep(0.5)
                    continue
                d = r.json()
                poll_count += 1
                new_ver = d.get("packetVersion")
                if new_ver:
                    version = new_ver

                for cf in d.get("customFactors", []):
                    eid = cf.get("e")
                    for f in cf.get("factors", []):
                        updates_by_fid[f.get("f")] += 1
                        updates_by_event[eid] += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"  error: {e}")
                await asyncio.sleep(1)

        print(f"\n  Опросов за {seconds}с: {poll_count}")
        print(f"  Событий с обновлениями: {len(updates_by_event)}")
        print(f"  Обновлений по factor_id:")
        for fid, cnt in updates_by_fid.most_common(30):
            print(f"    f={fid:<8} → {cnt}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", default="1600")
    ap.add_argument("--no-fetch", action="store_true")
    ap.add_argument("--event", type=int)
    ap.add_argument("--delta", action="store_true")
    ap.add_argument("--delta-seconds", type=int, default=30)
    args = ap.parse_args()

    if args.delta:
        await listen_delta(args.delta_seconds)
        return

    snap_path = OUT_DIR / "snapshot.json"
    if args.no_fetch and snap_path.exists():
        print(f"📂 Читаем {snap_path}")
        data = json.loads(snap_path.read_text(encoding="utf-8"))
    else:
        print(f"🌐 Тянем снапшот scopeMarket={args.scope}...")
        t0 = time.time()
        data = await fetch_snapshot(args.scope)
        print(f"✅ Получено за {time.time()-t0:.2f}с")
        save_raw(data, f"snapshot_scope{args.scope}.json")

    summary(data)
    sports_breakdown(data)
    events_breakdown(data)
    factors_breakdown(data)
    top_events_by_size(data, 15)

    if args.event:
        deep_event(data, args.event)

    print(f"\n💡 Дальше:")
    print(f"   python scripts/sniff_fonbet.py --no-fetch --event <id_из_топа>")
    print(f"   python scripts/sniff_fonbet.py --delta --delta-seconds 30")


if __name__ == "__main__":
    asyncio.run(main())