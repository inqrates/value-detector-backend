# scripts/sniff_fonbet_full.py
"""
Полный сниффер Fonbet — ищет максимум рынков, перебирая scopeMarket.

Что делает:
  1. Пробует большой набор scopeMarket на /ma/events/listBase
  2. Для каждого рабочего — считает events, factors/event, unique factor_id
  3. Собирает объединённый список всех factor_id со всех scope
  4. Показывает какие scope дают уникальные рынки (диффы между scope)
  5. Анализирует live (через eventMiscs, liveEventInfos, isLive)
  6. Опционально: --events N — полные факторы топ-N событий на scope

Запуск:
    python scripts/sniff_fonbet_full.py                    # все scope
    python scripts/sniff_fonbet_full.py --scopes 1600,2300,5000
    python scripts/sniff_fonbet_full.py --events 3         # +топ-3 события детально
    python scripts/sniff_fonbet_full.py --no-fetch         # только анализ сохранённого
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from collections import Counter, defaultdict
from pathlib import Path

from curl_cffi.requests import AsyncSession


CLUSTER = "line-lb54-w.bk6bba-resources.com"
BASE = f"https://{CLUSTER}"
LIST_BASE_URL = f"{BASE}/ma/events/listBase"

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

# Набор scope для перебора. Лишние вернут 404 — не страшно.
DEFAULT_SCOPES = [
    # популярные пакеты Fonbet
    1600, 2300, 2400, 2500, 3000,
    5000, 6000, 7000, 8000,
    10000, 12000, 15000, 20000, 30000,
    # промежуточные на всякий случай
    1000, 1100, 1200, 1300, 1400, 1500, 1700, 1800, 1900,
    2100, 2200, 2600, 2700, 2800, 2900,
    4000, 4500, 5500, 6500, 7500, 9000,
]


def _human_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ============================================================
# Запросы
# ============================================================
async def probe_scope(session: AsyncSession, scope: int) -> tuple[dict | None, int]:
    """
    Один запрос к listBase со scope.
    Возвращает (data, raw_size) если ответ валидный JSON с событиями,
    иначе (None, 0).
    """
    try:
        r = await session.get(
            LIST_BASE_URL,
            headers=HEADERS,
            params={"lang": "ru", "scopeMarket": str(scope)},
        )
    except Exception as e:
        print(f"(err {type(e).__name__})", end=" ")
        return None, 0

    if r.status_code != 200:
        return None, 0

    try:
        data = r.json()
    except Exception:
        return None, 0

    if not isinstance(data, dict):
        return None, 0

    # 200 может прийти с пустым events — это нерабочий scope
    if not data.get("events"):
        return None, 0

    raw_size = len(r.content) if r.content else 0
    return data, raw_size


# ============================================================
# Аналитика одного снапшота
# ============================================================
def analyze_snapshot(data: dict) -> dict:
    events = data.get("events", []) or []
    custom = data.get("customFactors", []) or []
    miscs = data.get("eventMiscs", []) or []
    live_infos = data.get("liveEventInfos", []) or []
    event_blocks = data.get("eventBlocks", []) or []

    events_by_id = {e.get("id"): e for e in events if e.get("id")}

    factor_counts = []
    fid_counter = Counter()
    sport_factor_map: dict[int, Counter] = defaultdict(Counter)

    for cf in custom:
        eid = cf.get("e")
        factors = cf.get("factors") or []
        n = len(factors)
        if n == 0:
            continue
        factor_counts.append(n)
        sport_id = (events_by_id.get(eid) or {}).get("sportId")
        for f in factors:
            fid = f.get("f")
            if fid is None:
                continue
            fid_counter[fid] += 1
            if sport_id is not None:
                sport_factor_map[sport_id][fid] += 1

    factor_counts.sort()
    n = len(factor_counts)

    return {
        "events": len(events),
        "custom_events": len(custom),
        "miscs": len(miscs),
        "live_infos": len(live_infos),
        "event_blocks": len(event_blocks),
        "factor_events_total": n,
        "min": factor_counts[0] if n else 0,
        "median": factor_counts[n // 2] if n else 0,
        "p75": factor_counts[3 * n // 4] if n else 0,
        "p95": factor_counts[int(n * 0.95)] if n else 0,
        "max": factor_counts[-1] if n else 0,
        "gt_50": sum(1 for c in factor_counts if c > 50),
        "gt_100": sum(1 for c in factor_counts if c > 100),
        "gt_300": sum(1 for c in factor_counts if c > 300),
        "unique_fids": len(fid_counter),
        "fid_counter": fid_counter,
        "sport_factor_map": sport_factor_map,
    }


def top_events_by_factors(data: dict, top_n: int = 10) -> list[dict]:
    custom = data.get("customFactors", []) or []
    events = {e.get("id"): e for e in data.get("events", []) if e.get("id")}
    rows = []
    for cf in custom:
        eid = cf.get("e")
        n = len(cf.get("factors") or [])
        ev = events.get(eid) or {}
        rows.append({
            "factors": n,
            "id": eid,
            "sport_id": ev.get("sportId"),
            "team1": ev.get("team1"),
            "team2": ev.get("team2"),
            "tournament": ev.get("tournamentName"),
            "is_live": ev.get("isLive"),
        })
    rows.sort(key=lambda x: -x["factors"])
    return rows[:top_n]


def live_info(data: dict) -> dict:
    events = data.get("events", []) or []
    miscs = data.get("eventMiscs", []) or []
    live_infos = data.get("liveEventInfos", []) or []

    is_live_flag = sum(1 for e in events if e.get("isLive"))
    misc_ids = {m.get("id") for m in miscs if m.get("id")}
    events_with_misc = sum(1 for e in events if e.get("id") in misc_ids)

    samples = []
    for m in miscs[:3]:
        samples.append({
            "id": m.get("id"),
            "score1": m.get("score1"),
            "score2": m.get("score2"),
            "comment": m.get("comment"),
            "liveDelay": m.get("liveDelay"),
        })

    return {
        "is_live_flag": is_live_flag,
        "miscs_count": len(miscs),
        "live_infos_count": len(live_infos),
        "events_with_misc": events_with_misc,
        "samples": samples,
    }


# ============================================================
# Сохранение
# ============================================================
def save_json(obj, name: str) -> Path:
    p = OUT_DIR / name
    p.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    return p


def save_top_events_details(data: dict, scope: int, top_n: int):
    tops = top_events_by_factors(data, top_n)
    custom = {cf.get("e"): cf for cf in data.get("customFactors", []) or []}
    events = {e.get("id"): e for e in data.get("events", []) if e.get("id")}

    details = []
    for row in tops:
        eid = row["id"]
        details.append({
            "event": events.get(eid),
            "factors": (custom.get(eid) or {}).get("factors", []),
        })
    save_json(details, f"top{top_n}_scope{scope}.json")


# ============================================================
# Вывод
# ============================================================
def print_scope_result(scope: int, size: int, stats: dict):
    print(f"\n{'─' * 70}")
    print(f"  SCOPE {scope}  ({_human_size(size)})")
    print(f"{'─' * 70}")
    print(f"  events:            {stats['events']}")
    print(f"  custom_events:     {stats['custom_events']}")
    print(f"  eventMiscs:        {stats['miscs']}")
    print(f"  liveEventInfos:    {stats['live_infos']}")
    print(f"  factors on event:  min={stats['min']} med={stats['median']} "
          f"p75={stats['p75']} p95={stats['p95']} max={stats['max']}")
    print(f"  >50: {stats['gt_50']}  >100: {stats['gt_100']}  >300: {stats['gt_300']}")
    print(f"  unique factor_id:  {stats['unique_fids']}")


def print_summary(results: dict[int, dict]):
    if not results:
        print("\n❌ Ни один scope не сработал.")
        return

    print(f"\n{'=' * 70}")
    print(f"  СВОДКА ПО ВСЕМ SCOPE ({len(results)})")
    print(f"{'=' * 70}")
    header = f"  {'scope':<8}{'events':<10}{'med':<6}{'p95':<6}{'max':<6}{'>100':<6}{'#fid':<6}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    for scope in sorted(results.keys()):
        s = results[scope]["stats"]
        print(
            f"  {scope:<8}{s['events']:<10}{s['median']:<6}{s['p95']:<6}"
            f"{s['max']:<6}{s['gt_100']:<6}{s['unique_fids']:<6}"
        )

    # объединённый список factor_id
    all_fids: Counter = Counter()
    fids_per_scope: dict[int, set] = {}
    for scope, r in results.items():
        fids_per_scope[scope] = set(r["stats"]["fid_counter"].keys())
        all_fids.update(r["stats"]["fid_counter"])

    print(f"\n  Объединённый набор factor_id: {len(all_fids)}")

    # какие scope уникальны
    common_across = set.intersection(*fids_per_scope.values()) if fids_per_scope else set()
    print(f"\n  fid, встречающиеся во ВСЕХ scope: {len(common_across)}")
    if common_across:
        print(f"    {sorted(common_across)[:20]}{'...' if len(common_across) > 20 else ''}")

    print(f"\n  Уникальные fid (только в этом scope):")
    for scope in sorted(fids_per_scope.keys()):
        others = set().union(*[v for k, v in fids_per_scope.items() if k != scope]) \
            if len(fids_per_scope) > 1 else set()
        only_here = fids_per_scope[scope] - others
        if only_here:
            print(f"    scope {scope}: {len(only_here)} unique "
                  f"(примеры: {sorted(only_here)[:8]})")

    # -- сохраняем сводку --
    summary_obj = {
        "scopes": {
            str(scope): {
                "size_bytes": r["size"],
                "stats": {k: v for k, v in r["stats"].items()
                          if k not in ("fid_counter", "sport_factor_map")},
                "top_200_fids": dict(r["stats"]["fid_counter"].most_common(200)),
            }
            for scope, r in results.items()
        },
        "merged_unique_fids": sorted(all_fids.keys()),
        "common_fids_all_scopes": sorted(common_across),
    }
    save_json(summary_obj, "summary.json")
    print(f"\n  💾 sniff_out/summary.json")


# ============================================================
# Main
# ============================================================
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scopes", type=str, default="",
                    help="через запятую: 1600,2300,5000")
    ap.add_argument("--events", type=int, default=0,
                    help="сохранить полные факторы топ-N событий на каждый scope")
    ap.add_argument("--no-fetch", action="store_true",
                    help="не делать запросы, использовать сохранённые snapshot_scope*.json")
    ap.add_argument("--jitter", type=float, default=0.6,
                    help="базовая пауза между запросами (сек)")
    args = ap.parse_args()

    scopes = ([int(x.strip()) for x in args.scopes.split(",") if x.strip()]
              if args.scopes else DEFAULT_SCOPES)

    print(f"🔎 Перебираем {len(scopes)} scope, jitter≈{args.jitter}s")
    print(f"   рабочие снапшоты → sniff_out/snapshot_scope<N>.json")

    results: dict[int, dict] = {}

    async with AsyncSession(impersonate=IMPERSONATE, timeout=90, headers=HEADERS) as session:
        for i, scope in enumerate(scopes, 1):
            print(f"\n[{i}/{len(scopes)}] scope={scope} ...", end=" ", flush=True)

            if args.no_fetch:
                p = OUT_DIR / f"snapshot_scope{scope}.json"
                if not p.exists():
                    print("skip (no file)")
                    continue
                data = json.loads(p.read_text(encoding="utf-8"))
                size = p.stat().st_size
                print(f"loaded {_human_size(size)}")
            else:
                t0 = time.monotonic()
                data, size = await probe_scope(session, scope)
                dt = time.monotonic() - t0
                if data is None:
                    print(f"404/empty → skip  ({dt:.1f}s)")
                    await asyncio.sleep(args.jitter + random.uniform(0, 0.4))
                    continue
                print(f"OK {_human_size(size)} ({dt:.1f}s)")
                save_json(data, f"snapshot_scope{scope}.json")

            stats = analyze_snapshot(data)
            results[scope] = {"size": size, "stats": stats}

            print_scope_result(scope, size, stats)

            print(f"  Топ-5 событий по числу факторов:")
            for row in top_events_by_factors(data, 5):
                live_mark = "🔴" if row.get("is_live") else "  "
                print(f"    {row['factors']:>5} | id={row['id']:<12} "
                      f"sport={row['sport_id']:<8} {live_mark} "
                      f"{row['team1']} vs {row['team2']}")

            li = live_info(data)
            print(f"  live: isLive_flag={li['is_live_flag']} "
                  f"miscs={li['miscs_count']} "
                  f"liveEventInfos={li['live_infos_count']} "
                  f"events_with_misc={li['events_with_misc']}")

            if args.events > 0:
                save_top_events_details(data, scope, args.events)
                print(f"  💾 top{args.events}_scope{scope}.json")

            if not args.no_fetch:
                await asyncio.sleep(args.jitter + random.uniform(0, 0.5))

    print_summary(results)
    print("\n✅ Готово.")


if __name__ == "__main__":
    asyncio.run(main())