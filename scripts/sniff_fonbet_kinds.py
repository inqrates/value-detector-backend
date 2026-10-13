# scripts/sniff_fonbet_kinds.py
"""
Полная карта Fonbet: sportKinds + иерархия событий + периоды/статистика.

Что делает:
  1. Тянет /ma/oraExt/sportKinds — таблицу kind → name для ВСЕХ видов спорта.
  2. Для каждого вида спорта (футбол/хоккей/баскет/теннис/НТ/волейбол/футзал):
       - берёт топовый матч из listBase
       - дёргает /ma/events/event?eventId=X
       - выводит дерево: корень → дочерние (parentId) → внуки
       - для каждого события показывает kind, name, число факторов
  3. Сохраняет итоговую карту: 
       {sport: {kind: {name, parent_kind, level, n_factors, example_fids}}}
  4. Проверяет наличие периодов/статистики и мапит их на kind.

Запуск:
  python scripts/sniff_fonbet_kinds.py
  python scripts/sniff_fonbet_kinds.py --sports football,table_tennis
  python scripts/sniff_fonbet_kinds.py --no-fetch    # использовать сохранённый sportKinds
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import defaultdict
from pathlib import Path

from curl_cffi.requests import AsyncSession


BASE = "https://line-lb54-w.bk6bba-resources.com"
LIST_BASE_URL = f"{BASE}/ma/events/listBase"
EVENT_URL = f"{BASE}/ma/events/event"
SPORT_KINDS_URL = f"{BASE}/ma/oraExt/sportKinds"

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


SPORTS = {
    "football":     {"id": 1,    "root_kind": 1},
    "hockey":       {"id": 2,    "root_kind": 1},
    "basketball":   {"id": 3,    "root_kind": 1},
    "tennis":       {"id": 4,    "root_kind": 1},
    "table_tennis": {"id": 3088, "root_kind": 1},
    "volleyball":   {"id": 9,    "root_kind": 1},
    "futsal":       {"id": 1434, "root_kind": 1},
}


# ============================================================
# 1. sportKinds
# ============================================================
async def fetch_sport_kinds() -> dict:
    cache = OUT / "sportKinds.json"
    if cache.exists():
        print(f"  (кэш) {cache}")
        return json.loads(cache.read_text(encoding="utf-8"))

    async with AsyncSession(impersonate=IMPERSONATE, timeout=30, headers=HEADERS) as s:
        r = await s.get(SPORT_KINDS_URL, headers=HEADERS,
                        params={"version": "0", "lang": "ru", "sysId": "21"})
        r.raise_for_status()
        data = r.json()
    cache.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def parse_sport_kinds(data) -> dict:
    """
    Извлекает {kind_id: {name, alias, ...}} из ответа sportKinds.
    Структура неизвестна заранее, поэтому перебираем разные варианты.
    """
    result: dict[int, dict] = {}

    def walk(node, depth=0):
        if depth > 10:
            return
        if isinstance(node, dict):
            # варианты вложенности: {"id":..., "name":..., "kind":...}
            for id_key in ("id", "kind", "kindId", "rootKind"):
                if id_key in node and isinstance(node.get(id_key), (int, str)):
                    try:
                        kid = int(node[id_key])
                    except (ValueError, TypeError):
                        continue
                    name = node.get("name") or node.get("alias") or node.get("title") or ""
                    if name and kid not in result:
                        result[kid] = {
                            "name": name,
                            "alias": node.get("alias") or "",
                            "rootKind": node.get("rootKind"),
                        }
                    break
            for v in node.values():
                walk(v, depth + 1)
        elif isinstance(node, list):
            for item in node:
                walk(item, depth + 1)

    walk(data)
    return result


# ============================================================
# 2. Снапшот и выбор топовых матчей
# ============================================================
async def fetch_snapshot() -> dict:
    async with AsyncSession(impersonate=IMPERSONATE, timeout=60, headers=HEADERS) as s:
        r = await s.get(LIST_BASE_URL, headers=HEADERS,
                        params={"lang": "ru", "scopeMarket": "1600"})
        r.raise_for_status()
        return r.json()


def pick_top_matches(data: dict, sport_ids: list[int]) -> dict[int, dict]:
    events = data.get("events") or []
    custom = data.get("customFactors") or []
    factors_by_eid = {cf.get("e"): len(cf.get("factors") or []) for cf in custom}
    sports_index = {str(s.get("id")): s for s in (data.get("sports") or [])}

    def root_of(sid):
        cur = sports_index.get(str(sid))
        depth = 0
        while cur and cur.get("parentId") and depth < 10:
            cur = sports_index.get(str(cur["parentId"]))
            depth += 1
        return cur.get("id") if cur else None

    best: dict[int, dict] = {}
    for ev in events:
        root = root_of(ev.get("sportId"))
        if root is None or root not in sport_ids:
            continue
        eid = ev.get("id")
        n_f = factors_by_eid.get(eid, 0)
        if n_f == 0:
            continue
        prev = best.get(root)
        if prev is None or n_f > prev["factors_in_snap"]:
            best[root] = {
                "event_id": eid,
                "factors_in_snap": n_f,
                "team1": ev.get("team1"),
                "team2": ev.get("team2"),
                "sport_id": ev.get("sportId"),
            }
    return best


# ============================================================
# 3. Детальный endpoint
# ============================================================
async def fetch_event(event_id: int) -> dict:
    async with AsyncSession(impersonate=IMPERSONATE, timeout=30, headers=HEADERS) as s:
        r = await s.get(EVENT_URL, headers=HEADERS,
                        params={"lang": "ru", "version": "0",
                                "eventId": str(event_id),
                                "scopeMarket": "1600"})
        r.raise_for_status()
        return r.json()


# ============================================================
# 4. Разбор иерархии
# ============================================================
def build_event_tree(data: dict, root_id: int) -> dict:
    """
    Строит дерево событий из data['events'].
    Возвращает {root: {children: [...]}} с полной вложенностью.
    """
    events = data.get("events") or []
    by_id = {ev.get("id"): ev for ev in events if ev.get("id")}

    factors_by_eid = {}
    for cf in data.get("customFactors") or []:
        factors_by_eid[cf.get("e")] = len(cf.get("factors") or [])

    children_of: dict[int, list[int]] = defaultdict(list)
    for ev in events:
        pid = ev.get("parentId")
        if pid:
            children_of[pid].append(ev.get("id"))

    def walk(eid: int, level: int = 0) -> dict:
        ev = by_id.get(eid, {})
        node = {
            "id": eid,
            "level": level,
            "kind": ev.get("kind"),
            "rootKind": ev.get("rootKind"),
            "name": ev.get("name") or "",
            "team1": ev.get("team1") or "",
            "team2": ev.get("team2") or "",
            "n_factors": factors_by_eid.get(eid, 0),
            "children": [],
        }
        for cid in children_of.get(eid, []):
            node["children"].append(walk(cid, level + 1))
        return node

    return walk(root_id)


def flatten_tree(node: dict) -> list[dict]:
    """Разворачивает дерево в плоский список (для сохранения)."""
    out = []

    def rec(n, parent_kind, parent_id):
        out.append({
            "id": n["id"],
            "level": n["level"],
            "kind": n["kind"],
            "rootKind": n["rootKind"],
            "name": n["name"],
            "parent_id": parent_id,
            "parent_kind": parent_kind,
            "n_factors": n["n_factors"],
        })
        for c in n["children"]:
            rec(c, n["kind"], n["id"])

    rec(node, None, None)
    return out


def print_tree(node: dict, kinds: dict, indent: int = 0, max_children: int = 50):
    """Печатает дерево."""
    prefix = "  " * indent
    kind = node["kind"]
    kn = kinds.get(kind, {}) if kind is not None else {}
    kind_name = kn.get("name", "") if kn else ""
    name = node["name"] or ""

    if indent == 0:
        print(f"{prefix}📍 ROOT  id={node['id']}  kind={kind} ({kind_name})  "
              f"factors={node['n_factors']}  «{node['team1']} vs {node['team2']}»")
    else:
        display = name or kind_name or f"kind={kind}"
        print(f"{prefix}├─ id={node['id']:<10} kind={str(kind):<10} "
              f"factors={node['n_factors']:<4} «{display}»")

    children = node["children"]
    for c in children[:max_children]:
        print_tree(c, kinds, indent + 1, max_children)
    if len(children) > max_children:
        print(f"{prefix}  └─ ... и ещё {len(children) - max_children}")


# ============================================================
# 5. Прогон одного вида спорта
# ============================================================
async def process_sport(sport_key: str, match: dict,
                        kinds: dict, sem: asyncio.Semaphore) -> dict:
    async with sem:
        try:
            data = await fetch_event(match["event_id"])
        except Exception as e:
            return {"sport": sport_key, "error": f"{type(e).__name__}: {e}"}

    tree = build_event_tree(data, match["event_id"])
    flat = flatten_tree(tree)

    print(f"\n{'=' * 78}")
    print(f"  {sport_key.upper()}  eventId={match['event_id']}  "
          f"«{match['team1']} vs {match['team2']}»")
    print(f"  Всего событий: {len(flat)}   "
          f"Всего факторов: {sum(n['n_factors'] for n in flat)}")
    print(f"{'=' * 78}")
    print_tree(tree, kinds, indent=0, max_children=80)

    # ---- собираем итоговую карту kind → {name, fids} ----
    kinds_used: dict[int, dict] = {}
    factors_by_eid = {}
    for cf in data.get("customFactors") or []:
        factors_by_eid[cf.get("e")] = cf.get("factors") or []

    for node in flat:
        k = node["kind"]
        if k is None:
            continue
        if k not in kinds_used:
            kinds_used[k] = {
                "name": kinds.get(k, {}).get("name", "") or node["name"],
                "parent_kind": node["parent_kind"],
                "example_event_name": node["name"],
                "n_events": 0,
                "total_factors": 0,
                "example_fids": set(),
            }
        kinds_used[k]["n_events"] += 1
        kinds_used[k]["total_factors"] += node["n_factors"]
        for f in factors_by_eid.get(node["id"], [])[:50]:
            if f.get("f") is not None:
                kinds_used[k]["example_fids"].add(f.get("f"))

    # преобразуем sets в списки (для JSON)
    for v in kinds_used.values():
        v["example_fids"] = sorted(v["example_fids"])

    return {
        "sport": sport_key,
        "match": match,
        "tree": tree,
        "flat": flat,
        "kinds_used": {str(k): v for k, v in kinds_used.items()},
    }


# ============================================================
# 6. Main
# ============================================================
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sports", type=str,
                    default=",".join(SPORTS.keys()))
    ap.add_argument("--no-fetch", action="store_true",
                    help="использовать кэш sportKinds")
    ap.add_argument("--concurrency", type=int, default=3)
    args = ap.parse_args()

    requested = [s.strip() for s in args.sports.split(",") if s.strip()]
    requested = [s for s in requested if s in SPORTS]
    if not requested:
        print(f"❌ Неверный список. Доступно: {list(SPORTS.keys())}")
        return

    # ---- 1. sportKinds ----
    print(f"\n{'=' * 78}")
    print(f"  SPORT KINDS")
    print(f"{'=' * 78}")
    raw_kinds = await fetch_sport_kinds()
    kinds_map = parse_sport_kinds(raw_kinds)
    print(f"  Всего kind записей: {len(kinds_map)}")
    # покажем первые 40
    for kid in sorted(kinds_map.keys())[:40]:
        v = kinds_map[kid]
        print(f"    kind={kid:<12} → {v.get('name','')[:50]:<50} "
              f"root={v.get('rootKind','')}")
    if len(kinds_map) > 40:
        print(f"    ... и ещё {len(kinds_map) - 40}")

    # ---- 2. снапшот ----
    print(f"\n{'=' * 78}")
    print(f"  ВЫБОР ТОПОВЫХ МАТЧЕЙ")
    print(f"{'=' * 78}")
    snap = await fetch_snapshot()
    sport_ids = [SPORTS[s]["id"] for s in requested]
    top = pick_top_matches(snap, sport_ids)
    for s in requested:
        sid = SPORTS[s]["id"]
        if sid in top:
            m = top[sid]
            print(f"  {s:<15} eventId={m['event_id']:<10} "
                  f"factors={m['factors_in_snap']:<4} "
                  f"«{m['team1']} vs {m['team2']}»")
        else:
            print(f"  {s:<15} ❌ не найдено")

    # ---- 3. прогон ----
    print(f"\n{'=' * 78}")
    print(f"  ИЕРАРХИЯ ПО КАЖДОМУ ВИДУ СПОРТА")
    print(f"{'=' * 78}")
    sem = asyncio.Semaphore(args.concurrency)
    tasks = []
    for s in requested:
        sid = SPORTS[s]["id"]
        if sid in top:
            tasks.append(process_sport(s, top[sid], kinds_map, sem))
    results = await asyncio.gather(*tasks)

    # ---- 4. сводка ----
    print(f"\n{'=' * 78}")
    print(f"  СВОДНАЯ ТАБЛИЦА KIND ПО ВИДАМ СПОРТА")
    print(f"{'=' * 78}")
    for r in results:
        if "error" in r:
            print(f"  {r['sport']}: ❌ {r['error']}")
            continue
        print(f"\n  [{r['sport'].upper()}]  событий: {len(r['flat'])}  "
              f"kind-групп: {len(r['kinds_used'])}")
        for kid, v in sorted(r["kinds_used"].items(), key=lambda x: int(x[0])):
            nm = v["name"][:45] or "—"
            print(f"     kind={kid:<10} × {v['n_events']:<3} "
                  f"factors={v['total_factors']:<5} «{nm}»")

    # ---- 5. сохраняем всё ----
    out = {
        "generated_at": time.time(),
        "kinds_map": {str(k): v for k, v in kinds_map.items()},
        "sports": {
            r["sport"]: {
                "match": r.get("match"),
                "flat_tree": r.get("flat"),
                "kinds_used": r.get("kinds_used"),
            }
            for r in results if "error" not in r
        },
        "errors": [r for r in results if "error" in r],
    }
    path = OUT / "fonbet_kinds_map.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    print(f"\n💾 {path} ({path.stat().st_size / 1024:.1f} KB)")

    # ---- 6. краткая карта периодов ----
    print(f"\n{'=' * 78}")
    print(f"  КАРТА ПЕРИОДОВ И СТАТИСТИКИ (по всем видам)")
    print(f"{'=' * 78}")
    merged: dict[int, dict] = {}
    for r in results:
        if "error" in r:
            continue
        for kid_str, v in (r.get("kinds_used") or {}).items():
            kid = int(kid_str)
            if kid not in merged:
                merged[kid] = {
                    "name": v["name"],
                    "parent_kind": v["parent_kind"],
                    "sports": set(),
                    "example_fids": set(),
                }
            merged[kid]["sports"].add(r["sport"])
            for f in v["example_fids"][:20]:
                merged[kid]["example_fids"].add(f)

    # фильтруем только периоды / статистику (kind > 100 или есть имя)
    print(f"\n  Все kind, встреченные в матчах (все виды):")
    for kid in sorted(merged.keys()):
        v = merged[kid]
        sports_str = ",".join(sorted(v["sports"]))
        nm = v["name"][:40] or "—"
        fids = sorted(v["example_fids"])[:6]
        print(f"    kind={kid:<12} «{nm:<40}» "
              f"parent={str(v['parent_kind']):<8} "
              f"[{sports_str[:35]}] fids={fids}")

    print(f"\n✅ Готово. Карта сохранена в {path}")


if __name__ == "__main__":
    asyncio.run(main())