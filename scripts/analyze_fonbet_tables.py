# scripts/analyze_fonbet_tables.py
"""
Fonbet: анализ eventViewTables → компактный digest.

Что вытаскивает:
  1. Все configs (какие sports + place: line/live)
  2. Все tabs → tablesInTab с eventKind, title, grid, sortOrder
  3. Уникальные market → eventKind связки
  4. Сопоставление с factor_details (fid → market → outcome)

Запуск:
  python scripts/analyze_fonbet_tables.py \
      --tables fonbet_raw_eventViewTables.json \
      --factors fonbet_factor_details.json \
      --out fonbet_digest.json
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def analyze_event_view_tables(raw: dict) -> dict:
    """Извлекает компактную карту из eventViewTables."""
    configs = raw.get("configs") or []
    print(f"  configs: {len(configs)}")

    digest = {
        "version": raw.get("version"),
        "lang": raw.get("lang"),
        "configs": [],
    }

    unique_tables: dict = {}          # key: (eventKind, grid, title)
    all_event_kinds: dict = defaultdict(lambda: {
        "titles": set(), "grids": set(), "sports": set(), "places": set(),
    })

    for cfg in configs:
        cfg_id = cfg.get("id")
        place = cfg.get("place")
        sports = cfg.get("sports") or []

        tabs_out = []
        for tab in cfg.get("tabs") or []:
            tab_id = tab.get("id")
            tab_title = tab.get("title")
            tab_sort = tab.get("sortOrder")

            tables_out = []
            for tbl in tab.get("tablesInTab") or []:
                ek = tbl.get("eventKind")
                title = tbl.get("title")
                grid = tbl.get("grid")
                sort_order = tbl.get("sortOrder")
                group_index = tbl.get("groupIndex")

                tables_out.append({
                    "id": tbl.get("id"),
                    "eventKind": ek,
                    "grid": grid,
                    "title": title,
                    "sortOrder": sort_order,
                    "groupIndex": group_index,
                })

                key = (ek, grid, title)
                if key not in unique_tables:
                    unique_tables[key] = {
                        "eventKind": ek,
                        "grid": grid,
                        "title": title,
                        "sports": set(),
                        "places": set(),
                    }
                unique_tables[key]["sports"].update(str(s) for s in sports)
                unique_tables[key]["places"].add(place)

                if ek is not None:
                    all_event_kinds[str(ek)]["titles"].add(title or "")
                    all_event_kinds[str(ek)]["grids"].add(str(grid))
                    all_event_kinds[str(ek)]["sports"].update(str(s) for s in sports)
                    all_event_kinds[str(ek)]["places"].add(place)

            tabs_out.append({
                "id": tab_id,
                "title": tab_title,
                "sortOrder": tab_sort,
                "tables": tables_out,
            })

        digest["configs"].append({
            "id": cfg_id,
            "place": place,
            "sports": sports,
            "tabs": tabs_out,
            "specialTables": cfg.get("specialTables") or [],
            "tableInTabGroups": cfg.get("tableInTabGroups") or [],
        })

    # ---- уникальные tables (компактно) ----
    digest["unique_tables"] = [
        {
            "eventKind": v["eventKind"],
            "grid": v["grid"],
            "title": v["title"],
            "sports_count": len(v["sports"]),
            "sports_sample": sorted(v["sports"])[:10],
            "places": sorted(v["places"]),
        }
        for v in sorted(unique_tables.values(),
                        key=lambda x: (str(x["eventKind"]), str(x["grid"])))
    ]

    # ---- eventKind → сводка ----
    digest["event_kinds"] = [
        {
            "eventKind": ek,
            "titles": sorted(v["titles"]),
            "grids": sorted(v["grids"]),
            "sports_count": len(v["sports"]),
            "sports_sample": sorted(v["sports"])[:10],
            "places": sorted(v["places"]),
        }
        for ek, v in sorted(all_event_kinds.items(), key=lambda x: x[0])
    ]

    return digest


def merge_with_factors(digest: dict, factors: dict) -> dict:
    """Прикрепляет примеры fid к каждому eventKind — через таблицы factor_details."""
    # factor_details содержит {fid: {market, outcome, source}}
    # но не содержит eventKind. Значит нужно вытянуть eventKind из связки
    # factor_details.source → таблица.
    # Пока просто сохраняем top-N fid по markets.

    markets: dict = defaultdict(list)
    for fid_str, info in factors.items():
        market = info.get("market") or "?"
        markets[market].append({
            "fid": int(fid_str),
            "outcome": info.get("outcome"),
        })

    digest["factor_markets"] = [
        {
            "market": m,
            "outcomes_count": len(v),
            "outcomes_sample": v[:20],
        }
        for m, v in sorted(markets.items(), key=lambda x: -len(x[1]))[:300]
    ]
    return digest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", required=True)
    ap.add_argument("--factors", required=True)
    ap.add_argument("--out", default="fonbet_digest.json")
    args = ap.parse_args()

    print(f"📂 Reading {args.tables} ...")
    tables = load_json(Path(args.tables))
    print(f"📂 Reading {args.factors} ...")
    factors = load_json(Path(args.factors))

    print("\n🔍 Analyzing tables...")
    digest = analyze_event_view_tables(tables)

    print("🔗 Merging with factors...")
    digest = merge_with_factors(digest, factors)

    out = Path(args.out)
    out.write_text(
        json.dumps(digest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    size_kb = out.stat().st_size / 1024
    print(f"\n💾 {out} ({size_kb:.1f} KB)")

    # ---- сводка ----
    print(f"\n{'=' * 70}")
    print(f"  СВОДКА")
    print(f"{'=' * 70}")
    print(f"  configs:          {len(digest['configs'])}")
    print(f"  unique_tables:    {len(digest.get('unique_tables', []))}")
    print(f"  event_kinds:      {len(digest.get('event_kinds', []))}")
    print(f"  factor_markets:   {len(digest.get('factor_markets', []))}")

    print(f"\n  Уникальные eventKind (первые 30):")
    for ek in digest.get("event_kinds", [])[:30]:
        titles = ", ".join(ek["titles"][:2])
        print(f"    ek={ek['eventKind']:<12} titles=[{titles[:60]}] "
              f"sports={ek['sports_count']}")

    print(f"\n  Топ-20 рынков:")
    for m in digest.get("factor_markets", [])[:20]:
        print(f"    [{m['outcomes_count']:>4}] {m['market'][:60]}")


if __name__ == "__main__":
    main()