# scripts/analyze_linebet_tables.py
"""
Linebet (XBet): анализ eventViewTables → компактный digest.

Структура raw (по предоставленному примеру):
  raw["short"][sport_id][category][group_id]["M"] = {
     fid: {"N": name, "T": type, "G": group_id, "ST": status}
  }
  group has: N (name), C (count), GN (group names)

Запуск:
  python scripts/analyze_linebet_tables.py \
      --tables linebet_raw_eventViewTables.json \
      --factors linebet_factor_details.json \
      --out linebet_digest.json
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


# Известные спортивные ID (можно расширять)
SPORT_NAMES = {
    1:  "football",
    2:  "hockey",
    3:  "basketball",
    4:  "tennis",
    9:  "volleyball",
    10: "table_tennis",
    11: "futsal",
}


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def walk_linebet_short(short: dict) -> dict:
    """Обходит дерево raw['short'] и извлекает уникальные рынки."""
    result = {
        "sports": {},
        "unique_markets": {},          # (sport, market_name) → sample
        "unique_groups": {},           # (sport, group_id) → sample
        "outcome_names_by_sport": defaultdict(set),
    }

    for sport_id_str, sport_tree in short.items():
        try:
            sport_id = int(sport_id_str)
        except ValueError:
            sport_id = sport_id_str
        sport_name = SPORT_NAMES.get(sport_id, f"sport_{sport_id}")

        result["sports"][sport_name] = {
            "sport_id": sport_id,
            "categories_count": len(sport_tree),
        }

        for cat_key, cat_tree in sport_tree.items():
            for group_id_str, group_tree in cat_tree.items():
                market_name = group_tree.get("N") or ""
                group_name_map = group_tree.get("GN") or {}
                group_id = None
                if isinstance(group_name_map, dict) and group_name_map:
                    group_id = list(group_name_map.keys())[0]
                    group_name = group_name_map[group_id]
                else:
                    group_name = ""

                markets_in_group = group_tree.get("M") or {}
                for fid_str, m in markets_in_group.items():
                    outcome_name = m.get("N") or ""
                    result["outcome_names_by_sport"][sport_name].add(outcome_name)

                    key = (sport_name, market_name)
                    if key not in result["unique_markets"]:
                        result["unique_markets"][key] = {
                            "sport": sport_name,
                            "market": market_name,
                            "group_id": group_id,
                            "group": group_name,
                            "sample_fids": [],
                            "outcomes_count": 0,
                        }
                    entry = result["unique_markets"][key]
                    entry["outcomes_count"] += 1
                    if len(entry["sample_fids"]) < 10:
                        entry["sample_fids"].append({
                            "fid": int(fid_str) if fid_str.isdigit() else fid_str,
                            "outcome": outcome_name,
                        })

                    gkey = (sport_name, group_id, group_name)
                    if gkey not in result["unique_groups"]:
                        result["unique_groups"][gkey] = {
                            "sport": sport_name,
                            "group_id": group_id,
                            "group": group_name,
                            "markets_count": 0,
                        }
                    result["unique_groups"][gkey]["markets_count"] += 1

    # sets → lists
    result["outcome_names_by_sport"] = {
        k: sorted(v) for k, v in result["outcome_names_by_sport"].items()
    }
    return result


def merge_with_factors(digest: dict, factors: dict) -> dict:
    """Из factor_details делаем сводку markets → outcomes."""
    markets: dict = defaultdict(list)
    groups: dict = defaultdict(int)
    for fid_str, info in factors.items():
        market = info.get("market") or "?"
        group = info.get("group") or "?"
        markets[market].append({
            "fid": int(fid_str) if fid_str.isdigit() else fid_str,
            "outcome": info.get("outcome"),
            "group": group,
        })
        groups[group] += 1

    digest["factor_markets"] = [
        {
            "market": m,
            "outcomes_count": len(v),
            "outcomes_sample": v[:20],
        }
        for m, v in sorted(markets.items(), key=lambda x: -len(x[1]))[:500]
    ]
    digest["factor_groups"] = [
        {"group": g, "fid_count": c}
        for g, c in sorted(groups.items(), key=lambda x: -x[1])
    ]
    return digest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", required=True)
    ap.add_argument("--factors", required=True)
    ap.add_argument("--out", default="linebet_digest.json")
    args = ap.parse_args()

    print(f"📂 Reading {args.tables} ...")
    raw = load_json(Path(args.tables))
    print(f"📂 Reading {args.factors} ...")
    factors = load_json(Path(args.factors))

    print("\n🔍 Analyzing tables...")
    short = raw.get("short") or {}
    if not short:
        # может быть "long" или другой ключ
        print(f"  ⚠️  Ключа 'short' нет. Доступные ключи: {list(raw.keys())[:10]}")
        for key in ("long", "tables", "data"):
            if key in raw:
                short = raw[key]
                print(f"  Использую ключ {key!r}")
                break

    digest = walk_linebet_short(short)

    print("🔗 Merging with factors...")
    digest = merge_with_factors(digest, factors)

    # Сжимаем всё в JSON-сериализуемое
    digest["unique_markets"] = [
        {"sport": k[0], "market": k[1], **v}
        for k, v in list(digest["unique_markets"].items())[:1000]
    ]
    digest["unique_groups"] = [
        {"sport": k[0], "group_id": k[1], "group": k[2], **v}
        for k, v in digest["unique_groups"].items()
    ]

    out = Path(args.out)
    out.write_text(
        json.dumps(digest, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\n💾 {out} ({out.stat().st_size / 1024:.1f} KB)")

    # ---- сводка ----
    print(f"\n{'=' * 70}")
    print(f"  СВОДКА")
    print(f"{'=' * 70}")
    print(f"  Спортов:           {len(digest['sports'])}")
    print(f"  Уникальных рынков: {len(digest['unique_markets'])}")
    print(f"  Уникальных групп:  {len(digest['unique_groups'])}")
    print(f"  factor_markets:    {len(digest.get('factor_markets', []))}")
    print(f"  factor_groups:     {len(digest.get('factor_groups', []))}")

    print(f"\n  Топ-25 групп (по числу fid):")
    for g in digest.get("factor_groups", [])[:25]:
        print(f"    [{g['fid_count']:>5}] {g['group'][:60]}")

    print(f"\n  Топ-20 рынков:")
    for m in digest.get("factor_markets", [])[:20]:
        print(f"    [{m['outcomes_count']:>5}] {m['market'][:60]}")


if __name__ == "__main__":
    main()