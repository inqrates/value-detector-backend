# scripts/sniff_fonbet_canon.py
"""
Финальный сборщик канона Fonbet.

Собирает из sportBasicFactors + tables + independentFactors полную
карту: каждый factor_id → {canon_type, side, market_caption, sports, ...}.

canon_type — из поля 'tag' (result, double_chances, handicaps, totals, ...)
side       — из columns[].caption (1, X, 2, ФОРА 1, Тотал, Да, Нет, ...)
line_kind  — из columnType (1=fix, 2=handicap, 3=total)
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path


OUT = Path("sniff_out/catalogs")


def load(name: str) -> dict:
    return json.loads((OUT / f"lb54_{name}.json").read_text(encoding="utf-8"))


# ============================================================
# Разбор sportBasicFactors → канон
# ============================================================
def build_canon(basic: dict) -> dict[int, dict]:
    """
    Проходим по всем configs. Для каждого market и каждой колонки:
      column.factors[] → canon-запись.

    Если factor_id встречается несколько раз (в разных видах спорта) —
    мерджим sports.
    """
    canon: dict[int, dict] = {}

    for cfg in basic.get("configs") or []:
        cfg_id = cfg.get("id")
        sports = [str(s) for s in (cfg.get("sports") or [])]

        for market in cfg.get("markets") or []:
            caption = market.get("caption") or ""
            tag = market.get("tag") or ""
            alias = market.get("alias") or ""
            priority = market.get("priority")

            for kind_set in market.get("marketEventKindsSets") or []:
                if not isinstance(kind_set, dict):
                    continue
                event_kind = kind_set.get("eventKind")  # может быть None
                for fset in kind_set.get("factorSets") or []:
                    if not isinstance(fset, dict):
                        continue
                    for col in fset.get("columns") or []:
                        if not isinstance(col, dict):
                            continue
                        col_caption = col.get("caption") or ""
                        col_type = col.get("columnType")
                        factors = col.get("factors") or []
                        compact = set(str(f) for f in (col.get("compactLineFactors") or []))

                        for f in factors:
                            try:
                                fid = int(f)
                            except (TypeError, ValueError):
                                continue

                            entry = canon.get(fid)
                            if entry is None:
                                entry = {
                                    "canon_type": tag,
                                    "side": col_caption,
                                    "line_kind": col_type,
                                    "market_caption": caption,
                                    "alias": alias,
                                    "priority": priority,
                                    "sports": [],
                                    "event_kinds": [],
                                    "compact": fid in compact,
                                    "config_id": cfg_id,
                                }
                                canon[fid] = entry
                            else:
                                # дополняем sports
                                for s in sports:
                                    if s not in entry["sports"]:
                                        entry["sports"].append(s)
                                if event_kind is not None and event_kind not in entry["event_kinds"]:
                                    entry["event_kinds"].append(event_kind)
                                # помечаем compact если хотя бы где-то был compact
                                if fid in compact:
                                    entry["compact"] = True

    # финальный проход — долить sports/event_kinds из последнего cfg
    for cfg in basic.get("configs") or []:
        sports = [str(s) for s in (cfg.get("sports") or [])]
        for market in cfg.get("markets") or []:
            for kind_set in market.get("marketEventKindsSets") or []:
                event_kind = kind_set.get("eventKind")
                for fset in kind_set.get("factorSets") or []:
                    for col in fset.get("columns") or []:
                        for f in col.get("factors") or []:
                            try:
                                fid = int(f)
                            except (TypeError, ValueError):
                                continue
                            entry = canon.get(fid)
                            if not entry:
                                continue
                            for s in sports:
                                if s not in entry["sports"]:
                                    entry["sports"].append(s)
                            if event_kind is not None and event_kind not in entry["event_kinds"]:
                                entry["event_kinds"].append(event_kind)

    return canon


# ============================================================
# Мерджим с names из tables (для редких рынков)
# ============================================================
def extract_tables_names(tables: dict) -> dict[int, dict]:
    result = {}
    for group in tables.get("groups") or []:
        gname = group.get("name") or ""
        for t in group.get("tables") or []:
            for row in t.get("rows") or []:
                if not isinstance(row, list):
                    continue
                row_ctx = ""
                for cell in row:
                    if isinstance(cell, dict) and "name" in cell and "factorId" not in cell:
                        row_ctx = cell["name"]
                        break
                for cell in row:
                    if not isinstance(cell, dict):
                        continue
                    fid = cell.get("factorId")
                    if not isinstance(fid, int):
                        continue
                    nm = cell.get("name", "") or row_ctx
                    if fid not in result or (not result[fid].get("name") and nm):
                        result[fid] = {"name": nm, "tables_group": gname}
    return result


# ============================================================
# Мерджим с market_id из independentFactors
# ============================================================
def extract_market_ids(indep: dict) -> dict[int, int]:
    result = {}
    for item in indep.get("independentFactors") or []:
        try:
            mid = int(item.get("id"))
        except (TypeError, ValueError):
            continue
        for gkey in ("group1", "group2"):
            grp = item.get(gkey)
            if not isinstance(grp, dict):
                continue
            for f in grp.get("factors") or []:
                try:
                    fid = int(f)
                except (TypeError, ValueError):
                    continue
                result.setdefault(fid, mid)
    return result


# ============================================================
# Финальная сборка
# ============================================================
def main():
    print("📂 Загружаем каталоги...")
    basic = load("sportBasicFactors")
    tables = load("tables")
    indep = load("independentFactors")

    print("\n🔧 Строим канон из sportBasicFactors...")
    canon = build_canon(basic)
    print(f"  factor_id в каноне: {len(canon)}")

    print("\n🔧 Достаём имена из tables (редкие рынки)...")
    tbl_names = extract_tables_names(tables)
    print(f"  factor_id с именем:  {len(tbl_names)}")

    print("\n🔧 Достаём market_id из independentFactors...")
    market_ids = extract_market_ids(indep)
    print(f"  factor_id с market:  {len(market_ids)}")

    # ---- слияние ----
    all_fids = set(canon) | set(tbl_names) | set(market_ids)
    merged: dict[int, dict] = {}

    for fid in all_fids:
        entry = {}
        if fid in canon:
            entry.update(canon[fid])
        if fid in tbl_names:
            entry["name"] = tbl_names[fid]["name"]
            entry["tables_group"] = tbl_names[fid]["tables_group"]
        if fid in market_ids:
            entry["market_id"] = market_ids[fid]
        merged[fid] = entry

    # ---- статистика ----
    with_canon = sum(1 for e in merged.values() if e.get("canon_type"))
    with_name = sum(1 for e in merged.values() if e.get("name"))
    with_market = sum(1 for e in merged.values() if e.get("market_id"))
    full = sum(1 for e in merged.values()
               if e.get("canon_type") or e.get("name"))

    print(f"\n{'=' * 70}")
    print(f"  ПОКРЫТИЕ")
    print(f"{'=' * 70}")
    print(f"  Всего factor_id:              {len(merged)}")
    print(f"  С canon_type (tag):           {with_canon}")
    print(f"  С name (tables):              {with_name}")
    print(f"  С market_id:                  {with_market}")
    print(f"  Хотя бы одно (canon или name):{full}")

    # ---- распределение по canon_type ----
    print(f"\n{'=' * 70}")
    print(f"  CANON-ТИПЫ (поле tag)")
    print(f"{'=' * 70}")
    by_tag = defaultdict(list)
    for fid, e in merged.items():
        if e.get("canon_type"):
            by_tag[e["canon_type"]].append(fid)
    for tag, fids in sorted(by_tag.items(), key=lambda x: -len(x[1])):
        print(f"  {tag:<40} {len(fids):<6} факторов")

    # ---- распределение по market_caption ----
    print(f"\n{'=' * 70}")
    print(f"  MARKET_CAPTION (топ-25)")
    print(f"{'=' * 70}")
    by_cap = defaultdict(list)
    for fid, e in merged.items():
        if e.get("market_caption"):
            by_cap[e["market_caption"]].append(fid)
    for cap, fids in sorted(by_cap.items(), key=lambda x: -len(x[1]))[:25]:
        print(f"  {cap[:55]:<55} {len(fids):<5}")

    # ---- тест на баварском матче ----
    print(f"\n{'=' * 70}")
    print(f"  ТЕСТ: fid из Бавария vs Лейпциг")
    print(f"{'=' * 70}")
    test_fids = [921, 922, 923, 924, 925, 927, 928, 930, 931,
                 974, 976, 978, 980, 989, 991,
                 1809, 1810, 1812, 1813,
                 4241, 4242, 1672, 1675, 587, 3090, 3091]
    for fid in test_fids:
        e = merged.get(fid, {})
        tag = e.get("canon_type", "")[:18]
        side = e.get("side", "")[:15]
        cap = e.get("market_caption", "")[:20]
        nm = e.get("name", "")[:25]
        print(f"    f={fid:<6} [{tag:<18}] [{side:<15}] {cap:<20} | {nm}")

    # ---- распределение по видам спорта ----
    print(f"\n{'=' * 70}")
    print(f"  РЫНКИ ПО ВИДАМ СПОРТА")
    print(f"{'=' * 70}")
    sport_names = {
        "1": "Футбол", "2": "Хоккей", "3": "Баскетбол", "4": "Теннис",
        "9": "Волейбол", "3088": "Настольный теннис", "1434": "Футзал",
        "29086": "Киберспорт", "8": "Гандбол",
    }
    sport_fids = defaultdict(set)
    for fid, e in merged.items():
        for s in (e.get("sports") or []):
            sport_fids[s].add(fid)
    for sid, fids in sorted(sport_fids.items(), key=lambda x: -len(x[1]))[:15]:
        name = sport_names.get(sid, f"sport_{sid}")
        print(f"  {name:<25} {len(fids):<5} рынков")

    # ---- сохраняем всё ----
    out = {
        "factor_canon": {
            str(fid): {
                k: v for k, v in e.items()
                if k in ("canon_type", "side", "line_kind", "market_caption",
                         "alias", "priority", "sports", "event_kinds",
                         "compact", "name", "tables_group", "market_id")
            }
            for fid, e in merged.items()
        },
        "stats": {
            "total": len(merged),
            "with_canon": with_canon,
            "with_name": with_name,
            "with_market": with_market,
        },
    }
    path = OUT / "fonbet_canon.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True),
                    encoding="utf-8")
    print(f"\n  💾 {path}")
    print(f"  Размер: {path.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()