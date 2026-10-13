# scripts/sniff_fonbet_catalog_parse.py
"""
Правильный разбор каталогов Fonbet.

Структура (изучено):
  - tables.groups[].tables[].rows[][] — матрица ячеек.
    Ячейки: {"name": "..."} или {"kind": "value", "factorId": N} или
            {"name": "...", "factorId": N}
    Название рынка — первая ячейка строки. Значения — дальше.
  - independentFactors.independentFactors[] — группировка:
    {id: market_id, sports: [...], group1: {factors: [fid,...]}, group2: {...}}
  - sportBasicFactors.configs[] — какие market captions для каких sports.

Запуск:
    python scripts/sniff_fonbet_catalog_parse.py
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path


OUT = Path("sniff_out/catalogs")


def load(name: str) -> dict:
    p = OUT / f"lb54_{name}.json"
    if not p.exists():
        raise FileNotFoundError(p)
    return json.loads(p.read_text(encoding="utf-8"))


# ============================================================
# Разбор tables.groups
# ============================================================
def extract_from_tables(tables: dict) -> dict[int, dict]:
    """
    Извлекает {factor_id: {group, table_num, row_context, col_name}}.
    row_context — название строки (первая ячейка с name, если есть).
    col_name    — название колонки (если ячейка сама имела name).
    """
    result: dict[int, dict] = {}
    groups = tables.get("groups") or []

    for group in groups:
        group_name = group.get("name") or ""
        for t in group.get("tables") or []:
            table_num = t.get("num")
            rows = t.get("rows") or []

            for row_idx, row in enumerate(rows):
                if not isinstance(row, list):
                    continue

                # контекст строки — первая ячейка с "name" (без factorId)
                row_ctx = ""
                for cell in row:
                    if isinstance(cell, dict) and "name" in cell and "factorId" not in cell:
                        row_ctx = cell["name"]
                        break
                if not row_ctx:
                    # fallback: первая ячейка вообще
                    if row and isinstance(row[0], dict):
                        row_ctx = row[0].get("name", "")

                for cell in row:
                    if not isinstance(cell, dict):
                        continue
                    fid = cell.get("factorId")
                    if not isinstance(fid, int):
                        continue
                    name = cell.get("name", "")
                    # если имя не у ячейки — используем контекст строки
                    effective_name = name or row_ctx

                    # сохраняем, если ещё нет, или если новое имя информативнее
                    old = result.get(fid)
                    if old is None or (not old.get("name") and effective_name):
                        result[fid] = {
                            "name": effective_name,
                            "group": group_name,
                            "table_num": table_num,
                            "row_context": row_ctx,
                            "cell_name": name,
                        }

    return result


# ============================================================
# Разбор independentFactors
# ============================================================
def extract_market_grouping(independent: dict) -> tuple[dict[int, list[int]], dict[int, dict]]:
    """
    Возвращает:
      market_to_factors: {market_id: [factor_id, ...]}
      factor_to_market:  {factor_id: {"market_id": ..., "sports": [...], "eventKind": ...}}
    """
    market_to_factors: dict[int, list[int]] = {}
    factor_to_market: dict[int, dict] = {}

    items = independent.get("independentFactors") or []
    for item in items:
        try:
            market_id = int(item.get("id"))
        except (TypeError, ValueError):
            continue
        sports = item.get("sports") or []

        for gkey in ("group1", "group2"):
            grp = item.get(gkey)
            if not isinstance(grp, dict):
                continue
            event_kind = grp.get("eventKind")
            factors = grp.get("factors") or []
            fids = []
            for f in factors:
                try:
                    fids.append(int(f))
                except (TypeError, ValueError):
                    continue
            if fids:
                market_to_factors.setdefault(market_id, []).extend(fids)
                for fid in fids:
                    # первый раз — инициализируем
                    if fid not in factor_to_market:
                        factor_to_market[fid] = {
                            "market_id": market_id,
                            "sports": list(sports),
                            "event_kind": event_kind,
                        }
                    else:
                        # дополняем sports
                        existing = factor_to_market[fid]
                        for s in sports:
                            if s not in existing["sports"]:
                                existing["sports"].append(s)

    return market_to_factors, factor_to_market


# ============================================================
# Разбор sportBasicFactors
# ============================================================
def extract_basic_markets(basic: dict) -> dict[str, list[dict]]:
    """
    {sport_id: [{caption, kinds: [...]}, ...]}
    """
    result: dict[str, list[dict]] = {}
    configs = basic.get("configs") or []
    for cfg in configs:
        sports = cfg.get("sports") or []
        markets = cfg.get("markets") or []
        for sid in sports:
            result.setdefault(str(sid), [])
            for m in markets:
                result[str(sid)].append({
                    "caption": m.get("caption"),
                    "kinds": m.get("marketEventKindsSets"),
                })
    return result


# ============================================================
# Сводка
# ============================================================
def main():
    print("📂 Загружаем каталоги...")
    tables = load("tables")
    independent = load("independentFactors")
    basic = load("sportBasicFactors")

    print("\n🔍 Разбор tables ...")
    fid_names = extract_from_tables(tables)
    print(f"  Найдено пар factorId → name: {len(fid_names)}")

    print("\n🔍 Разбор independentFactors ...")
    market_to_factors, factor_to_market = extract_market_grouping(independent)
    print(f"  Маркетов: {len(market_to_factors)}")
    print(f"  Уникальных factor_id в маркетах: {len(factor_to_market)}")

    print("\n🔍 Разбор sportBasicFactors ...")
    basic_markets = extract_basic_markets(basic)
    print(f"  Спортов с настройками: {len(basic_markets)}")

    # ---- сводный маппинг ----
    print(f"\n{'=' * 70}")
    print(f"  СВОДНЫЙ МАППИНГ factor_id")
    print(f"{'=' * 70}")

    merged: dict[int, dict] = {}
    all_fids = set(fid_names) | set(factor_to_market)

    for fid in all_fids:
        entry = {}
        # 1. имя из tables
        if fid in fid_names:
            entry["name"] = fid_names[fid]["name"]
            entry["group"] = fid_names[fid]["group"]
        # 2. маркет-принадлежность
        if fid in factor_to_market:
            m = factor_to_market[fid]
            entry["market_id"] = m["market_id"]
            entry["sports"] = m["sports"]
            entry["event_kind"] = m["event_kind"]
        merged[fid] = entry

    # только те, у кого есть имя
    with_name = {fid: e for fid, e in merged.items() if e.get("name")}
    print(f"  Всего factor_id: {len(merged)}")
    print(f"  С человеческим именем: {len(with_name)}")
    print(f"  С привязкой к маркету: {len([1 for e in merged.values() if 'market_id' in e])}")

    # ---- показываем топ базовых ----
    print(f"\n  Базовые рынки (с именем):")
    for fid in sorted(with_name.keys())[:60]:
        e = with_name[fid]
        print(f"    f={fid:<8} [{e.get('group','')[:30]:<30}] {e['name'][:60]}")

    # ---- группировка по groups ----
    print(f"\n{'=' * 70}")
    print(f"  ГРУППЫ РЫНКОВ (из tables)")
    print(f"{'=' * 70}")
    by_group: dict[str, list[int]] = defaultdict(list)
    for fid, e in with_name.items():
        by_group[e.get("group", "unknown")].append(fid)
    for group, fids in sorted(by_group.items(), key=lambda x: -len(x[1])):
        print(f"  {group:<50} {len(fids):<5} факторов")

    # ---- сохраняем всё ----
    out = {
        "factor_names": {str(fid): with_name[fid]["name"] for fid in with_name},
        "factor_full": {str(fid): e for fid, e in merged.items()},
        "market_to_factors": {str(m): f for m, f in market_to_factors.items()},
        "basic_markets_by_sport": basic_markets,
    }
    path = OUT / "fonbet_catalog_parsed.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True),
                    encoding="utf-8")
    print(f"\n  💾 {path}")

    # ---- конкретно: как выглядят те самые fid из футбола ----
    print(f"\n{'=' * 70}")
    print(f"  ТЕСТ: fid из футбольного матча (Бавария vs Лейпциг)")
    print(f"{'=' * 70}")
    test_fids = [921, 922, 923, 924, 925, 927, 928, 930, 931, 974, 976, 978, 980,
                 989, 991, 1809, 1810, 1812, 1813, 4241, 4242, 1672, 1675]
    for fid in test_fids:
        e = merged.get(fid)
        if not e:
            print(f"    f={fid:<6} ❌ нет данных")
            continue
        name = e.get("name", "❓")
        grp = e.get("group", "")
        mid = e.get("market_id", "")
        print(f"    f={fid:<6} m={mid:<5} [{grp[:25]:<25}] {name[:60]}")


if __name__ == "__main__":
    main()