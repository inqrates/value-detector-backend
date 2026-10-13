# scripts/sniff_fonbet_catalog_final.py
"""
Финальный сборщик маппинга Fonbet: сшивает 3 каталога.

Цепочка:
    factor_id ──independentFactors──▶ market_id
    market_id ──sportBasicFactors──▶ caption ("Исходы", "Тотал", ...)
    factor_id ──tables─────────────▶ name (для редких рынков)

На выходе: полный маппинг с именами, группами, market_caption, sports.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path


OUT = Path("sniff_out/catalogs")


def load(name: str) -> dict:
    return json.loads((OUT / f"lb54_{name}.json").read_text(encoding="utf-8"))


# ============================================================
# 1. factor_id → market_id  (independentFactors)
# ============================================================
def build_fid_to_market(indep: dict) -> dict[int, dict]:
    result: dict[int, dict] = {}
    for item in indep.get("independentFactors") or []:
        try:
            mid = int(item.get("id"))
        except (TypeError, ValueError):
            continue
        sports = item.get("sports") or []
        for gkey in ("group1", "group2"):
            grp = item.get(gkey)
            if not isinstance(grp, dict):
                continue
            for f in grp.get("factors") or []:
                try:
                    fid = int(f)
                except (TypeError, ValueError):
                    continue
                prev = result.get(fid)
                if prev is None:
                    result[fid] = {
                        "market_id": mid,
                        "sports": list(sports),
                        "event_kind": grp.get("eventKind"),
                    }
                else:
                    for s in sports:
                        if s not in prev["sports"]:
                            prev["sports"].append(s)
    return result


# ============================================================
# 2. market_id → caption + sports  (sportBasicFactors)
# ============================================================
def build_market_caption(basic: dict) -> dict[int, dict]:
    """
    Ищем в sportBasicFactors связи market_id → caption.
    Структура configs[].markets[].marketEventKindsSets может содержать market_id,
    либо у configs[].id — это market_id и есть (проверим оба варианта).
    """
    result: dict[int, dict] = {}

    for cfg in basic.get("configs") or []:
        sports = cfg.get("sports") or []
        cfg_id = cfg.get("id")
        markets = cfg.get("markets") or []

        for m in markets:
            caption = m.get("caption") or ""
            # копаем marketEventKindsSets
            for s in (m.get("marketEventKindsSets") or []):
                if not isinstance(s, dict):
                    continue
                # ищем market ids разными способами
                for key in ("marketIds", "marketId", "ids", "id"):
                    val = s.get(key)
                    if val is None:
                        continue
                    ids = val if isinstance(val, list) else [val]
                    for v in ids:
                        try:
                            mid = int(v)
                        except (TypeError, ValueError):
                            continue
                        entry = result.setdefault(mid, {
                            "caption": caption,
                            "sports": list(sports),
                            "config_id": cfg_id,
                        })
                        for sp in sports:
                            if sp not in entry["sports"]:
                                entry["sports"].append(sp)

    return result


# ============================================================
# 3. factor_id → name (tables, как было)
# ============================================================
def extract_from_tables(tables: dict) -> dict[int, dict]:
    result: dict[int, dict] = {}
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
                    name = cell.get("name", "") or row_ctx
                    old = result.get(fid)
                    if old is None or (not old.get("name") and name):
                        result[fid] = {"name": name, "group": gname}
    return result


# ============================================================
# 4. Сшивка
# ============================================================
def main():
    print("📂 Загружаем каталоги...")
    tables_d = load("tables")
    indep_d = load("independentFactors")
    basic_d = load("sportBasicFactors")

    print("\n🔗 Строим связи...")
    fid_to_market = build_fid_to_market(indep_d)
    market_caption = build_market_caption(basic_d)
    fid_names = extract_from_tables(tables_d)

    print(f"  factor_id → market_id: {len(fid_to_market)}")
    print(f"  market_id → caption:   {len(market_caption)}")
    print(f"  factor_id → name:      {len(fid_names)}")

    # проверим: сколько базовых market_id покрыто captions
    base_market_ids = {474, 475, 584, 14, 846}
    print(f"\n  Проверка базовых market_id:")
    for mid in sorted(base_market_ids):
        cap = market_caption.get(mid)
        print(f"    m={mid:<6} → {cap['caption'] if cap else '❌ нет caption'}")

    # ---- сборка финального маппинга ----
    all_fids = set(fid_to_market) | set(fid_names)
    merged: dict[int, dict] = {}

    for fid in all_fids:
        entry = {}
        if fid in fid_to_market:
            entry.update(fid_to_market[fid])
            mid = entry.get("market_id")
            if mid in market_caption:
                entry["market_caption"] = market_caption[mid]["caption"]
                entry["market_sports"] = market_caption[mid]["sports"]
        if fid in fid_names:
            entry["name"] = fid_names[fid]["name"]
            entry["group"] = fid_names[fid]["group"]
        merged[fid] = entry

    # ---- статистика ----
    with_name = sum(1 for e in merged.values() if e.get("name"))
    with_caption = sum(1 for e in merged.values() if e.get("market_caption"))
    with_either = sum(1 for e in merged.values()
                      if e.get("name") or e.get("market_caption"))
    with_both = sum(1 for e in merged.values()
                    if e.get("name") and e.get("market_caption"))

    print(f"\n{'=' * 70}")
    print(f"  ПОКРЫТИЕ")
    print(f"{'=' * 70}")
    print(f"  Всего factor_id:              {len(merged)}")
    print(f"  С именем из tables:           {with_name}")
    print(f"  С caption из basic:           {with_caption}")
    print(f"  Хотя бы одно:                 {with_either}")
    print(f"  Оба (name + caption):         {with_both}")

    # ---- тест на футбольном матче ----
    print(f"\n{'=' * 70}")
    print(f"  ТЕСТ: fid из Бавария vs Лейпциг (749 факторов)")
    print(f"{'=' * 70}")
    test_fids = [921, 922, 923, 924, 925, 927, 928, 930, 931,
                 974, 976, 978, 980, 989, 991,
                 1809, 1810, 1812, 1813,
                 4241, 4242, 1672, 1675,
                 587, 698, 699, 890, 891,
                 3090, 3091, 3093, 3094]
    for fid in test_fids:
        e = merged.get(fid, {})
        mid = e.get("market_id", "?")
        cap = e.get("market_caption", "") or ""
        nm = e.get("name", "") or ""
        grp = e.get("group", "") or ""
        display = cap or nm or "❓"
        print(f"    f={fid:<6} m={str(mid):<5} [{grp[:22]:<22}] {display[:55]}")

    # ---- группировка ----
    print(f"\n{'=' * 70}")
    print(f"  ЧТО ЕСТЬ ПО ГРУППАМ")
    print(f"{'=' * 70}")
    by_group = defaultdict(list)
    for fid, e in merged.items():
        g = e.get("group", "—")
        by_group[g].append(fid)
    for g, fids in sorted(by_group.items(), key=lambda x: -len(x[1])):
        print(f"  {g:<40} {len(fids):<6} факторов")

    # ---- какие sports у базовых рынков ----
    print(f"\n{'=' * 70}")
    print(f"  РЫНКИ ПО ВИДАМ СПОРТА (топ)")
    print(f"{'=' * 70}")
    fid_sports = defaultdict(set)
    for fid, e in merged.items():
        for s in (e.get("market_sports") or []) + (e.get("sports") or []):
            fid_sports[str(s)].add(fid)
    for sid, fids in sorted(fid_sports.items(), key=lambda x: -len(x[1]))[:15]:
        print(f"  sport={sid:<10} {len(fids):<6} рынков")

    # ---- сохраняем ----
    out = {
        "factor_names": {str(fid): e["name"] for fid, e in merged.items() if e.get("name")},
        "market_captions": {str(mid): c for mid, c in market_caption.items()},
        "factor_full": {str(fid): e for fid, e in merged.items()},
    }
    path = OUT / "fonbet_final_mapping.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True),
                    encoding="utf-8")
    print(f"\n  💾 {path}")


if __name__ == "__main__":
    main()