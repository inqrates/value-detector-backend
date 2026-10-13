# scripts/explore_basic.py
"""Показывает точную структуру sportBasicFactors и tables для базовых рынков."""
import json
from pathlib import Path

OUT = Path("sniff_out/catalogs")

basic = json.loads((OUT / "lb54_sportBasicFactors.json").read_text(encoding="utf-8"))
configs = basic.get("configs") or []

print(f"Всего configs: {len(configs)}")

# ---- первый config целиком ----
print(f"\n{'=' * 70}")
print(f"  CONFIG[0] — ПОЛНАЯ СТРУКТУРА")
print(f"{'=' * 70}")
cfg0 = configs[0]
print(f"  id={cfg0.get('id')}  systemType={cfg0.get('systemType')}")
print(f"  sports: {cfg0.get('sports')}")
markets = cfg0.get("markets") or []
print(f"  markets count: {len(markets)}")
for i, m in enumerate(markets[:3]):
    print(f"\n  market[{i}]:")
    print(f"    keys: {list(m.keys())}")
    print(f"    caption: {m.get('caption')!r}")
    print(f"    вся нода (обрез): {json.dumps(m, ensure_ascii=False)[:1500]}")

# ---- config с футболом ----
print(f"\n{'=' * 70}")
print(f"  CONFIG С ФУТБОЛОМ (sports содержит '1')")
print(f"{'=' * 70}")
for cfg in configs:
    sports = cfg.get("sports") or []
    if "1" in sports:
        print(f"  id={cfg.get('id')}  sports={sports[:8]}")
        for i, m in enumerate((cfg.get("markets") or [])[:5]):
            print(f"    market[{i}] caption={m.get('caption')!r}")
            kinds = m.get("marketEventKindsSets")
            if isinstance(kinds, list):
                for j, k in enumerate(kinds[:2]):
                    print(f"      set[{j}]: {json.dumps(k, ensure_ascii=False)[:400]}")
        break

# ---- tables — как выглядит "Основные ставки" ----
print(f"\n{'=' * 70}")
print(f"  TABLES — группа 'Основные ставки'")
print(f"{'=' * 70}")
tables = json.loads((OUT / "lb54_tables.json").read_text(encoding="utf-8"))
for group in tables.get("groups") or []:
    if group.get("name") != "Основные ставки":
        continue
    ts = group.get("tables") or []
    print(f"  tables в группе: {len(ts)}")
    for t in ts[:3]:
        print(f"\n  table num={t.get('num')}  keys={list(t.keys())}")
        rows = t.get("rows") or []
        print(f"    rows count: {len(rows)}")
        for i, row in enumerate(rows[:3]):
            print(f"    row[{i}]: {json.dumps(row, ensure_ascii=False)[:600]}")
    break