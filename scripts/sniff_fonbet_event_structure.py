# scripts/sniff_fonbet_event_structure.py
"""
Полная структура ответа /ma/events/event — ищем ВСЕ поля,
которые могут разбивать факторы по периодам.

Показывает:
  1. Все ключи верхнего уровня ответа
  2. Все ключи внутри customFactors[i]
  3. Все ключи внутри customFactors[i].factors[j] (уникальные по всему ответу)
  4. Разные варианты группировки фактора: eventKind, kind, period, stage, phase, ...
  5. Если у факторов есть какой-то "родитель"/"группа" — показываем это дерево

Запуск:
    python scripts/sniff_fonbet_event_structure.py
    python scripts/sniff_fonbet_event_structure.py --event 68291132
"""
from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict, Counter
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


async def fetch_event(event_id: int) -> dict:
    async with AsyncSession(impersonate=IMPERSONATE, timeout=30, headers=HEADERS) as s:
        r = await s.get(
            EVENT_URL, headers=HEADERS,
            params={"lang": "ru", "version": "0",
                    "eventId": str(event_id), "scopeMarket": "1600"},
        )
        r.raise_for_status()
        return r.json()


def dump_keys(obj, prefix="", max_depth=3, depth=0):
    """Рекурсивно печатает структуру dict — только пути и типы."""
    if depth > max_depth:
        return
    if isinstance(obj, dict):
        for k in list(obj.keys())[:40]:
            v = obj[k]
            t = type(v).__name__
            if isinstance(v, (list, dict)):
                size = len(v)
                print(f"  {prefix}{k}: {t}[{size}]")
                if isinstance(v, list) and v and isinstance(v[0], (dict, list)):
                    print(f"  {prefix}  └─ [0] →")
                    dump_keys(v[0], prefix + "      ", max_depth, depth + 1)
                elif isinstance(v, dict):
                    dump_keys(v, prefix + "    ", max_depth, depth + 1)
            else:
                sample = str(v)[:60]
                print(f"  {prefix}{k}: {t} = {sample}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", type=int, default=68291132)
    args = ap.parse_args()

    print(f"🌐 Тянем eventId={args.event}...")
    data = await fetch_event(args.event)

    print(f"\n{'=' * 70}")
    print(f"  ВЕРХНИЙ УРОВЕНЬ ОТВЕТА")
    print(f"{'=' * 70}")
    dump_keys(data, max_depth=2)

    # ---- все уникальные ключи внутри factors ----
    print(f"\n{'=' * 70}")
    print(f"  КЛЮЧИ ВНУТРИ customFactors[].factors[] — УНИКАЛЬНЫЕ")
    print(f"{'=' * 70}")
    factor_keys = Counter()
    factor_sample = {}
    all_factors = []
    for cf in data.get("customFactors") or []:
        for f in cf.get("factors") or []:
            all_factors.append((cf.get("e"), f))
            for k in f.keys():
                factor_keys[k] += 1
                if k not in factor_sample:
                    factor_sample[k] = f
    print(f"  Всего factors: {len(all_factors)}")
    print(f"  Уникальные ключи в factor:")
    for k, cnt in factor_keys.most_common():
        sample = factor_sample[k].get(k)
        print(f"    {k:<15} → встречается {cnt:>6} раз, пример: {str(sample)[:50]}")

    # ---- все уникальные ключи внутри customFactors[i] ----
    print(f"\n{'=' * 70}")
    print(f"  КЛЮЧИ ВНУТРИ customFactors[i] — УНИКАЛЬНЫЕ")
    print(f"{'=' * 70}")
    cf_keys = Counter()
    cf_sample = {}
    for cf in data.get("customFactors") or []:
        for k in cf.keys():
            cf_keys[k] += 1
            if k not in cf_sample:
                cf_sample[k] = cf
    for k, cnt in cf_keys.most_common():
        sample = cf_sample[k].get(k)
        if isinstance(sample, list):
            sample_str = f"list[{len(sample)}]"
        elif isinstance(sample, dict):
            sample_str = f"dict с ключами {list(sample.keys())[:5]}"
        else:
            sample_str = str(sample)[:60]
        print(f"    {k:<15} → встречается {cnt:>6} раз, пример: {sample_str}")

    # ---- все ключи верхнего уровня в events[] ----
    print(f"\n{'=' * 70}")
    print(f"  КЛЮЧИ ВНУТРИ events[i] — УНИКАЛЬНЫЕ")
    print(f"{'=' * 70}")
    ev_keys = Counter()
    ev_sample = {}
    for ev in data.get("events") or []:
        for k in ev.keys():
            ev_keys[k] += 1
            if k not in ev_sample:
                ev_sample[k] = ev
    for k, cnt in ev_keys.most_common():
        sample = ev_sample[k].get(k)
        print(f"    {k:<20} → встречается {cnt:>6} раз, пример: {str(sample)[:60]}")

    # ---- ищем все поля, где встречается "kind" или "period" ----
    print(f"\n{'=' * 70}")
    print(f"  ПОИСК 'kind', 'period', 'event_kind', 'phase' ВО ВСЕХ ПОЛЯХ")
    print(f"{'=' * 70}")
    field_names = set()
    for k in factor_keys:
        field_names.add(f"customFactors[].factors[].{k}")
    for k in cf_keys:
        field_names.add(f"customFactors[].{k}")
    for k in ev_keys:
        field_names.add(f"events[].{k}")
    for k in data.keys():
        field_names.add(f"<root>.{k}")

    matches = [f for f in field_names if any(s in f.lower()
                                              for s in ("kind", "period", "phase", "stage", "half"))]
    if matches:
        for m in matches:
            print(f"    🔸 {m}")
    else:
        print(f"    ❌ Нет полей с 'kind'/'period'/'phase'")
        print(f"    Все доступные поля:")
        for f in sorted(field_names):
            print(f"      {f}")

    # ---- разбор eventKind если он есть где-то ----
    print(f"\n{'=' * 70}")
    print(f"  АНАЛИЗ: если есть eventKind — сколько уникальных значений")
    print(f"{'=' * 70}")
    # пробуем найти eventKind или kind во всех местах
    kind_values = Counter()
    for eid, f in all_factors:
        for key in ("eventKind", "kind", "period", "phase"):
            if key in f:
                kind_values[(key, f[key])] += 1
    for cf in data.get("customFactors") or []:
        for key in ("eventKind", "kind", "period", "phase"):
            if key in cf:
                kind_values[("cf." + key, cf[key])] += 1
    for ev in data.get("events") or []:
        for key in ("eventKind", "kind", "period", "phase"):
            if key in ev:
                kind_values[("event." + key, ev[key])] += 1

    if kind_values:
        for (key, val), cnt in kind_values.most_common(30):
            print(f"    {key}={val}  → {cnt} раз")
    else:
        print(f"    ❌ Не найдено ни в одном месте")

    # ---- сохраним для ручного разбора ----
    path = OUT / f"event_structure_{args.event}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    print(f"\n💾 Полный дамп: {path}")
    print(f"   Размер: {path.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    asyncio.run(main())
    