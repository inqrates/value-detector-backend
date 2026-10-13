# scripts/sniff_fonbet_catalogs.py
"""
Скачивает все каталоги Fonbet (расшифровка рынков) и анализирует структуру.

Запуск:
    python scripts/sniff_fonbet_catalogs.py
    python scripts/sniff_fonbet_catalogs.py --cluster vk  # второй кластер
"""
from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter, defaultdict
from pathlib import Path

from curl_cffi.requests import AsyncSession


CLUSTERS = {
    "lb54": "https://line-lb54-w.bk6bba-resources.com",
    "lb61": "https://line-lb61-w.bk6bba-resources.com",
    "vk":   "https://line-vk-w.bk6bba-resources.ru",
}

# для vk пути без /ma, для .com — с /ma
PATHS = {
    "lb54": "/ma/line/factorsCatalog",
    "lb61": "/ma/line/factorsCatalog",
    "vk":   "/line/factorsCatalog",
}

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
OUT = Path("sniff_out/catalogs")
OUT.mkdir(parents=True, exist_ok=True)


ENDPOINTS = [
    ("eventViewTables",      {"version": "0", "lang": "ru", "sysId": "21"}),
    ("independentFactors",   {"version": "0", "lang": "ru", "sysId": "21", "scopeMarket": "1600"}),
    ("tables",               {"version": "0", "lang": "ru", "sysId": "21"}),
    ("sportBasicFactors",    {"version": "0", "lang": "ru", "systemType": "desktop"}),
    ("specialTables",        {"version": "0", "lang": "ru", "sysId": "21"}),
]


def _human(n: int) -> str:
    for u in ("B", "KB", "MB"):
        if n < 1024:
            return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} GB"


async def download(cluster_key: str):
    base = CLUSTERS[cluster_key]
    prefix = PATHS[cluster_key]
    print(f"\n🌐 Кластер: {cluster_key} ({base})")

    results = {}
    async with AsyncSession(impersonate=IMPERSONATE, timeout=120, headers=HEADERS) as s:
        for name, params in ENDPOINTS:
            url = f"{base}{prefix}/{name}"
            print(f"  → {name} ...", end=" ", flush=True)
            try:
                r = await s.get(url, headers=HEADERS, params=params)
                if r.status_code != 200:
                    print(f"HTTP {r.status_code}")
                    continue
                data = r.json()
                path = OUT / f"{cluster_key}_{name}.json"
                path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                print(f"OK {_human(len(r.content))} → {path.name}")
                results[name] = data
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"💥 {type(e).__name__}: {e}")
    return results


def analyze(name: str, data) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {name}")
    print(f"{'=' * 70}")

    if isinstance(data, list):
        print(f"  тип: список из {len(data)}")
        if data:
            print(f"  пример элемента:")
            print(json.dumps(data[0], ensure_ascii=False, indent=2)[:800])
        return

    if not isinstance(data, dict):
        print(f"  тип: {type(data).__name__}")
        return

    print(f"  ключи: {list(data.keys())[:15]}")

    for k, v in data.items():
        if isinstance(v, list):
            print(f"    {k}: список из {len(v)}")
            if v and isinstance(v[0], dict):
                print(f"      пример: {json.dumps(v[0], ensure_ascii=False)[:300]}")
        elif isinstance(v, dict):
            print(f"    {k}: dict с {len(v)} ключами")
            sub_keys = list(v.keys())[:5]
            print(f"      ключи: {sub_keys}")
            first_key = sub_keys[0] if sub_keys else None
            if first_key is not None:
                print(f"      {first_key}: {json.dumps(v[first_key], ensure_ascii=False)[:300]}")
        else:
            print(f"    {k}: {v}")


def find_factor_names(data: dict) -> dict[int, str]:
    """
    Пытается найти маппинг factor_id → имя.
    Разные каталоги имеют разную структуру, посмотрим что выйдет.
    """
    result = {}

    def walk(node, depth=0):
        if depth > 8:
            return
        if isinstance(node, dict):
            # паттерн 1: {"id": 921, "name": "П1"}
            if "id" in node and "name" in node and isinstance(node.get("id"), int):
                result[node["id"]] = node["name"]
            # паттерн 2: {"f": 921, "name": "..."}
            if "f" in node and "name" in node:
                result[node["f"]] = node["name"]
            for v in node.values():
                walk(v, depth + 1)
        elif isinstance(node, list):
            for item in node:
                walk(item, depth + 1)

    walk(data)
    return result


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cluster", default="lb54", choices=list(CLUSTERS.keys()))
    args = ap.parse_args()

    results = await download(args.cluster)

    for name, data in results.items():
        analyze(name, data)

    # пробуем вытащить маппинг factor_id → name
    print(f"\n{'=' * 70}")
    print(f"  ПОПЫТКА ИЗВЛЕЧЬ factor_id → NAME")
    print(f"{'=' * 70}")

    merged = {}
    for name, data in results.items():
        found = find_factor_names(data)
        print(f"  {name}: найдено {len(found)} связок id→name")
        merged.update(found)

    print(f"\n  Итог: {len(merged)} уникальных factor_id с именами")

    if merged:
        # показываем что получилось для базовых рынков
        print(f"\n  Базовые рынки (921-931):")
        for fid in sorted([921, 922, 923, 924, 925, 927, 928, 930, 931]):
            name = merged.get(fid, "❓ не найдено")
            print(f"    f={fid:<6} → {name}")

        print(f"\n  Фор/тоталов (1800-1830):")
        for fid in sorted([f for f in merged if 1800 <= f <= 1830])[:20]:
            print(f"    f={fid:<6} → {merged[fid]}")

        # сохраняем
        path = OUT / f"factor_names_{args.cluster}.json"
        path.write_text(json.dumps(merged, ensure_ascii=False, indent=2, sort_keys=True),
                        encoding="utf-8")
        print(f"\n  💾 {path}")

    print(f"\n✅ Готово.")


if __name__ == "__main__":
    asyncio.run(main())