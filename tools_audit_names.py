# -*- coding: utf-8 -*-
"""
Диагностика нормализации на РЕАЛЬНЫХ данных живого сервера.

Запуск (из папки ten_ten):
    venv\\Scripts\\python.exe tools_audit_names.py

Что проверяет:
  1. Совпадает ли ключ матча, построенный локально, с ключом сервера.
  2. КОЛЛИЗИИ — разные матчи, получившие ОДИН ключ (ложное склеивание).
     Это опаснее потери: сигнал уйдёт на чужой матч.
  3. Имена, где нормализация выбрала НЕ последнее слово (обычно имя
     вместо фамилии). Само по себе не баг, но повышает риск коллизий.

Когда запускать: после добавления новой БК или правки нормализации.
Ориентир: коллизий должно быть 0, совпадений ключей — максимум.
"""
import re
import sys
from collections import defaultdict

import requests

from core import names

URL = "https://62.113.115.185:8000/matches"


def fetch():
    r = requests.get(URL, verify="certs/ca.pem", timeout=25)
    r.raise_for_status()
    return r.json()


def words_of(name: str):
    """Слова имени после очистки (как внутри normalize_name)."""
    text = re.sub(r'\(.*?\)', '', name or '')
    text = re.sub(r'\d+', '', text)
    text = re.sub(r'[^\w\s]', ' ', text).lower()
    text = names.transliterate(text)
    ws = [w for w in text.split() if len(w) > 1] or text.split()
    return [names.ALIASES.get(w, w) for w in ws]


def main() -> int:
    try:
        data = fetch()
    except Exception as e:
        print(f"Не удалось получить данные: {e}")
        return 1

    print(f"Матчей с сервера: {len(data)}\n")

    # ── 1. Ключ локально vs ключ сервера ──
    ok = mismatch = 0
    broken = []
    for m in data:
        p1, p2 = m.get("player1") or "", m.get("player2") or ""
        if not p1 or not p2:
            continue
        n1, n2 = names.normalize_name(p1), names.normalize_name(p2)
        key = "||".join(sorted([n1, n2]))
        srv_key = (m.get("key") or "").split("::", 1)[-1]
        if key == srv_key:
            ok += 1
        else:
            mismatch += 1
            broken.append((p1, p2, (n1, n2), key, srv_key))

    print(f"── КЛЮЧИ ──")
    print(f"  совпало с сервером: {ok}")
    print(f"  отличается:         {mismatch}")
    for p1, p2, norm, key, srv in broken[:10]:
        print(f"\n    {p1!r} vs {p2!r}")
        print(f"      нормализовано: {norm}")
        print(f"      наш:    {key!r}")
        print(f"      сервер: {srv!r}")

    # ── 2. Коллизии ──
    groups = defaultdict(set)
    for m in data:
        p1, p2 = m.get("player1") or "", m.get("player2") or ""
        if not p1 or not p2:
            continue
        n1, n2 = names.normalize_name(p1), names.normalize_name(p2)
        key = "||".join(sorted([n1, n2]))
        groups[key].add((p1, p2))

    collisions = {k: v for k, v in groups.items() if len(v) > 1}
    print(f"\n── КОЛЛИЗИИ (разные матчи → один ключ) ──")
    print(f"  уникальных ключей: {len(groups)}")
    print(f"  коллизий:          {len(collisions)}")
    if collisions:
        print("  ⚠️ ЭТО ОПАСНО: сигнал может уйти не на тот матч")
        for k, v in list(collisions.items())[:10]:
            print(f"\n    ключ {k!r}:")
            for p in v:
                print(f"      {p[0]!r} vs {p[1]!r}")
    else:
        print("  ✅ склейки матчей нет")

    # ── 3. Где выбрано не последнее слово ──
    odd = []
    seen = set()
    for m in data:
        for f in ("player1", "player2"):
            nm = m.get(f) or ""
            if not nm or nm in seen:
                continue
            seen.add(nm)
            ws = words_of(nm)
            if len(ws) < 2:
                continue
            if names.normalize_name(nm) != ws[-1]:
                odd.append((nm, names.normalize_name(nm), ws))

    print(f"\n── ИМЯ ВМЕСТО ФАМИЛИИ ──")
    print(f"  имён с 2+ словами: {len(seen)}")
    print(f"  выбрано не последнее слово: {len(odd)}")
    print("  (не баг сам по себе — обе БК пишут одинаково, ключ совпадает;")
    print("   но повышает риск коллизий при разном написании между БК)")
    for nm, chosen, ws in odd[:10]:
        print(f"    {nm!r} → {chosen!r}  (слова: {ws})")

    # ── Итог ──
    print("\n── ВЕРДИКТ ──")
    if not collisions and mismatch == 0:
        print("  ✅ Потерь и склейки нет. Нормализацию менять не нужно.")
        return 0
    print("  ⚠️ Есть проблемы — см. выше. Нужна правка нормализации.")
    return 2


if __name__ == "__main__":
    sys.exit(main())
