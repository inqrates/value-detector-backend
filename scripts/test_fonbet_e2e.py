# scripts/test_fonbet_e2e.py
"""
End-to-end тест Fonbet: сигнал → матч → кэф.

Цепочка:
  1. Разбираем сигнал ("W1", "ТБ 2.5", "Ф1 -1.5", ...) → (canon_type, side, line)
  2. Загружаем канон fonbet_canon.json
  3. Находим все factor_id, подходящие под (canon_type, side)
  4. Находим матч по названиям команд в снапшоте
  5. Дёргаем /ma/events/event?eventId=X — полная линия
  6. Ищем в customFactors нужный fid (+ фильтр по pt=line)
  7. Возвращаем актуальный кэф

Запуск:
    python scripts/test_fonbet_e2e.py --signal "W1" --match "Бавария,Лейпциг"
    python scripts/test_fonbet_e2e.py --signal "ТБ 2.5" --match "Бавария,Лейпциг"
    python scripts/test_fonbet_e2e.py --signal "Ф1 -1.5" --match "Бавария,Лейпциг"
    python scripts/test_fonbet_e2e.py --run-tests    # прогнать набор тестов
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
from pathlib import Path

from curl_cffi.requests import AsyncSession


CATALOG_DIR = Path("sniff_out/catalogs")
CANON_PATH = CATALOG_DIR / "fonbet_canon.json"
SNAPSHOT_PATH = Path("sniff_out/snapshot_scope1600.json")

BASE = "https://line-lb54-w.bk6bba-resources.com"
EVENT_URL = f"{BASE}/ma/events/event"
LIST_BASE_URL = f"{BASE}/ma/events/listBase"

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


# ============================================================
# 1. Парсер сигналов
# ============================================================
SIGNAL_FIXED = {
    # Победа
    "W1":       ("result", "1"),
    "П1":       ("result", "1"),
    "1":        ("result", "1"),
    "HOME":     ("result", "1"),
    "W2":       ("result", "2"),
    "П2":       ("result", "2"),
    "2":        ("result", "2"),
    "AWAY":     ("result", "2"),
    "X":        ("result", "Х"),
    "DRAW":     ("result", "Х"),
    "НИЧЬЯ":    ("result", "Х"),

    # Двойной шанс
    "1X":       ("double_chances", "1X"),
    "X2":       ("double_chances", "X2"),
    "12":       ("double_chances", "12"),

    # Обе забьют
    "ОЗ ДА":    ("both_to_score", "Да"),
    "ОЗ НЕТ":   ("both_to_score", "Нет"),
    "BTTS YES": ("both_to_score", "Да"),
    "BTTS NO":  ("both_to_score", "Нет"),
}


def parse_signal(text: str) -> dict:
    """Разбирает сигнал в структуру (canon_type, side, line)."""
    t = " ".join(text.strip().upper().split())

    # 1. точные совпадения
    if t in SIGNAL_FIXED:
        ct, side = SIGNAL_FIXED[t]
        return {"canon_type": ct, "side": side, "line": None, "raw": text}

    # 2. общий тотал: ТБ/ТМ/Over/Under + линия
    m = re.match(r"^(ТБ|Б|БОЛЬШЕ|OVER|O)\s*([\d]+[.,]?[\d]*)$", t)
    if m:
        line = float(m.group(2).replace(",", "."))
        return {"canon_type": "totals", "side": "Тотал", "line": line, "raw": text}
    m = re.match(r"^(ТМ|М|МЕНЬШЕ|UNDER|U)\s*([\d]+[.,]?[\d]*)$", t)
    if m:
        line = float(m.group(2).replace(",", "."))
        return {"canon_type": "totals", "side": "М", "line": line, "raw": text}

    # 3. фора: Ф1/Ф2/F1/F2 + линия
    m = re.match(r"^(Ф1|F1|HCP1)\s*([+\-]?[\d]+[.,]?[\d]*)$", t)
    if m:
        line = float(m.group(2).replace(",", "."))
        return {"canon_type": "handicaps", "side": "ФОРА 1", "line": line, "raw": text}
    m = re.match(r"^(Ф2|F2|HCP2)\s*([+\-]?[\d]+[.,]?[\d]*)$", t)
    if m:
        line = float(m.group(2).replace(",", "."))
        return {"canon_type": "handicaps", "side": "ФОРА 2", "line": line, "raw": text}

    # 4. инд. тотал: ИТ1/ИТ2 + ТБ/ТМ + линия
    m = re.match(r"^(ИТ1|IT1)\s*(ТБ|Б|БОЛЬШЕ|OVER)\s*([\d]+[.,]?[\d]*)$", t)
    if m:
        line = float(m.group(3).replace(",", "."))
        return {"canon_type": "team1_total", "side": "ИТ 1", "line": line, "raw": text}
    m = re.match(r"^(ИТ1|IT1)\s*(ТМ|М|МЕНЬШЕ|UNDER)\s*([\d]+[.,]?[\d]*)$", t)
    if m:
        line = float(m.group(3).replace(",", "."))
        return {"canon_type": "team1_total", "side": "М", "line": line, "raw": text}
    m = re.match(r"^(ИТ2|IT2)\s*(ТБ|Б|БОЛЬШЕ|OVER)\s*([\d]+[.,]?[\d]*)$", t)
    if m:
        line = float(m.group(3).replace(",", "."))
        return {"canon_type": "team2_total", "side": "ИТ 2", "line": line, "raw": text}
    m = re.match(r"^(ИТ2|IT2)\s*(ТМ|М|МЕНЬШЕ|UNDER)\s*([\d]+[.,]?[\d]*)$", t)
    if m:
        line = float(m.group(3).replace(",", "."))
        return {"canon_type": "team2_total", "side": "М", "line": line, "raw": text}

    raise ValueError(f"Не могу разобрать сигнал: {text!r}")


# ============================================================
# 2. Канон
# ============================================================
def load_canon() -> dict[int, dict]:
    data = json.loads(CANON_PATH.read_text(encoding="utf-8"))
    raw = data.get("factor_canon") or {}
    return {int(k): v for k, v in raw.items()}


def find_candidate_fids(canon: dict[int, dict],
                        canon_type: str, side: str) -> list[int]:
    """Все fid, у которых совпадает canon_type и side."""
    result = []
    for fid, meta in canon.items():
        if meta.get("canon_type") != canon_type:
            continue
        if meta.get("side") != side:
            continue
        result.append(fid)
    return result


# ============================================================
# 3. Поиск матча в снапшоте
# ============================================================
def _norm(s: str) -> str:
    return " ".join((s or "").lower().split())


def find_events_in_snapshot(team1: str, team2: str) -> list[dict]:
    if not SNAPSHOT_PATH.exists():
        return []
    data = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    events = data.get("events") or []

    t1 = _norm(team1)
    t2 = _norm(team2)
    found = []

    for ev in events:
        e1 = _norm(ev.get("team1") or "")
        e2 = _norm(ev.get("team2") or "")
        # прямая или обратная пара
        if ((t1 in e1 and t2 in e2) or (t1 in e2 and t2 in e1)):
            found.append({
                "id": ev.get("id"),
                "team1": ev.get("team1"),
                "team2": ev.get("team2"),
                "sportId": ev.get("sportId"),
                "startTime": ev.get("startTime"),
                "reverse": t1 in e2,
            })
    return found


# ============================================================
# 4. Фетч детального endpoint
# ============================================================
async def fetch_event(event_id: int, scope: int = 1600) -> dict:
    async with AsyncSession(impersonate=IMPERSONATE, timeout=30, headers=HEADERS) as s:
        r = await s.get(
            EVENT_URL,
            headers=HEADERS,
            params={"lang": "ru", "version": "0",
                    "eventId": str(event_id), "scopeMarket": str(scope)},
        )
        r.raise_for_status()
        return r.json()


# ============================================================
# 5. Поиск кэфа
# ============================================================
def find_odd(factors: list[dict],
             candidate_fids: set[int],
             line: float | None,
             reverse: bool = False) -> dict | None:
    """
    Ищет фактор среди factors по fids и опционально по pt (line).
    reverse: если True — флип pt (для случая когда команды в обратном порядке).
    """
    hits = []
    for f in factors:
        fid = f.get("f")
        if fid not in candidate_fids:
            continue

        # фильтр по линии
        if line is not None:
            pt_raw = f.get("pt")
            if pt_raw is None:
                continue
            try:
                pt_val = float(str(pt_raw).replace(",", "."))
            except ValueError:
                continue
            target = line
            if reverse:
                target = -line
            if abs(pt_val - target) > 0.01:
                continue

        hits.append(f)

    if not hits:
        return None

    # если несколько — берём первый по порядку
    return hits[0]


# ============================================================
# 6. Прогон одной связки
# ============================================================
async def run_pipeline(signal_text: str, team1: str, team2: str,
                       canon: dict[int, dict], verbose: bool = True) -> dict:
    result = {
        "signal": signal_text,
        "match_query": [team1, team2],
        "status": "unknown",
        "error": None,
        "found_odd": None,
    }

    # --- Шаг 1: разбор сигнала ---
    try:
        parsed = parse_signal(signal_text)
    except ValueError as e:
        result["status"] = "signal_parse_error"
        result["error"] = str(e)
        if verbose:
            print(f"❌ {e}")
        return result

    if verbose:
        print(f"\n{'─' * 70}")
        print(f"  СИГНАЛ: {signal_text!r}")
        print(f"  → canon_type={parsed['canon_type']!r}  side={parsed['side']!r}  line={parsed['line']}")
        print(f"{'─' * 70}")

    # --- Шаг 2: кандидаты fid ---
    cands = find_candidate_fids(canon, parsed["canon_type"], parsed["side"])
    if not cands:
        result["status"] = "no_fids"
        result["error"] = f"В каноне нет fid для {parsed['canon_type']}/{parsed['side']}"
        if verbose:
            print(f"❌ {result['error']}")
        return result
    if verbose:
        print(f"  🎯 Кандидатов fid: {len(cands)}  (первые 10: {cands[:10]})")
    result["candidate_fids"] = cands

    # --- Шаг 3: поиск матча ---
    events = find_events_in_snapshot(team1, team2)
    if not events:
        result["status"] = "no_match"
        result["error"] = f"Матч {team1} vs {team2} не найден в снапшоте"
        if verbose:
            print(f"❌ {result['error']}")
        return result
    if verbose:
        print(f"  🔍 Найдено матчей: {len(events)}")
        for ev in events[:3]:
            rev = " [обратный порядок]" if ev["reverse"] else ""
            print(f"     eventId={ev['id']} {ev['team1']} vs {ev['team2']}{rev}")

    ev = events[0]
    result["event_id"] = ev["id"]
    result["event_teams"] = [ev["team1"], ev["team2"]]
    result["reverse"] = ev["reverse"]

    # --- Шаг 4: фетч детального ---
    try:
        detail = await fetch_event(ev["id"])
    except Exception as e:
        result["status"] = "fetch_error"
        result["error"] = f"Ошибка получения данных: {e}"
        if verbose:
            print(f"❌ {result['error']}")
        return result

    # --- Шаг 5: собираем все факторы ---
    all_factors = []
    for cf in detail.get("customFactors") or []:
        all_factors.extend(cf.get("factors") or [])

    if verbose:
        print(f"  📊 Получено факторов: {len(all_factors)}")

    # --- Шаг 6: ищем кэф ---
    cand_set = set(cands)
    odd = find_odd(all_factors, cand_set, parsed["line"], reverse=ev["reverse"])
    if odd is None:
        result["status"] = "no_odd"
        result["error"] = (f"Не нашлось кэфа для fid={cands[:5]}..., "
                           f"line={parsed['line']}")
        if verbose:
            print(f"❌ {result['error']}")
            # подсказка: какие вообще pt есть для этих fid
            pts = sorted({
                f.get("pt") for f in all_factors
                if f.get("f") in cand_set and f.get("pt")
            })
            if pts:
                print(f"   Подсказка: доступные pt для этих fid: {pts[:20]}")
        return result

    result["status"] = "ok"
    result["found_odd"] = {
        "factor_id": odd.get("f"),
        "pt": odd.get("pt"),
        "p": odd.get("p"),
        "value": odd.get("v"),
    }
    if verbose:
        print(f"  ✅ НАЙДЕНО:")
        print(f"     factor_id={odd.get('f')}  pt={odd.get('pt')!r}  p={odd.get('p')!r}")
        print(f"     КОЭФФИЦИЕНТ: {odd.get('v')}")
    return result


# ============================================================
# 7. Автотест
# ============================================================
async def run_tests(canon: dict[int, dict]):
    cases = [
        ("W1",        "Бавария", "Лейпциг"),
        ("W2",        "Бавария", "Лейпциг"),
        ("X",         "Бавария", "Лейпциг"),
        ("ТБ 2.5",    "Бавария", "Лейпциг"),
        ("ТМ 2.5",    "Бавария", "Лейпциг"),
        ("ТБ 3.5",    "Бавария", "Лейпциг"),
        ("Ф1 -1.5",   "Бавария", "Лейпциг"),
        ("Ф1 -2.5",   "Бавария", "Лейпциг"),
        ("Ф2 +1.5",   "Бавария", "Лейпциг"),
        ("ОЗ Да",     "Бавария", "Лейпциг"),
        ("1X",        "Бавария", "Лейпциг"),
        ("ИТ1 ТБ 1.5","Бавария", "Лейпциг"),
    ]
    print(f"\n{'=' * 70}")
    print(f"  АВТОТЕСТ: {len(cases)} сигналов")
    print(f"{'=' * 70}")

    ok = 0
    fail = 0
    for sig, t1, t2 in cases:
        r = await run_pipeline(sig, t1, t2, canon, verbose=False)
        if r["status"] == "ok":
            odd = r["found_odd"]
            print(f"  ✅ {sig:<12} → fid={odd['factor_id']:<6} "
                  f"pt={str(odd['pt']):<6} v={odd['value']}")
            ok += 1
        else:
            print(f"  ❌ {sig:<12} → {r['status']}: {r.get('error')}")
            fail += 1

    print(f"\n  Итого: {ok}/{len(cases)} успешно")



async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signal", type=str)
    ap.add_argument("--match", type=str, help="team1,team2")
    ap.add_argument("--run-tests", action="store_true")
    ap.add_argument("--force-refresh", action="store_true",
                    help="перекачать снапшот перед запуском")
    args = ap.parse_args()

    if not CANON_PATH.exists():
        print(f"❌ Нет {CANON_PATH}. Запусти сначала sniff_fonbet_canon.py")
        return

    canon = load_canon()
    print(f"📂 Канон: {len(canon)} factor_id")

    # опционально обновим снапшот
    if args.force_refresh or not SNAPSHOT_PATH.exists():
        print(f"🌐 Обновляем снапшот...")
        async with AsyncSession(impersonate=IMPERSONATE, timeout=60, headers=HEADERS) as s:
            r = await s.get(
                LIST_BASE_URL, headers=HEADERS,
                params={"lang": "ru", "scopeMarket": "1600"},
            )
            if r.status_code == 200:
                SNAPSHOT_PATH.write_bytes(r.content)
                print(f"  ✅ {SNAPSHOT_PATH} ({len(r.content)/1024/1024:.1f} MB)")
            else:
                print(f"  ⚠️ HTTP {r.status_code}")

    if args.run_tests:
        await run_tests(canon)
        return

    if not args.signal or not args.match:
        print("Укажи --signal и --match team1,team2, либо --run-tests")
        return

    parts = [p.strip() for p in args.match.split(",")]
    if len(parts) < 2:
        print("--match должен быть в формате team1,team2")
        return

    result = await run_pipeline(args.signal, parts[0], parts[1], canon, verbose=True)

    print(f"\n{'=' * 70}")
    print(f"  ИТОГ: {result['status']}")
    print(f"{'=' * 70}")
    if result["status"] == "ok":
        o = result["found_odd"]
        print(f"  ✅ {result['event_teams'][0]} vs {result['event_teams'][1]}")
        print(f"     {result['signal']} = {o['value']}  "
              f"(fid={o['factor_id']}, pt={o['pt']})")
    else:
        print(f"  ❌ {result.get('error')}")


if __name__ == "__main__":
    asyncio.run(main())