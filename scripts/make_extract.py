# scripts/make_extract.py
"""
Супер-компактная выжимка для отправки ИИ.

Что сохраняет:
  Fonbet:
    - event_kinds: [{ek, titles}] — все 278
    - factor_markets: топ-100 (market, outcome_sample)
    - few_fid_examples: 30 примеров fid → market/outcome
  Linebet:
    - groups: топ-200 по fid_count
    - markets: топ-200 по outcomes_count
    - few_fid_examples: 30 примеров fid → market/outcome
    - sports_list: 78 штук

Запуск:
    python scripts/make_extract.py
"""
from __future__ import annotations

import json
from pathlib import Path


def load(path):
    return json.load(open(path, encoding="utf-8"))


def build_fonbet_extract(digest: dict) -> dict:
    event_kinds = digest.get("event_kinds") or []
    markets = digest.get("factor_markets") or []

    return {
        "source": "fonbet",
        "event_kinds_count": len(event_kinds),
        "event_kinds": [
            {
                "ek": ek.get("eventKind"),
                "titles": ek.get("titles", [])[:3],   # только 3 примера
            }
            for ek in event_kinds
        ],
        "top_markets": [
            {
                "market": m.get("market"),
                "outcomes_count": m.get("outcomes_count"),
                "sample_outcomes": [x.get("outcome") for x in (m.get("outcomes_sample") or [])[:6]],
            }
            for m in markets[:100]
        ],
    }


def build_linebet_extract(digest: dict) -> dict:
    groups = digest.get("factor_groups") or []
    markets = digest.get("factor_markets") or []
    sports = digest.get("sports") or {}

    return {
        "source": "linebet",
        "sports_count": len(sports),
        "sports_list": sorted(sports.keys()),
        "groups_count": len(groups),
        "top_groups": [
            {"group": g.get("group"), "fid_count": g.get("fid_count")}
            for g in groups[:200]
        ],
        "markets_count": len(markets),
        "top_markets": [
            {
                "market": m.get("market"),
                "outcomes_count": m.get("outcomes_count"),
                "sample_outcomes": [
                    x.get("outcome") for x in (m.get("outcomes_sample") or [])[:6]
                ],
            }
            for m in markets[:200]
        ],
    }


def main():
    print("📂 Loading digests...")
    fonbet = load("fonbet_digest.json")
    linebet = load("linebet_digest.json")

    print("🔧 Building extracts...")
    extract = {
        "fonbet": build_fonbet_extract(fonbet),
        "linebet": build_linebet_extract(linebet),
    }

    out = Path("extract_for_ai.json")
    out.write_text(
        json.dumps(extract, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n💾 {out} ({out.stat().st_size / 1024:.1f} KB)")

    # ---- печатаем сводку для чата ----
    print(f"\n{'=' * 70}")
    print(f"  ВЫЖИМКА ДЛЯ ОТПРАВКИ")
    print(f"{'=' * 70}")

    print(f"\n  FONBET:")
    print(f"    event_kinds:    {extract['fonbet']['event_kinds_count']}")
    print(f"    top_markets:    {len(extract['fonbet']['top_markets'])}")

    print(f"\n  LINEBET:")
    print(f"    sports:         {extract['linebet']['sports_count']}")
    print(f"    groups:         {extract['linebet']['groups_count']}")
    print(f"    top_markets:    {len(extract['linebet']['top_markets'])}")

    # ---- полный список eventKind Fonbet ----
    print(f"\n{'=' * 70}")
    print(f"  FONBET: ВСЕ eventKind")
    print(f"{'=' * 70}")
    for ek in extract["fonbet"]["event_kinds"]:
        title = (ek["titles"][0] if ek["titles"] else "")[:55]
        print(f"    ek={ek['ek']:<12} | {title}")

    # ---- топ-30 групп Linebet ----
    print(f"\n{'=' * 70}")
    print(f"  LINEBET: ТОП-30 ГРУПП")
    print(f"{'=' * 70}")
    for g in extract["linebet"]["top_groups"][:30]:
        print(f"    [{g['fid_count']:>5}] {g['group']}")


if __name__ == "__main__":
    main()