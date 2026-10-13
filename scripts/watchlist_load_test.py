# scripts/watchlist_load_test.py
"""
Часовой (или N-минутный) нагрузочный тест watchlist-подхода Fonbet.

Что делает:
  1. Тянет свежий снапшот listBase
  2. Выбирает N live/актуальных матчей
  3. Каждые INTERVAL сек (с jitter) дёргает детальный endpoint
     /ma/events/event?eventId=X по каждому матчу
  4. Логирует: код ответа, тайминг, размер, кол-во факторов
  5. Итоговая сводка: min/avg/max, распределение кодов, число ошибок

Опции:
  --duration      сколько секунд крутить (по умолчанию 3600 = час)
  --watchlist     размер watchlist (по умолчанию 30)
  --interval      базовый интервал обновления матча в сек (по умолчанию 10)
  --jitter        максимальный jitter в сек (по умолчанию 3)
  --cluster       lb54 / lb61 / vk (по умолчанию lb54)
  --sport         фильтр по корневому sportId (по умолчанию все)
  --dry-run       не делать запросы, только показать план

Примеры:
  python scripts/watchlist_load_test.py --duration 300              # 5 мин, быстрый тест
  python scripts/watchlist_load_test.py --duration 3600             # полный час
  python scripts/watchlist_load_test.py --duration 300 --watchlist 50 --interval 5
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import time
from collections import Counter, defaultdict
from pathlib import Path

from curl_cffi.requests import AsyncSession


CLUSTERS = {
    "lb54": {
        "base": "https://line-lb54-w.bk6bba-resources.com",
        "path_prefix": "/ma",
    },
    "lb61": {
        "base": "https://line-lb61-w.bk6bba-resources.com",
        "path_prefix": "/ma",
    },
    "vk": {
        "base": "https://line-vk-w.bk6bba-resources.ru",
        "path_prefix": "",   # у vk пути без /ma
    },
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
OUT = Path("sniff_out/watchlist_test")
OUT.mkdir(parents=True, exist_ok=True)


# ============================================================
# Статистика по одному матчу
# ============================================================
class MatchStats:
    __slots__ = ("event_id", "codes", "times", "sizes", "factor_counts",
                 "errors", "last_ts")

    def __init__(self, event_id: int):
        self.event_id = event_id
        self.codes: Counter = Counter()
        self.times: list[float] = []
        self.sizes: list[int] = []
        self.factor_counts: list[int] = []
        self.errors: Counter = Counter()
        self.last_ts: float = 0.0


# ============================================================
# Выбор матчей из снапшота
# ============================================================
async def pick_watchlist(session: AsyncSession, cluster_key: str,
                         n: int, sport_filter: int | None) -> list[dict]:
    base = CLUSTERS[cluster_key]["base"]
    prefix = CLUSTERS[cluster_key]["path_prefix"]
    url = f"{base}{prefix}/events/listBase"

    print(f"🌐 Тянем снапшот ({cluster_key}) ...")
    r = await session.get(url, headers=HEADERS,
                          params={"lang": "ru", "scopeMarket": "1600"})
    r.raise_for_status()
    data = r.json()

    events = data.get("events") or []
    custom = data.get("customFactors") or []
    # сколько факторов у каждого события
    factors_by_eid = {cf.get("e"): len(cf.get("factors") or []) for cf in custom}

    # только те, у кого есть факторы и (если задан) совпадает sport
    candidates = []
    for ev in events:
        eid = ev.get("id")
        if not eid:
            continue
        n_f = factors_by_eid.get(eid, 0)
        if n_f == 0:
            continue
        sid = ev.get("sportId")
        if sport_filter is not None and sid != sport_filter:
            continue
        candidates.append({
            "id": eid,
            "sport": sid,
            "team1": ev.get("team1"),
            "team2": ev.get("team2"),
            "factors_in_snap": n_f,
        })

    # предпочитаем live (у которых много факторов в listBase)
    candidates.sort(key=lambda x: -x["factors_in_snap"])
    picked = candidates[:n]

    print(f"✅ В снапшоте: {len(events)} событий, {len(candidates)} с факторами")
    print(f"✅ В watchlist: {len(picked)} матчей (топ по кол-ву факторов)")
    for m in picked[:5]:
        print(f"   id={m['id']:<12} sport={m['sport']:<6} "
              f"snap_factors={m['factors_in_snap']:<4} "
              f"{(m['team1'] or '?')[:25]} vs {(m['team2'] or '?')[:25]}")
    if len(picked) > 5:
        print(f"   ... и ещё {len(picked) - 5}")

    return picked


# ============================================================
# Один запрос детального endpoint
# ============================================================
async def fetch_detail(session: AsyncSession, cluster_key: str,
                       event_id: int) -> tuple[int, float, int, int, str | None]:
    """
    Возвращает (status_code, elapsed_sec, size_bytes, n_factors, error).
    """
    base = CLUSTERS[cluster_key]["base"]
    prefix = CLUSTERS[cluster_key]["path_prefix"]
    url = f"{base}{prefix}/events/event"

    t0 = time.monotonic()
    try:
        r = await session.get(
            url,
            headers=HEADERS,
            params={
                "lang": "ru",
                "version": "0",
                "eventId": str(event_id),
                "scopeMarket": "1600",
            },
        )
        dt = time.monotonic() - t0
        size = len(r.content) if r.content else 0

        n_factors = 0
        if r.status_code == 200:
            try:
                j = r.json()
                n_factors = sum(
                    len(cf.get("factors") or [])
                    for cf in (j.get("customFactors") or [])
                )
            except Exception:
                pass
        return (r.status_code, dt, size, n_factors, None)
    except Exception as e:
        dt = time.monotonic() - t0
        return (-1, dt, 0, 0, f"{type(e).__name__}: {e}")


# ============================================================
# Worker: один матч, свой интервал, своя статистика
# ============================================================
async def watch_one(session: AsyncSession, cluster_key: str,
                    match: dict, interval: float, jitter: float,
                    stop_at: float,
                    stats: MatchStats,
                    global_log: list,
                    sem: asyncio.Semaphore):
    eid = match["id"]

    while time.monotonic() < stop_at:
        async with sem:
            code, dt, size, n_f, err = await fetch_detail(session, cluster_key, eid)

        stats.codes[code] += 1
        stats.times.append(dt)
        if size:
            stats.sizes.append(size)
        if n_f:
            stats.factor_counts.append(n_f)
        if err:
            stats.errors[err] += 1
        stats.last_ts = time.time()

        global_log.append({
            "ts": time.time(),
            "eid": eid,
            "code": code,
            "dt": dt,
            "size": size,
            "n_f": n_f,
            "err": err,
        })

        # ждём свой интервал + jitter
        sleep_for = interval + random.uniform(0, jitter)
        await asyncio.sleep(sleep_for)


# ============================================================
# Печать живого статуса раз в 15 сек
# ============================================================
async def progress_printer(all_stats: dict[int, MatchStats],
                            stop_at: float, started_at: float):
    while time.monotonic() < stop_at:
        await asyncio.sleep(15)
        elapsed = time.monotonic() - started_at
        total_reqs = sum(sum(s.codes.values()) for s in all_stats.values())
        total_200 = sum(s.codes.get(200, 0) for s in all_stats.values())
        total_403 = sum(s.codes.get(403, 0) for s in all_stats.values())
        total_429 = sum(s.codes.get(429, 0) for s in all_stats.values())
        total_err = sum(sum(s.errors.values()) for s in all_stats.values())

        all_times = [t for s in all_stats.values() for t in s.times]
        avg_t = statistics.mean(all_times) if all_times else 0
        p95_t = (statistics.quantiles(all_times, n=20)[18]
                 if len(all_times) > 20 else avg_t)

        print(f"[{elapsed:>6.0f}s]  "
              f"reqs={total_reqs:<6} "
              f"200={total_200:<6} "
              f"403={total_403:<4} "
              f"429={total_429:<4} "
              f"err={total_err:<4}  "
              f"avg={avg_t*1000:>6.0f}ms  "
              f"p95={p95_t*1000:>6.0f}ms")


# ============================================================
# Финальный отчёт
# ============================================================
def print_final_report(all_stats: dict[int, MatchStats],
                        matches: list[dict], started_at: float,
                        global_log: list, duration: float):
    elapsed = time.monotonic() - started_at

    total_codes = Counter()
    total_errors = Counter()
    all_times = []
    all_sizes = []
    all_factors = []

    for s in all_stats.values():
        total_codes.update(s.codes)
        total_errors.update(s.errors)
        all_times.extend(s.times)
        all_sizes.extend(s.sizes)
        all_factors.extend(s.factor_counts)

    total_reqs = sum(total_codes.values())

    print(f"\n{'=' * 70}")
    print(f"  ИТОГОВЫЙ ОТЧЁТ")
    print(f"{'=' * 70}")
    print(f"  Длительность:       {elapsed:.0f} сек ({elapsed/60:.1f} мин)")
    print(f"  Матчей в watchlist: {len(matches)}")
    print(f"  Всего запросов:     {total_reqs}")
    print(f"  RPS (усредн.):      {total_reqs/elapsed:.2f}")
    print(f"  Интервал/матч:      {elapsed * len(matches) / total_reqs:.1f} сек")

    print(f"\n  ── КОДЫ ОТВЕТОВ ──")
    for code in sorted(total_codes.keys(), key=lambda c: -total_codes[c]):
        pct = total_codes[code] / total_reqs * 100
        marker = "✅" if code == 200 else "⚠️"
        print(f"    {marker} HTTP {code:>4}: {total_codes[code]:<6} ({pct:.1f}%)")

    if total_errors:
        print(f"\n  ── ОШИБКИ ──")
        for err, cnt in total_errors.most_common(10):
            print(f"    💥 {err[:70]:<70} {cnt}")

    if all_times:
        print(f"\n  ── ТАЙМИНГ ──")
        print(f"    min:    {min(all_times)*1000:>7.0f} ms")
        print(f"    median: {statistics.median(all_times)*1000:>7.0f} ms")
        print(f"    mean:   {statistics.mean(all_times)*1000:>7.0f} ms")
        if len(all_times) > 20:
            p95 = statistics.quantiles(all_times, n=20)[18]
            p99 = statistics.quantiles(all_times, n=100)[98]
            print(f"    p95:    {p95*1000:>7.0f} ms")
            print(f"    p99:    {p99*1000:>7.0f} ms")
        print(f"    max:    {max(all_times)*1000:>7.0f} ms")

    if all_sizes:
        print(f"\n  ── РАЗМЕР ОТВЕТА ──")
        print(f"    min:    {min(all_sizes)/1024:>7.1f} KB")
        print(f"    median: {statistics.median(all_sizes)/1024:>7.1f} KB")
        print(f"    mean:   {statistics.mean(all_sizes)/1024:>7.1f} KB")
        print(f"    max:    {max(all_sizes)/1024:>7.1f} KB")

    if all_factors:
        print(f"\n  ── ФАКТОРОВ НА ОТВЕТ ──")
        print(f"    min:    {min(all_factors):>7}")
        print(f"    median: {int(statistics.median(all_factors)):>7}")
        print(f"    mean:   {int(statistics.mean(all_factors)):>7}")
        print(f"    max:    {max(all_factors):>7}")

    # оценка безопасности
    print(f"\n  ── ВЕРДИКТ ──")
    ok = total_codes.get(200, 0)
    bad_403 = total_codes.get(403, 0)
    bad_429 = total_codes.get(429, 0)
    ok_pct = ok / total_reqs * 100 if total_reqs else 0

    if ok_pct >= 99.5 and bad_403 == 0 and bad_429 == 0:
        print(f"  ✅ БЕЗОПАСНО. {ok_pct:.1f}% успешных, ни одного 403/429.")
    elif ok_pct >= 95:
        print(f"  🟡 ВНИМАНИЕ. {ok_pct:.1f}% успешных. "
              f"403={bad_403}, 429={bad_429}. Проверь retry/backoff.")
    else:
        print(f"  ❌ ОПАСНО. {ok_pct:.1f}% успешных. "
              f"403={bad_403}, 429={bad_429}. Нужны прокси/jitter.")

    # распределение по матчам
    print(f"\n  ── ТОП-5 МАТЧЕЙ ПО ЧИСЛУ ЗАПРОСОВ ──")
    sorted_stats = sorted(all_stats.values(),
                           key=lambda s: -sum(s.codes.values()))[:5]
    for s in sorted_stats:
        n = sum(s.codes.values())
        ok = s.codes.get(200, 0)
        avg = statistics.mean(s.times) * 1000 if s.times else 0
        print(f"    id={s.event_id:<12} reqs={n:<5} 200={ok:<5} "
              f"avg={avg:>6.0f}ms")

    # сохраняем сырой лог
    log_path = OUT / f"raw_log_{int(time.time())}.json"
    log_path.write_text(
        json.dumps({
            "started_at": started_at,
            "duration_sec": elapsed,
            "watchlist_size": len(matches),
            "summary": {
                "total_reqs": total_reqs,
                "codes": dict(total_codes),
                "errors": dict(total_errors),
                "timing_ms": {
                    "min": min(all_times) * 1000 if all_times else 0,
                    "mean": statistics.mean(all_times) * 1000 if all_times else 0,
                    "max": max(all_times) * 1000 if all_times else 0,
                },
            },
            "matches": [
                {"id": m["id"], "sport": m["sport"],
                 "team1": m["team1"], "team2": m["team2"]}
                for m in matches
            ],
            "log": global_log,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n  💾 Сырой лог: {log_path} ({len(global_log)} записей)")


# ============================================================
# Main
# ============================================================
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=3600,
                    help="длительность в секундах (по умолч. 3600 = час)")
    ap.add_argument("--watchlist", type=int, default=30,
                    help="сколько матчей держать (по умолч. 30)")
    ap.add_argument("--interval", type=float, default=10.0,
                    help="базовый интервал обновления в сек (по умолч. 10)")
    ap.add_argument("--jitter", type=float, default=3.0,
                    help="jitter в сек (по умолч. 3)")
    ap.add_argument("--cluster", type=str, default="lb54",
                    choices=list(CLUSTERS.keys()))
    ap.add_argument("--sport", type=int, default=None,
                    help="фильтр по sportId (1=футбол, 3088=НТ, 4=теннис)")
    ap.add_argument("--concurrency", type=int, default=5,
                    help="макс. одновременных запросов (по умолч. 5)")
    ap.add_argument("--dry-run", action="store_true",
                    help="показать план и выйти")
    args = ap.parse_args()

    # ожидаемый RPS
    est_rps = args.watchlist / (args.interval + args.jitter / 2)
    est_per_min = est_rps * 60
    est_total = est_rps * args.duration

    print(f"🎯 ПЛАН ТЕСТА:")
    print(f"   Кластер:        {args.cluster}")
    print(f"   Watchlist:      {args.watchlist} матчей")
    print(f"   Интервал:       {args.interval} ± {args.jitter} сек")
    print(f"   Длительность:   {args.duration} сек ({args.duration/60:.1f} мин)")
    print(f"   Concurrency:    {args.concurrency}")
    print(f"   Фильтр sport:   {args.sport or 'нет'}")
    print(f"   ~RPS:           {est_rps:.2f} ({est_per_min:.0f}/мин)")
    print(f"   ~Всего запросов:{est_total:.0f}")

    if args.dry_run:
        return

    # запрашиваем подтверждение для долгих тестов
    if args.duration > 300:
        print(f"\n⚠️  Длительный тест ({args.duration} сек). Ctrl+C чтобы прервать.")

    async with AsyncSession(impersonate=IMPERSONATE, timeout=30, headers=HEADERS) as session:
        matches = await pick_watchlist(session, args.cluster,
                                       args.watchlist, args.sport)
        if not matches:
            print("❌ Не нашлось подходящих матчей")
            return

        # запуск
        started_at = time.monotonic()
        stop_at = started_at + args.duration

        all_stats: dict[int, MatchStats] = {
            m["id"]: MatchStats(m["id"]) for m in matches
        }
        global_log: list = []
        sem = asyncio.Semaphore(args.concurrency)

        print(f"\n🚀 Стартуем {len(matches)} воркеров...")
        print(f"   Первый прогресс через 15 сек.\n")

        tasks = [
            asyncio.create_task(watch_one(
                session, args.cluster, m,
                interval=args.interval, jitter=args.jitter,
                stop_at=stop_at,
                stats=all_stats[m["id"]],
                global_log=global_log,
                sem=sem,
            ))
            for m in matches
        ]
        printer = asyncio.create_task(
            progress_printer(all_stats, stop_at, started_at)
        )

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("\n⏹️  Прервано.")
        finally:
            printer.cancel()
            try:
                await printer
            except asyncio.CancelledError:
                pass

        print_final_report(all_stats, matches, started_at, global_log, args.duration)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Остановлено пользователем")