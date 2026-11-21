import asyncio
import logging
import sys
import os
import time
import uvicorn

# ── ЗАГРУЗКА .env ДО ВСЕХ ИМПОРТОВ ПРОЕКТА ──
# Критично: api.py читает ADMIN_TOKEN на уровне модуля,
# поэтому .env надо загрузить РАНЬШЕ, чем произойдёт `from api import ...`
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️ python-dotenv не установлен. Установи: pip install python-dotenv")

# ---- Теперь импорты проекта ----
from parsers.fonbet_api import FonbetApiParser
from parsers.pari_api import PariApiParser
from parsers.winline_api import WinlineApiParser
from parsers.ligastavok_api import LigaStavokApiParser
from parsers.leon_api import LeonApiParser
from parsers.olimp_api import OlimpApiParser
from parsers.betcity_api import BetcityApiParser
from parsers.marathon_api import MarathonApiParser
from parsers.zenit_api import ZenitApiParser
from parsers.sportbet_api import SportbetApiParser

from core.detector import Detector
from core.aggregator import OddsAggregator
from core.browser_manager import browser_manager
from api import app, set_aggregator, broadcast_message

from config import (
    PAGE_RELOAD_ENABLED, PAGE_RELOAD_STAGGER,
    PAGE_KEEP_FRONT, PAGE_KEEP_FRONT_INTERVAL,
)

# ---- Настройка логирования ----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DEBUG_PARSERS = os.getenv("DEBUG_PARSERS", "0") == "1"

LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(message)s'

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Отключаем шумные библиотеки (всегда)
for lib in ['playwright', 'httpx', 'urllib3', 'asyncio']:
    logging.getLogger(lib).setLevel(logging.WARNING)

# Уровень логов парсеров: WARNING по умолчанию, INFO если DEBUG_PARSERS=1
_parser_level = logging.INFO if DEBUG_PARSERS else logging.WARNING
for name in ['parsers', 'parsers.fonbet_api', 'parsers.winline_api', 'parsers.ligastavok_api',
             'parsers.leon_api', 'parsers.olimp_api', 'parsers.betcity_api', 'parsers.marathon_api',
             'parsers.zenit_api', 'parsers.sportbet_api']:
    logging.getLogger(name).setLevel(_parser_level)

# Временно включаем INFO для winline для теста мультиспорта
# logging.getLogger('parsers.leon_api').setLevel(logging.INFO)


logger = logging.getLogger(__name__)


# ============================================================
# Дедупликация: не отправляем один и тот же объект повторно
# ============================================================
_sent_forks: set = set()
_sent_values: set = set()
_sent_corridors: set = set()


def _hash_obj(obj: dict) -> tuple:
    """Стабильный хеш объекта для дедупликации."""
    return tuple(sorted(
        (k, str(v)[:64]) for k, v in obj.items()
        if k in ('key', 'match_id', 'match_id_p1', 'match_id_p2',
                 'bk', 'bk_p1', 'bk_p2', 'bk1', 'bk2',
                 'best_p1', 'best_p2', 'odd', 'odd1', 'odd2',
                 'profit_percent', 'profit', 'side1', 'side2',
                 'outcome', 'ratio')
    ))


async def print_stats(aggregator: OddsAggregator):
    _last_summary = 0.0
    _seen_forks = set()     # дедупликация: не повторять одну вилку
    while True:
        try:
            await asyncio.sleep(5)
            now = time.time()

            # ── СВОДКА раз в 15 сек ──
            if now - _last_summary >= 15:
                _last_summary = now
                matches = aggregator.get_all_matches()
                multi = [m for m in matches if len(m['bks']) >= 2]
                triple = [m for m in matches if len(m['bks']) >= 3]
                four = [m for m in matches if len(m['bks']) >= 4]
                logger.info(
                    f"📊 Матчей: {len(matches)} | 2+БК: {len(multi)} | "
                    f"3+БК: {len(triple)} | 4+БК: {len(four)}"
                )

            # ── ФИКС 2026-10: вилки отключены — фронт их не использует
            # (place_arbitrage_bet возвращает not_implemented, стратегии
            # 'Arbitrage' мигрируются в 'After-goal' при загрузке).
            # Считались каждые 5 сек вхолостую: O(matches × BK).
            # Если нужно вернуть — раскомментировать. ──
            # forks = aggregator.find_arbitrage(min_profit=0.5)
            # for f in forks:
            #     ...

            # ── ВАЛУИ — только топ-5 по ratio, без повторов ──
            values = aggregator.find_value_bets(threshold=1.05)
            if values:
                top = sorted(values, key=lambda v: -v['ratio'])[:5]
                for v in top:
                    logger.info(
                        f"💎 {v['player1']} vs {v['player2']} | "
                        f"{v['outcome']} @ {v['odd']:.2f} "
                        f"(ratio {v['ratio']:.2f}) @ {v['bk']}"
                    )
                    await broadcast_message({"type": "value", "payload": v}, channel="value")

            # ── ФИКС 2026-10: коридоры отключены — фронт их не использует
            # (place_corridor_bet возвращает not_implemented, стратегии
            # 'Corridor' мигрируются в 'After-goal' при загрузке).
            # Считались каждые 5 сек вхолостую: O(matches × BK²) —
            # самая дорогая из мёртвых функций.
            # Если нужно вернуть — раскомментировать. ──
            # corridors = aggregator.find_corridors()
            # if corridors:
            #     ...

            # ── Статистика советника ──
            stats = aggregator.get_advisor_stats()
            await broadcast_message({"type": "advisor", "payload": stats}, channel="advisor")

        except Exception as e:
            logger.error(f"print_stats: {e}", exc_info=True)
            await asyncio.sleep(1)

# ============================================================
# Пуш изменений match_state подписчикам через WebSocket
# ============================================================
async def _match_state_pusher(aggregator: OddsAggregator):
    """Раз в 0.5 сек пушит подписчикам изменения из aggregator."""
    from api import push_match_state
    while True:
        try:
            await asyncio.sleep(0.5)
            keys = aggregator.flush_dirty()
            if not keys:
                continue
            now = time.time()
            _MAX_PER_TICK = 100
            keys_list = list(keys)
            for i in range(0, len(keys_list), _MAX_PER_TICK):
                batch = keys_list[i:i + _MAX_PER_TICK]
                for key in batch:
                    bk_data = aggregator._matches.get(key)
                    if not bk_data:
                        continue
                    bks = {}
                    for bk_id, m in bk_data.items():
                        bks[bk_id] = {
                            "match_id": m.match_id,
                            "score1": m.score1,
                            "score2": m.score2,
                            "sub_score1": m.sub_score1,
                            "sub_score2": m.sub_score2,
                            "odds1": m.odds1,
                            "odds2": m.odds2,
                            "raw_time": getattr(m, "raw_time", "") or "",
                            "sport": getattr(m, "sport", "table_tennis"),
                            "timestamp": m.timestamp,
                            "match_url": getattr(m, "match_url", "") or "",
                        }
                    await push_match_state(key, bks, now)
                if i + _MAX_PER_TICK < len(keys_list):
                    await asyncio.sleep(0)
        except Exception as e:
            logger.error(f"_match_state_pusher: {e}", exc_info=True)
            await asyncio.sleep(1)


# ============================================================
# Глобальная чистка кэшей: удаляем только «мёртвые» события
# ============================================================
async def global_cache_cleaner(parsers, ttl_seconds: float = 900.0,
                                check_interval: float = 60.0):
    """
    Раз в минуту чистит «мёртвые» события во всех парсерах.

    Удаляет событие только если:
      - оно не обновлялось больше ttl_seconds
      - И мы не отправляли его больше ttl_seconds

    => Событие, у которого есть неотправленные апдейты (last_update > last_sent),
       НИКОГДА не удалится. Гарантия: ни один «гол» не потеряется.
    """
    while True:
        try:
            await asyncio.sleep(check_interval)
            now = time.time()
            total_removed = 0

            for p in parsers:
                # какой у парсера основной кэш
                cache = (getattr(p, '_matches_cache', None)
                         or getattr(p, '_events_cache', None))
                if not isinstance(cache, dict):
                    continue

                last_update = getattr(p, '_last_update_time', None)
                last_sent = getattr(p, '_last_sent_time', None)
                first_seen = getattr(p, '_first_seen', None)

                # У некоторых БК (Fonbet) несколько словарей по событиям
                extra_caches = []
                for attr in ('_factors_cache', '_live_cache',
                             '_lines_cache', '_last_sent_state'):
                    d = getattr(p, attr, None)
                    if isinstance(d, dict):
                        extra_caches.append(d)

                removed = 0

                live_cache = getattr(p, '_live_cache', None)
                factors_cache = getattr(p, '_factors_cache', None)
                is_fonbet_like = (
                    p.bk_id in ('fonbet', 'pari')
                    and isinstance(live_cache, dict)
                )

                if is_fonbet_like:
                    # ═══════════════════════════════════════════════
                    # PATCH 2a (2026-10-29): для fonbet/pari — чистка
                    # ТОЛЬКО по возрасту от first_seen. Игнорируем
                    # upd>snt (у этих БК snt=0 для "висящих" live
                    # событий, которые не отправляются в агрегатор).
                    #
                    # Условия удаления:
                    #   1) first_seen > 10 минут
                    #   2) last_update > 2 минут (данные не менялись)
                    #
                    # Активные live (счёт меняется каждые 0.5-2 сек)
                    # под условие (2) НЕ попадают — остаются.
                    # ═══════════════════════════════════════════════
                    HARD_TTL = 600.0
                    for eid in list(cache.keys()):
                        _fs = first_seen.get(eid, 0) if isinstance(first_seen, dict) else 0
                        if not _fs:
                            continue
                        upd = last_update.get(eid, 0) if isinstance(last_update, dict) else 0
                        # Если данные обновлялись <120 сек назад — активные
                        if upd and (now - upd) < 120.0:
                            continue
                        if (now - _fs) > HARD_TTL:
                            cache.pop(eid, None)
                            if isinstance(live_cache, dict):
                                live_cache.pop(eid, None)
                            if isinstance(factors_cache, dict):
                                factors_cache.pop(eid, None)
                            for d in (last_update, last_sent, first_seen, *extra_caches):
                                if isinstance(d, dict):
                                    d.pop(eid, None)
                            removed += 1

                    if removed:
                        logger.info(
                            f"🧹 [cache-cleaner] [{p.bk_id}] "
                            f"удалено {removed}, осталось {len(cache)}"
                        )
                        total_removed += removed
                    else:
                        logger.debug(
                            f"🧹 [cache-cleaner] [{p.bk_id}] "
                            f"жив, в кэше {len(cache)}"
                        )
                    continue   # старая логика для fonbet/pari не нужна

                # ═══════════════════════════════════════════════════════
                # Старая логика для остальных БК (betcity, olimp,
                # winline, marathon, zenit, ligastavok, sportbet).
                # Работает нормально, не трогаем.
                # ═══════════════════════════════════════════════════════
                for eid in list(cache.keys()):
                    upd = last_update.get(eid, 0) if isinstance(last_update, dict) else 0
                    snt = last_sent.get(eid, 0) if isinstance(last_sent, dict) else 0

                    if upd > snt:
                        continue

                    if now - upd > ttl_seconds and now - snt > ttl_seconds:
                        cache.pop(eid, None)
                        for d in (last_update, last_sent, first_seen, *extra_caches):
                            if isinstance(d, dict):
                                d.pop(eid, None)
                        removed += 1

                if removed:
                    logger.info(
                        f"🧹 [cache-cleaner] [{p.bk_id}] "
                        f"удалено {removed}, осталось {len(cache)}"
                    )
                    total_removed += removed

            if total_removed:
                logger.debug(f"🧹 [cache-cleaner] всего удалено: {total_removed}")

        except Exception as e:
            logger.error(f"Ошибка cache-cleaner: {e}", exc_info=True)
            await asyncio.sleep(5)


async def main():
    logger.info("🚀 Запуск системы парсинга настольного тенниса")
    if DEBUG_PARSERS:
        logger.info("🔍 DEBUG_PARSERS=1 — логи парсеров включены (INFO)")

    detector = Detector(broadcast_callback=broadcast_message)
    aggregator = OddsAggregator(ttl=10)
    set_aggregator(aggregator)

    FONBET_SPORTS = ["table_tennis", "volleyball", "basketball", "cyber_basketball"]
    BETCITY_SPORTS = ["table_tennis", "volleyball", "basketball", "cyber_basketball"]
    OLIMP_SPORTS = ["table_tennis", "volleyball", "basketball", "cyber_basketball"]
    SPORTBET_SPORTS = ["table_tennis", "volleyball", "basketball"]
    ZENIT_SPORTS = ["table_tennis", "volleyball", "basketball", "cyber_basketball"]
    LIGASTAVOK_SPORTS = ["table_tennis", "volleyball", "basketball", "cyber_basketball"]
    #LEON_SPORTS = ["table_tennis", "volleyball", "basketball", "cyber_basketball"]
    MARATHON_SPORTS = ["table_tennis", "volleyball", "basketball", "cyber_basketball"]
    WINLINE_SPORTS = ["table_tennis", "volleyball", "basketball", "cyber_basketball"]

    parsers = [
        FonbetApiParser(detector=detector, aggregator=aggregator, enabled_sports=FONBET_SPORTS),
        PariApiParser(detector=detector, aggregator=aggregator, enabled_sports=FONBET_SPORTS),
        WinlineApiParser(detector=detector, aggregator=aggregator, enabled_sports=WINLINE_SPORTS),
        LigaStavokApiParser(detector=detector, aggregator=aggregator, enabled_sports=LIGASTAVOK_SPORTS),
        #LeonApiParser(detector=detector, aggregator=aggregator, enabled_sports=LEON_SPORTS),
        OlimpApiParser(detector=detector, aggregator=aggregator, enabled_sports=OLIMP_SPORTS),
        BetcityApiParser(detector=detector, aggregator=aggregator, enabled_sports=BETCITY_SPORTS),
        MarathonApiParser(detector=detector, aggregator=aggregator, enabled_sports=MARATHON_SPORTS),
        ZenitApiParser(detector=detector, aggregator=aggregator, enabled_sports=ZENIT_SPORTS),
        SportbetApiParser(detector=detector, aggregator=aggregator, enabled_sports=SPORTBET_SPORTS),
    ]

    logger.info(f"✅ Создано {len(parsers)} парсеров")
    if PAGE_RELOAD_ENABLED:
        logger.info(
            f"🔄 Watchdog залипаний: проверка раз в 20с, "
            f"порог 45с без данных (сдвиг {PAGE_RELOAD_STAGGER}с)"
        )
    if PAGE_KEEP_FRONT:
        logger.info(
            f"👁 Keep-alive для: {', '.join(PAGE_KEEP_FRONT)} "
            f"(раз в {PAGE_KEEP_FRONT_INTERVAL}с)"
        )

    asyncio.create_task(print_stats(aggregator))
    asyncio.create_task(global_cache_cleaner(parsers, ttl_seconds=900.0))
    asyncio.create_task(_match_state_pusher(aggregator))
    asyncio.create_task(detector.cleanup_loop())

    # ── Обёртки с рестартом + Health Monitor ──
    from core.health_monitor import ManagedParser, HealthMonitor

    managed = {}
    # enumerate → index → ManagedParser._restart_offset = index * 60.
    # Сдвигаем плановые рестарты, чтобы они не пиковали.
    for i, p in enumerate(parsers):
        mp = ManagedParser(p, index=i)
        mp.start()
        managed[p.bk_id] = mp

    health_monitor = HealthMonitor(
        managed,
        check_interval=30.0,
        broadcast_callback=broadcast_message,
    )
    asyncio.create_task(health_monitor.run())
    logger.info(f"🩺 Health Monitor запущен для {len(managed)} парсеров")

    # Для /health/parsers
    from api import set_health_monitor
    set_health_monitor(health_monitor)

    config = uvicorn.Config(
        app, host="0.0.0.0", port=8000, loop="asyncio",
        ws_ping_interval=None,
        ws_ping_timeout=None,
        ws_max_size=8 * 1024 * 1024,
    )
    server = uvicorn.Server(config)
    asyncio.create_task(server.serve())

    logger.info("🌐 API запущен на http://0.0.0.0:8000")

    # Задачи парсеров живут сами в фоне (в managed[k].task).
    # main НЕ должен ждать их через gather, иначе рестарт из health_monitor
    # (task.cancel()) уронит весь процесс.
    try:
        await asyncio.Event().wait()   # бесконечное ожидание до Ctrl+C
    except KeyboardInterrupt:
        logger.info("🛑 Остановка по Ctrl+C")
        for mp in managed.values():
            mp.parser.is_running = False
        for mp in managed.values():
            try:
                await mp.stop()
            except Exception:
                pass
        await browser_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())