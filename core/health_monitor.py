# core/health_monitor.py
"""
Центральный монитор здоровья всех парсеров.

Каждые 30 сек проверяет:
  - Жива ли asyncio-задача парсера
  - Когда последний раз приходили данные от источника
  - Не «залип» ли кэш (нет обновлений > dead_threshold)
  - Не пора ли сделать ПЛАНОВЫЙ рестарт по возрасту (раз в час)

Действия:
  - warn (лог)             — если тишина > warn_threshold
  - dead (рестарт)         — если тишина > dead_threshold, или задача упала
  - scheduled (рестарт)    — если uptime >= FORCED_RESTART_INTERVAL + offset
  - broadcast в WS         — статус каждые 30 сек для фронта

Защита от шторма:
  - Не более 20 рестартов в час на каждый парсер
  - Сдвиг по индексу парсера (index * 60 сек), чтобы рестарты
    не пиковали одновременно (9 парсеров → растянуто на 8 минут)
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable

logger = logging.getLogger(__name__)


# Пороги (сек): warn, dead
# Для WS-парсеров (push) — жёстче, для HTTP-polling — мягче
DEAD_THRESHOLDS: Dict[str, tuple] = {
    # WS-парсеры (push): данные идут постоянно, даже при малом числе матчей
    "winline":    (90, 300),
    "zenit":      (90, 300),

    # HTTP polling с delta (vtag) — могут молчать при простое
    "sportbet":   (120, 420),
    "marathon":   (120, 420),
    "ligastavok": (120, 480),
    "betcity":    (120, 420),
    "fonbet":     (120, 420),
    "leon":       (180, 600),   # Leon особенно редко шлёт изменения
    "olimp":      (120, 420),
}
DEFAULT_THRESHOLDS = (90, 300)
WARMUP_SEC = 120        # первые 2 минуты после старта не считаем «мёртвым»


@dataclass
class ParserHealth:
    bk_id: str
    status: str = "unknown"          # ok / warn / dead / warmup / disabled
    is_running: bool = False
    cache_size: int = 0
    last_update_ago: float = -1.0    # сек назад, -1 если вообще не было
    last_sent_ago: float = -1.0
    uptime_sec: float = 0.0
    restarts_last_hour: int = 0
    note: str = ""


class ManagedParser:
    """Обёртка над парсером — держит задачу, позволяет рестарт."""

    def __init__(self, parser, index: int = 0):
        self.parser = parser
        self.bk_id = getattr(parser, "bk_id", "?")
        self.task: Optional[asyncio.Task] = None
        self.started_at: float = time.time()
        self.restart_times: List[float] = []

        # ── Сдвиг рестарта ──
        # Каждому парсеру даём смещение index * 60 сек, чтобы плановые
        # рестарты не срабатывали одновременно. При 9 парсерах — растянуты
        # на 8 минут. Иначе будет пик CPU + API-лимитов (особенно Olimp).
        self.index = index
        self._restart_offset = index * 60.0

        # ── Защита от двойного рестарта ──
        self._restarting = False

    def start(self):
        self.parser.is_running = True
        self.started_at = time.time()
        self.task = asyncio.create_task(self.parser.run())

    async def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await asyncio.wait_for(self.task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
        try:
            await self.parser.stop()
        except Exception:
            pass

    async def restart(self) -> bool:
        """Пробует перезапустить. Возвращает False если лимит исчерпан.
        Никогда не выбрасывает наружу — иначе уронит HealthMonitor."""
        now = time.time()
        self.restart_times = [t for t in self.restart_times if now - t < 3600]
        if len(self.restart_times) >= 20:
            return False

        logger.warning(f"[health] 🔄 Рестарт [{self.bk_id}]")

        try:
            await self.stop()
        except Exception as e:
            logger.warning(f"[health] stop [{self.bk_id}]: {e}")

        await asyncio.sleep(2)

        try:
            self.start()
            self.restart_times.append(time.time())
            logger.info(
                f"[health] ✅ [{self.bk_id}] перезапущен "
                f"(рестартов за час: {len(self.restart_times)})"
            )
            return True
        except Exception as e:
            logger.error(f"[health] start [{self.bk_id}]: {e}", exc_info=True)
            return False

    def health(self) -> ParserHealth:
        p = self.parser
        now = time.time()

        # cache size — максимум из возможных кэшей
        cache_size = 0
        for attr in ("_matches_cache", "_events_cache", "_live_cache"):
            c = getattr(p, attr, None)
            if isinstance(c, dict) and len(c) > cache_size:
                cache_size = len(c)

        # last update — максимальный timestamp во всех временных кэшах
        last_update = 0.0
        for attr in ("_last_update_time", "_last_sent_time"):
            d = getattr(p, attr, None)
            if isinstance(d, dict) and d:
                last_update = max(last_update, max(d.values()))

        # Если у парсера есть _last_successful_response_at — это более
        # честный признак «я жив». Leon, например, отвечает 200 OK,
        # но при отсутствии live-матчей cache не растёт.
        lsr = getattr(p, "_last_successful_response_at", None)
        if isinstance(lsr, (int, float)) and lsr > 0:
            last_update = max(last_update, lsr)

        last_sent = 0.0
        d_sent = getattr(p, "_last_sent_time", None)
        if isinstance(d_sent, dict) and d_sent:
            last_sent = max(d_sent.values())
        # если last_sent нет — используем last_update как fallback
        if last_sent == 0 and last_update > 0:
            last_sent = last_update

        return ParserHealth(
            bk_id=self.bk_id,
            is_running=getattr(p, "is_running", False),
            cache_size=cache_size,
            last_update_ago=(now - last_update) if last_update > 0 else -1.0,
            last_sent_ago=(now - last_sent) if last_sent > 0 else -1.0,
            uptime_sec=now - self.started_at,
            restarts_last_hour=len(self.restart_times),
        )


class HealthMonitor:
    # ── Плановый рестарт раз в час ──
    # Парсеры текут со временем: WS-соединения теряют кадры, HTTP-сессии
    # накапливают stale cookies, cookies LigaStavok qrator протухает.
    # Периодический рестарт сбрасывает всё это.
    #
    # Сдвиг по индексу парсера (ManagedParser._restart_offset) гарантирует
    # что рестарты НЕ пикуют: 9 парсеров рестартуют в течение 8 минут,
    # а не одновременно.
    FORCED_RESTART_INTERVAL = 3600.0   # 1 час

    def __init__(
        self,
        managed: Dict[str, ManagedParser],
        check_interval: float = 30.0,
        broadcast_callback: Optional[Callable] = None,
    ):
        self.managed = managed
        self.check_interval = check_interval
        self.broadcast = broadcast_callback
        self._last_broadcast = 0.0
        self._snapshot: Dict[str, ParserHealth] = {}

    def _analyze(self, mp: ManagedParser) -> ParserHealth:
        h = mp.health()

        warn_s, dead_s = DEAD_THRESHOLDS.get(mp.bk_id, DEFAULT_THRESHOLDS)

        if not h.is_running:
            h.status = "disabled"
            h.note = "is_running=False"
            return h

        # Задача упала
        if mp.task and mp.task.done() and h.is_running:
            exc = None
            try:
                exc = mp.task.exception()
            except Exception:
                pass
            h.status = "dead"
            h.note = f"task завершилась, exc={exc!r}"
            return h

        # Warmup — первые 2 минуты не считаем по тишине
        if h.uptime_sec < WARMUP_SEC:
            h.status = "warmup"
            h.note = f"стартует ({h.uptime_sec:.0f}с)"
            return h

        # Нет данных вообще
        if h.last_update_ago < 0:
            if h.cache_size == 0:
                h.status = "warn"
                h.note = "нет данных с запуска"
            else:
                h.status = "ok"
            return h

        if h.last_update_ago >= dead_s:
            h.status = "dead"
            h.note = f"тишина {h.last_update_ago:.0f}с (порог {dead_s}с)"
        elif h.last_update_ago >= warn_s:
            h.status = "warn"
            h.note = f"тишина {h.last_update_ago:.0f}с"
        else:
            h.status = "ok"
        return h

    async def _restart_one(self, mp: ManagedParser, reason: str):
        """
        Асинхронный рестарт одного парсера.
        Не блокирует _check_all — иначе 9 рестартов растянутся на минуту.
        """
        if mp._restarting:
            logger.debug(f"[health] [{mp.bk_id}] рестарт уже идёт — skip")
            return
        mp._restarting = True
        try:
            try:
                ok = await mp.restart()
            except Exception as e:
                logger.error(
                    f"[health] критическая ошибка рестарта [{mp.bk_id}]: {e}",
                    exc_info=True,
                )
                ok = False

            if not ok:
                logger.error(
                    f"[health] ⛔ [{mp.bk_id}] рестартов за час больше 20 — пауза"
                )
                return

            logger.warning(
                f"[health] 🔄 Рестарт [{mp.bk_id}] выполнен (reason={reason})"
            )

            if self.broadcast:
                try:
                    await self.broadcast({
                        "type": "health_restart",
                        "payload": {
                            "bk_id": mp.bk_id,
                            "reason": reason,
                            "ts": time.time(),
                        },
                    })
                except Exception:
                    pass
        finally:
            mp._restarting = False

    async def _check_all(self):
        to_restart: List[tuple] = []
        now = time.time()

        for bk_id, mp in self.managed.items():
            h = self._analyze(mp)
            self._snapshot[bk_id] = h

            if h.status == "warn":
                logger.warning(f"[health] ⚠️ [{bk_id}] {h.note}")

            elif h.status == "dead":
                logger.error(f"[health] ❌ [{bk_id}] {h.note}")
                to_restart.append((mp, "dead"))

            elif h.status == "ok":
                # ── ПЛАНОВЫЙ РЕСТАРТ ПО UPTIME ──
                # Срабатывает раз в FORCED_RESTART_INTERVAL + сдвиг.
                # Парсеры накапливают stale state (cookies, WS-буферы,
                # кэши) — периодический рестарт чистит это.
                uptime = now - mp.started_at
                threshold = self.FORCED_RESTART_INTERVAL + mp._restart_offset
                if uptime >= threshold:
                    logger.info(
                        f"[health] 🔄 [{bk_id}] плановый рестарт "
                        f"(uptime {uptime/60:.0f} мин, "
                        f"порог {(threshold/60):.0f} мин)"
                    )
                    to_restart.append((mp, "scheduled"))

        # Рестарты запускаем асинхронно — не блокируем _check_all.
        # Каждый рестарт — отдельная task, ~7 сек (stop + sleep + start).
        for mp, reason in to_restart:
            asyncio.create_task(self._restart_one(mp, reason))

        # Broadcast раз в 30 сек
        if self.broadcast:
            if now - self._last_broadcast >= 30:
                self._last_broadcast = now
                try:
                    await self.broadcast({
                        "type": "health",
                        "payload": {
                            bk: {
                                "status": h.status,
                                "note": h.note,
                                "cache_size": h.cache_size,
                                "last_update_ago": round(h.last_update_ago, 1)
                                    if h.last_update_ago >= 0 else None,
                                "last_sent_ago": round(h.last_sent_ago, 1)
                                    if h.last_sent_ago >= 0 else None,
                                "uptime_sec": int(h.uptime_sec),
                                "restarts_last_hour": h.restarts_last_hour,
                            }
                            for bk, h in self._snapshot.items()
                        },
                    })
                except Exception:
                    pass

    def snapshot(self) -> Dict:
        """Публичный доступ к последнему снимку (для /health/parsers)."""
        return {
            bk: {
                "status": h.status,
                "note": h.note,
                "is_running": h.is_running,
                "cache_size": h.cache_size,
                "last_update_ago": round(h.last_update_ago, 1)
                    if h.last_update_ago >= 0 else None,
                "last_sent_ago": round(h.last_sent_ago, 1)
                    if h.last_sent_ago >= 0 else None,
                "uptime_sec": int(h.uptime_sec),
                "restarts_last_hour": h.restarts_last_hour,
            }
            for bk, h in self._snapshot.items()
        }

    async def run(self):
        logger.info(
            f"[health] Монитор запущен: проверка раз в {self.check_interval:.0f}с, "
            f"плановый рестарт раз в {self.FORCED_RESTART_INTERVAL/60:.0f} мин "
            f"(+ сдвиг 60с на каждый парсер)"
        )
        # первый прогон через 15 сек
        await asyncio.sleep(15)
        while True:
            try:
                await self._check_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[health] ошибка: {e}", exc_info=True)
            await asyncio.sleep(self.check_interval)