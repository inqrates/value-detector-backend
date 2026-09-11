# parsers/base.py
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from playwright.async_api import Page
from core.browser_manager import browser_manager
from config import (
    TABLE_TENNIS_URLS, VIEWPORT_WIDTH, VIEWPORT_HEIGHT,
    PAGE_LOAD_TIMEOUT, PAGE_STABILIZE_TIME, ZOOM, PARSE_INTERVAL,
    PAGE_RELOAD_ENABLED, PAGE_RELOAD_STAGGER,
    PAGE_KEEP_FRONT, PAGE_KEEP_FRONT_INTERVAL,
    PAGE_STUCK_TIMEOUT_BY_BK, PAGE_STUCK_TIMEOUT_DEFAULT,
    PAGE_PERIODIC_RELOAD_BY_BK,
)

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    def __init__(self, bk_id: str, detector=None, aggregator=None, verbose=False):
        self.bk_id = bk_id
        self.detector = detector
        self.aggregator = aggregator
        self.url = TABLE_TENNIS_URLS[bk_id]
        self.page: Page = None
        self.is_running = False
        self._verbose = verbose

        # ---- Антисон: задачи ----
        self._reload_task: asyncio.Task = None
        self._keep_front_task: asyncio.Task = None

        # Ограничение логирования количества матчей
        self._last_match_count = None
        self._last_log_time = 0
        self._log_interval = 30

        # Защита от зависаний
        self._stuck_counter = 0
        self._stuck_threshold = 30
        self._last_reload_time = 0

    def _log_matches_collected(self, count):
        now = time.time()
        if count != self._last_match_count or (now - self._last_log_time) > self._log_interval:
            logger.info(f"[{self.bk_id}] 🟢 Найдено матчей: {count}")
            self._last_match_count = count
            self._last_log_time = now

    async def _ensure_page(self):
        if self.page is None or self.page.is_closed():
            self.page = await browser_manager.new_page()
            logger.info(f"[{self.bk_id}] Открыта вкладка")

            await self.page.goto(self.url, wait_until='load', timeout=PAGE_LOAD_TIMEOUT)
            await self.page.wait_for_timeout(PAGE_STABILIZE_TIME)

            await self.page.evaluate(f"document.body.style.zoom = '{int(ZOOM * 100)}%'")
            await self.page.wait_for_timeout(500)
            logger.info(f"[{self.bk_id}] ✅ Страница загружена")

    async def start(self):
        await self._ensure_page()
        self._start_keeper_tasks()

    # ---------- Антисон: задачи ----------
    def _start_keeper_tasks(self):
        if PAGE_RELOAD_ENABLED:
            if self._reload_task is None or self._reload_task.done():
                self._reload_task = asyncio.create_task(self._reload_loop())

        if self.bk_id in PAGE_KEEP_FRONT:
            if self._keep_front_task is None or self._keep_front_task.done():
                self._keep_front_task = asyncio.create_task(self._keep_front_loop())

    def _stop_keeper_tasks(self):
        for task in (self._reload_task, self._keep_front_task):
            if task and not task.done():
                try:
                    task.cancel()
                except Exception:
                    pass
        self._reload_task = None
        self._keep_front_task = None

    async def _reload_loop(self):
        """
        Комбинированный watchdog:
        - Периодический reload (для БК из PAGE_PERIODIC_RELOAD_BY_BK) —
          чтобы подтягивались новые матчи.
        - Watchdog залипаний — reload, если БК давно не отдавала данные.
        """
        STUCK_TIMEOUT = PAGE_STUCK_TIMEOUT_BY_BK.get(
            self.bk_id, PAGE_STUCK_TIMEOUT_DEFAULT
        )
        # Периодический интервал (для Sportbet и подобных), может быть None
        PERIODIC_INTERVAL = PAGE_PERIODIC_RELOAD_BY_BK.get(self.bk_id)
        CHECK_INTERVAL = 15
        MIN_UPTIME = 30 if self.bk_id in PAGE_KEEP_FRONT else 60
        MAX_SILENT_RELOADS = 3

        # Сдвиг старта, чтобы БК не перезагружались одновременно
        bk_order = [
            'fonbet', 'winline', 'ligastavok', 'leon', 'olimp',
            'betcity', 'marathon', 'zenit', 'sportbet',
        ]
        try:
            idx = bk_order.index(self.bk_id)
        except ValueError:
            idx = 0
        initial_delay = PAGE_RELOAD_STAGGER * idx

        mode_desc = []
        if PERIODIC_INTERVAL:
            mode_desc.append(f"период {PERIODIC_INTERVAL}с")
        mode_desc.append(f"залипание {STUCK_TIMEOUT}с")
        logger.info(
            f"[{self.bk_id}] 🔄 Watchdog: проверка раз в {CHECK_INTERVAL}с "
            f"({', '.join(mode_desc)}, сдвиг {initial_delay}с)"
        )
        await asyncio.sleep(initial_delay)

        started_at = time.time()
        last_reload_at = started_at
        silent_reloads = 0

        while self.is_running:
            try:
                await asyncio.sleep(CHECK_INTERVAL)

                if time.time() - started_at < MIN_UPTIME:
                    continue

                if not self.page or self.page.is_closed():
                    continue

                now = time.time()
                need_reload = False
                reason = ""

                # --- Проверка 1: периодический reload ---
                if PERIODIC_INTERVAL and (now - last_reload_at) >= PERIODIC_INTERVAL:
                    need_reload = True
                    reason = f"периодический ({int(now - last_reload_at)}с)"

                # --- Проверка 2: залипание ---
                if not need_reload:
                    last_send = getattr(self, '_last_sent_time', None)
                    if last_send:
                        most_recent = max(last_send.values()) if last_send else 0
                        silence = now - most_recent
                        if silence >= STUCK_TIMEOUT:
                            need_reload = True
                            reason = f"залипание ({int(silence)}с)"

                if not need_reload:
                    silent_reloads = 0
                    continue

                logger.warning(f"[{self.bk_id}] 🔄 Reload — {reason}")
                try:
                    await self.page.reload(
                        wait_until='domcontentloaded',
                        timeout=PAGE_LOAD_TIMEOUT
                    )
                    await asyncio.sleep(PAGE_STABILIZE_TIME / 1000)
                    logger.info(f"[{self.bk_id}] ✅ Перезагрузка завершена")
                    last_reload_at = time.time()
                    silent_reloads = 0
                    # Сбрасываем счётчик "нет данных" для новой страницы
                    started_at = time.time()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warning(f"[{self.bk_id}] Ошибка reload: {e}")
                    silent_reloads += 1

                if silent_reloads >= MAX_SILENT_RELOADS:
                    logger.error(
                        f"[{self.bk_id}] ❌ {MAX_SILENT_RELOADS} reload подряд "
                        f"без результата, пауза 5 мин"
                    )
                    await asyncio.sleep(300)
                    silent_reloads = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.bk_id}] watchdog: {e}")
                await asyncio.sleep(30)

    async def _keep_front_loop(self):
        """Мягкий пинг + активная вкладка внутри Chromium (для Betcity)."""
        logger.info(
            f"[{self.bk_id}] 👁 Keep-alive: активная вкладка + пинг раз в "
            f"{PAGE_KEEP_FRONT_INTERVAL}с"
        )
        while self.is_running:
            try:
                await asyncio.sleep(PAGE_KEEP_FRONT_INTERVAL)
                if not self.page or self.page.is_closed():
                    continue

                try:
                    await self.page.bring_to_front()
                except Exception:
                    pass

                try:
                    await self.page.evaluate(
                        "() => { window.__keepalive = (window.__keepalive || 0) + 1; "
                        "return window.__keepalive; }"
                    )
                except Exception:
                    pass

                logger.debug(f"[{self.bk_id}] keep-alive ping")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[{self.bk_id}] keep-alive: {e}")

    # ---------- Остановка ----------
    async def stop(self):
        self._stop_keeper_tasks()
        if self.page and not self.page.is_closed():
            await browser_manager.close_page(self.page)
            logger.info(f"[{self.bk_id}] Вкладка закрыта")
        self.page = None

    # ---------- Основной цикл ----------
    async def run(self):
        logger.info(f"[{self.bk_id}] 🚀 Парсер запущен")
        while True:
            try:
                await self.start()
                self.is_running = True
                while self.is_running:
                    start_time = time.time()
                    try:
                        matches = await self.parse()
                    except Exception as parse_err:
                        logger.error(f"[{self.bk_id}] Ошибка parse(): {parse_err}", exc_info=True)
                        matches = []

                    if matches:
                        count = len(matches)
                        self._log_matches_collected(count)

                        if self._last_match_count == count:
                            self._stuck_counter += 1
                        else:
                            self._stuck_counter = 0
                            self._last_match_count = count

                        if self._stuck_counter >= self._stuck_threshold:
                            now = time.time()
                            if now - self._last_reload_time > 30:
                                logger.warning(f"[{self.bk_id}] Страница зависла, перезагрузка...")
                                await self.stop()
                                await asyncio.sleep(1)
                                await self.start()
                                self._stuck_counter = 0
                                self._last_reload_time = now
                                continue

                        if self.detector:
                            try:
                                await asyncio.gather(*[self.detector.process(m) for m in matches])
                            except Exception as det_err:
                                logger.error(f"[{self.bk_id}] Ошибка детектора: {det_err}", exc_info=True)
                        if self.aggregator:
                            try:
                                for m in matches:
                                    self.aggregator.update(m)
                            except Exception as agg_err:
                                logger.error(f"[{self.bk_id}] Ошибка агрегатора: {agg_err}", exc_info=True)
                    else:
                        self._stuck_counter = 0
                        self._last_match_count = None
                        await asyncio.sleep(1)
                        continue

                    elapsed = time.time() - start_time
                    sleep_time = max(0, PARSE_INTERVAL - elapsed)
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                    else:
                        if self._verbose:
                            logger.warning(f"[{self.bk_id}] Парсинг занял {elapsed:.2f}с, превышает интервал")
            except Exception as e:
                logger.error(f"[{self.bk_id}] Критическая ошибка, перезапуск через 5 сек: {e}", exc_info=True)
                await self.stop()
                await asyncio.sleep(5)

    @abstractmethod
    async def parse(self):
        pass