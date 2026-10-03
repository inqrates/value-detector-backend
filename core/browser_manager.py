# core/browser_manager.py
import asyncio
import logging
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from config import HEADLESS, VIEWPORT_WIDTH, VIEWPORT_HEIGHT

logger = logging.getLogger(__name__)


class BrowserManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._pw = None
        self._browser: Browser = None
        self._context: BrowserContext = None
        self._pages_count = 0
        self._browser_lock: asyncio.Lock = None
        logger.debug("BrowserManager инициализирован (singleton)")

    def _ensure_lock(self):
        if self._browser_lock is None:
            self._browser_lock = asyncio.Lock()

    async def get_browser(self) -> Browser:
        self._ensure_lock()
        async with self._browser_lock:
            if self._browser is None or not self._browser.is_connected():
                logger.info("🌐 Запускаем общий браузер Chromium...")
                self._pw = await async_playwright().start()
                self._browser = await self._pw.chromium.launch(
                    headless=HEADLESS,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--start-maximized',

                        # ---- АНТИСОН: отключаем троттлинг фоновых вкладок ----
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding',
                        '--disable-features=CalculateNativeWinOcclusion,IntensiveWakeUpThrottling',
                        '--disable-ipc-flooding-protection',

                        # ---- ЭКОНОМИЯ ПАМЯТИ ----
                        # Не загружать и не рендерить картинки (в SPA это десятки МБ)
                        '--blink-settings=imagesEnabled=false',
                        '--disable-images',

                        # Отключить GPU-процесс и его буферы (~500 МБ на 9 вкладок)
                        '--disable-gpu',
                        '--disable-gpu-compositing',
                        '--disable-software-rasterizer',

                        # Ограничить V8 heap на вкладку (защита от утечек SPA)
                        '--js-flags=--max-old-space-size=256',

                        # Не держать в кэше лишние ресурсы
                        '--disk-cache-size=1',
                        '--media-cache-size=1',
                        '--disable-application-cache',
                        '--disable-offline-load-stale-cache',
                    ]
                )
                self._context = await self._browser.new_context(
                    viewport={'width': VIEWPORT_WIDTH, 'height': VIEWPORT_HEIGHT},
                    no_viewport=False,
                    # Не грузить картинки и медиа на уровне контекста тоже
                    ignore_https_errors=True,
                )

                # Дополнительный фильтр: обрывать запросы к картинкам/шрифтам/медиа
                # на уровне контекста (быстрее чем через page.route).
                try:
                    await self._context.route(
                        "**/*",
                        self._route_handler,
                    )
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось установить route-фильтр: {e}")

                logger.info("✅ Общий браузер и контекст запущены (оптимизация памяти)")
            return self._browser

    @staticmethod
    async def _route_handler(route, request):
        """
        Блокируем загрузку ресурсов, которые жрут память и трафик,
        но не нужны для перехвата API: картинки, медиа, шрифты, стили
        сторонних трекеров.
        """
        try:
            rtype = request.resource_type
            if rtype in ("image", "media", "font"):
                await route.abort()
                return
            # Блокируем явные рекламные/аналитические домены
            url = request.url
            block_markers = (
                "mc.yandex.ru", "google-analytics", "googletagmanager",
                "doubleclick.net", "facebook.net", "vk.com/rtrg",
                "top-fwz1.mail.ru", "adservice.google",
            )
            if any(m in url for m in block_markers):
                await route.abort()
                return
            await route.continue_()
        except Exception:
            # Не валим запрос из-за ошибок фильтра
            try:
                await route.continue_()
            except Exception:
                pass

    async def new_page(self) -> Page:
        await self.get_browser()
        page = await self._context.new_page()
        self._pages_count += 1
        logger.debug(f"📄 Открыта вкладка (всего активных: {self._pages_count})")
        return page

    async def close_page(self, page: Page):
        try:
            if not page.is_closed():
                await page.close()
        except Exception:
            pass
        self._pages_count = max(0, self._pages_count - 1)
        logger.debug(f"📄 Закрыта вкладка (осталось: {self._pages_count})")

    async def shutdown(self):
        self._ensure_lock()
        async with self._browser_lock:
            if self._context:
                try:
                    await self._context.close()
                    logger.info("📑 Контекст браузера закрыт")
                except Exception:
                    pass
            if self._browser:
                try:
                    await self._browser.close()
                    logger.info("🛑 Общий браузер остановлен")
                except Exception:
                    pass
            if self._pw:
                try:
                    await self._pw.stop()
                except Exception:
                    pass
            self._browser = None
            self._context = None
            self._pw = None
            self._pages_count = 0


browser_manager = BrowserManager()