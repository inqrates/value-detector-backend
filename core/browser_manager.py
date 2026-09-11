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
                    ]
                )
                self._context = await self._browser.new_context(
                    viewport={'width': VIEWPORT_WIDTH, 'height': VIEWPORT_HEIGHT},
                    no_viewport=False,
                )
                logger.info("✅ Общий браузер и контекст запущены")
            return self._browser

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