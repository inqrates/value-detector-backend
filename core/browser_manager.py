# core/browser_manager.py
import asyncio
import logging
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from config import HEADLESS, VIEWPORT_WIDTH, VIEWPORT_HEIGHT

logger = logging.getLogger(__name__)

# ── Нормализация viewport ──
# VIEWPORT_HEIGHT в config.py может быть 4000 (для полной lazy-загрузки
# матчей LigaStavok). Такой размер палит Qrator на входе — поэтому контекст
# создаём с "человеческим" 1080, а конкретный сборщик куки ставит 4000
# через page.set_viewport_size() ПОСЛЕ прохождения Qrator.
_SAFE_VH = min(int(VIEWPORT_HEIGHT or 1080), 2160)


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
                logger.info("🌐 Запуск Chromium (headless=new + антидетект)")
                self._pw = await async_playwright().start()

                self._browser = await self._pw.chromium.launch(
                    headless=True,
                    args=[
                        # ── Headless нового поколения ──
                        # Полноценный рендер через SwiftShader: работает
                        # requestAnimationFrame, WebGL, canvas — критично
                        # для прохождения Qrator без активной RDP-сессии.
                        '--headless=new',

                        # ── Антидетект ──
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-dev-shm-usage',

                        # ── Стабильный размер окна ──
                        '--window-size=1920,1080',
                        '--window-position=0,0',

                        # ── Антисон для фоновых вкладок ──
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding',
                        '--disable-features='
                            'CalculateNativeWinOcclusion,'
                            'IntensiveWakeUpThrottling',
                        '--disable-ipc-flooding-protection',

                        # ── ВАЖНО: НЕ отключаем GPU / rasterizer ──
                        # Раньше было --disable-gpu, --disable-gpu-compositing,
                        # --disable-software-rasterizer, --blink-settings=
                        # imagesEnabled=false, --disable-images.
                        # Все они ломают WebGL/canvas fingerprint и Qrator
                        # отказывает. Не возвращать.
                    ],
                )

                self._context = await self._browser.new_context(
                    viewport={'width': int(VIEWPORT_WIDTH or 1920),
                              'height': _SAFE_VH},
                    no_viewport=False,
                    ignore_https_errors=True,
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                    locale="ru-RU",
                    timezone_id="Europe/Moscow",
                    device_scale_factor=1,
                    java_script_enabled=True,
                )

                # ── Антидетект: подмена полей, которые смотрит Qrator ──
                await self._context.add_init_script(self._antidetect_js())

                # ── Route: блокируем только рекламу ──
                # image / media / font НЕ блокируем — Qrator использует
                # canvas-картинку как challenge.
                try:
                    await self._context.route("**/*", self._route_handler)
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось установить route: {e}")

                logger.info("✅ Браузер запущен (headless=new, антидетект)")
            return self._browser

    @staticmethod
    def _antidetect_js() -> str:
        return r"""
        // webdriver
        try {
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        } catch (e) {}

        // languages
        try {
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ru-RU', 'ru', 'en-US', 'en']
            });
        } catch (e) {}

        // hardwareConcurrency / deviceMemory
        try {
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8
            });
        } catch (e) {}
        try {
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 8
            });
        } catch (e) {}

        // plugins
        try {
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {name: 'Chrome PDF Plugin'},
                    {name: 'Chrome PDF Viewer'},
                    {name: 'Native Client'},
                ]
            });
        } catch (e) {}

        // chrome runtime
        try {
            if (!window.chrome) window.chrome = {};
            if (!window.chrome.runtime) window.chrome.runtime = {};
        } catch (e) {}

        // WebGL vendor / renderer — Qrator собирает fingerprint
        try {
            const patchGL = (proto) => {
                const orig = proto.getParameter;
                proto.getParameter = function(p) {
                    if (p === 37445) return 'Intel Inc.';
                    if (p === 37446) return 'Intel Iris OpenGL Engine';
                    return orig.call(this, p);
                };
            };
            patchGL(WebGLRenderingContext.prototype);
            patchGL(WebGL2RenderingContext.prototype);
        } catch (e) {}

        // permissions — Playwright ставит 'denied', Qrator видит
        try {
            const origQuery = navigator.permissions.query.bind(
                navigator.permissions
            );
            navigator.permissions.query = (params) => {
                if (params && params.name === 'notifications') {
                    return Promise.resolve({ state: 'default' });
                }
                return origQuery(params);
            };
        } catch (e) {}
        """

    @staticmethod
    async def _route_handler(route, request):
        """
        Блокируем только явную рекламу/трекеры.
        image / font / media / css НЕ трогаем — Qrator использует их
        в canvas/challenge.
        """
        try:
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