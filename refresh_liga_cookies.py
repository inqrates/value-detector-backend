# refresh_liga_cookies.py  (версия с invisible_playwright)
import asyncio
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    stream=sys.stdout,
)
for lib in ('websockets', 'curl_cffi', 'playwright', 'invisible_playwright'):
    logging.getLogger(lib).setLevel(logging.WARNING)

COOKIES_PATH = Path("cookies/ligastavok.json")

# ════════════════════════════════════════════════════════════
# Тот же двухфазный алгоритм, но на InvisiblePlaywright (Firefox)
# ════════════════════════════════════════════════════════════
async def collect():
    from invisible_playwright.async_api import InvisiblePlaywright

    # Сохраняем старый файл
    if COOKIES_PATH.exists():
        bak = COOKIES_PATH.with_suffix(".json.bak")
        COOKIES_PATH.replace(bak)
        print(f"📦 Старый файл сохранён как {bak.name}")

    async with InvisiblePlaywright(headless=True) as browser:
        page = await browser.new_page()

        # ── Фаза 1: Qrator при 1920×1080 ──
        await page.set_viewport_size({"width": 1920, "height": 1080})
        print("Фаза 1: Qrator (1920×1080)")

        await page.goto(
            "https://www.ligastavok.ru/live",
            wait_until="domcontentloaded",
            timeout=60000,
        )

        MAX_WAIT = 60.0
        start = asyncio.get_event_loop().time()
        passed = False
        last_log = 0.0

        while asyncio.get_event_loop().time() - start < MAX_WAIT:
            await asyncio.sleep(1.5)
            elapsed = asyncio.get_event_loop().time() - start

            # InvisiblePlaywright сам делает «человеческие» движения,
            # но дополнительные не помешают
            try:
                await page.mouse.move(
                    200 + (int(elapsed * 137) % 1200),
                    150 + (int(elapsed * 91) % 600),
                    steps=5,
                )
            except Exception:
                pass
            try:
                await page.evaluate(
                    f"window.scrollTo(0, {int((elapsed * 150) % 800)})"
                )
            except Exception:
                pass

            cur = await page.context.cookies()
            names = {(c.get("name") or "").lower() for c in cur}
            has_qrator = "qrator_jsr" in names
            title = (await page.title()) or ""
            try:
                body_len = len(await page.locator("body").inner_text())
            except Exception:
                body_len = 0

            tl = title.lower()
            blocked = any(m in tl for m in (
                "проверка", "qrator", "challenge",
                "just a moment", "ddos-guard", "безопасности",
            ))

            if has_qrator and not blocked and body_len > 500:
                passed = True
                print(
                    f"✅ Qrator пройден за {elapsed:.1f}с "
                    f"(cookies={len(cur)}, body={body_len})"
                )
                break

            if elapsed - last_log >= 5.0:
                last_log = elapsed
                print(
                    f"  Qrator: {elapsed:.0f}/{MAX_WAIT:.0f}с "
                    f"cookies={len(cur)} qrator_jsr={has_qrator} "
                    f"title={title[:50]!r} body={body_len}"
                )

        if not passed:
            print("❌ Qrator НЕ пройден за 60с")
            return False

        # ── Фаза 2: ресайз до 4000 для lazy-load всех матчей ──
        print("Фаза 2: viewport 1920×4000 — грузим все матчи")
        await page.set_viewport_size({"width": 1920, "height": 4000})
        await asyncio.sleep(2.0)

        for y in [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000,
                  5000, 3000, 1000, 0]:
            try:
                await page.evaluate(f"window.scrollTo(0, {y})")
            except Exception:
                pass
            await asyncio.sleep(0.4)

        # Ждём стабилизации DOM
        try:
            prev = 0
            stable = 0
            for _ in range(20):
                cnt = await page.evaluate(
                    """() => document.querySelectorAll(
                        "[class*='event'], [class*='Event'], "
                        "a[href*='/sports/']"
                    ).length"""
                )
                if cnt == prev:
                    stable += 1
                    if stable >= 3:
                        break
                else:
                    stable = 0
                    prev = cnt
                await page.wait_for_timeout(500)
            print(f"после скролла в DOM ~{prev} элементов матчей")
        except Exception as e:
            print(f"DOM count: {e}")

        await asyncio.sleep(1.5)

        # ── Фаза 3: снимаем и сохраняем куки ──
        cookies = await page.context.cookies()
        names = {(c.get("name") or "").lower() for c in cookies}
        if "qrator_jsr" not in names:
            print("qrator_jsr пропал — не сохраняю")
            return False

        COOKIES_PATH.parent.mkdir(exist_ok=True)
        tmp = COOKIES_PATH.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"cookies": cookies}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(COOKIES_PATH)

        print(f"✅ Cookies сохранены: {len(cookies)} шт.")
        return True


if __name__ == "__main__":
    ok = asyncio.run(collect())
    if ok:
        print("\nТеперь запусти: python debug_ligastavok.py")
    else:
        print("\n❌ Не удалось. Смотри лог выше.")