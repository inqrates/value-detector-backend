# scripts/sniff_fonbet_browser.py
"""
Слушает ВСЕ сетевые запросы (XHR/fetch/WS) при живой сессии fon.bet.
Ты просто открываешь матчи, скрипт логирует куда браузер ходит.

Запуск:
    python scripts/sniff_fonbet_browser.py
Затем:
    1. Откроется Chromium на fon.bet/live/football
    2. Кликай по матчам, открывай рынки, статистику
    3. Через 120 сек (или Ctrl+C) скрипт сохранит всё в sniff_out/browser/
"""
import asyncio
import json
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright

OUT = Path("sniff_out/browser")
OUT.mkdir(parents=True, exist_ok=True)

DURATION = 120  # сколько слушать

# что логировать — только интересные домены
TARGET_DOMAINS = (
    "bk6bba-resources",
    "bkfon-resources",
    "fon.bet",
)

_captured = []


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        ctx = await browser.new_context(
            viewport={"width": 1600, "height": 1000},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()

        # ---- перехват ответов ----
        async def on_response(response):
            try:
                url = response.url
                host = urlparse(url).netloc
                if not any(d in host for d in TARGET_DOMAINS):
                    return

                rtype = response.request.resource_type
                if rtype not in ("xhr", "fetch", "websocket"):
                    return

                status = response.status
                if status != 200:
                    return

                # пробуем вытащить размер и preview
                size = 0
                preview = ""
                try:
                    body = await response.body()
                    size = len(body)
                    if rtype in ("xhr", "fetch"):
                        # пробуем json
                        try:
                            j = json.loads(body)
                            preview = json.dumps(j, ensure_ascii=False)[:300]
                        except Exception:
                            preview = body[:300].decode("utf-8", errors="replace")
                except Exception:
                    pass

                _captured.append({
                    "ts": time.time(),
                    "method": response.request.method,
                    "url": url,
                    "host": host,
                    "type": rtype,
                    "status": status,
                    "size": size,
                    "preview": preview,
                })
                print(f"[{size:>9}] {rtype:<10} {url[:150]}")
            except Exception as e:
                pass

        page.on("response", on_response)

        print(f"🌐 Открываю fon.bet...")
        await page.goto("https://fon.bet/live/football", wait_until="domcontentloaded")
        print(f"\n✅ Кликай по матчам, открывай рынки. Слушаю {DURATION} сек.\n")

        try:
            await asyncio.sleep(DURATION)
        except asyncio.CancelledError:
            pass

        # ---- сохраняем ----
        _captured.sort(key=lambda x: -x["size"])
        out_file = OUT / "network_log.json"
        out_file.write_text(
            json.dumps(_captured, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # ---- отчёт ----
        print(f"\n{'=' * 70}")
        print(f"  СОХРАНЕНО: {out_file} ({len(_captured)} записей)")
        print(f"{'=' * 70}")
        print(f"\n  ТОП-20 ПО РАЗМЕРУ:")
        for r in _captured[:20]:
            print(f"  {r['size']:>10} B | {r['method']:<5} | {r['url'][:130]}")

        # группируем по эндпоинту
        print(f"\n  УНИКАЛЬНЫЕ ЭНДПОИНТЫ (по path без query):")
        from collections import Counter
        endpoints = Counter()
        for r in _captured:
            p = urlparse(r["url"])
            endpoints[f"{r['method']} {p.netloc}{p.path}"] += 1
        for ep, cnt in endpoints.most_common(40):
            print(f"  {cnt:>4}x  {ep}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())