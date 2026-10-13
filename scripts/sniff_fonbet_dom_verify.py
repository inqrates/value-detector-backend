# scripts/sniff_fonbet_dom_verify.py
"""
Гибридная сверка: API ↔ DOM Fonbet.

Что делает:
  1. Тянет свежий снапшот listBase
  2. Для каждого вида спорта находит топовый матч
  3. Для каждого матча:
       A) HTTP  → /ma/events/event?eventId=X  (полная линия из API)
       B) Playwright → открывает страницу матча на fon.bet,
          разворачивает ВСЕ рынки (вкладки + "показать все"),
          собирает DOM: (секция, рынок, линия, кэф)
  4. Сопоставляет:
       - по совпадению (line, кэф) находит fid ↔ человеческое название
       - выводит, где периоды (таймы/четверти) реально лежат
  5. Сохраняет всё в sniff_out/dom_verify/

Запуск:
  python scripts/sniff_fonbet_dom_verify.py
  python scripts/sniff_fonbet_dom_verify.py --sports football,table_tennis
  python scripts/sniff_fonbet_dom_verify.py --headless
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import time
from collections import defaultdict
from pathlib import Path

from curl_cffi.requests import AsyncSession
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout


BASE = "https://line-lb54-w.bk6bba-resources.com"
LIST_BASE_URL = f"{BASE}/ma/events/listBase"
EVENT_URL = f"{BASE}/ma/events/event"

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

OUT = Path("sniff_out/dom_verify")
OUT.mkdir(parents=True, exist_ok=True)


SPORTS = {
    "football":     {"id": 1,    "slug": "football"},
    "hockey":       {"id": 2,    "slug": "hockey"},
    "tennis":       {"id": 4,    "slug": "tennis"},
    "basketball":   {"id": 3,    "slug": "basketball"},
    "table_tennis": {"id": 3088, "slug": "table-tennis"},
    "volleyball":   {"id": 9,    "slug": "volleyball"},
    "futsal":       {"id": 1434, "slug": "futsal"},
}


# ============================================================
# 1. Снапшот и выбор топовых матчей
# ============================================================
async def fetch_snapshot() -> dict:
    async with AsyncSession(impersonate=IMPERSONATE, timeout=60, headers=HEADERS) as s:
        r = await s.get(LIST_BASE_URL, headers=HEADERS,
                        params={"lang": "ru", "scopeMarket": "1600"})
        r.raise_for_status()
        return r.json()


def pick_top_matches(data: dict, sport_ids: list[int]) -> dict[str, dict]:
    """
    Для каждого root sport_id берёт матч с максимальным числом факторов
    (эвристика: чем больше — тем полнее линия; он же и живёт live).
    """
    events = data.get("events") or []
    custom = data.get("customFactors") or []
    factors_by_eid = {cf.get("e"): len(cf.get("factors") or []) for cf in custom}
    # соберём дерево спортов
    sports_index = {str(s.get("id")): s for s in (data.get("sports") or [])}

    def root_of(sid) -> int | None:
        cur = sports_index.get(str(sid))
        depth = 0
        while cur and cur.get("parentId") and depth < 10:
            cur = sports_index.get(str(cur["parentId"]))
            depth += 1
        return cur.get("id") if cur else None

    best: dict[int, tuple] = {}
    for ev in events:
        sid = ev.get("sportId")
        root = root_of(sid)
        if root is None or root not in sport_ids:
            continue
        eid = ev.get("id")
        n_f = factors_by_eid.get(eid, 0)
        if n_f == 0:
            continue
        prev = best.get(root)
        if prev is None or n_f > prev[1]:
            best[root] = (eid, n_f, ev.get("team1"), ev.get("team2"))

    return best


# ============================================================
# 2. HTTP: детальный endpoint
# ============================================================
async def fetch_event_api(event_id: int) -> dict:
    async with AsyncSession(impersonate=IMPERSONATE, timeout=30, headers=HEADERS) as s:
        r = await s.get(EVENT_URL, headers=HEADERS,
                        params={"lang": "ru", "version": "0",
                                "eventId": str(event_id),
                                "scopeMarket": "1600"})
        r.raise_for_status()
        return r.json()


# ============================================================
# 3. Playwright: открыть матч и развернуть всё
# ============================================================
class DomScanner:
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.pw = None
        self.browser = None
        self.ctx = None

    async def start(self):
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self.ctx = await self.browser.new_context(
            viewport={"width": 1600, "height": 2400},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
        )

    async def close(self):
        try:
            if self.ctx:
                await self.ctx.close()
            if self.browser:
                await self.browser.close()
            if self.pw:
                await self.pw.stop()
        except Exception:
            pass

    async def _accept_cookies_if_any(self, page: Page):
        for sel in [
            "button:has-text('Принять')",
            "button:has-text('Согласен')",
            "button:has-text('OK')",
            "[data-testid='cookie-accept']",
        ]:
            try:
                el = page.locator(sel).first
                if await el.is_visible(timeout=800):
                    await el.click()
                    await page.wait_for_timeout(400)
                    return
            except Exception:
                continue

    async def scan(self, url: str, max_wait_sec: int = 25) -> dict:
        """
        Открывает URL, разворачивает рынки, собирает DOM.
        """
        page = await self.ctx.new_page()
        result = {
            "url": url,
            "dom_sections": [],
            "dom_raw_count": 0,
            "tabs_clicked": [],
            "expanders_clicked": 0,
        }
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            await self._accept_cookies_if_any(page)
            await page.wait_for_timeout(3500)

            # Скроллим, чтобы лениво-загруженные блоки отрисовались
            for _ in range(6):
                await page.evaluate("window.scrollBy(0, window.innerHeight * 0.9)")
                await page.wait_for_timeout(400)

            # Кликаем по всем вкладкам (Основные / Дополнительные / Статистика / Таймы)
            tab_selectors = [
                "[role='tab']",
                "button[data-testid*='tab']",
                "[class*='Tab']",
                "[class*='tabs'] button",
            ]
            clicked_tabs = 0
            for sel in tab_selectors:
                try:
                    tabs = await page.query_selector_all(sel)
                    for t in tabs[:12]:
                        try:
                            if await t.is_visible():
                                txt = (await t.inner_text() or "").strip()[:40]
                                if txt:
                                    await t.click(timeout=1200)
                                    clicked_tabs += 1
                                    result["tabs_clicked"].append(txt)
                                    await page.wait_for_timeout(700)
                        except Exception:
                            continue
                except Exception:
                    continue

            # Кликаем на все "показать больше"/"развернуть"
            expander_texts = [
                "Показать все", "Развернуть", "Ещё", "Все рынки",
                "Дополнительные", "Показать", "+ ещё",
            ]
            clicked_ex = 0
            for txt in expander_texts:
                try:
                    els = await page.query_selector_all(
                        f"button:has-text('{txt}'), [role='button']:has-text('{txt}')"
                    )
                    for el in els[:10]:
                        try:
                            if await el.is_visible():
                                await el.click(timeout=1000)
                                clicked_ex += 1
                                await page.wait_for_timeout(350)
                        except Exception:
                            continue
                except Exception:
                    continue
            result["expanders_clicked"] = clicked_ex

            # Финальный скролл
            for _ in range(4):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(300)

            # ---- Собираем секции рынков ----
            dom_sections = await page.evaluate("""() => {
                const out = [];
                // Ищем заголовки, похожие на названия секций
                const headerHints = [
                    'Основные', 'Дополнительные', 'Статистика',
                    'Тайм', 'Период', 'Четверть', 'Сет', 'Партия',
                    'Игроки', 'Лотереи', 'Интервалы', 'Сравнение',
                    'Фора', 'Тотал', 'Исход', 'Обе забьют',
                ];

                // Ищем все элементы, содержащие "коэффициент-подобные" числа
                const all = document.querySelectorAll('*');
                const sections = [];
                const seen = new Set();

                for (const el of all) {
                    if (el.children.length > 3) continue;   // листья и малые узлы
                    const txt = (el.innerText || '').trim();
                    if (!txt || txt.length > 200) continue;
                    if (seen.has(txt)) continue;

                    // Проверяем, есть ли в тексте кэф (типа 1.85, 2.10)
                    const hasOdds = /\\b[1-9]\\d?[.,]\\d{1,3}\\b/.test(txt);
                    // Или есть намёк на рынок
                    const hasHint = headerHints.some(h => txt.includes(h));
                    if (!hasOdds && !hasHint) continue;

                    seen.add(txt);
                    sections.push(txt);
                    if (sections.length > 3000) break;
                }

                return sections;
            }""")
            result["dom_sections"] = dom_sections
            result["dom_raw_count"] = len(dom_sections)

            # Попробуем собрать структурированно: секция → рынки → коэффициенты
            structured = await page.evaluate("""() => {
                const out = [];
                // Общий подход: ищем элементы, внутри которых есть 2+ кэфа
                const blocks = document.querySelectorAll('div, section, article');
                let counter = 0;
                for (const bl of blocks) {
                    if (bl.querySelectorAll('div').length < 3) continue;
                    const text = (bl.innerText || '').trim();
                    if (!text || text.length > 1500) continue;
                    // ищем числа-кэфы
                    const odds = text.match(/\\b[1-9]\\d?[.,]\\d{1,3}\\b/g) || [];
                    if (odds.length < 2 || odds.length > 40) continue;
                    // ищем название (первая строка)
                    const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
                    if (lines.length < 2) continue;
                    const caption = lines[0].slice(0, 80);
                    out.push({
                        caption,
                        odds_sample: odds.slice(0, 10),
                        lines: lines.slice(0, 15),
                    });
                    counter++;
                    if (counter > 800) break;
                }
                return out;
            }""")
            result["structured_blocks"] = structured
            result["structured_count"] = len(structured)

            # Сохраняем HTML для ручного разбора
            try:
                html = await page.content()
                (OUT / f"dom_{_slug(url)}.html").write_text(html, encoding="utf-8")
                result["html_saved"] = f"dom_{_slug(url)}.html"
            except Exception:
                pass

        except Exception as e:
            result["error"] = f"{type(e).__name__}: {e}"
        finally:
            try:
                await page.close()
            except Exception:
                pass

        return result


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower())[:80]


# ============================================================
# 4. Сопоставление API ↔ DOM
# ============================================================
def extract_api_factors(api_data: dict) -> list[dict]:
    """
    Возвращает список факторов с привязкой к eventId (главный или дочерний).
    """
    out = []
    for cf in api_data.get("customFactors") or []:
        eid = cf.get("e")
        for f in cf.get("factors") or []:
            try:
                v = float(f.get("v")) if f.get("v") is not None else None
            except (ValueError, TypeError):
                v = None
            out.append({
                "event_id": eid,
                "fid": f.get("f"),
                "v": v,
                "pt": f.get("pt"),
                "p": f.get("p"),
            })
    return out


def extract_dom_odds(structured_blocks: list[dict]) -> list[dict]:
    """
    Из structured_blocks вытаскиваем плоский список
    (caption_context, все odds-числа).
    """
    out = []
    for bl in structured_blocks or []:
        cap = bl.get("caption") or ""
        odds = []
        for o in bl.get("odds_sample") or []:
            try:
                odds.append(float(o.replace(",", ".")))
            except ValueError:
                continue
        if odds:
            out.append({"caption": cap, "odds": odds})
    return out


def match_by_odds(api_factors: list[dict], dom_blocks: list[dict],
                  tolerance: float = 0.02) -> list[dict]:
    """
    Для каждого DOM-блока ищем в API факторы, чьи v совпадают
    хотя бы с одним из odds.
    """
    # индекс API по v (с точностью до 0.01)
    api_by_v: dict[float, list[dict]] = defaultdict(list)
    for f in api_factors:
        if f["v"] is None:
            continue
        key = round(f["v"], 2)
        api_by_v[key].append(f)

    results = []
    for bl in dom_blocks or []:
        matched = []
        for odd in bl.get("odds") or []:
            key = round(odd, 2)
            hits = api_by_v.get(key, [])
            for h in hits:
                matched.append({
                    "dom_odd": odd,
                    "fid": h["fid"],
                    "api_event_id": h["event_id"],
                    "api_v": h["v"],
                    "api_pt": h["pt"],
                    "api_p": h["p"],
                })
        if matched:
            results.append({
                "caption": bl.get("caption"),
                "matched": matched[:20],
                "n_matched": len(matched),
            })
    return results


# ============================================================
# 5. Прогон одного вида спорта
# ============================================================
async def process_sport(scanner: DomScanner, sport_key: str,
                        sport_info: dict, event_id: int,
                        team1: str, team2: str) -> dict:
    print(f"\n{'=' * 70}")
    print(f"  {sport_key.upper()}  eventId={event_id}  {team1} vs {team2}")
    print(f"{'=' * 70}")

    # --- API ---
    print(f"  🌐 API: тянем {EVENT_URL}?eventId={event_id}")
    try:
        api_data = await fetch_event_api(event_id)
    except Exception as e:
        print(f"  ❌ API ошибка: {e}")
        return {"sport": sport_key, "error": str(e)}

    api_factors = extract_api_factors(api_data)
    n_f = len(api_factors)
    unique_fids = len({f['fid'] for f in api_factors})
    children = [e for e in (api_data.get("events") or [])
                if e.get("parentId") == event_id]
    print(f"  ✅ API: {n_f} факторов, {unique_fids} уникальных fid, "
          f"{len(children)} дочерних")

    # --- Playwright ---
    url = f"https://fon.bet/live/{sport_info['slug']}/category/x/{sport_info['id']}/{event_id}"
    print(f"  🎭 DOM: открываем {url}")
    dom = await scanner.scan(url)
    print(f"     tabs clicked: {len(dom.get('tabs_clicked', []))} → "
          f"{dom.get('tabs_clicked', [])[:6]}")
    print(f"     expanders clicked: {dom.get('expanders_clicked')}")
    print(f"     dom_raw_count: {dom.get('dom_raw_count')}")
    print(f"     structured_count: {dom.get('structured_count')}")

    # --- сопоставление ---
    dom_odds = extract_dom_odds(dom.get("structured_blocks") or [])
    matched = match_by_odds(api_factors, dom_odds)
    total_matches = sum(m["n_matched"] for m in matched)

    print(f"\n  📊 СОПОСТАВЛЕНИЕ:")
    print(f"     DOM-блоков с кэфами:  {len(dom_odds)}")
    print(f"     Сопоставлено блоков:  {len(matched)}")
    print(f"     Всего совпадений:     {total_matches}")

    # Показать топ сопоставлений по числу совпадений
    print(f"\n  ── ТОП-10 DOM-БЛОКОВ ПО СОВПАДЕНИЯМ ──")
    matched_sorted = sorted(matched, key=lambda x: -x["n_matched"])[:10]
    for m in matched_sorted:
        cap = (m["caption"] or "")[:60].replace("\n", " ")
        fids = sorted({str(mm["fid"]) for mm in m["matched"]})[:8]
        print(f"     [{m['n_matched']:>3}] {cap:<60} | fids={fids}")

    # Ищем упоминания периодов в DOM
    periods_found = []
    for s in dom.get("dom_sections") or []:
        sl = s.lower()
        for kw in ["1-й тайм", "2-й тайм", "1-я четверть", "2-я четверть",
                   "3-я четверть", "4-я четверть", "1-й период", "2-й период",
                   "3-й период", "1-й сет", "2-й сет", "3-й сет",
                   "1-я партия", "2-я партия", "3-я партия"]:
            if kw in sl:
                periods_found.append((kw, s[:120]))

    print(f"\n  ── ПЕРИОДЫ В DOM ──")
    if periods_found:
        uniq_kw = defaultdict(list)
        for kw, txt in periods_found:
            uniq_kw[kw].append(txt)
        for kw, texts in uniq_kw.items():
            print(f"     {kw:<15} × {len(texts)}  пример: {texts[0][:80]}")
    else:
        print(f"     ❌ не найдено в тексте DOM")

    result = {
        "sport": sport_key,
        "event_id": event_id,
        "team1": team1,
        "team2": team2,
        "api": {
            "n_factors": n_f,
            "unique_fids": unique_fids,
            "children": len(children),
            "children_info": [
                {"id": ch.get("id"), "kind": ch.get("kind"),
                 "name": ch.get("name")}
                for ch in children
            ],
        },
        "dom": {
            "tabs_clicked": dom.get("tabs_clicked"),
            "expanders_clicked": dom.get("expanders_clicked"),
            "dom_raw_count": dom.get("dom_raw_count"),
            "structured_count": dom.get("structured_count"),
            "html_saved": dom.get("html_saved"),
            "periods_mentioned": list({kw for kw, _ in periods_found}),
        },
        "matching": {
            "dom_blocks_with_odds": len(dom_odds),
            "matched_blocks": len(matched),
            "total_matches": total_matches,
            "top_matches": matched_sorted,
        },
    }
    return result


# ============================================================
# 6. Main
# ============================================================
async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sports", type=str, default=",".join(SPORTS.keys()),
                    help="через запятую: football,table_tennis,...")
    ap.add_argument("--headless", action="store_true",
                    help="Playwright без окна (может ловить капчу)")
    args = ap.parse_args()

    requested = [s.strip() for s in args.sports.split(",") if s.strip()]
    requested = [s for s in requested if s in SPORTS]
    if not requested:
        print(f"❌ Неверный список видов спорта. Доступно: {list(SPORTS.keys())}")
        return

    print(f"🎯 Виды спорта для сверки: {requested}")
    print(f"🎭 Playwright mode: {'headless' if args.headless else 'visible'}")

    # --- снапшот ---
    print(f"\n🌐 Тянем снапшот listBase ...")
    snap = await fetch_snapshot()
    sport_ids = [SPORTS[s]["id"] for s in requested]
    top = pick_top_matches(snap, sport_ids)
    print(f"✅ Выбраны топовые матчи:")
    for s in requested:
        sid = SPORTS[s]["id"]
        if sid in top:
            eid, n_f, t1, t2 = top[sid]
            print(f"   {s:<15} eventId={eid} factors_in_snap={n_f} {t1} vs {t2}")
        else:
            print(f"   {s:<15} ❌ нет матчей")

    # --- сканер ---
    scanner = DomScanner(headless=args.headless)
    await scanner.start()

    all_results = []
    try:
        for sport_key in requested:
            sid = SPORTS[sport_key]["id"]
            if sid not in top:
                continue
            eid, n_f, t1, t2 = top[sid]
            try:
                res = await process_sport(
                    scanner, sport_key, SPORTS[sport_key],
                    eid, t1 or "?", t2 or "?"
                )
                all_results.append(res)
            except Exception as e:
                print(f"  ❌ {sport_key}: {type(e).__name__}: {e}")
                all_results.append({"sport": sport_key, "error": str(e)})

            # пауза между видами
            await asyncio.sleep(2)
    finally:
        await scanner.close()

    # --- сводка ---
    print(f"\n{'=' * 70}")
    print(f"  ИТОГОВАЯ СВОДКА")
    print(f"{'=' * 70}")
    print(f"  {'sport':<15} {'api_fid':<10} {'dom_raw':<10} "
          f"{'matched':<10} {'periods'}")
    for r in all_results:
        if "error" in r:
            print(f"  {r['sport']:<15} ❌ {r['error'][:40]}")
            continue
        a = r["api"]
        d = r["dom"]
        m = r["matching"]
        per = ",".join(d.get("periods_mentioned") or [])[:30] or "—"
        print(f"  {r['sport']:<15} {a['unique_fids']:<10} "
              f"{d['dom_raw_count']:<10} {m['total_matches']:<10} {per}")

    # --- сохраняем всё ---
    out_path = OUT / f"report_{int(time.time())}.json"
    out_path.write_text(
        json.dumps({
            "sports_requested": requested,
            "snapshot_top": {s: {"event_id": top[SPORTS[s]['id']][0],
                                  "team1": top[SPORTS[s]['id']][2],
                                  "team2": top[SPORTS[s]['id']][3]}
                              for s in requested if SPORTS[s]['id'] in top},
            "results": all_results,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n💾 Сырой отчёт: {out_path}")
    print(f"💾 HTML-дампы: {OUT}/dom_*.html")


if __name__ == "__main__":
    asyncio.run(main())