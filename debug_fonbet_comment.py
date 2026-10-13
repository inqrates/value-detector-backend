# debug_fonbet_match.py
"""
Отладка парсера Fonbet для КОНКРЕТНОГО матча.

Матч: https://fon.bet/live/table-tennis/category/ligapro/55117/68825943
ID события: 68825943

ЧТО ДЕЛАЕТ:
  - Каждые 0.5 сек запрашивает /ma/events/event?eventId=68825943
  - Логирует:
      * score1:score2 (счёт партий)
      * comment (сырой)
      * sub1:sub2 текущий парсер (pairs[-1])
      * sub1:sub2 новый парсер (pairs[score1+score2])
      * phase_num
      * флаг ⚠️ если текущий и новый парсер расходятся
  - Отслеживает момент СМЕНЫ ПАРТИИ — показывает, как comment
    меняется при переходе (6-11) → (6-11 5-11) → (0-0)

ЗАПУСК:
    python debug_fonbet_match.py
    python debug_fonbet_match.py --interval 0.3
    python debug_fonbet_match.py --event 68825943
    python debug_fonbet_match.py --csv debug_match.csv

ОСТАНОВКА: Ctrl+C
"""
import argparse
import asyncio
import csv
import logging
import re
import sys
import time
from datetime import datetime
from typing import Optional, Tuple

from curl_cffi.requests import AsyncSession


# ── Настройки по умолчанию ──
DEFAULT_EVENT_ID = "68825943"

FONBET_HOST = "line-lb54-w.bk6bba-resources.com"
EVENT_URL = f"https://{FONBET_HOST}/ma/events/event"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9",
    "origin": "https://fon.bet",
    "referer": "https://fon.bet/",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("dbg_match")


# ══════════════════════════════════════════════════════════════
# Парсинг comment: ТЕКУЩАЯ vs НОВАЯ логика
# ══════════════════════════════════════════════════════════════
def current_parse(comment: str) -> Tuple[int, int]:
    """Как это делает fonbet_api.py СЕЙЧАС — pairs[-1]."""
    if not comment:
        return 0, 0
    pairs = re.findall(r'\d+[*]?-\d+[*]?', comment)
    if pairs:
        last = pairs[-1].replace('*', '')
        parts = last.split('-')
        if len(parts) == 2:
            return (int(parts[0]) if parts[0].isdigit() else 0,
                    int(parts[1]) if parts[1].isdigit() else 0)
    return 0, 0


def new_parse(comment: str, score1: int, score2: int) -> Tuple[int, int, str]:
    """
    Новая логика — берём пару по индексу активной партии.
    Возвращает (sub1, sub2, status).
    """
    if not comment:
        return 0, 0, "no_comment"
    pairs = re.findall(r'\d+[*]?-\d+[*]?', comment)
    if not pairs:
        return 0, 0, "no_pairs"

    # Сколько партий УЖЕ ЗАВЕРШЕНО = score1 + score2
    # Активная партия идёт следующей
    active_idx = score1 + score2

    if 0 <= active_idx < len(pairs):
        active = pairs[active_idx].replace('*', '')
        parts = active.split('-')
        if len(parts) == 2:
            return (int(parts[0]) if parts[0].isdigit() else 0,
                    int(parts[1]) if parts[1].isdigit() else 0,
                    f"ok (idx={active_idx}/{len(pairs)})")
    return 0, 0, f"NO_ACTIVE_PART (idx={active_idx}, pairs={len(pairs)})"


def count_pairs(comment: str) -> int:
    return len(re.findall(r'\d+[*]?-\d+[*]?', comment)) if comment else 0


def find_asterisk(comment: str) -> str:
    """Показывает, где стоит * (подача)."""
    if not comment:
        return ""
    if '*' not in comment:
        return "(no *)"
    # Показать пару со звёздочкой
    pairs = re.findall(r'\d+[*]?-\d+[*]?', comment)
    for p in pairs:
        if '*' in p:
            return f"ast on: {p}"
    return ""


# ══════════════════════════════════════════════════════════════
# Запрос события
# ══════════════════════════════════════════════════════════════
async def fetch_event(session: AsyncSession, event_id: str) -> Optional[dict]:
    try:
        r = await session.get(
            EVENT_URL,
            headers=HEADERS,
            params={
                "lang": "ru",
                "eventId": event_id,
                "scopeMarket": "1600",
                "version": "0",
            },
        )
        if r.status_code != 200:
            logger.warning(f"HTTP {r.status_code}")
            return None
        return r.json()
    except Exception as e:
        logger.warning(f"fetch: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# Извлечение данных по матчу
# ══════════════════════════════════════════════════════════════
def extract_match_data(data: dict, event_id: str) -> Optional[dict]:
    """
    Возвращает:
        {
          "score1": int, "score2": int,
          "comment": str,
          "sub1": int, "sub2": int,
          "phase_num": int,
          "team1": str, "team2": str,
          "is_live": bool,
          "pairs_count": int,
          "current": (int, int),
          "new": (int, int, str),
        }
    """
    if not data:
        return None

    events = data.get("events") or []
    event = None
    for ev in events:
        if str(ev.get("id")) == str(event_id):
            event = ev
            break

    if not event:
        return None

    # eventMiscs — где score1/score2/comment
    misc = None
    for m in data.get("eventMiscs") or []:
        if str(m.get("id")) == str(event_id):
            misc = m
            break

    if not misc:
        return None

    s1 = int(misc.get("score1") or 0)
    s2 = int(misc.get("score2") or 0)
    comment = (misc.get("comment") or "").strip()

    if not comment:
        return None

    # isLive?
    is_live = bool(event.get("isLive")) or bool(comment)

    # Текущий парсер
    cur_sub = current_parse(comment)

    # Новый парсер
    new_sub1, new_sub2, new_status = new_parse(comment, s1, s2)

    # phase_num — как в fonbet_api.py
    phase_num = s1 + s2 + 1

    return {
        "score1": s1,
        "score2": s2,
        "comment": comment,
        "sub1": cur_sub[0],
        "sub2": cur_sub[1],
        "phase_num": phase_num,
        "team1": event.get("team1", "?"),
        "team2": event.get("team2", "?"),
        "is_live": is_live,
        "pairs_count": count_pairs(comment),
        "current": cur_sub,
        "new": (new_sub1, new_sub2, new_status),
    }


# ══════════════════════════════════════════════════════════════
# Основной цикл
# ══════════════════════════════════════════════════════════════
class DebugSession:
    def __init__(self, event_id: str, interval: float, csv_path: Optional[str]):
        self.event_id = event_id
        self.interval = interval
        self.csv_path = csv_path
        self._last_sig = None
        self._last_comment = None
        self._iter = 0
        self._diffs_total = 0
        self._csv_file = None
        self._csv_writer = None

    def open_csv(self):
        if not self.csv_path:
            return
        self._csv_file = open(self.csv_path, "w", newline="",
                              encoding="utf-8")
        self._csv_writer = csv.writer(self._csv_file)
        self._csv_writer.writerow([
            "time", "score1", "score2", "comment",
            "sub1_cur", "sub2_cur", "sub1_new", "sub2_new", "new_status",
            "phase_num", "is_diff", "pairs_count",
        ])

    def close_csv(self):
        if self._csv_file:
            self._csv_file.close()

    def write_csv(self, m: dict, is_diff: bool):
        if not self._csv_writer:
            return
        self._csv_writer.writerow([
            datetime.now().strftime("%H:%M:%S.%f")[:-3],
            m["score1"], m["score2"], m["comment"],
            m["current"][0], m["current"][1],
            m["new"][0], m["new"][1], m["new"][2],
            m["phase_num"], "YES" if is_diff else "",
            m["pairs_count"],
        ])
        self._csv_file.flush()

    def log_match(self, m: dict):
        self._iter += 1

        # ── Отпечаток чтобы не спамить одинаковыми строками ──
        sig = (
            m["score1"], m["score2"],
            m["comment"],
            m["current"], m["new"],
        )
        if sig == self._last_sig:
            return
        self._last_sig = sig

        # ── Определяем расхождение ──
        is_diff = (m["current"] != (m["new"][0], m["new"][1]))

        if is_diff:
            self._diffs_total += 1

        # ── Отслеживаем СМЕНУ comment (переход партии) ──
        comment_changed = (self._last_comment is not None
                           and m["comment"] != self._last_comment)
        self._last_comment = m["comment"]

        # ── Формируем строку ──
        flag = "⚠️ DIFF" if is_diff else "   OK  "
        if comment_changed:
            flag = "🔄 CHG " + flag

        header = (
            f"\n{flag} [{m['team1']} vs {m['team2']}]"
            f"  (event={self.event_id})\n"
            f"     ⏱  {datetime.now().strftime('%H:%M:%S.%f')[:-3]}\n"
            f"     score партий:  {m['score1']}:{m['score2']}   "
            f"(phase_num={m['phase_num']})\n"
            f"     comment:       {m['comment']!r}   "
            f"(pairs={m['pairs_count']})   {find_asterisk(m['comment'])}\n"
            f"     CURRENT parse: sub={m['current'][0]}:{m['current'][1]}"
            f"   (pairs[-1])\n"
            f"     NEW     parse: sub={m['new'][0]}:{m['new'][1]}"
            f"   ({m['new'][2]})"
        )

        if is_diff:
            logger.warning(header)
        else:
            logger.info(header)

        self.write_csv(m, is_diff)

    def summary(self):
        logger.warning("\n" + "=" * 60)
        logger.warning("ИТОГО:")
        logger.warning(f"  Итераций: {self._iter}")
        logger.warning(f"  Рассинхронов: {self._diffs_total}")
        if self.csv_path:
            logger.warning(f"  CSV сохранён: {self.csv_path}")
        logger.warning("=" * 60)


async def run(args):
    logger.info("=" * 70)
    logger.info(f"🔍 ОТЛАДКА МАТЧА Fonbet")
    logger.info(f"   event_id:  {args.event}")
    logger.info(f"   URL:       https://fon.bet/live/.../{args.event}")
    logger.info(f"   API:       {EVENT_URL}")
    logger.info(f"   interval:  {args.interval}s")
    if args.csv:
        logger.info(f"   CSV:       {args.csv}")
    logger.info("=" * 70)
    logger.info("Ждём обновлений... Ctrl+C для остановки\n")

    dbg = DebugSession(args.event, args.interval, args.csv)
    dbg.open_csv()

    async with AsyncSession(impersonate="chrome150", timeout=15) as session:
        while True:
            t0 = time.monotonic()
            try:
                data = await fetch_event(session, args.event)
                if data:
                    m = extract_match_data(data, args.event)
                    if m:
                        dbg.log_match(m)
                    else:
                        if dbg._iter % 20 == 0:
                            logger.debug("нет данных по event (не live или нет comment)")
                else:
                    logger.debug("пустой ответ")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"loop: {e}", exc_info=True)

            elapsed = time.monotonic() - t0
            sleep = max(0.0, args.interval - elapsed)
            if sleep > 0:
                await asyncio.sleep(sleep)

    dbg.close_csv()
    dbg.summary()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", default=DEFAULT_EVENT_ID,
                    help=f"ID события Fonbet (default: {DEFAULT_EVENT_ID})")
    ap.add_argument("--interval", type=float, default=0.5,
                    help="Секунд между запросами (default: 0.5)")
    ap.add_argument("--csv", default=None,
                    help="Путь для CSV-лога (опционально)")
    args = ap.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        logger.info("\n\n🛑 Остановлено пользователем")


if __name__ == "__main__":
    main()