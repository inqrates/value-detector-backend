#!/usr/bin/env python3
"""
Мониторинг системы – компактный вывод состояния с отладкой матчинга.
"""

import asyncio
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from core.detector import Detector
from core.aggregator import OddsAggregator
from core.browser_manager import browser_manager

from parsers.fonbet_api import FonbetApiParser
from parsers.winline_api import WinlineApiParser
from parsers.ligastavok_api import LigaStavokApiParser
from parsers.leon_api import LeonApiParser
from parsers.olimp_api import OlimpApiParser
from parsers.betcity_api import BetcityApiParser
from parsers.marathon_api import MarathonApiParser
from parsers.zenit_api import ZenitApiParser
from parsers.sportbet_api import SportbetApiParser


def print_header(text):
    print(f"\n{'='*60}")
    print(f"  {text}")
    print('='*60)


async def monitor():
    print("\n🚀 ЗАПУСК МОНИТОРИНГА СИСТЕМЫ (С ОТЛАДКОЙ)")
    print("Обновление каждые 5 секунд. Нажмите Ctrl+C для остановки.\n")

    detector = Detector()
    aggregator = OddsAggregator(ttl=30)

    parsers = [
        FonbetApiParser(detector=detector, aggregator=aggregator),
        WinlineApiParser(detector=detector, aggregator=aggregator),
        LigaStavokApiParser(detector=detector, aggregator=aggregator),
        LeonApiParser(detector=detector, aggregator=aggregator),
        OlimpApiParser(detector=detector, aggregator=aggregator),
        BetcityApiParser(detector=detector, aggregator=aggregator),
        MarathonApiParser(detector=detector, aggregator=aggregator),
        ZenitApiParser(detector=detector, aggregator=aggregator),
        SportbetApiParser(detector=detector, aggregator=aggregator),
    ]

    tasks = [asyncio.create_task(p.run()) for p in parsers]

    try:
        while True:
            await asyncio.sleep(5)

            matches = aggregator.get_all_matches()
            arbitrages = aggregator.find_arbitrage(min_profit=0.1)
            values = aggregator.find_value_bets(threshold=1.02)
            corridors = aggregator.find_corridors()

            # ---------- АНАЛИЗ МАТЧЕЙ ----------
            bk_counts = defaultdict(int)
            multi_bk_matches = 0
            for m in matches:
                n_bks = len(m['bks'])
                bk_counts[n_bks] += 1
                if n_bks >= 2:
                    multi_bk_matches += 1

            # Вывод первых 3 матчей с деталями
            debug_matches = matches[:3]
            debug_info = []
            for idx, m in enumerate(debug_matches, 1):
                bks = list(m['bks'].keys())
                debug_info.append(f"  {idx}. {m['player1']} vs {m['player2']} | БК: {bks} ({len(bks)} шт.)")

            active_bks = set()
            for m in matches:
                active_bks.update(m['bks'].keys())
            active_bks = sorted(active_bks)

            # ---------- ВЫВОД ----------
            print_header("📊 СОСТОЯНИЕ СИСТЕМЫ")
            print(f"  Всего матчей:          {len(matches)}")
            print(f"  Матчей с >=2 БК:       {multi_bk_matches} ({(multi_bk_matches/len(matches)*100 if matches else 0):.1f}%)")
            print(f"  Активные БК:           {len(active_bks)}")
            print(f"    {', '.join(active_bks)}")
            print(f"  Вилки (>=0.1%):        {len(arbitrages)}")
            print(f"  Валуи (>=2%):          {len(values)}")
            print(f"  Коридоры:              {len(corridors)}")

            if debug_info:
                print("\n🔍 ПРИМЕРЫ МАТЧЕЙ (первые 3):")
                for line in debug_info:
                    print(line)

            # Если есть вилки – показать
            if arbitrages:
                print("\n🔥 ВИЛКИ:")
                for arb in arbitrages[:3]:
                    print(f"  {arb['player1']} vs {arb['player2']}: "
                          f"П1 {arb['best_p1']:.2f} ({arb['bk_p1']}) | "
                          f"П2 {arb['best_p2']:.2f} ({arb['bk_p2']}) → "
                          f"прибыль {arb['profit_percent']:.2f}%")

    except KeyboardInterrupt:
        print("\n⏹️ Остановка по Ctrl+C...")
    finally:
        for p in parsers:
            p.is_running = False
        await asyncio.gather(*[p.stop() for p in parsers])
        await browser_manager.shutdown()
        print("✅ Мониторинг завершён.")


if __name__ == "__main__":
    asyncio.run(monitor())