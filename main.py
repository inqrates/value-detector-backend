# main.py
import asyncio
import logging
import sys
import uvicorn

from parsers.fonbet_api import FonbetApiParser
from parsers.winline_api import WinlineApiParser
from parsers.ligastavok_api import LigaStavokApiParser
from parsers.leon_api import LeonApiParser
from parsers.olimp_api import OlimpApiParser
from parsers.betcity_api import BetcityApiParser
from parsers.marathon_api import MarathonApiParser
from parsers.zenit_api import ZenitApiParser
from parsers.sportbet_api import SportbetApiParser

from core.detector import Detector
from core.aggregator import OddsAggregator
from core.browser_manager import browser_manager
from api import app, set_aggregator, broadcast_message

# ---- Настройка логирования ----
LOG_LEVEL = logging.INFO      # <-- установлен INFO
LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(message)s'

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Отключаем шумные библиотеки
for lib in ['playwright', 'httpx', 'urllib3', 'asyncio']:
    logging.getLogger(lib).setLevel(logging.WARNING)

# --- ОТКЛЮЧАЕМ ИНФОРМАЦИОННЫЕ ЛОГИ ПАРСЕРОВ (оставляем только ошибки) ---
for name in ['parsers', 'parsers.fonbet_api', 'parsers.winline_api', 'parsers.ligastavok_api',
             'parsers.leon_api', 'parsers.olimp_api', 'parsers.betcity_api', 'parsers.marathon_api',
             'parsers.zenit_api', 'parsers.sportbet_api']:
    logging.getLogger(name).setLevel(logging.WARNING)

# Также можно отключить api и core – но оставим их на INFO
# logging.getLogger('api').setLevel(logging.WARNING)
# logging.getLogger('core').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


async def print_stats(aggregator: OddsAggregator):
    """
    Упрощённая версия – только количество вилок/валуев/коридоров,
    без логирования каждого объекта.
    """
    while True:
        try:
            await asyncio.sleep(5)

            # ---- Вилки ----
            forks = aggregator.find_arbitrage(min_profit=0.5)
            if forks:
                logger.info(f"🔍 Найдено {len(forks)} вилок")
                for f in forks:
                    await broadcast_message({"type": "arbitrage", "payload": f})

            # ---- Валуи ----
            values = aggregator.find_value_bets(threshold=1.05)
            if values:
                logger.info(f"💰 Найдено {len(values)} валуев")
                for v in values:
                    await broadcast_message({"type": "value", "payload": v})

            # ---- Коридоры ----
            corridors = aggregator.find_corridors()
            if corridors:
                logger.info(f"🚪 Найдено {len(corridors)} коридоров")
                for c in corridors:
                    await broadcast_message({"type": "corridor", "payload": c})

            # ---- Статистика советника ----
            stats = aggregator.get_advisor_stats()
            if stats:
                logger.info(f"📊 Активных БК: {len(stats)} -> {list(stats.keys())}")
            await broadcast_message({"type": "advisor", "payload": stats})

        except Exception as e:
            logger.error(f"Ошибка в print_stats: {e}", exc_info=True)
            await asyncio.sleep(1)


async def main():
    logger.info("🚀 Запуск системы парсинга настольного тенниса")

    detector = Detector(broadcast_callback=broadcast_message)
    aggregator = OddsAggregator(ttl=10)
    set_aggregator(aggregator)

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

    logger.info(f"✅ Создано {len(parsers)} парсеров")

    asyncio.create_task(print_stats(aggregator))
    tasks = [asyncio.create_task(p.run()) for p in parsers]

    config = uvicorn.Config(app, host="0.0.0.0", port=8000, loop="asyncio")
    server = uvicorn.Server(config)
    asyncio.create_task(server.serve())

    logger.info("🌐 API запущен на http://0.0.0.0:8000")

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("🛑 Остановка по Ctrl+C")
        for p in parsers:
            p.is_running = False
        await asyncio.gather(*[p.stop() for p in parsers])
        await browser_manager.shutdown()


if __name__ == "__main__":
    asyncio.run(main())