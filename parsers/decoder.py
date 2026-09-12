from __future__ import annotations

import gzip
from typing import Any


class WebSocketDecodeError(ValueError):
    pass


class BinaryReader:
    """Последовательно читает числа и строки из бинарного сообщения."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.position = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.position

    def read(self, size: int) -> bytes:
        end = self.position + size
        if end > len(self.data):
            raise WebSocketDecodeError(f"Недостаточно данных в позиции {self.position}")
        value = self.data[self.position:end]
        self.position = end
        return value

    def uint8(self) -> int:
        return self.read(1)[0]

    def uint16(self) -> int:
        return int.from_bytes(self.read(2), "little")

    def uint32(self) -> int:
        return int.from_bytes(self.read(4), "little")

    def int32(self) -> int:
        return int.from_bytes(self.read(4), "little", signed=True)

    def string(self) -> str:
        size = self.uint16()
        value = self.read(size).split(b"\x1b", 1)[0]
        return value.decode("utf-8", errors="replace")


class DataNgDecoder:
    """Декодируем из data_ng только прематч и лайв, потому что все остальное технические данные."""

    def __init__(self) -> None:
        self.sports: dict[int, str] = {}
        self.markets: dict[int, tuple[int, str]] = {}
        self.championships: dict[int, dict[str, Any]] = {}
        self.live_events: dict[int, dict[str, Any]] = {}

    def decode(self, message: bytes) -> dict[str, Any] | list[dict[str, Any]] | None:
        # Если сообщение начинается с 1F 8B, оно сжато gzip.
        if message.startswith(b"\x1f\x8b"):
            message = gzip.decompress(message)

        if len(message) < 2:
            raise WebSocketDecodeError("Сообщение data_ng слишком короткое")

        # Первые два байта — номер типа сообщения.
        step = int.from_bytes(message[:2], "little")
        reader = BinaryReader(message[2:])

        # шаг 20000 означает, что внутри лежит несколько обычных сообщений, которые нам не интересны.
        if step == 20_000:
            result: list[dict[str, Any]] = []
            while reader.remaining:
                size = reader.uint32()
                decoded = self.decode(reader.read(size))
                if isinstance(decoded, list):
                    result.extend(decoded)
                elif decoded is not None:
                    result.append(decoded)
            return result or None

        # меню наружу не возвращаем, хлам. Из него запоминаем названия спорта и
        # правила чтения коэффициентов, которые понадобятся в прематч, но это не точно.
        if step == 16:
            sport_count = reader.uint32()
            for _ in range(sport_count):
                sport_id = reader.int32()
                reader.int32()  # Порядок сортировки.
                self.sports[sport_id] = reader.string()

                # Девять служебных подписей времени матча.
                for _ in range(9):
                    reader.string()

            market_count = reader.uint32()
            for _ in range(market_count):
                market_id = reader.int32()
                reader.string()  # Список видов спорта.
                reader.int32()   # Фаворит.
                reader.int32()   # Количество коэффициентов.
                market_type = reader.int32()
                reader.string()  # Дополнительный текст.

                labels = [reader.string() for _ in range(30)]
                self.markets[market_id] = (market_type, labels[0])

            return None

        # Прематч состоит из стран, чемпионатов, событий и линий.
        if step == 3:
            reader.read(16)  # Служебный заголовок.
            events: list[dict[str, Any]] = []
            lines: list[dict[str, Any]] = []
            current_championship_id: int | None = None
            current_event_id: int | None = None

            while reader.remaining:
                record_type = reader.uint8()

                # Страна. Эти данные не возвращаем, но должны правильно пропустить.
                # Для тебя не представляет полезной нагрузки, но если нужно включим.
                if record_type == 1:
                    reader.int32()   # ID
                    reader.int32()   # Регион
                    reader.string()  # Название
                    reader.int32()   # Сорировка
                    reader.uint8()   # Континент
                    reader.uint16()  # X
                    reader.uint16()  # Y
                    reader.uint8()   # Гргуппа

                # Чемпионат. Запоминаем его, чтобы дополнить следующие события
                elif record_type == 2:
                    championship_id = reader.uint32()
                    sport_id = reader.uint32()
                    country_id = reader.uint32()
                    championship_name = reader.string()
                    reader.int32()  # Сортировка
                    reader.int32()  # Код
                    reader.int32()  # Количество специальных соббытий
                    reader.uint8()  # Уровень отоброжения
                    reader.int32()  # Сортировка уровня
                    reader.int32()  # Новая сортировка уровн

                    self.championships[championship_id] = {
                        "id": championship_id,
                        "name": championship_name,
                        "sportId": sport_id,
                        "sport": self.sports.get(sport_id),
                        "countryId": country_id,
                    }
                    current_championship_id = championship_id

                # Новое событие или полное обновление события
                elif record_type in (3, 34):
                    event = {
                        "id": reader.int32(),
                        "radarId": reader.int32(),
                        "nativeId": reader.int32(),
                        "provider": reader.uint8(),
                        "category": reader.uint8(),
                        "liveId": reader.uint8(),
                        "linesAvailability": reader.uint8(),
                    }
                    reader.read(2)  # Два зарезервированных байта

                    # Блок видео и статитики нам не нужен
                    widget_size = reader.uint16()
                    reader.read(widget_size)

                    if record_type == 34:
                        current_championship_id = reader.int32()

                    championship = self.championships.get(
                        current_championship_id or -1, {}
                    )
                    event.update(
                        championshipId=current_championship_id,
                        championship=championship.get("name"),
                        sportId=championship.get("sportId"),
                        sport=championship.get("sport"),
                        timestamp=reader.int32(),
                        additionalLines=reader.uint8(),
                        isOD=reader.uint8(),
                        participants=[reader.string(), reader.string()],
                    )
                    current_event_id = event["id"]
                    events.append(event)

                # Новая линия или обнвление коэфициентов существующей линии
                elif record_type in (4, 43):
                    line_id = reader.uint32()
                    event_id = reader.uint32() if record_type == 43 else current_event_id
                    market_id = reader.uint16()
                    margin_cash = reader.uint16() / 10_000
                    margin = reader.uint16() / 10_000

                    if market_id not in self.markets:
                        raise WebSocketDecodeError(
                            f"В menu отсутствует marketId={market_id}"
                        )

                    market_type, market_name = self.markets[market_id]
                    line: dict[str, Any] = {
                        "id": line_id,
                        "eventId": event_id,
                        "marketId": market_id,
                        "market": market_name,
                        "marketType": market_type,
                        "marginCash": margin_cash,
                        "margin": margin,
                    }

                    # Некоторые рынки хранят перед значениями дополнительный параметр
                    if market_type in (3, 6):
                        line["favorite"] = reader.uint8()
                        line["coefficient"] = reader.uint16() / 100
                    elif market_type in (4, 7):
                        line["coefficient"] = reader.uint16() / 100
                    elif market_type in (51, 151):
                        line["coefficient"] = 1
                    elif market_type == 61:
                        line["favorite"] = reader.uint8()
                        line["coefficient"] = f"1/{reader.uint16() / 100:g}"
                    elif market_type == 71:
                        line["coefficient"] = f"1/{reader.uint16() / 100:g}"

                    value_count = 2
                    if market_type in (2, 5, 51):
                        value_count = 3
                    elif market_type == 9:
                        value_count = 4

                    line["values"] = [
                        reader.uint16() / 100 for _ in range(value_count)
                    ]
                    lines.append(line)

                elif record_type == 31:
                    events.append({
                        "id": reader.uint32(),
                        "timestamp": reader.uint32(),
                        "update": True,
                    })

                elif record_type in (32, 42):
                    target = events if record_type == 32 else lines
                    target.append({"id": reader.uint32(), "deleted": True})

                elif record_type == 33:
                    events.append({
                        "id": reader.uint32(),
                        "additionalLines": reader.uint8(),
                        "isOD": reader.uint8(),
                        "update": True,
                    })

                else:
                    raise WebSocketDecodeError(
                        f"Неизвестный prematch recordType={record_type}"
                    )

            return {"type": "prematch", "events": events, "lines": lines}

        # лайв устроен похоже, но имеет немного другой набор полей
        if step == 4:
            reader.read(12)  # Служебный заголовок
            events: list[dict[str, Any]] = []
            lines: list[dict[str, Any]] = []

            while reader.remaining:
                record_type = reader.uint8()

                # Чемпионат для следующих лайв событий
                if record_type == 2:
                    championship_id = reader.uint32()
                    sport_id = reader.uint32()
                    reader.int32()  # Сортировка
                    country_id = reader.uint32()
                    reader.uint8()  # Код
                    championship_name = reader.string()
                    reader.uint8()   # Уровень отображения
                    reader.uint32()  # Сортировка уровня
                    reader.uint32()  # Новая сортировка уров

                    self.championships[championship_id] = {
                        "id": championship_id,
                        "name": championship_name,
                        "sportId": sport_id,
                        "sport": self.sports.get(sport_id),
                        "countryId": country_id,
                    }

                # Новое лайв событие или его короткое обновление
                elif record_type in (3, 4):
                    is_update = record_type == 4
                    event: dict[str, Any] = {"id": reader.int32()}

                    if not is_update:
                        event.update(
                            radarId=reader.int32(),
                            nativeId=reader.int32(),
                            provider=reader.uint8(),
                            category=reader.uint8(),
                        )

                    widget_size = reader.uint16()
                    reader.read(widget_size)
                    event["isOD"] = reader.uint8()
                    reader.uint8()  # Зарезервированный байт
                    event["duration"] = reader.uint8()
                    event["state"] = reader.int32()

                    if event["state"] > 3:
                        self.live_events.pop(event["id"], None)
                        events.append({"id": event["id"], "deleted": True})
                        continue

                    if not is_update:
                        championship_id = reader.uint32()
                        championship = self.championships.get(championship_id, {})
                        event.update(
                            championshipId=championship_id,
                            championship=championship.get("name"),
                            sportId=championship.get("sportId"),
                            sport=championship.get("sport"),
                            participants=[reader.string(), reader.string()],
                            timestamp=reader.uint32(),
                        )

                    event["time"] = reader.string()
                    event["cards"] = [reader.uint8() for _ in range(4)]
                    reader.uint8()  # Источник данных feed.
                    event["score"] = reader.string()
                    event["setScores"] = reader.string()
                    event["addInfo"] = reader.string()
                    event["lineCount"] = reader.uint8()

                    # Короткое обновление несодержит команды и чемпионат
                    # поэтому дополняем его даными из предыдущей версии
                    if is_update:
                        previous = self.live_events.get(event["id"], {})
                        event = {**previous, **event, "update": True}

                    self.live_events[event["id"]] = event
                    events.append(event)

                # Линия с live-коэффициентами
                elif record_type == 5:
                    line_id = reader.uint32()
                    state = reader.uint8()

                    if state == 5:
                        lines.append({"id": line_id, "deleted": True})
                        continue

                    event_id = reader.uint32()
                    value_count = reader.uint8()
                    values = [
                        reader.uint16() / 100
                        for _ in range(min(value_count, 31))
                    ]
                    market_id = reader.uint16()

                    line = {
                        "id": line_id,
                        "eventId": event_id,
                        "state": state,
                        "marketId": market_id,
                        "values": values,
                        "coefficient": reader.string(),
                        "favorite": reader.uint8(),
                    }

                    if market_id in self.markets:
                        market_type, market_name = self.markets[market_id]
                        line.update(market=market_name, marketType=market_type)

                    lines.append(line)

                # Количество линий уже есть в событии, поэтому игнорим
                elif record_type == 6:
                    reader.uint32()
                    reader.uint8()

                else:
                    raise WebSocketDecodeError(
                        f"Неизвестный live recordType={record_type}"
                    )

            return {"type": "live", "events": events, "lines": lines}

        # Все остальные команды data_ng игнорируем
        return None
