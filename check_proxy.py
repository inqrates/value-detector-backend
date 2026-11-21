async def _fetch_snapshot(self):
    if time.time() < self._qrator_cooldown_until:
        self._last_snapshot_at = time.time()
        return
    all_items = []
    skip = 0
    page = 0
    got_403 = False

    # ── ФИКС: единый ts на весь цикл пагинации ──
    # Раньше ts слался только на skip>0 — сервер не связывал
    # страницы между собой и отдавал одни и те же 40.
    ts = int(time.time() * 1000)

    while self.is_running:
        body = {
            "gameId": [],
            "limit": self.LIMIT,
            "ns": ["live"],
            "topEvents": False,
            "view": "priority",
            "widgetVideo": False,
            "ts": ts,          # ── ФИКС: ts всегда ──
        }
        if skip > 0:
            body["skip"] = skip

        try:
            r = await self._session.post(
                self.EVENTS_URL,
                headers=self._make_headers(),
                json=body,
            )
        except Exception as e:
            logger.warning(
                f"[{self.bk_id}] snapshot HTTP error skip={skip}: {e}"
            )
            break

        if r.status_code == 403:
            got_403 = True
            self._consecutive_403 += 1
            logger.warning(
                f"[{self.bk_id}] snapshot HTTP 403 skip={skip} "
                f"(подряд: {self._consecutive_403})"
            )
            break

        if r.status_code in (502, 503):
            logger.debug(
                f"[{self.bk_id}] snapshot HTTP {r.status_code} (Qrator)"
            )
            break

        if r.status_code != 200:
            logger.warning(
                f"[{self.bk_id}] snapshot HTTP {r.status_code} skip={skip}"
            )
            break

        try:
            data = r.json()
        except Exception:
            break

        result = data.get("result") or {}
        items = result.get("data") or []
        if not items:
            break

        all_items.extend(items)
        for it in items:
            self._process_http_item(it)

        # ── Конец пагинации ──
        _total_api = result.get("total") or result.get("totalCount") or 0
        if _total_api and skip + len(items) >= _total_api:
            break
        if len(items) < self.LIMIT:
            break

        skip += self.LIMIT
        page += 1
        if page > 20:
            break

        await asyncio.sleep(0.5)

    if not got_403:
        self._consecutive_403 = 0
        self._qrator_cooldown_until = 0.0

    if all_items:
        self._last_successful_response_at = time.time()

    self._last_snapshot_at = time.time()
    if all_items:
        logger.info(
            f"[{self.bk_id}] 📸 Снапшот: {len(all_items)} событий "
            f"({page + 1} стр.), в кеше: {len(self._matches_cache)}"
        )

    if got_403:
        if self._consecutive_403 >= 5:
            now = time.time()
            if now < self._qrator_cooldown_until:
                pass
            else:
                self._qrator_cooldown_until = now + 600.0
                logger.warning(
                    f"[{self.bk_id}] 🧊 {self._consecutive_403} 403 "
                    f"подряд — cooldown 10 мин"
                )
        elif self._consecutive_403 >= self.REFRESH_AFTER_403:
            asyncio.create_task(
                self._refresh_cookies_via_playwright("(после 403)")
            )