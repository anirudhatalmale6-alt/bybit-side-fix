from __future__ import annotations

import asyncio
import logging

from p2p_bot.config import Settings
from p2p_bot.db.repositories import RawOrderRepository


class DataMaintenanceService:
    def __init__(
        self,
        settings: Settings,
        raw_order_repository: RawOrderRepository,
        logger: logging.Logger,
    ) -> None:
        self.settings = settings
        self.raw_order_repository = raw_order_repository
        self.logger = logger

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            started = asyncio.get_running_loop().time()
            try:
                refreshed = await self.raw_order_repository.refresh_market_activity(
                    self.settings.market_activity_windows_hours,
                    asset=self.settings.base_asset,
                    fiats=self.settings.fiats,
                )
                deleted = await self.raw_order_repository.prune_history()
                finished = asyncio.get_running_loop().time()
                self.logger.info(
                    "Maintenance refreshed %s market-activity rows and pruned %s raw orders in %.2fs",
                    refreshed,
                    deleted,
                    finished - started,
                )
            except Exception as exc:
                self.logger.exception("Maintenance iteration failed: %s", exc)

            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.settings.raw_orders_prune_interval_sec,
                )
            except asyncio.TimeoutError:
                continue
