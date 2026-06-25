from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from p2p_bot.config import Settings
from p2p_bot.db.repositories import RawOrderRepository
from p2p_bot.models.events import Event
from p2p_bot.models.order import P2POrder
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.state import AppState
from p2p_bot.utils.binance_p2p import BinanceP2PClient
from p2p_bot.utils.bingx_p2p import BingXP2PClient
from p2p_bot.utils.bybit_p2p import BybitP2PClient


@dataclass
class FetchResult:
    platform: str
    asset: str
    fiat: str
    side: str
    orders: list[P2POrder]
    ok: bool


@dataclass
class ScanCycleResult:
    orders: list[P2POrder]
    total_fetches: int
    successful_fetches: int
    failed_fetches: int


class Scanner:
    def __init__(
        self,
        settings: Settings,
        state: AppState,
        risk_guard: RiskGuard,
        raw_order_repository: RawOrderRepository,
        binance_client: BinanceP2PClient,
        bingx_client: BingXP2PClient,
        bybit_client: BybitP2PClient,
        scan_queue: asyncio.Queue[Event],
        alert_queue: asyncio.Queue[Event],
        logger: logging.Logger,
    ) -> None:
        self.settings = settings
        self.state = state
        self.risk_guard = risk_guard
        self.raw_order_repository = raw_order_repository
        self.binance_client = binance_client
        self.bingx_client = bingx_client
        self.bybit_client = bybit_client
        self.scan_queue = scan_queue
        self.alert_queue = alert_queue
        self.logger = logger
        self._last_skip_reason: str | None = None
        self._kill_switch_notified = False
        self._fiat_rotation_offsets: dict[str, int] = {}
        self._platform_dynamic_fiat_caps: dict[str, int] = {}
        self._platform_semaphores = {
            platform: asyncio.Semaphore(self._platform_scan_concurrency(platform))
            for platform in ("binance", "bingx", "bybit")
        }

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            iteration_started = asyncio.get_running_loop().time()
            can_trade, reason = self.risk_guard.can_trade()
            if not can_trade:
                if reason != self._last_skip_reason:
                    self.logger.info("Scanner paused: %s", reason)
                    self._last_skip_reason = reason
                await asyncio.sleep(self._remaining_cycle_delay(iteration_started))
                continue
            self._last_skip_reason = None
            try:
                result = await self.scan_once()
                if result.successful_fetches > 0:
                    self.risk_guard.record_api_success()
                    self._kill_switch_notified = False
                else:
                    if self.risk_guard.record_api_error():
                        await self._notify_kill_switch_once("API errors exceeded threshold")
            except Exception as exc:
                self.logger.exception("Scanner iteration failed: %s", exc)
                if self.risk_guard.record_api_error():
                    await self._notify_kill_switch_once("API errors exceeded threshold")
            await asyncio.sleep(self._remaining_cycle_delay(iteration_started))

    async def scan_once(self) -> ScanCycleResult:
        started = asyncio.get_running_loop().time()
        tasks = []
        cycle_fiats_by_platform = {
            platform: self._fiats_for_cycle(platform)
            for platform in self.settings.enabled_platforms
        }
        for platform, cycle_fiats in cycle_fiats_by_platform.items():
            asset = self.settings.base_asset
            for fiat in cycle_fiats:
                for side in ("buy", "sell"):
                    tasks.append(self._safe_fetch(platform, asset, fiat, side))

        batches = await asyncio.gather(*tasks)
        fetched_at = asyncio.get_running_loop().time()
        orders = [order for batch in batches for order in batch.orders]
        successful_fetches = sum(1 for batch in batches if batch.ok)
        failed_fetches = len(batches) - successful_fetches
        await self.raw_order_repository.save_batch(orders)
        self.state.record_orders(orders)
        await self.scan_queue.put(Event("orders_scanned", orders))
        finished = asyncio.get_running_loop().time()
        self._update_dynamic_fiat_caps(
            cycle_fiats_by_platform=cycle_fiats_by_platform,
            batches=batches,
            fetch_elapsed_sec=fetched_at - started,
        )
        self.logger.info(
            "Scanner saved %s orders from %s/%s successful fetches across fiats=%s in %.2fs "
            "(fetch=%.2fs, persist+queue=%.2fs)",
            len(orders),
            successful_fetches,
            len(batches),
            self._cycle_fiats_log_text(cycle_fiats_by_platform),
            finished - started,
            fetched_at - started,
            finished - fetched_at,
        )
        if failed_fetches and successful_fetches:
            self.logger.warning(
                "Scanner degraded but continuing: %s/%s fetches failed this cycle",
                failed_fetches,
                len(batches),
            )
        return ScanCycleResult(
            orders=orders,
            total_fetches=len(batches),
            successful_fetches=successful_fetches,
            failed_fetches=failed_fetches,
        )

    async def _safe_fetch(
        self,
        platform: str,
        asset: str,
        fiat: str,
        side: str,
    ) -> FetchResult:
        timeout_sec = max(1, int(self.settings.scanner_fetch_timeout_sec))
        try:
            semaphore = self._platform_semaphores.get(platform)
            if semaphore is None:
                self.logger.warning("Unsupported platform in scanner: %s", platform)
                return FetchResult(platform=platform, asset=asset, fiat=fiat, side=side, orders=[], ok=False)
            async with semaphore:
                if platform == "binance":
                    orders = await asyncio.wait_for(
                        self.binance_client.fetch_orders(asset, fiat, side),
                        timeout=timeout_sec,
                    )
                    return FetchResult(platform=platform, asset=asset, fiat=fiat, side=side, orders=orders, ok=True)
                if platform == "bingx":
                    orders = await asyncio.wait_for(
                        self.bingx_client.fetch_orders(asset, fiat, side),
                        timeout=timeout_sec,
                    )
                    return FetchResult(platform=platform, asset=asset, fiat=fiat, side=side, orders=orders, ok=True)
                if platform == "bybit":
                    orders = await asyncio.wait_for(
                        self.bybit_client.get_online_ads(asset, fiat, side),
                        timeout=timeout_sec,
                    )
                    return FetchResult(platform=platform, asset=asset, fiat=fiat, side=side, orders=orders, ok=True)
            self.logger.warning("Unsupported platform in scanner: %s", platform)
            return FetchResult(platform=platform, asset=asset, fiat=fiat, side=side, orders=[], ok=False)
        except asyncio.TimeoutError:
            self.logger.warning(
                "Fetch timed out for %s %s/%s %s after %ss",
                platform,
                asset,
                fiat,
                side,
                timeout_sec,
            )
            return FetchResult(platform=platform, asset=asset, fiat=fiat, side=side, orders=[], ok=False)
        except Exception as exc:
            self.logger.warning(
                "Fetch failed for %s %s/%s %s: %s",
                platform,
                asset,
                fiat,
                side,
                exc,
            )
            return FetchResult(platform=platform, asset=asset, fiat=fiat, side=side, orders=[], ok=False)

    async def _notify_kill_switch_once(self, reason: str) -> None:
        if self._kill_switch_notified:
            return
        self._kill_switch_notified = True
        await self.alert_queue.put(Event("kill_switch", {"reason": reason}))

    def _fiats_for_cycle(self, platform: str | None = None) -> list[str]:
        fiats = [fiat.upper() for fiat in self.settings.fiats]
        if not fiats:
            return []
        max_fiats = min(len(fiats), self._target_fiats_per_cycle(platform))
        if max_fiats >= len(fiats):
            return fiats

        core_count = min(2, max_fiats, len(fiats))
        core_fiats = fiats[:core_count]
        rotating_fiats = fiats[core_count:]
        rotating_slots = max_fiats - len(core_fiats)
        if rotating_slots <= 0 or not rotating_fiats:
            return core_fiats

        rotation_key = str(platform or "__shared__").lower()
        rotation_offset = self._fiat_rotation_offsets.get(rotation_key, 0)
        selected_rotating = [
            rotating_fiats[(rotation_offset + index) % len(rotating_fiats)]
            for index in range(rotating_slots)
        ]
        self._fiat_rotation_offsets[rotation_key] = (rotation_offset + rotating_slots) % len(rotating_fiats)
        return [*core_fiats, *selected_rotating]

    def _max_fiats_per_cycle(self, platform: str | None = None) -> int:
        if platform is not None:
            return max(1, self._platform_request_budget(platform) // 2)
        if not self.settings.enabled_platforms:
            return max(1, len(self.settings.fiats))
        requests_per_fiat = 2
        platform_budgets = [
            max(2, self._platform_request_budget(platform))
            for platform in self.settings.enabled_platforms
        ]
        return max(1, min(platform_budgets) // requests_per_fiat)

    def _platform_request_budget(self, platform: str) -> int:
        cycle_sec = max(1.0, float(self.settings.scan_interval_sec))
        platform_lower = str(platform).lower()
        if platform_lower == "binance":
            max_calls = max(1, int(self.settings.binance.rate_limit_max_calls))
            period_sec = max(0.1, float(self.settings.binance.rate_limit_period_sec))
            return int(cycle_sec / period_sec * max_calls)
        if platform_lower == "bingx":
            max_calls = max(1, int(self.settings.bingx.rate_limit_max_calls))
            period_sec = max(0.1, float(self.settings.bingx.rate_limit_period_sec))
            return int(cycle_sec / period_sec * max_calls)
        if platform_lower == "bybit":
            if self.settings.bybit.use_official_p2p_api and self.settings.bybit.has_credentials:
                timeout_sec = max(1.0, float(self.settings.scanner_fetch_timeout_sec))
                concurrency = max(1, self._platform_scan_concurrency("bybit"))
                return max(2, int(cycle_sec // timeout_sec) * concurrency)
            return int(cycle_sec / 60.0 * 20)
        return int(cycle_sec)

    def _target_fiats_per_cycle(self, platform: str | None) -> int:
        fiats_total = max(1, len(self.settings.fiats))
        if platform is None:
            return min(fiats_total, self._max_fiats_per_cycle())
        platform_lower = str(platform).lower()
        if platform_lower != "bybit":
            return min(fiats_total, self._max_fiats_per_cycle(platform))
        if not (self.settings.bybit.use_official_p2p_api and self.settings.bybit.has_credentials):
            return min(fiats_total, self._max_fiats_per_cycle(platform))
        floor_cap = min(fiats_total, self._max_fiats_per_cycle(platform))
        if floor_cap >= fiats_total:
            return fiats_total
        return max(floor_cap, self._platform_dynamic_fiat_caps.get(platform_lower, floor_cap))

    def _update_dynamic_fiat_caps(
        self,
        *,
        cycle_fiats_by_platform: dict[str, list[str]],
        batches: list[FetchResult],
        fetch_elapsed_sec: float,
    ) -> None:
        platform = "bybit"
        if platform not in cycle_fiats_by_platform:
            return
        if not (self.settings.bybit.use_official_p2p_api and self.settings.bybit.has_credentials):
            return
        fiats_total = max(1, len(self.settings.fiats))
        floor_cap = min(fiats_total, self._max_fiats_per_cycle(platform))
        current_cap = max(floor_cap, len(cycle_fiats_by_platform.get(platform, ())))
        platform_batches = [batch for batch in batches if batch.platform == platform]
        failures = sum(1 for batch in platform_batches if not batch.ok)
        cycle_target_sec = max(1.0, float(self.settings.scan_interval_sec))

        next_cap = current_cap
        if failures > 0 or fetch_elapsed_sec > cycle_target_sec:
            backoff = max(1, failures if failures > 0 else 2)
            next_cap = max(floor_cap, current_cap - backoff)
        elif fetch_elapsed_sec <= cycle_target_sec * 0.80:
            next_cap = min(fiats_total, current_cap + 3)
        elif fetch_elapsed_sec <= cycle_target_sec * 0.90:
            next_cap = min(fiats_total, current_cap + 2)
        elif fetch_elapsed_sec <= cycle_target_sec * 0.98:
            next_cap = min(fiats_total, current_cap + 1)

        self._platform_dynamic_fiat_caps[platform] = next_cap
        if next_cap != current_cap:
            self.logger.info(
                "Adjusted %s fiat cap from %s to %s (failures=%s, fetch=%.2fs)",
                platform,
                current_cap,
                next_cap,
                failures,
                fetch_elapsed_sec,
            )

    @staticmethod
    def _cycle_fiats_log_text(cycle_fiats_by_platform: dict[str, list[str]]) -> str:
        if not cycle_fiats_by_platform:
            return "-"
        parts = []
        for platform, fiats in cycle_fiats_by_platform.items():
            parts.append(f"{platform}[{len(fiats)}]={','.join(fiats)}")
        return "; ".join(parts)

    def _platform_scan_concurrency(self, platform: str) -> int:
        platform_lower = str(platform).lower()
        if platform_lower == "binance":
            return 1
        if platform_lower == "bingx":
            return 2
        if platform_lower == "bybit":
            if self.settings.bybit.use_official_p2p_api and self.settings.bybit.has_credentials:
                return 2
            return 1
        return 1

    def _remaining_cycle_delay(self, started_at: float) -> float:
        elapsed = asyncio.get_running_loop().time() - started_at
        return max(0.0, float(self.settings.scan_interval_sec) - elapsed)
