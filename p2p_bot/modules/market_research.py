from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal

from p2p_bot.config import Settings
from p2p_bot.db.repositories import RawOrderRepository
from p2p_bot.models.order import P2POrder
from p2p_bot.modules.analyzer import Analyzer
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.state import AppState, utc_now
from p2p_bot.utils.binance_p2p import BinanceP2PClient
from p2p_bot.utils.bingx_p2p import BingXP2PClient
from p2p_bot.utils.bybit_p2p import BybitP2PClient


class MarketResearchService:
    def __init__(
        self,
        settings: Settings,
        state: AppState,
        risk_guard: RiskGuard,
        raw_order_repository: RawOrderRepository,
        analyzer: Analyzer,
        binance_client: BinanceP2PClient,
        bingx_client: BingXP2PClient,
        bybit_client: BybitP2PClient,
        logger: logging.Logger,
    ) -> None:
        self.settings = settings
        self.state = state
        self.risk_guard = risk_guard
        self.raw_order_repository = raw_order_repository
        self.analyzer = analyzer
        self.binance_client = binance_client
        self.bingx_client = bingx_client
        self.bybit_client = bybit_client
        self.logger = logger

    async def refresh(self, *, force: bool = False) -> list[dict[str, object]]:
        if not force and self.state.market_last_run_at is not None:
            ttl = timedelta(minutes=self.settings.market_discovery_cache_ttl_min)
            if utc_now() - self.state.market_last_run_at < ttl and self.state.market_overview:
                return self.state.market_overview

        orders = await self._scan_discovery_orders()
        if orders:
            await self.raw_order_repository.save_batch(orders)
        opportunities = await self.analyzer.analyze_orders(orders)
        overview = await self._build_overview(orders, opportunities)
        self.state.record_market_overview(overview)
        return overview

    async def _scan_discovery_orders(self) -> list[P2POrder]:
        tasks = []
        for fiat in self.settings.market_discovery_fiats:
            for side in ("buy", "sell"):
                for platform in self.settings.enabled_platforms:
                    tasks.append(self._safe_fetch(platform, self.settings.base_asset, fiat, side))
        batches = await self._gather(tasks)
        return [order for batch in batches for order in batch]

    async def _gather(self, tasks: list[object]) -> list[list[P2POrder]]:
        import asyncio

        return await asyncio.gather(*tasks)

    async def _safe_fetch(
        self,
        platform: str,
        asset: str,
        fiat: str,
        side: str,
    ) -> list[P2POrder]:
        try:
            if platform == "binance":
                return await self.binance_client.fetch_orders(asset, fiat, side)
            if platform == "bingx":
                return await self.bingx_client.fetch_orders(asset, fiat, side)
            if platform == "bybit":
                return await self.bybit_client.get_online_ads(asset, fiat, side)
            return []
        except Exception as exc:
            self.logger.warning("Market research fetch failed for %s %s/%s %s: %s", platform, asset, fiat, side, exc)
            return []

    async def _build_overview(
        self,
        orders: list[P2POrder],
        opportunities: list[object],
    ) -> list[dict[str, object]]:
        history = await self.raw_order_repository.recent_activity(
            (1, 3),
            asset=self.settings.base_asset,
            fiats=self.settings.market_discovery_fiats,
        )

        overview: list[dict[str, object]] = []
        for fiat in self.settings.market_discovery_fiats:
            fiat_orders = [order for order in orders if order.fiat == fiat]
            safe_orders = self.risk_guard.filter_orders(fiat_orders)
            buy_orders = [order for order in safe_orders if order.side == "buy"]
            sell_orders = [order for order in safe_orders if order.side == "sell"]

            depth_buy = sum(
                (order.available for order in sorted(buy_orders, key=lambda item: item.available, reverse=True)[:10]),
                start=Decimal("0"),
            )
            depth_sell = sum(
                (order.available for order in sorted(sell_orders, key=lambda item: item.available, reverse=True)[:10]),
                start=Decimal("0"),
            )
            tradable_depth = min(depth_buy, depth_sell)
            quote_floor = min(len(buy_orders), len(sell_orders)) if buy_orders and sell_orders else 0

            fiat_opps = [item for item in opportunities if item.buy_order.fiat == fiat]
            confirmed = [item for item in fiat_opps if item.rail_status == "confirmed"]
            liquid_confirmed = [
                item
                for item in confirmed
                if item.liquidity_status == "liquid"
                or (
                    self.settings.alert_allow_tradable_liquidity
                    and item.liquidity_status == "tradable"
                )
            ]
            mixed_or_unknown = [item for item in fiat_opps if item.rail_status != "confirmed"]
            best_confirmed = max(confirmed, key=lambda item: (item.estimated_profit_usd, item.spread_pct), default=None)
            best_liquid = max(liquid_confirmed, key=lambda item: (item.estimated_profit_usd, item.spread_pct), default=None)

            ads_1h = self._sum_history_ads(history.get(1, {}), fiat)
            ads_3h = self._sum_history_ads(history.get(3, {}), fiat)

            min_spread, min_profit = self.settings.alert_thresholds_for(fiat, self.settings.base_asset)
            if best_liquid and best_liquid.estimated_profit_usd >= min_profit and best_liquid.spread_pct >= min_spread:
                status = "ACTIVE"
                reason = (
                    f"profit≈{best_liquid.estimated_profit_usd} USD, "
                    f"spread={best_liquid.spread_pct}% | {best_liquid.note}"
                )
                best_profit = best_liquid.estimated_profit_usd
                best_spread = best_liquid.spread_pct
                best_route = best_liquid.note
                liquidity = best_liquid.liquidity_status
                liquidity_score = best_liquid.liquidity_score
            elif best_confirmed:
                status = "WATCH"
                reason = (
                    f"confirmed route exists, but profit/spread below target: "
                    f"profit≈{best_confirmed.estimated_profit_usd} USD, spread={best_confirmed.spread_pct}%"
                )
                best_profit = best_confirmed.estimated_profit_usd
                best_spread = best_confirmed.spread_pct
                best_route = best_confirmed.note
                liquidity = best_confirmed.liquidity_status
                liquidity_score = best_confirmed.liquidity_score
            elif mixed_or_unknown and ads_3h >= 20:
                status = "NO_RAIL"
                reason = "market is active, but current payment rails do not fit your setup"
                best_profit = Decimal("0")
                best_spread = Decimal("0")
                best_route = "n/a"
                liquidity = "unknown"
                liquidity_score = Decimal("0")
            elif quote_floor >= 3 and tradable_depth >= Decimal("1500") and ads_3h >= 10:
                status = "WATCH"
                reason = "market has liquidity, but no confirmed route for your current filters"
                best_profit = Decimal("0")
                best_spread = Decimal("0")
                best_route = "n/a"
                liquidity = "tradable"
                liquidity_score = Decimal("0")
            else:
                status = "DEAD"
                reason = "thin market or not enough usable quotes"
                best_profit = Decimal("0")
                best_spread = Decimal("0")
                best_route = "n/a"
                liquidity = "illiquid"
                liquidity_score = Decimal("0")

            score = (
                best_profit
                + (tradable_depth / Decimal("1000"))
                + Decimal(str(ads_3h)) / Decimal("10")
            ).quantize(Decimal("0.01"))
            overview.append(
                {
                    "fiat": fiat,
                    "status": status,
                    "score": score,
                    "best_profit_usd": best_profit,
                    "best_spread_pct": best_spread,
                    "best_route": best_route,
                    "liquidity_status": liquidity,
                    "liquidity_score": liquidity_score,
                    "tradable_depth_usdt": tradable_depth.quantize(Decimal("0.1")),
                    "quote_floor": quote_floor,
                    "ads_1h": ads_1h,
                    "ads_3h": ads_3h,
                    "reason": reason,
                }
            )

        priority = {"ACTIVE": 0, "WATCH": 1, "NO_RAIL": 2, "DEAD": 3}
        return sorted(
            overview,
            key=lambda item: (
                priority.get(str(item["status"]), 9),
                -Decimal(str(item["best_profit_usd"])),
                -Decimal(str(item["score"])),
            ),
        )

    @staticmethod
    def _sum_history_ads(
        history_window: dict[tuple[str, str, str, str], dict[str, float]],
        fiat: str,
    ) -> int:
        total = 0
        for (platform, row_fiat, asset, side), stats in history_window.items():
            if row_fiat != fiat:
                continue
            total += int(float(stats.get("unique_ads", 0)))
        return total
