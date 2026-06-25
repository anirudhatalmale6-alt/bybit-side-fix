from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal

from p2p_bot.config import PairOrderConfig, Settings
from p2p_bot.models.events import Event
from p2p_bot.models.order import P2POrder
from p2p_bot.modules.order_manager import OrderManager
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.state import AppState


@dataclass
class OrderProposal:
    side: str
    asset: str
    fiat: str
    price: Decimal
    min_amount: Decimal
    max_amount: Decimal
    payment_methods: list[str]


class StrategyEngine:
    def __init__(
        self,
        settings: Settings,
        state: AppState,
        risk_guard: RiskGuard,
        order_manager: OrderManager,
        alert_queue: asyncio.Queue[Event],
        logger: logging.Logger,
    ) -> None:
        self.settings = settings
        self.state = state
        self.risk_guard = risk_guard
        self.order_manager = order_manager
        self.alert_queue = alert_queue
        self.logger = logger

    @property
    def enabled(self) -> bool:
        return self.settings.enable_order_execution and self.order_manager.enabled

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self.enabled:
            return
        while not stop_event.is_set():
            try:
                await self.sync_once()
            except Exception as exc:
                self.logger.warning("Strategy sync failed: %s", exc)
            await asyncio.sleep(self.settings.strategy_interval_sec)

    async def sync_once(self) -> None:
        can_trade, reason = self.risk_guard.can_trade()
        if not can_trade:
            await self._pause_all_managed(reason)
            return

        await self.order_manager.get_active_orders()
        balance_usdt = await self.order_manager.get_available_usdt_balance()
        sell_budget_usdt = max(balance_usdt - self.settings.min_usdt_balance_reserve, Decimal("0"))
        sell_budget_per_fiat = (
            sell_budget_usdt / Decimal(str(max(len(self.settings.fiats), 1)))
            if self.settings.fiats
            else Decimal("0")
        )

        for fiat in self.settings.fiats:
            config = self.settings.order_config_for(fiat, self.settings.base_asset)
            proposals = self._build_pair_proposals(fiat, config, sell_budget_per_fiat)
            if proposals is None:
                await self.order_manager.pause_managed_order(self.settings.base_asset, fiat, "buy")
                await self.order_manager.pause_managed_order(self.settings.base_asset, fiat, "sell")
                continue

            buy_proposal, sell_proposal = proposals
            if self.settings.auto_manage_buy_side and buy_proposal is not None:
                await self.order_manager.sync_managed_order(**buy_proposal.__dict__)
            elif self.settings.auto_manage_buy_side:
                await self.order_manager.pause_managed_order(self.settings.base_asset, fiat, "buy")

            if self.settings.auto_manage_sell_side and sell_proposal is not None:
                await self.order_manager.sync_managed_order(**sell_proposal.__dict__)
            elif self.settings.auto_manage_sell_side:
                await self.order_manager.pause_managed_order(self.settings.base_asset, fiat, "sell")

    def _build_pair_proposals(
        self,
        fiat: str,
        config: PairOrderConfig,
        sell_budget_per_fiat: Decimal,
    ) -> tuple[OrderProposal | None, OrderProposal | None] | None:
        buy_orders = self._bybit_orders(fiat, "buy")
        sell_orders = self._bybit_orders(fiat, "sell")
        if not buy_orders or not sell_orders:
            return None

        best_bid = max(order.price for order in sell_orders)
        best_ask = min(order.price for order in buy_orders)
        if best_bid <= 0 or best_ask <= 0:
            return None

        step = self.settings.price_improvement_step_pct / Decimal("100")
        target_factor = Decimal("1") + (config.target_spread_pct / Decimal("100"))

        buy_price_ceiling = (best_ask / target_factor).quantize(Decimal("0.0001"))
        competitive_buy = (best_bid * (Decimal("1") + step)).quantize(Decimal("0.0001"))
        buy_price = min(competitive_buy, buy_price_ceiling)

        sell_price_floor = (best_bid * target_factor).quantize(Decimal("0.0001"))
        competitive_sell = (best_ask * (Decimal("1") - step)).quantize(Decimal("0.0001"))
        sell_price = max(competitive_sell, sell_price_floor)

        realized_spread = self._spread_pct(sell_price, buy_price)
        if realized_spread < config.min_spread_pct:
            return None

        buy_qty_usdt = config.max_single_order_usd / max(buy_price, Decimal("0.0001"))
        buy_max_amount = (buy_qty_usdt * buy_price).quantize(Decimal("0.01"))
        buy_min_amount = min(config.min_order_fiat, buy_max_amount).quantize(Decimal("0.01"))
        buy_proposal = None
        if buy_max_amount >= config.min_order_fiat:
            buy_proposal = OrderProposal(
                side="buy",
                asset=self.settings.base_asset,
                fiat=fiat,
                price=buy_price,
                min_amount=buy_min_amount,
                max_amount=buy_max_amount,
                payment_methods=list(config.preferred_payments),
            )

        sell_proposal = None
        sell_qty_usdt = min(
            config.max_single_order_usd,
            sell_budget_per_fiat,
        )
        if sell_qty_usdt > 0:
            sell_max_amount = (sell_qty_usdt * sell_price).quantize(Decimal("0.01"))
            sell_min_amount = min(config.min_order_fiat, sell_max_amount).quantize(Decimal("0.01"))
            if sell_max_amount >= config.min_order_fiat:
                sell_proposal = OrderProposal(
                    side="sell",
                    asset=self.settings.base_asset,
                    fiat=fiat,
                    price=sell_price,
                    min_amount=sell_min_amount,
                    max_amount=sell_max_amount,
                    payment_methods=list(config.preferred_payments),
                )

        return buy_proposal, sell_proposal

    def _bybit_orders(self, fiat: str, side: str) -> list[P2POrder]:
        key = f"bybit:{self.settings.base_asset}:{fiat}:{side}"
        return self.risk_guard.filter_orders(self.state.latest_orders.get(key, []))

    async def _pause_all_managed(self, reason: str) -> None:
        paused = await self.order_manager.pause_all_managed_orders()
        if paused:
            await self.alert_queue.put(
                Event(
                    "warning",
                    {"message": f"Paused {paused} managed ads: {reason}"},
                )
            )

    @staticmethod
    def _spread_pct(sell_price: Decimal, buy_price: Decimal) -> Decimal:
        if buy_price <= 0:
            return Decimal("0")
        return ((sell_price - buy_price) / buy_price) * Decimal("100")
