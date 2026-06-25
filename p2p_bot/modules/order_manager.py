from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from p2p_bot.config import PairOrderConfig, Settings
from p2p_bot.db.repositories import TradeRepository
from p2p_bot.models.events import Event
from p2p_bot.models.trade import Trade
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.state import AppState, utc_now
from p2p_bot.utils.bybit_p2p import BybitP2PClient
from p2p_bot.utils.canonical_side import (
    bybit_api_side_code,
    bybit_v5_side_code,
    canonical_side_from_bybit_response,
    normalize_side,
)
from p2p_bot.utils.http import ExchangeConfigurationError

MANAGED_REMARK_PREFIX = "Managed by p2p_bot"


@dataclass
class ManagedAdSnapshot:
    order_id: str
    side: str
    asset: str
    fiat: str
    price: Decimal
    min_amount: Decimal
    max_amount: Decimal
    payment_ids: list[str]
    payment_names: list[str]
    remark: str
    quantity: Decimal
    payment_period: str = "15"
    price_type: str = "0"
    premium: str = "0"
    item_type: str = "ORIGIN"
    trading_preferences: dict[str, str] | None = None

    def _serialized_trading_preferences(self) -> dict[str, str]:
        preferences = self.trading_preferences or {}
        serialized: dict[str, str] = {}
        for key, value in preferences.items():
            if value is None:
                continue
            if isinstance(value, bool):
                serialized[str(key)] = "1" if value else "0"
            else:
                serialized[str(key)] = str(value)
        return serialized

    def to_create_payload(self) -> dict[str, Any]:
        return {
            "tokenId": self.asset,
            "currencyId": self.fiat,
            "side": bybit_v5_side_code(normalize_side(self.side)),
            "priceType": self.price_type,
            "premium": self.premium,
            "price": str(self.price),
            "minAmount": str(self.min_amount),
            "maxAmount": str(self.max_amount),
            "remark": self.remark,
            "tradingPreferenceSet": self._serialized_trading_preferences(),
            "paymentIds": list(self.payment_ids),
            "quantity": str(self.quantity),
            "paymentPeriod": self.payment_period,
            "itemType": self.item_type,
        }

    def to_update_payload(self, order_id: str, *, action_type: str = "MODIFY") -> dict[str, Any]:
        return {
            "id": order_id,
            "priceType": self.price_type,
            "premium": self.premium,
            "price": str(self.price),
            "minAmount": str(self.min_amount),
            "maxAmount": str(self.max_amount),
            "remark": self.remark,
            "tradingPreferenceSet": self._serialized_trading_preferences(),
            "paymentIds": list(self.payment_ids),
            "actionType": action_type,
            "quantity": str(self.quantity),
            "paymentPeriod": self.payment_period,
        }


class OrderManager:
    def __init__(
        self,
        settings: Settings,
        state: AppState,
        risk_guard: RiskGuard,
        trade_repository: TradeRepository,
        bybit_client: BybitP2PClient,
        alert_queue: asyncio.Queue[Event],
        logger: logging.Logger,
    ) -> None:
        self.settings = settings
        self.state = state
        self.risk_guard = risk_guard
        self.trade_repository = trade_repository
        self.bybit_client = bybit_client
        self.alert_queue = alert_queue
        self.logger = logger
        self._active_ads: dict[str, ManagedAdSnapshot] = {}
        self._paused_ads: dict[str, ManagedAdSnapshot] = {}
        self._pending_release_alerted: set[str] = set()
        self._notified_order_status: dict[str, int] = {}

    @property
    def enabled(self) -> bool:
        return self.bybit_client.config.has_credentials and self.bybit_client.private_api_access_allowed

    async def create_order(
        self,
        side: str,
        asset: str,
        fiat: str,
        price: Decimal,
        min_amount: Decimal,
        max_amount: Decimal,
        payment_methods: list[str],
    ) -> str:
        self._ensure_enabled()
        config = self.settings.order_config_for(fiat, asset)
        payment_ids, payment_names = await self._resolve_payment_ids(payment_methods)
        quantity = min(max_amount / price, config.max_single_order_usd / price)
        snapshot = ManagedAdSnapshot(
            order_id="",
            side=side,
            asset=asset,
            fiat=fiat,
            price=price,
            min_amount=min_amount,
            max_amount=max_amount,
            payment_ids=payment_ids,
            payment_names=payment_names,
            remark=f"{MANAGED_REMARK_PREFIX} | target spread {config.target_spread_pct}%",
            quantity=quantity,
            trading_preferences=self._default_trading_preferences(),
        )
        response = await self.bybit_client.create_ad(snapshot.to_create_payload())
        order_id = str(response.get("itemId") or "")
        snapshot.order_id = order_id
        self._active_ads[order_id] = snapshot
        return order_id

    async def update_price(self, order_id: str, new_price: Decimal) -> bool:
        self._ensure_enabled()
        snapshot = await self._ensure_snapshot(order_id)
        if snapshot is None:
            return False
        snapshot.price = new_price
        await self.bybit_client.update_ad(snapshot.to_update_payload(order_id))
        self._active_ads[order_id] = snapshot
        return True

    async def pause_order(self, order_id: str) -> bool:
        self._ensure_enabled()
        snapshot = await self._ensure_snapshot(order_id)
        if snapshot is None:
            return False
        await self.bybit_client.remove_ad(order_id)
        self._paused_ads[order_id] = snapshot
        self._active_ads.pop(order_id, None)
        return True

    async def resume_order(self, order_id: str) -> bool:
        self._ensure_enabled()
        snapshot = self._paused_ads.get(order_id)
        if snapshot is None:
            return False
        response = await self.bybit_client.create_ad(snapshot.to_create_payload())
        new_order_id = str(response.get("itemId") or order_id)
        snapshot.order_id = new_order_id
        self._active_ads[new_order_id] = snapshot
        self._paused_ads.pop(order_id, None)
        return True

    async def cancel_order(self, order_id: str) -> bool:
        self._ensure_enabled()
        await self.bybit_client.remove_ad(order_id)
        self._active_ads.pop(order_id, None)
        self._paused_ads.pop(order_id, None)
        return True

    async def get_active_orders(self) -> list[dict[str, Any]]:
        self._ensure_enabled()
        ads = await self.bybit_client.get_my_ads(status="2")
        self._active_ads.clear()
        for ad in ads:
            snapshot = self._snapshot_from_ad(ad)
            self._active_ads[snapshot.order_id] = snapshot
        return [self._snapshot_to_dict(snapshot) for snapshot in self._active_ads.values()]

    async def get_order_status(self, order_id: str) -> dict[str, Any]:
        self._ensure_enabled()
        return await self.bybit_client.get_order_detail(order_id)

    async def release_trade(self, order_id: str) -> bool:
        self._ensure_enabled()
        await self.bybit_client.release_assets(order_id)
        detail = await self.bybit_client.get_order_detail(order_id)
        await self._upsert_trade_from_detail(detail, status="completed")
        return True

    async def mark_trade_as_paid(self, order_id: str) -> bool:
        self._ensure_enabled()
        detail = await self.bybit_client.get_order_detail(order_id)
        payment_info = (detail.get("confirmedPayTerm") or {}).copy()
        if not payment_info or not payment_info.get("id"):
            payment_terms = detail.get("paymentTermList") or []
            if payment_terms:
                payment_info = payment_terms[0]
        payment_id = str(payment_info.get("id") or "")
        payment_type = str(payment_info.get("paymentType") or "")
        if not payment_id or not payment_type:
            raise RuntimeError("Order payment details are missing, cannot mark as paid.")
        await self.bybit_client.mark_as_paid(order_id, payment_type, payment_id)
        await self._upsert_trade_from_detail(detail, status="open")
        return True

    async def pause_all_active_orders(self) -> int:
        await self.get_active_orders()
        count = 0
        for order_id in list(self._active_ads):
            if await self.pause_order(order_id):
                count += 1
        return count

    async def pause_all_managed_orders(self) -> int:
        await self.get_active_orders()
        count = 0
        for snapshot in list(self._active_ads.values()):
            if not self._is_managed_snapshot(snapshot):
                continue
            if await self.pause_order(snapshot.order_id):
                count += 1
        return count

    async def resume_paused_orders(self) -> int:
        count = 0
        for order_id in list(self._paused_ads):
            if await self.resume_order(order_id):
                count += 1
        return count

    async def price_optimizer_loop(self, stop_event: asyncio.Event) -> None:
        if not self.enabled:
            return
        while not stop_event.is_set():
            try:
                if not self.state.paused:
                    await self.optimize_prices_once()
            except Exception as exc:
                self.logger.warning("Price optimizer failed: %s", exc)
            await asyncio.sleep(self.settings.price_update_interval_sec)

    async def optimize_prices_once(self) -> None:
        await self.get_active_orders()
        managed_snapshots = [
            snapshot
            for snapshot in self._active_ads.values()
            if self._is_managed_snapshot(snapshot)
        ]
        for snapshot in managed_snapshots:
            side = snapshot.side
            asset = snapshot.asset
            fiat = snapshot.fiat
            order_id = snapshot.order_id
            current_price = snapshot.price
            competitor_orders = await self.bybit_client.get_online_ads(asset, fiat, side)
            competitors = [order for order in competitor_orders if order.order_id != order_id]
            if len(competitors) < 3:
                continue

            better_competitors = self._better_competitors(side, competitors)
            better_prices = [order.price for order in better_competitors if self._is_better_price(side, order.price, current_price)]
            rank = 1 + len(better_prices)
            if rank <= 3:
                continue

            target_price = self._target_price(side, better_competitors[0].price)
            acceptable = self._acceptable_price_bound(side, fiat)
            if acceptable is None:
                continue

            if side == "buy" and target_price > acceptable:
                await self.pause_order(order_id)
                await self.alert_queue.put(
                    Event(
                        "warning",
                        {
                            "message": f"Paused {fiat} buy ad {order_id}: top-3 would break min spread",
                        },
                    )
                )
                continue
            if side == "sell" and target_price < acceptable:
                await self.pause_order(order_id)
                await self.alert_queue.put(
                    Event(
                        "warning",
                        {
                            "message": f"Paused {fiat} sell ad {order_id}: top-3 would break min spread",
                        },
                    )
                )
                continue

            await self.update_price(order_id, target_price)

    async def monitor_pending_orders(self, stop_event: asyncio.Event) -> None:
        if not self.enabled:
            return
        while not stop_event.is_set():
            try:
                orders = await self.bybit_client.get_orders(page=1, size=30)
                for order in orders:
                    order_id = str(order.get("id") or "")
                    if not order_id:
                        continue
                    detail = await self.bybit_client.get_order_detail(order_id)
                    await self._handle_order_event(detail)
            except Exception as exc:
                self.logger.warning("Pending-order monitor failed: %s", exc)
            await asyncio.sleep(self.settings.order_poll_interval_sec)

    async def sync_managed_order(
        self,
        *,
        side: str,
        asset: str,
        fiat: str,
        price: Decimal,
        min_amount: Decimal,
        max_amount: Decimal,
        payment_methods: list[str],
    ) -> str:
        self._ensure_enabled()
        snapshot = await self.find_managed_order(asset, fiat, side)
        if snapshot is None:
            return await self.create_order(
                side=side,
                asset=asset,
                fiat=fiat,
                price=price,
                min_amount=min_amount,
                max_amount=max_amount,
                payment_methods=payment_methods,
            )

        changed = False
        if snapshot.price != price:
            snapshot.price = price
            changed = True
        if snapshot.min_amount != min_amount:
            snapshot.min_amount = min_amount
            changed = True
        if snapshot.max_amount != max_amount:
            snapshot.max_amount = max_amount
            changed = True
        if snapshot.payment_names != payment_methods:
            payment_ids, payment_names = await self._resolve_payment_ids(payment_methods)
            snapshot.payment_ids = payment_ids
            snapshot.payment_names = payment_names
            changed = True
        if not changed:
            return snapshot.order_id

        await self.bybit_client.update_ad(snapshot.to_update_payload(snapshot.order_id))
        self._active_ads[snapshot.order_id] = snapshot
        return snapshot.order_id

    async def find_managed_order(
        self,
        asset: str,
        fiat: str,
        side: str,
    ) -> ManagedAdSnapshot | None:
        if not self._active_ads:
            await self.get_active_orders()
        for snapshot in self._active_ads.values():
            if (
                snapshot.asset == asset
                and snapshot.fiat == fiat
                and snapshot.side == side
                and self._is_managed_snapshot(snapshot)
            ):
                return snapshot
        return None

    async def pause_managed_order(self, asset: str, fiat: str, side: str) -> bool:
        snapshot = await self.find_managed_order(asset, fiat, side)
        if snapshot is None:
            return False
        return await self.pause_order(snapshot.order_id)

    async def get_available_usdt_balance(self) -> Decimal:
        self._ensure_enabled()
        result = await self.bybit_client.get_coin_balance(
            account_type=self.settings.bybit.balance_account_type,
            coin=self.settings.base_asset,
        )
        balances = result.get("balance") or []
        if not balances:
            return Decimal("0")
        return Decimal(str((balances[0] or {}).get("transferBalance") or (balances[0] or {}).get("walletBalance") or "0"))

    async def _resolve_payment_ids(self, payment_methods: list[str]) -> tuple[list[str], list[str]]:
        payment_types = await self.bybit_client.get_user_payment_types()
        resolved_ids: list[str] = []
        resolved_names: list[str] = []
        wanted = [method.lower() for method in payment_methods]
        for payment in payment_types:
            name = str(((payment.get("paymentConfigVo") or {}).get("paymentName")) or payment.get("bankName") or "")
            bank_name = str(payment.get("bankName") or "")
            haystack = f"{name} {bank_name}".lower()
            if any(target in haystack for target in wanted):
                resolved_ids.append(str(payment.get("id")))
                resolved_names.append(name or bank_name)
        if not resolved_ids:
            raise ExchangeConfigurationError(
                f"None of requested payment methods were found on Bybit account: {payment_methods}"
            )
        return resolved_ids, resolved_names

    def _default_trading_preferences(self) -> dict[str, str]:
        filter_cfg = self.settings.counterparty_filter
        return {
            "hasUnPostAd": "0",
            "isKyc": "1",
            "isEmail": "0",
            "isMobile": "0",
            "hasRegisterTime": "1" if filter_cfg.block_new_accounts else "0",
            "registerTimeThreshold": str(filter_cfg.min_account_days),
            "orderFinishNumberDay30": str(filter_cfg.min_completed_orders),
            "completeRateDay30": str(filter_cfg.min_rating),
            "nationalLimit": "",
            "hasOrderFinishNumberDay30": "1",
            "hasCompleteRateDay30": "1",
        }

    async def _ensure_snapshot(self, order_id: str) -> ManagedAdSnapshot | None:
        snapshot = self._active_ads.get(order_id)
        if snapshot is not None:
            return snapshot
        await self.get_active_orders()
        return self._active_ads.get(order_id)

    def _snapshot_from_ad(self, ad: dict[str, Any]) -> ManagedAdSnapshot:
        payment_ids = [str(item) for item in (ad.get("payments") or [])]
        payment_names = [self.bybit_client._payment_catalog.get(item, item) for item in payment_ids]
        canonical_side = canonical_side_from_bybit_response(ad.get("side")) or "sell"
        return ManagedAdSnapshot(
            order_id=str(ad.get("id") or ""),
            side=normalize_side(canonical_side),
            asset=str(ad.get("tokenId") or self.settings.base_asset),
            fiat=str(ad.get("currencyId") or ""),
            price=Decimal(str(ad.get("price") or "0")),
            min_amount=Decimal(str(ad.get("minAmount") or "0")),
            max_amount=Decimal(str(ad.get("maxAmount") or "0")),
            payment_ids=payment_ids,
            payment_names=payment_names,
            remark=str(ad.get("remark") or ""),
            quantity=Decimal(str(ad.get("quantity") or ad.get("lastQuantity") or "0")),
            trading_preferences=ad.get("tradingPreferenceSet") or self._default_trading_preferences(),
        )

    def _snapshot_to_dict(self, snapshot: ManagedAdSnapshot) -> dict[str, Any]:
        return {
            "order_id": snapshot.order_id,
            "side": snapshot.side,
            "asset": snapshot.asset,
            "fiat": snapshot.fiat,
            "price": str(snapshot.price),
            "min_amount": str(snapshot.min_amount),
            "max_amount": str(snapshot.max_amount),
            "payment_methods": snapshot.payment_names,
        }

    def _better_competitors(self, side: str, competitors: list[Any]) -> list[Any]:
        return sorted(
            competitors,
            key=lambda order: order.price,
            reverse=(side == "buy"),
        )

    def _is_better_price(self, side: str, candidate: Decimal, current: Decimal) -> bool:
        return candidate > current if side == "buy" else candidate < current

    def _target_price(self, side: str, competitor_price: Decimal) -> Decimal:
        delta = self.settings.price_improvement_step_pct / Decimal("100")
        if side == "buy":
            return (competitor_price * (Decimal("1") + delta)).quantize(Decimal("0.0001"))
        return (competitor_price * (Decimal("1") - delta)).quantize(Decimal("0.0001"))

    def _acceptable_price_bound(self, side: str, fiat: str) -> Decimal | None:
        config: PairOrderConfig = self.settings.order_config_for(fiat)
        opposite_key = f"bybit:{self.settings.base_asset}:{fiat}:{'sell' if side == 'buy' else 'buy'}"
        orders = self.state.latest_orders.get(opposite_key, [])
        if not orders:
            return None
        spread_factor = Decimal("1") + (config.min_spread_pct / Decimal("100"))
        if side == "buy":
            best_exit = max(order.price for order in orders)
            return (best_exit / spread_factor).quantize(Decimal("0.0001"))
        best_entry = min(order.price for order in orders)
        return (best_entry * spread_factor).quantize(Decimal("0.0001"))

    async def _upsert_trade_from_detail(self, detail: dict[str, Any], status: str) -> None:
        trade = Trade(
            platform="bybit",
            trade_id=str(detail.get("id") or ""),
            side=canonical_side_from_bybit_response(detail.get("side")) or "sell",
            fiat=str(detail.get("currencyId") or ""),
            price=Decimal(str(detail.get("price") or "0")),
            volume_usdt=Decimal(str(detail.get("quantity") or "0")),
            volume_fiat=Decimal(str(detail.get("amount") or "0")),
            profit_usd=Decimal("0"),
            counterparty_id=str(detail.get("targetUserId") or ""),
            counterparty_rating=0.0,
            status=status,
            opened_at=utc_now(),
        )
        await self.trade_repository.upsert(trade)

    async def _handle_order_event(self, detail: dict[str, Any]) -> None:
        order_id = str(detail.get("id") or "")
        if not order_id:
            return
        status = int(detail.get("status") or 0)
        side = int(detail.get("side") or 0)

        if status == 50:
            await self._upsert_trade_from_detail(detail, status="completed")
            self._notified_order_status[order_id] = status
            return

        await self._upsert_trade_from_detail(detail, status="open")
        previous_status = self._notified_order_status.get(order_id)
        if previous_status == status:
            return
        self._notified_order_status[order_id] = status

        if status == 10 and side == 1:
            await self.alert_queue.put(Event("payment_required", detail))
            return
        if status == 20 and side == 0:
            await self.alert_queue.put(Event("payment_pending", detail))
            return

    @staticmethod
    def _is_managed_snapshot(snapshot: ManagedAdSnapshot) -> bool:
        return snapshot.remark.startswith(MANAGED_REMARK_PREFIX)

    def _ensure_enabled(self) -> None:
        if not self.enabled:
            raise ExchangeConfigurationError("Bybit advertiser API credentials are not configured.")
