from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from p2p_bot.config import Settings
from p2p_bot.db.repositories import ExecutionSessionRepository, InventoryRepository, SignalJournalRepository
from p2p_bot.utils.binance_p2p import BinanceP2PClient
from p2p_bot.utils.bybit_p2p import BybitP2PClient


class ExchangeExecutionEngine:
    """
    Disabled-by-default scaffold for future exchange-leg automation.

    Current purpose:
    - hold feature flags and capability checks
    - persist execution sessions safely
    - provide a stable integration point before private API keys arrive

    This module must not interfere with today's scanner / analyzer / Telegram flow.
    """

    def __init__(
        self,
        settings: Settings,
        signal_journal_repository: SignalJournalRepository,
        execution_session_repository: ExecutionSessionRepository,
        inventory_repository: InventoryRepository,
        bybit_client: BybitP2PClient,
        binance_client: BinanceP2PClient,
        logger: logging.Logger,
    ) -> None:
        self.settings = settings
        self.signal_journal_repository = signal_journal_repository
        self.execution_session_repository = execution_session_repository
        self.inventory_repository = inventory_repository
        self.bybit_client = bybit_client
        self.binance_client = binance_client
        self.logger = logger

    @property
    def enabled(self) -> bool:
        return self.settings.exchange_execution.enabled

    def capability_snapshot(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "dry_run": self.settings.exchange_execution.dry_run,
            "reserve_both_legs": self.settings.exchange_execution.reserve_both_legs,
            "require_manual_confirm": self.settings.exchange_execution.require_manual_confirm,
            "max_capital_per_signal_pct": str(self.settings.exchange_execution.max_capital_per_signal_pct),
            "rebalance_tolerance_pct": str(self.settings.exchange_execution.rebalance_tolerance_pct),
            "allowed_platforms": list(self.settings.exchange_execution.allowed_platforms),
            "allowed_treasury_providers": list(self.settings.exchange_execution.allowed_treasury_providers),
            "bybit_private_ready": (
                self.bybit_client.config.has_credentials and self.bybit_client.private_api_access_allowed
            ),
            "binance_private_ready": self.settings.binance_execution.has_credentials,
            "revolut_business_enabled": self.settings.revolut_business.enabled,
            "revolut_business_ready": (
                self.settings.revolut_business.enabled and self.settings.revolut_business.has_credentials
            ),
        }

    async def prepare_session(self, signal_id: int) -> int:
        signal = await self.signal_journal_repository.get(signal_id)
        if signal is None:
            raise RuntimeError(f"Signal #{signal_id} not found")

        revalidation = await self._pre_execution_revalidation(signal)
        rm_snapshot = await self._rm_snapshot_for_signal(signal)
        payload = {
            "prepared_from_signal": signal_id,
            "capabilities": self.capability_snapshot(),
            "revalidation": revalidation,
            "rm": rm_snapshot,
        }
        treasury_provider = self._treasury_provider_for_signal(signal)
        mode = "dry_run" if self.settings.exchange_execution.dry_run else "live"
        initial_status = "prepared"
        initial_error_text = ""
        event_type = "prepared"
        event_payload: dict[str, Any] = {
            "signal_id": signal_id,
            "treasury_provider": treasury_provider,
            "rm_recommended_volume_usdt": rm_snapshot["recommended_volume_usdt"],
            "rm_limit_reason": rm_snapshot["limiting_factor"],
            "revalidation_checked_at": revalidation.get("checked_at"),
        }
        if not bool(revalidation.get("passed")):
            initial_status = "revalidation_failed"
            initial_error_text = str(revalidation.get("reason") or "Pre-execution revalidation failed")
            event_type = "revalidation_failed"
            event_payload = revalidation
        session_id = await self.execution_session_repository.create(
            signal_id=signal_id,
            route=str(signal["route"]),
            status=initial_status,
            mode=mode,
            buy_platform=str(signal["buy_platform"]),
            sell_platform=str(signal["sell_platform"]),
            buy_fiat=str(signal["buy_fiat"]),
            sell_fiat=str(signal["sell_fiat"]),
            volume_usdt=self._as_decimal(signal.get("volume_usdt")),
            estimated_profit_usd=self._as_decimal(signal.get("estimated_profit_usd")),
            treasury_provider=treasury_provider,
            payload=payload,
            error_text=initial_error_text,
        )
        await self.execution_session_repository.append_event(
            session_id,
            event_type,
            event_payload,
        )
        return session_id

    async def reserve_signal(self, signal_id: int) -> int:
        """
        Future entrypoint for one-click exchange reservation.

        For now it persists the intent safely and returns a session id.
        This keeps the operational flow ready without risking current production behavior.
        """
        session_id = await self.prepare_session(signal_id)
        snapshot = self.capability_snapshot()
        session = await self.execution_session_repository.get(session_id)
        if session is not None and str(session.get("status") or "") == "revalidation_failed":
            return session_id
        rm_snapshot = self._session_rm_snapshot(session)

        if not rm_snapshot.get("eligible_for_full_auto", False):
            await self.execution_session_repository.mark_status(
                session_id,
                "out_of_scope",
                error_text=str(rm_snapshot.get("scope_reason") or "Signal is out of auto-execution scope"),
            )
            await self.execution_session_repository.append_event(
                session_id,
                "out_of_scope",
                rm_snapshot,
            )
            return session_id

        if Decimal(str(rm_snapshot.get("recommended_volume_usdt") or "0")) <= 0:
            await self.execution_session_repository.mark_status(
                session_id,
                "blocked_rm",
                error_text=str(rm_snapshot.get("limit_reason") or "Risk manager blocked execution"),
            )
            await self.execution_session_repository.append_event(
                session_id,
                "blocked_rm",
                rm_snapshot,
            )
            return session_id

        if not self.enabled:
            await self.execution_session_repository.mark_status(
                session_id,
                "disabled",
                error_text="Exchange execution is disabled in settings",
            )
            await self.execution_session_repository.append_event(
                session_id,
                "disabled",
                {"reason": "feature_flag_off"},
            )
            return session_id

        if not snapshot["bybit_private_ready"] and not snapshot["binance_private_ready"]:
            await self.execution_session_repository.mark_status(
                session_id,
                "waiting_credentials",
                error_text="Missing private exchange credentials for reservation",
            )
            await self.execution_session_repository.append_event(
                session_id,
                "waiting_credentials",
                snapshot,
            )
            return session_id

        await self.execution_session_repository.mark_status(
            session_id,
            "waiting_adapter",
            error_text=(
                "Private reservation adapters are not wired yet. "
                "Infrastructure is prepared; connect provider-specific endpoints next."
            ),
        )
        await self.execution_session_repository.append_event(
            session_id,
            "waiting_adapter",
            snapshot,
        )
        return session_id

    @staticmethod
    def _as_decimal(value: object) -> Any:
        return Decimal(str(value or "0"))

    async def rebalance_snapshot(self) -> dict[str, Any]:
        balances = await self._inventory_balances()
        binance_usdt = balances.get(("binance:wallet", self.settings.base_asset.upper()), Decimal("0"))
        bybit_usdt = balances.get(("bybit:wallet", self.settings.base_asset.upper()), Decimal("0"))
        total = binance_usdt + bybit_usdt
        if total <= 0:
            return {
                "total_exchange_usdt": "0",
                "target_per_exchange_usdt": "0",
                "tolerance_pct": str(self.settings.exchange_execution.rebalance_tolerance_pct),
                "actions": [],
            }

        target = (total / Decimal("2")).quantize(Decimal("0.0001"))
        tolerance = self.settings.exchange_execution.rebalance_tolerance_pct / Decimal("100")
        high_water = (target * (Decimal("1") + tolerance)).quantize(Decimal("0.0001"))
        low_water = (target * (Decimal("1") - tolerance)).quantize(Decimal("0.0001"))
        actions: list[dict[str, str]] = []

        if binance_usdt > high_water and bybit_usdt < low_water:
            amount = min(binance_usdt - target, target - bybit_usdt).quantize(Decimal("0.0001"))
            if amount > 0:
                actions.append(
                    {
                        "from": "binance",
                        "to": "bybit",
                        "asset": self.settings.base_asset.upper(),
                        "amount": str(amount),
                        "reason": "restore balanced exchange working capital",
                    }
                )
        if bybit_usdt > high_water and binance_usdt < low_water:
            amount = min(bybit_usdt - target, target - binance_usdt).quantize(Decimal("0.0001"))
            if amount > 0:
                actions.append(
                    {
                        "from": "bybit",
                        "to": "binance",
                        "asset": self.settings.base_asset.upper(),
                        "amount": str(amount),
                        "reason": "restore balanced exchange working capital",
                    }
                )

        return {
            "total_exchange_usdt": str(total.quantize(Decimal("0.0001"))),
            "target_per_exchange_usdt": str(target),
            "tolerance_pct": str(self.settings.exchange_execution.rebalance_tolerance_pct),
            "balances": {
                "binance_usdt": str(binance_usdt.quantize(Decimal("0.0001"))),
                "bybit_usdt": str(bybit_usdt.quantize(Decimal("0.0001"))),
            },
            "actions": actions,
        }

    @staticmethod
    def _treasury_provider_for_signal(signal: dict[str, Any]) -> str:
        payload_raw = signal.get("payload_json") or "{}"
        if isinstance(payload_raw, bytes):
            payload_raw = payload_raw.decode("utf-8", errors="ignore")
        try:
            payload = json.loads(str(payload_raw) or "{}")
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        fx_provider = ExchangeExecutionEngine._treasury_provider_from_rail(payload.get("fx_rail"))
        if fx_provider is not None:
            return fx_provider

        providers: list[str] = []
        for field in ("sell_rail", "buy_rail"):
            provider = ExchangeExecutionEngine._treasury_provider_from_rail(payload.get(field))
            if provider is None or provider in providers:
                continue
            providers.append(provider)
        if len(providers) == 1:
            return providers[0]
        if len(providers) >= 2:
            return "mixed"
        return "unknown"

    @staticmethod
    def _treasury_provider_from_rail(rail: object) -> str | None:
        normalized = str(rail or "").strip().lower()
        if not normalized or normalized in {"unknown", "fiat_balance"}:
            return None
        if normalized.startswith("revolut_"):
            return "revolut"
        if normalized.startswith("wise_"):
            return "wise"
        if normalized in {"bank_transfer", "bank_fx", "sepa", "blik", "zen"}:
            return "bank"
        return None

    async def _pre_execution_revalidation(self, signal: dict[str, Any]) -> dict[str, Any]:
        payload = self._signal_payload(signal)
        sell_order = self._payload_order(payload, "sell_order")
        buy_order = self._payload_order(payload, "buy_order")
        checked_at = datetime.now(timezone.utc).isoformat()

        if not sell_order or not buy_order:
            return {
                "passed": False,
                "reason": "signal payload is missing buy/sell order snapshots",
                "checked_at": checked_at,
            }

        if str(sell_order.get("side") or "").strip().lower() != "sell":
            return {
                "passed": False,
                "reason": f"step 1 payload side mismatch: expected sell, got {sell_order.get('side')}",
                "checked_at": checked_at,
            }
        if str(buy_order.get("side") or "").strip().lower() != "buy":
            return {
                "passed": False,
                "reason": f"step 3 payload side mismatch: expected buy, got {buy_order.get('side')}",
                "checked_at": checked_at,
            }

        signal_sell_platform = str(signal.get("sell_platform") or "").strip().lower()
        signal_buy_platform = str(signal.get("buy_platform") or "").strip().lower()
        signal_sell_fiat = str(signal.get("sell_fiat") or "").strip().upper()
        signal_buy_fiat = str(signal.get("buy_fiat") or "").strip().upper()
        payload_sell_platform = str(sell_order.get("platform") or "").strip().lower()
        payload_buy_platform = str(buy_order.get("platform") or "").strip().lower()
        payload_sell_fiat = str(sell_order.get("fiat") or "").strip().upper()
        payload_buy_fiat = str(buy_order.get("fiat") or "").strip().upper()
        if signal_sell_platform and payload_sell_platform and signal_sell_platform != payload_sell_platform:
            return {
                "passed": False,
                "reason": f"signal/payload mismatch for sell platform: {signal_sell_platform} != {payload_sell_platform}",
                "checked_at": checked_at,
            }
        if signal_buy_platform and payload_buy_platform and signal_buy_platform != payload_buy_platform:
            return {
                "passed": False,
                "reason": f"signal/payload mismatch for buy platform: {signal_buy_platform} != {payload_buy_platform}",
                "checked_at": checked_at,
            }
        if signal_sell_fiat and payload_sell_fiat and signal_sell_fiat != payload_sell_fiat:
            return {
                "passed": False,
                "reason": f"signal/payload mismatch for sell fiat: {signal_sell_fiat} != {payload_sell_fiat}",
                "checked_at": checked_at,
            }
        if signal_buy_fiat and payload_buy_fiat and signal_buy_fiat != payload_buy_fiat:
            return {
                "passed": False,
                "reason": f"signal/payload mismatch for buy fiat: {signal_buy_fiat} != {payload_buy_fiat}",
                "checked_at": checked_at,
            }

        if self.settings.strict_single_merchant_mode:
            for label, order_payload in (("step 1", sell_order), ("step 3", buy_order)):
                components = order_payload.get("components")
                if isinstance(components, list) and len(components) >= 2:
                    return {
                        "passed": False,
                        "reason": f"{label} uses multi-merchant ladder while strict single-merchant mode is enabled",
                        "checked_at": checked_at,
                    }

        volume_usdt = self._as_decimal(signal.get("volume_usdt") or payload.get("volume_usdt"))
        if volume_usdt <= 0:
            return {
                "passed": False,
                "reason": f"invalid signal volume for revalidation: {volume_usdt}",
                "checked_at": checked_at,
            }

        sell_live, sell_leg = await self._revalidate_live_leg(sell_order, expected_side="sell")
        if sell_live is None:
            sell_leg["checked_at"] = checked_at
            return {
                "passed": False,
                "reason": f"step 1 revalidation failed: {sell_leg['reason']}",
                "checked_at": checked_at,
                "legs": {"sell": sell_leg},
            }
        buy_live, buy_leg = await self._revalidate_live_leg(buy_order, expected_side="buy")
        if buy_live is None:
            buy_leg["checked_at"] = checked_at
            return {
                "passed": False,
                "reason": f"step 3 revalidation failed: {buy_leg['reason']}",
                "checked_at": checked_at,
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }

        price_drift_limit = self.settings.exchange_execution.revalidation_max_price_drift_pct
        sell_price_drift = self._price_drift_pct(
            expected_price=self._as_decimal(sell_order.get("price")),
            current_price=sell_live.price,
        )
        buy_price_drift = self._price_drift_pct(
            expected_price=self._as_decimal(buy_order.get("price")),
            current_price=buy_live.price,
        )
        sell_leg["price_drift_pct"] = str(sell_price_drift)
        buy_leg["price_drift_pct"] = str(buy_price_drift)
        if sell_price_drift > price_drift_limit:
            return {
                "passed": False,
                "reason": (
                    f"step 1 price drift {sell_price_drift:.4f}% exceeds "
                    f"{price_drift_limit:.4f}%"
                ),
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }
        if buy_price_drift > price_drift_limit:
            return {
                "passed": False,
                "reason": (
                    f"step 3 price drift {buy_price_drift:.4f}% exceeds "
                    f"{price_drift_limit:.4f}%"
                ),
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }

        tolerance_usdt = Decimal("0.0001")
        tolerance_fiat = Decimal("0.01")
        fx_rate = self._as_decimal(payload.get("fx_rate_used") or "1")
        current_sell_notional = (volume_usdt * sell_live.price).quantize(Decimal("0.01"))
        current_rebuy_budget = (current_sell_notional * fx_rate).quantize(Decimal("0.01"))
        current_return_usdt = (
            (current_rebuy_budget / buy_live.price).quantize(Decimal("0.0001"))
            if buy_live.price > 0
            else Decimal("0")
        )
        sell_leg["required_volume_usdt"] = str(volume_usdt.quantize(Decimal("0.0001")))
        sell_leg["required_notional_fiat"] = str(current_sell_notional)
        buy_leg["required_notional_fiat"] = str(current_rebuy_budget)
        buy_leg["required_volume_usdt"] = str(current_return_usdt)

        if sell_live.available + tolerance_usdt < volume_usdt:
            return {
                "passed": False,
                "reason": f"step 1 available {sell_live.available} below required {volume_usdt}",
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }
        if current_sell_notional + tolerance_fiat < sell_live.min_amount:
            return {
                "passed": False,
                "reason": f"step 1 notional {current_sell_notional} below current min {sell_live.min_amount}",
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }
        if current_sell_notional > sell_live.max_amount + tolerance_fiat:
            return {
                "passed": False,
                "reason": f"step 1 notional {current_sell_notional} exceeds current max {sell_live.max_amount}",
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }
        if current_rebuy_budget + tolerance_fiat < buy_live.min_amount:
            return {
                "passed": False,
                "reason": f"step 3 budget {current_rebuy_budget} below current min {buy_live.min_amount}",
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }
        if current_rebuy_budget > buy_live.max_amount + tolerance_fiat:
            return {
                "passed": False,
                "reason": f"step 3 budget {current_rebuy_budget} exceeds current max {buy_live.max_amount}",
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }
        if buy_live.available + tolerance_usdt < current_return_usdt:
            return {
                "passed": False,
                "reason": f"step 3 available {buy_live.available} below required {current_return_usdt}",
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }
        if current_return_usdt + tolerance_usdt < volume_usdt:
            return {
                "passed": False,
                "reason": (
                    f"route no longer closes back to start volume: "
                    f"{current_return_usdt} < {volume_usdt}"
                ),
                "checked_at": checked_at,
                "max_price_drift_pct": str(price_drift_limit),
                "legs": {"sell": sell_leg, "buy": buy_leg},
            }

        return {
            "passed": True,
            "reason": "OK",
            "checked_at": checked_at,
            "max_price_drift_pct": str(price_drift_limit),
            "fx_rate_used": str(fx_rate),
            "required_start_volume_usdt": str(volume_usdt.quantize(Decimal("0.0001"))),
            "current_sell_notional": str(current_sell_notional),
            "current_rebuy_budget": str(current_rebuy_budget),
            "current_return_usdt": str(current_return_usdt),
            "legs": {"sell": sell_leg, "buy": buy_leg},
        }

    async def _revalidate_live_leg(
        self,
        order_payload: dict[str, Any],
        *,
        expected_side: str,
    ) -> tuple[Any | None, dict[str, Any]]:
        platform = str(order_payload.get("platform") or "").strip().lower()
        asset = str(order_payload.get("asset") or self.settings.base_asset).strip().upper()
        fiat = str(order_payload.get("fiat") or "").strip().upper()
        order_id = str(order_payload.get("order_id") or "").strip()
        merchant_id = str(order_payload.get("merchant_id") or "").strip()
        merchant_name = str(order_payload.get("merchant_name") or "").strip()

        leg_result: dict[str, Any] = {
            "platform": platform,
            "asset": asset,
            "fiat": fiat,
            "expected_side": expected_side,
            "expected_order_id": order_id,
            "expected_merchant_id": merchant_id,
            "expected_merchant_name": merchant_name,
        }

        if not platform or not fiat or not order_id:
            leg_result["reason"] = "incomplete order snapshot"
            return None, leg_result

        try:
            live_orders = await self._fetch_live_orders(
                platform=platform,
                asset=asset,
                fiat=fiat,
                side=expected_side,
            )
        except Exception as exc:
            leg_result["reason"] = f"live fetch failed: {type(exc).__name__}: {exc}"
            return None, leg_result

        leg_result["live_book_size"] = len(live_orders)
        matched = next((order for order in live_orders if order.order_id == order_id), None)
        if matched is None:
            if merchant_id:
                same_merchant = next((order for order in live_orders if order.merchant_id == merchant_id), None)
                if same_merchant is not None:
                    leg_result["reason"] = (
                        f"order {order_id} missing; merchant still present with ad {same_merchant.order_id}"
                    )
                    leg_result["live_order_id"] = same_merchant.order_id
                    leg_result["live_merchant_id"] = same_merchant.merchant_id
                    return None, leg_result
            leg_result["reason"] = f"order {order_id} not found in live {platform} {expected_side} book"
            return None, leg_result

        if str(matched.side).lower() != expected_side:
            leg_result["reason"] = f"live side mismatch: expected {expected_side}, got {matched.side}"
            return None, leg_result
        if merchant_id and matched.merchant_id and matched.merchant_id != merchant_id:
            leg_result["reason"] = (
                f"merchant mismatch for order {order_id}: expected {merchant_id}, got {matched.merchant_id}"
            )
            return None, leg_result
        if not merchant_id and merchant_name and matched.merchant_name and matched.merchant_name != merchant_name:
            leg_result["reason"] = (
                f"merchant name mismatch for order {order_id}: expected {merchant_name}, got {matched.merchant_name}"
            )
            return None, leg_result

        leg_result.update(
            {
                "reason": "OK",
                "live_order_id": matched.order_id,
                "live_merchant_id": matched.merchant_id,
                "live_merchant_name": matched.merchant_name,
                "live_price": str(matched.price),
                "live_available": str(matched.available),
                "live_min_amount": str(matched.min_amount),
                "live_max_amount": str(matched.max_amount),
            }
        )
        return matched, leg_result

    async def _fetch_live_orders(
        self,
        *,
        platform: str,
        asset: str,
        fiat: str,
        side: str,
    ) -> list[Any]:
        if platform == "binance":
            return await self.binance_client.fetch_orders(asset, fiat, side)
        if platform == "bybit":
            return await self.bybit_client.get_online_ads(asset, fiat, side)
        raise RuntimeError(f"Unsupported execution revalidation platform: {platform}")

    @staticmethod
    def _payload_order(payload: dict[str, Any], block: str) -> dict[str, Any]:
        node = payload.get(block) or {}
        return node if isinstance(node, dict) else {}

    @staticmethod
    def _price_drift_pct(*, expected_price: Decimal, current_price: Decimal) -> Decimal:
        if expected_price <= 0 or current_price <= 0:
            return Decimal("0")
        return ((abs(current_price - expected_price) / expected_price) * Decimal("100")).quantize(
            Decimal("0.0001")
        )

    async def _rm_snapshot_for_signal(self, signal: dict[str, Any]) -> dict[str, Any]:
        payload = self._signal_payload(signal)
        balances = await self._inventory_balances()

        sell_platform = str(signal.get("sell_platform") or "").lower()
        buy_platform = str(signal.get("buy_platform") or "").lower()
        treasury_provider = self._treasury_provider_for_signal(signal)
        volume_usdt = self._as_decimal(signal.get("volume_usdt"))
        sell_fiat = str(signal.get("sell_fiat") or "")
        buy_fiat = str(signal.get("buy_fiat") or "")
        sell_price = self._decimal_from_payload(payload, "sell_order", "price")
        buy_price = self._decimal_from_payload(payload, "buy_order", "price")
        fx_rate = self._as_decimal(payload.get("fx_rate_used") or "1")

        allowed_platforms = set(self.settings.exchange_execution.allowed_platforms)
        allowed_treasury = set(self.settings.exchange_execution.allowed_treasury_providers)
        eligible_for_full_auto = (
            sell_platform in allowed_platforms
            and buy_platform in allowed_platforms
            and treasury_provider in allowed_treasury
        )
        scope_reason = ""
        if not eligible_for_full_auto:
            scope_reason = (
                f"Supported only for platforms {sorted(allowed_platforms)} "
                f"and treasury {sorted(allowed_treasury)}; got "
                f"{sell_platform}->{buy_platform} via {treasury_provider}"
            )

        source_exchange_balance = balances.get((f"{sell_platform}:wallet", self.settings.base_asset.upper()), Decimal("0"))
        source_exchange_cap = self._cap_by_pct(source_exchange_balance)

        total_exchange_usdt = sum(
            balances.get((f"{platform}:wallet", self.settings.base_asset.upper()), Decimal("0"))
            for platform in allowed_platforms
        )
        global_exchange_cap = self._cap_by_pct(total_exchange_usdt)

        treasury_capacity_usdt = self._treasury_capacity_usdt(
            balances=balances,
            treasury_provider=treasury_provider,
            sell_fiat=sell_fiat,
            buy_fiat=buy_fiat,
            sell_price=sell_price,
            buy_price=buy_price,
            fx_rate=fx_rate,
        )
        treasury_cap = self._cap_by_pct(treasury_capacity_usdt)

        candidates = {
            "signal_volume": volume_usdt,
            "source_exchange_30pct": source_exchange_cap,
            "global_exchange_30pct": global_exchange_cap,
        }
        if treasury_provider in {"revolut", "wise", "bank"}:
            candidates[f"{treasury_provider}_30pct"] = treasury_cap

        limiting_factor, recommended_volume = min(
            candidates.items(),
            key=lambda item: item[1],
        )
        recommended_volume = recommended_volume.quantize(Decimal("0.0001"))
        limit_reason = (
            "Insufficient working capital under RM cap"
            if recommended_volume <= 0
            else f"Limited by {limiting_factor}"
        )

        rebalance = await self.rebalance_snapshot()
        return {
            "eligible_for_full_auto": eligible_for_full_auto,
            "scope_reason": scope_reason,
            "signal_volume_usdt": str(volume_usdt.quantize(Decimal("0.0001"))),
            "recommended_volume_usdt": str(max(recommended_volume, Decimal("0")).quantize(Decimal("0.0001"))),
            "limiting_factor": limiting_factor,
            "limit_reason": limit_reason,
            "max_capital_pct": str(self.settings.exchange_execution.max_capital_per_signal_pct),
            "source_exchange_balance_usdt": str(source_exchange_balance.quantize(Decimal("0.0001"))),
            "source_exchange_cap_usdt": str(source_exchange_cap.quantize(Decimal("0.0001"))),
            "global_exchange_balance_usdt": str(total_exchange_usdt.quantize(Decimal("0.0001"))),
            "global_exchange_cap_usdt": str(global_exchange_cap.quantize(Decimal("0.0001"))),
            "treasury_provider": treasury_provider,
            "treasury_capacity_usdt": str(treasury_capacity_usdt.quantize(Decimal("0.0001"))),
            "treasury_cap_usdt": str(treasury_cap.quantize(Decimal("0.0001"))),
            "rebalance": rebalance,
        }

    @staticmethod
    def _session_rm_snapshot(session: dict[str, Any] | None) -> dict[str, Any]:
        if session is None:
            return {}
        payload_raw = str(session.get("payload_json") or "{}")
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            return {}
        rm = payload.get("rm")
        return rm if isinstance(rm, dict) else {}

    async def _inventory_balances(self) -> dict[tuple[str, str], Decimal]:
        rows = await self.inventory_repository.list_positions()
        return {
            (str(item["location"]), str(item["asset"]).upper()): Decimal(str(item["amount"]))
            for item in rows
        }

    def _treasury_capacity_usdt(
        self,
        *,
        balances: dict[tuple[str, str], Decimal],
        treasury_provider: str,
        sell_fiat: str,
        buy_fiat: str,
        sell_price: Decimal,
        buy_price: Decimal,
        fx_rate: Decimal,
    ) -> Decimal:
        if treasury_provider not in {"revolut", "wise", "bank"} or buy_price <= 0:
            return Decimal("0")

        provider_bucket = treasury_provider
        total = Decimal("0")
        direct_buy_fiat = balances.get((provider_bucket, buy_fiat.upper()), Decimal("0"))
        total += direct_buy_fiat / buy_price if buy_price > 0 else Decimal("0")

        source_fiat_balance = balances.get((provider_bucket, sell_fiat.upper()), Decimal("0"))
        if fx_rate > 0 and buy_price > 0:
            total += (source_fiat_balance * fx_rate) / buy_price

        return total.quantize(Decimal("0.0001"))

    def _cap_by_pct(self, amount: Decimal) -> Decimal:
        pct = self.settings.exchange_execution.max_capital_per_signal_pct / Decimal("100")
        return (amount * pct).quantize(Decimal("0.0001"))

    @staticmethod
    def _signal_payload(signal: dict[str, Any]) -> dict[str, Any]:
        payload_raw = str(signal.get("payload_json") or "{}")
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {}
        return payload if isinstance(payload, dict) else {}

    def _decimal_from_payload(self, payload: dict[str, Any], block: str, field: str) -> Decimal:
        node = payload.get(block) or {}
        if not isinstance(node, dict):
            return Decimal("0")
        return self._as_decimal(node.get(field))
