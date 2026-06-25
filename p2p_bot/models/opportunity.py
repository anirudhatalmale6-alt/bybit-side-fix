from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from p2p_bot.models.order import P2POrder


@dataclass
class ArbitrageOpportunity:
    type: str
    spread_pct: Decimal
    estimated_profit_usd: Decimal
    gross_profit_usd: Decimal
    total_fees_usd: Decimal
    buy_fee_usd: Decimal
    sell_fee_usd: Decimal
    fx_fee_usd: Decimal
    volume_usdt: Decimal
    buy_order: P2POrder
    sell_order: P2POrder
    detected_at: datetime
    expires_at: datetime
    base_type: str = ""
    status: str = "pending"
    note: str = ""
    rail_status: str = "unknown"
    buy_rail: str = ""
    sell_rail: str = ""
    fx_rail: str = ""
    buy_user_method: str = ""
    sell_user_method: str = ""
    buy_payment_summary: str = ""
    sell_payment_summary: str = ""
    liquidity_score: Decimal = Decimal("0")
    liquidity_status: str = "unknown"
    liquidity_summary: str = ""
    sell_fiat_amount: Decimal = Decimal("0")
    rebuy_fiat_amount: Decimal = Decimal("0")
    gross_return_usdt: Decimal = Decimal("0")
    fx_rate_used: Decimal = Decimal("1")
    settlement_ready: bool | None = None
    settlement_requirements: tuple[tuple[str, str, Decimal], ...] = ()
    transfer_source_location: str = ""
    transfer_target_location: str = ""
    post_trade_rebalance_source: str = ""
    post_trade_rebalance_target: str = ""
    manual_execution_hint: str = ""
    internal_quote_platform: str = ""
    internal_quote_venue: str = ""
    internal_quote_exact: bool = False
    internal_quote_rate: Decimal = Decimal("0")
    internal_quote_fiat_amount: Decimal = Decimal("0")
    internal_quote_expected_usdt: Decimal = Decimal("0")
    internal_quote_advantage_usdt: Decimal = Decimal("0")

    @property
    def key(self) -> str:
        return "|".join(
            [
                self.type,
                self.base_type or self.type,
                self.buy_order.platform,
                self.buy_order.order_id,
                self.sell_order.platform,
                self.sell_order.order_id,
                self.buy_order.fiat,
                self.sell_order.fiat,
            ]
        )

    @property
    def alert_fingerprint(self) -> str:
        route = self.note.split(" @ FX ")[0].strip()
        return "|".join(
            [
                self.type,
                self.base_type or self.type,
                route,
                self.buy_order.platform,
                self.sell_order.platform,
                self.buy_order.fiat,
                self.sell_order.fiat,
                f"{self.buy_order.price:.4f}",
                f"{self.sell_order.price:.4f}",
                f"{self.volume_usdt:.2f}",
                self.buy_rail,
                self.sell_rail,
                self.fx_rail,
            ]
        )

    @property
    def alert_route_key(self) -> str:
        route = self.note.split(" @ FX ")[0].strip()
        return "|".join(
            [
                self.type,
                self.base_type or self.type,
                route,
                self.buy_order.platform,
                self.sell_order.platform,
                self.buy_order.fiat,
                self.sell_order.fiat,
                self.buy_rail,
                self.sell_rail,
                self.fx_rail,
            ]
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "base_type": self.base_type or self.type,
            "spread_pct": str(self.spread_pct),
            "estimated_profit_usd": str(self.estimated_profit_usd),
            "gross_profit_usd": str(self.gross_profit_usd),
            "total_fees_usd": str(self.total_fees_usd),
            "buy_fee_usd": str(self.buy_fee_usd),
            "sell_fee_usd": str(self.sell_fee_usd),
            "fx_fee_usd": str(self.fx_fee_usd),
            "volume_usdt": str(self.volume_usdt),
            "note": self.note,
            "rail_status": self.rail_status,
            "buy_rail": self.buy_rail,
            "sell_rail": self.sell_rail,
            "fx_rail": self.fx_rail,
            "buy_user_method": self.buy_user_method,
            "sell_user_method": self.sell_user_method,
            "buy_payment_summary": self.buy_payment_summary,
            "sell_payment_summary": self.sell_payment_summary,
            "liquidity_status": self.liquidity_status,
            "sell_fiat_amount": str(self.sell_fiat_amount),
            "rebuy_fiat_amount": str(self.rebuy_fiat_amount),
            "gross_return_usdt": str(self.gross_return_usdt),
            "fx_rate_used": str(self.fx_rate_used),
            "settlement_ready": self.settlement_ready,
            "settlement_requirements": list(self.settlement_requirements),
            "transfer_source_location": self.transfer_source_location,
            "transfer_target_location": self.transfer_target_location,
            "post_trade_rebalance_source": self.post_trade_rebalance_source,
            "post_trade_rebalance_target": self.post_trade_rebalance_target,
            "manual_execution_hint": self.manual_execution_hint,
            "buy_order": {
                "platform": self.buy_order.platform,
                "order_id": self.buy_order.order_id,
                "side": self.buy_order.side,
                "raw_side": str(self.buy_order.raw.get("side") or ""),
                "raw_book_rank": self.buy_order.raw.get("_raw_book_rank"),
                "asset": self.buy_order.asset,
                "fiat": self.buy_order.fiat,
                "price": str(self.buy_order.price),
                "min_amount": str(self.buy_order.min_amount),
                "max_amount": str(self.buy_order.max_amount),
                "available": str(self.buy_order.available),
                "observed_at": self.buy_order.timestamp.isoformat(),
                "payment_methods": list(self.buy_order.payment_methods),
                "merchant_id": self.buy_order.resolved_merchant_id(),
                "merchant_name": self.buy_order.resolved_merchant_name(),
                "merchant_rating": self.buy_order.merchant_rating,
                "merchant_orders": self.buy_order.merchant_orders,
                "merchant_days": self.buy_order.merchant_days,
                "merchant_online": self.buy_order.merchant_online,
                "merchant_kyc": self.buy_order.merchant_kyc,
                "merchant_last_active_minutes": self.buy_order.merchant_last_active_minutes,
                "components": self.buy_order.raw.get("components", []),
            },
            "sell_order": {
                "platform": self.sell_order.platform,
                "order_id": self.sell_order.order_id,
                "side": self.sell_order.side,
                "raw_side": str(self.sell_order.raw.get("side") or ""),
                "raw_book_rank": self.sell_order.raw.get("_raw_book_rank"),
                "asset": self.sell_order.asset,
                "fiat": self.sell_order.fiat,
                "price": str(self.sell_order.price),
                "min_amount": str(self.sell_order.min_amount),
                "max_amount": str(self.sell_order.max_amount),
                "available": str(self.sell_order.available),
                "observed_at": self.sell_order.timestamp.isoformat(),
                "payment_methods": list(self.sell_order.payment_methods),
                "merchant_id": self.sell_order.resolved_merchant_id(),
                "merchant_name": self.sell_order.resolved_merchant_name(),
                "merchant_rating": self.sell_order.merchant_rating,
                "merchant_orders": self.sell_order.merchant_orders,
                "merchant_days": self.sell_order.merchant_days,
                "merchant_online": self.sell_order.merchant_online,
                "merchant_kyc": self.sell_order.merchant_kyc,
                "merchant_last_active_minutes": self.sell_order.merchant_last_active_minutes,
                "components": self.sell_order.raw.get("components", []),
            },
        }
