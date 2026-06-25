from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from p2p_bot.models.opportunity import ArbitrageOpportunity
from p2p_bot.models.order import P2POrder


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _opportunity_sort_key(item: ArbitrageOpportunity) -> tuple[int, int, Decimal, Decimal]:
    rank = {
        "confirmed": 0,
        "mixed": 1,
        "unknown": 2,
    }.get(item.rail_status, 3)
    liquidity_rank = {
        "liquid": 0,
        "tradable": 1,
        "illiquid": 2,
    }.get(item.liquidity_status, 3)
    return (rank, liquidity_rank, -item.spread_pct, -item.estimated_profit_usd)


@dataclass
class AppState:
    latest_orders: dict[str, list[P2POrder]] = field(default_factory=dict)
    latest_opportunities: list[ArbitrageOpportunity] = field(default_factory=list)
    market_overview: list[dict[str, object]] = field(default_factory=list)
    latest_spreads: dict[str, Decimal] = field(default_factory=dict)
    last_scan_at: datetime | None = None
    market_last_run_at: datetime | None = None
    last_forex_rate_eur_pln: Decimal | None = None
    paused: bool = False
    kill_switch_reason: str | None = None

    def record_orders(self, orders: list[P2POrder]) -> None:
        grouped: dict[str, list[P2POrder]] = {}
        for order in orders:
            key = f"{order.platform}:{order.asset}:{order.fiat}:{order.side}"
            grouped.setdefault(key, []).append(order)
        self.latest_orders.update(grouped)
        self.last_scan_at = utc_now()

    def record_opportunities(self, opportunities: list[ArbitrageOpportunity]) -> None:
        self.latest_opportunities = sorted(opportunities, key=_opportunity_sort_key)[:10]

    def record_market_overview(self, overview: list[dict[str, object]]) -> None:
        self.market_overview = overview
        self.market_last_run_at = utc_now()

    def set_spread(self, key: str, value: Decimal) -> None:
        self.latest_spreads[key] = value

    def set_paused(self, paused: bool, reason: str | None = None) -> None:
        self.paused = paused
        if paused:
            self.kill_switch_reason = reason
        elif reason is None:
            self.kill_switch_reason = None
