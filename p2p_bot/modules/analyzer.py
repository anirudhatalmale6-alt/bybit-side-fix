from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from asyncio import QueueEmpty
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import permutations
from zoneinfo import ZoneInfo

from p2p_bot.config import Settings
from dataclasses import replace

from p2p_bot.db.repositories import InventoryRepository, OpportunityRepository, RawOrderRepository, SignalJournalRepository
from p2p_bot.models.events import Event
from p2p_bot.models.opportunity import ArbitrageOpportunity
from p2p_bot.models.order import P2POrder
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.state import AppState
from p2p_bot.utils.canonical_side import fx_chain_candidates, order_action_url, prefer_high_price
from p2p_bot.utils.forex import ForexClient
from p2p_bot.utils.internal_quotes import InternalQuoteClient


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def opportunity_sort_key(item: ArbitrageOpportunity) -> tuple[int, int, Decimal, Decimal]:
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
    # Prefer the best price combination inside the already-correct side books.
    # Estimated profit remains the tiebreaker after route safety/liquidity and spread.
    return (rank, liquidity_rank, -item.spread_pct, -item.estimated_profit_usd)


_UNKNOWN_LAST_ACTIVE_MINUTES = 10**9


class Analyzer:
    def __init__(
        self,
        settings: Settings,
        state: AppState,
        risk_guard: RiskGuard,
        raw_order_repository: RawOrderRepository,
        opportunity_repository: OpportunityRepository,
        signal_journal_repository: SignalJournalRepository | None,
        inventory_repository: InventoryRepository | None,
        forex_client: ForexClient,
        internal_quote_client: InternalQuoteClient,
        scan_queue: asyncio.Queue[Event],
        alert_queue: asyncio.Queue[Event],
        logger: logging.Logger,
    ) -> None:
        self.settings = settings
        self.state = state
        self.risk_guard = risk_guard
        self.raw_order_repository = raw_order_repository
        self.opportunity_repository = opportunity_repository
        self.signal_journal_repository = signal_journal_repository
        self.inventory_repository = inventory_repository
        self.forex_client = forex_client
        self.internal_quote_client = internal_quote_client
        self.scan_queue = scan_queue
        self.alert_queue = alert_queue
        self.logger = logger
        self._recent_alerts: dict[str, tuple[datetime, Decimal]] = {}
        self._liquidity_history_cache: dict[int, dict[tuple[str, str, str, str], dict[str, float]]] | None = None
        self._liquidity_history_cache_expires_at: float = 0.0
        self._next_internal_graph_run_at: float = 0.0

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                event = await asyncio.wait_for(self.scan_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if event.type != "orders_scanned":
                continue
            while True:
                try:
                    newer_event = self.scan_queue.get_nowait()
                except QueueEmpty:
                    break
                if newer_event.type == "orders_scanned" and isinstance(newer_event.payload, list):
                    event = newer_event
            orders = event.payload
            if not isinstance(orders, list):
                continue
            started = asyncio.get_running_loop().time()
            opportunities = await self.analyze_orders(orders)
            finished = asyncio.get_running_loop().time()
            self.state.record_opportunities(opportunities)
            await self.opportunity_repository.save_many(opportunities)
            self.logger.info(
                "Analyzer built %s opportunities from %s orders in %.2fs",
                len(opportunities),
                len(orders),
                finished - started,
            )
            alerts_sent = 0
            route_best_candidates = self._best_per_route(opportunities)
            alert_candidates = [
                opportunity
                for opportunity in route_best_candidates
                if self._is_alert_candidate_prequote(opportunity)
            ]
            await self._annotate_internal_quotes(alert_candidates)
            alert_candidates = [opportunity for opportunity in alert_candidates if self._is_alert_candidate(opportunity)]
            alert_candidates = self._best_per_first_leg(alert_candidates)
            if route_best_candidates and not alert_candidates:
                self._log_non_alert_reasons(route_best_candidates)
            await self._reconcile_open_routes(alert_candidates)
            for opportunity in alert_candidates:
                if alerts_sent >= self.settings.max_alerts_per_scan:
                    break
                if await self._should_alert(opportunity):
                    await self.alert_queue.put(Event("opportunity", opportunity))
                    alerts_sent += 1

    async def analyze_orders(self, orders: list[P2POrder]) -> list[ArbitrageOpportunity]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        stage_started = started
        grouped: dict[tuple[str, str, str], list[P2POrder]] = defaultdict(list)
        for order in orders:
            grouped[(order.platform, order.fiat, order.side)].append(order)
        liquidity_context = await self._build_liquidity_context(grouped)
        liquidity_elapsed = loop.time() - stage_started

        opportunities: list[ArbitrageOpportunity] = []
        stage_started = loop.time()
        opportunities.extend(self._find_internal(grouped, liquidity_context))
        internal_elapsed = loop.time() - stage_started

        stage_started = loop.time()
        opportunities.extend(self._find_cross_platform(orders, liquidity_context))
        cross_platform_elapsed = loop.time() - stage_started

        stage_started = loop.time()
        if self._internal_graph_due(stage_started):
            opportunities.extend(await self._find_internal_graph_opportunities(orders, liquidity_context))
            interval_sec = max(0, int(self.settings.internal_graph_interval_sec))
            if interval_sec > 0:
                self._next_internal_graph_run_at = loop.time() + interval_sec
        internal_graph_elapsed = loop.time() - stage_started

        stage_started = loop.time()
        fx_matrix = await self.forex_client.get_rate_matrix(self.settings.fiats)
        self.state.last_forex_rate_eur_pln = fx_matrix.get(("EUR", "PLN"))
        forex_elapsed = loop.time() - stage_started

        stage_started = loop.time()
        if self.settings.enable_cross_currency:
            opportunities.extend(self._find_cross_currency(orders, fx_matrix, liquidity_context))
        cross_currency_elapsed = loop.time() - stage_started

        stage_started = loop.time()
        opportunities = self._filter_sane_opportunities(opportunities, fx_matrix)
        sane_elapsed = loop.time() - stage_started

        stage_started = loop.time()
        opportunities = await self._apply_inventory_routing(opportunities)
        inventory_elapsed = loop.time() - stage_started

        stage_started = loop.time()
        opportunities = await self._annotate_settlement_requirements(opportunities)
        settlement_elapsed = loop.time() - stage_started

        total_elapsed = loop.time() - started
        if total_elapsed >= 10:
            self.logger.info(
                "Analyzer stages: liquidity=%.2fs internal=%.2fs cross_platform=%.2fs "
                "internal_graph=%.2fs forex=%.2fs cross_currency=%.2fs sane=%.2fs "
                "inventory=%.2fs settlement=%.2fs total=%.2fs",
                liquidity_elapsed,
                internal_elapsed,
                cross_platform_elapsed,
                internal_graph_elapsed,
                forex_elapsed,
                cross_currency_elapsed,
                sane_elapsed,
                inventory_elapsed,
                settlement_elapsed,
                total_elapsed,
            )
        return sorted(opportunities, key=opportunity_sort_key)

    def _internal_graph_due(self, now_monotonic: float) -> bool:
        if not self.settings.enable_internal_graph:
            return False
        interval_sec = max(0, int(self.settings.internal_graph_interval_sec))
        if interval_sec <= 0:
            return True
        return now_monotonic >= self._next_internal_graph_run_at

    def _log_non_alert_reasons(self, opportunities: list[ArbitrageOpportunity]) -> None:
        reason_counts: dict[str, int] = defaultdict(int)
        samples: dict[str, str] = {}
        for opportunity in opportunities:
            reasons = self._non_alert_reasons(opportunity)
            for reason in reasons:
                reason_counts[reason] += 1
                samples.setdefault(reason, opportunity.note)
        if not reason_counts:
            return
        summary = ", ".join(
            f"{reason}={count} ({samples.get(reason, '')})"
            for reason, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:8]
        )
        self.logger.info("No alert candidates after filtering: %s", summary)

    def _non_alert_reasons(self, opportunity: ArbitrageOpportunity) -> list[str]:
        reasons: list[str] = []
        min_spread, min_profit = self.settings.alert_thresholds_for(
            opportunity.buy_order.fiat,
            opportunity.buy_order.asset,
        )
        min_profit = self._min_profit_for_route(opportunity, min_profit)
        if not self._is_settlement_path_allowed(opportunity):
            reasons.append("settlement_path")
        if not self._is_route_chain_coherent(opportunity):
            reasons.append("chain:broken")
        if self._is_outperformed_by_internal_route(opportunity):
            reasons.append("internal_better")
        if opportunity.rail_status != "confirmed":
            reasons.append(f"rail:{opportunity.rail_status}")
        if not self._liquidity_allows_alert(opportunity):
            reasons.append(f"liquidity:{opportunity.liquidity_status}")
        if opportunity.spread_pct < min_spread:
            reasons.append(f"spread<{min_spread}")
        if opportunity.estimated_profit_usd < min_profit:
            reasons.append(f"profit<{min_profit}")
        if opportunity.volume_usdt < self.settings.min_alert_volume_usdt:
            reasons.append(f"volume<{self.settings.min_alert_volume_usdt}")
        return reasons

    async def _find_internal_graph_opportunities(
        self,
        orders: list[P2POrder],
        liquidity_context: dict[str, object],
    ) -> list[ArbitrageOpportunity]:
        opportunities: list[ArbitrageOpportunity] = []
        active_fiats = sorted({order.fiat.upper() for order in orders if order.fiat.upper() in self.settings.fiats})
        if len(active_fiats) < 2:
            return opportunities

        start_volume = min(
            self.settings.default_pair_order_config.max_single_order_usd,
            self.settings.daily_limits.max_open_usd,
        ).quantize(Decimal("0.0001"))
        if self.settings.prefunded_mode:
            start_volume = min(start_volume, self.settings.prefunded_exchange_usdt).quantize(Decimal("0.0001"))
        if start_volume <= 0:
            return opportunities

        for platform in self.settings.enabled_platforms:
            quote_map = await self._internal_graph_quote_map(
                platform=platform,
                active_fiats=active_fiats,
                start_volume=start_volume,
            )
            quotable_fiats = [
                fiat
                for fiat in active_fiats
                if self._internal_graph_quote_key(self.settings.base_asset, fiat, start_volume) in quote_map
                and self._internal_graph_quote_key(fiat, self.settings.base_asset, Decimal("1000")) in quote_map
            ]
            if len(quotable_fiats) < 2:
                continue
            for hop_count in range(2, min(4, len(quotable_fiats)) + 1):
                for chain in permutations(quotable_fiats, hop_count):
                    amount = start_volume
                    current = self.settings.base_asset
                    quotes = []
                    valid = True
                    for next_asset in (*chain, self.settings.base_asset):
                        probe_amount = start_volume if current == self.settings.base_asset else Decimal("1000")
                        quote = quote_map.get(
                            self._internal_graph_quote_key(current, next_asset, probe_amount)
                        )
                        if quote is None or quote.rate <= 0:
                            valid = False
                            break
                        quotes.append(quote)
                        amount = amount * quote.rate
                        current = next_asset
                    if not valid or amount <= start_volume:
                        continue

                    spread = ((amount - start_volume) / start_volume) * Decimal("100")
                    if spread < self.settings.min_internal_spread_pct:
                        continue

                    first_quote = quotes[0]
                    last_quote = quotes[-1]
                    first_fiat = chain[0]
                    last_fiat = chain[-1]
                    synthetic_sell = self._internal_graph_order(
                        platform=platform,
                        side="sell",
                        fiat=first_fiat,
                        price=first_quote.rate,
                        amount_usdt=start_volume,
                        quote_chain=quotes,
                        chain=chain,
                    )
                    last_price = (Decimal("1") / last_quote.rate) if last_quote.rate > 0 else Decimal("0")
                    synthetic_buy = self._internal_graph_order(
                        platform=platform,
                        side="buy",
                        fiat=last_fiat,
                        price=last_price,
                        amount_usdt=amount,
                        quote_chain=quotes,
                        chain=chain,
                    )
                    opportunity = self._build_opportunity(
                        opportunity_type="internal_graph",
                        spread_pct=spread,
                        volume_usdt=start_volume,
                        buy_order=synthetic_buy,
                        sell_order=synthetic_sell,
                        note=f"{platform.upper()} EXCHANGE {'->'.join((self.settings.base_asset, *chain, self.settings.base_asset))}",
                        liquidity_context=liquidity_context,
                    )
                    opportunities.append(opportunity)
        return opportunities

    @staticmethod
    def _internal_graph_quote_key(
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> tuple[str, str, str]:
        return (
            from_asset.upper(),
            to_asset.upper(),
            f"{from_amount.quantize(Decimal('0.01'))}",
        )

    async def _internal_graph_quote_map(
        self,
        *,
        platform: str,
        active_fiats: list[str],
        start_volume: Decimal,
    ) -> dict[tuple[str, str, str], object]:
        requests: list[tuple[str, str, Decimal]] = []
        for fiat in active_fiats:
            requests.append((self.settings.base_asset, fiat, start_volume))
            requests.append((fiat, self.settings.base_asset, Decimal("1000")))
            for other in active_fiats:
                if other == fiat:
                    continue
                requests.append((fiat, other, Decimal("1000")))

        unique_requests = list(dict.fromkeys(requests))
        semaphore = asyncio.Semaphore(max(1, self.settings.internal_graph_quote_concurrency))

        async def fetch_quote(request: tuple[str, str, Decimal]) -> tuple[tuple[str, str, str], object | None]:
            from_asset, to_asset, from_amount = request
            async with semaphore:
                quote = await self.internal_quote_client.platform_quote(
                    platform=platform,
                    from_asset=from_asset,
                    to_asset=to_asset,
                    from_amount=from_amount,
                )
            return self._internal_graph_quote_key(from_asset, to_asset, from_amount), quote

        results = await asyncio.gather(*(fetch_quote(request) for request in unique_requests))
        return {
            key: quote
            for key, quote in results
            if quote is not None and getattr(quote, "rate", Decimal("0")) > 0
        }

    def _internal_graph_order(
        self,
        *,
        platform: str,
        side: str,
        fiat: str,
        price: Decimal,
        amount_usdt: Decimal,
        quote_chain: list,
        chain: tuple[str, ...],
    ) -> P2POrder:
        return P2POrder(
            platform=platform,
            order_id=f"internal-graph:{platform}:{side}:{'->'.join(chain)}",
            side=side,
            asset=self.settings.base_asset,
            fiat=fiat,
            price=price.quantize(Decimal("0.0000001")),
            min_amount=Decimal("0"),
            max_amount=amount_usdt.quantize(Decimal("0.01")),
            available=amount_usdt.quantize(Decimal("0.0001")),
            payment_methods=["Balance"],
            merchant_id=f"internal-graph:{platform}",
            merchant_rating=100.0,
            merchant_orders=999999,
            merchant_days=9999,
            merchant_online=True,
            merchant_kyc=True,
            merchant_last_active_minutes=0,
            raw={
                "internal_graph": True,
                "chain": [self.settings.base_asset, *chain, self.settings.base_asset],
                "quotes": [
                    {
                        "platform": item.platform,
                        "venue": item.venue,
                        "from_asset": item.from_asset,
                        "to_asset": item.to_asset,
                        "rate": str(item.rate),
                        "exact": item.exact,
                    }
                    for item in quote_chain
                ],
            },
        )

    async def _apply_inventory_routing(
        self,
        opportunities: list[ArbitrageOpportunity],
    ) -> list[ArbitrageOpportunity]:
        if self.settings.prefunded_mode:
            return opportunities
        if self.inventory_repository is None:
            return opportunities
        positions = await self.inventory_repository.list_positions()
        if not positions:
            return opportunities

        balances: dict[tuple[str, str], Decimal] = {}
        for item in positions:
            balances[(str(item["location"]), str(item["asset"]).upper())] = Decimal(str(item["amount"]))

        adjusted: list[ArbitrageOpportunity] = []
        base_asset = self.settings.base_asset.upper()
        for opportunity in opportunities:
            start_location = f"{opportunity.sell_order.platform}:wallet"
            start_balance = balances.get((start_location, base_asset), Decimal("0"))
            if start_balance >= opportunity.volume_usdt:
                adjusted.append(opportunity)
                continue

            deficit = opportunity.volume_usdt - start_balance
            transfer_source = None
            for (location, asset), amount in balances.items():
                if asset != base_asset or location == start_location:
                    continue
                if amount >= deficit:
                    transfer_source = location
                    break
            if transfer_source is None:
                adjusted.append(opportunity)
                continue

            fee_usdt = self.settings.inter_exchange_transfer_fee_usdt
            risk_buffer_usdt = opportunity.volume_usdt * self.settings.inter_exchange_transfer_risk_pct / Decimal("100")
            extra_cost = fee_usdt + risk_buffer_usdt
            adjusted.append(
                replace(
                    opportunity,
                    type="transfer_route",
                    base_type=opportunity.base_type or opportunity.type,
                    estimated_profit_usd=(opportunity.estimated_profit_usd - extra_cost).quantize(Decimal("0.01")),
                    total_fees_usd=(opportunity.total_fees_usd + extra_cost).quantize(Decimal("0.01")),
                    note=(
                        f"{opportunity.note} | transfer {transfer_source}->{start_location} "
                        f"fee {fee_usdt} {base_asset}"
                    ),
                    transfer_source_location=transfer_source,
                    transfer_target_location=start_location,
                )
            )
        return adjusted

    async def _annotate_settlement_requirements(
        self,
        opportunities: list[ArbitrageOpportunity],
    ) -> list[ArbitrageOpportunity]:
        if self.settings.prefunded_mode:
            return [
                replace(
                    opportunity,
                    settlement_ready=True,
                    settlement_requirements=(),
                )
                for opportunity in opportunities
            ]
        balances = await self._inventory_balances()
        annotated: list[ArbitrageOpportunity] = []
        for opportunity in opportunities:
            requirements = self._settlement_requirements(opportunity)
            ready = self._requirements_satisfied(requirements, balances) if balances else None
            annotated.append(
                replace(
                    opportunity,
                    settlement_ready=ready,
                    settlement_requirements=tuple(requirements),
                )
            )
        return annotated

    async def _inventory_balances(self) -> dict[tuple[str, str], Decimal]:
        if self.inventory_repository is None:
            return {}
        positions = await self.inventory_repository.list_positions()
        balances: dict[tuple[str, str], Decimal] = {}
        for item in positions:
            balances[(str(item["location"]), str(item["asset"]).upper())] = Decimal(str(item["amount"]))
        return balances

    def _settlement_requirements(
        self,
        opportunity: ArbitrageOpportunity,
    ) -> list[tuple[str, str, Decimal]]:
        if self.settings.prefunded_mode:
            return []
        if opportunity.type == "internal_graph":
            return []

        requirements: list[tuple[str, str, Decimal]] = []

        start_bucket = opportunity.transfer_target_location or f"{opportunity.sell_order.platform}:wallet"
        start_source_bucket = opportunity.transfer_source_location or start_bucket
        requirements.append((start_source_bucket, self.settings.base_asset.upper(), opportunity.volume_usdt))

        sell_bucket = self._rail_bucket(
            rail=opportunity.sell_rail,
            platform=opportunity.sell_order.platform,
            fiat=opportunity.sell_order.fiat,
        )
        buy_bucket = self._rail_bucket(
            rail=opportunity.buy_rail,
            platform=opportunity.buy_order.platform,
            fiat=opportunity.buy_order.fiat,
        )

        if opportunity.type == "cross_currency" and opportunity.buy_order.fiat != opportunity.sell_order.fiat:
            fx_bucket = self._rail_bucket(
                rail=opportunity.fx_rail,
                platform=opportunity.sell_order.platform,
                fiat=opportunity.sell_order.fiat,
            )
            if not fx_bucket:
                return []
            if sell_bucket != fx_bucket:
                requirements.append((fx_bucket, opportunity.sell_order.fiat.upper(), opportunity.sell_fiat_amount))
            if fx_bucket != buy_bucket:
                requirements.append((buy_bucket, opportunity.buy_order.fiat.upper(), opportunity.rebuy_fiat_amount))
            return self._dedupe_requirements(requirements)

        if sell_bucket != buy_bucket:
            requirements.append((buy_bucket, opportunity.buy_order.fiat.upper(), opportunity.rebuy_fiat_amount))
        return self._dedupe_requirements(requirements)

    @staticmethod
    def _bucket_has_balance(
        balances: dict[tuple[str, str], Decimal],
        *,
        bucket: str,
        asset: str,
        amount: Decimal,
    ) -> bool:
        if not bucket or amount <= 0:
            return False
        return balances.get((bucket, asset.upper()), Decimal("0")) >= amount

    def _requirements_satisfied(
        self,
        requirements: list[tuple[str, str, Decimal]],
        balances: dict[tuple[str, str], Decimal],
    ) -> bool:
        return all(
            self._bucket_has_balance(
                balances,
                bucket=bucket,
                asset=asset,
                amount=amount,
            )
            for bucket, asset, amount in requirements
        )

    @staticmethod
    def _dedupe_requirements(
        requirements: list[tuple[str, str, Decimal]],
    ) -> list[tuple[str, str, Decimal]]:
        merged: dict[tuple[str, str], Decimal] = {}
        for bucket, asset, amount in requirements:
            if not bucket or amount <= 0:
                continue
            key = (bucket, asset.upper())
            merged[key] = max(merged.get(key, Decimal("0")), amount)
        return [(bucket, asset, amount.quantize(Decimal("0.01"))) for (bucket, asset), amount in merged.items()]

    @staticmethod
    def _rail_bucket(*, rail: str, platform: str, fiat: str) -> str:
        normalized = (rail or "").lower()
        fiat_upper = fiat.upper()
        if normalized == "fiat_balance":
            return f"{platform}:fiat_balance"
        if normalized.startswith("revolut_"):
            return "revolut"
        if normalized.startswith("wise_"):
            return "wise"
        if normalized in {"bank_transfer", "bank_fx", "sepa", "blik", "zen"}:
            return "bank"
        if normalized in {"", "unknown"}:
            return ""
        return f"{normalized}:{fiat_upper}"

    @staticmethod
    def _bucket_label(bucket: str) -> str:
        if not bucket:
            return "unknown rail"
        if bucket == "revolut":
            return "Revolut"
        if bucket == "wise":
            return "Wise"
        if bucket == "bank":
            return "bank transfer"
        if bucket.endswith(":fiat_balance"):
            platform = bucket.split(":", 1)[0]
            return f"{platform} fiat balance"
        if bucket.endswith(":wallet"):
            platform = bucket.split(":", 1)[0]
            return f"{platform} wallet"
        return bucket.replace("_", " ")

    def _rail_debug_label(self, rail: str, bucket: str) -> str:
        rail_label = self.risk_guard._user_method_label(rail)
        bucket_label = self._bucket_label(bucket)
        if not rail_label or rail_label == "Не определено":
            return bucket_label
        if rail_label.lower() == bucket_label.lower():
            return rail_label
        return f"{rail_label} ({bucket_label})"

    @staticmethod
    def _is_treasury_bucket(bucket: str) -> bool:
        return bucket in {"revolut", "wise", "bank"}

    def _post_trade_rebalance_bridge(
        self,
        *,
        sell_bucket: str,
        fx_bucket: str,
        buy_bucket: str,
        is_cross_currency: bool,
    ) -> tuple[str, str] | None:
        if is_cross_currency:
            if not fx_bucket or fx_bucket != buy_bucket or sell_bucket == fx_bucket:
                return None
            if self._is_treasury_bucket(sell_bucket) and self._is_treasury_bucket(fx_bucket):
                return sell_bucket, fx_bucket
            return None

        if sell_bucket == buy_bucket:
            return None
        if self._is_treasury_bucket(sell_bucket) and self._is_treasury_bucket(buy_bucket):
            return sell_bucket, buy_bucket
        if self._is_treasury_bucket(sell_bucket) and buy_bucket.endswith(":fiat_balance"):
            return sell_bucket, buy_bucket
        if sell_bucket.endswith(":fiat_balance") and self._is_treasury_bucket(buy_bucket):
            return sell_bucket, buy_bucket
        return None

    async def _annotate_internal_quotes(self, opportunities: list[ArbitrageOpportunity]) -> None:
        for opportunity in opportunities:
            if (opportunity.base_type or opportunity.type) != "cross_currency":
                continue
            try:
                await self._annotate_internal_quote(opportunity)
            except Exception as exc:
                self.logger.debug("Internal quote comparison failed for %s: %s", opportunity.note, exc)

    async def _annotate_internal_quote(self, opportunity: ArbitrageOpportunity) -> None:
        target_fiat = opportunity.buy_order.fiat.upper()
        if target_fiat == self.settings.base_asset.upper():
            return

        quote = await self.internal_quote_client.best_quote(
            from_asset=self.settings.base_asset,
            to_asset=target_fiat,
            from_amount=opportunity.volume_usdt,
        )
        if quote is None or quote.to_amount <= 0:
            return

        expected_internal_usdt = (quote.to_amount / opportunity.buy_order.price).quantize(Decimal("0.0001"))
        expected_signal_usdt = (opportunity.volume_usdt + opportunity.estimated_profit_usd).quantize(Decimal("0.0001"))

        opportunity.internal_quote_platform = quote.platform
        opportunity.internal_quote_venue = quote.venue
        opportunity.internal_quote_exact = quote.exact
        opportunity.internal_quote_rate = quote.rate.quantize(Decimal("0.0000001"))
        opportunity.internal_quote_fiat_amount = quote.to_amount.quantize(Decimal("0.01"))
        opportunity.internal_quote_expected_usdt = expected_internal_usdt
        opportunity.internal_quote_advantage_usdt = (expected_internal_usdt - expected_signal_usdt).quantize(
            Decimal("0.0001")
        )

    def _find_internal(
        self,
        grouped: dict[tuple[str, str, str], list[P2POrder]],
        liquidity_context: dict[str, object],
    ) -> list[ArbitrageOpportunity]:
        opportunities: list[ArbitrageOpportunity] = []
        for platform in self.settings.enabled_platforms:
            for _, fiat in self.settings.pairs:
                sell_orders = self._candidate_orders(grouped.get((platform, fiat, "sell"), []), prefer_high_price=prefer_high_price("sell"))
                buy_orders = self._candidate_orders(grouped.get((platform, fiat, "buy"), []), prefer_high_price=prefer_high_price("buy"))
                if not sell_orders or not buy_orders:
                    continue

                best_spread = Decimal("-999")
                for sell_order in sell_orders:
                    for buy_order in buy_orders:
                        spread = self._spread_pct(sell_order.price, buy_order.price)
                        best_spread = max(best_spread, spread)
                        if spread < self.settings.min_internal_spread_pct:
                            continue

                        volume = self._max_route_volume_usdt(
                            buy_order=buy_order,
                            sell_order=sell_order,
                            fx_rate=Decimal("1"),
                        )
                        if volume <= 0:
                            continue

                        opportunities.append(
                            self._build_opportunity(
                                opportunity_type="internal",
                                spread_pct=spread,
                                volume_usdt=volume,
                                buy_order=buy_order,
                                sell_order=sell_order,
                                note=f"{platform.upper()} {fiat}",
                                liquidity_context=liquidity_context,
                            )
                        )
                if best_spread > Decimal("-999"):
                    self.state.set_spread(f"internal:{platform}:{fiat}", best_spread)
        return opportunities

    def _find_cross_platform(
        self,
        orders: list[P2POrder],
        liquidity_context: dict[str, object],
    ) -> list[ArbitrageOpportunity]:
        if len(self.settings.enabled_platforms) < 2:
            return []
        opportunities: list[ArbitrageOpportunity] = []
        for _, fiat in self.settings.pairs:
            sell_orders = self._candidate_orders(
                [order for order in orders if order.fiat == fiat and order.side == "sell"],
                prefer_high_price=prefer_high_price("sell"),
            )
            buy_orders = self._candidate_orders(
                [order for order in orders if order.fiat == fiat and order.side == "buy"],
                prefer_high_price=prefer_high_price("buy"),
            )

            best_spread = Decimal("-999")
            for sell_order in sell_orders:
                for buy_order in buy_orders:
                    if buy_order.platform == sell_order.platform:
                        continue
                    gross_spread = self._spread_pct(sell_order.price, buy_order.price)
                    net_spread = gross_spread - self.settings.cross_platform_fee_buffer_pct
                    best_spread = max(best_spread, net_spread)
                    if net_spread < self.settings.min_cross_platform_spread_pct:
                        continue
                    volume = self._max_route_volume_usdt(
                        buy_order=buy_order,
                        sell_order=sell_order,
                        fx_rate=Decimal("1"),
                    )
                    if volume <= 0:
                        continue
                    opportunities.append(
                        self._build_opportunity(
                            opportunity_type="cross_platform",
                            spread_pct=net_spread,
                            volume_usdt=volume,
                            buy_order=buy_order,
                            sell_order=sell_order,
                            note=f"{sell_order.platform} -> {buy_order.platform} {fiat}",
                            liquidity_context=liquidity_context,
                        )
                    )
            if best_spread > Decimal("-999"):
                self.state.set_spread(f"cross_platform:{fiat}", best_spread)
        return opportunities

    def _find_cross_currency(
        self,
        orders: list[P2POrder],
        fx_matrix: dict[tuple[str, str], Decimal],
        liquidity_context: dict[str, object],
    ) -> list[ArbitrageOpportunity]:
        opportunities: list[ArbitrageOpportunity] = []
        sell_candidates_by_fiat = {
            fiat: self._candidate_orders(
                [order for order in orders if order.fiat == fiat and order.side == "sell"],
                prefer_high_price=prefer_high_price("sell"),
            )
            for fiat in self.settings.fiats
        }
        buy_candidates_by_fiat = {
            fiat: self._candidate_orders(
                [order for order in orders if order.fiat == fiat and order.side == "buy"],
                prefer_high_price=prefer_high_price("buy"),
            )
            for fiat in self.settings.fiats
        }

        for source_fiat in self.settings.fiats:
            sell_orders = sell_candidates_by_fiat.get(source_fiat, [])
            if not sell_orders:
                continue
            for target_fiat in self.settings.fiats:
                if target_fiat == source_fiat:
                    continue
                buy_orders = buy_candidates_by_fiat.get(target_fiat, [])
                if not buy_orders:
                    continue
                fx_candidates = self._fx_chain_candidates(
                    source_fiat=source_fiat,
                    target_fiat=target_fiat,
                    fx_matrix=fx_matrix,
                )
                if not fx_candidates:
                    continue

                best_spread = Decimal("-999")
                for sell_order in sell_orders:
                    for buy_order in buy_orders:
                        for fx_path, fx_rate in fx_candidates:
                            cycle_return = (sell_order.price * fx_rate) / buy_order.price
                            if cycle_return > Decimal("2"):
                                self.logger.warning(
                                    "Suspicious FX cycle_return=%s sell_price=%s fx_rate=%s buy_price=%s source=%s target=%s path=%s",
                                    cycle_return,
                                    sell_order.price,
                                    fx_rate,
                                    buy_order.price,
                                    source_fiat,
                                    target_fiat,
                                    "->".join(fx_path),
                                )
                            gross_spread = (cycle_return - Decimal("1")) * Decimal("100")
                            fee_buffer = self.settings.cross_currency_fee_buffer_pct
                            if buy_order.platform != sell_order.platform:
                                fee_buffer += self.settings.cross_platform_fee_buffer_pct
                            net_spread = gross_spread - fee_buffer
                            best_spread = max(best_spread, net_spread)
                            if net_spread < self.settings.min_cross_platform_spread_pct:
                                continue
                            volume = self._max_route_volume_usdt(
                                buy_order=buy_order,
                                sell_order=sell_order,
                                fx_rate=fx_rate,
                            )
                            if volume <= 0:
                                continue
                            opportunities.append(
                                self._build_opportunity(
                                    opportunity_type="cross_currency",
                                    spread_pct=net_spread,
                                    volume_usdt=volume,
                                    buy_order=buy_order,
                                    sell_order=sell_order,
                                    fx_rate=fx_rate,
                                    note=(
                                        f"{source_fiat}->{self.settings.base_asset}->{target_fiat}->{source_fiat} "
                                        f"@ FX {'->'.join(fx_path)} {fx_rate}"
                                    ),
                                    liquidity_context=liquidity_context,
                                )
                            )
                if best_spread > Decimal("-999"):
                    self.state.set_spread(f"cross_currency:{source_fiat}_{target_fiat}", best_spread)
        return opportunities

    def _fx_chain_candidates(
        self,
        *,
        source_fiat: str,
        target_fiat: str,
        fx_matrix: dict[tuple[str, str], Decimal],
    ) -> list[tuple[tuple[str, ...], Decimal]]:
        return fx_chain_candidates(
            fx_matrix,
            source_fiat=source_fiat,
            target_fiat=target_fiat,
        )

    def _candidate_orders(self, orders: list[P2POrder], *, prefer_high_price: bool) -> list[P2POrder]:
        window_orders = self._candidate_book_window(orders, prefer_high_price=prefer_high_price)
        safe_orders = self.risk_guard.filter_orders(window_orders)
        safe_orders = self._candidate_quality_orders(safe_orders)
        if not safe_orders:
            return []
        sorted_orders = sorted(
            safe_orders,
            key=lambda order: self._candidate_sort_key(
                order,
                prefer_high_price=prefer_high_price,
            ),
        )
        base_limit = self.settings.max_candidates_per_side
        base = sorted_orders[:base_limit]
        best_per_tag: list[P2POrder] = []
        seen_tags: set[str] = set()
        for order in sorted_orders:
            profile = self.risk_guard.payment_profile_for_order(order)
            tag = self.risk_guard._primary_order_tag(
                order,
                profile,
                blocked_wallets=self.risk_guard._blocked_wallet_families(order),
            )
            if not tag or tag in seen_tags:
                continue
            seen_tags.add(tag)
            best_per_tag.append(order)
        ladders: list[P2POrder] = []
        if not self.settings.strict_single_merchant_mode:
            ladders = sorted(
                self._build_ladder_orders(sorted_orders, prefer_high_price=prefer_high_price),
                key=lambda order: self._candidate_sort_key(
                    order,
                    prefer_high_price=prefer_high_price,
                ),
            )[:base_limit]
        combined = base + best_per_tag + ladders
        deduped: list[P2POrder] = []
        seen_order_ids: set[str] = set()
        for order in combined:
            if order.order_id in seen_order_ids:
                continue
            seen_order_ids.add(order.order_id)
            deduped.append(order)
        combined = deduped
        combined.sort(
            key=lambda order: self._candidate_sort_key(
                order,
                prefer_high_price=prefer_high_price,
            )
        )
        candidate_limit = min(
            len(combined),
            max(
                base_limit,
                base_limit + len(seen_tags),
                base_limit + min(len(ladders), base_limit),
            ),
        )
        return combined[:candidate_limit]

    def _candidate_quality_orders(self, orders: list[P2POrder]) -> list[P2POrder]:
        filtered: list[P2POrder] = []
        for order in orders:
            if self._candidate_quality_reason(order) is not None:
                continue
            filtered.append(order)
        return filtered

    def _candidate_quality_reason(self, order: P2POrder) -> str | None:
        if order.merchant_rating <= 0 or order.merchant_rating < float(self.settings.candidate_min_rating):
            return "rating"
        if order.merchant_orders < int(self.settings.candidate_min_completed_orders):
            return "merchant_orders"

        max_last_active = int(self.settings.candidate_max_last_active_minutes)
        is_recent = (
            order.merchant_last_active_minutes is not None
            and order.merchant_last_active_minutes <= max_last_active
        )
        if order.merchant_online is not True and not is_recent:
            return "merchant_online"
        if order.merchant_kyc is False:
            return "merchant_kyc"

        target_volume = self._candidate_target_volume(order)
        if order.available < self.settings.candidate_min_available_base:
            return "available"

        target_notional = (target_volume * order.price).quantize(Decimal("0.01"))
        if order.min_amount > 0 and target_notional < order.min_amount:
            return "min_amount"
        if order.max_amount > 0 and target_notional > order.max_amount:
            return "max_amount"
        return None

    def _candidate_book_window(self, orders: list[P2POrder], *, prefer_high_price: bool) -> list[P2POrder]:
        if not orders:
            return []
        pool_depth = max(
            int(self.settings.max_candidates_per_side),
            int(self.settings.candidate_book_scan_depth),
        )
        rank_start = max(1, int(self.settings.candidate_book_rank_start))
        rank_end = max(rank_start, int(self.settings.candidate_book_rank_end))
        price_sorted = sorted(
            orders,
            key=lambda order: self._candidate_book_price_key(order, prefer_high_price=prefer_high_price),
        )
        pool = price_sorted[:pool_depth]
        if not pool:
            return []
        median_price = self._candidate_book_median_price(pool)
        if median_price <= 0:
            return pool
        if len(pool) < rank_start or len(pool) < rank_end:
            window = pool
        else:
            window = pool[rank_start - 1 : rank_end]
        filtered_window = [
            order
            for order in window
            if self._candidate_order_is_within_book_band(
                order,
                median_price=median_price,
                prefer_high_price=prefer_high_price,
            )
        ]
        return filtered_window

    @staticmethod
    def _candidate_book_median_price(orders: list[P2POrder]) -> Decimal:
        if not orders:
            return Decimal("0")
        prices = sorted(order.price for order in orders)
        mid = len(prices) // 2
        if len(prices) % 2 == 1:
            return prices[mid]
        return (prices[mid - 1] + prices[mid]) / Decimal("2")

    def _candidate_order_is_within_book_band(
        self,
        order: P2POrder,
        *,
        median_price: Decimal,
        prefer_high_price: bool,
    ) -> bool:
        if median_price <= 0:
            return True
        deviation_pct = (abs(order.price - median_price) / median_price) * Decimal("100")
        if deviation_pct > self.settings.candidate_median_deviation_pct:
            return False
        phantom_multiplier = self.settings.candidate_phantom_deviation_pct / Decimal("100")
        if prefer_high_price and order.price > median_price * (Decimal("1") + phantom_multiplier):
            return False
        if not prefer_high_price and order.price < median_price * (Decimal("1") - phantom_multiplier):
            return False
        return True

    def _candidate_target_volume(self, order: P2POrder) -> Decimal:
        config = self.settings.order_config_for(order.fiat, order.asset)
        target = config.max_single_order_usd
        if target <= 0:
            target = self.settings.default_pair_order_config.max_single_order_usd
        return target

    @staticmethod
    def _candidate_book_price_key(
        order: P2POrder,
        *,
        prefer_high_price: bool,
    ) -> tuple[Decimal, Decimal, float, int]:
        price_rank = -order.price if prefer_high_price else order.price
        return (
            price_rank,
            -order.available,
            -float(order.merchant_rating or 0.0),
            -int(order.merchant_orders or 0),
        )

    def _candidate_sort_key(
        self,
        order: P2POrder,
        *,
        prefer_high_price: bool,
    ) -> tuple[Decimal, int, int, int, int, float, int, int, Decimal]:
        price_rank = -order.price if prefer_high_price else order.price
        component_count = self._component_count(order)
        ladder_rank = 1 if component_count >= 2 else 0
        online_rank = 0 if order.merchant_online is True else 1 if order.merchant_online is None else 2
        kyc_rank = 0 if order.merchant_kyc is True else 1 if order.merchant_kyc is None else 2
        last_active_rank = (
            order.merchant_last_active_minutes
            if order.merchant_last_active_minutes is not None
            else _UNKNOWN_LAST_ACTIVE_MINUTES
        )
        return (
            price_rank,
            ladder_rank,
            online_rank,
            kyc_rank,
            last_active_rank,
            -float(order.merchant_rating or 0.0),
            -int(order.merchant_orders or 0),
            -int(order.merchant_days or 0),
            -order.available,
        )

    @staticmethod
    def _component_count(order: P2POrder) -> int:
        raw = order.raw or {}
        components = raw.get("components") if isinstance(raw, dict) else None
        if isinstance(components, list):
            return len(components)
        return 0

    @staticmethod
    def _min_buffer_multiplier(buffer_pct: Decimal) -> Decimal:
        return Decimal("1") + max(buffer_pct, Decimal("0")) / Decimal("100")

    @staticmethod
    def _headroom_multiplier(headroom_pct: Decimal) -> Decimal:
        multiplier = Decimal("1") - max(headroom_pct, Decimal("0")) / Decimal("100")
        return max(multiplier, Decimal("0"))

    def _max_route_volume_usdt(
        self,
        *,
        buy_order: P2POrder,
        sell_order: P2POrder,
        fx_rate: Decimal,
    ) -> Decimal:
        min_buffer_multiplier = self._min_buffer_multiplier(self.settings.route_min_amount_buffer_pct)
        max_headroom_multiplier = self._headroom_multiplier(self.settings.route_max_amount_headroom_pct)
        available_headroom_multiplier = self._headroom_multiplier(self.settings.route_available_headroom_pct)
        max_single = Decimal(str(self.settings.counterparty_filter.max_single_trade_usd))
        if self.settings.prefunded_mode:
            max_single = min(max_single, self.settings.prefunded_exchange_usdt)
        limits = [max_single, sell_order.available * available_headroom_multiplier]
        min_required = Decimal("0")

        sell_price = sell_order.price
        if sell_price > 0:
            limits.append((sell_order.max_amount * max_headroom_multiplier) / sell_price)
            if sell_order.min_amount > 0:
                min_required = max(
                    min_required,
                    (sell_order.min_amount * min_buffer_multiplier) / sell_price,
                )

        rebuy_budget_per_start_usdt = sell_price * fx_rate
        if rebuy_budget_per_start_usdt > 0:
            limits.append((buy_order.max_amount * max_headroom_multiplier) / rebuy_budget_per_start_usdt)
            if buy_order.min_amount > 0:
                min_required = max(
                    min_required,
                    (buy_order.min_amount * min_buffer_multiplier) / rebuy_budget_per_start_usdt,
                )
            if buy_order.available > 0 and buy_order.price > 0:
                limits.append(
                    ((buy_order.available * available_headroom_multiplier) * buy_order.price)
                    / rebuy_budget_per_start_usdt
                )

        volume = min(limits) if limits else Decimal("0")
        if volume < min_required:
            return Decimal("0")
        return max(volume, Decimal("0")).quantize(Decimal("0.0001"))

    def _filter_sane_opportunities(
        self,
        opportunities: list[ArbitrageOpportunity],
        fx_matrix: dict[tuple[str, str], Decimal],
    ) -> list[ArbitrageOpportunity]:
        filtered: list[ArbitrageOpportunity] = []
        for opportunity in opportunities:
            if opportunity.spread_pct > Decimal("30"):
                self.logger.error(
                    "Rejecting unrealistic spread %.4f%%: sell=%s fx=%s buy=%s note=%s",
                    opportunity.spread_pct,
                    opportunity.sell_order.price,
                    opportunity.fx_rate_used,
                    opportunity.buy_order.price,
                    opportunity.note,
                )
                continue
            sane, reason = self._opportunity_prices_sane(opportunity, fx_matrix)
            if sane:
                filtered.append(opportunity)
                continue
            self.logger.info("Rejecting opportunity %s: %s", opportunity.note, reason)
        return filtered

    def _opportunity_prices_sane(
        self,
        opportunity: ArbitrageOpportunity,
        fx_matrix: dict[tuple[str, str], Decimal],
    ) -> tuple[bool, str]:
        if self.settings.strict_single_merchant_mode and self._has_multi_merchant_leg(opportunity):
            return False, "strict single-merchant mode rejects ladder routes"
        if str(opportunity.sell_order.side).lower() != "sell":
            return False, f"step 1 side mismatch: expected sell, got {opportunity.sell_order.side}"
        if str(opportunity.buy_order.side).lower() != "buy":
            return False, f"step 3 side mismatch: expected buy, got {opportunity.buy_order.side}"
        for label, order in (("step 1", opportunity.sell_order), ("step 3", opportunity.buy_order)):
            identity_sane, identity_reason = self._order_identity_sane(order)
            if not identity_sane:
                return False, f"{label}: {identity_reason}"
            link_sane, link_reason = self._order_link_sane(order)
            if not link_sane:
                return False, f"{label}: {link_reason}"
            sane, reason = self._order_price_sane_against_fx(order, fx_matrix)
            if not sane:
                return False, f"{label}: {reason}"
        flow_sane, flow_reason = self._opportunity_flow_sane(opportunity)
        if not flow_sane:
            return False, flow_reason
        chain_sane, chain_reason = self._opportunity_chain_sane(opportunity)
        if not chain_sane:
            return False, chain_reason
        return True, "OK"

    @staticmethod
    def _order_identity_sane(order: P2POrder) -> tuple[bool, str]:
        raw = order.raw or {}
        components = raw.get("components") if isinstance(raw, dict) else None
        if isinstance(components, list) and components:
            for component in components:
                if not isinstance(component, dict):
                    return False, "invalid ladder component payload"
                merchant_name = str(component.get("merchant_name") or "").strip()
                merchant_id = str(component.get("merchant_id") or "").strip()
                if merchant_name or merchant_id:
                    continue
                return False, "ladder component merchant identity missing"
            return True, "OK"
        if not order.resolved_merchant_name():
            return False, "merchant name missing"
        if not order.resolved_merchant_id():
            return False, "merchant id missing"
        return True, "OK"

    @staticmethod
    def _order_link_sane(order: P2POrder) -> tuple[bool, str]:
        url = order_action_url(order)
        if not url:
            return True, "OK"
        side = str(order.side or "").strip().lower()
        platform = str(order.platform or "").strip().lower()
        normalized_url = url.lower()
        if platform in {"binance", "bybit"}:
            expected = "/sell/" if side == "sell" else "/buy/"
            if expected not in normalized_url:
                return False, f"link side mismatch for {platform}: expected {side}, got {url}"
            return True, "OK"
        if platform == "bingx":
            expected = "type=2" if side == "sell" else "type=1"
            if expected not in normalized_url:
                return False, f"link side mismatch for bingx: expected {side}, got {url}"
        return True, "OK"

    def _opportunity_chain_sane(self, opportunity: ArbitrageOpportunity) -> tuple[bool, str]:
        if opportunity.type == "internal_graph":
            return self._internal_graph_chain_sane(opportunity)

        sell_bucket = self._rail_bucket(
            rail=opportunity.sell_rail,
            platform=opportunity.sell_order.platform,
            fiat=opportunity.sell_order.fiat,
        )
        buy_bucket = self._rail_bucket(
            rail=opportunity.buy_rail,
            platform=opportunity.buy_order.platform,
            fiat=opportunity.buy_order.fiat,
        )
        if not sell_bucket or not buy_bucket:
            return False, "unresolved settlement rail"

        if self.settings.prefunded_mode:
            if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper():
                fx_bucket = self._rail_bucket(
                    rail=opportunity.fx_rail,
                    platform=opportunity.sell_order.platform,
                    fiat=opportunity.sell_order.fiat,
                )
                if not fx_bucket:
                    return False, "unresolved FX settlement rail"
                if sell_bucket != fx_bucket:
                    if not self._bucket_transition_allowed(
                        source_bucket=sell_bucket,
                        target_bucket=fx_bucket,
                        target_rail=opportunity.fx_rail,
                        target_fiat=opportunity.sell_order.fiat,
                    ) and self._post_trade_rebalance_bridge(
                        sell_bucket=sell_bucket,
                        fx_bucket=fx_bucket,
                        buy_bucket=buy_bucket,
                        is_cross_currency=True,
                    ) is None:
                        return (
                            False,
                            f"step 1 settles to {self._rail_debug_label(opportunity.sell_rail, sell_bucket)}, "
                            f"but step 2 executes via {self._rail_debug_label(opportunity.fx_rail, fx_bucket)}",
                        )
                if not self._bucket_transition_allowed(
                    source_bucket=fx_bucket,
                    target_bucket=buy_bucket,
                    target_rail=opportunity.buy_rail,
                    target_fiat=opportunity.buy_order.fiat,
                ):
                    return (
                        False,
                        f"step 2 settles to {self._rail_debug_label(opportunity.fx_rail, fx_bucket)}, "
                        f"but step 3 executes via {self._rail_debug_label(opportunity.buy_rail, buy_bucket)}",
                    )
                return True, "OK"
            if not self._bucket_transition_allowed(
                source_bucket=sell_bucket,
                target_bucket=buy_bucket,
                target_rail=opportunity.buy_rail,
                target_fiat=opportunity.buy_order.fiat,
            ) and self._post_trade_rebalance_bridge(
                sell_bucket=sell_bucket,
                fx_bucket="",
                buy_bucket=buy_bucket,
                is_cross_currency=False,
            ) is None:
                return (
                    False,
                    f"step 1 settles to {self._rail_debug_label(opportunity.sell_rail, sell_bucket)}, "
                    f"but next step executes via {self._rail_debug_label(opportunity.buy_rail, buy_bucket)}",
                )
            return True, "OK"

        if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper():
            fx_bucket = self._rail_bucket(
                rail=opportunity.fx_rail,
                platform=opportunity.sell_order.platform,
                fiat=opportunity.sell_order.fiat,
            )
            if not fx_bucket:
                return False, "unresolved FX settlement rail"
            if sell_bucket != fx_bucket:
                if not self._bucket_transition_allowed(
                    source_bucket=sell_bucket,
                    target_bucket=fx_bucket,
                    target_rail=opportunity.fx_rail,
                    target_fiat=opportunity.sell_order.fiat,
                ):
                    return (
                        False,
                        f"step 1 settles to {self._bucket_label(sell_bucket)}, "
                        f"but step 2 executes in {self._bucket_label(fx_bucket)}",
                    )
            if fx_bucket != buy_bucket:
                if not self._bucket_transition_allowed(
                    source_bucket=fx_bucket,
                    target_bucket=buy_bucket,
                    target_rail=opportunity.buy_rail,
                    target_fiat=opportunity.buy_order.fiat,
                ):
                    return (
                        False,
                        f"step 2 settles to {self._rail_debug_label(opportunity.fx_rail, fx_bucket)}, "
                        f"but step 3 executes via {self._rail_debug_label(opportunity.buy_rail, buy_bucket)}",
                    )
            return True, "OK"

        if sell_bucket != buy_bucket:
            if not self._bucket_transition_allowed(
                source_bucket=sell_bucket,
                target_bucket=buy_bucket,
                target_rail=opportunity.buy_rail,
                target_fiat=opportunity.buy_order.fiat,
            ):
                return (
                    False,
                    f"step 1 settles to {self._rail_debug_label(opportunity.sell_rail, sell_bucket)}, "
                    f"but next step executes via {self._rail_debug_label(opportunity.buy_rail, buy_bucket)}",
                )
        return True, "OK"

    def _internal_graph_chain_sane(self, opportunity: ArbitrageOpportunity) -> tuple[bool, str]:
        raw = opportunity.sell_order.raw or {}
        chain = raw.get("chain") or []
        quotes = raw.get("quotes") or []
        base_asset = self.settings.base_asset.upper()

        if not isinstance(chain, list) or len(chain) < 3:
            return False, "internal_graph chain is incomplete"
        if str(chain[0]).upper() != base_asset or str(chain[-1]).upper() != base_asset:
            return False, "internal_graph chain must start and end in base asset"
        if not isinstance(quotes, list) or len(quotes) != len(chain) - 1:
            return False, "internal_graph quote chain length mismatch"

        expected_platform = opportunity.sell_order.platform.lower()
        for index, quote in enumerate(quotes, start=1):
            if str(quote.get("platform") or "").lower() != expected_platform:
                return False, f"internal_graph quote {index} platform mismatch"
            try:
                rate = Decimal(str(quote.get("rate") or "0"))
            except Exception:
                rate = Decimal("0")
            if rate <= 0:
                return False, f"internal_graph quote {index} has non-positive rate"
            if not bool(quote.get("exact")):
                return False, f"internal_graph quote {index} is indicative"
        return True, "OK"

    def _bucket_transition_allowed(
        self,
        *,
        source_bucket: str,
        target_bucket: str,
        target_rail: str,
        target_fiat: str,
    ) -> bool:
        if not source_bucket or not target_bucket:
            return False
        if source_bucket == target_bucket:
            return True
        if source_bucket in {"revolut", "wise", "bank"} and target_bucket.endswith(":fiat_balance"):
            return True
        if source_bucket.endswith(":fiat_balance") and target_bucket in {"revolut", "wise", "bank"}:
            return True
        normalized_target_rail = (target_rail or "").lower()
        if (
            normalized_target_rail == "sepa"
            and target_fiat.upper() == "EUR"
            and source_bucket in {"revolut", "wise", "bank"}
            and target_bucket == "bank"
        ):
            return True
        if (
            normalized_target_rail == "bank_transfer"
            and target_bucket == "bank"
        ):
            target_fiat_upper = target_fiat.upper()
            if source_bucket == "revolut":
                return target_fiat_upper in {fiat.upper() for fiat in self.settings.revolut_bank_transfer_fiats}
            if source_bucket == "wise":
                return target_fiat_upper in {fiat.upper() for fiat in self.settings.wise_bank_transfer_fiats}
            if source_bucket == "bank":
                return target_fiat_upper in {fiat.upper() for fiat in self.settings.local_bank_fiats}
        if normalized_target_rail == "blik" and target_bucket == "bank" and target_fiat.upper() == "PLN":
            return source_bucket in {"revolut", "wise", "bank"}
        if normalized_target_rail == "zen" and target_bucket == "bank" and target_fiat.upper() == "PLN":
            return source_bucket in {"revolut", "wise", "bank"}
        return False

    def _route_buckets(self, opportunity: ArbitrageOpportunity) -> tuple[str, str, str]:
        sell_bucket = self._rail_bucket(
            rail=opportunity.sell_rail,
            platform=opportunity.sell_order.platform,
            fiat=opportunity.sell_order.fiat,
        )
        buy_bucket = self._rail_bucket(
            rail=opportunity.buy_rail,
            platform=opportunity.buy_order.platform,
            fiat=opportunity.buy_order.fiat,
        )
        fx_bucket = ""
        if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper():
            fx_bucket = self._rail_bucket(
                rail=opportunity.fx_rail,
                platform=opportunity.sell_order.platform,
                fiat=opportunity.sell_order.fiat,
            )
        return sell_bucket, fx_bucket, buy_bucket

    def _is_prefunded_route(self, opportunity: ArbitrageOpportunity) -> bool:
        if not self.settings.prefunded_mode or opportunity.type == "internal_graph":
            return False
        sell_bucket, fx_bucket, buy_bucket = self._route_buckets(opportunity)
        if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper():
            if not sell_bucket or not fx_bucket or not buy_bucket:
                return False
            if sell_bucket != fx_bucket:
                return True
            return not self._bucket_transition_allowed(
                source_bucket=fx_bucket,
                target_bucket=buy_bucket,
                target_rail=opportunity.buy_rail,
                target_fiat=opportunity.buy_order.fiat,
            )
        if not sell_bucket or not buy_bucket:
            return False
        return not self._bucket_transition_allowed(
            source_bucket=sell_bucket,
            target_bucket=buy_bucket,
            target_rail=opportunity.buy_rail,
            target_fiat=opportunity.buy_order.fiat,
        )

    def _opportunity_flow_sane(self, opportunity: ArbitrageOpportunity) -> tuple[bool, str]:
        if opportunity.type == "internal_graph":
            return True, "OK"

        sell_fiat_amount, rebuy_fiat_amount, gross_return_usdt, _ = self._route_flow_metrics(
            volume_usdt=opportunity.volume_usdt,
            buy_order=opportunity.buy_order,
            sell_order=opportunity.sell_order,
            fx_rate=opportunity.fx_rate_used if opportunity.fx_rate_used > 0 else Decimal("1"),
        )

        if sell_fiat_amount <= 0 or rebuy_fiat_amount <= 0 or gross_return_usdt <= 0:
            return False, "non-positive step flow"
        tolerance = Decimal("0.01")
        volume_tolerance = Decimal("0.0001")
        min_buffer_multiplier = self._min_buffer_multiplier(self.settings.route_min_amount_buffer_pct)
        max_headroom_multiplier = self._headroom_multiplier(self.settings.route_max_amount_headroom_pct)
        available_headroom_multiplier = self._headroom_multiplier(self.settings.route_available_headroom_pct)
        sell_min_with_buffer = opportunity.sell_order.min_amount * min_buffer_multiplier
        sell_max_with_headroom = opportunity.sell_order.max_amount * max_headroom_multiplier
        buy_min_with_buffer = opportunity.buy_order.min_amount * min_buffer_multiplier
        buy_max_with_headroom = opportunity.buy_order.max_amount * max_headroom_multiplier
        sell_available_with_headroom = opportunity.sell_order.available * available_headroom_multiplier
        buy_available_with_headroom = opportunity.buy_order.available * available_headroom_multiplier
        if sell_fiat_amount + tolerance < sell_min_with_buffer:
            return False, f"step 1 below buffered sell min amount: {sell_fiat_amount} < {sell_min_with_buffer}"
        if sell_fiat_amount > sell_max_with_headroom + tolerance:
            return False, f"step 1 exceeds buffered sell max amount: {sell_fiat_amount} > {sell_max_with_headroom}"
        if rebuy_fiat_amount + tolerance < buy_min_with_buffer:
            return False, f"step 3 below buffered buy min amount: {rebuy_fiat_amount} < {buy_min_with_buffer}"
        if rebuy_fiat_amount > buy_max_with_headroom + tolerance:
            return False, f"step 3 exceeds buffered buy max amount: {rebuy_fiat_amount} > {buy_max_with_headroom}"
        if opportunity.volume_usdt > sell_available_with_headroom + volume_tolerance:
            return False, (
                f"step 1 exceeds buffered available {self.settings.base_asset}: "
                f"{opportunity.volume_usdt} > {sell_available_with_headroom}"
            )
        if gross_return_usdt > buy_available_with_headroom + volume_tolerance:
            return False, (
                f"step 3 exceeds buffered available {self.settings.base_asset}: "
                f"{gross_return_usdt} > {buy_available_with_headroom}"
            )
        return True, "OK"

    def _order_price_sane_against_fx(
        self,
        order: P2POrder,
        fx_matrix: dict[tuple[str, str], Decimal],
    ) -> tuple[bool, str]:
        if order.asset.upper() != self.settings.base_asset.upper():
            return True, "OK"
        if order.raw.get("internal_graph"):
            return True, "OK"

        direct_ok, direct_reason = self._price_sane_against_fx(
            fiat=order.fiat,
            platform=order.platform,
            price=order.price,
            fx_matrix=fx_matrix,
        )
        if not direct_ok:
            return False, direct_reason

        for component in order.raw.get("components", []):
            component_price = component.get("price")
            if component_price in (None, ""):
                continue
            sane, reason = self._price_sane_against_fx(
                fiat=order.fiat,
                platform=order.platform,
                price=Decimal(str(component_price)),
                fx_matrix=fx_matrix,
            )
            if not sane:
                component_id = component.get("order_id", "unknown")
                return False, f"component {component_id}: {reason}"

        return True, "OK"

    def _price_sane_against_fx(
        self,
        *,
        fiat: str,
        platform: str | None = None,
        price: Decimal,
        fx_matrix: dict[tuple[str, str], Decimal],
    ) -> tuple[bool, str]:
        fiat_upper = fiat.upper()
        if price <= 0:
            return False, f"non-positive {fiat_upper}/{self.settings.base_asset.upper()} price: {price}"
        min_price, max_price = self.settings.p2p_price_bounds_for_fiat(fiat_upper)
        if min_price is not None and price < min_price:
            return (
                False,
                f"{fiat_upper}/{self.settings.base_asset.upper()} price {price} below minimum {min_price}",
            )
        if max_price is not None and price > max_price:
            return (
                False,
                f"{fiat_upper}/{self.settings.base_asset.upper()} price {price} above maximum {max_price}",
            )
        if fiat_upper == "USD":
            return True, "OK"

        fair_price = fx_matrix.get(("USD", fiat_upper))
        if fair_price is None or fair_price <= 0:
            return True, "OK"

        max_deviation_pct = self.settings.p2p_price_max_deviation_pct_for(
            fiat_upper,
            platform=platform,
        )
        deviation_pct = (abs(price - fair_price) / fair_price) * Decimal("100")
        if deviation_pct > max_deviation_pct:
            return (
                False,
                f"{fiat_upper}/{self.settings.base_asset.upper()} price {price} too far from "
                f"FX fair {fair_price} ({deviation_pct.quantize(Decimal('0.01'))}% deviation > "
                f"{max_deviation_pct}%)",
            )
        return True, "OK"

    def _build_ladder_orders(self, orders: list[P2POrder], *, prefer_high_price: bool) -> list[P2POrder]:
        grouped: dict[tuple[str, str, str, str], list[P2POrder]] = defaultdict(list)
        for order in orders:
            profile = self.risk_guard.payment_profile_for_order(order)
            tag = self.risk_guard._primary_order_tag(order, profile) or "unknown"
            grouped[(order.platform, order.fiat, order.side, tag)].append(order)

        ladders: list[P2POrder] = []
        for (_, _, _, tag), group in grouped.items():
            ranked = sorted(group, key=lambda item: item.price, reverse=prefer_high_price)
            if len(ranked) < 2:
                continue
            components: list[P2POrder] = []
            total_available = Decimal("0")
            total_fiat = Decimal("0")
            total_max_amount = Decimal("0")
            for order in ranked[: max(self.settings.max_candidates_per_side, 3)]:
                components.append(order)
                total_available += order.available
                total_fiat += order.available * order.price
                total_max_amount += order.max_amount
                if total_available <= 0:
                    continue
                weighted_price = total_fiat / total_available
                if len(components) >= 2:
                    known_online = [item.merchant_online for item in components if item.merchant_online is not None]
                    known_kyc = [item.merchant_kyc for item in components if item.merchant_kyc is not None]
                    known_last_active = [
                        item.merchant_last_active_minutes
                        for item in components
                        if item.merchant_last_active_minutes is not None
                    ]
                    ladders.append(
                        P2POrder(
                            platform=order.platform,
                            order_id="+".join(item.order_id for item in components),
                            side=order.side,
                            asset=order.asset,
                            fiat=order.fiat,
                            price=weighted_price.quantize(Decimal("0.0001")),
                            min_amount=max(item.min_amount for item in components),
                            max_amount=total_max_amount.quantize(Decimal("0.01")),
                            available=total_available.quantize(Decimal("0.0001")),
                            payment_methods=[self._ladder_payment_label(tag)],
                            merchant_id=f"ladder:{order.platform}:{order.fiat}:{order.side}:{tag}",
                            merchant_rating=min(float(item.merchant_rating) for item in components),
                            merchant_orders=sum(item.merchant_orders for item in components),
                            merchant_days=min(item.merchant_days for item in components),
                            merchant_online=all(known_online) if known_online else None,
                            merchant_kyc=all(known_kyc) if known_kyc else None,
                            merchant_last_active_minutes=max(known_last_active) if known_last_active else None,
                            timestamp=max(item.timestamp for item in components),
                            raw={
                                "components": [
                                    {
                                        "order_id": item.order_id,
                                        "price": str(item.price),
                                        "available": str(item.available),
                                        "payment_methods": list(item.payment_methods),
                                        "merchant_id": item.merchant_id,
                                        "merchant_name": item.merchant_name,
                                        "merchant_rating": item.merchant_rating,
                                        "merchant_orders": item.merchant_orders,
                                    }
                                    for item in components
                                ]
                            },
                        )
                    )
        return ladders

    def _ladder_payment_label(self, tag: str) -> str:
        return {
            "revolut_balance": "Revolut",
            "revolut_bank_transfer": "Revolut",
            "revolut_card": "Revolut",
            "revolut_card_manual": "Revolut (карта)",
            "wise_balance": "Wise",
            "wise_bank_transfer": "Wise",
            "wise_card": "Wise",
            "bank_fx": "Bank FX",
            "fiat_balance": "Balance",
            "blik": "BLIK",
            "sepa": "SEPA",
            "bank_transfer": "Polish Bank Transfer",
        }.get(tag, "Polish Bank Transfer")

    def _build_opportunity(
        self,
        *,
        opportunity_type: str,
        spread_pct: Decimal,
        volume_usdt: Decimal,
        buy_order: P2POrder,
        sell_order: P2POrder,
        note: str,
        liquidity_context: dict[str, object],
        fx_rate: Decimal = Decimal("1"),
    ) -> ArbitrageOpportunity:
        detected_at = utc_now()
        if opportunity_type == "internal_graph":
            sell_fiat_amount = Decimal("0")
            rebuy_fiat_amount = Decimal("0")
            gross_return_usdt = (volume_usdt + ((spread_pct / Decimal("100")) * volume_usdt)).quantize(Decimal("0.0001"))
            gross_profit = (gross_return_usdt - volume_usdt).quantize(Decimal("0.01"))
            gross_spread_pct = spread_pct
        else:
            sell_fiat_amount, rebuy_fiat_amount, gross_return_usdt, gross_spread_pct = self._route_flow_metrics(
                volume_usdt=volume_usdt,
                buy_order=buy_order,
                sell_order=sell_order,
                fx_rate=fx_rate,
            )
            gross_profit = (gross_return_usdt - volume_usdt).quantize(Decimal("0.01"))
        (
            rail_status,
            buy_payment_summary,
            sell_payment_summary,
            buy_rail,
            sell_rail,
            buy_user_method,
            sell_user_method,
        ) = self.risk_guard.route_details(
            buy_order,
            sell_order,
        )
        buy_fee, sell_fee, fx_fee, fx_rail = self._fee_breakdown(
            opportunity_type=opportunity_type,
            volume_usdt=volume_usdt,
            buy_order=buy_order,
            sell_order=sell_order,
            buy_rail=buy_rail,
            sell_rail=sell_rail,
            raw_spread_pct=spread_pct,
        )
        total_fees = buy_fee + sell_fee + fx_fee
        estimated_profit = gross_profit - total_fees
        fee_spread_pct = (
            (total_fees / volume_usdt) * Decimal("100")
            if volume_usdt > 0
            else Decimal("0")
        )
        net_spread = gross_spread_pct - fee_spread_pct
        liquidity_score, liquidity_status, liquidity_summary = self._route_liquidity(
            buy_order=buy_order,
            sell_order=sell_order,
            volume_usdt=volume_usdt,
            liquidity_context=liquidity_context,
        )
        sell_bucket = self._rail_bucket(
            rail=sell_rail or "",
            platform=sell_order.platform,
            fiat=sell_order.fiat,
        )
        buy_bucket = self._rail_bucket(
            rail=buy_rail or "",
            platform=buy_order.platform,
            fiat=buy_order.fiat,
        )
        fx_bucket = ""
        is_cross_currency = opportunity_type == "cross_currency" and buy_order.fiat.upper() != sell_order.fiat.upper()
        if is_cross_currency:
            fx_bucket = self._rail_bucket(
                rail=fx_rail,
                platform=sell_order.platform,
                fiat=sell_order.fiat,
            )
        rebalance_source = ""
        rebalance_target = ""
        if self.settings.prefunded_mode:
            rebalance = self._post_trade_rebalance_bridge(
                sell_bucket=sell_bucket,
                fx_bucket=fx_bucket,
                buy_bucket=buy_bucket,
                is_cross_currency=is_cross_currency,
            )
            if rebalance is not None:
                rebalance_source, rebalance_target = rebalance
        manual_execution_hint = self._manual_execution_hint(
            buy_rail=buy_rail or "",
            buy_order=buy_order,
            sell_bucket=sell_bucket,
            fx_bucket=fx_bucket,
        )
        return ArbitrageOpportunity(
            type=opportunity_type,
            base_type=opportunity_type,
            spread_pct=net_spread.quantize(Decimal("0.0001")),
            estimated_profit_usd=estimated_profit.quantize(Decimal("0.01")),
            gross_profit_usd=gross_profit.quantize(Decimal("0.01")),
            total_fees_usd=total_fees.quantize(Decimal("0.01")),
            buy_fee_usd=buy_fee.quantize(Decimal("0.01")),
            sell_fee_usd=sell_fee.quantize(Decimal("0.01")),
            fx_fee_usd=fx_fee.quantize(Decimal("0.01")),
            volume_usdt=volume_usdt.quantize(Decimal("0.0001")),
            buy_order=buy_order,
            sell_order=sell_order,
            detected_at=detected_at,
            expires_at=detected_at + timedelta(seconds=self.settings.opportunity_ttl_sec),
            note=note,
            rail_status=rail_status,
            buy_rail=buy_rail or "",
            sell_rail=sell_rail or "",
            fx_rail=fx_rail,
            buy_user_method=buy_user_method,
            sell_user_method=sell_user_method,
            buy_payment_summary=buy_payment_summary,
            sell_payment_summary=sell_payment_summary,
            liquidity_score=liquidity_score,
            liquidity_status=liquidity_status,
            liquidity_summary=liquidity_summary,
            sell_fiat_amount=sell_fiat_amount.quantize(Decimal("0.01")),
            rebuy_fiat_amount=rebuy_fiat_amount.quantize(Decimal("0.01")),
            gross_return_usdt=gross_return_usdt.quantize(Decimal("0.0001")),
            fx_rate_used=fx_rate.quantize(Decimal("0.0000001")) if fx_rate > 0 else Decimal("1"),
            post_trade_rebalance_source=rebalance_source,
            post_trade_rebalance_target=rebalance_target,
            manual_execution_hint=manual_execution_hint,
        )

    def _route_flow_metrics(
        self,
        *,
        volume_usdt: Decimal,
        buy_order: P2POrder,
        sell_order: P2POrder,
        fx_rate: Decimal,
    ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        sell_fiat_amount = volume_usdt * sell_order.price
        rebuy_fiat_amount = sell_fiat_amount * fx_rate
        gross_return_usdt = (rebuy_fiat_amount / buy_order.price) if buy_order.price > 0 else Decimal("0")
        gross_spread_pct = (
            ((gross_return_usdt - volume_usdt) / volume_usdt) * Decimal("100")
            if volume_usdt > 0
            else Decimal("0")
        )
        return (
            sell_fiat_amount.quantize(Decimal("0.01")),
            rebuy_fiat_amount.quantize(Decimal("0.01")),
            gross_return_usdt.quantize(Decimal("0.0001")),
            gross_spread_pct.quantize(Decimal("0.0001")),
        )

    def _fee_breakdown(
        self,
        *,
        opportunity_type: str,
        volume_usdt: Decimal,
        buy_order: P2POrder,
        sell_order: P2POrder,
        buy_rail: str | None,
        sell_rail: str | None,
        raw_spread_pct: Decimal,
    ) -> tuple[Decimal, Decimal, Decimal, str]:
        buy_fee = self._rail_fee_usd(
            buy_rail,
            volume_usdt,
            leg="send",
            primary_fiat=buy_order.fiat,
            primary_price=buy_order.price,
        )
        sell_fee = self._rail_fee_usd(
            sell_rail,
            volume_usdt,
            leg="receive",
            primary_fiat=sell_order.fiat,
            primary_price=sell_order.price,
        )
        fx_fee = Decimal("0")
        fx_rail = ""
        if opportunity_type == "cross_currency":
            fx_rail, fx_fee = self._best_fx_rail(
                volume_usdt=volume_usdt,
                buy_order=buy_order,
                sell_order=sell_order,
                buy_rail=buy_rail,
                sell_rail=sell_rail,
                buy_fee=buy_fee,
                sell_fee=sell_fee,
                raw_spread_pct=raw_spread_pct,
            )
        return buy_fee, sell_fee, fx_fee, fx_rail

    def _best_fx_rail(
        self,
        *,
        volume_usdt: Decimal,
        buy_order: P2POrder,
        sell_order: P2POrder,
        buy_rail: str | None,
        sell_rail: str | None,
        buy_fee: Decimal,
        sell_fee: Decimal,
        raw_spread_pct: Decimal,
    ) -> tuple[str, Decimal]:
        candidate_rails = self.settings.fx_candidate_rails or (self.settings.fx_conversion_rail,)
        candidate_rails = self._eligible_fx_rails_for_route(
            candidate_rails=candidate_rails,
            buy_rail=buy_rail,
            sell_rail=sell_rail,
            source_fiat=sell_order.fiat,
            target_fiat=buy_order.fiat,
        )
        best_rail = ""
        best_fee = Decimal("0")
        best_profit = Decimal("-999999999")

        gross_profit = (raw_spread_pct / Decimal("100")) * volume_usdt
        for rail in candidate_rails:
            if not self._fx_rail_supported_for_pair(
                rail=rail,
                source_fiat=sell_order.fiat,
                target_fiat=buy_order.fiat,
            ):
                continue
            fx_fee = self._rail_fee_usd(
                rail,
                volume_usdt,
                leg="fx",
                primary_fiat=sell_order.fiat,
                primary_price=sell_order.price,
                secondary_fiat=buy_order.fiat,
                secondary_price=buy_order.price,
            )
            markup_pct = self.settings.rail_fee_profile(rail).fx_rate_markup_pct
            markup_fee = (volume_usdt * markup_pct / Decimal("100")) if markup_pct > 0 else Decimal("0")
            safety_fee = self._fx_safety_fee_usd(rail, volume_usdt)
            total_cost = buy_fee + sell_fee + fx_fee + markup_fee + safety_fee
            profit = gross_profit - total_cost
            if profit > best_profit:
                best_profit = profit
                best_rail = rail
                best_fee = fx_fee + markup_fee + safety_fee
        return best_rail, best_fee

    def _eligible_fx_rails_for_route(
        self,
        *,
        candidate_rails: tuple[str, ...],
        buy_rail: str | None,
        sell_rail: str | None,
        source_fiat: str,
        target_fiat: str,
    ) -> tuple[str, ...]:
        normalized_buy = (buy_rail or "").lower()
        normalized_sell = (sell_rail or "").lower()
        if self.settings.prefunded_mode:
            if normalized_buy.startswith("revolut_"):
                preferred = tuple(rail for rail in candidate_rails if rail.lower().startswith("revolut_"))
                return preferred or candidate_rails
            if normalized_buy.startswith("wise_"):
                preferred = tuple(rail for rail in candidate_rails if rail.lower().startswith("wise_"))
                return preferred or candidate_rails
            if normalized_buy in {"bank_transfer", "blik", "bank_fx", "sepa", "zen"}:
                bank_only = tuple(
                    rail
                    for rail in candidate_rails
                    if rail.lower() == "bank_fx"
                    and self._fx_rail_supported_for_pair(
                        rail=rail,
                        source_fiat=source_fiat,
                        target_fiat=target_fiat,
                    )
                )
                if bank_only:
                    return bank_only
        if normalized_sell.startswith("revolut_"):
            preferred = tuple(rail for rail in candidate_rails if rail.lower().startswith("revolut_"))
            return preferred or candidate_rails
        if normalized_sell.startswith("wise_"):
            preferred = tuple(rail for rail in candidate_rails if rail.lower().startswith("wise_"))
            return preferred or candidate_rails
        if normalized_sell in {"bank_transfer", "blik", "bank_fx", "sepa", "zen"}:
            bank_only = tuple(
                rail
                for rail in candidate_rails
                if rail.lower() == "bank_fx"
                and self._fx_rail_supported_for_pair(
                    rail=rail,
                    source_fiat=source_fiat,
                    target_fiat=target_fiat,
                )
            )
            return bank_only
        return candidate_rails

    def _fx_rail_supported_for_pair(
        self,
        *,
        rail: str,
        source_fiat: str,
        target_fiat: str,
    ) -> bool:
        normalized = (rail or "").lower()
        if normalized != "bank_fx":
            return True
        supported = {fiat.upper() for fiat in self.settings.bank_fx_fiats}
        return source_fiat.upper() in supported and target_fiat.upper() in supported

    def _fx_safety_fee_usd(self, rail: str, amount_usd: Decimal) -> Decimal:
        normalized = (rail or "").lower()
        extra = Decimal("0")
        if normalized.startswith("revolut_"):
            extra += self._revolut_standard_fee_usd(amount_usd)
        if normalized.startswith("wise_"):
            extra += amount_usd * self.settings.wise_estimated_fee_safety_pct / Decimal("100")
        return extra

    def _revolut_standard_fee_usd(self, amount_usd: Decimal) -> Decimal:
        plan = self.settings.revolut_plan.lower()
        if plan == "standard":
            fee_pct = Decimal("0")
            if self.settings.revolut_assume_allowance_used:
                fee_pct += self.settings.revolut_standard_fair_usage_pct
            if self._is_revolut_weekend_window():
                fee_pct += self.settings.revolut_standard_weekend_pct
            return amount_usd * fee_pct / Decimal("100")
        if plan == "plus":
            fee_pct = Decimal("0")
            if self.settings.revolut_assume_allowance_used:
                fee_pct += Decimal("0.5")
            if self._is_revolut_weekend_window():
                fee_pct += Decimal("0.5")
            return amount_usd * fee_pct / Decimal("100")
        if plan in {"premium", "metal", "ultra"}:
            return Decimal("0")
        return Decimal("0")

    @staticmethod
    def _is_revolut_weekend_window(now_utc: datetime | None = None) -> bool:
        now = now_utc or utc_now()
        et = now.astimezone(ZoneInfo("America/New_York"))
        weekday = et.weekday()
        minutes = et.hour * 60 + et.minute
        if weekday == 4 and minutes >= 17 * 60:
            return True
        if weekday == 5:
            return True
        if weekday == 6 and minutes < 18 * 60:
            return True
        return False

    @staticmethod
    def _best_per_route(opportunities: list[ArbitrageOpportunity]) -> list[ArbitrageOpportunity]:
        seen_routes: set[str] = set()
        unique: list[ArbitrageOpportunity] = []
        for opportunity in opportunities:
            route_key = opportunity.alert_route_key
            if route_key in seen_routes:
                continue
            seen_routes.add(route_key)
            unique.append(opportunity)
        return unique

    @staticmethod
    def _best_per_first_leg(opportunities: list[ArbitrageOpportunity]) -> list[ArbitrageOpportunity]:
        seen_first_legs: set[str] = set()
        unique: list[ArbitrageOpportunity] = []
        for opportunity in opportunities:
            first_leg_key = Analyzer._first_leg_anchor_key(opportunity)
            if first_leg_key in seen_first_legs:
                continue
            seen_first_legs.add(first_leg_key)
            unique.append(opportunity)
        return unique

    @staticmethod
    def _first_leg_anchor_key(opportunity: ArbitrageOpportunity) -> str:
        order = opportunity.sell_order
        raw = order.raw or {}
        components = raw.get("components") if isinstance(raw, dict) else None
        if isinstance(components, list) and components:
            component_ids = ",".join(
                str((component or {}).get("order_id") or "")
                for component in components
            )
        else:
            component_ids = str(order.order_id or "")
        return "|".join(
            [
                str(order.platform).lower(),
                str(order.side).lower(),
                str(order.fiat).upper(),
                f"{order.price:.4f}",
                component_ids,
                opportunity.sell_rail or "",
            ]
        )

    def _rail_fee_usd(
        self,
        rail: str | None,
        amount_usd: Decimal,
        *,
        leg: str,
        primary_fiat: str | None = None,
        primary_price: Decimal | None = None,
        secondary_fiat: str | None = None,
        secondary_price: Decimal | None = None,
    ) -> Decimal:
        profile = self.settings.rail_fee_profile(rail)
        if leg == "send":
            send_pct = profile.send_pct
            if rail == "revolut_card_manual" and primary_fiat:
                send_pct = self.settings.revolut_card_manual_send_pct_by_fiat.get(
                    primary_fiat.upper(),
                    send_pct,
                )
            if rail == "wise_bank_transfer" and primary_fiat:
                send_pct = self.settings.wise_bank_transfer_send_pct_by_fiat.get(
                    primary_fiat.upper(),
                    send_pct,
                )
            pct_fee = (amount_usd * send_pct / Decimal("100"))
            fixed_fee = self._fixed_fee_usd(
                fixed_usd=profile.send_fixed_usd,
                fixed_amount=profile.send_fixed_amount,
                fixed_ccy=profile.send_fixed_ccy,
                primary_fiat=primary_fiat,
                primary_price=primary_price,
                secondary_fiat=secondary_fiat,
                secondary_price=secondary_price,
            )
            return pct_fee + fixed_fee
        if leg == "receive":
            pct_fee = (amount_usd * profile.receive_pct / Decimal("100"))
            fixed_fee = self._fixed_fee_usd(
                fixed_usd=profile.receive_fixed_usd,
                fixed_amount=profile.receive_fixed_amount,
                fixed_ccy=profile.receive_fixed_ccy,
                primary_fiat=primary_fiat,
                primary_price=primary_price,
                secondary_fiat=secondary_fiat,
                secondary_price=secondary_price,
            )
            return pct_fee + fixed_fee
        if leg == "fx":
            pct_fee = (amount_usd * profile.fx_pct / Decimal("100"))
            fixed_fee = self._fixed_fee_usd(
                fixed_usd=profile.fx_fixed_usd,
                fixed_amount=profile.fx_fixed_amount,
                fixed_ccy=profile.fx_fixed_ccy,
                primary_fiat=primary_fiat,
                primary_price=primary_price,
                secondary_fiat=secondary_fiat,
                secondary_price=secondary_price,
            )
            return pct_fee + fixed_fee
        return Decimal("0")

    def _fixed_fee_usd(
        self,
        *,
        fixed_usd: Decimal,
        fixed_amount: Decimal,
        fixed_ccy: str,
        primary_fiat: str | None,
        primary_price: Decimal | None,
        secondary_fiat: str | None,
        secondary_price: Decimal | None,
    ) -> Decimal:
        if fixed_amount > 0:
            currency = (fixed_ccy or "").upper()
            if currency in {"", "USD", "USDT", self.settings.base_asset.upper()}:
                return fixed_amount
            if primary_fiat and currency == primary_fiat.upper() and primary_price and primary_price > 0:
                return fixed_amount / primary_price
            if secondary_fiat and currency == secondary_fiat.upper() and secondary_price and secondary_price > 0:
                return fixed_amount / secondary_price
            return fixed_amount
        return fixed_usd

    async def _should_alert(self, opportunity: ArbitrageOpportunity) -> bool:
        now = utc_now()
        if self.signal_journal_repository is not None:
            has_open_route = await self.signal_journal_repository.has_open_route(
                route=opportunity.note,
                buy_platform=opportunity.buy_order.platform,
                sell_platform=opportunity.sell_order.platform,
                buy_fiat=opportunity.buy_order.fiat,
                sell_fiat=opportunity.sell_order.fiat,
            )
            if has_open_route:
                return False
        expire_before = now - timedelta(seconds=self.settings.alert_route_cooldown_sec)
        self._recent_alerts = {
            key: alert_state
            for key, alert_state in self._recent_alerts.items()
            if alert_state[0] >= expire_before
        }
        alert_key = opportunity.alert_route_key
        previous = self._recent_alerts.get(alert_key)
        if previous is not None:
            last_sent_at, last_profit = previous
            within_cooldown = (now - last_sent_at) < timedelta(
                seconds=self.settings.alert_route_cooldown_sec
            )
            if within_cooldown:
                if last_profit <= 0:
                    return False
                profit_gain_pct = (
                    ((opportunity.estimated_profit_usd - last_profit) / last_profit) * Decimal("100")
                )
                if (
                    opportunity.estimated_profit_usd <= last_profit
                    or profit_gain_pct < self.settings.alert_route_min_profit_improvement_pct
                ):
                    return False
        self._recent_alerts[alert_key] = (now, opportunity.estimated_profit_usd)
        return True

    async def _reconcile_open_routes(self, opportunities: list[ArbitrageOpportunity]) -> None:
        if self.signal_journal_repository is None:
            return
        active_routes = {
            (
                opportunity.note,
                opportunity.buy_order.platform,
                opportunity.sell_order.platform,
                opportunity.buy_order.fiat,
                opportunity.sell_order.fiat,
            )
            for opportunity in opportunities
        }
        await self.signal_journal_repository.expire_open_routes_except(
            active_routes,
            stale_after_sec=max(
                self.settings.scan_interval_sec * 3,
                self.settings.opportunity_ttl_sec,
                self.settings.alert_route_cooldown_sec,
                600,
            ),
        )

    def _is_alert_candidate(self, opportunity: ArbitrageOpportunity) -> bool:
        return self._is_alert_candidate_prequote(opportunity) and not self._is_outperformed_by_internal_route(opportunity)

    def _is_alert_candidate_prequote(self, opportunity: ArbitrageOpportunity) -> bool:
        min_spread, min_profit = self.settings.alert_thresholds_for(
            opportunity.buy_order.fiat,
            opportunity.buy_order.asset,
        )
        min_profit = self._min_profit_for_route(opportunity, min_profit)
        return (
            self._is_settlement_path_allowed(opportunity)
            and self._is_route_chain_coherent(opportunity)
            and opportunity.volume_usdt >= self.settings.min_alert_volume_usdt
            and
            opportunity.rail_status == "confirmed"
            and self._liquidity_allows_alert(opportunity)
            and opportunity.spread_pct >= min_spread
            and opportunity.estimated_profit_usd >= min_profit
        )

    def _is_route_chain_coherent(self, opportunity: ArbitrageOpportunity) -> bool:
        coherent, _ = self._opportunity_chain_sane(opportunity)
        return coherent

    def _liquidity_allows_alert(self, opportunity: ArbitrageOpportunity) -> bool:
        if opportunity.liquidity_status == "liquid":
            return True
        return self.settings.alert_allow_tradable_liquidity and opportunity.liquidity_status == "tradable"

    def _min_profit_for_route(self, opportunity: ArbitrageOpportunity, base_min_profit: Decimal) -> Decimal:
        if self._is_prefunded_route(opportunity):
            return max(base_min_profit, Decimal("400"))
        if self._route_speed(opportunity) == "slow":
            return max(base_min_profit, Decimal("250"))
        return base_min_profit

    def _route_speed(self, opportunity: ArbitrageOpportunity) -> str:
        receive_method = self._route_method_text(
            opportunity.sell_user_method,
            opportunity.sell_payment_summary,
            opportunity.sell_order.payment_methods,
        )
        send_method = self._route_method_text(
            opportunity.buy_user_method,
            opportunity.buy_payment_summary,
            opportunity.buy_order.payment_methods,
        )
        sell_bucket, fx_bucket, buy_bucket = self._route_buckets(opportunity)
        prefunded = self._is_prefunded_route(opportunity)
        receive_currency = opportunity.sell_order.fiat.upper()
        send_currency = opportunity.buy_order.fiat.upper()
        revolut_local = {fiat.upper() for fiat in self.settings.revolut_bank_transfer_fiats}
        wise_receive_local = {fiat.upper() for fiat in self.settings.wise_receive_bank_fiats}
        wise_send_slow = {fiat.upper() for fiat in self.settings.wise_bank_transfer_slow_fiats}
        wise_send_local = {
            fiat.upper()
            for fiat in self.settings.wise_bank_transfer_fiats
            if fiat.upper() not in wise_send_slow
        }
        receive_fast_currencies = revolut_local | wise_receive_local
        send_fast_currencies = revolut_local | wise_send_local
        explicit_revolut_receive = "revolut" in receive_method
        explicit_wise_receive = "wise" in receive_method
        explicit_revolut_send = "revolut" in send_method
        explicit_wise_send = "wise" in send_method
        receive_mismatch = self._method_currency_mismatch(receive_method, receive_currency)
        send_mismatch = self._method_currency_mismatch(send_method, send_currency)
        has_ladder = self._has_multi_merchant_leg(opportunity)
        blik_manual = self._is_blik_manual_route(opportunity, sell_bucket=sell_bucket, fx_bucket=fx_bucket)
        zen_manual = self._is_zen_manual_route(opportunity, sell_bucket=sell_bucket, fx_bucket=fx_bucket)
        revolut_card_manual = opportunity.buy_rail == "revolut_card_manual"

        if prefunded:
            return "slow"
        if (
            not has_ladder
            and not revolut_card_manual
            and
            sell_bucket == "revolut"
            and buy_bucket == "revolut"
            and (not fx_bucket or fx_bucket == "revolut")
            and explicit_revolut_receive
            and explicit_revolut_send
        ):
            return "instant"
        if (
            not has_ladder
            and
            sell_bucket == "wise"
            and buy_bucket == "wise"
            and (not fx_bucket or fx_bucket == "wise")
            and explicit_wise_receive
            and explicit_wise_send
        ):
            return "instant"
        if opportunity.buy_rail == "wise_bank_transfer" and send_currency in wise_send_slow:
            return "slow"
        if (
            not has_ladder
            and opportunity.sell_rail == "blik"
            and opportunity.buy_rail == "blik"
            and sell_bucket == "bank"
            and buy_bucket == "bank"
        ):
            return "instant"
        if not has_ladder and any(keyword in receive_method for keyword in ("pko", "bank polski")):
            return "instant"
        if not has_ladder and "sepa instant" in receive_method:
            return "instant"
        if "swift" in receive_method or "swift" in send_method or receive_mismatch or send_mismatch:
            return "slow"

        receive_fast = (
            receive_currency in receive_fast_currencies
            or any(keyword in receive_method for keyword in ("sepa", "interac", "rbc", "td bank", "otp", "zen"))
        )
        send_fast = send_currency in send_fast_currencies or blik_manual or zen_manual or revolut_card_manual
        if receive_fast and send_fast:
            return "fast"
        if receive_currency not in receive_fast_currencies and not (explicit_revolut_receive or explicit_wise_receive):
            return "slow"
        if (
            send_currency not in send_fast_currencies
            and not (explicit_revolut_send or explicit_wise_send)
            and not blik_manual
            and not zen_manual
            and not revolut_card_manual
        ):
            return "slow"
        return "fast"

    @staticmethod
    def _is_blik_manual_route(
        opportunity: ArbitrageOpportunity,
        *,
        sell_bucket: str,
        fx_bucket: str,
    ) -> bool:
        source_bucket = fx_bucket or sell_bucket
        return (
            opportunity.buy_rail == "blik"
            and opportunity.buy_order.fiat.upper() == "PLN"
            and source_bucket in {"revolut", "wise", "bank"}
        )

    @staticmethod
    def _is_zen_manual_route(
        opportunity: ArbitrageOpportunity,
        *,
        sell_bucket: str,
        fx_bucket: str,
    ) -> bool:
        source_bucket = fx_bucket or sell_bucket
        return (
            opportunity.buy_rail == "zen"
            and opportunity.buy_order.fiat.upper() == "PLN"
            and source_bucket in {"revolut", "wise", "bank"}
        )

    def _manual_execution_hint(
        self,
        *,
        buy_rail: str,
        buy_order: P2POrder,
        sell_bucket: str,
        fx_bucket: str,
    ) -> str:
        source_bucket = fx_bucket or sell_bucket
        if buy_rail == "blik" and buy_order.fiat.upper() == "PLN" and source_bucket in {"revolut", "wise"}:
            source_label = "Revolut" if source_bucket == "revolut" else "Wise"
            return f"{source_label} -> PKO -> BLIK (2 шага)"
        if buy_rail == "zen" and buy_order.fiat.upper() == "PLN" and source_bucket in {"revolut", "wise"}:
            source_label = "Revolut" if source_bucket == "revolut" else "Wise"
            return f"{source_label} -> польский банк / ZEN"
        if buy_rail == "revolut_card_manual" and buy_order.fiat.upper() in {"RON", "AUD", "CAD", "SGD", "CZK", "SEK", "NOK", "DKK"}:
            return (
                f"Открой ордер и проверь номер карты; "
                f"Revolut -> карта контрагента ({buy_order.fiat.upper()}, комиссия учтена)"
            )
        return ""

    @staticmethod
    def _has_multi_merchant_leg(opportunity: ArbitrageOpportunity) -> bool:
        for order in (opportunity.buy_order, opportunity.sell_order):
            raw = order.raw or {}
            components = raw.get("components") if isinstance(raw, dict) else None
            if isinstance(components, list) and len(components) >= 2:
                return True
        return False

    @staticmethod
    def _route_method_text(user_method: str, payment_summary: str, payment_methods: tuple[str, ...] | list[str]) -> str:
        parts = [user_method or "", payment_summary or "", *[str(item) for item in payment_methods]]
        return " | ".join(part.strip().lower() for part in parts if str(part).strip())

    @staticmethod
    def _method_currency_mismatch(method_text: str, currency: str) -> bool:
        hint = Analyzer._method_country_hint(method_text)
        if hint is None:
            return False
        country_currency = {
            "gb": "GBP",
            "ca": "CAD",
            "au": "AUD",
            "nz": "NZD",
            "sg": "SGD",
            "ph": "PHP",
            "us": "USD",
            "hu": "HUF",
            "ro": "RON",
            "pl": "PLN",
        }.get(hint)
        return bool(country_currency and currency.upper() != country_currency)

    @staticmethod
    def _method_country_hint(method_text: str) -> str | None:
        text = (method_text or "").lower()
        hints = (
            ("gb", ("faster payments", "uk bank transfer", "sort code", "barclays", "lloyds", "natwest", "hsbc", "halifax", "monzo", "starling")),
            ("ca", ("interac", "rbc", "royal bank", "td bank", "scotiabank", "cibc", "bmo")),
            ("au", ("bsb", "payid", "commonwealth", "westpac", "nab", "australia and new zealand")),
            ("nz", ("kiwibank", "anz nz", "asb", "bnz", "new zealand")),
            ("sg", ("fast", "paynow", "dbs", "ocbc", "uob", "singapore")),
            ("ph", ("instapay", "pesonet", "wise pilipinas", "gcash", "maya", "bpi", "bdo", "philippines")),
            ("us", ("ach", "wire", "zelle", "routing", "aba", "chase", "bofa", "bank of america", "wells fargo", "citibank")),
            ("hu", ("azonnali", "otp bank", "otp", "k&h", "raiffeisen hu")),
            ("ro", ("brd", "bcr", "bank transilvania", "bt pay", "btpay", "raiffeisen bank aval")),
            ("pl", ("pko", "blik", "zen", "santander poland", "millennium", "mbank", "ing poland")),
        )
        for country, keywords in hints:
            if any(keyword in text for keyword in keywords):
                return country
        return None

    @staticmethod
    def _is_outperformed_by_internal_route(opportunity: ArbitrageOpportunity) -> bool:
        if (opportunity.base_type or opportunity.type) != "cross_currency":
            return False
        if not opportunity.internal_quote_exact:
            return False
        return opportunity.internal_quote_advantage_usdt > 0

    def _is_settlement_path_allowed(self, opportunity: ArbitrageOpportunity) -> bool:
        if self.settings.allow_external_settlement_signals:
            return True
        if opportunity.type != "cross_currency":
            return True
        if not opportunity.sell_rail or not opportunity.buy_rail:
            return False
        if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper() and not opportunity.fx_rail:
            return False
        return True

    async def _build_liquidity_context(
        self,
        grouped: dict[tuple[str, str, str], list[P2POrder]],
    ) -> dict[str, object]:
        current: dict[tuple[str, str, str, str], dict[str, Decimal | int]] = {}
        for (platform, fiat, side), orders in grouped.items():
            safe_orders = self.risk_guard.filter_orders(list(orders))
            top_orders = sorted(safe_orders, key=lambda item: item.available, reverse=True)[:5]
            current[(platform, fiat, self.settings.base_asset, side)] = {
                "quote_count": len(safe_orders),
                "merchant_count": len({order.merchant_id for order in safe_orders if order.merchant_id}),
                "depth_top5_usdt": sum((order.available for order in top_orders), start=Decimal("0")),
            }
        loop_now = asyncio.get_running_loop().time()
        if self._liquidity_history_cache is not None and loop_now < self._liquidity_history_cache_expires_at:
            history = self._liquidity_history_cache
        else:
            history = await self.raw_order_repository.recent_activity(
                (1, 3),
                asset=self.settings.base_asset,
                fiats=self.settings.fiats,
            )
            self._liquidity_history_cache = history
            self._liquidity_history_cache_expires_at = loop_now + self.settings.liquidity_history_cache_ttl_sec
        return {"current": current, "history": history}

    def _route_liquidity(
        self,
        *,
        buy_order: P2POrder,
        sell_order: P2POrder,
        volume_usdt: Decimal,
        liquidity_context: dict[str, object],
    ) -> tuple[Decimal, str, str]:
        current = liquidity_context.get("current", {})
        history = liquidity_context.get("history", {})

        buy_key = (buy_order.platform, buy_order.fiat, buy_order.asset, buy_order.side)
        sell_key = (sell_order.platform, sell_order.fiat, sell_order.asset, sell_order.side)

        buy_current = current.get(buy_key, {})
        sell_current = current.get(sell_key, {})
        buy_depth = Decimal(str(buy_current.get("depth_top5_usdt", 0)))
        sell_depth = Decimal(str(sell_current.get("depth_top5_usdt", 0)))
        min_depth = min(buy_depth, sell_depth)
        quote_floor = min(
            int(buy_current.get("quote_count", 0)),
            int(sell_current.get("quote_count", 0)),
        )
        merchant_floor = min(
            int(buy_current.get("merchant_count", 0)),
            int(sell_current.get("merchant_count", 0)),
        )

        def hist_metric(hours: int, key: tuple[str, str, str, str], metric: str) -> int:
            window = history.get(hours, {})
            data = window.get(key, {})
            return int(float(data.get(metric, 0)))

        ads_1h = min(hist_metric(1, buy_key, "unique_ads"), hist_metric(1, sell_key, "unique_ads"))
        ads_3h = min(hist_metric(3, buy_key, "unique_ads"), hist_metric(3, sell_key, "unique_ads"))
        merchants_3h = min(
            hist_metric(3, buy_key, "unique_merchants"),
            hist_metric(3, sell_key, "unique_merchants"),
        )

        required_depth = max(volume_usdt * Decimal("2"), Decimal("1000"))
        depth_score = min(min_depth / required_depth, Decimal("1")) * Decimal("50")
        quotes_score = min(Decimal(quote_floor) / Decimal("5"), Decimal("1")) * Decimal("20")
        ads_score = min(Decimal(ads_3h) / Decimal("20"), Decimal("1")) * Decimal("15")
        merchants_score = min(Decimal(merchants_3h) / Decimal("8"), Decimal("1")) * Decimal("15")
        score = (depth_score + quotes_score + ads_score + merchants_score).quantize(Decimal("0.01"))

        if min_depth >= volume_usdt * Decimal("2") and quote_floor >= 3 and ads_3h >= 6 and merchants_3h >= 3:
            status = "liquid"
        elif min_depth >= volume_usdt * Decimal("1.2") and quote_floor >= 2 and ads_1h >= 2:
            status = "tradable"
        else:
            status = "illiquid"

        summary = (
            f"{status} | depth≈{min_depth.quantize(Decimal('0.1'))} {self.settings.base_asset} | "
            f"ads 1h/3h {ads_1h}/{ads_3h}"
        )
        return score, status, summary

    @staticmethod
    def _spread_pct(sell_price: Decimal, buy_price: Decimal) -> Decimal:
        if buy_price <= 0:
            return Decimal("0")
        return ((sell_price - buy_price) / buy_price) * Decimal("100")
