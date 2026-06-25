from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from p2p_bot.config import BinanceConfig, BingXConfig, Settings
from p2p_bot.models.opportunity import ArbitrageOpportunity
from p2p_bot.models.order import P2POrder
from p2p_bot.modules.analyzer import Analyzer, opportunity_sort_key
from p2p_bot.modules.risk_guard import PaymentProfile, RiskGuard
from p2p_bot.state import AppState
from p2p_bot.utils.binance_p2p import BinanceP2PClient
from p2p_bot.utils.bingx_p2p import BingXP2PClient
from p2p_bot.utils.internal_quotes import InternalQuote, InternalQuoteClient


def _settings() -> Settings:
    return Settings.from_env(Path.cwd())


@contextmanager
def _event_loop():
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        yield loop
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def _order(
    *,
    platform: str = "bybit",
    side: str,
    fiat: str = "EUR",
    price: str = "1.00",
    order_id: str | None = None,
) -> P2POrder:
    return P2POrder(
        platform=platform,
        order_id=order_id or f"{platform}-{side}-{fiat}",
        side=side,
        asset="USDT",
        fiat=fiat,
        price=Decimal(price),
        min_amount=Decimal("10"),
        max_amount=Decimal("10000"),
        available=Decimal("10000"),
        payment_methods=["Revolut"],
        merchant_id="merchant-1",
        merchant_rating=99.0,
        merchant_orders=1000,
        merchant_days=365,
        merchant_online=True,
        merchant_kyc=True,
        raw={},
    )


def _opportunity(
    *,
    buy_order: P2POrder,
    sell_order: P2POrder,
) -> ArbitrageOpportunity:
    now = datetime.utcnow()
    return ArbitrageOpportunity(
        type="cross_currency",
        base_type="cross_currency",
        spread_pct=Decimal("2.00"),
        estimated_profit_usd=Decimal("20.00"),
        gross_profit_usd=Decimal("20.00"),
        total_fees_usd=Decimal("0"),
        buy_fee_usd=Decimal("0"),
        sell_fee_usd=Decimal("0"),
        fx_fee_usd=Decimal("0"),
        volume_usdt=Decimal("1000"),
        buy_order=buy_order,
        sell_order=sell_order,
        detected_at=now,
        expires_at=now,
        rail_status="confirmed",
        buy_rail="revolut_balance",
        sell_rail="revolut_balance",
        fx_rail="revolut_balance",
        buy_user_method="Revolut",
        sell_user_method="Revolut",
        buy_payment_summary="Revolut",
        sell_payment_summary="Revolut",
    )


class _DummyForexClient:
    async def get_rate_matrix(
        self,
        fiats: tuple[str, ...],
        force: bool = False,
        *,
        base: str = "EUR",
    ) -> dict[tuple[str, str], Decimal]:
        return {
            ("EUR", "EUR"): Decimal("1"),
            ("PLN", "PLN"): Decimal("1"),
            ("EUR", "PLN"): Decimal("4.200000"),
            ("PLN", "EUR"): Decimal("0.238095"),
        }


class _DummyInternalQuoteClient:
    pass


def test_binance_sell_side_maps_to_buy_trade_type(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_request_json(
        session,
        method: str,
        url: str,
        *,
        json_body=None,
        logger=None,
        **kwargs,
    ) -> dict[str, object]:
        captured["method"] = method
        captured["url"] = url
        captured["json_body"] = json_body
        return {"data": []}

    monkeypatch.setattr("p2p_bot.utils.binance_p2p.request_json", fake_request_json)
    with _event_loop() as loop:
        client = BinanceP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BinanceConfig(
                api_key=None,
                api_secret=None,
                base_url="https://p2p.binance.com",
                adv_search_path="/bapi/c2c/v2/friendly/c2c/adv/search",
                rows=20,
            ),
            logger=logging.getLogger("test.binance"),
        )
        loop.run_until_complete(client.fetch_orders("USDT", "EUR", "sell"))

    assert captured["method"] == "POST"
    assert captured["url"] == "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    assert captured["json_body"]["tradeType"] == "SELL"


def test_binance_buy_side_maps_to_sell_trade_type(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_request_json(
        session,
        method: str,
        url: str,
        *,
        json_body=None,
        logger=None,
        **kwargs,
    ) -> dict[str, object]:
        captured["json_body"] = json_body
        return {"data": []}

    monkeypatch.setattr("p2p_bot.utils.binance_p2p.request_json", fake_request_json)
    with _event_loop() as loop:
        client = BinanceP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BinanceConfig(
                api_key=None,
                api_secret=None,
                base_url="https://p2p.binance.com",
                adv_search_path="/bapi/c2c/v2/friendly/c2c/adv/search",
                rows=20,
            ),
            logger=logging.getLogger("test.binance"),
        )
        loop.run_until_complete(client.fetch_orders("USDT", "EUR", "buy"))

    assert captured["json_body"]["tradeType"] == "BUY"


def test_binance_fetch_orders_uses_response_trade_type_for_canonical_side(monkeypatch) -> None:
    async def fake_request_json(
        session,
        method: str,
        url: str,
        *,
        json_body=None,
        logger=None,
        **kwargs,
    ) -> dict[str, object]:
        return {
            "data": [
                {
                    "adv": {
                        "advNo": "adv-1",
                        "asset": "USDT",
                        "fiatUnit": "EUR",
                        "tradeType": "BUY",
                        "price": "1.50",
                        "minSingleTransAmount": "10",
                        "dynamicMaxSingleTransAmount": "1000",
                        "surplusAmount": "500",
                        "tradeMethods": [],
                    },
                    "advertiser": {},
                }
            ]
        }

    monkeypatch.setattr("p2p_bot.utils.binance_p2p.request_json", fake_request_json)
    with _event_loop() as loop:
        client = BinanceP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BinanceConfig(
                api_key=None,
                api_secret=None,
                base_url="https://p2p.binance.com",
                adv_search_path="/bapi/c2c/v2/friendly/c2c/adv/search",
                rows=20,
            ),
            logger=logging.getLogger("test.binance"),
        )
        orders = loop.run_until_complete(client.fetch_orders("USDT", "EUR", "sell"))

    assert [order.side for order in orders] == ["sell"]


def test_bingx_request_type_matches_canonical_side() -> None:
    with _event_loop():
        client = BingXP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BingXConfig(
                api_key=None,
                api_secret=None,
                base_url="https://open-api.bingx.com",
                p2p_api_base="https://api-base.bingx.com/api",
                fiat_site_url="https://paycat.com",
                supported_payment_methods=("Revolut",),
                supported_fiats=("EUR",),
            ),
            logger=logging.getLogger("test.bingx"),
        )

        assert client._request_type_for_side("sell") == 2
        assert client._request_type_for_side("buy") == 1


def test_bingx_fetch_orders_uses_response_type_for_canonical_side(monkeypatch) -> None:
    async def fake_request_p2p(self, method, path, *, payload=None, params=None):
        return {
            "code": 0,
            "data": {
                "result": [
                    {
                        "advertNo": "adv-1",
                        "type": 1,
                        "asset": "USDT",
                        "fiat": "EUR",
                        "price": "1.55",
                        "minAmount": "10",
                        "maxAmount": "1000",
                        "availableNumber": "500",
                        "paymentMethodList": [],
                        "merchantInfo": {},
                        "merchantStat": {},
                    }
                ]
            },
        }

    monkeypatch.setattr(BingXP2PClient, "_request_p2p", fake_request_p2p)
    with _event_loop() as loop:
        client = BingXP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BingXConfig(
                api_key=None,
                api_secret=None,
                base_url="https://open-api.bingx.com",
                p2p_api_base="https://api-base.bingx.com/api",
                fiat_site_url="https://paycat.com",
                supported_payment_methods=("Revolut",),
                supported_fiats=("EUR",),
            ),
            logger=logging.getLogger("test.bingx"),
        )
        orders = loop.run_until_complete(client.fetch_orders("USDT", "EUR", "sell"))

    assert [order.side for order in orders] == ["sell"]


def test_risk_guard_sell_side_uses_receive_tag(monkeypatch) -> None:
    guard = RiskGuard(_settings(), AppState())
    order = _order(side="sell")
    profile = PaymentProfile(tags=("revolut_balance",), known_methods=("Revolut",), unknown_methods=())
    called: list[str] = []

    def fake_receive(*args, **kwargs) -> str:
        called.append("receive")
        return "receive"

    def fake_send(*args, **kwargs) -> str:
        called.append("send")
        return "send"

    monkeypatch.setattr(guard, "_primary_receive_tag", fake_receive)
    monkeypatch.setattr(guard, "_primary_send_tag", fake_send)

    assert guard._primary_order_tag(order, profile) == "receive"
    assert called == ["receive"]


def test_risk_guard_buy_side_uses_send_tag(monkeypatch) -> None:
    guard = RiskGuard(_settings(), AppState())
    order = _order(side="buy")
    profile = PaymentProfile(tags=("revolut_balance",), known_methods=("Revolut",), unknown_methods=())
    called: list[str] = []

    def fake_receive(*args, **kwargs) -> str:
        called.append("receive")
        return "receive"

    def fake_send(*args, **kwargs) -> str:
        called.append("send")
        return "send"

    monkeypatch.setattr(guard, "_primary_receive_tag", fake_receive)
    monkeypatch.setattr(guard, "_primary_send_tag", fake_send)

    assert guard._primary_order_tag(order, profile) == "send"
    assert called == ["send"]


def test_route_flow_metrics_use_direct_fx_rate() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        sell_order = _order(side="sell", fiat="PLN", price="4.00")
        buy_order = _order(side="buy", fiat="GBP", price="0.80")

        sell_fiat_amount, rebuy_fiat_amount, gross_return_usdt, _ = analyzer._route_flow_metrics(
            volume_usdt=Decimal("1000"),
            buy_order=buy_order,
            sell_order=sell_order,
            fx_rate=Decimal("0.20"),
        )

    assert sell_fiat_amount == Decimal("4000.00")
    assert rebuy_fiat_amount == Decimal("800.00")
    assert gross_return_usdt == Decimal("1000.0000")


def test_max_route_volume_applies_execution_headroom_buffers() -> None:
    settings = _settings()
    settings.route_min_amount_buffer_pct = Decimal("5")
    settings.route_max_amount_headroom_pct = Decimal("10")
    settings.route_available_headroom_pct = Decimal("20")
    settings.counterparty_filter.max_single_trade_usd = Decimal("10000")
    settings.prefunded_mode = False
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        sell_order = _order(side="sell", fiat="EUR", price="1.00", order_id="sell-buffered")
        sell_order.min_amount = Decimal("100")
        sell_order.max_amount = Decimal("1000")
        sell_order.available = Decimal("900")
        buy_order = _order(side="buy", fiat="EUR", price="1.00", order_id="buy-buffered")
        buy_order.min_amount = Decimal("100")
        buy_order.max_amount = Decimal("1200")
        buy_order.available = Decimal("800")

        volume = analyzer._max_route_volume_usdt(
            buy_order=buy_order,
            sell_order=sell_order,
            fx_rate=Decimal("1"),
        )

    assert volume == Decimal("640.0000")


def test_opportunity_flow_sane_rejects_routes_without_limit_headroom() -> None:
    settings = _settings()
    settings.route_min_amount_buffer_pct = Decimal("5")
    settings.route_max_amount_headroom_pct = Decimal("10")
    settings.route_available_headroom_pct = Decimal("15")
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        sell_order = _order(side="sell", fiat="EUR", price="1.00", order_id="sell-tight")
        sell_order.min_amount = Decimal("100")
        sell_order.max_amount = Decimal("1000")
        sell_order.available = Decimal("2000")
        buy_order = _order(side="buy", fiat="EUR", price="1.00", order_id="buy-tight")
        buy_order.min_amount = Decimal("100")
        buy_order.max_amount = Decimal("2000")
        buy_order.available = Decimal("2000")
        opportunity = _opportunity(buy_order=buy_order, sell_order=sell_order)
        opportunity.volume_usdt = Decimal("950")
        opportunity.fx_rate_used = Decimal("1")

        sane, reason = analyzer._opportunity_flow_sane(opportunity)

    assert sane is False
    assert reason == "step 1 exceeds buffered sell max amount: 950.00 > 900.0"


def test_bybit_api_side_codes_match_canonical_contract() -> None:
    from p2p_bot.utils.canonical_side import bybit_api_side_code

    assert bybit_api_side_code("sell") == "1"
    assert bybit_api_side_code("buy") == "0"
    assert isinstance(bybit_api_side_code("sell"), str)
    assert isinstance(bybit_api_side_code("buy"), str)


def test_opportunity_sort_prefers_best_spread_before_estimated_profit() -> None:
    better_price = _opportunity(
        buy_order=_order(side="buy", fiat="EUR", price="0.88", order_id="buy-better"),
        sell_order=_order(side="sell", fiat="EUR", price="1.05", order_id="sell-better"),
    )
    better_price.spread_pct = Decimal("5.00")
    better_price.estimated_profit_usd = Decimal("20.00")

    bigger_volume = _opportunity(
        buy_order=_order(side="buy", fiat="EUR", price="0.89", order_id="buy-bigger"),
        sell_order=_order(side="sell", fiat="EUR", price="1.04", order_id="sell-bigger"),
    )
    bigger_volume.spread_pct = Decimal("4.00")
    bigger_volume.estimated_profit_usd = Decimal("50.00")

    ordered = sorted([bigger_volume, better_price], key=opportunity_sort_key)

    assert ordered[0] is better_price


def test_candidate_orders_prefer_stronger_merchant_when_price_is_equal() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        weaker = _order(side="sell", fiat="EUR", price="1.01", order_id="weak")
        weaker.merchant_rating = 94.0
        weaker.merchant_orders = 60
        weaker.merchant_days = 45
        weaker.merchant_online = None
        weaker.merchant_kyc = None
        weaker.merchant_last_active_minutes = 20

        stronger = _order(side="sell", fiat="EUR", price="1.01", order_id="strong")
        stronger.merchant_rating = 99.8
        stronger.merchant_orders = 1800
        stronger.merchant_days = 720
        stronger.merchant_online = True
        stronger.merchant_kyc = True
        stronger.merchant_last_active_minutes = 1

        ranked = analyzer._candidate_orders([weaker, stronger], prefer_high_price=True)

    assert ranked[0].order_id == "strong"


def test_candidate_orders_apply_merchant_filter_before_best_rate_selection() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        unsafe_better = _order(side="sell", fiat="EUR", price="1.05", order_id="unsafe-better")
        unsafe_better.merchant_rating = 80.0

        safe_worse = _order(side="sell", fiat="EUR", price="1.04", order_id="safe-worse")
        safe_worse.merchant_rating = 99.0

        ranked = analyzer._candidate_orders([unsafe_better, safe_worse], prefer_high_price=True)

    assert [order.order_id for order in ranked] == ["safe-worse"]


def test_candidate_orders_skip_top_rank_and_use_book_window_for_sell() -> None:
    settings = replace(
        _settings(),
        candidate_book_scan_depth=6,
        candidate_book_rank_start=2,
        candidate_book_rank_end=4,
        max_candidates_per_side=3,
    )
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        orders = [
            _order(side="sell", fiat="EUR", price="1.05", order_id="rank-1"),
            _order(side="sell", fiat="EUR", price="1.04", order_id="rank-2"),
            _order(side="sell", fiat="EUR", price="1.03", order_id="rank-3"),
            _order(side="sell", fiat="EUR", price="1.02", order_id="rank-4"),
            _order(side="sell", fiat="EUR", price="1.01", order_id="rank-5"),
        ]

        ranked = analyzer._candidate_orders(orders, prefer_high_price=True)

    assert [order.order_id for order in ranked[:3]] == ["rank-2", "rank-3", "rank-4"]


def test_candidate_orders_preserve_raw_book_ranks_before_filters() -> None:
    settings = replace(
        _settings(),
        candidate_book_scan_depth=6,
        candidate_book_rank_start=2,
        candidate_book_rank_end=4,
        max_candidates_per_side=3,
    )
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        rank_1 = _order(side="sell", fiat="EUR", price="1.05", order_id="rank-1")
        rank_2_unsafe = _order(side="sell", fiat="EUR", price="1.04", order_id="rank-2-unsafe")
        rank_2_unsafe.merchant_rating = 80.0
        rank_3_unsafe = _order(side="sell", fiat="EUR", price="1.03", order_id="rank-3-unsafe")
        rank_3_unsafe.merchant_rating = 80.0
        rank_4_safe = _order(side="sell", fiat="EUR", price="1.02", order_id="rank-4-safe")
        rank_5_safe = _order(side="sell", fiat="EUR", price="1.01", order_id="rank-5-safe")

        ranked = analyzer._candidate_orders(
            [rank_1, rank_2_unsafe, rank_3_unsafe, rank_4_safe, rank_5_safe],
            prefer_high_price=True,
        )

    assert [order.order_id for order in ranked] == ["rank-4-safe"]


def test_candidate_orders_skip_top_rank_and_use_book_window_for_buy() -> None:
    settings = replace(
        _settings(),
        candidate_book_scan_depth=6,
        candidate_book_rank_start=2,
        candidate_book_rank_end=4,
        max_candidates_per_side=3,
    )
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        orders = [
            _order(side="buy", fiat="EUR", price="0.84", order_id="rank-1"),
            _order(side="buy", fiat="EUR", price="0.85", order_id="rank-2"),
            _order(side="buy", fiat="EUR", price="0.86", order_id="rank-3"),
            _order(side="buy", fiat="EUR", price="0.87", order_id="rank-4"),
            _order(side="buy", fiat="EUR", price="0.88", order_id="rank-5"),
        ]

        ranked = analyzer._candidate_orders(orders, prefer_high_price=False)

    assert [order.order_id for order in ranked[:3]] == ["rank-2", "rank-3", "rank-4"]


def test_candidate_orders_fall_back_when_book_is_shallow() -> None:
    settings = replace(
        _settings(),
        candidate_book_scan_depth=20,
        candidate_book_rank_start=2,
        candidate_book_rank_end=15,
        max_candidates_per_side=5,
    )
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        only_order = _order(side="sell", fiat="EUR", price="1.05", order_id="only")

        ranked = analyzer._candidate_orders([only_order], prefer_high_price=True)

    assert [order.order_id for order in ranked] == ["only"]


def test_candidate_orders_require_recent_or_online_activity() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        stale = _order(side="sell", fiat="EUR", price="1.02", order_id="stale")
        stale.merchant_online = None
        stale.merchant_last_active_minutes = 16

        recent = _order(side="sell", fiat="EUR", price="1.01", order_id="recent")
        recent.merchant_online = None
        recent.merchant_last_active_minutes = 5

        ranked = analyzer._candidate_orders([stale, recent], prefer_high_price=True)

    assert [order.order_id for order in ranked] == ["recent"]


def test_candidate_orders_require_target_volume_liquidity_and_limits() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        low_available = _order(side="sell", fiat="EUR", price="1.02", order_id="low-available")
        low_available.available = Decimal("499")

        low_max = _order(side="sell", fiat="EUR", price="1.02", order_id="low-max")
        low_max.max_amount = Decimal("1200")

        valid = _order(side="sell", fiat="EUR", price="1.01", order_id="valid")

        ranked = analyzer._candidate_orders([low_available, low_max, valid], prefer_high_price=True)

    assert [order.order_id for order in ranked] == ["valid"]


def test_candidate_orders_reject_sell_price_far_above_book_median() -> None:
    settings = replace(
        _settings(),
        candidate_book_scan_depth=5,
        candidate_book_rank_start=1,
        candidate_book_rank_end=5,
        max_candidates_per_side=5,
    )
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        orders = [
            _order(side="sell", fiat="EUR", price="1.20", order_id="phantom"),
            _order(side="sell", fiat="EUR", price="1.03", order_id="rank-2"),
            _order(side="sell", fiat="EUR", price="1.02", order_id="rank-3"),
            _order(side="sell", fiat="EUR", price="1.01", order_id="rank-4"),
            _order(side="sell", fiat="EUR", price="1.00", order_id="rank-5"),
        ]

        ranked = analyzer._candidate_orders(orders, prefer_high_price=True)

    assert "phantom" not in [order.order_id for order in ranked]
    assert ranked[0].order_id == "rank-2"


def test_candidate_orders_reject_buy_price_far_below_book_median() -> None:
    settings = replace(
        _settings(),
        candidate_book_scan_depth=5,
        candidate_book_rank_start=1,
        candidate_book_rank_end=5,
        max_candidates_per_side=5,
    )
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        orders = [
            _order(side="buy", fiat="EUR", price="0.70", order_id="phantom"),
            _order(side="buy", fiat="EUR", price="0.86", order_id="rank-2"),
            _order(side="buy", fiat="EUR", price="0.87", order_id="rank-3"),
            _order(side="buy", fiat="EUR", price="0.88", order_id="rank-4"),
            _order(side="buy", fiat="EUR", price="0.89", order_id="rank-5"),
        ]

        ranked = analyzer._candidate_orders(orders, prefer_high_price=False)

    assert "phantom" not in [order.order_id for order in ranked]
    assert ranked[0].order_id == "rank-2"


def test_find_internal_includes_multi_merchant_ladder_volume() -> None:
    settings = _settings()
    settings.strict_single_merchant_mode = False
    settings.route_min_amount_buffer_pct = Decimal("0")
    settings.route_max_amount_headroom_pct = Decimal("0")
    settings.route_available_headroom_pct = Decimal("0")
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )

        sell_a = _order(side="sell", fiat="EUR", price="1.04", order_id="sell-a")
        sell_a.available = Decimal("600")
        sell_a.merchant_name = "Alice"
        sell_b = _order(side="sell", fiat="EUR", price="1.04", order_id="sell-b")
        sell_b.available = Decimal("600")
        sell_b.merchant_name = "Bob"
        buy = _order(side="buy", fiat="EUR", price="1.00", order_id="buy-a")
        buy.available = Decimal("5000")

        opportunities = analyzer._find_internal(
            {
                ("bybit", "EUR", "sell"): [sell_a, sell_b],
                ("bybit", "EUR", "buy"): [buy],
            },
            liquidity_context={},
        )

    assert opportunities
    assert max(opportunity.volume_usdt for opportunity in opportunities) == Decimal("1200.0000")
    assert any(
        isinstance(opportunity.sell_order.raw.get("components"), list)
        and len(opportunity.sell_order.raw["components"]) == 2
        and {component.get("merchant_name") for component in opportunity.sell_order.raw["components"]} == {"Alice", "Bob"}
        for opportunity in opportunities
    )


def test_find_internal_skips_multi_merchant_ladder_in_strict_mode() -> None:
    settings = _settings()
    settings.strict_single_merchant_mode = True
    settings.route_min_amount_buffer_pct = Decimal("0")
    settings.route_max_amount_headroom_pct = Decimal("0")
    settings.route_available_headroom_pct = Decimal("0")
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )

        sell_a = _order(side="sell", fiat="EUR", price="1.04", order_id="sell-a")
        sell_a.available = Decimal("600")
        sell_b = _order(side="sell", fiat="EUR", price="1.04", order_id="sell-b")
        sell_b.available = Decimal("600")
        buy = _order(side="buy", fiat="EUR", price="1.00", order_id="buy-a")
        buy.available = Decimal("5000")

        opportunities = analyzer._find_internal(
            {
                ("bybit", "EUR", "sell"): [sell_a, sell_b],
                ("bybit", "EUR", "buy"): [buy],
            },
            liquidity_context={},
        )

    assert opportunities
    assert max(opportunity.volume_usdt for opportunity in opportunities) == Decimal("600.0000")
    assert all(
        not isinstance(opportunity.sell_order.raw.get("components"), list)
        or len(opportunity.sell_order.raw["components"]) < 2
        for opportunity in opportunities
    )


def test_risk_guard_merchant_summary_uses_ladder_component_names() -> None:
    guard = RiskGuard(_settings(), AppState())
    order = _order(side="sell", fiat="EUR")
    order.raw = {
        "components": [
            {"merchant_name": "Alice"},
            {"merchant_name": "Bob"},
        ]
    }

    summary = guard.merchant_summary(order)

    assert "Лестница (2 мерч.)" in summary
    assert "Alice, Bob" in summary


def test_risk_guard_merchant_summary_falls_back_to_raw_merchant_name() -> None:
    guard = RiskGuard(_settings(), AppState())
    order = _order(platform="bybit", side="sell", fiat="AUD")
    order.merchant_name = ""
    order.raw = {"nickName": "BENICE_XCHANGE"}

    summary = guard.merchant_summary(order)

    assert "BENICE_XCHANGE" in summary


def test_find_cross_currency_uses_direct_fx_only() -> None:
    settings = _settings()
    settings.enable_cross_currency = True
    state = AppState()
    with _event_loop() as loop:
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        sell_order = _order(side="sell", fiat="PLN", price="4.20")
        buy_order = _order(side="buy", fiat="EUR", price="0.95")
        fx_matrix = {
            ("PLN", "EUR"): Decimal("0.240000"),
            ("EUR", "PLN"): Decimal("4.166667"),
        }
        opportunities = analyzer._find_cross_currency(
            [sell_order, buy_order],
            fx_matrix,
            liquidity_context={},
        )

    assert opportunities
    assert opportunities[0].fx_rate_used == Decimal("0.240000")
    assert "PLN->USDT->EUR->PLN" in opportunities[0].note
    assert opportunities[0].sell_order.side == "sell"
    assert opportunities[0].buy_order.side == "buy"


def test_analyze_orders_reenables_cross_currency_stage(monkeypatch) -> None:
    settings = _settings()
    settings.enable_cross_currency = True
    state = AppState()
    with _event_loop() as loop:
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        buy_order = _order(side="buy", fiat="EUR", price="1.00")
        sell_order = _order(side="sell", fiat="PLN", price="4.10")
        marker = _opportunity(buy_order=buy_order, sell_order=sell_order)

        async def fake_build_liquidity_context(grouped):
            return {}

        async def fake_internal_graph(orders, liquidity_context):
            return []

        async def fake_apply_inventory(opportunities):
            return opportunities

        async def fake_annotate_settlement(opportunities):
            return opportunities

        monkeypatch.setattr(analyzer, "_build_liquidity_context", fake_build_liquidity_context)
        monkeypatch.setattr(analyzer, "_find_internal", lambda grouped, liquidity_context: [])
        monkeypatch.setattr(analyzer, "_find_cross_platform", lambda orders, liquidity_context: [])
        monkeypatch.setattr(analyzer, "_find_internal_graph_opportunities", fake_internal_graph)
        monkeypatch.setattr(analyzer, "_find_cross_currency", lambda orders, fx_matrix, liquidity_context: [marker])
        monkeypatch.setattr(analyzer, "_filter_sane_opportunities", lambda opportunities, fx_matrix: opportunities)
        monkeypatch.setattr(analyzer, "_apply_inventory_routing", fake_apply_inventory)
        monkeypatch.setattr(analyzer, "_annotate_settlement_requirements", fake_annotate_settlement)

        result = loop.run_until_complete(analyzer.analyze_orders([buy_order, sell_order]))

    assert result == [marker]


def test_filter_sane_opportunities_rejects_unrealistic_spread() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        opportunity = _opportunity(
            buy_order=_order(side="buy", fiat="EUR", price="1.00"),
            sell_order=_order(side="sell", fiat="PLN", price="4.10"),
        )
        opportunity.spread_pct = Decimal("31.00")

        filtered = analyzer._filter_sane_opportunities(
            [opportunity],
            {
                ("EUR", "EUR"): Decimal("1"),
                ("PLN", "PLN"): Decimal("1"),
                ("EUR", "PLN"): Decimal("4.200000"),
                ("PLN", "EUR"): Decimal("0.238095"),
            },
        )

    assert filtered == []


def test_risk_guard_rejects_order_below_configured_fiat_min_price() -> None:
    settings = _settings()
    settings.p2p_price_min_by_fiat = {"EUR": Decimal("0.84")}
    settings.p2p_price_max_by_fiat = {"EUR": Decimal("1.10")}
    guard = RiskGuard(settings, AppState())
    order = _order(side="buy", fiat="EUR", price="0.80")

    safe, reason = guard.is_order_price_sane(order)

    assert safe is False
    assert reason == "EUR/USDT price 0.80 below minimum 0.84"


def test_analyzer_price_sane_against_fx_respects_configured_fiat_bounds() -> None:
    settings = _settings()
    settings.p2p_price_min_by_fiat = {"EUR": Decimal("0.84")}
    settings.p2p_price_max_by_fiat = {"EUR": Decimal("1.10")}
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        sane_low, reason_low = analyzer._price_sane_against_fx(
            fiat="EUR",
            price=Decimal("0.80"),
            fx_matrix={("USD", "EUR"): Decimal("0.86")},
        )
        sane_high, reason_high = analyzer._price_sane_against_fx(
            fiat="EUR",
            price=Decimal("1.12"),
            fx_matrix={("USD", "EUR"): Decimal("0.86")},
        )

    assert sane_low is False
    assert reason_low == "EUR/USDT price 0.80 below minimum 0.84"
    assert sane_high is False
    assert reason_high == "EUR/USDT price 1.12 above maximum 1.10"


def test_analyzer_price_sane_against_fx_uses_stricter_bybit_limit() -> None:
    settings = _settings()
    settings.p2p_price_max_deviation_pct = Decimal("40")
    settings.bybit_p2p_price_max_deviation_pct = Decimal("20")
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        bybit_sane, bybit_reason = analyzer._price_sane_against_fx(
            fiat="EUR",
            platform="bybit",
            price=Decimal("1.057"),
            fx_matrix={("USD", "EUR"): Decimal("0.86")},
        )
        binance_sane, binance_reason = analyzer._price_sane_against_fx(
            fiat="EUR",
            platform="binance",
            price=Decimal("1.057"),
            fx_matrix={("USD", "EUR"): Decimal("0.86")},
        )

    assert bybit_sane is False
    assert "22.91% deviation" in bybit_reason
    assert "> 20%" in bybit_reason
    assert binance_sane is True
    assert binance_reason == "OK"


def test_analyzer_price_sane_against_fx_uses_bybit_fiat_override() -> None:
    settings = _settings()
    settings.p2p_price_max_deviation_pct = Decimal("40")
    settings.bybit_p2p_price_max_deviation_pct = Decimal("20")
    settings.bybit_p2p_price_max_deviation_pct_by_fiat = {"EUR": Decimal("15")}
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        sane, reason = analyzer._price_sane_against_fx(
            fiat="EUR",
            platform="bybit",
            price=Decimal("1.00"),
            fx_matrix={("USD", "EUR"): Decimal("0.86")},
        )

    assert sane is False
    assert "> 15%" in reason


def test_analyze_orders_skips_internal_graph_until_interval_expires(monkeypatch) -> None:
    settings = replace(
        _settings(),
        enable_internal_graph=True,
        enable_cross_currency=False,
        internal_graph_interval_sec=180,
    )
    state = AppState()
    with _event_loop() as loop:
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        buy_order = _order(side="buy", fiat="EUR", price="1.00")
        sell_order = _order(side="sell", fiat="PLN", price="4.10")
        internal_graph_calls = 0

        async def fake_build_liquidity_context(grouped):
            return {}

        async def fake_internal_graph(orders, liquidity_context):
            nonlocal internal_graph_calls
            internal_graph_calls += 1
            return []

        async def fake_apply_inventory(opportunities):
            return opportunities

        async def fake_annotate_settlement(opportunities):
            return opportunities

        monkeypatch.setattr(analyzer, "_build_liquidity_context", fake_build_liquidity_context)
        monkeypatch.setattr(analyzer, "_find_internal", lambda grouped, liquidity_context: [])
        monkeypatch.setattr(analyzer, "_find_cross_platform", lambda orders, liquidity_context: [])
        monkeypatch.setattr(analyzer, "_find_internal_graph_opportunities", fake_internal_graph)
        monkeypatch.setattr(analyzer, "_filter_sane_opportunities", lambda opportunities, fx_matrix: opportunities)
        monkeypatch.setattr(analyzer, "_apply_inventory_routing", fake_apply_inventory)
        monkeypatch.setattr(analyzer, "_annotate_settlement_requirements", fake_annotate_settlement)

        loop.run_until_complete(analyzer.analyze_orders([buy_order, sell_order]))
        loop.run_until_complete(analyzer.analyze_orders([buy_order, sell_order]))

    assert internal_graph_calls == 1


def test_opportunity_prices_sane_rejects_step_side_mismatch() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        wrong_sell = _opportunity(
            buy_order=_order(side="buy", fiat="EUR", price="1.00"),
            sell_order=_order(side="buy", fiat="PLN", price="4.10"),
        )
        sane, reason = analyzer._opportunity_prices_sane(
            wrong_sell,
            {
                ("EUR", "EUR"): Decimal("1"),
                ("PLN", "PLN"): Decimal("1"),
                ("EUR", "PLN"): Decimal("4.200000"),
                ("PLN", "EUR"): Decimal("0.238095"),
            },
        )
        wrong_buy = _opportunity(
            buy_order=_order(side="sell", fiat="EUR", price="1.00"),
            sell_order=_order(side="sell", fiat="PLN", price="4.10"),
        )
        sane_buy, reason_buy = analyzer._opportunity_prices_sane(
            wrong_buy,
            {
                ("EUR", "EUR"): Decimal("1"),
                ("PLN", "PLN"): Decimal("1"),
                ("EUR", "PLN"): Decimal("4.200000"),
                ("PLN", "EUR"): Decimal("0.238095"),
            },
        )

    assert sane is False
    assert reason == "step 1 side mismatch: expected sell, got buy"
    assert sane_buy is False
    assert reason_buy == "step 3 side mismatch: expected buy, got sell"


def test_opportunity_prices_sane_rejects_missing_single_merchant_identity() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        opportunity = _opportunity(
            buy_order=_order(side="buy", fiat="EUR", price="1.00"),
            sell_order=_order(side="sell", fiat="PLN", price="4.10"),
        )
        opportunity.sell_order.merchant_name = ""
        opportunity.sell_order.merchant_id = ""
        opportunity.sell_order.raw = {}
        sane, reason = analyzer._opportunity_prices_sane(
            opportunity,
            {
                ("EUR", "EUR"): Decimal("1"),
                ("PLN", "PLN"): Decimal("1"),
                ("EUR", "PLN"): Decimal("4.200000"),
                ("PLN", "EUR"): Decimal("0.238095"),
            },
        )

    assert sane is False
    assert reason == "step 1: merchant name missing"


def test_opportunity_prices_sane_rejects_link_side_mismatch(monkeypatch) -> None:
    import p2p_bot.modules.analyzer as analyzer_module

    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        opportunity = _opportunity(
            buy_order=_order(side="buy", fiat="EUR", price="1.00"),
            sell_order=_order(side="sell", fiat="PLN", price="4.10"),
        )
        opportunity.sell_order.merchant_name = "Seller"
        opportunity.sell_order.merchant_id = "seller-1"
        opportunity.buy_order.merchant_name = "Buyer"
        opportunity.buy_order.merchant_id = "buyer-1"
        monkeypatch.setattr(
            analyzer_module,
            "order_action_url",
            lambda order: "https://www.bybit.com/en/p2p/buy/USDT/PLN"
            if order.side == "sell"
            else "https://www.bybit.com/en/p2p/buy/USDT/EUR",
        )
        sane, reason = analyzer._opportunity_prices_sane(
            opportunity,
            {
                ("EUR", "EUR"): Decimal("1"),
                ("PLN", "PLN"): Decimal("1"),
                ("EUR", "PLN"): Decimal("4.200000"),
                ("PLN", "EUR"): Decimal("0.238095"),
            },
        )

    assert sane is False
    assert reason == (
        "step 1: link side mismatch for bybit: expected sell, "
        "got https://www.bybit.com/en/p2p/buy/USDT/PLN"
    )


def test_filter_sane_opportunities_rejects_multi_merchant_leg_in_strict_mode() -> None:
    settings = _settings()
    settings.strict_single_merchant_mode = True
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )
        opportunity = _opportunity(
            buy_order=_order(side="buy", fiat="EUR", price="1.00"),
            sell_order=_order(side="sell", fiat="EUR", price="1.04"),
        )
        opportunity.sell_order.raw = {
            "components": [
                {"order_id": "sell-a", "merchant_name": "Alice"},
                {"order_id": "sell-b", "merchant_name": "Bob"},
            ]
        }

        filtered = analyzer._filter_sane_opportunities(
            [opportunity],
            {
                ("EUR", "EUR"): Decimal("1"),
            },
        )

    assert filtered == []


def test_app_state_uses_same_route_priority_as_analyzer() -> None:
    better_price = _opportunity(
        buy_order=_order(side="buy", fiat="EUR", price="0.88", order_id="buy-better-state"),
        sell_order=_order(side="sell", fiat="EUR", price="1.05", order_id="sell-better-state"),
    )
    better_price.spread_pct = Decimal("5.00")
    better_price.estimated_profit_usd = Decimal("20.00")

    bigger_volume = _opportunity(
        buy_order=_order(side="buy", fiat="EUR", price="0.89", order_id="buy-bigger-state"),
        sell_order=_order(side="sell", fiat="EUR", price="1.04", order_id="sell-bigger-state"),
    )
    bigger_volume.spread_pct = Decimal("4.00")
    bigger_volume.estimated_profit_usd = Decimal("50.00")

    state = AppState()
    state.record_opportunities([bigger_volume, better_price])

    assert state.latest_opportunities[0] is better_price


def test_internal_quote_client_prefers_exact_quote_over_better_indicative(monkeypatch) -> None:
    async def fake_exact(self, from_asset: str, to_asset: str, from_amount: Decimal):
        return InternalQuote(
            platform="bybit",
            venue="fiat-convert",
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
            to_amount=Decimal("100"),
            rate=Decimal("1.00"),
            exact=True,
        )

    async def fake_indicative(self, from_asset: str, to_asset: str, from_amount: Decimal):
        return InternalQuote(
            platform="binance",
            venue="spot",
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
            to_amount=Decimal("110"),
            rate=Decimal("1.10"),
            exact=False,
        )

    async def fake_none(self, from_asset: str, to_asset: str, from_amount: Decimal):
        return None

    with _event_loop() as loop:
        client = InternalQuoteClient(
            session=object(),  # type: ignore[arg-type]
            settings=_settings(),
            logger=logging.getLogger("test.internal_quotes"),
        )
        monkeypatch.setattr(InternalQuoteClient, "_binance_convert_quote", fake_none)
        monkeypatch.setattr(InternalQuoteClient, "_bybit_fiat_convert_quote", fake_exact)
        monkeypatch.setattr(InternalQuoteClient, "_binance_spot_quote", fake_indicative)
        monkeypatch.setattr(InternalQuoteClient, "_bybit_spot_quote", fake_none)
        monkeypatch.setattr(InternalQuoteClient, "_bingx_spot_quote", fake_none)

        best = loop.run_until_complete(
            client.best_quote(from_asset="USDT", to_asset="EUR", from_amount=Decimal("100"))
        )

    assert best is not None
    assert best.exact is True
    assert best.venue == "fiat-convert"


def test_cross_currency_is_not_suppressed_by_indicative_internal_quote() -> None:
    opportunity = _opportunity(
        buy_order=_order(side="buy", fiat="EUR", price="1.00"),
        sell_order=_order(side="sell", fiat="PLN", price="4.10"),
    )
    opportunity.internal_quote_exact = False
    opportunity.internal_quote_advantage_usdt = Decimal("25")

    assert Analyzer._is_outperformed_by_internal_route(opportunity) is False

    opportunity.internal_quote_exact = True
    assert Analyzer._is_outperformed_by_internal_route(opportunity) is True


def test_internal_graph_chain_requires_exact_quotes() -> None:
    settings = _settings()
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )

        sell_order = _order(side="sell", fiat="EUR", price="1.10", platform="bybit", order_id="graph-sell")
        buy_order = _order(side="buy", fiat="EUR", price="0.95", platform="bybit", order_id="graph-buy")
        sell_order.raw = {
            "internal_graph": True,
            "chain": ["USDT", "EUR", "USDT"],
            "quotes": [
                {"platform": "bybit", "venue": "fiat-convert", "from_asset": "USDT", "to_asset": "EUR", "rate": "1.10", "exact": True},
                {"platform": "bybit", "venue": "spot", "from_asset": "EUR", "to_asset": "USDT", "rate": "1.05", "exact": False},
            ],
        }
        opportunity = _opportunity(buy_order=buy_order, sell_order=sell_order)
        opportunity.type = "internal_graph"
        opportunity.base_type = "internal_graph"

        sane, reason = analyzer._opportunity_chain_sane(opportunity)

    assert sane is False
    assert "indicative" in reason


def test_fixed_fee_usd_treats_base_asset_as_usd_equivalent_for_usdc_contour() -> None:
    settings = replace(_settings(), base_asset="USDC")
    state = AppState()
    with _event_loop():
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=RiskGuard(settings, state),
            raw_order_repository=None,  # type: ignore[arg-type]
            opportunity_repository=None,  # type: ignore[arg-type]
            signal_journal_repository=None,
            inventory_repository=None,
            forex_client=_DummyForexClient(),
            internal_quote_client=_DummyInternalQuoteClient(),
            scan_queue=asyncio.Queue(),
            alert_queue=asyncio.Queue(),
            logger=logging.getLogger("test.analyzer"),
        )

        fee = analyzer._fixed_fee_usd(
            fixed_usd=Decimal("0"),
            fixed_amount=Decimal("12.5"),
            fixed_ccy="USDC",
            primary_fiat="EUR",
            primary_price=Decimal("1.10"),
            secondary_fiat=None,
            secondary_price=None,
        )

    assert fee == Decimal("12.5")


def test_opportunity_snapshot_preserves_merchant_name() -> None:
    buy_order = _order(platform="binance", side="buy", fiat="EUR", price="0.88")
    sell_order = _order(platform="bybit", side="sell", fiat="GBP", price="0.91")
    buy_order.merchant_name = "Tether_Boss_Team"
    buy_order.merchant_id = "merchant-buy"
    sell_order.merchant_name = "Unicorn"
    sell_order.merchant_id = "merchant-sell"

    snapshot = _opportunity(buy_order=buy_order, sell_order=sell_order).snapshot()

    assert snapshot["buy_order"]["merchant_name"] == "Tether_Boss_Team"
    assert snapshot["buy_order"]["merchant_id"] == "merchant-buy"
    assert snapshot["sell_order"]["merchant_name"] == "Unicorn"
    assert snapshot["sell_order"]["merchant_id"] == "merchant-sell"


def test_opportunity_snapshot_falls_back_to_raw_merchant_identity() -> None:
    buy_order = _order(platform="binance", side="buy", fiat="EUR", price="0.88")
    sell_order = _order(platform="bybit", side="sell", fiat="AUD", price="1.74")
    buy_order.merchant_name = ""
    buy_order.merchant_id = ""
    buy_order.raw = {
        "advertiser": {
            "nickName": "TEZER_Exchange",
            "userNo": "merchant-binance",
        }
    }
    sell_order.merchant_name = ""
    sell_order.merchant_id = ""
    sell_order.raw = {
        "nickName": "BENICE_XCHANGE",
        "userId": "merchant-bybit",
    }

    snapshot = _opportunity(buy_order=buy_order, sell_order=sell_order).snapshot()

    assert snapshot["buy_order"]["merchant_name"] == "TEZER_Exchange"
    assert snapshot["buy_order"]["merchant_id"] == "merchant-binance"
    assert snapshot["sell_order"]["merchant_name"] == "BENICE_XCHANGE"
    assert snapshot["sell_order"]["merchant_id"] == "merchant-bybit"
