from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from p2p_bot.config import Settings
from p2p_bot.models.opportunity import ArbitrageOpportunity
from p2p_bot.models.order import P2POrder
from p2p_bot.modules.telegram_bot import TelegramBotService


def _order(
    *,
    platform: str,
    side: str,
    order_id: str = "abc123",
    fiat: str = "EUR",
    payment_methods: list[str] | None = None,
    raw: dict | None = None,
) -> P2POrder:
    return P2POrder(
        platform=platform,
        order_id=order_id,
        side=side,
        asset="USDT",
        fiat=fiat,
        price=Decimal("1"),
        min_amount=Decimal("10"),
        max_amount=Decimal("1000"),
        available=Decimal("1000"),
        payment_methods=payment_methods or [],
        merchant_id="m1",
        merchant_rating=99.0,
        merchant_orders=100,
        merchant_days=365,
        raw=raw or {},
    )


def _service() -> TelegramBotService:
    service = TelegramBotService.__new__(TelegramBotService)
    service.settings = Settings.from_env(Path.cwd())
    return service


def _opportunity(*, buy_order: P2POrder, sell_order: P2POrder) -> ArbitrageOpportunity:
    now = datetime.utcnow()
    return ArbitrageOpportunity(
        type="cross_platform",
        base_type="cross_platform",
        spread_pct=Decimal("2.00"),
        estimated_profit_usd=Decimal("10.00"),
        gross_profit_usd=Decimal("10.00"),
        total_fees_usd=Decimal("0"),
        buy_fee_usd=Decimal("0"),
        sell_fee_usd=Decimal("0"),
        fx_fee_usd=Decimal("0"),
        volume_usdt=Decimal("100"),
        buy_order=buy_order,
        sell_order=sell_order,
        detected_at=now,
        expires_at=now,
    )


def test_binance_sell_side_opens_sell_tab() -> None:
    order = _order(
        platform="binance",
        side="sell",
        order_id="987654",
        fiat="AUD",
        payment_methods=["Express Bank Transfer"],
        raw={
            "adv": {
                "tradeMethods": [
                    {
                        "identifier": "p2plusAUDBank",
                        "tradeMethodName": "Express Bank Transfer",
                    }
                ]
            }
        },
    )
    assert (
        TelegramBotService._order_action_url(order)
        == "https://p2p.binance.com/en/trade/sell/USDT?fiat=AUD"
    )
    assert TelegramBotService._order_action_link_text(order) == "ОТКРЫТЬ SELL"


def test_binance_buy_side_opens_buy_tab() -> None:
    order = _order(
        platform="binance",
        side="buy",
        order_id="987654",
        fiat="AUD",
        payment_methods=["PayID"],
        raw={
            "adv": {
                "tradeMethods": [
                    {
                        "identifier": "PayID",
                        "tradeMethodName": "PayID",
                    }
                ]
            }
        },
    )
    assert (
        TelegramBotService._order_action_url(order)
        == "https://p2p.binance.com/en/trade/buy/USDT?fiat=AUD"
    )
    assert TelegramBotService._order_action_link_text(order) == "ОТКРЫТЬ BUY"


def test_binance_falls_back_to_all_payments_without_identifier() -> None:
    order = _order(platform="binance", side="sell", order_id="987654", fiat="EUR")
    assert (
        TelegramBotService._order_action_url(order)
        == "https://p2p.binance.com/en/trade/sell/USDT?fiat=EUR"
    )


def test_binance_buy_link_uses_binance_buy_route_even_without_raw_identifier() -> None:
    order = _order(
        platform="binance",
        side="buy",
        order_id="987654",
        fiat="GBP",
        payment_methods=["Instant Transfer", "Bank Transfer", "Monzo"],
    )
    assert (
        TelegramBotService._order_action_url(order)
        == "https://p2p.binance.com/en/trade/buy/USDT?fiat=GBP"
    )


def test_binance_sell_link_uses_binance_sell_route_even_with_payment_identifiers() -> None:
    order = _order(
        platform="binance",
        side="sell",
        order_id="987654",
        fiat="CAD",
        payment_methods=["Bank Transfer", "RBC Royal Bank", "TD Bank"],
        raw={
            "adv": {
                "tradeMethods": [
                    {"identifier": "BANK", "tradeMethodName": "Bank Transfer"},
                    {"identifier": "RBCRoyalbank", "tradeMethodName": "RBC Royal Bank"},
                    {"identifier": "TDbank", "tradeMethodName": "TD Bank"},
                ]
            }
        },
    )
    assert (
        TelegramBotService._order_action_url(order)
        == "https://p2p.binance.com/en/trade/sell/USDT?fiat=CAD"
    )


def test_bingx_sell_side_opens_sell_tab() -> None:
    order = _order(platform="bingx", side="sell", fiat="ILS")
    assert (
        TelegramBotService._order_action_url(order)
        == "https://fiat.bingx.com/en/p2p/h5?fiat=ILS&type=2"
    )
    assert TelegramBotService._order_action_link_text(order) == "ОТКРЫТЬ SELL"


def test_bingx_buy_side_opens_buy_tab() -> None:
    order = _order(platform="bingx", side="buy", fiat="ILS")
    assert (
        TelegramBotService._order_action_url(order)
        == "https://fiat.bingx.com/en/p2p/h5?fiat=ILS&type=1"
    )
    assert TelegramBotService._order_action_link_text(order) == "ОТКРЫТЬ BUY"


def test_bybit_buy_side_opens_buy_tab() -> None:
    order = _order(platform="bybit", side="buy")
    assert (
        TelegramBotService._order_action_url(order)
        == "https://www.bybit.com/en/p2p/buy/USDT/EUR"
    )
    assert TelegramBotService._order_action_link_text(order) == "ОТКРЫТЬ BUY"


def test_bybit_sell_side_opens_sell_tab() -> None:
    order = _order(platform="bybit", side="sell")
    assert (
        TelegramBotService._order_action_url(order)
        == "https://www.bybit.com/en/p2p/sell/USDT/EUR"
    )
    assert TelegramBotService._order_action_link_text(order) == "ОТКРЫТЬ SELL"


def test_bybit_usdc_sell_side_opens_sell_tab() -> None:
    order = _order(platform="bybit", side="sell")
    order.asset = "USDC"
    assert (
        TelegramBotService._order_action_url(order)
        == "https://www.bybit.com/en/p2p/sell/USDC/EUR"
    )
    assert TelegramBotService._order_action_link_text(order) == "ОТКРЫТЬ SELL"


def test_bybit_usdc_buy_side_opens_buy_tab() -> None:
    order = _order(platform="bybit", side="buy")
    order.asset = "USDC"
    assert (
        TelegramBotService._order_action_url(order)
        == "https://www.bybit.com/en/p2p/buy/USDC/EUR"
    )
    assert TelegramBotService._order_action_link_text(order) == "ОТКРЫТЬ BUY"


def test_execution_text_uses_sell_to_buy_direction() -> None:
    service = _service()
    opportunity = _opportunity(
        buy_order=_order(platform="bybit", side="buy", fiat="EUR"),
        sell_order=_order(platform="binance", side="sell", fiat="EUR"),
    )

    assert service._execution_text(opportunity) == "BINANCE → BYBIT"


def test_signal_detail_text_uses_sell_to_buy_direction() -> None:
    service = _service()

    text = service._signal_detail_text(
        {
            "id": 42,
            "status": "new",
            "opportunity_type": "cross_currency",
            "sell_platform": "binance",
            "buy_platform": "bybit",
            "sell_fiat": "PLN",
            "buy_fiat": "EUR",
            "estimated_profit_usd": "12.50",
            "gross_profit_usd": "14.00",
            "fees_usd": "1.50",
            "volume_usdt": "500",
            "liquidity_status": "ok",
            "created_at": "2026-06-24T10:00:00+00:00",
            "updated_at": "2026-06-24T10:01:00+00:00",
            "note": "",
        }
    )

    assert "Платформы: binance -> bybit" in text
    assert "Валюты: PLN -> EUR" in text


def test_prepare_fiat_step_uses_plain_platform_name_without_link() -> None:
    service = _service()
    opportunity = _opportunity(
        buy_order=_order(platform="bybit", side="buy", fiat="EUR"),
        sell_order=_order(platform="bybit", side="sell", fiat="EUR"),
    )
    opportunity.buy_rail = "fiat_balance"
    opportunity.sell_rail = "revolut_balance"
    opportunity.buy_payment_summary = "Balance"
    opportunity.sell_payment_summary = "Bank Transfer"

    steps = service._execution_steps_text(
        opportunity=opportunity,
        buy_platform='BYBIT <a href="https://www.bybit.com/en/p2p/buy/USDT/EUR">ОТКРЫТЬ BUY</a>',
        buy_platform_name="BYBIT",
        sell_platform='BYBIT <a href="https://www.bybit.com/en/p2p/sell/USDT/EUR">ОТКРЫТЬ SELL</a>',
        buy_price="0.881",
        sell_price="1.057",
        buy_currency="EUR",
        sell_currency="EUR",
        sell_notional="1585.5",
        rebuy_budget="1585.5",
        buy_method="Фиат внутри биржи",
        sell_method="Revolut",
        buy_merchant="Мерчант: alvik",
        sell_merchant="Мерчант: NERAH-PAYMENT",
        input_volume="1500",
        step3_gross_return_usdt="1799.6595",
    )

    assert "ШАГ 2 — ПОДГОТОВЬ ФИАТ НА BYBIT" in steps
    assert "ШАГ 2 — ПОДГОТОВЬ ФИАТ НА BYBIT <a href=" not in steps
    assert "ШАГ 3 — КУПИ USDT ОБРАТНО" in steps
    assert 'BYBIT <a href="https://www.bybit.com/en/p2p/buy/USDT/EUR">ОТКРЫТЬ BUY</a>' in steps
