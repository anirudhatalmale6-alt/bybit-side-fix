from decimal import Decimal

from p2p_bot.models.order import P2POrder
from p2p_bot.utils.canonical_side import (
    canonical_side_from_binance_response,
    canonical_side_from_bingx_response,
    canonical_side_from_bybit_response,
    bybit_api_side_code,
    fx_chain_candidates,
    fx_rate_direct,
    order_action_url,
    prefer_high_price,
    select_best_order,
)


def _order(*, side: str, price: str, platform: str = "bybit", fiat: str = "EUR") -> P2POrder:
    return P2POrder(
        platform=platform,
        order_id=f"{platform}-{side}-{fiat}",
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
    )


def test_prefer_high_price_for_sell_only() -> None:
    assert prefer_high_price("sell") is True
    assert prefer_high_price("buy") is False


def test_select_best_order_uses_canonical_price_direction() -> None:
    sell_orders = [_order(side="sell", price="4.10"), _order(side="sell", price="4.05")]
    buy_orders = [_order(side="buy", price="4.20"), _order(side="buy", price="4.15")]

    assert select_best_order(sell_orders, "sell").price == Decimal("4.10")
    assert select_best_order(buy_orders, "buy").price == Decimal("4.15")


def test_bybit_api_side_codes_match_canonical_contract() -> None:
    assert bybit_api_side_code("sell") == "1"
    assert bybit_api_side_code("buy") == "0"


def test_response_side_decoders_match_live_exchange_contracts() -> None:
    assert canonical_side_from_binance_response("SELL") == "buy"
    assert canonical_side_from_binance_response("BUY") == "sell"
    assert canonical_side_from_bybit_response("1") == "sell"
    assert canonical_side_from_bybit_response("0") == "buy"
    assert canonical_side_from_bingx_response(1) == "sell"
    assert canonical_side_from_bingx_response(2) == "buy"


def test_fx_rate_direct_never_inverts_direction() -> None:
    matrix = {
        ("PLN", "GBP"): Decimal("0.20"),
        ("GBP", "PLN"): Decimal("5.00"),
    }

    pln_to_gbp = fx_rate_direct(matrix, source_fiat="PLN", target_fiat="GBP")
    gbp_to_pln = fx_rate_direct(matrix, source_fiat="GBP", target_fiat="PLN")

    assert pln_to_gbp == Decimal("0.20")
    assert gbp_to_pln == Decimal("5.00")
    assert pln_to_gbp < Decimal("1")
    assert gbp_to_pln > Decimal("1")


def test_fx_chain_candidates_reject_missing_direct_rate() -> None:
    matrix = {("GBP", "PLN"): Decimal("5.00")}

    assert fx_chain_candidates(matrix, source_fiat="PLN", target_fiat="GBP") == []
    assert fx_chain_candidates(matrix, source_fiat="GBP", target_fiat="PLN") == [
        (("GBP", "PLN"), Decimal("5.00"))
    ]


def test_order_action_urls_follow_canonical_side() -> None:
    sell = _order(platform="bybit", side="sell", price="4.00", fiat="PLN")
    buy = _order(platform="bybit", side="buy", price="4.00", fiat="PLN")

    assert order_action_url(sell) == "https://www.bybit.com/en/p2p/sell/USDT/PLN"
    assert order_action_url(buy) == "https://www.bybit.com/en/p2p/buy/USDT/PLN"
