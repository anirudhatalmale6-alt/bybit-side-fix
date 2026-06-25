from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable, TypeVar
from urllib.parse import quote

from p2p_bot.models.order import P2POrder

T = TypeVar("T")

# Canonical contract for the entire bot:
#   sell = we sell USDT and receive fiat
#   buy  = we buy USDT and pay fiat
SELL = "sell"
BUY = "buy"


def normalize_side(side: str) -> str:
    normalized = str(side or "").strip().lower()
    if normalized not in {SELL, BUY}:
        raise ValueError(f"Invalid canonical side: {side!r}")
    return normalized


def prefer_high_price(side: str) -> bool:
    """Sell USDT -> maximize fiat/USDT; buy USDT -> minimize fiat/USDT."""
    return normalize_side(side) == SELL


def select_best_order(orders: Iterable[P2POrder], side: str) -> P2POrder | None:
    items = list(orders)
    if not items:
        return None
    reverse = prefer_high_price(side)
    return max(items, key=lambda order: order.price) if reverse else min(items, key=lambda order: order.price)


def binance_trade_type(canonical_side: str) -> str:
    # Binance search tradeType is user-action side:
    # - SELL returns the "Sell USDT" book
    # - BUY  returns the "Buy USDT" book
    return "SELL" if normalize_side(canonical_side) == SELL else "BUY"


def canonical_side_from_binance_response(trade_type: Any) -> str | None:
    normalized = str(trade_type or "").strip().upper()
    if normalized == "SELL":
        return BUY
    if normalized == "BUY":
        return SELL
    return None


def bybit_api_side_code(canonical_side: str) -> str:
    # Bybit P2P API side parameter (from the user/taker perspective):
    #   "0" = Buy  tab  (user buys  crypto, merchants sell)
    #   "1" = Sell tab  (user sells crypto, merchants buy)
    #
    # SELL (we sell USDT) -> browser SELL tab -> API side "1"
    # BUY  (we buy  USDT) -> browser BUY  tab -> API side "0"
    #
    # The endpoint still expects strings; sending integers returns 10001.
    return "1" if normalize_side(canonical_side) == SELL else "0"


def canonical_side_from_bybit_response(side_value: Any) -> str | None:
    if side_value is None:
        return None
    normalized = str(side_value).strip()
    # Decode the Bybit side back into the bot canonical contract.
    #   "0" = Buy side  -> canonical buy
    #   "1" = Sell side -> canonical sell
    if normalized == "1":
        return SELL
    if normalized == "0":
        return BUY
    return None


def bybit_v5_side_code(canonical_side: str) -> str:
    """Side code for the V5 official API (advertiser perspective).

    The V5 /v5/p2p/item/online and /v5/p2p/item/create endpoints use
    advertiser-perspective side codes:
      "0" = advertiser buys  crypto → for our canonical SELL (find buyers)
      "1" = advertiser sells crypto → for our canonical BUY  (find sellers)

    This is the OPPOSITE of the legacy public endpoint's tab convention
    used by bybit_api_side_code().
    """
    return "0" if normalize_side(canonical_side) == SELL else "1"


def bybit_web_action(canonical_side: str, *, asset: str = "USDT") -> str:
    _ = asset
    return normalize_side(canonical_side)


def bingx_request_type(canonical_side: str) -> int:
    # BingX API types are opposite to the visible web tabs:
    # - type 1 returns the public "Buy USDT" book
    # - type 2 returns the public "Sell USDT" book
    return 2 if normalize_side(canonical_side) == SELL else 1


def canonical_side_from_bingx_response(type_value: Any) -> str | None:
    normalized = str(type_value or "").strip()
    if normalized == "1":
        return SELL
    if normalized == "2":
        return BUY
    return None


def bingx_trade_url(*, side: str, fiat: str) -> str:
    fiat_q = quote(fiat.upper(), safe="")
    if normalize_side(side) == SELL:
        return f"https://fiat.bingx.com/en/p2p/h5?fiat={fiat_q}&type=2"
    return f"https://fiat.bingx.com/en/p2p/h5?fiat={fiat_q}&type=1"


_BINANCE_PAYMENT_ROUTE_ALIASES = {
    "instant transfer": "FPS",
    "faster payments": "FPS",
    "fps": "FPS",
    "bank transfer": "BANK",
    "bank transfer (australia)": "BankAustralia",
    "payid": "PayID",
    "lightning payid": "p2plusAUDPayID",
    "express bank transfer": "p2plusAUDBank",
    "osko": "OKSO",
    "monzo": "Monzo",
    "starling bank": "StarlingBank",
    "lloyds bank": "LloydsBank",
    "cash deposit to bank": "CashDeposit",
}


_BINANCE_GENERIC_ROUTE_SEGMENTS = {"BANK", "all-payments"}


def _binance_payment_route_segment(*, raw: dict | None, payment_methods: list[str] | None) -> str:
    if isinstance(raw, dict):
        adv = raw.get("adv") or {}
        trade_methods = adv.get("tradeMethods") or raw.get("tradeMethods") or []
        generic_identifier = ""
        for method in trade_methods:
            if not isinstance(method, dict):
                continue
            identifier = (
                method.get("identifier")
                or method.get("payType")
                or method.get("tradeMethodShortName")
                or method.get("tradeMethodName")
            )
            if identifier:
                token = str(identifier)
                if token not in _BINANCE_GENERIC_ROUTE_SEGMENTS:
                    return token
                if not generic_identifier:
                    generic_identifier = token
        if generic_identifier:
            return generic_identifier
    generic_alias = ""
    for method in payment_methods or []:
        token = str(method or "").strip()
        alias = _BINANCE_PAYMENT_ROUTE_ALIASES.get(token.lower())
        if alias:
            if alias not in _BINANCE_GENERIC_ROUTE_SEGMENTS:
                return alias
            if not generic_alias:
                generic_alias = alias
            continue
        if token and " " not in token:
            if token not in _BINANCE_GENERIC_ROUTE_SEGMENTS:
                return token
            if not generic_alias:
                generic_alias = token
    if generic_alias:
        return generic_alias
    return "all-payments"


def binance_trade_url(*, side: str, asset: str, fiat: str) -> str:
    action = "sell" if normalize_side(side) == SELL else "buy"
    asset_q = quote(asset.upper(), safe="")
    fiat_q = quote(fiat.upper(), safe="")
    return f"https://p2p.binance.com/en/trade/{action}/{asset_q}?fiat={fiat_q}"


def bybit_trade_url(*, side: str, asset: str, fiat: str) -> str:
    action = bybit_web_action(side, asset=asset)
    asset_q = quote(asset.upper(), safe="")
    fiat_q = quote(fiat.upper(), safe="")
    return f"https://www.bybit.com/en/p2p/{action}/{asset_q}/{fiat_q}"


def order_action_url(order: P2POrder) -> str:
    platform = str(order.platform or "").lower()
    asset = str(order.asset or "USDT").upper()
    fiat = str(order.fiat or "").upper()
    side = normalize_side(order.side)

    raw = order.raw or {}
    components = raw.get("components") if isinstance(raw, dict) else None
    if components:
        first = components[0] or {}
        _ = str(first.get("order_id") or order.order_id or "")

    if platform == "binance":
        return binance_trade_url(side=side, asset=asset, fiat=fiat)
    if platform == "bingx":
        return bingx_trade_url(side=side, fiat=fiat)
    if platform == "bybit":
        return bybit_trade_url(side=side, asset=asset, fiat=fiat)
    return ""


def fx_rate_direct(
    fx_matrix: dict[tuple[str, str], Decimal],
    *,
    source_fiat: str,
    target_fiat: str,
) -> Decimal | None:
    """
    How much `target_fiat` we receive for 1 unit of `source_fiat`.
    Never guesses or inverts direction.
    """
    source = source_fiat.upper()
    target = target_fiat.upper()
    if source == target:
        return Decimal("1")
    rate = fx_matrix.get((source, target))
    if rate is None or rate <= 0:
        return None
    return rate


def fx_chain_candidates(
    fx_matrix: dict[tuple[str, str], Decimal],
    *,
    source_fiat: str,
    target_fiat: str,
) -> list[tuple[tuple[str, ...], Decimal]]:
    direct = fx_rate_direct(fx_matrix, source_fiat=source_fiat, target_fiat=target_fiat)
    if direct is None:
        return []
    return [((source_fiat.upper(), target_fiat.upper()), direct)]
