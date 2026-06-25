from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import aiohttp

from p2p_bot.config import BinanceConfig
from p2p_bot.models.order import P2POrder
from p2p_bot.utils.canonical_side import (
    binance_trade_type,
    canonical_side_from_binance_response,
    normalize_side,
)
from p2p_bot.utils.http import AsyncRateLimiter, request_json


def _decimal(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def _percentage(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    raw = str(value).replace("%", "").strip()
    numeric = float(raw)
    return numeric * 100 if 0 < numeric <= 1 else numeric


def _int(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(float(value))


class BinanceP2PClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: BinanceConfig,
        logger: logging.Logger,
    ) -> None:
        self.session = session
        self.config = config
        self.logger = logger
        self.rate_limiter = AsyncRateLimiter(
            max_calls=max(1, int(self.config.rate_limit_max_calls)),
            period_sec=max(0.1, float(self.config.rate_limit_period_sec)),
        )

    async def fetch_orders(
        self,
        asset: str,
        fiat: str,
        side: str,
        size: int | None = None,
    ) -> list[P2POrder]:
        await self.rate_limiter.acquire()

        canonical_side = normalize_side(side)
        trade_type = binance_trade_type(canonical_side)
        payload = {
            "asset": asset,
            "fiat": fiat,
            "tradeType": trade_type,
            "page": 1,
            "rows": size or self.config.rows,
            "publisherType": None,
            "payTypes": [],
        }
        url = f"{self.config.base_url.rstrip('/')}{self.config.adv_search_path}"
        response = await request_json(
            self.session,
            "POST",
            url,
            json_body=payload,
            logger=self.logger,
        )
        items = response.get("data") or []
        orders: list[P2POrder] = []
        for item in items:
            adv = item.get("adv") or {}
            response_side = canonical_side_from_binance_response(adv.get("tradeType"))
            response_asset = str(adv.get("asset") or asset).upper()
            response_fiat = str(adv.get("fiatUnit") or adv.get("fiat") or fiat).upper()
            if response_asset != asset.upper() or response_fiat != fiat.upper():
                self.logger.warning(
                    "Skipping Binance ad %s due to pair mismatch: requested=%s/%s response=%s/%s",
                    adv.get("advNo") or adv.get("id") or "",
                    asset.upper(),
                    fiat.upper(),
                    response_asset,
                    response_fiat,
                )
                continue
            if response_side is not None and response_side != canonical_side:
                self.logger.warning(
                    "Skipping Binance ad %s due to side mismatch: requested=%s response=%s raw_tradeType=%s",
                    adv.get("advNo") or adv.get("id") or "",
                    canonical_side,
                    response_side,
                    adv.get("tradeType"),
                )
                continue
            advertiser = item.get("advertiser") or item.get("advertiserInfo") or {}
            trade_methods = adv.get("tradeMethods") or item.get("tradeMethods") or []
            payment_methods = []
            for method in trade_methods:
                if isinstance(method, dict):
                    name = (
                        method.get("tradeMethodName")
                        or method.get("identifier")
                        or method.get("tradeMethodShortName")
                        or method.get("paymentMethod")
                    )
                    if name:
                        payment_methods.append(str(name))
                elif method:
                    payment_methods.append(str(method))

            merchant_days = 0
            register_time = advertiser.get("registerTime") or advertiser.get("registrationTime")
            if register_time:
                register_ts = float(register_time) / (1000 if float(register_time) > 10_000_000_000 else 1)
                merchant_days = max(
                    int((datetime.now(timezone.utc).timestamp() - register_ts) // 86_400),
                    0,
                )
            merchant_last_active_minutes = None
            active_seconds = advertiser.get("activeTimeInSecond")
            if active_seconds not in (None, ""):
                merchant_last_active_minutes = max(int(float(active_seconds)) // 60, 0)
            merchant_online = (
                merchant_last_active_minutes is not None and merchant_last_active_minutes <= 15
            )

            orders.append(
                P2POrder(
                    platform="binance",
                    order_id=str(adv.get("advNo") or adv.get("id") or ""),
                    side=response_side or canonical_side,
                    asset=response_asset,
                    fiat=response_fiat,
                    price=_decimal(adv.get("price")),
                    min_amount=_decimal(adv.get("minSingleTransAmount") or adv.get("minAmount")),
                    max_amount=_decimal(
                        adv.get("dynamicMaxSingleTransAmount")
                        or adv.get("maxSingleTransAmount")
                        or adv.get("maxAmount")
                    ),
                    available=_decimal(adv.get("surplusAmount") or adv.get("tradableQuantity") or "0"),
                    payment_methods=payment_methods,
                    merchant_id=str(
                        advertiser.get("userNo")
                        or advertiser.get("userId")
                        or item.get("sellerId")
                        or ""
                    ),
                    merchant_rating=_percentage(
                        advertiser.get("monthFinishRate")
                        or advertiser.get("positiveRate")
                        or advertiser.get("userTradeRate")
                    ),
                    merchant_orders=_int(
                        advertiser.get("monthOrderCount")
                        or advertiser.get("monthFinishOrderCount")
                        or advertiser.get("orderCount")
                        or advertiser.get("completedOrderNum")
                    ),
                    merchant_days=merchant_days,
                    merchant_name=str(
                        advertiser.get("nickName")
                        or advertiser.get("nickname")
                        or advertiser.get("realName")
                        or ""
                    ),
                    merchant_online=merchant_online if active_seconds not in (None, "") else None,
                    merchant_kyc=None,
                    merchant_last_active_minutes=merchant_last_active_minutes,
                    raw=item,
                )
            )
        return orders
