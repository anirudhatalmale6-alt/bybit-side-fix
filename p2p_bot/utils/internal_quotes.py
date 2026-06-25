from __future__ import annotations

import hmac
import hashlib
import logging
import math
import time
import urllib.parse
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import aiohttp

from p2p_bot.config import Settings
from p2p_bot.utils.http import HTTPClientError, compact_json_dumps, request_json


@dataclass
class InternalQuote:
    platform: str
    venue: str
    from_asset: str
    to_asset: str
    from_amount: Decimal
    to_amount: Decimal
    rate: Decimal
    exact: bool


class InternalQuoteClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        settings: Settings,
        logger: logging.Logger,
    ) -> None:
        self.session = session
        self.settings = settings
        self.logger = logger
        self._cache_ttl_sec = max(1, int(settings.internal_quote_cache_ttl_sec))
        self._cache: dict[tuple[str, str, str, str], tuple[float, InternalQuote | None]] = {}

    async def best_quote(
        self,
        *,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> InternalQuote | None:
        candidates = await self.all_quotes(
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
        )
        return self._best_candidate(candidates)

    async def all_quotes(
        self,
        *,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> list[InternalQuote]:
        cache_key = (
            from_asset.upper(),
            to_asset.upper(),
            f"{from_amount.quantize(Decimal('0.01'))}",
            "best",
        )
        cached = self._cache_get(cache_key)
        if cache_key in self._cache and cached is not None:
            return [cached]
        if cache_key in self._cache and cached is None:
            return []

        candidates: list[InternalQuote] = []

        for candidate in (
            await self._binance_convert_quote(from_asset, to_asset, from_amount),
            await self._bybit_fiat_convert_quote(from_asset, to_asset, from_amount),
            await self._binance_spot_quote(from_asset, to_asset, from_amount),
            await self._bybit_spot_quote(from_asset, to_asset, from_amount),
            await self._bingx_spot_quote(from_asset, to_asset, from_amount),
        ):
            if candidate is not None and candidate.to_amount > 0:
                candidates.append(candidate)

        best = self._best_candidate(candidates)
        self._cache_set(cache_key, best)
        return candidates

    async def platform_quote(
        self,
        *,
        platform: str,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> InternalQuote | None:
        candidates = await self.platform_quotes(
            platform=platform,
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
        )
        return self._best_candidate(candidates)

    async def platform_quotes(
        self,
        *,
        platform: str,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> list[InternalQuote]:
        normalized = platform.lower()
        cache_key = (
            normalized,
            from_asset.upper(),
            to_asset.upper(),
            f"{from_amount.quantize(Decimal('0.01'))}",
        )
        cached = self._cache_get(cache_key)
        if cache_key in self._cache and cached is not None:
            return [cached]
        if cache_key in self._cache and cached is None:
            return []

        candidates: list[InternalQuote] = []
        checks: tuple[InternalQuote | None, ...]
        if normalized == "binance":
            checks = (
                await self._binance_convert_quote(from_asset, to_asset, from_amount),
                await self._binance_spot_quote(from_asset, to_asset, from_amount),
            )
        elif normalized == "bybit":
            checks = (
                await self._bybit_fiat_convert_quote(from_asset, to_asset, from_amount),
                await self._bybit_spot_quote(from_asset, to_asset, from_amount),
            )
        elif normalized == "bingx":
            checks = (
                await self._bingx_spot_quote(from_asset, to_asset, from_amount),
            )
        else:
            checks = ()
        for candidate in checks:
            if candidate is not None and candidate.to_amount > 0:
                candidates.append(candidate)
        best = self._best_candidate(candidates)
        self._cache_set(cache_key, best)
        return candidates

    @staticmethod
    def _best_candidate(candidates: list[InternalQuote]) -> InternalQuote | None:
        if not candidates:
            return None
        exact_candidates = [candidate for candidate in candidates if candidate.exact]
        pool = exact_candidates or candidates
        return max(pool, key=lambda item: item.to_amount)

    async def _binance_convert_quote(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> InternalQuote | None:
        api_key = self.settings.binance.api_key
        api_secret = self.settings.binance.api_secret
        if not api_key or not api_secret:
            return None

        params = {
            "fromAsset": from_asset.upper(),
            "toAsset": to_asset.upper(),
            "fromAmount": f"{from_amount.normalize()}",
            "timestamp": str(int(time.time() * 1000)),
        }
        query = urllib.parse.urlencode(params)
        signature = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"https://api.binance.com/sapi/v1/convert/getQuote?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": api_key}
        try:
            payload = await request_json(
                self.session,
                "POST",
                url,
                headers=headers,
                timeout=15.0,
                retries=0,
                logger=self.logger,
            )
        except HTTPClientError as exc:
            self.logger.debug("Binance Convert quote unavailable for %s->%s: %s", from_asset, to_asset, exc)
            return None

        to_amount = _to_decimal(payload.get("toAmount"))
        ratio = _to_decimal(payload.get("ratio")) or _to_decimal(payload.get("ratio", "0"))
        if to_amount <= 0:
            return None
        rate = ratio if ratio > 0 else (to_amount / from_amount)
        return InternalQuote(
            platform="binance",
            venue="convert",
            from_asset=from_asset.upper(),
            to_asset=to_asset.upper(),
            from_amount=from_amount,
            to_amount=to_amount,
            rate=rate,
            exact=True,
        )

    async def _bybit_fiat_convert_quote(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> InternalQuote | None:
        api_key = self.settings.bybit.api_key
        api_secret = self.settings.bybit.api_secret
        if not api_key or not api_secret:
            return None

        from_symbol = from_asset.upper()
        to_symbol = to_asset.upper()
        known_fiats = {fiat.upper() for fiat in self.settings.fiats}
        from_coin_type = "fiat" if from_symbol in known_fiats else "crypto"
        to_coin_type = "fiat" if to_symbol in known_fiats else "crypto"
        if from_coin_type == to_coin_type == "crypto":
            return None

        payload = {
            "fromCoin": from_symbol,
            "fromCoinType": from_coin_type,
            "toCoin": to_symbol,
            "toCoinType": to_coin_type,
            "requestAmount": f"{from_amount.normalize()}",
            "requestCoinType": from_coin_type,
        }
        body = compact_json_dumps(payload)
        timestamp = str(int(time.time() * 1000))
        recv_window = str(self.settings.bybit.recv_window_ms)
        signature_payload = f"{timestamp}{api_key}{recv_window}{body}".encode("utf-8")
        signature = hmac.new(
            api_secret.encode("utf-8"),
            signature_payload,
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json",
        }
        url = f"{self.settings.bybit.base_url.rstrip('/')}/v5/fiat/quote-apply"
        try:
            payload = await request_json(
                self.session,
                "POST",
                url,
                headers=headers,
                data=body,
                timeout=15.0,
                retries=0,
                logger=self.logger,
            )
        except HTTPClientError as exc:
            self.logger.debug(
                "Bybit fiat-convert quote unavailable for %s->%s: %s",
                from_asset,
                to_asset,
                exc,
            )
            return None

        code = payload.get("retCode", payload.get("ret_code", 0))
        if code != 0:
            self.logger.debug(
                "Bybit fiat-convert quote rejected for %s->%s: %s %s",
                from_asset,
                to_asset,
                code,
                payload.get("retMsg", payload.get("ret_msg", "")),
            )
            return None

        result = payload.get("result") or {}
        to_amount = _to_decimal(result.get("toAmount"))
        rate = _to_decimal(result.get("exchangeRate"))
        if to_amount <= 0:
            return None
        if rate <= 0:
            rate = to_amount / from_amount
        return InternalQuote(
            platform="bybit",
            venue="fiat-convert",
            from_asset=from_symbol,
            to_asset=to_symbol,
            from_amount=from_amount,
            to_amount=to_amount,
            rate=rate,
            exact=True,
        )

    async def _binance_spot_quote(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> InternalQuote | None:
        return await self._spot_quote(
            platform="binance",
            url_template="https://api.binance.com/api/v3/ticker/price",
            response_parser=_parse_binance_spot_ticker,
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
        )

    async def _bybit_spot_quote(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> InternalQuote | None:
        return await self._spot_quote(
            platform="bybit",
            url_template="https://api.bybit.com/v5/market/tickers",
            response_parser=_parse_bybit_spot_ticker,
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
        )

    async def _bingx_spot_quote(
        self,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> InternalQuote | None:
        return await self._spot_quote(
            platform="bingx",
            url_template="https://open-api.bingx.com/openApi/spot/v1/ticker/24hr",
            response_parser=_parse_bingx_spot_ticker,
            from_asset=from_asset,
            to_asset=to_asset,
            from_amount=from_amount,
        )

    async def _spot_quote(
        self,
        *,
        platform: str,
        url_template: str,
        response_parser,
        from_asset: str,
        to_asset: str,
        from_amount: Decimal,
    ) -> InternalQuote | None:
        direct_symbol = self._spot_symbol(platform, from_asset.upper(), to_asset.upper())
        inverse_symbol = self._spot_symbol(platform, to_asset.upper(), from_asset.upper())

        direct = await self._fetch_spot_price(
            platform=platform,
            url=url_template,
            symbol=direct_symbol,
            response_parser=response_parser,
        )
        inverse = await self._fetch_spot_price(
            platform=platform,
            url=url_template,
            symbol=inverse_symbol,
            response_parser=response_parser,
        )

        if direct is None and inverse is None:
            return None

        if direct is not None:
            rate = direct
        else:
            if inverse is None or inverse <= 0:
                return None
            rate = Decimal("1") / inverse

        to_amount = from_amount * rate
        return InternalQuote(
            platform=platform,
            venue="spot",
            from_asset=from_asset.upper(),
            to_asset=to_asset.upper(),
            from_amount=from_amount,
            to_amount=to_amount,
            rate=rate,
            exact=False,
        )

    async def _fetch_spot_price(
        self,
        *,
        platform: str,
        url: str,
        symbol: str,
        response_parser,
    ) -> Decimal | None:
        cache_key = (platform, symbol, "spot", "0")
        cached = self._cache_get(cache_key)
        if cache_key in self._cache:
            return cached.rate if cached is not None else None

        params: dict[str, Any]
        if platform == "bybit":
            params = {"category": "spot", "symbol": symbol}
        else:
            params = {"symbol": symbol}
        try:
            payload = await request_json(
                self.session,
                "GET",
                url,
                params=params,
                timeout=10.0,
                retries=0,
                logger=self.logger,
            )
        except HTTPClientError:
            self._cache_set(cache_key, None)
            return None

        price = response_parser(payload)
        self._cache_set(
            cache_key,
            None
            if price is None
            else InternalQuote(
                platform=platform,
                venue="spot",
                from_asset=symbol,
                to_asset=symbol,
                from_amount=Decimal("1"),
                to_amount=price,
                rate=price,
                exact=False,
            ),
        )
        return price

    @staticmethod
    def _spot_symbol(platform: str, base_asset: str, quote_asset: str) -> str:
        normalized = platform.lower()
        if normalized == "bingx":
            return f"{base_asset}-{quote_asset}"
        return f"{base_asset}{quote_asset}"

    def _cache_get(self, key: tuple[str, str, str, str]) -> InternalQuote | None:
        item = self._cache.get(key)
        if item is None:
            return None
        created_at, value = item
        if math.isfinite(created_at) and (time.monotonic() - created_at) <= self._cache_ttl_sec:
            return value
        self._cache.pop(key, None)
        return None

    def _cache_set(self, key: tuple[str, str, str, str], value: InternalQuote | None) -> None:
        self._cache[key] = (time.monotonic(), value)


def _to_decimal(value: Any) -> Decimal:
    try:
        if value is None or value == "":
            return Decimal("0")
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _parse_binance_spot_ticker(payload: dict[str, Any]) -> Decimal | None:
    if "price" not in payload:
        return None
    price = _to_decimal(payload.get("price"))
    return price if price > 0 else None


def _parse_bybit_spot_ticker(payload: dict[str, Any]) -> Decimal | None:
    result = payload.get("result")
    if not isinstance(result, dict):
        return None
    rows = result.get("list")
    if not isinstance(rows, list) or not rows:
        return None
    price = _to_decimal(rows[0].get("lastPrice"))
    return price if price > 0 else None


def _parse_bingx_spot_ticker(payload: dict[str, Any]) -> Decimal | None:
    if int(payload.get("code", -1)) != 0:
        return None
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        return None
    row = rows[0] or {}
    price = _to_decimal(row.get("lastPrice") or row.get("bidPrice") or row.get("askPrice"))
    return price if price > 0 else None
