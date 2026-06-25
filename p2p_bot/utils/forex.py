from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import aiohttp

from p2p_bot.utils.http import AsyncRateLimiter, request_json


class ForexClient:
    PRIMARY_URL = "https://api.exchangerate-api.com/v4/latest/EUR"
    FALLBACK_BASE_URL = "https://api.frankfurter.app/latest"

    def __init__(
        self,
        session: aiohttp.ClientSession,
        logger: logging.Logger,
        cache_minutes: int = 5,
    ) -> None:
        self.session = session
        self.logger = logger
        self.cache_ttl = timedelta(minutes=cache_minutes)
        self._cached_rate: Decimal | None = None
        self._cached_at: datetime | None = None
        self._cached_base: str | None = None
        self._cached_rates: dict[str, Decimal] | None = None
        self._rate_limiter = AsyncRateLimiter(max_calls=1, period_sec=5.0)

    async def get_eur_pln_rate(self, force: bool = False) -> Decimal:
        matrix = await self.get_rate_matrix(("EUR", "PLN"), force=force)
        rate = matrix.get(("EUR", "PLN"))
        if rate is None:
            raise RuntimeError("Unable to fetch EUR/PLN forex rate.")
        self._cached_rate = rate
        return rate

    async def get_rate_matrix(
        self,
        fiats: tuple[str, ...],
        force: bool = False,
        *,
        base: str = "EUR",
    ) -> dict[tuple[str, str], Decimal]:
        normalized = tuple(sorted({fiat.upper() for fiat in fiats if fiat}))
        if base.upper() not in normalized:
            normalized = tuple(sorted(set(normalized) | {base.upper()}))

        rates = await self._get_base_rates(base.upper(), normalized, force=force)
        matrix: dict[tuple[str, str], Decimal] = {}
        currencies = set(normalized)
        currencies.add(base.upper())
        for source in currencies:
            for target in currencies:
                if source == target:
                    matrix[(source, target)] = Decimal("1")
                    continue
                source_rate = Decimal("1") if source == base.upper() else rates.get(source)
                target_rate = Decimal("1") if target == base.upper() else rates.get(target)
                if source_rate is None or target_rate is None or source_rate <= 0:
                    continue
                matrix[(source, target)] = (target_rate / source_rate).quantize(Decimal("0.000001"))
        return matrix

    async def _get_base_rates(
        self,
        base: str,
        fiats: tuple[str, ...],
        *,
        force: bool = False,
    ) -> dict[str, Decimal]:
        if (
            not force
            and self._cached_at is not None
            and self._cached_rates is not None
            and self._cached_base == base
            and datetime.now(timezone.utc) - self._cached_at < self.cache_ttl
            and all(fiat == base or fiat in self._cached_rates for fiat in fiats)
        ):
            return self._cached_rates

        await self._rate_limiter.acquire()
        fallback_symbols = ",".join(fiat for fiat in fiats if fiat != base)
        fallback_params = {"from": base}
        if fallback_symbols:
            fallback_params["to"] = fallback_symbols

        primary_payload = await request_json(
            self.session,
            "GET",
            self.PRIMARY_URL,
            logger=self.logger,
            retries=1,
            backoff=(2.0,),
        )
        rates_payload = primary_payload.get("rates") or {}
        primary_rates = {
            code.upper(): Decimal(str(value))
            for code, value in rates_payload.items()
            if value is not None
        }
        if all(fiat == base or fiat in primary_rates for fiat in fiats):
            self._cached_at = datetime.now(timezone.utc)
            self._cached_base = base
            self._cached_rates = primary_rates
            return primary_rates

        fallback_payload = await request_json(
            self.session,
            "GET",
            self.FALLBACK_BASE_URL,
            params=fallback_params,
            logger=self.logger,
            retries=1,
            backoff=(2.0,),
        )
        fallback_rates_payload = fallback_payload.get("rates") or {}
        fallback_rates = {
            code.upper(): Decimal(str(value))
            for code, value in fallback_rates_payload.items()
            if value is not None
        }
        if all(fiat == base or fiat in fallback_rates for fiat in fiats):
            self._cached_at = datetime.now(timezone.utc)
            self._cached_base = base
            self._cached_rates = fallback_rates
            return fallback_rates

        raise RuntimeError(f"Unable to fetch FX rates for: {', '.join(fiats)}")
