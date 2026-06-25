from __future__ import annotations

import hashlib
import hmac
import logging
import time
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import aiohttp

from p2p_bot.config import BingXConfig
from p2p_bot.utils.http import request_json


def _decimal(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


class BingXAPIClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: BingXConfig,
        logger: logging.Logger,
    ) -> None:
        self.session = session
        self.config = config
        self.logger = logger
        self._private_api_ready = False
        self._private_api_error: str | None = None

    @property
    def private_api_ready(self) -> bool:
        return self._private_api_ready

    @property
    def private_api_error(self) -> str | None:
        return self._private_api_error

    def capability_snapshot(self) -> dict[str, object]:
        return {
            "has_credentials": self.config.has_credentials,
            "private_api_ready": self._private_api_ready,
            "private_api_error": self._private_api_error,
            "fiat_site_url": self.config.fiat_site_url,
            "supported_payment_methods": list(self.config.supported_payment_methods),
        }

    async def verify_private_access(self) -> tuple[bool, str | None]:
        if not self.config.has_credentials:
            self._private_api_ready = False
            self._private_api_error = "missing API credentials"
            return False, self._private_api_error

        try:
            balances = await self.get_fund_balances()
        except Exception as exc:
            self._private_api_ready = False
            self._private_api_error = str(exc)
            return False, self._private_api_error

        self._private_api_ready = True
        asset_preview = ",".join(sorted(balances.keys())[:5]) or "no assets"
        self._private_api_error = f"verified via fund balance endpoint ({asset_preview})"
        return True, self._private_api_error

    async def get_fund_balances(self) -> dict[str, Decimal]:
        payload = await self._signed_request("GET", "/openApi/fund/v1/account/balance")
        if int(payload.get("code", -1)) != 0:
            raise RuntimeError(f"BingX API error: {payload}")
        assets = (payload.get("data") or {}).get("assets") or []
        balances: dict[str, Decimal] = {}
        for item in assets:
            asset = str(item.get("asset") or "").upper()
            if not asset:
                continue
            balances[asset] = _decimal(item.get("free"))
        return balances

    async def _signed_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.config.has_credentials:
            raise RuntimeError("BingX API credentials are missing")

        query_params = {**(params or {}), "timestamp": int(time.time() * 1000)}
        query_string = urlencode(query_params)
        signature = hmac.new(
            str(self.config.api_secret).encode(),
            query_string.encode(),
            hashlib.sha256,
        ).hexdigest()
        signed_url = f"{self.config.base_url.rstrip('/')}{path}?{query_string}&signature={signature}"
        return await request_json(
            self.session,
            method,
            signed_url,
            headers={"X-BX-APIKEY": str(self.config.api_key)},
            logger=self.logger,
        )
