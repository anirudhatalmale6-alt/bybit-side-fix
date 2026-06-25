from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

import aiohttp

from p2p_bot.config import BybitConfig
from p2p_bot.models.order import P2POrder
from p2p_bot.utils.canonical_side import (
    bybit_api_side_code,
    canonical_side_from_bybit_response,
    normalize_side,
)
from p2p_bot.utils.http import AsyncRateLimiter, ExchangeConfigurationError, compact_json_dumps, request_json


def _decimal(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def _float_percentage(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    raw = float(value)
    return raw * 100 if 0 < raw <= 1 else raw


def _drop_none_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _drop_none_fields(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_drop_none_fields(item) for item in value]
    return value


class BybitP2PClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: BybitConfig,
        logger: logging.Logger,
    ) -> None:
        self.session = session
        self.config = config
        self.logger = logger
        self.official_rate_limiter = AsyncRateLimiter(max_calls=4, period_sec=1.0)
        self.legacy_rate_limiter = AsyncRateLimiter(max_calls=20, period_sec=60.0)
        self._payment_catalog: dict[str, str] = dict(config.payment_code_map)
        self._official_scan_disabled = False
        self.private_api_access_allowed = config.has_credentials
        self._public_payment_catalog_loaded = False
        self._public_payment_catalog_lock = asyncio.Lock()

    async def get_online_ads(
        self,
        asset: str,
        fiat: str,
        side: str,
        page: int = 1,
        size: int = 20,
    ) -> list[P2POrder]:
        canonical_side = normalize_side(side)
        # Keep official scan parameters as strings; the live Bybit endpoint rejects ints.
        payload = {
            "tokenId": asset,
            "currencyId": fiat,
            "side": bybit_api_side_code(canonical_side),
            "page": str(page),
            "size": str(size),
        }
        self.logger.debug(
            "Bybit online ads fetch asset=%s fiat=%s side=%s api_side=%s",
            asset,
            fiat,
            canonical_side,
            payload["side"],
        )

        if (
            self.config.use_official_p2p_api
            and self.config.has_credentials
            and self.private_api_access_allowed
            and not self._official_scan_disabled
        ):
            response = await self._try_official_online_ads(
                payload,
                asset=asset,
                fiat=fiat,
                side=canonical_side,
            )
            if response is not None:
                items = ((response.get("result") or {}).get("items")) or []
                if items and any(
                    str(value) not in self._payment_catalog
                    for item in items
                    for value in (item.get("payments") or [])
                ):
                    await self._load_payment_catalog()
                if items and any(
                    str(value) not in self._payment_catalog
                    for item in items
                    for value in (item.get("payments") or [])
                ):
                    await self._load_public_payment_catalog()
                orders = []
                for item in items:
                    parsed = self._parse_online_ad(item, canonical_side, asset, fiat)
                    if parsed is not None:
                        orders.append(parsed)
                return orders

        legacy_payload = {**payload, "amount": ""}
        response = await self._legacy_post(self.config.legacy_public_scan_url, legacy_payload)
        result = response.get("result") or response
        items = result.get("items") or result.get("data") or []
        if items and any(
            str(value) not in self._payment_catalog
            for item in items
            for value in (item.get("payments") or [])
        ):
            await self._load_public_payment_catalog()
        orders = []
        for item in items:
            parsed = self._parse_online_ad(item, canonical_side, asset, fiat)
            if parsed is not None:
                orders.append(parsed)
        return orders

    async def get_account_information(self) -> dict[str, Any]:
        return (await self._official_post("/v5/p2p/user/personal/info", {})).get("result") or {}

    async def has_private_p2p_access(self) -> tuple[bool, str | None]:
        if not self.config.has_credentials:
            self.private_api_access_allowed = False
            return False, "Missing API credentials"
        try:
            await self.get_account_information()
        except Exception as exc:
            self.private_api_access_allowed = False
            return False, str(exc)
        self.private_api_access_allowed = True
        return True, None

    async def get_coin_balance(
        self,
        *,
        account_type: str,
        coin: str,
        member_id: str | None = None,
        with_bonus: int = 0,
    ) -> dict[str, Any]:
        if not self.config.has_credentials:
            raise ExchangeConfigurationError("Bybit official P2P API requires API credentials.")
        query = {
            "accountType": account_type,
            "coin": coin,
        }
        if member_id is not None:
            query["memberId"] = member_id
        if with_bonus:
            query["withBonus"] = with_bonus
        return await self._official_get("/v5/asset/transfer/query-account-coins-balance", query)

    async def get_user_payment_types(self) -> list[dict[str, Any]]:
        response = await self._official_post("/v5/p2p/user/payment/list", {})
        result = response.get("result") or []
        if result:
            for item in result:
                name = (
                    ((item.get("paymentConfigVo") or {}).get("paymentName"))
                    or item.get("bankName")
                    or str(item.get("paymentType") or item.get("id") or "")
                )
                for key in (
                    item.get("id"),
                    item.get("paymentType"),
                    ((item.get("paymentConfigVo") or {}).get("paymentType")),
                ):
                    if key is not None:
                        self._payment_catalog[str(key)] = str(name)
        return result

    async def create_ad(self, payload: dict[str, Any]) -> dict[str, Any]:
        return (await self._official_post("/v5/p2p/item/create", payload)).get("result") or {}

    async def update_ad(self, payload: dict[str, Any]) -> dict[str, Any]:
        return (await self._official_post("/v5/p2p/item/update", payload)).get("result") or {}

    async def remove_ad(self, item_id: str) -> bool:
        await self._official_post("/v5/p2p/item/cancel", {"itemId": item_id})
        return True

    async def get_my_ads(
        self,
        *,
        item_id: str | None = None,
        status: str | None = "2",
        side: str | None = None,
        token_id: str | None = None,
        currency_id: str | None = None,
        page: str | None = None,
        size: str | None = None,
    ) -> list[dict[str, Any]]:
        payload = _drop_none_fields(
            {
                "itemId": item_id,
                "status": status,
                "side": side,
                "tokenId": token_id,
                "page": page,
                "size": size,
                "currencyId": currency_id,
            }
        )
        response = await self._official_post("/v5/p2p/item/personal/list", payload)
        return ((response.get("result") or {}).get("items")) or []

    async def get_orders(
        self,
        *,
        page: int = 1,
        size: int = 30,
        status: int | None = None,
        token_id: str | None = None,
        side: int | None = None,
    ) -> list[dict[str, Any]]:
        payload = _drop_none_fields(
            {
                "status": status,
                "beginTime": None,
                "endTime": None,
                "tokenId": token_id,
                "side": side,
                "page": page,
                "size": size,
            }
        )
        response = await self._official_post("/v5/p2p/order/simplifyList", payload)
        return ((response.get("result") or {}).get("items")) or []

    async def get_order_detail(self, order_id: str) -> dict[str, Any]:
        response = await self._official_post("/v5/p2p/order/info", {"orderId": order_id})
        return response.get("result") or {}

    async def release_assets(self, order_id: str) -> bool:
        await self._official_post("/v5/p2p/order/finish", {"orderId": order_id})
        return True

    async def mark_as_paid(
        self,
        order_id: str,
        payment_type: str,
        payment_id: str,
    ) -> bool:
        await self._official_post(
            "/v5/p2p/order/pay",
            {
                "orderId": order_id,
                "paymentType": payment_type,
                "paymentId": payment_id,
            },
        )
        return True

    async def _load_payment_catalog(self) -> None:
        if not self.config.has_credentials:
            return
        try:
            await self.get_user_payment_types()
        except Exception as exc:
            self.logger.warning("Unable to warm up Bybit payment catalog: %s", exc)

    async def _load_public_payment_catalog(self) -> None:
        if self._public_payment_catalog_loaded:
            return

        async with self._public_payment_catalog_lock:
            if self._public_payment_catalog_loaded:
                return

            catalog_url = self._legacy_public_catalog_url()
            if not catalog_url:
                return

            try:
                response = await self._legacy_post(catalog_url, {})
                result = response.get("result") or {}
                payment_configs = result.get("paymentConfigVo") or []
                loaded = 0
                for item in payment_configs:
                    payment_type = item.get("paymentType")
                    payment_name = item.get("paymentName")
                    if payment_type in (None, "") or not payment_name:
                        continue
                    self._payment_catalog[str(payment_type)] = str(payment_name)
                    loaded += 1
                self._public_payment_catalog_loaded = True
                if loaded:
                    self.logger.info("Loaded %s Bybit public payment codes", loaded)
            except Exception as exc:
                self.logger.warning("Unable to warm up Bybit public payment catalog: %s", exc)

    def _legacy_public_catalog_url(self) -> str:
        parsed = urlparse(self.config.legacy_public_scan_url)
        if not parsed.scheme or not parsed.netloc:
            return ""
        return f"{parsed.scheme}://{parsed.netloc}/fiat/otc/configuration/queryAllPaymentList"

    async def _try_official_online_ads(
        self,
        payload: dict[str, Any],
        *,
        asset: str,
        fiat: str,
        side: str,
    ) -> dict[str, Any] | None:
        try:
            return await self._official_post("/v5/p2p/item/online", payload)
        except Exception as exc:
            if self._is_permission_denied(exc):
                self._official_scan_disabled = True
                self.private_api_access_allowed = False
                self.logger.warning(
                    "Bybit official P2P scan unavailable, falling back to legacy public scan: %s",
                    exc,
                )
                return None
            if not self._is_transient_scan_error(exc):
                raise

            retry_delay = self._transient_retry_delay(exc)
            if retry_delay > 0:
                await asyncio.sleep(retry_delay)
            try:
                return await self._official_post("/v5/p2p/item/online", payload)
            except Exception as retry_exc:
                if self._is_permission_denied(retry_exc):
                    self._official_scan_disabled = True
                    self.private_api_access_allowed = False
                    self.logger.warning(
                        "Bybit official P2P scan unavailable, falling back to legacy public scan: %s",
                        retry_exc,
                    )
                    return None
                if not self._is_transient_scan_error(retry_exc):
                    raise
                self.logger.warning(
                    "Bybit official P2P scan temporary issue for %s/%s %s, "
                    "falling back to legacy public scan for this request: %s",
                    asset,
                    fiat,
                    side,
                    retry_exc,
                )
                return None

    @staticmethod
    def _is_transient_scan_error(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(
            marker in text
            for marker in (
                "10002",
                "10006",
                "recv_window",
                "server timestamp",
                "too many visits",
                "rate limit",
                "429",
            )
        )

    @staticmethod
    def _transient_retry_delay(exc: Exception) -> float:
        text = str(exc).lower()
        if "10006" in text or "too many visits" in text or "rate limit" in text or "429" in text:
            return 1.0
        if "10002" in text or "recv_window" in text or "server timestamp" in text:
            return 0.3
        return 0.0

    async def _official_post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.config.has_credentials:
            raise ExchangeConfigurationError("Bybit official P2P API requires API credentials.")

        await self.official_rate_limiter.acquire()
        body = compact_json_dumps(_drop_none_fields(payload))
        timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        signature_payload = (
            f"{timestamp}{self.config.api_key}{self.config.recv_window_ms}{body}".encode("utf-8")
        )
        signature = hmac.new(
            (self.config.api_secret or "").encode("utf-8"),
            signature_payload,
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.config.api_key or "",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": str(self.config.recv_window_ms),
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json",
        }
        response = await request_json(
            self.session,
            "POST",
            f"{self.config.base_url.rstrip('/')}{path}",
            headers=headers,
            data=body,
            logger=self.logger,
        )
        code = response.get("retCode", response.get("ret_code", 0))
        if code != 0:
            message = response.get("retMsg", response.get("ret_msg", "Unknown Bybit error"))
            raise RuntimeError(f"Bybit P2P API error {code}: {message}")
        return response

    async def _official_get(self, path: str, query: dict[str, Any]) -> dict[str, Any]:
        if not self.config.has_credentials:
            raise ExchangeConfigurationError("Bybit official P2P API requires API credentials.")

        await self.official_rate_limiter.acquire()
        query_string = "&".join(
            f"{key}={value}"
            for key, value in query.items()
            if value is not None
        )
        timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        signature_payload = (
            f"{timestamp}{self.config.api_key}{self.config.recv_window_ms}{query_string}".encode("utf-8")
        )
        signature = hmac.new(
            (self.config.api_secret or "").encode("utf-8"),
            signature_payload,
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.config.api_key or "",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": str(self.config.recv_window_ms),
            "X-BAPI-SIGN": signature,
        }
        response = await request_json(
            self.session,
            "GET",
            f"{self.config.base_url.rstrip('/')}{path}",
            headers=headers,
            params={key: value for key, value in query.items() if value is not None},
            logger=self.logger,
        )
        code = response.get("retCode", response.get("ret_code", 0))
        if code != 0:
            message = response.get("retMsg", response.get("ret_msg", "Unknown Bybit error"))
            raise RuntimeError(f"Bybit API error {code}: {message}")
        return response.get("result") or {}

    async def _legacy_post(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        await self.legacy_rate_limiter.acquire()
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        if self.config.cookies:
            headers["Cookie"] = self.config.cookies
        return await request_json(
            self.session,
            "POST",
            url,
            headers=headers,
            json_body=payload,
            logger=self.logger,
        )

    def _parse_online_ad(
        self,
        item: dict[str, Any],
        requested_side: str,
        asset: str,
        fiat: str,
    ) -> P2POrder | None:
        expected_raw_side = bybit_api_side_code(requested_side)
        raw_side = str(item.get("side")).strip() if item.get("side") is not None else ""
        response_side = canonical_side_from_bybit_response(item.get("side"))
        response_asset = str(item.get("tokenId") or asset).upper()
        response_fiat = str(item.get("currencyId") or fiat).upper()
        if response_asset != asset.upper() or response_fiat != fiat.upper():
            self.logger.warning(
                "Skipping Bybit ad %s due to pair mismatch: requested=%s/%s response=%s/%s",
                item.get("id") or "",
                asset.upper(),
                fiat.upper(),
                response_asset,
                response_fiat,
            )
            return None
        if raw_side and raw_side != expected_raw_side:
            self.logger.warning(
                "Skipping Bybit ad %s due to raw side mismatch: requested=%s expected_raw=%s response_raw=%s",
                item.get("id") or "",
                requested_side,
                expected_raw_side,
                raw_side,
            )
            return None
        if response_side is not None and response_side != requested_side:
            self.logger.warning(
                "Skipping Bybit ad %s due to side mismatch: requested=%s response=%s raw_side=%s",
                item.get("id") or "",
                requested_side,
                response_side,
                item.get("side"),
            )
            return None
        payment_values = item.get("payments") or []
        payment_methods = []
        for value in payment_values:
            payment_methods.append(self._payment_catalog.get(str(value), str(value)))
        trading_pref = item.get("tradingPreferenceSet") or {}
        merchant_kyc = None
        if item.get("authStatus") is not None or trading_pref.get("isKyc") is not None:
            merchant_kyc = bool(int(item.get("authStatus") or 0) == 1 or int(trading_pref.get("isKyc") or 0) == 1)
        merchant_last_active_minutes = None
        last_logout_time = item.get("lastLogoutTime")
        if last_logout_time not in (None, ""):
            try:
                logout_ts = float(last_logout_time)
                if logout_ts > 10_000_000_000:
                    logout_ts /= 1000
                merchant_last_active_minutes = max(
                    int((datetime.now(timezone.utc).timestamp() - logout_ts) // 60),
                    0,
                )
            except (TypeError, ValueError):
                merchant_last_active_minutes = None

        return P2POrder(
            platform="bybit",
            order_id=str(item.get("id") or ""),
            side=response_side or requested_side,
            asset=response_asset,
            fiat=response_fiat,
            price=_decimal(item.get("price")),
            min_amount=_decimal(item.get("minAmount")),
            max_amount=_decimal(item.get("maxAmount")),
            available=_decimal(item.get("lastQuantity") or item.get("quantity")),
            payment_methods=payment_methods,
            merchant_id=str(item.get("userId") or item.get("accountId") or ""),
            merchant_rating=_float_percentage(item.get("recentExecuteRate")),
            merchant_orders=int(float(item.get("recentOrderNum") or item.get("orderNum") or 0)),
            merchant_days=0,
            merchant_name=str(item.get("nickName") or item.get("userMaskId") or ""),
            merchant_online=bool(item.get("isOnline")) if item.get("isOnline") is not None else None,
            merchant_kyc=merchant_kyc,
            merchant_last_active_minutes=merchant_last_active_minutes,
            raw=item,
        )

    @staticmethod
    def _is_permission_denied(exc: Exception) -> bool:
        text = str(exc).lower()
        return (
            "10005" in text
            or "10010" in text
            or "permission denied" in text
            or "unmatched ip" in text
        )
