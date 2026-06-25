from __future__ import annotations

import hashlib
import json
import logging
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

import aiohttp

from p2p_bot.config import BingXConfig
from p2p_bot.models.order import P2POrder
from p2p_bot.utils.canonical_side import (
    bingx_request_type,
    canonical_side_from_bingx_response,
    normalize_side,
)
from p2p_bot.utils.http import AsyncRateLimiter, request_json

_SIGN_KEY_PREFIX = "95d65c73dc5c4370ae9018fb7f2eab69"


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


def _stable_json_dumps(payload: Any) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).replace("\u2028", "\\u2028").replace("\u2029", "\\u2029").replace("\u0027", "\\u0027")


def _clean_object(payload: Any) -> Any:
    if isinstance(payload, dict):
        for key in list(payload.keys()):
            value = payload[key]
            if isinstance(value, list):
                if value:
                    _clean_object(value)
            elif isinstance(value, dict):
                _clean_object(value)
            if (
                (isinstance(value, dict) and not value)
                or value is None
                or (isinstance(value, float) and str(value) == "nan")
            ):
                del payload[key]
        return payload
    if isinstance(payload, list):
        index = 0
        while index < len(payload):
            value = payload[index]
            if isinstance(value, list):
                if value:
                    _clean_object(value)
            elif isinstance(value, dict):
                _clean_object(value)
            if (
                (isinstance(value, dict) and not value)
                or value is None
                or (isinstance(value, float) and str(value) == "nan")
            ):
                payload.pop(index)
                continue
            index += 1
        return payload
    return payload


def _normalize_for_sign(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {key: _normalize_for_sign(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_normalize_for_sign(item) for item in payload]
    if isinstance(payload, bool):
        return "true" if payload else "false"
    if isinstance(payload, (int, float)):
        return str(payload).upper()
    return payload


class BingXP2PClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        config: BingXConfig,
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
        if fiat.upper() not in self.config.supported_fiats:
            return []
        await self.rate_limiter.acquire()

        canonical_side = normalize_side(side)
        request_type = self._request_type_for_side(canonical_side)
        self.logger.debug(
            "BingX ads fetch asset=%s fiat=%s side=%s request_type=%s",
            asset,
            fiat,
            canonical_side,
            request_type,
        )
        payload = {
            "type": request_type,
            "fiat": fiat.upper(),
            "asset": asset.upper(),
            "pageId": 1,
            "pageSize": size or self.config.rows,
            "paymentMethodIds": [],
            "paymentTimeLimits": [0],
            "sortType": 0,
            "amount": "",
            "advertFilter": {
                "verifiedMerchantOnly": 0,
                "tradedWithMerchantOnly": 0,
                "noPaymentMethodVerification": 0,
                "matchUserCondition": 0,
            },
        }
        response = await self._request_p2p("POST", "/c2c/v3/advert/list", payload=payload)
        data = response.get("data") or {}
        items = data.get("result") or []
        orders: list[P2POrder] = []
        for item in items:
            parsed = self._parse_order(item, side=canonical_side, asset=asset, fiat=fiat, context=data)
            if parsed is not None:
                orders.append(parsed)
        return orders

    async def get_payment_methods(
        self,
        *,
        asset: str,
        fiat: str,
        side: str,
    ) -> dict[str, list[dict[str, Any]]]:
        if fiat.upper() not in self.config.supported_fiats:
            return {
                "frequentPaymentMethods": [],
                "otherPaymentMethods": [],
            }
        request_type = self._request_type_for_side(side)
        params = {
            "fiat": fiat.upper(),
            "asset": asset.upper(),
            "type": request_type,
        }
        response = await self._request_p2p("GET", "/c2c/v3/advert/payment/list", params=params)
        data = response.get("data") or {}
        return {
            "frequentPaymentMethods": list(data.get("frequentPaymentMethods") or []),
            "otherPaymentMethods": list(data.get("otherPaymentMethods") or []),
        }

    def _request_type_for_side(self, side: str) -> int:
        return bingx_request_type(normalize_side(side))

    async def _request_p2p(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        timestamp = str(timestamp_ms)
        trace_id = uuid.uuid4().hex
        device_id = uuid.uuid4().hex
        anti_device_id = ""
        request_payload = payload if payload is not None else params or {}
        sign = self._build_sign(
            timestamp=timestamp,
            trace_id=trace_id,
            device_id=device_id,
            request_payload=request_payload,
            anti_device_id=anti_device_id,
        )
        url = f"{self.config.p2p_api_base.rstrip('/')}{path}"
        headers = self._build_headers(
            timestamp=timestamp,
            trace_id=trace_id,
            device_id=device_id,
            sign=sign,
            anti_device_id=anti_device_id,
            referer_payload=request_payload,
            has_body=payload is not None,
        )
        response = await request_json(
            self.session,
            method,
            url,
            headers=headers,
            params=params if payload is None else None,
            data=_stable_json_dumps(payload).encode("utf-8") if payload is not None else None,
            logger=self.logger,
        )
        if int(response.get("code", -1)) != 0:
            raise RuntimeError(f"BingX P2P API error: {response}")
        return response

    def _build_sign(
        self,
        *,
        timestamp: str,
        trace_id: str,
        device_id: str,
        request_payload: dict[str, Any],
        anti_device_id: str,
    ) -> str:
        prepared_payload = _normalize_for_sign(_clean_object(deepcopy(request_payload)))
        payload_string = _stable_json_dumps(prepared_payload) if prepared_payload else "{}"
        sign_content = (
            f"{_SIGN_KEY_PREFIX}"
            f"{timestamp}"
            f"{trace_id}"
            f"{device_id}"
            f"{self.config.p2p_platform_id}"
            f"{self.config.p2p_app_version}"
            f"{anti_device_id}"
            f"{payload_string}"
        )
        return hashlib.sha256(sign_content.encode("utf-8")).hexdigest().upper()

    def _build_headers(
        self,
        *,
        timestamp: str,
        trace_id: str,
        device_id: str,
        sign: str,
        anti_device_id: str,
        referer_payload: dict[str, Any],
        has_body: bool,
    ) -> dict[str, str]:
        site_url = self.config.fiat_site_url.rstrip("/")
        parsed_site = urlparse(site_url)
        origin = f"{parsed_site.scheme}://{parsed_site.netloc}" if parsed_site.scheme and parsed_site.netloc else "https://paycat.com"
        fiat = str(referer_payload.get("fiat") or "USD").upper()
        request_type = int(referer_payload.get("type") or 1)
        type_hint = "buy" if request_type == 1 else "sell"
        headers = {
            "accept": "application/json, text/plain, */*",
            "origin": origin,
            "referer": f"{origin}/en/p2p?fiat={fiat}&type={type_hint}",
            "lang": "en",
            "timestamp": timestamp,
            "traceId": trace_id,
            "device_id": device_id,
            "platformId": str(self.config.p2p_platform_id),
            "app_version": self.config.p2p_app_version,
            "antiDeviceId": anti_device_id,
            "sign": sign,
            "appId": str(self.config.p2p_main_app_id),
            "mainAppId": str(self.config.p2p_main_app_id),
            "channel": "official",
            "reg_channel": "official",
            "user-agent": "Mozilla/5.0",
        }
        if has_body:
            headers["content-type"] = "application/json"
        return headers

    def _parse_order(
        self,
        item: dict[str, Any],
        *,
        side: str,
        asset: str,
        fiat: str,
        context: dict[str, Any],
    ) -> P2POrder | None:
        response_side = canonical_side_from_bingx_response(item.get("type"))
        response_asset = str(item.get("asset") or asset).upper()
        response_fiat = str(item.get("fiat") or fiat).upper()
        if response_asset != asset.upper() or response_fiat != fiat.upper():
            self.logger.warning(
                "Skipping BingX ad %s due to pair mismatch: requested=%s/%s response=%s/%s",
                item.get("advertNo") or item.get("orderNo") or "",
                asset.upper(),
                fiat.upper(),
                response_asset,
                response_fiat,
            )
            return None
        if response_side is not None and response_side != side:
            self.logger.warning(
                "Skipping BingX ad %s due to side mismatch: requested=%s response=%s raw_type=%s",
                item.get("advertNo") or item.get("orderNo") or "",
                side,
                response_side,
                item.get("type"),
            )
            return None
        merchant_info = item.get("merchantInfo") or {}
        merchant_stat = item.get("merchantStat") or {}
        payment_method_list = item.get("paymentMethodList") or []
        payment_methods = [
            str(method.get("name"))
            for method in payment_method_list
            if isinstance(method, dict) and method.get("name")
        ]
        merchant_online = merchant_info.get("onlineStatus")
        if merchant_online not in (True, False):
            merchant_online = None
        merchant_kyc = None
        if merchant_info.get("kycType") not in (None, "", 0) or merchant_info.get("verificationType") not in (None, "", 0):
            merchant_kyc = True
        raw_item = dict(item)
        raw_item["_response_context"] = {
            "priceSnapshotId": context.get("priceSnapshotId"),
            "c2cPlatformExtraHint": context.get("c2cPlatformExtraHint"),
        }
        return P2POrder(
            platform="bingx",
            order_id=str(item.get("advertNo") or item.get("orderNo") or ""),
            side=response_side or side,
            asset=response_asset,
            fiat=response_fiat,
            price=_decimal(item.get("price")),
            min_amount=_decimal(item.get("minAmount")),
            max_amount=_decimal(item.get("maxAmount")),
            available=_decimal(item.get("availableNumber")),
            payment_methods=payment_methods,
            merchant_id=str(merchant_info.get("merchantUid") or ""),
            merchant_rating=_percentage(merchant_stat.get("latestTradeSuccessRate")),
            merchant_orders=_int(merchant_stat.get("latestSuccessOrderCount")),
            merchant_days=0,
            merchant_name=str(
                merchant_info.get("nickname")
                or merchant_info.get("merchantName")
                or merchant_info.get("userName")
                or ""
            ),
            merchant_online=merchant_online,
            merchant_kyc=merchant_kyc,
            merchant_last_active_minutes=0 if merchant_online is True else None,
            raw=raw_item,
        )
