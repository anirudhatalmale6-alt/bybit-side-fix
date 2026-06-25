from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class P2POrder:
    platform: str
    order_id: str
    side: str
    asset: str
    fiat: str
    price: Decimal
    min_amount: Decimal
    max_amount: Decimal
    available: Decimal
    payment_methods: list[str]
    merchant_id: str
    merchant_rating: float
    merchant_orders: int
    merchant_days: int
    merchant_name: str = ""
    merchant_online: bool | None = None
    merchant_kyc: bool | None = None
    merchant_last_active_minutes: int | None = None
    timestamp: datetime = field(default_factory=utc_now)
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def pair(self) -> str:
        return f"{self.asset}/{self.fiat}"

    def resolved_merchant_id(self) -> str:
        direct = str(self.merchant_id or "").strip()
        if direct:
            return direct
        raw = self.raw if isinstance(self.raw, dict) else {}
        platform = str(self.platform or "").strip().lower()
        if platform == "binance":
            advertiser = raw.get("advertiser") or {}
            return str(
                advertiser.get("userNo")
                or advertiser.get("userId")
                or raw.get("sellerId")
                or ""
            ).strip()
        if platform == "bybit":
            return str(raw.get("userId") or raw.get("accountId") or "").strip()
        if platform == "bingx":
            merchant_info = raw.get("merchantInfo") or {}
            return str(merchant_info.get("merchantUid") or "").strip()
        return ""

    def resolved_merchant_name(self) -> str:
        direct = str(self.merchant_name or "").strip()
        if direct:
            return direct
        raw = self.raw if isinstance(self.raw, dict) else {}
        platform = str(self.platform or "").strip().lower()
        if platform == "binance":
            advertiser = raw.get("advertiser") or {}
            return str(
                advertiser.get("nickName")
                or advertiser.get("nickname")
                or advertiser.get("realName")
                or ""
            ).strip()
        if platform == "bybit":
            return str(raw.get("nickName") or raw.get("userMaskId") or raw.get("userName") or "").strip()
        if platform == "bingx":
            merchant_info = raw.get("merchantInfo") or {}
            return str(
                merchant_info.get("nickname")
                or merchant_info.get("merchantName")
                or merchant_info.get("userName")
                or ""
            ).strip()
        return ""
