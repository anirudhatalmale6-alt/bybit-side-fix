from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class Trade:
    platform: str
    trade_id: str
    side: str
    fiat: str
    price: Decimal
    volume_usdt: Decimal
    volume_fiat: Decimal
    profit_usd: Decimal
    counterparty_id: str
    counterparty_rating: float
    status: str
    opened_at: datetime
    closed_at: datetime | None = None
