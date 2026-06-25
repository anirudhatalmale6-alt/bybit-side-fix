from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Event:
    type: str
    payload: Any
    created_at: datetime = field(default_factory=utc_now)
