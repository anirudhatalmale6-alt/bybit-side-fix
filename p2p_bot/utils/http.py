from __future__ import annotations

import asyncio
import json
import logging
import math
from collections import deque
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

from p2p_bot.utils.logger import redact_sensitive_text


_RETRYABLE_HTTP_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}


class HTTPClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        retry_after_sec: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.retry_after_sec = retry_after_sec


class ExchangeConfigurationError(RuntimeError):
    pass


class AsyncRateLimiter:
    def __init__(self, max_calls: int, period_sec: float) -> None:
        self.max_calls = max_calls
        self.period_sec = period_sec
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0) -> None:
        async with self._lock:
            while True:
                now = asyncio.get_running_loop().time()
                while self._timestamps and now - self._timestamps[0] >= self.period_sec:
                    self._timestamps.popleft()
                current_weight = float(len(self._timestamps))
                if current_weight + max(cost, 0.0) <= float(self.max_calls):
                    slots = max(1, math.ceil(cost))
                    for _ in range(slots):
                        self._timestamps.append(now)
                    return
                sleep_for = self.period_sec - (now - self._timestamps[0]) + 0.01
                await asyncio.sleep(max(sleep_for, 0.01))


def compact_json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _parse_retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return max(float(raw), 0.0)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return max(delay, 0.0)


async def request_json(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    data: str | bytes | None = None,
    timeout: float = 20.0,
    retries: int = 3,
    backoff: tuple[float, ...] = (5.0, 10.0, 30.0),
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    if json_body is not None and data is not None:
        raise ValueError("Use json_body or data, not both.")

    safe_url = redact_sensitive_text(url)
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            async with session.request(
                method.upper(),
                url,
                headers=headers,
                params=params,
                json=json_body,
                data=data,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                text = await response.text()
                if response.status >= 400:
                    retry_after_sec = _parse_retry_after_seconds(response.headers.get("Retry-After"))
                    raise HTTPClientError(
                        f"{response.status} {safe_url}: {text[:500]}",
                        retryable=response.status in _RETRYABLE_HTTP_STATUSES,
                        retry_after_sec=retry_after_sec,
                    )
                if not text:
                    return {}
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    return parsed
                raise HTTPClientError(f"Unexpected JSON payload from {safe_url}")
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, HTTPClientError) as exc:
            if isinstance(exc, HTTPClientError) and not exc.retryable:
                raise
            last_error = exc
            if attempt >= retries:
                break
            delay = backoff[min(attempt, len(backoff) - 1)]
            if isinstance(exc, HTTPClientError) and exc.retry_after_sec is not None:
                delay = max(delay, exc.retry_after_sec)
            if logger:
                logger.warning(
                    "HTTP retry %s/%s for %s %s after error: %s",
                    attempt + 1,
                    retries,
                    method.upper(),
                    safe_url,
                    exc,
                )
            await asyncio.sleep(delay)
    raise HTTPClientError(str(last_error))
