from __future__ import annotations

import asyncio

import pytest

from p2p_bot.utils.http import HTTPClientError, request_json


class _FakeResponse:
    def __init__(self, status: int, body: str, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def text(self) -> str:
        return self._body


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def request(self, *args, **kwargs) -> _FakeResponse:
        self.calls.append((args, kwargs))
        return self._responses.pop(0)


def test_request_json_retries_with_retry_after_header(monkeypatch) -> None:
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("p2p_bot.utils.http.asyncio.sleep", fake_sleep)
    session = _FakeSession(
        [
            _FakeResponse(429, '{"error":"rate limited"}', headers={"Retry-After": "2"}),
            _FakeResponse(200, '{"ok": true}'),
        ]
    )

    result = asyncio.run(
        request_json(
            session,  # type: ignore[arg-type]
            "GET",
            "https://example.com/test",
            retries=1,
            backoff=(0.1,),
        )
    )

    assert result == {"ok": True}
    assert delays == [2.0]
    assert len(session.calls) == 2


def test_request_json_does_not_retry_non_retryable_http_error() -> None:
    session = _FakeSession([_FakeResponse(400, '{"error":"bad request"}')])

    with pytest.raises(HTTPClientError):
        asyncio.run(
            request_json(
                session,  # type: ignore[arg-type]
                "GET",
                "https://example.com/test",
                retries=3,
                backoff=(0.1,),
            )
        )

    assert len(session.calls) == 1
