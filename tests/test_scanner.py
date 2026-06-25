from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

from p2p_bot.config import Settings
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.modules.scanner import FetchResult, Scanner
from p2p_bot.state import AppState


def _settings() -> Settings:
    return Settings.from_env(Path.cwd())


@contextmanager
def _event_loop():
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        yield loop
    finally:
        asyncio.set_event_loop(None)
        loop.close()


class _DummyRepo:
    async def save_batch(self, orders):
        return None


class _DummyBinanceClient:
    async def fetch_orders(self, asset: str, fiat: str, side: str):
        return []


class _DummyBingXClient:
    async def fetch_orders(self, asset: str, fiat: str, side: str):
        return []


class _DummyBybitClient:
    async def get_online_ads(self, asset: str, fiat: str, side: str):
        return []


def _scanner(settings: Settings) -> Scanner:
    state = AppState()
    return Scanner(
        settings=settings,
        state=state,
        risk_guard=RiskGuard(settings, state),
        raw_order_repository=_DummyRepo(),  # type: ignore[arg-type]
        binance_client=_DummyBinanceClient(),  # type: ignore[arg-type]
        bingx_client=_DummyBingXClient(),  # type: ignore[arg-type]
        bybit_client=_DummyBybitClient(),  # type: ignore[arg-type]
        scan_queue=asyncio.Queue(),
        alert_queue=asyncio.Queue(),
        logger=logging.getLogger("test.scanner"),
    )


def test_safe_fetch_timeout_returns_empty_and_records_error(monkeypatch) -> None:
    settings = replace(_settings(), scanner_fetch_timeout_sec=10)
    with _event_loop() as loop:
        scanner = _scanner(settings)

        async def fake_wait_for(awaitable, timeout):
            awaitable.close()
            raise asyncio.TimeoutError

        monkeypatch.setattr("p2p_bot.modules.scanner.asyncio.wait_for", fake_wait_for)

        result = loop.run_until_complete(scanner._safe_fetch("binance", "USDT", "EUR", "sell"))

        assert result.orders == []
        assert result.ok is False
        assert scanner.risk_guard.consecutive_api_errors == 0


def test_fiats_for_cycle_keeps_core_and_rotates_rest() -> None:
    base_settings = _settings()
    settings = replace(
        base_settings,
        scan_interval_sec=30,
        enabled_platforms=("bybit",),
        fiats=("PLN", "EUR", "RON", "GBP", "CZK", "HUF"),
        pairs=tuple(("USDT", fiat) for fiat in ("PLN", "EUR", "RON", "GBP", "CZK", "HUF")),
        bybit=replace(
            base_settings.bybit,
            api_key=None,
            api_secret=None,
            use_official_p2p_api=False,
            use_legacy_public_scan=True,
        ),
    )
    with _event_loop():
        scanner = _scanner(settings)

        first_cycle = scanner._fiats_for_cycle("bybit")
        second_cycle = scanner._fiats_for_cycle("bybit")

    assert first_cycle == ["PLN", "EUR", "RON", "GBP", "CZK"]
    assert second_cycle == ["PLN", "EUR", "HUF", "RON", "GBP"]


def test_bybit_budget_respects_timeout_and_concurrency() -> None:
    base_settings = _settings()
    settings = replace(
        base_settings,
        scan_interval_sec=30,
        scanner_fetch_timeout_sec=10,
        enabled_platforms=("bybit",),
        bybit=replace(
            base_settings.bybit,
            api_key="key",
            api_secret="secret",
            use_official_p2p_api=True,
            use_legacy_public_scan=False,
        ),
    )
    with _event_loop():
        scanner = _scanner(settings)

        assert scanner._platform_request_budget("bybit") == 6
        assert scanner._max_fiats_per_cycle("bybit") == 3


def test_scan_once_uses_platform_specific_fiat_windows(monkeypatch) -> None:
    base_settings = _settings()
    fiats = ("PLN", "EUR", "RON", "GBP", "CZK", "HUF")
    settings = replace(
        base_settings,
        scan_interval_sec=30,
        scanner_fetch_timeout_sec=10,
        enabled_platforms=("binance", "bybit"),
        fiats=fiats,
        pairs=tuple(("USDT", fiat) for fiat in fiats),
        bybit=replace(
            base_settings.bybit,
            api_key="key",
            api_secret="secret",
            use_official_p2p_api=True,
            use_legacy_public_scan=False,
        ),
        binance=replace(
            base_settings.binance,
            rate_limit_max_calls=2,
            rate_limit_period_sec=1.0,
        ),
    )
    with _event_loop() as loop:
        scanner = _scanner(settings)
        calls: list[tuple[str, str, str, str]] = []

        async def fake_safe_fetch(platform: str, asset: str, fiat: str, side: str):
            calls.append((platform, asset, fiat, side))
            return type(
                "R",
                (),
                {"platform": platform, "asset": asset, "fiat": fiat, "side": side, "orders": [], "ok": True},
            )()

        monkeypatch.setattr(scanner, "_safe_fetch", fake_safe_fetch)

        result = loop.run_until_complete(scanner.scan_once())

    assert result.total_fetches == 18
    bybit_calls = [call for call in calls if call[0] == "bybit"]
    binance_calls = [call for call in calls if call[0] == "binance"]
    assert len(binance_calls) == 12
    assert len(bybit_calls) == 6
    assert sorted({fiat for _, _, fiat, _ in bybit_calls}) == ["EUR", "PLN", "RON"]
    assert sorted({fiat for _, _, fiat, _ in binance_calls}) == sorted(fiats)


def test_bybit_dynamic_cap_grows_after_clean_cycle() -> None:
    base_settings = _settings()
    fiats = ("PLN", "EUR", "RON", "GBP", "CZK", "HUF", "SEK")
    settings = replace(
        base_settings,
        scan_interval_sec=30,
        scanner_fetch_timeout_sec=10,
        enabled_platforms=("bybit", "binance", "bingx"),
        fiats=fiats,
        pairs=tuple(("USDT", fiat) for fiat in fiats),
        bybit=replace(
            base_settings.bybit,
            api_key="key",
            api_secret="secret",
            use_official_p2p_api=True,
            use_legacy_public_scan=False,
        ),
        binance=replace(
            base_settings.binance,
            rate_limit_max_calls=2,
            rate_limit_period_sec=1.0,
        ),
    )
    with _event_loop():
        scanner = _scanner(settings)
        scanner._update_dynamic_fiat_caps(
            cycle_fiats_by_platform={"bybit": ["PLN", "EUR", "RON"]},
            batches=[
                FetchResult("bybit", "USDT", "PLN", "buy", [], True),
                FetchResult("bybit", "USDT", "PLN", "sell", [], True),
                FetchResult("bybit", "USDT", "EUR", "buy", [], True),
                FetchResult("bybit", "USDT", "EUR", "sell", [], True),
                FetchResult("bybit", "USDT", "RON", "buy", [], True),
                FetchResult("bybit", "USDT", "RON", "sell", [], True),
            ],
            fetch_elapsed_sec=24.0,
        )

        assert scanner._target_fiats_per_cycle("bybit") == 6
        assert scanner._fiats_for_cycle("bybit") == ["PLN", "EUR", "RON", "GBP", "CZK", "HUF"]


def test_bybit_dynamic_cap_backs_off_after_failures() -> None:
    base_settings = _settings()
    fiats = ("PLN", "EUR", "RON", "GBP", "CZK", "HUF", "SEK")
    settings = replace(
        base_settings,
        scan_interval_sec=30,
        scanner_fetch_timeout_sec=10,
        enabled_platforms=("bybit",),
        fiats=fiats,
        pairs=tuple(("USDT", fiat) for fiat in fiats),
        bybit=replace(
            base_settings.bybit,
            api_key="key",
            api_secret="secret",
            use_official_p2p_api=True,
            use_legacy_public_scan=False,
        ),
    )
    with _event_loop():
        scanner = _scanner(settings)
        scanner._platform_dynamic_fiat_caps["bybit"] = 7
        scanner._update_dynamic_fiat_caps(
            cycle_fiats_by_platform={"bybit": ["PLN", "EUR", "RON", "GBP", "CZK", "HUF", "SEK"]},
            batches=[
                FetchResult("bybit", "USDT", "PLN", "buy", [], True),
                FetchResult("bybit", "USDT", "PLN", "sell", [], False),
                FetchResult("bybit", "USDT", "EUR", "buy", [], True),
                FetchResult("bybit", "USDT", "EUR", "sell", [], False),
            ],
            fetch_elapsed_sec=31.0,
        )

        assert scanner._target_fiats_per_cycle("bybit") == 5


def test_scan_once_partial_failures_do_not_trigger_kill_switch(monkeypatch) -> None:
    settings = replace(
        _settings(),
        enabled_platforms=("binance",),
        fiats=("PLN", "EUR"),
        pairs=(("USDT", "PLN"), ("USDT", "EUR")),
    )
    with _event_loop() as loop:
        scanner = _scanner(settings)
        calls: list[tuple[str, str, str, str]] = []

        async def fake_safe_fetch(platform: str, asset: str, fiat: str, side: str):
            calls.append((platform, asset, fiat, side))
            if fiat == "PLN":
                return type("R", (), {"platform": platform, "asset": asset, "fiat": fiat, "side": side, "orders": [], "ok": False})()
            return type("R", (), {"platform": platform, "asset": asset, "fiat": fiat, "side": side, "orders": [], "ok": True})()

        monkeypatch.setattr(scanner, "_safe_fetch", fake_safe_fetch)

        result = loop.run_until_complete(scanner.scan_once())
        if result.successful_fetches > 0:
            scanner.risk_guard.record_api_success()

    assert len(calls) == 4
    assert result.successful_fetches == 2
    assert result.failed_fetches == 2
    assert scanner.risk_guard.consecutive_api_errors == 0
    assert scanner.risk_guard.can_trade() == (True, "OK")


def test_remaining_cycle_delay_uses_target_start_to_start_interval(monkeypatch) -> None:
    settings = replace(_settings(), scan_interval_sec=30)
    with _event_loop():
        scanner = _scanner(settings)

        class _FakeLoop:
            def time(self) -> float:
                return 130.0

        monkeypatch.setattr("p2p_bot.modules.scanner.asyncio.get_running_loop", lambda: _FakeLoop())

        assert scanner._remaining_cycle_delay(100.0) == 0.0
        assert scanner._remaining_cycle_delay(115.0) == 15.0
