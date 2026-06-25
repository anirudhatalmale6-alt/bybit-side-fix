from __future__ import annotations

import asyncio
import json
import logging
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from p2p_bot.config import Settings
from p2p_bot.db.database import Database
from p2p_bot.db.repositories import ExecutionSessionRepository, InventoryRepository, SignalJournalRepository
from p2p_bot.models.opportunity import ArbitrageOpportunity
from p2p_bot.models.order import P2POrder
from p2p_bot.modules.execution_engine import ExchangeExecutionEngine


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


def _order(
    *,
    platform: str,
    side: str,
    fiat: str,
    price: str,
    order_id: str,
) -> P2POrder:
    return P2POrder(
        platform=platform,
        order_id=order_id,
        side=side,
        asset="USDT",
        fiat=fiat,
        price=Decimal(price),
        min_amount=Decimal("10"),
        max_amount=Decimal("10000"),
        available=Decimal("10000"),
        payment_methods=["Bank Transfer"],
        merchant_id="merchant-1",
        merchant_rating=99.0,
        merchant_orders=1000,
        merchant_days=365,
        merchant_online=True,
        merchant_kyc=True,
        raw={},
    )


def _opportunity(*, buy_order: P2POrder, sell_order: P2POrder) -> ArbitrageOpportunity:
    now = datetime.utcnow()
    return ArbitrageOpportunity(
        type="cross_platform",
        base_type="cross_platform",
        spread_pct=Decimal("2.00"),
        estimated_profit_usd=Decimal("10.00"),
        gross_profit_usd=Decimal("10.00"),
        total_fees_usd=Decimal("0"),
        buy_fee_usd=Decimal("0"),
        sell_fee_usd=Decimal("0"),
        fx_fee_usd=Decimal("0"),
        volume_usdt=Decimal("100.00"),
        buy_order=buy_order,
        sell_order=sell_order,
        detected_at=now,
        expires_at=now,
        rail_status="confirmed",
        buy_rail="bank_transfer",
        sell_rail="bank_transfer",
        fx_rail="",
        buy_user_method="Банковский перевод",
        sell_user_method="Банковский перевод",
        buy_payment_summary="Bank Transfer",
        sell_payment_summary="Bank Transfer",
    )


class _BybitStub:
    def __init__(self, orders: dict[tuple[str, str, str], list[P2POrder]] | None = None) -> None:
        self.config = type("Config", (), {"has_credentials": False})()
        self.private_api_access_allowed = False
        self.orders = orders or {}

    async def get_online_ads(self, asset: str, fiat: str, side: str) -> list[P2POrder]:
        return list(self.orders.get((asset.upper(), fiat.upper(), side.lower()), []))


class _BinanceStub:
    def __init__(self, orders: dict[tuple[str, str, str], list[P2POrder]] | None = None) -> None:
        self.orders = orders or {}

    async def fetch_orders(self, asset: str, fiat: str, side: str) -> list[P2POrder]:
        return list(self.orders.get((asset.upper(), fiat.upper(), side.lower()), []))


def test_inventory_apply_signal_execution_uses_sell_platform_for_fx_bucket(tmp_path) -> None:
    db = Database(tmp_path / "inventory.sqlite3")
    signal = {
        "id": 1,
        "payload_json": json.dumps(
            {
                "buy_rail": "revolut_balance",
                "sell_rail": "fiat_balance",
                "fx_rail": "fiat_balance",
                "buy_order": {
                    "platform": "bybit",
                    "fiat": "EUR",
                    "price": "1.00",
                    "asset": "USDT",
                },
                "sell_order": {
                    "platform": "binance",
                    "fiat": "GBP",
                    "price": "1.20",
                    "asset": "USDT",
                },
            }
        ),
        "volume_usdt": "100",
        "estimated_profit_usd": "10",
    }

    with _event_loop() as loop:
        loop.run_until_complete(db.initialize())
        inventory_repository = InventoryRepository(db)
        loop.run_until_complete(inventory_repository.apply_signal_execution(signal))
        rows = loop.run_until_complete(
            db.fetchall(
                """
                SELECT location, asset, reason
                FROM inventory_ledger
                ORDER BY id
                """
            )
        )

    ledger = [dict(row) for row in rows]
    assert any(
        row["location"] == "binance:fiat_balance" and row["reason"] == "signal_fx_source"
        for row in ledger
    )
    assert any(
        row["location"] == "binance:fiat_balance" and row["reason"] == "signal_fx_target"
        for row in ledger
    )
    assert all(row["location"] != "bybit:fiat_balance" for row in ledger)


def test_treasury_provider_uses_explicit_rails_not_counterparty_payment_names() -> None:
    signal = {
        "payload_json": json.dumps(
            {
                "buy_rail": "bank_transfer",
                "sell_rail": "bank_transfer",
                "fx_rail": "",
                "buy_order": {"payment_methods": ["Revolut", "Bank Transfer"]},
                "sell_order": {"payment_methods": ["Revolut"]},
            }
        )
    }

    assert ExchangeExecutionEngine._treasury_provider_for_signal(signal) == "bank"


def test_treasury_provider_marks_mixed_route_as_mixed() -> None:
    signal = {
        "payload_json": json.dumps(
            {
                "buy_rail": "wise_balance",
                "sell_rail": "revolut_balance",
                "fx_rail": "",
            }
        )
    }

    assert ExchangeExecutionEngine._treasury_provider_for_signal(signal) == "mixed"


def test_prepare_session_keeps_sell_side_as_source_for_rm_snapshot(tmp_path) -> None:
    settings = _settings()
    settings.exchange_execution.allowed_platforms = ("binance", "bybit")
    settings.exchange_execution.allowed_treasury_providers = ("bank", "revolut", "wise")

    db = Database(tmp_path / "execution.sqlite3")
    with _event_loop() as loop:
        loop.run_until_complete(db.initialize())
        signal_repository = SignalJournalRepository(db)
        execution_repository = ExecutionSessionRepository(db)
        inventory_repository = InventoryRepository(db)

        loop.run_until_complete(inventory_repository.set_balance("binance:wallet", "USDT", Decimal("500")))
        loop.run_until_complete(inventory_repository.set_balance("bybit:wallet", "USDT", Decimal("900")))
        loop.run_until_complete(inventory_repository.set_balance("bank", "EUR", Decimal("2000")))

        opportunity = _opportunity(
            buy_order=_order(platform="bybit", side="buy", fiat="EUR", price="1.00", order_id="buy-1"),
            sell_order=_order(platform="binance", side="sell", fiat="EUR", price="1.05", order_id="sell-1"),
        )
        signal_id = loop.run_until_complete(signal_repository.create_from_opportunity(opportunity))
        live_binance = _order(platform="binance", side="sell", fiat="EUR", price="1.05", order_id="sell-1")
        live_bybit = _order(platform="bybit", side="buy", fiat="EUR", price="1.00", order_id="buy-1")

        engine = ExchangeExecutionEngine(
            settings=settings,
            signal_journal_repository=signal_repository,
            execution_session_repository=execution_repository,
            inventory_repository=inventory_repository,
            bybit_client=_BybitStub({("USDT", "EUR", "buy"): [live_bybit]}),  # type: ignore[arg-type]
            binance_client=_BinanceStub({("USDT", "EUR", "sell"): [live_binance]}),  # type: ignore[arg-type]
            logger=logging.getLogger("test.execution"),
        )
        session_id = loop.run_until_complete(engine.prepare_session(signal_id))
        session = loop.run_until_complete(execution_repository.get(session_id))

    assert session is not None
    assert session["sell_platform"] == "binance"
    assert session["buy_platform"] == "bybit"
    assert session["sell_fiat"] == "EUR"
    assert session["buy_fiat"] == "EUR"

    payload = json.loads(str(session["payload_json"]))
    assert payload["rm"]["source_exchange_balance_usdt"] == "500.0000"
    assert payload["rm"]["treasury_provider"] == "bank"
    assert payload["revalidation"]["passed"] is True


def test_prepare_session_marks_revalidation_failed_when_live_order_drifts(tmp_path) -> None:
    settings = _settings()
    settings.exchange_execution.allowed_platforms = ("binance", "bybit")
    settings.exchange_execution.allowed_treasury_providers = ("bank", "revolut", "wise")
    settings.exchange_execution.revalidation_max_price_drift_pct = Decimal("0.50")

    db = Database(tmp_path / "execution-revalidation.sqlite3")
    with _event_loop() as loop:
        loop.run_until_complete(db.initialize())
        signal_repository = SignalJournalRepository(db)
        execution_repository = ExecutionSessionRepository(db)
        inventory_repository = InventoryRepository(db)

        loop.run_until_complete(inventory_repository.set_balance("binance:wallet", "USDT", Decimal("500")))
        loop.run_until_complete(inventory_repository.set_balance("bybit:wallet", "USDT", Decimal("900")))
        loop.run_until_complete(inventory_repository.set_balance("bank", "EUR", Decimal("2000")))

        opportunity = _opportunity(
            buy_order=_order(platform="bybit", side="buy", fiat="EUR", price="1.00", order_id="buy-1"),
            sell_order=_order(platform="binance", side="sell", fiat="EUR", price="1.05", order_id="sell-1"),
        )
        signal_id = loop.run_until_complete(signal_repository.create_from_opportunity(opportunity))
        drifting_sell = _order(platform="binance", side="sell", fiat="EUR", price="1.20", order_id="sell-1")
        live_bybit = _order(platform="bybit", side="buy", fiat="EUR", price="1.00", order_id="buy-1")

        engine = ExchangeExecutionEngine(
            settings=settings,
            signal_journal_repository=signal_repository,
            execution_session_repository=execution_repository,
            inventory_repository=inventory_repository,
            bybit_client=_BybitStub({("USDT", "EUR", "buy"): [live_bybit]}),  # type: ignore[arg-type]
            binance_client=_BinanceStub({("USDT", "EUR", "sell"): [drifting_sell]}),  # type: ignore[arg-type]
            logger=logging.getLogger("test.execution"),
        )
        session_id = loop.run_until_complete(engine.prepare_session(signal_id))
        session = loop.run_until_complete(execution_repository.get(session_id))
        events = loop.run_until_complete(execution_repository.list_events(session_id))

    assert session is not None
    assert session["status"] == "revalidation_failed"
    assert "price drift" in str(session["error_text"])
    assert any(event["event_type"] == "revalidation_failed" for event in events)


def test_finalize_signal_execution_marks_done_and_applies_inventory_atomically(tmp_path) -> None:
    db = Database(tmp_path / "finalize-signal.sqlite3")
    with _event_loop() as loop:
        loop.run_until_complete(db.initialize())
        signal_repository = SignalJournalRepository(db)
        inventory_repository = InventoryRepository(db)

        signal_id = loop.run_until_complete(
            signal_repository.create_from_opportunity(
                _opportunity(
                    buy_order=_order(platform="bybit", side="buy", fiat="EUR", price="1.00", order_id="buy-1"),
                    sell_order=_order(platform="binance", side="sell", fiat="EUR", price="1.05", order_id="sell-1"),
                )
            )
        )
        finalized = loop.run_until_complete(
            signal_repository.finalize_signal_execution(
                signal_id,
                inventory_repository=inventory_repository,
                actual_profit_usd=Decimal("12.50"),
                actual_volume_usdt=Decimal("120.00"),
            )
        )
        stored = loop.run_until_complete(signal_repository.get(signal_id))
        ledger_rows = loop.run_until_complete(
            db.fetchall(
                """
                SELECT reason, signal_id
                FROM inventory_ledger
                WHERE signal_id = ?
                ORDER BY id
                """,
                (signal_id,),
            )
        )

    assert finalized is not None
    assert stored is not None
    assert stored["status"] == "done"
    assert int(stored["inventory_applied"]) == 1
    assert Decimal(str(stored["actual_profit_usd"])) == Decimal("12.5")
    assert Decimal(str(stored["actual_volume_usdt"])) == Decimal("120")

    reasons = [str(row["reason"]) for row in ledger_rows]
    assert reasons[0] == "signal_start_usdt"
    assert reasons[-1] == "signal_finish_usdt"


def test_finalize_signal_execution_skips_duplicate_route_inventory_reapply(tmp_path) -> None:
    db = Database(tmp_path / "finalize-signal-duplicate.sqlite3")
    with _event_loop() as loop:
        loop.run_until_complete(db.initialize())
        signal_repository = SignalJournalRepository(db)
        inventory_repository = InventoryRepository(db)

        opportunity = _opportunity(
            buy_order=_order(platform="bybit", side="buy", fiat="EUR", price="1.00", order_id="buy-1"),
            sell_order=_order(platform="binance", side="sell", fiat="EUR", price="1.05", order_id="sell-1"),
        )
        signal_id = loop.run_until_complete(signal_repository.create_from_opportunity(opportunity))
        duplicate_signal_id = loop.run_until_complete(signal_repository.create_from_opportunity(opportunity))

        loop.run_until_complete(
            signal_repository.finalize_signal_execution(
                signal_id,
                inventory_repository=inventory_repository,
                close_matching_route=True,
            )
        )
        first_ledger_count = loop.run_until_complete(
            db.fetchone(
                "SELECT COUNT(*) AS count FROM inventory_ledger WHERE signal_id = ?",
                (signal_id,),
            )
        )
        loop.run_until_complete(
            signal_repository.finalize_signal_execution(
                duplicate_signal_id,
                inventory_repository=inventory_repository,
            )
        )
        duplicate_ledger_count = loop.run_until_complete(
            db.fetchone(
                "SELECT COUNT(*) AS count FROM inventory_ledger WHERE signal_id = ?",
                (duplicate_signal_id,),
            )
        )
        duplicate_signal = loop.run_until_complete(signal_repository.get(duplicate_signal_id))

    assert first_ledger_count is not None
    assert duplicate_ledger_count is not None
    assert duplicate_signal is not None
    assert int(first_ledger_count["count"]) > 0
    assert int(duplicate_ledger_count["count"]) == 0
    assert duplicate_signal["status"] == "done"
    assert int(duplicate_signal["inventory_applied"]) == 1
