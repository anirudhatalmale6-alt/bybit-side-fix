from decimal import Decimal
import asyncio
import logging

from p2p_bot.config import BybitConfig
from p2p_bot.modules.order_manager import ManagedAdSnapshot, OrderManager
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.state import AppState
from p2p_bot.config import Settings
from p2p_bot.utils.bybit_p2p import BybitP2PClient, _drop_none_fields
from pathlib import Path


def test_drop_none_fields_removes_optional_nulls() -> None:
    payload = {
        "itemId": None,
        "status": "2",
        "side": None,
        "page": "1",
        "size": "50",
        "tradingPreferenceSet": {
            "isKyc": "1",
            "nationalLimit": "",
            "hasNationalLimit": None,
        },
    }

    assert _drop_none_fields(payload) == {
        "status": "2",
        "page": "1",
        "size": "50",
        "tradingPreferenceSet": {
            "isKyc": "1",
            "nationalLimit": "",
        },
    }


def test_update_payload_matches_bybit_update_contract() -> None:
    snapshot = ManagedAdSnapshot(
        order_id="ad-123",
        side="buy",
        asset="USDT",
        fiat="EUR",
        price=Decimal("1.2345"),
        min_amount=Decimal("50"),
        max_amount=Decimal("500"),
        payment_ids=["7110"],
        payment_names=["SEPA"],
        remark="Managed by p2p_bot",
        quantity=Decimal("1000"),
        trading_preferences={
            "isKyc": 1,
            "hasOrderFinishNumberDay30": True,
            "orderFinishNumberDay30": 60,
            "nationalLimit": "",
            "hasNationalLimit": None,
        },
    )

    payload = snapshot.to_update_payload("ad-123")

    assert payload == {
        "id": "ad-123",
        "priceType": "0",
        "premium": "0",
        "price": "1.2345",
        "minAmount": "50",
        "maxAmount": "500",
        "remark": "Managed by p2p_bot",
        "tradingPreferenceSet": {
            "isKyc": "1",
            "hasOrderFinishNumberDay30": "1",
            "orderFinishNumberDay30": "60",
            "nationalLimit": "",
        },
        "paymentIds": ["7110"],
        "actionType": "MODIFY",
        "quantity": "1000",
        "paymentPeriod": "15",
    }


def test_get_my_ads_omits_page_and_size_by_default(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_official_post(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        captured["path"] = path
        captured["payload"] = payload
        return {"result": {"items": []}}

    monkeypatch.setattr(BybitP2PClient, "_official_post", fake_official_post)
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        client = BybitP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BybitConfig(
                api_key="key",
                api_secret="secret",
                base_url="https://api.bybit.com",
                legacy_public_scan_url="https://example.com",
                cookies=None,
                recv_window_ms=5000,
                use_official_p2p_api=True,
                use_legacy_public_scan=True,
                balance_account_type="FUND",
                payment_code_map={},
            ),
            logger=logging.getLogger("test.bybit"),
        )

        result = loop.run_until_complete(client.get_my_ads(status="2"))
    finally:
        asyncio.set_event_loop(None)
        loop.close()

    assert result == []
    assert captured == {
        "path": "/v5/p2p/item/personal/list",
        "payload": {"status": "2"},
    }


def test_get_online_ads_uses_sell_book_for_canonical_sell(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_try_official_online_ads(self, payload, *, asset, fiat, side):
        captured["payload"] = dict(payload)
        captured["side"] = side
        return {"result": {"items": []}}

    monkeypatch.setattr(BybitP2PClient, "_try_official_online_ads", fake_try_official_online_ads)

    async def exercise() -> list[object]:
        client = BybitP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BybitConfig(
                api_key="key",
                api_secret="secret",
                base_url="https://api.bybit.com",
                legacy_public_scan_url="https://example.com",
                cookies=None,
                recv_window_ms=5000,
                use_official_p2p_api=True,
                use_legacy_public_scan=True,
                balance_account_type="FUND",
                payment_code_map={},
            ),
            logger=logging.getLogger("test.bybit"),
        )
        return await client.get_online_ads("USDT", "NOK", "sell")

    result = asyncio.run(exercise())

    assert result == []
    assert captured["side"] == "sell"
    assert captured["payload"] == {
        "tokenId": "USDT",
        "currencyId": "NOK",
        "side": "1",
        "page": "1",
        "size": "20",
    }


def test_get_online_ads_skips_rows_from_opposite_bybit_side(monkeypatch) -> None:
    async def fake_try_official_online_ads(self, payload, *, asset, fiat, side):
        return {
            "result": {
                "items": [
                    {
                        "id": "ad-1",
                        "tokenId": "USDT",
                        "currencyId": "CAD",
                        "side": 0,
                        "price": "1.50",
                        "minAmount": "10",
                        "maxAmount": "1000",
                        "lastQuantity": "500",
                        "payments": [],
                    }
                ]
            }
        }

    monkeypatch.setattr(BybitP2PClient, "_try_official_online_ads", fake_try_official_online_ads)

    async def exercise() -> list[object]:
        client = BybitP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BybitConfig(
                api_key="key",
                api_secret="secret",
                base_url="https://api.bybit.com",
                legacy_public_scan_url="https://example.com",
                cookies=None,
                recv_window_ms=5000,
                use_official_p2p_api=True,
                use_legacy_public_scan=True,
                balance_account_type="FUND",
                payment_code_map={},
            ),
            logger=logging.getLogger("test.bybit"),
        )
        return await client.get_online_ads("USDT", "CAD", "sell")

    result = asyncio.run(exercise())

    assert result == []


def test_get_online_ads_maps_live_bybit_side_one_to_canonical_sell(monkeypatch) -> None:
    async def fake_try_official_online_ads(self, payload, *, asset, fiat, side):
        return {
            "result": {
                "items": [
                    {
                        "id": "ad-1",
                        "tokenId": "USDT",
                        "currencyId": "HUF",
                        "side": 1,
                        "price": "376.17",
                        "minAmount": "17000",
                        "maxAmount": "190000",
                        "lastQuantity": "20000",
                        "payments": ["65"],
                        "nickName": "kieran001",
                        "userId": "merchant-1",
                    }
                ]
            }
        }

    monkeypatch.setattr(BybitP2PClient, "_try_official_online_ads", fake_try_official_online_ads)

    async def exercise() -> list[object]:
        client = BybitP2PClient(
            session=object(),  # type: ignore[arg-type]
            config=BybitConfig(
                api_key="key",
                api_secret="secret",
                base_url="https://api.bybit.com",
                legacy_public_scan_url="https://example.com",
                cookies=None,
                recv_window_ms=5000,
                use_official_p2p_api=True,
                use_legacy_public_scan=True,
                balance_account_type="FUND",
                payment_code_map={"65": "Revolut"},
            ),
            logger=logging.getLogger("test.bybit"),
        )
        return await client.get_online_ads("USDT", "HUF", "sell")

    result = asyncio.run(exercise())

    assert len(result) == 1
    assert result[0].side == "sell"
    assert result[0].payment_methods == ["Revolut"]


def test_order_manager_decodes_bybit_side_from_response() -> None:
    settings = Settings.from_env(Path.cwd())
    state = AppState()
    bybit_stub = type("BybitStub", (), {"_payment_catalog": {}})()
    manager = OrderManager(
        settings=settings,
        state=state,
        risk_guard=RiskGuard(settings, state),
        trade_repository=object(),  # type: ignore[arg-type]
        bybit_client=bybit_stub,  # type: ignore[arg-type]
        alert_queue=object(),  # type: ignore[arg-type]
        logger=logging.getLogger("test.bybit.manager"),
    )

    snapshot = manager._snapshot_from_ad(
        {
            "id": "ad-1",
            "side": 1,
            "tokenId": "USDT",
            "currencyId": "CAD",
            "price": "1.50",
            "minAmount": "10",
            "maxAmount": "1000",
            "payments": [],
            "remark": "Managed by p2p_bot",
            "quantity": "1000",
        }
    )

    assert snapshot.side == "sell"
