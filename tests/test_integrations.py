from __future__ import annotations

import httpx

from app.integrations.binance_japan_api_client import BinanceJapanApiClient
from app.services.exchange_sync_service import ExchangeSyncService


def test_saved_api_credentials_can_be_reused(monkeypatch):
    def _fake_test_connection(self):
        return {
            "accountType": "SPOT",
            "canTrade": False,
            "balances": [],
        }

    monkeypatch.setattr(BinanceJapanApiClient, "test_connection", _fake_test_connection)

    first = ExchangeSyncService().save_connection(
        api_key="ABCD1234EFGH5678",
        api_secret="dummy-secret",
        base_url="https://api.binance.com",
    )
    assert first["accountType"] == "SPOT"

    state = ExchangeSyncService().connection_state()
    assert state["secret_saved"] is True
    assert state["api_key_hint"] == "ABCD...5678"

    reused = ExchangeSyncService().save_connection(
        api_key="",
        api_secret="",
        base_url="https://api.binance.com",
    )
    assert reused["accountType"] == "SPOT"


def test_binance_401_error_is_human_readable(monkeypatch):
    client = BinanceJapanApiClient(
        api_key="ABCD1234EFGH5678",
        api_secret="dummy-secret",
        base_url="https://api.binance.com",
    )

    def _fake_get(url):
        request = httpx.Request("GET", f"https://api.binance.com{url}")
        return httpx.Response(
            401,
            request=request,
            json={"code": -2015, "msg": "Invalid API-key, IP, or permissions for action."},
        )

    monkeypatch.setattr(client.client, "get", _fake_get)

    try:
        client._signed_get("/api/v3/myTrades", {"symbol": "BTCJPY", "timestamp": 1})
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover
        raise AssertionError("401 should raise ValueError")

    assert "401 Unauthorized" in message
    assert "BTCJPY" in message
    assert "ABCD...5678" in message
    assert "接続解除して再保存" in message
