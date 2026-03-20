from __future__ import annotations

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
