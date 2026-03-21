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

    restarted_state = ExchangeSyncService().connection_state()
    assert restarted_state["secret_saved"] is True
    assert restarted_state["ready_to_sync"] is True


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


def test_binance_trade_sync_paginates_with_from_id(monkeypatch):
    client = BinanceJapanApiClient(
        api_key="ABCD1234EFGH5678",
        api_secret="dummy-secret",
        base_url="https://api.binance.com",
    )

    calls = []

    def _fake_signed_get(path, params):
        calls.append(params.copy())
        from_id = params.get("fromId", 0)
        if from_id == 0:
            return [
                {"id": 10, "time": 1000, "price": "100", "qty": "1", "quoteQty": "100", "commission": "0", "commissionAsset": "JPY", "isBuyer": True, "orderId": 1},
                {"id": 11, "time": 2000, "price": "101", "qty": "1", "quoteQty": "101", "commission": "0", "commissionAsset": "JPY", "isBuyer": True, "orderId": 2},
            ]
        if from_id == 12:
            return [
                {"id": 12, "time": 3000, "price": "102", "qty": "1", "quoteQty": "102", "commission": "0", "commissionAsset": "JPY", "isBuyer": False, "orderId": 3},
            ]
        return []

    monkeypatch.setattr(client, "_signed_get", _fake_signed_get)

    rows = client._fetch_all_trades_by_from_id(symbol="BTCJPY", limit=2)
    assert len(rows) == 3
    assert [call.get("fromId", 0) for call in calls] == [0, 12]


def test_sync_uses_saved_default_symbols_when_request_is_empty(monkeypatch):
    class DummyClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def sync_transactions_with_meta(self, **kwargs):
            return {"transactions": [], "resolved_symbols": kwargs["symbols"], "warnings": []}

        def discover_symbols(self):
            return ["BTCJPY", "ETHJPY"]

    monkeypatch.setattr(
        "app.services.exchange_sync_service.BinanceJapanApiClient",
        DummyClient,
    )

    service = ExchangeSyncService()
    service.secrets.save({"api_key": "demo-key", "api_secret": "demo-secret", "base_url": "https://api.binance.com"})

    from app.storage.settings import load_settings, save_settings

    settings = load_settings()
    settings["binance_japan_api"]["default_symbols"] = "BTCJPY,ETHJPY"
    save_settings(settings)

    result = service.sync(symbols=[], start_time_ms=None, end_time_ms=None)
    assert result["resolved_symbols"] == ["BTCJPY", "ETHJPY"]
    assert result["symbol_source"] == "saved_default"
