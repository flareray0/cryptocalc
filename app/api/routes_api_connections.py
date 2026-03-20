from __future__ import annotations

from fastapi import APIRouter

from app.api.schemas import BinanceConnectRequest, BinanceSyncRequest
from app.services.exchange_sync_service import ExchangeSyncService


router = APIRouter(prefix="/api/v1", tags=["integrations"])


@router.post("/integrations/binance-japan/connect")
def connect_binance_japan(request: BinanceConnectRequest):
    service = ExchangeSyncService()
    result = service.save_connection(
        api_key=request.api_key,
        api_secret=request.api_secret,
        base_url=request.base_url,
    )
    return {
        "connected": True,
        "account_type": result.get("accountType"),
        "can_trade": result.get("canTrade"),
        "balances_count": len(result.get("balances", [])),
    }


@router.post("/integrations/binance-japan/sync")
def sync_binance_japan(request: BinanceSyncRequest):
    service = ExchangeSyncService()
    return service.sync(
        symbols=request.symbols,
        start_time_ms=request.start_time_ms,
        end_time_ms=request.end_time_ms,
    )


@router.get("/integrations/status")
def integration_status():
    return ExchangeSyncService().connection_state()


@router.post("/integrations/binance-japan/disconnect")
def disconnect_binance_japan():
    service = ExchangeSyncService()
    service.disconnect()
    return {"connected": False}
