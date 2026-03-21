from __future__ import annotations

from datetime import datetime

from app.calc.normalizer import merge_transactions
from app.integrations.binance_japan_api_client import BinanceJapanApiClient
from app.services.audit_service import AuditService
from app.services.source_reconcile_service import (
    build_authoritative_binance_windows,
    filter_api_transactions_by_authoritative_windows,
    prune_existing_api_transactions,
)
from app.storage.app_state import (
    load_import_batches,
    load_sync_status,
    load_transactions,
    save_sync_status,
    save_transactions,
)
from app.storage.secrets_store import SecretsStore
from app.storage.settings import load_settings, save_settings


class ExchangeSyncService:
    def __init__(self) -> None:
        self.secrets = SecretsStore()
        self.audit = AuditService()

    def save_connection(self, *, api_key: str | None, api_secret: str | None, base_url: str | None = None) -> dict:
        settings = load_settings()
        base_url = base_url or settings["binance_japan_api"]["base_url"]
        saved = self.secrets.load() or {}
        api_key = (api_key or "").strip() or saved.get("api_key")
        api_secret = (api_secret or "").strip() or saved.get("api_secret")
        if not api_key or not api_secret:
            raise ValueError("API Key / Secret が未設定です。保存済みが無い場合は入力してください")
        client = BinanceJapanApiClient(
            api_key=api_key,
            api_secret=api_secret,
            base_url=base_url,
        )
        result = client.test_connection()
        self.secrets.save({"api_key": api_key, "api_secret": api_secret, "base_url": base_url})
        settings["binance_japan_api"]["enabled"] = True
        settings["binance_japan_api"]["base_url"] = base_url
        save_settings(settings)
        save_sync_status(
            {
                "connected": True,
                "last_tested_at": datetime.now().isoformat(),
                "last_synced_at": None,
                "last_error": None,
                "last_sync_count": 0,
                "last_duplicate_count": 0,
                "base_url": base_url,
                "account_type": result.get("accountType"),
                "can_trade": result.get("canTrade"),
                "secret_saved": True,
                "api_key_hint": self._mask_key(api_key),
            }
        )
        return result

    def test_connection(self) -> dict:
        client = self._client()
        return client.test_connection()

    def sync(self, *, symbols: list[str], start_time_ms: int | None = None, end_time_ms: int | None = None) -> dict:
        client = self._client()
        settings = load_settings()
        resolved_symbols, symbol_source = self._resolve_symbols(
            requested_symbols=symbols,
            saved_symbols=settings["binance_japan_api"].get("default_symbols", ""),
            client=client,
        )
        sync_result = client.sync_transactions_with_meta(
            symbols=resolved_symbols,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        incoming = sync_result["transactions"]
        existing = load_transactions()
        import_batches = load_import_batches()
        windows = build_authoritative_binance_windows(
            transactions=existing,
            import_batches=import_batches,
        )
        incoming, skipped_by_csv_coverage = filter_api_transactions_by_authoritative_windows(
            incoming_transactions=incoming,
            windows=windows,
        )
        if skipped_by_csv_coverage:
            sync_result.setdefault("warnings", []).append(
                f"CSV/XLSX の帳票取込でカバー済みの期間があったため、API 取引 {skipped_by_csv_coverage} 件を追加しませんでした。"
            )
        merged, duplicate_count = merge_transactions(existing, incoming)
        merged, pruned_existing_api_count = prune_existing_api_transactions(
            transactions=merged,
            windows=windows,
        )
        save_transactions(merged)
        status = load_sync_status()
        status.update(
            {
                "connected": True,
                "last_synced_at": datetime.now().isoformat(),
                "last_error": None,
                "last_sync_count": len(incoming),
                "last_duplicate_count": duplicate_count + pruned_existing_api_count,
                "symbols": resolved_symbols,
                "secret_saved": True,
                "ready_to_sync": True,
                "symbol_source": symbol_source,
                "warnings": list(sync_result.get("warnings", [])),
            }
        )
        save_sync_status(status)
        settings["binance_japan_api"]["default_symbols"] = ",".join(resolved_symbols)
        settings["binance_japan_api"]["default_start_time_ms"] = start_time_ms
        settings["binance_japan_api"]["default_end_time_ms"] = end_time_ms
        save_settings(settings)
        self.audit.write_event(
            "api_sync",
            {
                "timestamp": datetime.now().isoformat(),
                "symbol_count": len(resolved_symbols),
                "symbols": resolved_symbols,
                "symbol_source": symbol_source,
                "fetched_count": len(incoming),
                "duplicate_count": duplicate_count + pruned_existing_api_count,
                "skipped_by_csv_coverage": skipped_by_csv_coverage,
                "start_time_ms": start_time_ms,
                "end_time_ms": end_time_ms,
                "warning_count": len(sync_result.get("warnings", [])),
            },
        )
        return {
            "synced_count": len(incoming),
            "duplicate_count": duplicate_count + pruned_existing_api_count,
            "last_synced_at": status["last_synced_at"],
            "resolved_symbols": resolved_symbols,
            "symbol_source": symbol_source,
            "warnings": list(sync_result.get("warnings", [])),
            "skipped_by_csv_coverage": skipped_by_csv_coverage,
        }

    def disconnect(self) -> None:
        self.secrets.delete()
        settings = load_settings()
        settings["binance_japan_api"]["enabled"] = False
        save_settings(settings)
        save_sync_status(
            {
                "connected": False,
                "last_tested_at": datetime.now().isoformat(),
                "last_synced_at": None,
                "last_error": None,
                "last_sync_count": 0,
                "last_duplicate_count": 0,
                "base_url": settings["binance_japan_api"]["base_url"],
                "secret_saved": False,
                "api_key_hint": None,
            }
        )

    def connection_state(self) -> dict:
        status = load_sync_status()
        settings = load_settings()
        secret = self.secrets.load()
        merged = {
            **status,
            "secret_saved": bool(secret),
            "ready_to_sync": bool(secret),
            "api_key_hint": self._mask_key(secret.get("api_key")) if secret else None,
            "base_url": (secret or {}).get("base_url") or settings["binance_japan_api"]["base_url"],
            "default_symbols": settings["binance_japan_api"].get("default_symbols", ""),
            "default_start_time_ms": settings["binance_japan_api"].get("default_start_time_ms"),
            "default_end_time_ms": settings["binance_japan_api"].get("default_end_time_ms"),
        }
        return merged

    def _client(self) -> BinanceJapanApiClient:
        secret = self.secrets.load()
        if not secret:
            raise ValueError("Binance Japan API の接続情報が保存されていません")
        return BinanceJapanApiClient(
            api_key=secret["api_key"],
            api_secret=secret["api_secret"],
            base_url=secret["base_url"],
        )

    def _mask_key(self, api_key: str | None) -> str | None:
        if not api_key:
            return None
        if len(api_key) <= 8:
            return "*" * len(api_key)
        return f"{api_key[:4]}...{api_key[-4:]}"

    def _resolve_symbols(self, *, requested_symbols: list[str], saved_symbols: str, client) -> tuple[list[str], str]:
        normalized = [token.strip().upper() for token in requested_symbols if token and token.strip()]
        if normalized:
            return normalized, "request"

        saved = [token.strip().upper() for token in str(saved_symbols or "").split(",") if token.strip()]
        if saved:
            return saved, "saved_default"

        discovered = client.discover_symbols()
        if discovered:
            return discovered, "exchange_info"

        raise ValueError("同期対象の symbols が空です。保存済み設定も見つからないため、少なくとも1回は symbols を指定してください。")
