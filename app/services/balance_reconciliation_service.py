from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

import httpx

from app.domain.enums import CalculationMethod
from app.domain.validators import quantize_jpy
from app.integrations.binance_japan_api_client import BinanceJapanApiClient
from app.services.analysis_service import AnalysisService
from app.services.audit_service import AuditService
from app.storage.balance_reconciliation_state import (
    load_latest_balance_reconciliation,
    save_balance_reconciliation,
)
from app.storage.secrets_store import SecretsStore
from app.storage.settings import load_settings


ZERO = Decimal("0")


class BalanceReconciliationService:
    def __init__(self) -> None:
        self.secrets = SecretsStore()
        self.audit = AuditService()

    def refresh(
        self,
        *,
        start_year: int | None = None,
        end_year: int | None = None,
        method_reference: CalculationMethod | None = None,
    ) -> dict[str, Any]:
        resolved_method = method_reference or CalculationMethod(load_settings()["default_method"])
        client = self._load_client()
        account = client.test_connection()
        exchange_balances = self._extract_balances(account)
        secret = self.secrets.load()
        _base_url = secret.get("base_url") if secret else None
        prices = self._fetch_public_jpy_prices(exchange_balances.keys(), base_url=_base_url)
        reference = self._load_reference_analysis(
            start_year=start_year,
            end_year=end_year,
            method_reference=resolved_method,
        )
        payload = self._build_payload(
            exchange_balances=exchange_balances,
            prices=prices,
            reference=reference,
            method_reference=resolved_method,
            account_payload=account,
        )
        save_balance_reconciliation(payload)
        self.audit.write_event(
            "balance_reconciliation_refresh",
            {
                "run_id": payload["run_id"],
                "as_of": payload["as_of"],
                "start_year": payload.get("start_year"),
                "end_year": payload.get("end_year"),
                "method_reference": payload["method_reference"],
                "exchange_total_equity_jpy": payload["exchange_total_equity_jpy"],
                "difference_vs_analysis_jpy": payload["difference_vs_analysis_jpy"],
                "balance_row_count": len(payload["balance_rows"]),
                "warning_count": len(payload["review_notes"]),
            },
        )
        return payload

    def latest(
        self,
        *,
        start_year: int | None = None,
        end_year: int | None = None,
        method_reference: CalculationMethod | None = None,
    ) -> dict[str, Any] | None:
        return load_latest_balance_reconciliation(
            start_year=start_year,
            end_year=end_year,
            method_reference=method_reference,
        )

    def _load_client(self) -> BinanceJapanApiClient:
        secret = self.secrets.load()
        if not secret:
            raise ValueError("Binance Japan API の接続情報が保存されていません")
        return BinanceJapanApiClient(
            api_key=secret["api_key"],
            api_secret=secret["api_secret"],
            base_url=secret["base_url"],
        )

    def _extract_balances(self, account_payload: dict[str, Any]) -> dict[str, Decimal]:
        balances: dict[str, Decimal] = {}
        for row in account_payload.get("balances", []):
            asset = str(row.get("asset") or "").upper()
            if not asset:
                continue
            free_amount = Decimal(str(row.get("free") or "0"))
            locked_amount = Decimal(str(row.get("locked") or "0"))
            total = free_amount + locked_amount
            if total == ZERO:
                continue
            balances[asset] = total
        return balances

    def _fetch_public_jpy_prices(self, assets: Any, base_url: str | None = None) -> dict[str, Decimal | None]:
        prices: dict[str, Decimal | None] = {}
        _base = (base_url or "https://api.binance.com").rstrip("/")
        with httpx.Client(timeout=20.0) as client:
            for asset in assets:
                code = str(asset).upper()
                if code == "JPY":
                    prices[code] = Decimal("1")
                    continue
                response = client.get(
                    f"{_base}/api/v3/ticker/price",
                    params={"symbol": f"{code}JPY"},
                )
                if response.status_code == 200:
                    prices[code] = Decimal(str(response.json()["price"]))
                else:
                    prices[code] = None
        return prices

    def _load_reference_analysis(
        self,
        *,
        start_year: int | None,
        end_year: int | None,
        method_reference: CalculationMethod,
    ) -> dict[str, Any] | None:
        service = AnalysisService()
        settings = load_settings()
        resolved_start_year = (
            start_year if start_year is not None else settings["analysis_window"].get("default_start_year")
        )
        resolved_end_year = (
            end_year if end_year is not None else settings["analysis_window"].get("default_end_year")
        )
        if resolved_start_year is not None or resolved_end_year is not None:
            return service.latest_window_run(
                start_year=resolved_start_year,
                end_year=resolved_end_year,
                method_reference=method_reference,
            )
        return service.latest_run(method_reference=method_reference)

    def _build_payload(
        self,
        *,
        exchange_balances: dict[str, Decimal],
        prices: dict[str, Decimal | None],
        reference: dict[str, Any] | None,
        method_reference: CalculationMethod,
        account_payload: dict[str, Any],
    ) -> dict[str, Any]:
        as_of = datetime.now(timezone.utc).astimezone().isoformat()
        latest_snapshot = reference.get("latest_snapshot") if reference else None
        latest_snapshot = latest_snapshot or {}
        reference_rows = {
            str(row["symbol"]).upper(): row
            for row in reference.get("asset_summary_table", [])
        } if reference else {}

        reconstructed_cash_jpy = Decimal(str(latest_snapshot.get("cash_jpy") or "0"))
        analysis_total_equity_jpy = (
            Decimal(str(latest_snapshot["total_equity_jpy"]))
            if latest_snapshot.get("total_equity_jpy") not in (None, "")
            else None
        )

        symbols = sorted(set(exchange_balances) | set(reference_rows) | {"JPY"})
        balance_rows: list[dict[str, Any]] = []
        exchange_total_equity_jpy = ZERO
        reconstructed_current_price_total_jpy = ZERO
        unknown_price_assets: list[str] = []

        for symbol in symbols:
            exchange_quantity = exchange_balances.get(symbol, ZERO)
            if symbol == "JPY":
                reconstructed_quantity = reconstructed_cash_jpy
            else:
                reference_row = reference_rows.get(symbol, {})
                reconstructed_quantity = Decimal(str(reference_row.get("current_quantity") or "0"))

            price_jpy = prices.get(symbol)
            exchange_value_jpy = None if price_jpy is None else exchange_quantity * price_jpy
            reconstructed_value_jpy = None if price_jpy is None else reconstructed_quantity * price_jpy
            diff_quantity = exchange_quantity - reconstructed_quantity
            diff_value_jpy = None
            if exchange_value_jpy is not None and reconstructed_value_jpy is not None:
                diff_value_jpy = exchange_value_jpy - reconstructed_value_jpy
                exchange_total_equity_jpy += exchange_value_jpy
                reconstructed_current_price_total_jpy += reconstructed_value_jpy
            elif exchange_quantity != ZERO or reconstructed_quantity != ZERO:
                unknown_price_assets.append(symbol)

            balance_rows.append(
                {
                    "symbol": symbol,
                    "exchange_quantity": str(exchange_quantity),
                    "reconstructed_quantity": str(reconstructed_quantity),
                    "diff_quantity": str(diff_quantity),
                    "mark_price_jpy": str(quantize_jpy(price_jpy)) if price_jpy is not None else None,
                    "exchange_value_jpy": str(quantize_jpy(exchange_value_jpy)) if exchange_value_jpy is not None else None,
                    "reconstructed_value_jpy": str(quantize_jpy(reconstructed_value_jpy))
                    if reconstructed_value_jpy is not None
                    else None,
                    "diff_value_jpy": str(quantize_jpy(diff_value_jpy)) if diff_value_jpy is not None else None,
                }
            )

        balance_rows.sort(
            key=lambda row: Decimal(str(row["exchange_value_jpy"])) if row["exchange_value_jpy"] is not None else Decimal("-1"),
            reverse=True,
        )

        review_notes: list[str] = []
        if unknown_price_assets:
            joined = ", ".join(sorted(set(unknown_price_assets)))
            review_notes.append(f"{joined} は JPY の現在価格を取得できず、現在残高評価に反映していません。")
        if not reference:
            review_notes.append("比較用の分析結果が見つからないため、Binance 現在残高のみ表示しています。")

        difference_vs_analysis_jpy = None
        if analysis_total_equity_jpy is not None:
            difference_vs_analysis_jpy = exchange_total_equity_jpy - analysis_total_equity_jpy

        return {
            "run_id": f"balance_reconciliation_{uuid4().hex[:12]}",
            "as_of": as_of,
            "method_reference": method_reference.value,
            "start_year": reference.get("start_year") if reference else None,
            "end_year": reference.get("end_year") if reference else None,
            "reference_run_id": reference.get("run_id") if reference else None,
            "account_type": account_payload.get("accountType"),
            "can_trade": account_payload.get("canTrade"),
            "exchange_total_equity_jpy": str(quantize_jpy(exchange_total_equity_jpy)),
            "reconstructed_total_equity_jpy": (
                str(quantize_jpy(analysis_total_equity_jpy)) if analysis_total_equity_jpy is not None else None
            ),
            "reconstructed_current_price_total_jpy": str(quantize_jpy(reconstructed_current_price_total_jpy)),
            "difference_vs_analysis_jpy": (
                str(quantize_jpy(difference_vs_analysis_jpy)) if difference_vs_analysis_jpy is not None else None
            ),
            "difference_vs_current_price_reconstruction_jpy": str(
                quantize_jpy(exchange_total_equity_jpy - reconstructed_current_price_total_jpy)
            ),
            "balance_rows": balance_rows,
            "review_notes": review_notes,
        }
