from __future__ import annotations

import hashlib
import hmac
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx

from app.domain.enums import ClassificationStatus, ImportSourceKind, Side, TransactionType
from app.domain.models import NormalizedTransaction
from app.domain.validators import to_decimal, utc_to_jst
from app.integrations.exchange_base import ExchangeClientBase


class BinanceJapanApiClient(ExchangeClientBase):
    def __init__(self, *, api_key: str, api_secret: str, base_url: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=30.0,
            headers={"X-MBX-APIKEY": self.api_key},
        )

    def test_connection(self) -> dict:
        return self._signed_get("/api/v3/account", {"timestamp": self._timestamp()})

    def fetch_exchange_info(self) -> dict:
        response = self.client.get("/api/v3/exchangeInfo")
        response.raise_for_status()
        return response.json()

    def fetch_my_trades(
        self,
        *,
        symbol: str,
        from_id: int | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ) -> list[dict]:
        params = {"symbol": symbol, "timestamp": self._timestamp(), "limit": limit}
        if from_id is not None:
            params["fromId"] = from_id
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return self._signed_get("/api/v3/myTrades", params)

    def discover_symbols(self) -> list[str]:
        payload = self.fetch_exchange_info()
        discovered: list[str] = []
        for row in payload.get("symbols", []):
            symbol = row.get("symbol")
            if not symbol:
                continue
            status = str(row.get("status") or "").upper()
            if status and status != "TRADING":
                continue
            discovered.append(symbol)
        return discovered

    def sync_transactions(
        self,
        *,
        symbols: list[str],
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[NormalizedTransaction]:
        return self.sync_transactions_with_meta(
            symbols=symbols,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )["transactions"]

    def sync_transactions_with_meta(
        self,
        *,
        symbols: list[str],
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> dict[str, object]:
        symbol_map = self._symbol_map()
        rows: list[NormalizedTransaction] = []
        warnings: list[str] = []
        resolved_symbols: list[str] = []
        for symbol in symbols:
            symbol = symbol.strip().upper()
            if not symbol:
                continue
            resolved_symbols.append(symbol)
            base_asset, quote_asset = symbol_map.get(symbol, (None, None))
            page_rows, page_warning = self._fetch_my_trades_full_history(
                symbol=symbol,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            if page_warning:
                warnings.append(page_warning)
            normalized_rows = self._aggregate_trade_rows(page_rows)
            if len(normalized_rows) != len(page_rows):
                warnings.append(
                    f"{symbol} は API fill {len(page_rows)} 件を注文単位 {len(normalized_rows)} 件へ集約しました。"
                    " CSV の件数とは粒度が一致しない場合があります。"
                )
            for row in normalized_rows:
                rows.append(self._trade_to_tx(row, symbol, base_asset, quote_asset))
        return {
            "transactions": rows,
            "resolved_symbols": resolved_symbols,
            "warnings": warnings,
        }

    def _aggregate_trade_rows(self, rows: list[dict]) -> list[dict]:
        grouped: dict[tuple[int, bool], list[dict]] = {}
        passthrough: list[dict] = []
        for row in rows:
            order_id = row.get("orderId")
            if order_id is None:
                passthrough.append(row)
                continue
            grouped.setdefault((int(order_id), bool(row.get("isBuyer"))), []).append(row)

        aggregated: list[dict] = []
        for _, group in grouped.items():
            if len(group) == 1:
                aggregated.append(group[0])
                continue

            fee_assets = {(item.get("commissionAsset") or "").upper() for item in group}
            if len(fee_assets) != 1:
                aggregated.extend(group)
                continue

            total_qty = sum(to_decimal(item.get("qty")) or 0 for item in group)
            total_quote = sum(to_decimal(item.get("quoteQty")) or 0 for item in group)
            total_commission = sum(to_decimal(item.get("commission")) or 0 for item in group)
            weighted_price = None if not total_qty else total_quote / total_qty
            latest = max(group, key=lambda item: int(item.get("time") or 0))

            aggregated.append(
                {
                    **latest,
                    "id": latest.get("orderId") or latest.get("id"),
                    "qty": str(total_qty),
                    "quoteQty": str(total_quote),
                    "price": str(weighted_price) if weighted_price is not None else latest.get("price"),
                    "commission": str(total_commission),
                    "commissionAsset": next(iter(fee_assets)),
                    "_fill_count": len(group),
                    "_fill_ids": [item.get("id") for item in group],
                    "_aggregated": True,
                }
            )

        aggregated.extend(passthrough)
        aggregated.sort(key=lambda item: (int(item.get("time") or 0), int(item.get("id") or 0)))
        return aggregated

    def _fetch_my_trades_full_history(
        self,
        *,
        symbol: str,
        start_time_ms: int | None,
        end_time_ms: int | None,
        limit: int = 1000,
    ) -> tuple[list[dict], str | None]:
        if start_time_ms is None and end_time_ms is None:
            return self._fetch_all_trades_by_from_id(symbol=symbol, limit=limit), None

        rows: list[dict] = []
        warnings: list[str] = []
        window_start = start_time_ms
        window_end = end_time_ms

        if window_start is None and window_end is not None:
            window_start = max(0, window_end - 86_400_000 + 1)
        if window_end is None and window_start is not None:
            window_end = window_start + 86_400_000 - 1

        cursor = window_start
        final_end = window_end
        while cursor is not None and final_end is not None and cursor <= final_end:
            current_end = min(cursor + 86_400_000 - 1, final_end)
            page = self.fetch_my_trades(
                symbol=symbol,
                start_time_ms=cursor,
                end_time_ms=current_end,
                limit=limit,
            )
            rows.extend(page)
            if len(page) >= limit:
                warnings.append(
                    f"{symbol} は {cursor}-{current_end} の 24h 窓で {limit} 件に達しました。API仕様上、この時間帯は取りこぼしの可能性があります。CSV import を優先して確認してください。"
                )
            cursor = current_end + 1
        warning = " ".join(warnings) if warnings else None
        return rows, warning

    def _fetch_all_trades_by_from_id(self, *, symbol: str, limit: int = 1000) -> list[dict]:
        rows: list[dict] = []
        from_id = 0
        while True:
            page = self.fetch_my_trades(symbol=symbol, from_id=from_id, limit=limit)
            if not page:
                break
            rows.extend(page)
            if len(page) < limit:
                break
            last_id = page[-1].get("id")
            if last_id is None:
                break
            try:
                next_from_id = int(last_id) + 1
            except (TypeError, ValueError):
                break
            if next_from_id <= from_id:
                break
            from_id = next_from_id
        return rows

    def _symbol_map(self) -> dict[str, tuple[str | None, str | None]]:
        payload = self.fetch_exchange_info()
        mapping: dict[str, tuple[str | None, str | None]] = {}
        for row in payload.get("symbols", []):
            mapping[row.get("symbol")] = (row.get("baseAsset"), row.get("quoteAsset"))
        return mapping

    def _signed_get(self, path: str, params: dict) -> dict | list:
        query = urlencode(params, doseq=True)
        signature = hmac.new(self.api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        response = self.client.get(f"{path}?{query}&signature={signature}")
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ValueError(self._format_http_error(exc, path, params)) from exc
        return response.json()

    def _timestamp(self) -> int:
        return int(time.time() * 1000)

    def _format_http_error(self, exc: httpx.HTTPStatusError, path: str, params: dict) -> str:
        response = exc.response
        status_code = response.status_code
        detail = ""
        try:
            payload = response.json()
            code = payload.get("code")
            message = payload.get("msg") or payload.get("message") or payload
            detail = f" code={code} msg={message}" if code is not None else f" msg={message}"
        except Exception:
            text = (response.text or "").strip()
            if text:
                detail = f" body={text}"

        if status_code == 401:
            symbol = params.get("symbol")
            symbol_note = f" symbol={symbol}." if symbol else ""
            return (
                "Binance API 認証エラー (401 Unauthorized)。"
                f"{symbol_note} "
                f"base_url={self.base_url}, api_key={self._mask_key(self.api_key)}."
                " 保存済みキーが古い可能性があります。API連携画面でキーを入力し直すか、"
                "いったん接続解除して再保存してください。"
                " あわせて Binance 側で Read 権限と IP 制限の許可先を確認してください。"
                f"{detail}"
            )

        return (
            f"Binance API エラー ({status_code})。path={path}, "
            f"base_url={self.base_url}, api_key={self._mask_key(self.api_key)}.{detail}"
        )

    def _mask_key(self, api_key: str | None) -> str:
        if not api_key:
            return "(none)"
        if len(api_key) <= 8:
            return "*" * len(api_key)
        return f"{api_key[:4]}...{api_key[-4:]}"

    def _trade_to_tx(
        self,
        row: dict,
        symbol: str,
        base_asset: str | None,
        quote_asset: str | None,
    ) -> NormalizedTransaction:
        timestamp_utc = datetime.fromtimestamp(int(row["time"]) / 1000, tz=timezone.utc)
        timestamp_jst = utc_to_jst(timestamp_utc)
        is_buyer = bool(row.get("isBuyer"))
        tx_type = (
            TransactionType.BUY
            if quote_asset == "JPY" and is_buyer
            else TransactionType.SELL
            if quote_asset == "JPY"
            else TransactionType.CRYPTO_SWAP
        )
        side = Side.BUY if is_buyer else Side.SELL
        price = to_decimal(row.get("price"))
        qty = to_decimal(row.get("qty"))
        quote_qty = to_decimal(row.get("quoteQty"))
        fee_amount = to_decimal(row.get("commission"))
        fee_asset = (row.get("commissionAsset") or "").upper() or None
        gross_jpy = quote_qty if quote_asset == "JPY" else None
        review_reasons = []
        if quote_asset != "JPY":
            review_reasons.append("API取引のJPY換算が未確定")
        if fee_amount and fee_asset and fee_asset != "JPY":
            review_reasons.append("API取引手数料のJPY換算が未確定")
        fill_count = int(row.get("_fill_count") or 1)
        raw_payload = dict(row)
        if fill_count > 1:
            raw_payload["aggregation_note"] = "api fills aggregated by orderId"
        return NormalizedTransaction(
            id=f"api_trade_{symbol}_{row.get('id')}",
            source_exchange="binance_japan_api",
            source_file="api_sync",
            raw_row_number=int(row.get("id", 0)),
            timestamp_jst=timestamp_jst,
            timestamp_utc=timestamp_utc,
            tx_type=tx_type,
            base_asset=base_asset,
            quote_asset=quote_asset,
            quantity=qty,
            quote_quantity=quote_qty if quote_asset != "JPY" else None,
            unit_price_quote=price,
            price_per_unit_jpy=price if quote_asset == "JPY" else None,
            gross_amount_jpy=gross_jpy,
            fee_asset=fee_asset,
            fee_amount=fee_amount,
            fee_jpy=fee_amount if fee_asset == "JPY" else None,
            side=side,
            note=f"symbol={symbol}; orderId={row.get('orderId')}; fills={fill_count}",
            raw_payload=raw_payload,
            classification_status=(
                ClassificationStatus.REVIEW_REQUIRED if review_reasons else ClassificationStatus.CLASSIFIED
            ),
            review_flag=bool(review_reasons),
            review_reasons=review_reasons,
            source_kind=ImportSourceKind.API,
            jpy_rate_source="api:quote_jpy" if quote_asset == "JPY" else None,
        )
