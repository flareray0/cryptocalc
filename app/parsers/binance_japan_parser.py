from __future__ import annotations

import csv
import hashlib
from pathlib import Path
from typing import Any, Iterable

from app.domain.enums import ClassificationStatus, ImportSourceKind, Side, TransactionType
from app.domain.models import ImportBatchResult, NormalizedTransaction
from app.domain.validators import parse_utc_timestamp, safe_slug, to_decimal, utc_to_jst
from app.parsers.base_parser import BaseParser

EXPECTED_COLUMNS = [
    "Date(UTC)",
    "Pair",
    "Base Asset",
    "Quote Asset",
    "Type",
    "Price",
    "Amount",
    "Total",
    "Fee",
    "Fee Coin",
]


class BinanceJapanParser(BaseParser):
    exchange_name = "binance_japan"

    def can_parse(self, path: Path) -> bool:
        return path.suffix.lower() in {".csv", ".xlsx", ".xlsm"}

    def parse(self, path: Path) -> ImportBatchResult:
        rows = list(self._load_rows(path))
        transactions: list[NormalizedTransaction] = []
        unknown_columns: set[str] = set()
        unknown_types: set[str] = set()
        for row_number, row in rows:
            tx = self._row_to_tx(path, row_number, row)
            unknown_columns.update(tx.raw_payload.get("_unknown_columns", []))
            if tx.tx_type is TransactionType.UNKNOWN:
                unknown_types.add(str(row.get("Type", "")))
            transactions.append(tx)

        batch_id = self._batch_id(path)
        return ImportBatchResult(
            batch_id=batch_id,
            source_file=path.name,
            source_kind=self._source_kind(path),
            transaction_count=len(transactions),
            review_required_count=sum(1 for tx in transactions if tx.review_flag),
            duplicate_count=0,
            unknown_column_names=sorted(unknown_columns),
            unknown_tx_types=sorted(filter(None, unknown_types)),
            transactions=transactions,
        )

    def _batch_id(self, path: Path) -> str:
        return f"binance_japan_{safe_slug(path.stem)}_{int(path.stat().st_mtime)}"

    def _source_kind(self, path: Path) -> ImportSourceKind:
        return ImportSourceKind.XLSX if path.suffix.lower() != ".csv" else ImportSourceKind.CSV

    def _load_rows(self, path: Path) -> Iterable[tuple[int, dict[str, Any]]]:
        if path.suffix.lower() == ".csv":
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                for idx, row in enumerate(reader, start=2):
                    yield idx, {k: v for k, v in row.items()}
            return

        from openpyxl import load_workbook

        wb = load_workbook(path, read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]
        header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
        headers = [str(cell).strip() if cell is not None else "" for cell in header_row]
        for row_number, cells in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            row = {headers[index]: cells[index] for index in range(len(headers))}
            yield row_number, row

    def _row_to_tx(self, path: Path, row_number: int, row: dict[str, Any]) -> NormalizedTransaction:
        unknown_columns = sorted(
            key for key in row.keys() if key and key not in EXPECTED_COLUMNS
        )
        timestamp_utc = parse_utc_timestamp(str(row.get("Date(UTC)") or ""))
        timestamp_jst = utc_to_jst(timestamp_utc)
        base_asset = (row.get("Base Asset") or "").strip().upper() or None
        quote_asset = (row.get("Quote Asset") or "").strip().upper() or None
        pair = (row.get("Pair") or "").strip()
        order_type = (str(row.get("Type") or "")).strip().upper()
        quantity = to_decimal(row.get("Amount"))
        total = to_decimal(row.get("Total"))
        price = to_decimal(row.get("Price"))
        fee_amount = to_decimal(row.get("Fee"))
        fee_asset = (row.get("Fee Coin") or "").strip().upper() or None

        tx_type, side = self._classify(order_type=order_type, quote_asset=quote_asset)
        price_per_unit_jpy = price if quote_asset == "JPY" else None
        gross_amount_jpy = total if quote_asset == "JPY" else None
        fee_jpy = fee_amount if fee_asset == "JPY" else None

        review_reasons: list[str] = []
        if quote_asset != "JPY":
            review_reasons.append("JPY換算列がないため要確認")
        if tx_type is TransactionType.UNKNOWN:
            review_reasons.append("未知の取引種別")
        if fee_amount and fee_asset and fee_asset != "JPY":
            review_reasons.append("手数料のJPY換算が未確定")

        raw_payload = {**row, "_unknown_columns": unknown_columns}
        tx_hash = hashlib.sha1(
            f"{path.name}:{row_number}:{pair}:{timestamp_utc}:{order_type}".encode("utf-8")
        ).hexdigest()[:16]
        return NormalizedTransaction(
            id=f"tx_{tx_hash}",
            source_exchange=self.exchange_name,
            source_file=path.name,
            raw_row_number=row_number,
            timestamp_jst=timestamp_jst,
            timestamp_utc=timestamp_utc,
            tx_type=tx_type,
            base_asset=base_asset,
            quote_asset=quote_asset,
            quantity=quantity,
            quote_quantity=total if quote_asset and quote_asset != "JPY" else None,
            unit_price_quote=price,
            price_per_unit_jpy=price_per_unit_jpy,
            gross_amount_jpy=gross_amount_jpy,
            fee_asset=fee_asset,
            fee_amount=fee_amount,
            fee_jpy=fee_jpy,
            side=side,
            note=f"pair={pair}; order_type={order_type}",
            raw_payload=raw_payload,
            classification_status=(
                ClassificationStatus.CLASSIFIED
                if not review_reasons
                else ClassificationStatus.REVIEW_REQUIRED
            ),
            review_flag=bool(review_reasons),
            review_reasons=review_reasons,
            source_kind=self._source_kind(path),
            jpy_rate_source="file:quote_jpy" if quote_asset == "JPY" else None,
        )

    def _classify(
        self, *, order_type: str, quote_asset: str | None
    ) -> tuple[TransactionType, Side]:
        if order_type == "BUY":
            if quote_asset == "JPY":
                return TransactionType.BUY, Side.BUY
            return TransactionType.CRYPTO_SWAP, Side.BUY
        if order_type == "SELL":
            if quote_asset == "JPY":
                return TransactionType.SELL, Side.SELL
            return TransactionType.CRYPTO_SWAP, Side.SELL
        if "DEPOSIT" in order_type:
            return TransactionType.TRANSFER_IN, Side.NONE
        if "WITHDRAW" in order_type:
            return TransactionType.TRANSFER_OUT, Side.NONE
        if any(token in order_type for token in ("STAK", "REWARD", "BONUS", "EARN")):
            return TransactionType.REWARD, Side.NONE
        return TransactionType.UNKNOWN, Side.NONE
