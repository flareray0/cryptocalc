from __future__ import annotations

import csv
import hashlib
from pathlib import Path

from app.domain.enums import ClassificationStatus, ImportSourceKind, Side, TransactionType
from app.domain.models import ImportBatchResult, NormalizedTransaction
from app.domain.validators import parse_utc_timestamp, to_decimal, utc_to_jst
from app.parsers.base_parser import BaseParser


class ManualAdjustmentParser(BaseParser):
    exchange_name = "manual_adjustment"

    def can_parse(self, path: Path) -> bool:
        return path.suffix.lower() == ".csv"

    def parse(self, path: Path) -> ImportBatchResult:
        transactions: list[NormalizedTransaction] = []
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row_number, row in enumerate(reader, start=2):
                raw_tx_type = row.get("tx_type", TransactionType.ADJUSTMENT.value)
                try:
                    tx_type = TransactionType(raw_tx_type)
                except ValueError:
                    tx_type = TransactionType.ADJUSTMENT
                timestamp_utc = parse_utc_timestamp(row.get("timestamp_utc") or row.get("timestamp"))
                timestamp_jst = utc_to_jst(timestamp_utc)
                digest = hashlib.sha1(f"{path.name}:{row_number}:{row}".encode("utf-8")).hexdigest()[:16]
                transactions.append(
                    NormalizedTransaction(
                        id=f"adj_{digest}",
                        source_exchange=self.exchange_name,
                        source_file=path.name,
                        raw_row_number=row_number,
                        timestamp_jst=timestamp_jst,
                        timestamp_utc=timestamp_utc,
                        tx_type=tx_type,
                        base_asset=(row.get("asset") or row.get("base_asset") or "").upper() or None,
                        quote_asset=(row.get("quote_asset") or "").upper() or None,
                        quantity=to_decimal(row.get("quantity")),
                        quote_quantity=to_decimal(row.get("quote_quantity")),
                        unit_price_quote=to_decimal(row.get("unit_price_quote")),
                        price_per_unit_jpy=to_decimal(row.get("price_per_unit_jpy")),
                        gross_amount_jpy=to_decimal(row.get("gross_amount_jpy")),
                        fee_asset=(row.get("fee_asset") or "").upper() or None,
                        fee_amount=to_decimal(row.get("fee_amount")),
                        fee_jpy=to_decimal(row.get("fee_jpy")),
                        side=Side(row.get("side", Side.NONE.value)),
                        note=row.get("note", "manual adjustment"),
                        raw_payload=row,
                        classification_status=ClassificationStatus.CLASSIFIED,
                        review_flag=False,
                        source_kind=ImportSourceKind.MANUAL,
                        jpy_rate_source=row.get("jpy_rate_source"),
                    )
                )

        return ImportBatchResult(
            batch_id=f"manual_{path.stem}",
            source_file=path.name,
            source_kind=ImportSourceKind.MANUAL,
            transaction_count=len(transactions),
            review_required_count=0,
            duplicate_count=0,
            unknown_column_names=[],
            unknown_tx_types=[],
            transactions=transactions,
        )
