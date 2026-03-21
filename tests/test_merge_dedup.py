from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.calc.normalizer import merge_transactions
from app.domain.enums import ClassificationStatus, ImportSourceKind, Side, TransactionType
from app.domain.models import NormalizedTransaction


def _tx(
    *,
    tx_id: str,
    source_exchange: str,
    source_kind: ImportSourceKind,
) -> NormalizedTransaction:
    timestamp = datetime(2026, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
    return NormalizedTransaction(
        id=tx_id,
        source_exchange=source_exchange,
        source_file="source.csv" if source_kind != ImportSourceKind.API else "api_sync",
        raw_row_number=1,
        timestamp_jst=timestamp.astimezone(),
        timestamp_utc=timestamp,
        tx_type=TransactionType.BUY,
        base_asset="BTC",
        quote_asset="JPY",
        quantity=Decimal("0.01"),
        quote_quantity=None,
        unit_price_quote=Decimal("10000000"),
        price_per_unit_jpy=Decimal("10000000"),
        gross_amount_jpy=Decimal("100000"),
        fee_asset="JPY",
        fee_amount=Decimal("10"),
        fee_jpy=Decimal("10"),
        side=Side.BUY,
        note="same trade",
        raw_payload={},
        classification_status=ClassificationStatus.CLASSIFIED,
        review_flag=False,
        review_reasons=[],
        source_kind=source_kind,
        jpy_rate_source="file",
    )


def test_merge_prefers_csv_over_equivalent_api_transaction():
    csv_tx = _tx(
        tx_id="csv_tx_1",
        source_exchange="binance_japan",
        source_kind=ImportSourceKind.CSV,
    )
    api_tx = _tx(
        tx_id="api_tx_1",
        source_exchange="binance_japan_api",
        source_kind=ImportSourceKind.API,
    )

    merged, duplicate_count = merge_transactions([csv_tx], [api_tx])
    assert len(merged) == 1
    assert merged[0].id == "csv_tx_1"
    assert duplicate_count >= 1


def test_merge_replaces_equivalent_api_transaction_when_csv_arrives_later():
    csv_tx = _tx(
        tx_id="csv_tx_2",
        source_exchange="binance_japan",
        source_kind=ImportSourceKind.XLSX,
    )
    api_tx = _tx(
        tx_id="api_tx_2",
        source_exchange="binance_japan_api",
        source_kind=ImportSourceKind.API,
    )

    merged, duplicate_count = merge_transactions([api_tx], [csv_tx])
    assert len(merged) == 1
    assert merged[0].id == "csv_tx_2"
    assert merged[0].source_kind == ImportSourceKind.XLSX
    assert duplicate_count >= 1
