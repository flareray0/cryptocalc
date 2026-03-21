from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from app.calc.normalizer import merge_transactions
from app.domain.enums import ClassificationStatus, ImportSourceKind, Side, TransactionType
from app.domain.models import NormalizedTransaction
from app.services.import_service import ImportService
from app.storage.app_state import load_transactions, save_transactions


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


def test_japanese_balance_history_import_replaces_overlapping_api_rows(tmp_path):
    api_tx = NormalizedTransaction(
        id="api_trade_1",
        source_exchange="binance_japan_api",
        source_file="api_sync",
        raw_row_number=1,
        timestamp_jst=datetime(2025, 7, 17, 20, 6, 11, tzinfo=timezone.utc).astimezone(),
        timestamp_utc=datetime(2025, 7, 17, 11, 6, 11, tzinfo=timezone.utc),
        tx_type=TransactionType.CRYPTO_SWAP,
        base_asset="ETH",
        quote_asset="BTC",
        quantity=Decimal("0.0035"),
        quote_quantity=Decimal("0.00009908"),
        unit_price_quote=Decimal("0.02830857142857142857142857143"),
        price_per_unit_jpy=None,
        gross_amount_jpy=None,
        fee_asset=None,
        fee_amount=None,
        fee_jpy=None,
        side=Side.BUY,
        note="symbol=ETHBTC; orderId=1; fills=1",
        raw_payload={},
        classification_status=ClassificationStatus.REVIEW_REQUIRED,
        review_flag=True,
        review_reasons=["API取引のJPY換算が未確定"],
        source_kind=ImportSourceKind.API,
        jpy_rate_source=None,
    )
    save_transactions([api_tx])

    csv_path = tmp_path / "jp_balance.csv"
    csv_path.write_text(
        "\n".join(
            [
                "ユーザーID,時間,アカウント,操作,コイン,変更,備考",
                "1,25-07-17 20:06:11,Spot,Transaction Buy,ETH,0.0035,",
                "1,25-07-17 20:06:11,Spot,Transaction Spend,BTC,-0.00009908,",
            ]
        ),
        encoding="utf-8-sig",
    )

    batch = ImportService().import_file(Path(csv_path))
    merged = load_transactions()

    assert batch.detected_layout == "csv_japanese_balance_history"
    assert len(merged) == 1
    assert merged[0].source_exchange == "binance_japan"
    assert merged[0].source_kind == ImportSourceKind.CSV
