from __future__ import annotations

import json
from datetime import datetime
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable

from app.domain.enums import ClassificationStatus, ImportSourceKind, Side, TransactionType
from app.domain.models import (
    ImportBatchResult,
    LedgerEvent,
    NormalizedTransaction,
    RealizedPnlRecord,
    RunningPosition,
)
from app.domain.validators import parse_utc_timestamp, serialize_payload, to_decimal, utc_to_jst


def _parse_any_timestamp(value: Any):
    if not value:
        return None
    text = str(value)
    parsed = parse_utc_timestamp(text)
    if parsed is not None:
        return parsed
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _enum_value(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    return value


def dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(serialize_payload(payload), fh, ensure_ascii=False, indent=2)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def transaction_to_dict(tx: NormalizedTransaction) -> dict[str, Any]:
    raw = asdict(tx)
    for key in ("tx_type", "classification_status", "side", "source_kind"):
        raw[key] = _enum_value(raw[key])
    return serialize_payload(raw)


def transaction_from_dict(data: dict[str, Any]) -> NormalizedTransaction:
    timestamp_utc = _parse_any_timestamp(data.get("timestamp_utc"))
    timestamp_jst = _parse_any_timestamp(data.get("timestamp_jst"))
    if timestamp_jst is None and timestamp_utc is not None:
        timestamp_jst = utc_to_jst(timestamp_utc)
    return NormalizedTransaction(
        id=data["id"],
        source_exchange=data["source_exchange"],
        source_file=data["source_file"],
        raw_row_number=int(data["raw_row_number"]),
        timestamp_jst=timestamp_jst,
        timestamp_utc=timestamp_utc,
        tx_type=TransactionType(data["tx_type"]),
        base_asset=data.get("base_asset"),
        quote_asset=data.get("quote_asset"),
        quantity=to_decimal(data.get("quantity")),
        quote_quantity=to_decimal(data.get("quote_quantity")),
        unit_price_quote=to_decimal(data.get("unit_price_quote")),
        price_per_unit_jpy=to_decimal(data.get("price_per_unit_jpy")),
        gross_amount_jpy=to_decimal(data.get("gross_amount_jpy")),
        fee_asset=data.get("fee_asset"),
        fee_amount=to_decimal(data.get("fee_amount")),
        fee_jpy=to_decimal(data.get("fee_jpy")),
        side=Side(data.get("side", Side.NONE.value)),
        note=data.get("note", ""),
        raw_payload=data.get("raw_payload", {}),
        classification_status=ClassificationStatus(
            data.get("classification_status", ClassificationStatus.UNKNOWN.value)
        ),
        review_flag=bool(data.get("review_flag")),
        review_reasons=list(data.get("review_reasons", [])),
        source_kind=ImportSourceKind(data.get("source_kind", ImportSourceKind.CSV.value)),
        jpy_rate_source=data.get("jpy_rate_source"),
        duplicate_group_key=data.get("duplicate_group_key"),
    )


def record_to_dict(record: RealizedPnlRecord) -> dict[str, Any]:
    raw = asdict(record)
    raw["calculation_method"] = _enum_value(raw["calculation_method"])
    return serialize_payload(raw)


def running_position_to_dict(position: RunningPosition) -> dict[str, Any]:
    raw = asdict(position)
    raw["method"] = _enum_value(raw["method"])
    return serialize_payload(raw)


def ledger_event_to_dict(event: LedgerEvent) -> dict[str, Any]:
    raw = asdict(event)
    raw["kind"] = _enum_value(raw["kind"])
    return serialize_payload(raw)


def import_batch_to_dict(batch: ImportBatchResult) -> dict[str, Any]:
    return {
        "batch_id": batch.batch_id,
        "source_file": batch.source_file,
        "source_kind": batch.source_kind.value,
        "transaction_count": batch.transaction_count,
        "review_required_count": batch.review_required_count,
        "duplicate_count": batch.duplicate_count,
        "unknown_column_names": batch.unknown_column_names,
        "unknown_tx_types": batch.unknown_tx_types,
        "transactions": [transaction_to_dict(tx) for tx in batch.transactions],
        "audit_log_path": batch.audit_log_path,
        "detected_layout": batch.detected_layout,
        "header_row_number": batch.header_row_number,
    }


def transactions_from_json(rows: Iterable[dict[str, Any]]) -> list[NormalizedTransaction]:
    return [transaction_from_dict(row) for row in rows]
