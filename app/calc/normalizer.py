from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from decimal import Decimal
from datetime import datetime
from typing import Iterable

from app.domain.enums import ImportSourceKind
from app.domain.enums import ClassificationStatus
from app.domain.models import NormalizedTransaction


def _sortable_timestamp(tx: NormalizedTransaction) -> str:
    timestamp = tx.timestamp_utc or tx.timestamp_jst
    if isinstance(timestamp, datetime):
        return timestamp.isoformat()
    return ""


def sort_transactions(transactions: Iterable[NormalizedTransaction]) -> list[NormalizedTransaction]:
    return sorted(
        transactions,
        key=lambda tx: (
            _sortable_timestamp(tx),
            tx.source_file,
            tx.raw_row_number,
            tx.id,
        ),
    )


def duplicate_key(tx: NormalizedTransaction) -> str:
    quantity = tx.quantity or Decimal("0")
    gross = tx.gross_amount_jpy if tx.gross_amount_jpy is not None else tx.quote_quantity
    return "|".join(
        [
            tx.source_exchange,
            str(tx.timestamp_utc or tx.timestamp_jst or ""),
            tx.tx_type.value,
            tx.base_asset or "",
            tx.quote_asset or "",
            str(quantity),
            str(gross or ""),
            tx.side.value,
        ]
    )


def canonical_exchange(tx: NormalizedTransaction) -> str:
    if tx.source_exchange in {"binance_japan", "binance_japan_api"}:
        return "binance_japan"
    return tx.source_exchange


def canonical_timestamp(tx: NormalizedTransaction) -> datetime | str:
    timestamp = tx.timestamp_utc or tx.timestamp_jst or ""
    if isinstance(timestamp, datetime):
        return timestamp.replace(microsecond=0)
    return str(timestamp)


def cross_source_duplicate_key(tx: NormalizedTransaction) -> str:
    quantity = tx.quantity or Decimal("0")
    gross = tx.gross_amount_jpy if tx.gross_amount_jpy is not None else tx.quote_quantity
    return "|".join(
        [
            canonical_exchange(tx),
            str(canonical_timestamp(tx)),
            tx.tx_type.value,
            tx.base_asset or "",
            tx.quote_asset or "",
            str(quantity),
            str(gross or ""),
            tx.side.value,
        ]
    )


def _prefer_transaction(current: NormalizedTransaction, candidate: NormalizedTransaction) -> NormalizedTransaction:
    if current.source_kind == ImportSourceKind.API and candidate.source_kind != ImportSourceKind.API:
        return candidate
    if current.source_kind != ImportSourceKind.API and candidate.source_kind == ImportSourceKind.API:
        return current
    return candidate


def _should_cross_source_dedupe(current: NormalizedTransaction, candidate: NormalizedTransaction) -> bool:
    if canonical_exchange(current) != canonical_exchange(candidate):
        return False
    return (
        current.source_kind == ImportSourceKind.API and candidate.source_kind != ImportSourceKind.API
    ) or (
        current.source_kind != ImportSourceKind.API and candidate.source_kind == ImportSourceKind.API
    )


def apply_duplicate_review_flags(
    transactions: Iterable[NormalizedTransaction],
) -> tuple[list[NormalizedTransaction], int]:
    groups: dict[str, list[NormalizedTransaction]] = defaultdict(list)
    for tx in transactions:
        groups[duplicate_key(tx)].append(tx)

    duplicate_count = 0
    updated: list[NormalizedTransaction] = []
    for tx in transactions:
        key = duplicate_key(tx)
        group = groups[key]
        if len(group) == 1:
            updated.append(replace(tx, duplicate_group_key=key))
            continue

        duplicate_count += 1
        reasons = list(tx.review_reasons)
        if "重複疑い" not in reasons:
            reasons.append("重複疑い")
        updated.append(
            replace(
                tx,
                duplicate_group_key=key,
                review_flag=True,
                review_reasons=reasons,
                classification_status=ClassificationStatus.REVIEW_REQUIRED,
            )
        )
    return updated, duplicate_count


def merge_transactions(
    existing: Iterable[NormalizedTransaction],
    incoming: Iterable[NormalizedTransaction],
) -> tuple[list[NormalizedTransaction], int]:
    merged_map = {tx.id: tx for tx in existing}
    canonical_map = {cross_source_duplicate_key(tx): tx.id for tx in merged_map.values()}
    suppressed_cross_source_duplicates = 0
    for tx in incoming:
        if tx.id in merged_map:
            merged_map[tx.id] = tx
            canonical_map[cross_source_duplicate_key(tx)] = tx.id
            continue

        cross_key = cross_source_duplicate_key(tx)
        existing_id = canonical_map.get(cross_key)
        if existing_id and existing_id in merged_map:
            existing_tx = merged_map[existing_id]
            if _should_cross_source_dedupe(existing_tx, tx):
                preferred = _prefer_transaction(existing_tx, tx)
                suppressed_cross_source_duplicates += 1
                if preferred.id != existing_id:
                    del merged_map[existing_id]
                    merged_map[preferred.id] = preferred
                    canonical_map[cross_key] = preferred.id
                continue

        merged_map[tx.id] = tx
        canonical_map[cross_key] = tx.id
    merged = sort_transactions(merged_map.values())
    updated, duplicate_count = apply_duplicate_review_flags(merged)
    return updated, duplicate_count + suppressed_cross_source_duplicates
