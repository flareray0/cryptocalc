from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from decimal import Decimal
from typing import Iterable

from app.domain.enums import ClassificationStatus
from app.domain.models import NormalizedTransaction


def sort_transactions(transactions: Iterable[NormalizedTransaction]) -> list[NormalizedTransaction]:
    return sorted(
        transactions,
        key=lambda tx: (
            tx.timestamp_utc or tx.timestamp_jst or "",
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
    for tx in incoming:
        merged_map[tx.id] = tx
    merged = sort_transactions(merged_map.values())
    return apply_duplicate_review_flags(merged)
