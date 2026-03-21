from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from app.domain.models import NormalizedTransaction


AUTHORITATIVE_BINANCE_LAYOUTS = {
    "csv_japanese_balance_history",
    "xlsx_japanese_balance_history_report",
}

SUPPLEMENTARY_BINANCE_LAYOUTS = {
    "csv_spot_trade_history",
    "csv_deposit_history",
    "csv_withdraw_history",
    "csv_fiat_deposit_history",
    "csv_fiat_withdraw_history",
    "csv_fiat_conversion_history",
}


@dataclass(slots=True)
class AuthoritativeWindow:
    source_file: str
    start: datetime
    end: datetime


AUTHORITATIVE_WINDOW_PADDING = timedelta(minutes=5)


def _tx_timestamp(tx: NormalizedTransaction) -> datetime | None:
    return tx.timestamp_utc or tx.timestamp_jst


def build_authoritative_binance_windows(
    *,
    transactions: list[NormalizedTransaction],
    import_batches: list[dict],
) -> list[AuthoritativeWindow]:
    authoritative_files = {
        batch.get("source_file")
        for batch in import_batches
        if batch.get("detected_layout") in AUTHORITATIVE_BINANCE_LAYOUTS
    }
    windows: list[AuthoritativeWindow] = []
    for source_file in sorted(filter(None, authoritative_files)):
        timestamps = [
            timestamp
            for tx in transactions
            if tx.source_exchange == "binance_japan"
            and tx.source_file == source_file
            and (timestamp := _tx_timestamp(tx)) is not None
        ]
        if not timestamps:
            continue
        windows.append(
            AuthoritativeWindow(
                source_file=source_file,
                start=min(timestamps),
                end=max(timestamps),
            )
        )
    return windows


def build_binance_layout_source_files(*, import_batches: list[dict], layouts: set[str]) -> set[str]:
    files: set[str] = set()
    for batch in import_batches:
        source_file = batch.get("source_file")
        if batch.get("detected_layout") in layouts and isinstance(source_file, str) and source_file:
            files.add(source_file)
    return files


def filter_api_transactions_by_authoritative_windows(
    *,
    incoming_transactions: list[NormalizedTransaction],
    windows: list[AuthoritativeWindow],
) -> tuple[list[NormalizedTransaction], int]:
    if not windows:
        return incoming_transactions, 0

    filtered: list[NormalizedTransaction] = []
    removed = 0
    for tx in incoming_transactions:
        timestamp = _tx_timestamp(tx)
        covered = (
            tx.source_exchange == "binance_japan_api"
            and timestamp is not None
            and any(
                window.start - AUTHORITATIVE_WINDOW_PADDING <= timestamp <= window.end + AUTHORITATIVE_WINDOW_PADDING
                for window in windows
            )
        )
        if covered:
            removed += 1
            continue
        filtered.append(tx)
    return filtered, removed


def prune_existing_api_transactions(
    *,
    transactions: list[NormalizedTransaction],
    windows: list[AuthoritativeWindow],
) -> tuple[list[NormalizedTransaction], int]:
    if not windows:
        return transactions, 0

    filtered: list[NormalizedTransaction] = []
    removed = 0
    for tx in transactions:
        timestamp = _tx_timestamp(tx)
        covered = (
            tx.source_exchange == "binance_japan_api"
            and timestamp is not None
            and any(
                window.start - AUTHORITATIVE_WINDOW_PADDING <= timestamp <= window.end + AUTHORITATIVE_WINDOW_PADDING
                for window in windows
            )
        )
        if covered:
            removed += 1
            continue
        filtered.append(tx)
    return filtered, removed


def filter_incoming_binance_supplementary_transactions(
    *,
    incoming_transactions: list[NormalizedTransaction],
    windows: list[AuthoritativeWindow],
) -> tuple[list[NormalizedTransaction], int]:
    if not windows:
        return incoming_transactions, 0

    filtered: list[NormalizedTransaction] = []
    removed = 0
    for tx in incoming_transactions:
        timestamp = _tx_timestamp(tx)
        covered = (
            tx.source_exchange == "binance_japan"
            and timestamp is not None
            and any(
                window.start - AUTHORITATIVE_WINDOW_PADDING <= timestamp <= window.end + AUTHORITATIVE_WINDOW_PADDING
                for window in windows
            )
        )
        if covered:
            removed += 1
            continue
        filtered.append(tx)
    return filtered, removed


def prune_existing_binance_transactions_by_source_files(
    *,
    transactions: list[NormalizedTransaction],
    windows: list[AuthoritativeWindow],
    source_files: set[str],
) -> tuple[list[NormalizedTransaction], int]:
    if not windows or not source_files:
        return transactions, 0

    filtered: list[NormalizedTransaction] = []
    removed = 0
    for tx in transactions:
        timestamp = _tx_timestamp(tx)
        covered = (
            tx.source_exchange == "binance_japan"
            and tx.source_file in source_files
            and timestamp is not None
            and any(
                window.start - AUTHORITATIVE_WINDOW_PADDING <= timestamp <= window.end + AUTHORITATIVE_WINDOW_PADDING
                for window in windows
            )
        )
        if covered:
            removed += 1
            continue
        filtered.append(tx)
    return filtered, removed
