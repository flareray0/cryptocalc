from __future__ import annotations

from datetime import datetime
from pathlib import Path

from app.calc.normalizer import merge_transactions
from app.domain.enums import ImportSourceKind, TransactionType
from app.integrations.rate_input_adapter import ManualRateTable
from app.parsers.binance_japan_parser import BinanceJapanParser
from app.parsers.manual_adjustment_parser import ManualAdjustmentParser
from app.services.audit_service import AuditService
from app.storage.app_state import (
    append_import_batch,
    load_transactions,
    save_transactions,
)
from app.storage.settings import get_paths, load_settings, save_settings


class ImportService:
    def __init__(self) -> None:
        self.paths = get_paths()
        self.audit = AuditService()
        self.parsers = [BinanceJapanParser(), ManualAdjustmentParser()]

    def import_file(self, source_path: Path, import_kind: str | None = None):
        stored_path = self._copy_to_imports(source_path)
        parser = self._select_parser(stored_path, import_kind)
        batch = parser.parse(stored_path)
        existing = load_transactions()
        existing, suppressed_api_overlap_count = self._prefer_binance_csv_over_api(
            existing=existing,
            batch=batch,
        )
        merged, duplicate_count = merge_transactions(existing, batch.transactions)
        batch.duplicate_count = duplicate_count + suppressed_api_overlap_count
        batch.audit_log_path = str(
            self.audit.write_jsonl(
                "import_batch",
                [
                    {
                        "timestamp": datetime.now().isoformat(),
                        "source_file": batch.source_file,
                        "source_kind": batch.source_kind.value,
                        "transaction_count": batch.transaction_count,
                        "review_required_count": batch.review_required_count,
                        "duplicate_count": batch.duplicate_count,
                        "suppressed_api_overlap_count": suppressed_api_overlap_count,
                        "detected_layout": batch.detected_layout,
                        "header_row_number": batch.header_row_number,
                        "unknown_column_names": batch.unknown_column_names,
                        "unknown_tx_types": batch.unknown_tx_types,
                    }
                ],
            )
        )
        save_transactions(merged)
        append_import_batch(batch)
        return batch

    def import_manual_rate_file(self, source_path: Path) -> dict:
        stored_path = self._copy_to_imports(source_path)
        table = ManualRateTable.from_csv(stored_path)
        settings = load_settings()
        settings["manual_rate_file"] = str(stored_path)
        save_settings(settings)
        self.audit.write_event(
            "manual_rate_import",
            {
                "timestamp": datetime.now().isoformat(),
                "source_file": stored_path.name,
                "row_count": len(table.rows),
            },
        )
        return {"source_file": stored_path.name, "row_count": len(table.rows)}

    def _copy_to_imports(self, source_path: Path) -> Path:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        target = self.paths.imports / f"{stamp}_{source_path.name}"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source_path.read_bytes())
        return target

    def _select_parser(self, path: Path, import_kind: str | None):
        if import_kind == "manual_adjustment":
            return ManualAdjustmentParser()
        for parser in self.parsers:
            if parser.can_parse(path):
                return parser
        raise ValueError(f"対応していない入力形式です: {path.name}")

    def _prefer_binance_csv_over_api(self, *, existing: list, batch) -> tuple[list, int]:
        if batch.detected_layout not in {
            "csv_japanese_balance_history",
            "xlsx_japanese_balance_history_report",
        }:
            return existing, 0

        trade_like_types = {
            TransactionType.BUY,
            TransactionType.SELL,
            TransactionType.CRYPTO_SWAP,
        }
        batch_trade_rows = [tx for tx in batch.transactions if tx.tx_type in trade_like_types]
        if not batch_trade_rows:
            return existing, 0

        timestamps = [tx.timestamp_utc or tx.timestamp_jst for tx in batch_trade_rows if tx.timestamp_utc or tx.timestamp_jst]
        if not timestamps:
            return existing, 0

        pair_keys = {
            (tx.base_asset or "", tx.quote_asset or "", tx.tx_type.value, tx.side.value)
            for tx in batch_trade_rows
        }
        window_start = min(timestamps)
        window_end = max(timestamps)

        filtered = []
        removed = 0
        for tx in existing:
            timestamp = tx.timestamp_utc or tx.timestamp_jst
            pair_key = (tx.base_asset or "", tx.quote_asset or "", tx.tx_type.value, tx.side.value)
            should_replace = (
                tx.source_kind == ImportSourceKind.API
                and tx.source_exchange == "binance_japan_api"
                and tx.tx_type in trade_like_types
                and timestamp is not None
                and window_start <= timestamp <= window_end
                and pair_key in pair_keys
            )
            if should_replace:
                removed += 1
                continue
            filtered.append(tx)
        return filtered, removed
