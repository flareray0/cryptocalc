from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from app.calc.normalizer import merge_transactions
from app.domain.enums import ImportSourceKind, TransactionType
from app.integrations.rate_input_adapter import ManualRateTable
from app.parsers.binance_japan_parser import BinanceJapanParser
from app.parsers.manual_adjustment_parser import ManualAdjustmentParser
from app.services.audit_service import AuditService
from app.services.source_reconcile_service import (
    AUTHORITATIVE_BINANCE_LAYOUTS,
    SUPPLEMENTARY_BINANCE_LAYOUTS,
    build_authoritative_binance_windows,
    build_binance_layout_source_files,
    filter_incoming_binance_supplementary_transactions,
    prune_existing_binance_transactions_by_source_files,
    prune_existing_api_transactions,
)
from app.storage.app_state import (
    append_import_batch,
    load_import_batches,
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
        existing_import_batches = load_import_batches()
        existing_windows = build_authoritative_binance_windows(
            transactions=existing,
            import_batches=existing_import_batches,
        )
        suppressed_supplementary_overlap_count = 0
        if batch.detected_layout in SUPPLEMENTARY_BINANCE_LAYOUTS:
            batch.transactions, suppressed_supplementary_overlap_count = filter_incoming_binance_supplementary_transactions(
                incoming_transactions=batch.transactions,
                windows=existing_windows,
            )
            batch.transaction_count = len(batch.transactions)
            batch.review_required_count = sum(1 for tx in batch.transactions if tx.review_flag)
        existing, suppressed_api_overlap_count = self._prefer_binance_csv_over_api(
            existing=existing,
            batch=batch,
        )
        merged, duplicate_count = merge_transactions(existing, batch.transactions)
        import_batches = existing_import_batches + [
            {
                "source_file": batch.source_file,
                "detected_layout": batch.detected_layout,
            }
        ]
        windows = build_authoritative_binance_windows(
            transactions=merged,
            import_batches=import_batches,
        )
        merged, pruned_existing_api_count = prune_existing_api_transactions(
            transactions=merged,
            windows=windows,
        )
        pruned_existing_supplementary_count = 0
        if batch.detected_layout in AUTHORITATIVE_BINANCE_LAYOUTS:
            supplementary_files = build_binance_layout_source_files(
                import_batches=import_batches,
                layouts=SUPPLEMENTARY_BINANCE_LAYOUTS,
            )
            merged, pruned_existing_supplementary_count = prune_existing_binance_transactions_by_source_files(
                transactions=merged,
                windows=windows,
                source_files=supplementary_files,
            )
        batch.duplicate_count = duplicate_count + suppressed_api_overlap_count
        if suppressed_supplementary_overlap_count:
            batch.duplicate_count += suppressed_supplementary_overlap_count
        if pruned_existing_api_count:
            batch.duplicate_count += pruned_existing_api_count
        if pruned_existing_supplementary_count:
            batch.duplicate_count += pruned_existing_supplementary_count
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
                        "suppressed_supplementary_overlap_count": suppressed_supplementary_overlap_count,
                        "suppressed_api_overlap_count": suppressed_api_overlap_count,
                        "pruned_existing_api_count": pruned_existing_api_count,
                        "pruned_existing_supplementary_count": pruned_existing_supplementary_count,
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

    def import_data_directory(self, data_root: Path | None = None) -> dict[str, Any]:
        root = (data_root or self.paths.data).resolve()
        root.mkdir(parents=True, exist_ok=True)
        supported_suffixes = {".csv", ".xlsx", ".xlsm"}
        files = [
            path
            for path in root.rglob("*")
            if path.is_file() and path.suffix.lower() in supported_suffixes
        ]
        ordered_files = sorted(files, key=self._data_import_sort_key)

        imported_files: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for path in ordered_files:
            category = self._data_import_category(path)
            try:
                if category == "manual_rate":
                    result = self.import_manual_rate_file(path)
                    imported_files.append(
                        {
                            "path": str(path.relative_to(root)),
                            "category": category,
                            "source_file": result["source_file"],
                            "row_count": result["row_count"],
                        }
                    )
                else:
                    batch = self.import_file(
                        path,
                        import_kind="manual_adjustment" if category == "manual_adjustment" else None,
                    )
                    imported_files.append(
                        {
                            "path": str(path.relative_to(root)),
                            "category": category,
                            "source_file": batch.source_file,
                            "transaction_count": batch.transaction_count,
                            "review_required_count": batch.review_required_count,
                            "duplicate_count": batch.duplicate_count,
                            "detected_layout": batch.detected_layout,
                        }
                    )
            except Exception as exc:
                errors.append({"path": str(path.relative_to(root)), "error": str(exc)})

        return {
            "data_dir": str(root),
            "scanned_file_count": len(ordered_files),
            "imported_file_count": len(imported_files),
            "error_count": len(errors),
            "imported_files": imported_files,
            "errors": errors,
        }

    def _copy_to_imports(self, source_path: Path) -> Path:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        target = self.paths.imports / f"{stamp}_{source_path.name}"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source_path.read_bytes())
        return target

    def _data_import_category(self, path: Path) -> str:
        parts = [part.lower() for part in path.parts]
        name = path.name.lower()
        if any(part in {"manual_rates", "rates", "rate"} for part in parts) or "manual_rate" in name or "rate" in name or "レート" in path.name:
            return "manual_rate"
        if any(part in {"manual_adjustments", "adjustments", "adjustment"} for part in parts) or "manual_adjust" in name or "adjustment" in name or "補正" in path.name:
            return "manual_adjustment"
        return "exchange_history"

    def _data_import_sort_key(self, path: Path) -> tuple[int, str]:
        category = self._data_import_category(path)
        priority = {
            "exchange_history": 0,
            "manual_adjustment": 1,
            "manual_rate": 2,
        }.get(category, 9)
        return (priority, str(path).lower())

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
