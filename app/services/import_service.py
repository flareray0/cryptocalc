from __future__ import annotations

from datetime import datetime
from pathlib import Path

from app.calc.normalizer import merge_transactions
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
        merged, duplicate_count = merge_transactions(existing, batch.transactions)
        batch.duplicate_count = duplicate_count
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
                        "duplicate_count": duplicate_count,
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
        target.write_bytes(source_path.read_bytes())
        return target

    def _select_parser(self, path: Path, import_kind: str | None):
        if import_kind == "manual_adjustment":
            return ManualAdjustmentParser()
        for parser in self.parsers:
            if parser.can_parse(path):
                return parser
        raise ValueError(f"対応していない入力形式です: {path.name}")
