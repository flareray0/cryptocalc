from __future__ import annotations

from pathlib import Path
from typing import Any

from app.domain.enums import CalculationMethod
from app.domain.models import CalculationRunResult, ImportBatchResult
from app.storage.json_store import (
    dump_json,
    import_batch_to_dict,
    load_json,
    record_to_dict,
    running_position_to_dict,
    transaction_to_dict,
    transactions_from_json,
)
from app.storage.settings import get_paths


def _transactions_path() -> Path:
    return get_paths().app_data / "transactions.json"


def _batches_path() -> Path:
    return get_paths().app_data / "import_batches.json"


def _sync_status_path() -> Path:
    return get_paths().app_data / "integration_status.json"


def _runs_index_path() -> Path:
    return get_paths().app_data / "calc_runs" / "index.json"


def load_transactions() -> list:
    rows = load_json(_transactions_path(), [])
    return transactions_from_json(rows)


def save_transactions(transactions: list) -> None:
    dump_json(_transactions_path(), [transaction_to_dict(tx) for tx in transactions])


def load_import_batches() -> list[dict[str, Any]]:
    return load_json(_batches_path(), [])


def append_import_batch(batch: ImportBatchResult) -> None:
    rows = load_import_batches()
    rows.append(import_batch_to_dict(batch))
    dump_json(_batches_path(), rows)


def save_sync_status(status: dict[str, Any]) -> None:
    dump_json(_sync_status_path(), status)


def load_sync_status() -> dict[str, Any]:
    return load_json(
        _sync_status_path(),
        {
            "connected": False,
            "last_tested_at": None,
            "last_synced_at": None,
            "last_error": None,
            "last_sync_count": 0,
            "last_duplicate_count": 0,
        },
    )


def _run_path(run_id: str) -> Path:
    return get_paths().calc_runs / f"{run_id}.json"


def save_calc_run(result: CalculationRunResult) -> None:
    payload = {
        "run_id": result.run_id,
        "year": result.year,
        "method": result.method.value,
        "realized_records": [record_to_dict(row) for row in result.realized_records],
        "positions": [running_position_to_dict(row) for row in result.positions],
        "review_required_transactions": [
            transaction_to_dict(tx) for tx in result.review_required_transactions
        ],
        "yearly_summary": result.yearly_summary,
        "asset_summaries": result.asset_summaries,
        "audit_rows": result.audit_rows,
        "inventory_timeline": result.inventory_timeline,
        "source_transaction_count": result.source_transaction_count,
    }
    dump_json(_run_path(result.run_id), payload)
    index = load_json(_runs_index_path(), [])
    index = [row for row in index if row.get("run_id") != result.run_id]
    index.append(
        {
            "run_id": result.run_id,
            "year": result.year,
            "method": result.method.value,
            "created_at": result.yearly_summary.get("generated_at"),
        }
    )
    dump_json(_runs_index_path(), index)


def load_calc_run(run_id: str) -> dict[str, Any] | None:
    path = _run_path(run_id)
    if not path.exists():
        return None
    return load_json(path, None)


def load_latest_calc_run(
    method: CalculationMethod | None = None,
    year: int | None = None,
) -> dict[str, Any] | None:
    index = load_json(_runs_index_path(), [])
    rows = sorted(index, key=lambda row: row.get("created_at") or "", reverse=True)
    for row in rows:
        if method and row.get("method") != method.value:
            continue
        if year and row.get("year") != year:
            continue
        return load_calc_run(row["run_id"])
    return None
