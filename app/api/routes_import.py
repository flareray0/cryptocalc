from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from fastapi import APIRouter, File, Query, UploadFile

from app.storage.app_state import load_transactions
from app.storage.app_state import clear_imported_state
from app.storage.json_store import transaction_to_dict
from app.storage.settings import get_paths
from app.services.import_service import ImportService


router = APIRouter(prefix="/api/v1", tags=["import"])


def _save_upload_to_temp(upload: UploadFile) -> Path:
    suffix = Path(upload.filename or "upload.dat").suffix
    with tempfile.NamedTemporaryFile(
        delete=False,
        suffix=suffix,
        dir=get_paths().app_data,
    ) as fh:
        upload.file.seek(0)
        shutil.copyfileobj(upload.file, fh)
        return Path(fh.name)


@router.post("/import/csv")
def import_csv(file: UploadFile = File(...), import_kind: str | None = Query(default=None)):
    service = ImportService()
    temp_path = _save_upload_to_temp(file)
    try:
        batch = service.import_file(temp_path, import_kind=import_kind)
        return {
            "batch_id": batch.batch_id,
            "source_file": batch.source_file,
            "transaction_count": batch.transaction_count,
            "review_required_count": batch.review_required_count,
            "duplicate_count": batch.duplicate_count,
            "detected_layout": batch.detected_layout,
            "header_row_number": batch.header_row_number,
            "unknown_column_names": batch.unknown_column_names,
            "unknown_tx_types": batch.unknown_tx_types,
            "audit_log_path": batch.audit_log_path,
        }
    finally:
        temp_path.unlink(missing_ok=True)


@router.post("/import/manual-adjustments")
def import_manual_adjustments(file: UploadFile = File(...)):
    return import_csv(file=file, import_kind="manual_adjustment")


@router.post("/import/manual-rates")
def import_manual_rates(file: UploadFile = File(...)):
    service = ImportService()
    temp_path = _save_upload_to_temp(file)
    try:
        return service.import_manual_rate_file(temp_path)
    finally:
        temp_path.unlink(missing_ok=True)


@router.post("/import/reset")
def reset_imported_data():
    removed = clear_imported_state()
    return {
        "ok": True,
        "removed": removed,
        "message": "取引データと計算結果をリセットしました。設定やAPIキーはそのままだよ。",
    }


@router.post("/import/data-folder")
def import_data_folder():
    service = ImportService()
    return service.import_data_directory(get_paths().data)


@router.get("/transactions")
def list_transactions(
    year: int | None = None,
    asset: str | None = None,
    tx_type: str | None = None,
    review_required: bool | None = None,
):
    rows = load_transactions()
    filtered = []
    for tx in rows:
        if year and (tx.timestamp_jst is None or tx.timestamp_jst.year != year):
            continue
        if asset and asset.upper() not in {tx.base_asset or "", tx.quote_asset or "", tx.fee_asset or ""}:
            continue
        if tx_type and tx.tx_type.value != tx_type:
            continue
        if review_required is not None and tx.review_flag != review_required:
            continue
        filtered.append(transaction_to_dict(tx))
    return {"count": len(filtered), "transactions": filtered}


@router.get("/transactions/review-required")
def list_review_required_transactions(year: int | None = None):
    rows = load_transactions()
    filtered = []
    for tx in rows:
        if not tx.review_flag:
            continue
        if year and (tx.timestamp_jst is None or tx.timestamp_jst.year != year):
            continue
        filtered.append(transaction_to_dict(tx))
    return {"count": len(filtered), "transactions": filtered}
