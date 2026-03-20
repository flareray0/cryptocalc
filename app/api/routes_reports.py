from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

from app.domain.enums import CalculationMethod
from app.services.report_service import ReportService
from app.storage.settings import get_paths


router = APIRouter(prefix="/api/v1", tags=["reports"])


def _method_or_none(value: str | None):
    return CalculationMethod(value) if value else None


@router.get("/reports/yearly")
def yearly_report(year: int | None = None, method: str | None = None):
    service = ReportService()
    return service.yearly_summary(method=_method_or_none(method), year=year)


@router.get("/reports/assets")
def asset_report(year: int | None = None, method: str | None = None):
    service = ReportService()
    return {"rows": service.asset_summary(method=_method_or_none(method), year=year)}


@router.get("/reports/audit")
def audit_report(year: int | None = None, method: str | None = None):
    service = ReportService()
    return {"rows": service.audit_rows(method=_method_or_none(method), year=year)}


@router.get("/reports/inventory-timeline")
def inventory_timeline(year: int | None = None, method: str | None = None):
    service = ReportService()
    return {"rows": service.inventory_timeline(method=_method_or_none(method), year=year)}


@router.get("/reports/nta-export")
def nta_export(year: int | None = None, method: str | None = None):
    service = ReportService()
    return service.nta_export(method=_method_or_none(method), year=year)


@router.get("/reports/download")
def download_export(name: str):
    path = (get_paths().exports / name).resolve()
    exports_root = get_paths().exports.resolve()
    if exports_root not in path.parents and path != exports_root:
        raise ValueError("許可されていないパスです")
    if not path.exists():
        raise ValueError("指定ファイルが見つかりません")
    return FileResponse(path)
