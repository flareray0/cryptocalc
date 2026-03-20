from __future__ import annotations

from fastapi import APIRouter

from app.api.schemas import AnalysisRunRequest
from app.domain.enums import CalculationMethod
from app.services.analysis_service import AnalysisService


router = APIRouter(prefix="/api/v1", tags=["analysis"])


def _method_or_none(value: str | None):
    return CalculationMethod(value) if value else None


@router.post("/analysis/run")
def run_analysis(request: AnalysisRunRequest):
    return AnalysisService().run(
        year=request.year,
        method_reference=CalculationMethod(request.method_reference),
    )


@router.get("/analysis/latest")
def latest_analysis(year: int | None = None, method_reference: str | None = None):
    data = AnalysisService().latest_run(year=year, method_reference=_method_or_none(method_reference))
    if not data:
        raise ValueError("先に分析を実行してください")
    return data


@router.get("/analysis/portfolio-history")
def portfolio_history(year: int | None = None, method_reference: str | None = None):
    data = latest_analysis(year=year, method_reference=method_reference)
    return {"rows": data.get("portfolio_snapshots", [])}


@router.get("/analysis/asset-quantities")
def asset_quantities(year: int | None = None, method_reference: str | None = None):
    data = latest_analysis(year=year, method_reference=method_reference)
    return {"rows": data.get("asset_quantity_history", [])}


@router.get("/analysis/benchmark")
def benchmark_history(year: int | None = None, method_reference: str | None = None):
    data = latest_analysis(year=year, method_reference=method_reference)
    return {"rows": data.get("benchmark_snapshots", [])}


@router.get("/analysis/pnl-breakdown")
def pnl_breakdown(year: int | None = None, method_reference: str | None = None):
    data = latest_analysis(year=year, method_reference=method_reference)
    return {"rows": data.get("pnl_attribution_snapshots", [])}


@router.get("/analysis/edge-report")
def edge_report(year: int | None = None, method_reference: str | None = None):
    data = latest_analysis(year=year, method_reference=method_reference)
    return data.get("edge_report", {})


@router.get("/analysis/export")
def export_analysis(year: int | None = None, method_reference: str | None = None):
    return AnalysisService().export_analysis(year=year, method_reference=_method_or_none(method_reference))
