from __future__ import annotations

from fastapi import APIRouter

from app.api.schemas import CalcRunRequest, CalcWindowRunRequest
from app.domain.enums import CalculationMethod
from app.services.calc_service import CalcService


router = APIRouter(prefix="/api/v1", tags=["calc"])


@router.post("/calc/run")
def run_calc(request: CalcRunRequest):
    method = CalculationMethod(request.method)
    service = CalcService()
    return service.run(year=request.year, method=method)


@router.post("/calc/run-window")
def run_calc_window(request: CalcWindowRunRequest):
    method = CalculationMethod(request.method)
    service = CalcService()
    return service.run_window(
        start_year=request.start_year,
        end_year=request.end_year,
        method=method,
    )


@router.get("/calc/window-latest")
def latest_calc_window(
    method: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
):
    selected_method = CalculationMethod(method) if method else None
    run_data = CalcService().latest_window_run(
        method=selected_method,
        start_year=start_year,
        end_year=end_year,
    )
    if not run_data:
        raise ValueError("先に期間集計を実行してください")
    return run_data
