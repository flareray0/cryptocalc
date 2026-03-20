from __future__ import annotations

from fastapi import APIRouter

from app.api.schemas import CalcRunRequest
from app.domain.enums import CalculationMethod
from app.services.calc_service import CalcService


router = APIRouter(prefix="/api/v1", tags=["calc"])


@router.post("/calc/run")
def run_calc(request: CalcRunRequest):
    method = CalculationMethod(request.method)
    service = CalcService()
    return service.run(year=request.year, method=method)
