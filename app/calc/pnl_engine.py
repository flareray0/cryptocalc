from __future__ import annotations

from app.calc.moving_average import calculate_moving_average
from app.calc.total_average import calculate_total_average
from app.domain.enums import CalculationMethod
from app.domain.models import CalculationRunResult


def run_pnl_calculation(
    transactions: list,
    year: int,
    method: CalculationMethod,
    rate_lookup,
) -> CalculationRunResult:
    if method is CalculationMethod.MOVING_AVERAGE:
        return calculate_moving_average(transactions, year, rate_lookup)
    return calculate_total_average(transactions, year, rate_lookup)
