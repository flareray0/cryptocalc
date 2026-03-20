from __future__ import annotations

from pathlib import Path

from app.domain.enums import CalculationMethod
from app.services.calc_service import CalcService
from app.services.import_service import ImportService
from app.storage.app_state import load_latest_calc_run


def _load_sample_data():
    service = ImportService()
    service.import_file(Path(r"H:\cryptocalc\samples\manual_adjustments_sample.csv"), import_kind="manual_adjustment")
    service.import_file(Path(r"H:\cryptocalc\samples\binance_japan_sample.csv"))
    service.import_manual_rate_file(Path(r"H:\cryptocalc\samples\manual_rates_sample.csv"))


def test_moving_average_calculation():
    _load_sample_data()
    result = CalcService().run(year=2025, method=CalculationMethod.MOVING_AVERAGE)

    assert result["yearly_summary"]["realized_pnl_jpy"] == 21500
    assert result["yearly_summary"]["misc_income_candidate_jpy"] == 41500
    run_data = load_latest_calc_run(method=CalculationMethod.MOVING_AVERAGE, year=2025)
    btc_row = next(row for row in run_data["asset_summaries"] if row["asset"] == "BTC")
    assert str(btc_row["ending_quantity"]) == "0.006"


def test_total_average_calculation():
    _load_sample_data()
    result = CalcService().run(year=2025, method=CalculationMethod.TOTAL_AVERAGE)

    assert result["yearly_summary"]["realized_pnl_jpy"] == 21500
    assert result["yearly_summary"]["misc_income_candidate_jpy"] == 41500
    run_data = load_latest_calc_run(method=CalculationMethod.TOTAL_AVERAGE, year=2025)
    assert run_data["yearly_summary"]["year"] == 2025
