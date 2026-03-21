from __future__ import annotations

from pathlib import Path

from app.domain.enums import CalculationMethod
from app.services.calc_service import CalcService
from app.services.import_service import ImportService
from app.storage.app_state import load_latest_calc_run
from app.storage.calc_window_state import load_latest_calc_window_run

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "samples"


def _load_sample_data():
    service = ImportService()
    service.import_file(SAMPLES_DIR / "manual_adjustments_sample.csv", import_kind="manual_adjustment")
    service.import_file(SAMPLES_DIR / "binance_japan_sample.csv")
    service.import_manual_rate_file(SAMPLES_DIR / "manual_rates_sample.csv")


def _import_manual_csv(tmp_path, content: str):
    path = tmp_path / "manual_adjustments.csv"
    path.write_text(content.strip() + "\n", encoding="utf-8")
    ImportService().import_file(path, import_kind="manual_adjustment")


def test_moving_average_calculation():
    _load_sample_data()
    result = CalcService().run(year=2025, method=CalculationMethod.MOVING_AVERAGE)

    assert result["yearly_summary"]["realized_pnl_jpy"] == 21500
    assert result["yearly_summary"]["misc_income_candidate_jpy"] == 41500
    assert any("要確認取引が 2 件あります" in warning for warning in result["warnings"])
    assert not any("保有数量がマイナス" in warning for warning in result["warnings"])
    run_data = load_latest_calc_run(method=CalculationMethod.MOVING_AVERAGE, year=2025)
    assert run_data["warnings"] == result["warnings"]
    btc_row = next(row for row in run_data["asset_summaries"] if row["asset"] == "BTC")
    assert str(btc_row["ending_quantity"]) == "0.006"


def test_total_average_calculation():
    _load_sample_data()
    result = CalcService().run(year=2025, method=CalculationMethod.TOTAL_AVERAGE)

    assert result["yearly_summary"]["realized_pnl_jpy"] == 21500
    assert result["yearly_summary"]["misc_income_candidate_jpy"] == 41500
    assert any("要確認取引が 2 件あります" in warning for warning in result["warnings"])
    assert not any("保有数量がマイナス" in warning for warning in result["warnings"])
    run_data = load_latest_calc_run(method=CalculationMethod.TOTAL_AVERAGE, year=2025)
    assert run_data["yearly_summary"]["year"] == 2025
    assert run_data["warnings"] == result["warnings"]


def test_calc_window_range_and_all_time():
    _load_sample_data()
    service = CalcService()

    range_result = service.run_window(
        start_year=2024,
        end_year=2025,
        method=CalculationMethod.MOVING_AVERAGE,
    )
    assert range_result["aggregate_summary"]["start_year"] == 2024
    assert range_result["aggregate_summary"]["end_year"] == 2025
    assert range_result["aggregate_summary"]["realized_pnl_jpy"] == 21500
    assert range_result["aggregate_summary"]["misc_income_candidate_jpy"] == 41500
    assert range_result["aggregate_summary"]["scope_transaction_count"] == 5
    assert any("要確認取引が 2 件あります" in warning for warning in range_result["warnings"])
    assert not any("保有数量がマイナス" in warning for warning in range_result["warnings"])
    btc_row = next(row for row in range_result["asset_summaries"] if row["asset"] == "BTC")
    assert str(btc_row["opening_quantity"]) == "0"
    assert str(btc_row["ending_quantity"]) == "0.006"

    latest = load_latest_calc_window_run(
        method=CalculationMethod.MOVING_AVERAGE,
        start_year=2024,
        end_year=2025,
    )
    assert latest["run_id"] == range_result["run_id"]
    assert latest["warnings"] == range_result["warnings"]

    all_time_result = service.run_window(
        start_year=None,
        end_year=None,
        method=CalculationMethod.TOTAL_AVERAGE,
    )
    assert all_time_result["aggregate_summary"]["start_year"] == 2024
    assert all_time_result["aggregate_summary"]["end_year"] == 2025
    assert len(all_time_result["yearly_rows"]) == 2


def test_transfer_in_and_out_are_reflected_in_moving_average(tmp_path):
    _import_manual_csv(
        tmp_path,
        """
timestamp_utc,tx_type,asset,quantity,price_per_unit_jpy,gross_amount_jpy,side,note
2024-12-31 00:00:00,opening_balance,BTC,1,10000000,10000000,buy,opening
2025-01-10 00:00:00,transfer_in,BTC,0.5,,,none,external deposit
2025-02-15 00:00:00,transfer_out,BTC,0.2,,,none,external withdraw
""",
    )

    result = CalcService().run(year=2025, method=CalculationMethod.MOVING_AVERAGE)
    btc_row = next(row for row in result["asset_summaries"] if row["asset"] == "BTC")

    assert str(btc_row["ending_quantity"]) == "1.3"
    assert str(btc_row["transfer_in_quantity"]) == "0.5"
    assert str(btc_row["transfer_out_quantity"]) == "0.2"
    assert str(btc_row["unknown_cost_quantity"]) == "0.3"
    assert any("取得原価が未確定の入庫" in warning for warning in result["warnings"])


def test_transfer_in_and_out_are_reflected_in_total_average(tmp_path):
    _import_manual_csv(
        tmp_path,
        """
timestamp_utc,tx_type,asset,quantity,price_per_unit_jpy,gross_amount_jpy,side,note
2024-12-31 00:00:00,opening_balance,BTC,1,10000000,10000000,buy,opening
2025-01-10 00:00:00,transfer_in,BTC,0.5,,,none,external deposit
2025-02-15 00:00:00,transfer_out,BTC,0.2,,,none,external withdraw
""",
    )

    result = CalcService().run(year=2025, method=CalculationMethod.TOTAL_AVERAGE)
    btc_row = next(row for row in result["asset_summaries"] if row["asset"] == "BTC")

    assert str(btc_row["opening_quantity"]) == "1"
    assert str(btc_row["ending_quantity"]) == "1.3"
    assert str(btc_row["transfer_in_quantity"]) == "0.5"
    assert str(btc_row["transfer_out_quantity"]) == "0.2"
    assert str(btc_row["unknown_cost_quantity"]) == "0.3"
    assert any("取得原価が未確定の入庫" in warning for warning in result["warnings"])
