from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.api.main import app
from app.domain.enums import CalculationMethod
from app.services.analysis_service import AnalysisService
from app.services.import_service import ImportService
from app.storage.analysis_state import load_latest_analysis_run
from app.storage.analysis_window_state import load_latest_analysis_window_run


def _load_sample_data():
    service = ImportService()
    service.import_file(Path(r"H:\cryptocalc\samples\manual_adjustments_sample.csv"), import_kind="manual_adjustment")
    service.import_file(Path(r"H:\cryptocalc\samples\binance_japan_sample.csv"))
    service.import_manual_rate_file(Path(r"H:\cryptocalc\samples\manual_rates_sample.csv"))


def test_analysis_service_run_and_export():
    _load_sample_data()
    result = AnalysisService().run(year=2025, method_reference=CalculationMethod.MOVING_AVERAGE)

    assert result["latest_snapshot"]["total_equity_jpy"] == "67500.00"
    assert result["latest_snapshot"]["edge_vs_benchmark_jpy"] == "45500.00"
    assert result["latest_snapshot"]["trading_edge_jpy"] == "25500.00"

    stored = load_latest_analysis_run(year=2025, method_reference=CalculationMethod.MOVING_AVERAGE)
    assert stored is not None
    assert stored["asset_summary_table"][0]["symbol"] == "BNB"

    exports = AnalysisService().export_analysis(year=2025, method_reference=CalculationMethod.MOVING_AVERAGE)
    assert "portfolio_snapshot_csv" in exports
    assert exports["edge_report_json"].endswith(".json")


def test_analysis_api_and_page():
    _load_sample_data()
    client = TestClient(app)

    response = client.post(
        "/api/v1/analysis/run",
        json={"year": 2025, "method_reference": "moving_average"},
    )
    assert response.status_code == 200
    assert response.json()["latest_snapshot"]["total_equity_usd"] == "459.18"

    response = client.get("/api/v1/analysis/latest", params={"year": 2025, "method_reference": "moving_average"})
    assert response.status_code == 200
    assert response.json()["edge_report"]["final_edge_vs_benchmark_jpy"] == "45500.00"

    response = client.get("/analysis", params={"year": 2025, "method_reference": "moving_average"})
    assert response.status_code == 200
    assert "actual vs benchmark" in response.text


def test_analysis_window_service_and_api():
    _load_sample_data()
    service = AnalysisService()
    result = service.run_window(
        start_year=2024,
        end_year=2025,
        method_reference=CalculationMethod.MOVING_AVERAGE,
    )
    assert result["start_year"] == 2024
    assert result["end_year"] == 2025
    assert result["latest_snapshot"]["total_equity_jpy"] == "67500.00"

    stored = load_latest_analysis_window_run(
        start_year=2024,
        end_year=2025,
        method_reference=CalculationMethod.MOVING_AVERAGE,
    )
    assert stored is not None
    assert stored["portfolio_snapshots"][-1]["edge_vs_benchmark_jpy"] == "45500.00"

    client = TestClient(app)
    response = client.post(
        "/api/v1/analysis/run-window",
        json={"start_year": 2024, "end_year": 2025, "method_reference": "moving_average"},
    )
    assert response.status_code == 200
    assert response.json()["latest_snapshot"]["trading_edge_jpy"] == "25500.00"

    response = client.get(
        "/api/v1/analysis/window-latest",
        params={"start_year": 2024, "end_year": 2025, "method_reference": "moving_average"},
    )
    assert response.status_code == 200
    assert response.json()["start_year"] == 2024

    response = client.get(
        "/analysis",
        params={"start_year": 2024, "end_year": 2025, "method_reference": "moving_average"},
    )
    assert response.status_code == 200
    assert "期間分析 / 年跨ぎ分析" in response.text
