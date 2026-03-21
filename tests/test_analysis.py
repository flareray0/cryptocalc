from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient

from app.api.main import app
from app.domain.enums import CalculationMethod
from app.parsers.binance_japan_parser import BinanceJapanParser
from app.services.analysis_service import AnalysisService
from app.services.balance_reconciliation_service import BalanceReconciliationService
from app.services.import_service import ImportService
from app.storage.app_state import save_transactions
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


def test_analysis_review_swap_uses_cost_carry_forward_not_estimated_realized(tmp_path):
    sample = tmp_path / "review_swap.csv"
    sample.write_text(
        "\n".join(
            [
                "ユーザーID,時間,アカウント,操作,コイン,変更,備考",
                "1,25-07-17 09:18:07,Spot,Deposit,JPY,15000,",
                "1,25-07-17 09:55:49,Spot,Binance Convert,JPY,-15000,",
                "1,25-07-17 09:55:49,Spot,Binance Convert,ETH,0.02929314,",
                "1,25-07-17 10:26:25,Spot,Binance Convert,BTC,0.00082662,",
                "1,25-07-17 10:26:25,Spot,Binance Convert,ETH,-0.02929314,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    batch = BinanceJapanParser().parse(sample)
    save_transactions(batch.transactions)

    result = AnalysisService().run(year=2025, method_reference=CalculationMethod.TOTAL_AVERAGE)
    latest = result["latest_snapshot"]
    stored = load_latest_analysis_run(year=2025, method_reference=CalculationMethod.TOTAL_AVERAGE)

    assert latest["realized_pnl_jpy"] == "0.00"
    assert latest["fees_jpy"] == "0.00"
    assert stored is not None
    btc_row = next(row for row in stored["asset_summary_table"] if row["symbol"] == "BTC")
    assert btc_row["current_quantity"] == "0.00082662"


def test_balance_reconciliation_service_and_api(monkeypatch):
    _load_sample_data()
    AnalysisService().run_window(
        start_year=2024,
        end_year=2025,
        method_reference=CalculationMethod.MOVING_AVERAGE,
    )

    class DummyClient:
        def test_connection(self):
            return {
                "accountType": "SPOT",
                "canTrade": True,
                "balances": [
                    {"asset": "BTC", "free": "0.007", "locked": "0"},
                    {"asset": "ETH", "free": "1.2", "locked": "0"},
                    {"asset": "BNB", "free": "0.2", "locked": "0"},
                    {"asset": "JPY", "free": "1000", "locked": "0"},
                ],
            }

    monkeypatch.setattr(
        BalanceReconciliationService,
        "_load_client",
        lambda self: DummyClient(),
    )
    monkeypatch.setattr(
        BalanceReconciliationService,
        "_fetch_public_jpy_prices",
        lambda self, assets: {
            "BTC": Decimal("11000000"),
            "ETH": Decimal("22000"),
            "BNB": Decimal("100000"),
            "JPY": Decimal("1"),
        },
    )

    service = BalanceReconciliationService()
    payload = service.refresh(
        start_year=2024,
        end_year=2025,
        method_reference=CalculationMethod.MOVING_AVERAGE,
    )

    assert payload["exchange_total_equity_jpy"] == "124400.00"
    assert payload["reconstructed_total_equity_jpy"] == "67500.00"
    assert payload["difference_vs_analysis_jpy"] == "56900.00"
    assert payload["difference_vs_current_price_reconstruction_jpy"] is not None
    assert payload["balance_rows"][0]["symbol"] == "BTC"
    assert any(row["symbol"] == "JPY" and row["diff_quantity"] == "41500.00" for row in payload["balance_rows"])

    stored = service.latest(
        start_year=2024,
        end_year=2025,
        method_reference=CalculationMethod.MOVING_AVERAGE,
    )
    assert stored is not None
    assert stored["run_id"] == payload["run_id"]

    client = TestClient(app)
    response = client.post(
        "/api/v1/analysis/exchange-balance/refresh",
        json={
            "start_year": 2024,
            "end_year": 2025,
            "method_reference": "moving_average",
        },
    )
    assert response.status_code == 200
    assert response.json()["exchange_total_equity_jpy"] == "124400.00"

    response = client.get(
        "/api/v1/analysis/exchange-balance/latest",
        params={
            "start_year": 2024,
            "end_year": 2025,
            "method_reference": "moving_average",
        },
    )
    assert response.status_code == 200
    assert response.json()["difference_vs_analysis_jpy"] == "56900.00"

    page = client.get(
        "/analysis",
        params={
            "start_year": 2024,
            "end_year": 2025,
            "method_reference": "moving_average",
        },
    )
    assert page.status_code == 200
    assert "現在残高照合テーブル" in page.text
    assert "Binance残高ベース総資産 JPY" in page.text
