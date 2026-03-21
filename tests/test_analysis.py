from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from app.api.main import app
from app.domain.enums import CalculationMethod, ClassificationStatus, ImportSourceKind, Side, TransactionType
from app.domain.models import NormalizedTransaction
from app.parsers.binance_japan_parser import BinanceJapanParser
from app.services.analysis_service import AnalysisService
from app.services.balance_reconciliation_service import BalanceReconciliationService
from app.services.import_service import ImportService
from app.storage.app_state import save_transactions
from app.storage.analysis_state import load_latest_analysis_run
from app.storage.analysis_window_state import load_latest_analysis_window_run

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "samples"


def _load_sample_data():
    service = ImportService()
    service.import_file(SAMPLES_DIR / "manual_adjustments_sample.csv", import_kind="manual_adjustment")
    service.import_file(SAMPLES_DIR / "binance_japan_sample.csv")
    service.import_manual_rate_file(SAMPLES_DIR / "manual_rates_sample.csv")


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


def test_analysis_tracks_external_flows_and_no_withdrawal_equity():
    save_transactions(
        [
            NormalizedTransaction(
                id="tx_deposit",
                source_exchange="manual",
                source_file="fixture",
                raw_row_number=1,
                timestamp_jst=datetime(2025, 1, 1, 9, 0, tzinfo=timezone.utc).astimezone(),
                timestamp_utc=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
                tx_type=TransactionType.TRANSFER_IN,
                base_asset="JPY",
                quote_asset=None,
                quantity=Decimal("100000"),
                quote_quantity=None,
                unit_price_quote=None,
                price_per_unit_jpy=Decimal("1"),
                gross_amount_jpy=Decimal("100000"),
                fee_asset=None,
                fee_amount=None,
                fee_jpy=None,
                side=Side.NONE,
                note="deposit",
                raw_payload={},
                classification_status=ClassificationStatus.CLASSIFIED,
                review_flag=False,
                review_reasons=[],
                source_kind=ImportSourceKind.MANUAL,
                jpy_rate_source="manual",
            ),
            NormalizedTransaction(
                id="tx_buy",
                source_exchange="manual",
                source_file="fixture",
                raw_row_number=2,
                timestamp_jst=datetime(2025, 1, 10, 9, 0, tzinfo=timezone.utc).astimezone(),
                timestamp_utc=datetime(2025, 1, 10, 0, 0, tzinfo=timezone.utc),
                tx_type=TransactionType.BUY,
                base_asset="BTC",
                quote_asset="JPY",
                quantity=Decimal("0.01"),
                quote_quantity=Decimal("100000"),
                unit_price_quote=Decimal("10000000"),
                price_per_unit_jpy=Decimal("10000000"),
                gross_amount_jpy=Decimal("100000"),
                fee_asset="JPY",
                fee_amount=Decimal("0"),
                fee_jpy=Decimal("0"),
                side=Side.BUY,
                note="buy",
                raw_payload={},
                classification_status=ClassificationStatus.CLASSIFIED,
                review_flag=False,
                review_reasons=[],
                source_kind=ImportSourceKind.MANUAL,
                jpy_rate_source="manual",
            ),
            NormalizedTransaction(
                id="tx_sell",
                source_exchange="manual",
                source_file="fixture",
                raw_row_number=3,
                timestamp_jst=datetime(2025, 6, 1, 9, 0, tzinfo=timezone.utc).astimezone(),
                timestamp_utc=datetime(2025, 6, 1, 0, 0, tzinfo=timezone.utc),
                tx_type=TransactionType.SELL,
                base_asset="BTC",
                quote_asset="JPY",
                quantity=Decimal("0.004"),
                quote_quantity=Decimal("60000"),
                unit_price_quote=Decimal("15000000"),
                price_per_unit_jpy=Decimal("15000000"),
                gross_amount_jpy=Decimal("60000"),
                fee_asset="JPY",
                fee_amount=Decimal("500"),
                fee_jpy=Decimal("500"),
                side=Side.SELL,
                note="sell",
                raw_payload={},
                classification_status=ClassificationStatus.CLASSIFIED,
                review_flag=False,
                review_reasons=[],
                source_kind=ImportSourceKind.MANUAL,
                jpy_rate_source="manual",
            ),
            NormalizedTransaction(
                id="tx_withdraw",
                source_exchange="manual",
                source_file="fixture",
                raw_row_number=4,
                timestamp_jst=datetime(2025, 7, 1, 9, 0, tzinfo=timezone.utc).astimezone(),
                timestamp_utc=datetime(2025, 7, 1, 0, 0, tzinfo=timezone.utc),
                tx_type=TransactionType.TRANSFER_OUT,
                base_asset="JPY",
                quote_asset=None,
                quantity=Decimal("30000"),
                quote_quantity=None,
                unit_price_quote=None,
                price_per_unit_jpy=Decimal("1"),
                gross_amount_jpy=Decimal("30000"),
                fee_asset=None,
                fee_amount=None,
                fee_jpy=None,
                side=Side.NONE,
                note="withdraw",
                raw_payload={},
                classification_status=ClassificationStatus.CLASSIFIED,
                review_flag=False,
                review_reasons=[],
                source_kind=ImportSourceKind.MANUAL,
                jpy_rate_source="manual",
            ),
        ]
    )

    result = AnalysisService().run(year=2025, method_reference=CalculationMethod.MOVING_AVERAGE)
    latest = result["latest_snapshot"]

    assert latest["total_equity_jpy"] == "119500.00"
    assert latest["realized_pnl_jpy"] == "20000.00"
    assert latest["fees_jpy"] == "500.00"
    assert latest["external_inflows_jpy"] == "100000.00"
    assert latest["external_outflows_jpy"] == "30000.00"
    assert latest["net_external_flow_jpy"] == "70000.00"
    assert latest["equity_if_no_withdrawals_jpy"] == "149500.00"
    assert latest["equity_minus_net_contributions_jpy"] == "49500.00"

    page = TestClient(app).get(
        "/analysis",
        params={"year": 2025, "method_reference": "moving_average"},
    )
    assert page.status_code == 200
    assert "出金してなかったら JPY" in page.text
    assert "149500.00" in page.text
