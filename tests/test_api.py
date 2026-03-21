from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.api.main import app
from app.storage.app_state import load_import_batches, load_transactions
from app.storage.settings import get_paths

SAMPLES_DIR = Path(__file__).resolve().parents[1] / "samples"


def test_api_import_calc_and_report():
    client = TestClient(app)

    assert client.get("/dashboard").status_code == 200
    assert client.get("/import").status_code == 200

    with (SAMPLES_DIR / "manual_adjustments_sample.csv").open("rb") as fh:
        response = client.post(
            "/api/v1/import/manual-adjustments",
            files={"file": ("manual_adjustments_sample.csv", fh, "text/csv")},
        )
    assert response.status_code == 200

    with (SAMPLES_DIR / "binance_japan_sample.csv").open("rb") as fh:
        response = client.post(
            "/api/v1/import/csv",
            files={"file": ("binance_japan_sample.csv", fh, "text/csv")},
        )
    assert response.status_code == 200

    with (SAMPLES_DIR / "manual_rates_sample.csv").open("rb") as fh:
        response = client.post(
            "/api/v1/import/manual-rates",
            files={"file": ("manual_rates_sample.csv", fh, "text/csv")},
        )
    assert response.status_code == 200

    response = client.post(
        "/api/v1/calc/run",
        json={"year": 2025, "method": "moving_average"},
    )
    assert response.status_code == 200
    assert response.json()["yearly_summary"]["realized_pnl_jpy"] == 21500
    assert response.json()["yearly_summary"]["misc_income_candidate_jpy"] == 41500

    response = client.get("/api/v1/reports/yearly", params={"year": 2025, "method": "moving_average"})
    assert response.status_code == 200
    assert response.json()["year"] == 2025

    response = client.post(
        "/api/v1/calc/run-window",
        json={"start_year": 2024, "end_year": 2025, "method": "moving_average"},
    )
    assert response.status_code == 200
    assert response.json()["aggregate_summary"]["realized_pnl_jpy"] == 21500
    assert response.json()["aggregate_summary"]["scope_transaction_count"] == 5

    response = client.get(
        "/api/v1/calc/window-latest",
        params={"start_year": 2024, "end_year": 2025, "method": "moving_average"},
    )
    assert response.status_code == 200
    assert response.json()["start_year"] == 2024
    assert response.json()["end_year"] == 2025

    response = client.get("/api/v1/transactions/review-required", params={"year": 2025})
    assert response.status_code == 200
    assert response.json()["count"] >= 1

    response = client.post("/api/v1/import/reset")
    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_api_import_invalid_csv_is_review_required_not_500(tmp_path):
    client = TestClient(app)
    bad = tmp_path / "bad.csv"
    bad.write_text("foo,bar\n1,2\n", encoding="utf-8")

    with bad.open("rb") as fh:
        response = client.post(
            "/api/v1/import/csv",
            files={"file": ("bad.csv", fh, "text/csv")},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["transaction_count"] == 1
    assert payload["review_required_count"] == 1
    assert sorted(payload["unknown_column_names"]) == ["bar", "foo"]


def test_api_import_data_folder_recursively():
    client = TestClient(app)
    paths = get_paths()

    exchange_dir = paths.data / "exchange"
    manual_dir = paths.data / "manual_adjustments"
    rate_dir = paths.data / "manual_rates"
    exchange_dir.mkdir(parents=True, exist_ok=True)
    manual_dir.mkdir(parents=True, exist_ok=True)
    rate_dir.mkdir(parents=True, exist_ok=True)

    (exchange_dir / "Binance-取引履歴.csv").write_text(
        "\n".join(
            [
                "ユーザーID,時間,アカウント,操作,コイン,変更,備考",
                "1,25-07-17 09:18:07,Spot,Deposit,JPY,15000,",
                "1,25-07-17 09:55:49,Spot,Binance Convert,JPY,-15000,",
                "1,25-07-17 09:55:49,Spot,Binance Convert,ETH,0.02929314,",
            ]
        )
        + "\n",
        encoding="utf-8-sig",
    )
    (manual_dir / "my_adjustments.csv").write_text(
        "\n".join(
            [
                "timestamp_utc,tx_type,asset,quantity,gross_amount_jpy,side,note",
                "2025-12-31T00:00:00Z,adjustment,JPY,1000,1000,none,test adjustment",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (rate_dir / "jpy_rates.csv").write_text(
        "\n".join(
            [
                "timestamp_utc,asset,jpy_rate,source",
                "2025-07-17T00:00:00Z,ETH,500000,manual",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    response = client.post("/api/v1/import/data-folder")
    assert response.status_code == 200
    payload = response.json()
    assert payload["scanned_file_count"] == 3
    assert payload["imported_file_count"] == 3
    assert payload["error_count"] == 0

    categories = {row["category"] for row in payload["imported_files"]}
    assert categories == {"exchange_history", "manual_adjustment", "manual_rate"}
    assert len(load_transactions()) == 3
    assert len(load_import_batches()) == 2
