from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.api.main import app


def test_api_import_calc_and_report():
    client = TestClient(app)

    assert client.get("/dashboard").status_code == 200
    assert client.get("/import").status_code == 200

    with Path(r"H:\cryptocalc\samples\manual_adjustments_sample.csv").open("rb") as fh:
        response = client.post(
            "/api/v1/import/manual-adjustments",
            files={"file": ("manual_adjustments_sample.csv", fh, "text/csv")},
        )
    assert response.status_code == 200

    with Path(r"H:\cryptocalc\samples\binance_japan_sample.csv").open("rb") as fh:
        response = client.post(
            "/api/v1/import/csv",
            files={"file": ("binance_japan_sample.csv", fh, "text/csv")},
        )
    assert response.status_code == 200

    with Path(r"H:\cryptocalc\samples\manual_rates_sample.csv").open("rb") as fh:
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
