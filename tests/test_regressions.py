from __future__ import annotations

from pathlib import Path

from app.domain.enums import CalculationMethod
from app.parsers.binance_japan_parser import BinanceJapanParser
from app.services.calc_service import CalcService
from app.services.exchange_sync_service import ExchangeSyncService
from app.services.import_service import ImportService
from app.services.report_service import ReportService
from app.storage.app_state import load_sync_status


def test_methods_can_differ_when_late_year_buy_changes_total_average(tmp_path):
    manual_csv = tmp_path / "opening.csv"
    manual_csv.write_text(
        "timestamp_utc,tx_type,asset,quantity,price_per_unit_jpy,gross_amount_jpy,side,note\n",
        encoding="utf-8",
    )
    trade_csv = tmp_path / "late_buy.csv"
    trade_csv.write_text(
        "\n".join(
            [
                "Date(UTC),Pair,Base Asset,Quote Asset,Type,Price,Amount,Total,Fee,Fee Coin",
                "2025-01-01 00:00:00,BTC/JPY,BTC,JPY,BUY,100,1,100,0,JPY",
                "2025-02-01 00:00:00,BTC/JPY,BTC,JPY,SELL,200,1,200,0,JPY",
                "2025-12-01 00:00:00,BTC/JPY,BTC,JPY,BUY,300,1,300,0,JPY",
            ]
        ),
        encoding="utf-8",
    )

    importer = ImportService()
    importer.import_file(manual_csv, import_kind="manual_adjustment")
    importer.import_file(trade_csv)

    moving = CalcService().run(year=2025, method=CalculationMethod.MOVING_AVERAGE)
    total = CalcService().run(year=2025, method=CalculationMethod.TOTAL_AVERAGE)

    assert moving["yearly_summary"]["realized_pnl_jpy"] == 100
    assert total["yearly_summary"]["realized_pnl_jpy"] == 0


def test_unknown_type_is_review_required(tmp_path):
    path = tmp_path / "unknown.csv"
    path.write_text(
        "\n".join(
            [
                "Date(UTC),Pair,Base Asset,Quote Asset,Type,Price,Amount,Total,Fee,Fee Coin",
                "2025-01-01 00:00:00,BTC/JPY,BTC,JPY,MYSTERY,10000000,0.01,100000,0,JPY",
            ]
        ),
        encoding="utf-8",
    )
    batch = BinanceJapanParser().parse(path)
    assert batch.review_required_count == 1
    assert "MYSTERY" in batch.unknown_tx_types


def test_duplicate_detection_across_files(tmp_path):
    content = "\n".join(
        [
            "Date(UTC),Pair,Base Asset,Quote Asset,Type,Price,Amount,Total,Fee,Fee Coin",
            "2025-01-01 00:00:00,BTC/JPY,BTC,JPY,BUY,10000000,0.01,100000,0,JPY",
        ]
    )
    file_a = tmp_path / "a.csv"
    file_b = tmp_path / "b.csv"
    file_a.write_text(content, encoding="utf-8")
    file_b.write_text(content, encoding="utf-8")

    importer = ImportService()
    importer.import_file(file_a)
    batch_b = importer.import_file(file_b)

    assert batch_b.duplicate_count >= 2


def test_nta_export_files_are_generated():
    importer = ImportService()
    importer.import_file(Path(r"H:\cryptocalc\samples\manual_adjustments_sample.csv"), import_kind="manual_adjustment")
    importer.import_file(Path(r"H:\cryptocalc\samples\binance_japan_sample.csv"))
    importer.import_manual_rate_file(Path(r"H:\cryptocalc\samples\manual_rates_sample.csv"))
    CalcService().run(year=2025, method=CalculationMethod.MOVING_AVERAGE)

    export_paths = ReportService().nta_export(method=CalculationMethod.MOVING_AVERAGE, year=2025)

    assert Path(export_paths["csv"]).exists()
    assert Path(export_paths["xlsx"]).exists()


def test_api_connection_status_does_not_expose_secret(monkeypatch):
    class DummyClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def test_connection(self):
            return {"accountType": "SPOT", "canTrade": True, "balances": []}

    monkeypatch.setattr(
        "app.services.exchange_sync_service.BinanceJapanApiClient",
        DummyClient,
    )

    ExchangeSyncService().save_connection(
        api_key="demo-key",
        api_secret="demo-secret",
        base_url="https://api.binance.com",
    )
    status = load_sync_status()
    assert status["connected"] is True
    assert "api_secret" not in status
    assert "api_key" not in status
