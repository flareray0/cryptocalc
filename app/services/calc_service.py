from __future__ import annotations

from pathlib import Path

from app.calc.pnl_engine import run_pnl_calculation
from app.domain.enums import CalculationMethod
from app.integrations.rate_input_adapter import ManualRateTable
from app.services.audit_service import AuditService
from app.storage.app_state import load_transactions, save_calc_run
from app.storage.settings import load_settings


class CalcService:
    def __init__(self) -> None:
        self.audit = AuditService()

    def run(self, *, year: int, method: CalculationMethod) -> dict:
        settings = load_settings()
        transactions = load_transactions()
        rate_table = self._load_rates(settings.get("manual_rate_file"))
        result = run_pnl_calculation(transactions, year, method, rate_table)
        save_calc_run(result)
        self.audit.write_event(
            "calc_run",
            {
                "run_id": result.run_id,
                "year": year,
                "method": method.value,
                "transaction_count": result.source_transaction_count,
                "review_required_count": len(result.review_required_transactions),
                "realized_record_count": len(result.realized_records),
            },
        )
        return {
            "run_id": result.run_id,
            "year": result.year,
            "method": result.method.value,
            "yearly_summary": result.yearly_summary,
            "asset_summaries": result.asset_summaries,
            "audit_rows": result.audit_rows,
            "review_required_count": len(result.review_required_transactions),
        }

    def _load_rates(self, configured_path: str | None) -> ManualRateTable:
        if not configured_path:
            return ManualRateTable.empty()
        path = Path(configured_path)
        if not path.exists():
            return ManualRateTable.empty()
        return ManualRateTable.from_csv(path)
