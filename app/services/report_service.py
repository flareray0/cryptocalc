from __future__ import annotations

import csv
from pathlib import Path

from app.reports.asset_report import build_asset_report
from app.reports.audit_report import build_audit_report
from app.reports.nta_export import write_nta_exports
from app.reports.yearly_report import build_yearly_report
from app.storage.app_state import load_latest_calc_run
from app.storage.settings import get_paths


class ReportService:
    def __init__(self) -> None:
        self.paths = get_paths()

    def latest_run(self, method=None, year=None):
        return load_latest_calc_run(method=method, year=year)

    def yearly_summary(self, method=None, year=None) -> dict:
        run_data = self._require_run(method=method, year=year)
        return build_yearly_report(run_data)

    def asset_summary(self, method=None, year=None) -> list[dict]:
        run_data = self._require_run(method=method, year=year)
        return build_asset_report(run_data)

    def audit_rows(self, method=None, year=None) -> list[dict]:
        run_data = self._require_run(method=method, year=year)
        return build_audit_report(run_data)

    def inventory_timeline(self, method=None, year=None) -> list[dict]:
        run_data = self._require_run(method=method, year=year)
        return list(run_data.get("inventory_timeline", []))

    def export_csv(self, report_name: str, rows: list[dict]) -> Path:
        if not rows:
            raise ValueError("出力対象がありません")
        path = self.paths.exports / report_name
        headers = list(rows[0].keys())
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def nta_export(self, method=None, year=None) -> dict[str, str]:
        run_data = self._require_run(method=method, year=year)
        return write_nta_exports(run_data)

    def _require_run(self, method=None, year=None) -> dict:
        run_data = self.latest_run(method=method, year=year)
        if not run_data:
            raise ValueError("先に計算を実行してください")
        return run_data
