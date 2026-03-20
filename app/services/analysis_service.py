from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path
from typing import Any

from app.analysis.engine import run_portfolio_analysis, run_portfolio_analysis_window
from app.domain.enums import CalculationMethod
from app.integrations.rate_input_adapter import ManualRateTable
from app.services.audit_service import AuditService
from app.storage.analysis_state import load_latest_analysis_run, save_analysis_run
from app.storage.analysis_window_state import (
    load_latest_analysis_window_run,
    save_analysis_window_run,
)
from app.storage.app_state import load_transactions
from app.storage.json_store import dump_json
from app.storage.settings import get_paths, load_settings, save_settings
from app.domain.validators import safe_slug, serialize_payload


class AnalysisService:
    def __init__(self) -> None:
        self.paths = get_paths()
        self.audit = AuditService()

    def run(self, *, year: int, method_reference: CalculationMethod) -> dict[str, Any]:
        transactions = load_transactions()
        rate_table = self._load_rates(load_settings().get("manual_rate_file"))
        result = run_portfolio_analysis(
            transactions=transactions,
            year=year,
            method_reference=method_reference,
            manual_rate_table=rate_table,
        )
        save_analysis_run(result)
        self.audit.write_event(
            "analysis_run",
            {
                "run_id": result.run_id,
                "year": year,
                "method_reference": method_reference.value,
                "portfolio_snapshot_count": len(result.portfolio_snapshots),
                "asset_quantity_point_count": len(result.asset_quantity_history),
                "benchmark_snapshot_count": len(result.benchmark_snapshots),
                "review_note_count": len(result.review_notes),
            },
        )
        latest = result.portfolio_snapshots[-1] if result.portfolio_snapshots else None
        return {
            "run_id": result.run_id,
            "year": result.year,
            "method_reference": result.method_reference.value,
            "latest_snapshot": serialize_payload(asdict(latest)) if latest else None,
            "edge_report": result.edge_report,
            "review_notes": result.review_notes,
        }

    def latest_run(
        self,
        *,
        year: int | None = None,
        method_reference: CalculationMethod | None = None,
    ) -> dict[str, Any] | None:
        return load_latest_analysis_run(year=year, method_reference=method_reference)

    def export_analysis(
        self,
        *,
        year: int | None = None,
        method_reference: CalculationMethod | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> dict[str, str]:
        run_data = (
            self.latest_window_run(
                start_year=start_year,
                end_year=end_year,
                method_reference=method_reference,
            )
            if start_year is not None or end_year is not None
            else self.latest_run(year=year, method_reference=method_reference)
        )
        if not run_data:
            raise ValueError("先に分析を実行してください")

        method_slug = safe_slug(run_data["method_reference"])
        year_label = (
            f"{run_data['start_year']}_{run_data['end_year']}"
            if "start_year" in run_data and "end_year" in run_data
            else run_data["year"]
        )
        prefix = f"analysis_{year_label}_{method_slug}"

        export_map = {
            "portfolio_snapshot": run_data.get("portfolio_snapshots", []),
            "asset_quantity_history": run_data.get("asset_quantity_history", []),
            "pnl_breakdown": run_data.get("pnl_attribution_snapshots", []),
            "benchmark_comparison": run_data.get("benchmark_snapshots", []),
            "edge_report": [run_data.get("edge_report", {})],
        }

        generated: dict[str, str] = {}
        for name, rows in export_map.items():
            csv_path = self.paths.exports / f"{prefix}_{name}.csv"
            json_path = self.paths.exports / f"{prefix}_{name}.json"
            self._write_csv(csv_path, rows)
            dump_json(json_path, rows)
            generated[f"{name}_csv"] = csv_path.name
            generated[f"{name}_json"] = json_path.name
        return generated

    def run_window(
        self,
        *,
        start_year: int | None,
        end_year: int | None,
        method_reference: CalculationMethod,
    ) -> dict[str, Any]:
        transactions = load_transactions()
        available_years = self._available_years(transactions)
        if not available_years:
            raise ValueError("取引データがありません。先にCSVまたはAPI同期で取引を読み込んでください")

        resolved_start_year, resolved_end_year = self._resolve_year_window(
            available_years=available_years,
            start_year=start_year,
            end_year=end_year,
        )
        rate_table = self._load_rates(load_settings().get("manual_rate_file"))
        result = run_portfolio_analysis_window(
            transactions=transactions,
            start_year=resolved_start_year,
            end_year=resolved_end_year,
            method_reference=method_reference,
            manual_rate_table=rate_table,
        )
        result["available_years"] = available_years
        save_analysis_window_run(result)

        settings = load_settings()
        settings.setdefault("analysis_window", {})
        settings["analysis_window"]["default_start_year"] = resolved_start_year
        settings["analysis_window"]["default_end_year"] = resolved_end_year
        settings["default_method"] = method_reference.value
        save_settings(settings)

        self.audit.write_event(
            "analysis_window_run",
            {
                "run_id": result["run_id"],
                "start_year": resolved_start_year,
                "end_year": resolved_end_year,
                "method_reference": method_reference.value,
                "portfolio_snapshot_count": len(result.get("portfolio_snapshots", [])),
                "asset_quantity_point_count": len(result.get("asset_quantity_history", [])),
                "benchmark_snapshot_count": len(result.get("benchmark_snapshots", [])),
                "review_note_count": len(result.get("review_notes", [])),
            },
        )
        return {
            "run_id": result["run_id"],
            "start_year": resolved_start_year,
            "end_year": resolved_end_year,
            "method_reference": result["method_reference"],
            "latest_snapshot": result.get("latest_snapshot"),
            "edge_report": result.get("edge_report", {}),
            "review_notes": result.get("review_notes", []),
        }

    def latest_window_run(
        self,
        *,
        start_year: int | None = None,
        end_year: int | None = None,
        method_reference: CalculationMethod | None = None,
    ) -> dict[str, Any] | None:
        return load_latest_analysis_window_run(
            start_year=start_year,
            end_year=end_year,
            method_reference=method_reference,
        )

    def _write_csv(self, path: Path, rows: list[dict] | list[Any]) -> None:
        normalized_rows = [serialize_payload(row if isinstance(row, dict) else asdict(row)) for row in rows]
        if not normalized_rows:
            normalized_rows = [{"status": "empty"}]
        headers: list[str] = []
        for row in normalized_rows:
            for key in row.keys():
                if key not in headers:
                    headers.append(key)
        with path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=headers)
            writer.writeheader()
            writer.writerows(normalized_rows)

    def _load_rates(self, configured_path: str | None) -> ManualRateTable:
        if not configured_path:
            return ManualRateTable.empty()
        path = Path(configured_path)
        if not path.exists():
            return ManualRateTable.empty()
        return ManualRateTable.from_csv(path)

    def _available_years(self, transactions: list) -> list[int]:
        years = sorted(
            {
                tx.timestamp_jst.year
                for tx in transactions
                if tx.timestamp_jst is not None
            }
        )
        return years

    def _resolve_year_window(
        self,
        *,
        available_years: list[int],
        start_year: int | None,
        end_year: int | None,
    ) -> tuple[int, int]:
        minimum = min(available_years)
        maximum = max(available_years)
        resolved_start = max(start_year if start_year is not None else minimum, minimum)
        resolved_end = min(end_year if end_year is not None else maximum, maximum)
        if resolved_start > resolved_end:
            raise ValueError("開始年は終了年以下にしてください")
        return resolved_start, resolved_end
