from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from app.calc.inventory_engine import resolve_transaction_flow
from app.calc.normalizer import sort_transactions
from app.calc.pnl_engine import run_pnl_calculation
from app.domain.enums import CalculationMethod
from app.integrations.rate_input_adapter import ManualRateTable
from app.services.audit_service import AuditService
from app.storage.app_state import load_transactions, save_calc_run
from app.storage.calc_window_state import load_latest_calc_window_run, save_calc_window_run
from app.storage.settings import load_settings, save_settings


ZERO = Decimal("0")


class CalcService:
    def __init__(self) -> None:
        self.audit = AuditService()

    def run(self, *, year: int, method: CalculationMethod) -> dict:
        settings = load_settings()
        transactions = load_transactions()
        rate_table = self._load_rates(settings.get("manual_rate_file"))
        result = run_pnl_calculation(transactions, year, method, rate_table)
        result.warnings = self._build_inventory_warnings(result.asset_summaries, result.yearly_summary)
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
                "warning_count": len(result.warnings),
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
            "warnings": result.warnings,
        }

    def run_window(
        self,
        *,
        start_year: int | None,
        end_year: int | None,
        method: CalculationMethod,
    ) -> dict:
        settings = load_settings()
        transactions = load_transactions()
        available_years = self._available_years(transactions)
        if not available_years:
            raise ValueError("取引データがありません。先にCSVまたはAPI同期で取引を読み込んでください")

        resolved_start_year, resolved_end_year = self._resolve_year_window(
            available_years=available_years,
            start_year=start_year,
            end_year=end_year,
        )
        rate_table = self._load_rates(settings.get("manual_rate_file"))

        yearly_results = [
            run_pnl_calculation(transactions, year, method, rate_table)
            for year in range(resolved_start_year, resolved_end_year + 1)
        ]
        scope_transactions = [
            tx
            for tx in transactions
            if tx.timestamp_jst is not None and resolved_start_year <= tx.timestamp_jst.year <= resolved_end_year
        ]
        review_required_count = sum(1 for tx in scope_transactions if tx.review_flag)
        opening_positions = self._positions_until(
            transactions=transactions,
            cutoff_year=resolved_start_year - 1,
            method=method,
            rate_table=rate_table,
        )
        ending_positions = self._positions_until(
            transactions=transactions,
            cutoff_year=resolved_end_year,
            method=method,
            rate_table=rate_table,
        )
        generated_at = datetime.now().isoformat()
        payload = {
            "run_id": f"calc_window_{method.value}_{resolved_start_year}_{resolved_end_year}_{uuid4().hex[:10]}",
            "method": method.value,
            "start_year": resolved_start_year,
            "end_year": resolved_end_year,
            "generated_at": generated_at,
            "available_years": available_years,
            "yearly_rows": [result.yearly_summary for result in yearly_results],
            "aggregate_summary": self._aggregate_window_summary(
                yearly_results=yearly_results,
                start_year=resolved_start_year,
                end_year=resolved_end_year,
                generated_at=generated_at,
                scope_transaction_count=len(scope_transactions),
                review_required_count=review_required_count,
                source_transaction_count=len(transactions),
            ),
            "asset_summaries": self._aggregate_asset_summaries(
                yearly_results=yearly_results,
                opening_positions=opening_positions,
                ending_positions=ending_positions,
            ),
            "scope_transaction_count": len(scope_transactions),
            "source_transaction_count": len(transactions),
            "review_required_count": review_required_count,
        }
        payload["warnings"] = self._build_inventory_warnings(
            payload["asset_summaries"],
            payload["aggregate_summary"],
        )
        save_calc_window_run(payload)

        settings["calc_window"]["default_start_year"] = resolved_start_year
        settings["calc_window"]["default_end_year"] = resolved_end_year
        settings["default_method"] = method.value

        save_settings(settings)
        self.audit.write_event(
            "calc_window_run",
            {
                "run_id": payload["run_id"],
                "start_year": resolved_start_year,
                "end_year": resolved_end_year,
                "method": method.value,
                "scope_transaction_count": len(scope_transactions),
                "source_transaction_count": len(transactions),
                "review_required_count": review_required_count,
                "year_count": len(payload["yearly_rows"]),
                "warning_count": len(payload["warnings"]),
            },
        )
        return payload

    def latest_window_run(
        self,
        *,
        method: CalculationMethod | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> dict | None:
        return load_latest_calc_window_run(method=method, start_year=start_year, end_year=end_year)

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

    def _aggregate_window_summary(
        self,
        *,
        yearly_results: list,
        start_year: int,
        end_year: int,
        generated_at: str,
        scope_transaction_count: int,
        review_required_count: int,
        source_transaction_count: int,
    ) -> dict:
        def _sum(field_name: str) -> Decimal:
            total = ZERO
            for result in yearly_results:
                value = result.yearly_summary.get(field_name)
                if value is not None:
                    total += Decimal(str(value))
            return total

        return {
            "scope": "range",
            "start_year": start_year,
            "end_year": end_year,
            "year_count": len(yearly_results),
            "income_amount_jpy": _sum("income_amount_jpy"),
            "necessary_expenses_jpy": _sum("necessary_expenses_jpy"),
            "realized_pnl_jpy": _sum("realized_pnl_jpy"),
            "reward_income_jpy": _sum("reward_income_jpy"),
            "misc_income_candidate_jpy": _sum("misc_income_candidate_jpy"),
            "fee_jpy_total": _sum("fee_jpy_total"),
            "review_required_count": review_required_count,
            "scope_transaction_count": scope_transaction_count,
            "source_transaction_count": source_transaction_count,
            "generated_at": generated_at,
        }

    def _aggregate_asset_summaries(
        self,
        *,
        yearly_results: list,
        opening_positions: dict[str, dict[str, Decimal]],
        ending_positions: dict[str, dict[str, Decimal]],
    ) -> list[dict]:
        aggregated: dict[str, dict[str, Decimal | str | None]] = defaultdict(
            lambda: {
                "asset": None,
                "acquired_quantity": ZERO,
                "disposed_quantity": ZERO,
                "acquired_cost_jpy": ZERO,
                "proceeds_jpy": ZERO,
                "fee_jpy": ZERO,
                "reward_income_jpy": ZERO,
                "realized_pnl_jpy": ZERO,
            }
        )

        for result in yearly_results:
            for row in result.asset_summaries:
                asset = row["asset"]
                target = aggregated[asset]
                target["asset"] = asset
                for field_name in (
                    "acquired_quantity",
                    "disposed_quantity",
                    "acquired_cost_jpy",
                    "proceeds_jpy",
                    "fee_jpy",
                    "reward_income_jpy",
                    "realized_pnl_jpy",
                ):
                    value = row.get(field_name)
                    if value is not None:
                        target[field_name] += Decimal(str(value))

        rows = []
        all_assets = sorted(set(aggregated) | set(opening_positions) | set(ending_positions))
        for asset in all_assets:
            target = aggregated.get(
                asset,
                {
                    "asset": asset,
                    "acquired_quantity": ZERO,
                    "disposed_quantity": ZERO,
                    "acquired_cost_jpy": ZERO,
                    "proceeds_jpy": ZERO,
                    "fee_jpy": ZERO,
                    "reward_income_jpy": ZERO,
                    "realized_pnl_jpy": ZERO,
                },
            )
            opening = opening_positions.get(asset, {})
            ending = ending_positions.get(asset, {})
            rows.append(
                {
                    "asset": asset,
                    "opening_quantity": opening.get("quantity", ZERO),
                    "opening_cost_jpy": opening.get("cost_basis_total_jpy", ZERO),
                    "acquired_quantity": target["acquired_quantity"],
                    "disposed_quantity": target["disposed_quantity"],
                    "ending_quantity": ending.get("quantity", ZERO),
                    "ending_cost_jpy": ending.get("cost_basis_total_jpy", ZERO),
                    "average_cost_per_unit_jpy": ending.get("avg_cost_per_unit_jpy", ZERO),
                    "acquired_cost_jpy": target["acquired_cost_jpy"],
                    "proceeds_jpy": target["proceeds_jpy"],
                    "fee_jpy": target["fee_jpy"],
                    "reward_income_jpy": target["reward_income_jpy"],
                    "realized_pnl_jpy": target["realized_pnl_jpy"],
                    "period_net_quantity_change": ending.get("quantity", ZERO) - opening.get("quantity", ZERO),
                }
            )
        return rows

    def _positions_until(
        self,
        *,
        transactions: list,
        cutoff_year: int,
        method: CalculationMethod,
        rate_table: ManualRateTable,
    ) -> dict[str, dict[str, Decimal]]:
        if cutoff_year <= 0:
            return {}
        scoped_transactions = [
            tx
            for tx in transactions
            if tx.timestamp_jst is not None and tx.timestamp_jst.year <= cutoff_year
        ]
        if not scoped_transactions:
            return {}
        if method is CalculationMethod.TOTAL_AVERAGE:
            effective_year = max(
                tx.timestamp_jst.year
                for tx in scoped_transactions
                if tx.timestamp_jst is not None
            )
            result = run_pnl_calculation(scoped_transactions, effective_year, method, rate_table)
            return {
                row.asset: {
                    "quantity": row.quantity,
                    "cost_basis_total_jpy": row.cost_basis_total_jpy,
                    "avg_cost_per_unit_jpy": row.avg_cost_per_unit_jpy,
                }
                for row in result.positions
            }
        return self._moving_average_positions(scoped_transactions, rate_table)

    def _moving_average_positions(
        self,
        transactions: list,
        rate_table: ManualRateTable,
    ) -> dict[str, dict[str, Decimal]]:
        positions: dict[str, dict[str, Decimal]] = {}
        for tx in sort_transactions(transactions):
            flow = resolve_transaction_flow(tx, rate_table)
            if flow.acquire_asset and flow.acquire_quantity and flow.acquire_value_jpy is not None:
                position = positions.setdefault(
                    flow.acquire_asset,
                    {
                        "quantity": ZERO,
                        "cost_basis_total_jpy": ZERO,
                        "avg_cost_per_unit_jpy": ZERO,
                    },
                )
                position["quantity"] += flow.acquire_quantity
                position["cost_basis_total_jpy"] += flow.acquire_value_jpy
                position["avg_cost_per_unit_jpy"] = (
                    ZERO
                    if position["quantity"] == ZERO
                    else position["cost_basis_total_jpy"] / position["quantity"]
                )

            if flow.dispose_asset and flow.dispose_quantity:
                position = positions.setdefault(
                    flow.dispose_asset,
                    {
                        "quantity": ZERO,
                        "cost_basis_total_jpy": ZERO,
                        "avg_cost_per_unit_jpy": ZERO,
                    },
                )
                avg_cost = position["avg_cost_per_unit_jpy"]
                cost_basis = avg_cost * flow.dispose_quantity if flow.proceeds_jpy is not None else None
                if cost_basis is None:
                    continue
                position["quantity"] -= flow.dispose_quantity
                position["cost_basis_total_jpy"] -= cost_basis
                if position["quantity"] <= ZERO:
                    position["quantity"] = ZERO
                    position["cost_basis_total_jpy"] = ZERO
                    position["avg_cost_per_unit_jpy"] = ZERO
                else:
                    position["avg_cost_per_unit_jpy"] = (
                        position["cost_basis_total_jpy"] / position["quantity"]
                    )
        return positions

    def _build_inventory_warnings(self, asset_summaries: list[dict], summary: dict) -> list[str]:
        warnings: list[str] = []
        negative_assets = []
        for row in asset_summaries:
            opening = Decimal(str(row.get("opening_quantity", "0") or "0"))
            ending = Decimal(str(row.get("ending_quantity", "0") or "0"))
            if opening < ZERO or ending < ZERO:
                negative_assets.append(row["asset"])

        if negative_assets:
            joined = ", ".join(sorted(negative_assets))
            warnings.append(
                f"保有数量がマイナスになっている銘柄があります: {joined}。"
                " 期首残高・過去年の取引・外部入出庫が不足している可能性が高く、"
                "この集計は参考値として扱ってください。"
            )

        review_count = int(summary.get("review_required_count") or 0)
        if review_count > 0:
            warnings.append(
                f"要確認取引が {review_count} 件あります。"
                " JPY未評価や重複疑いが多い場合、実現損益が大きくぶれることがあります。"
            )
        return warnings
