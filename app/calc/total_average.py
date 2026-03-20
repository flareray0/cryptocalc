from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from app.calc.inventory_engine import resolve_transaction_flow
from app.calc.normalizer import sort_transactions
from app.domain.enums import CalculationMethod
from app.domain.models import CalculationRunResult, RealizedPnlRecord, RunningPosition


ZERO = Decimal("0")


def calculate_total_average(transactions: list, year: int, rate_lookup) -> CalculationRunResult:
    ordered = sort_transactions(transactions)
    asset_years: dict[str, dict[int, dict[str, Decimal]]] = defaultdict(
        lambda: defaultdict(
            lambda: {
                "opening_qty": ZERO,
                "opening_cost": ZERO,
                "acquired_qty": ZERO,
                "acquired_cost": ZERO,
                "disposed_qty": ZERO,
                "proceeds": ZERO,
                "fee": ZERO,
                "reward_income": ZERO,
            }
        )
    )
    review_required: list = []
    audit_rows: list[dict] = []
    inventory_timeline: list[dict] = []

    for tx in ordered:
        flow = resolve_transaction_flow(tx, rate_lookup)
        timestamp = tx.timestamp_jst
        if timestamp is None:
            continue

        if flow.acquire_asset and flow.acquire_quantity and flow.acquire_value_jpy is not None:
            asset_state = asset_years[flow.acquire_asset][timestamp.year]
            asset_state["acquired_qty"] += flow.acquire_quantity
            asset_state["acquired_cost"] += flow.acquire_value_jpy
            if flow.income_jpy:
                asset_state["reward_income"] += flow.income_jpy

        if flow.dispose_asset and flow.dispose_quantity:
            asset_state = asset_years[flow.dispose_asset][timestamp.year]
            asset_state["disposed_qty"] += flow.dispose_quantity
            asset_state["proceeds"] += flow.proceeds_jpy or ZERO
            asset_state["fee"] += flow.fee_jpy or ZERO

        if flow.review_flag and timestamp.year == year:
            review_required.append(tx)

        if timestamp.year == year:
            audit_rows.append(
                {
                    "tx_id": tx.id,
                    "source_file": tx.source_file,
                    "raw_row_number": tx.raw_row_number,
                    "timestamp_jst": timestamp,
                    "tx_type": tx.tx_type.value,
                    "base_asset": tx.base_asset,
                    "quote_asset": tx.quote_asset,
                    "used_rate_source": flow.rate_source,
                    "quantity": tx.quantity,
                    "gross_amount_jpy": tx.gross_amount_jpy,
                    "fee_jpy": flow.fee_jpy,
                    "review_flag": flow.review_flag,
                    "review_reason": "; ".join(flow.review_reasons or []),
                }
            )

    realized_records: list[RealizedPnlRecord] = []
    asset_summaries: list[dict] = []
    running_positions: dict[str, RunningPosition] = {}

    for asset, per_year in sorted(asset_years.items()):
        opening_qty = ZERO
        opening_cost = ZERO
        for calc_year in sorted(per_year.keys()):
            state = per_year[calc_year]
            state["opening_qty"] = opening_qty
            state["opening_cost"] = opening_cost
            total_qty = opening_qty + state["acquired_qty"]
            total_cost = opening_cost + state["acquired_cost"]
            avg_cost = ZERO if total_qty == ZERO else total_cost / total_qty
            cost_basis = avg_cost * state["disposed_qty"]
            ending_qty = total_qty - state["disposed_qty"]
            ending_cost = ZERO if ending_qty <= ZERO else avg_cost * ending_qty

            inventory_timeline.append(
                {
                    "timestamp": f"{calc_year}-12-31T23:59:59+09:00",
                    "asset": asset,
                    "event": "year_close",
                    "avg_cost_per_unit_jpy": avg_cost,
                    "cost_basis_total_jpy": ending_cost,
                    "year": calc_year,
                }
            )

            if calc_year == year:
                if state["disposed_qty"] > ZERO or state["proceeds"] > ZERO:
                    realized_records.append(
                        RealizedPnlRecord(
                            timestamp=None,
                            asset=asset,
                            quantity_disposed=state["disposed_qty"],
                            proceeds_jpy=state["proceeds"],
                            cost_basis_jpy=cost_basis,
                            fee_jpy=state["fee"],
                            realized_pnl_jpy=state["proceeds"] - cost_basis - state["fee"],
                            calculation_method=CalculationMethod.TOTAL_AVERAGE,
                            source_tx_ids=[],
                        )
                    )
                asset_summaries.append(
                    {
                        "asset": asset,
                        "acquired_quantity": state["acquired_qty"],
                        "disposed_quantity": state["disposed_qty"],
                        "ending_quantity": ending_qty,
                        "realized_pnl_jpy": state["proceeds"] - cost_basis - state["fee"],
                        "average_cost_per_unit_jpy": avg_cost,
                        "acquired_cost_jpy": state["acquired_cost"],
                        "proceeds_jpy": state["proceeds"],
                        "fee_jpy": state["fee"],
                        "reward_income_jpy": state["reward_income"],
                        "opening_quantity": state["opening_qty"],
                        "opening_cost_jpy": state["opening_cost"],
                    }
                )
                running_positions[asset] = RunningPosition(
                    asset=asset,
                    quantity=ending_qty,
                    cost_basis_total_jpy=ending_cost,
                    avg_cost_per_unit_jpy=avg_cost,
                    method=CalculationMethod.TOTAL_AVERAGE,
                    last_updated_at=None,
                )

            opening_qty = ending_qty
            opening_cost = ending_cost

    realized_pnl_total = sum((row.realized_pnl_jpy or ZERO) for row in realized_records)
    reward_income_total = sum(row["reward_income_jpy"] for row in asset_summaries)
    yearly_summary = {
        "year": year,
        "method": CalculationMethod.TOTAL_AVERAGE.value,
        "income_amount_jpy": sum((row.proceeds_jpy or ZERO) for row in realized_records)
        + reward_income_total,
        "necessary_expenses_jpy": sum((row.cost_basis_jpy or ZERO) + (row.fee_jpy or ZERO) for row in realized_records),
        "realized_pnl_jpy": realized_pnl_total,
        "reward_income_jpy": reward_income_total,
        "misc_income_candidate_jpy": realized_pnl_total + reward_income_total,
        "fee_jpy_total": sum((row.fee_jpy or ZERO) for row in realized_records),
        "review_required_count": len(review_required),
        "generated_at": datetime.now().isoformat(),
    }

    return CalculationRunResult(
        run_id=f"calc_{CalculationMethod.TOTAL_AVERAGE.value}_{year}_{uuid4().hex[:10]}",
        year=year,
        method=CalculationMethod.TOTAL_AVERAGE,
        realized_records=realized_records,
        positions=sorted(running_positions.values(), key=lambda row: row.asset),
        review_required_transactions=review_required,
        yearly_summary=yearly_summary,
        asset_summaries=asset_summaries,
        audit_rows=audit_rows,
        inventory_timeline=inventory_timeline,
        source_transaction_count=len(ordered),
    )
