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


def calculate_moving_average(transactions: list, year: int, rate_lookup) -> CalculationRunResult:
    ordered = sort_transactions(transactions)
    positions: dict[str, RunningPosition] = {}
    realized_records: list[RealizedPnlRecord] = []
    review_required: list = []
    audit_rows: list[dict] = []
    inventory_timeline: list[dict] = []
    asset_stats: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {
            "acquired_quantity": ZERO,
            "disposed_quantity": ZERO,
            "acquired_cost_jpy": ZERO,
            "proceeds_jpy": ZERO,
            "fee_jpy": ZERO,
            "reward_income_jpy": ZERO,
            "realized_pnl_jpy": ZERO,
        }
    )

    for tx in ordered:
        flow = resolve_transaction_flow(tx, rate_lookup)
        timestamp = tx.timestamp_jst
        year_matches = timestamp is not None and timestamp.year == year

        if flow.acquire_asset and flow.acquire_quantity and flow.acquire_value_jpy is not None:
            position = positions.setdefault(
                flow.acquire_asset,
                RunningPosition(
                    asset=flow.acquire_asset,
                    quantity=ZERO,
                    cost_basis_total_jpy=ZERO,
                    avg_cost_per_unit_jpy=ZERO,
                    method=CalculationMethod.MOVING_AVERAGE,
                    last_updated_at=None,
                ),
            )
            position.quantity += flow.acquire_quantity
            position.cost_basis_total_jpy += flow.acquire_value_jpy
            position.avg_cost_per_unit_jpy = (
                ZERO if position.quantity == ZERO else position.cost_basis_total_jpy / position.quantity
            )
            position.last_updated_at = timestamp
            inventory_timeline.append(
                {
                    "timestamp": timestamp,
                    "asset": flow.acquire_asset,
                    "event": "acquire",
                    "quantity": flow.acquire_quantity,
                    "avg_cost_per_unit_jpy": position.avg_cost_per_unit_jpy,
                    "cost_basis_total_jpy": position.cost_basis_total_jpy,
                    "tx_id": tx.id,
                }
            )
            if year_matches:
                asset_stats[flow.acquire_asset]["acquired_quantity"] += flow.acquire_quantity
                asset_stats[flow.acquire_asset]["acquired_cost_jpy"] += flow.acquire_value_jpy
                if flow.income_jpy:
                    asset_stats[flow.acquire_asset]["reward_income_jpy"] += flow.income_jpy

        if flow.dispose_asset and flow.dispose_quantity:
            position = positions.setdefault(
                flow.dispose_asset,
                RunningPosition(
                    asset=flow.dispose_asset,
                    quantity=ZERO,
                    cost_basis_total_jpy=ZERO,
                    avg_cost_per_unit_jpy=ZERO,
                    method=CalculationMethod.MOVING_AVERAGE,
                    last_updated_at=None,
                ),
            )
            avg_cost = position.avg_cost_per_unit_jpy
            cost_basis = avg_cost * flow.dispose_quantity if flow.proceeds_jpy is not None else None
            if position.quantity < flow.dispose_quantity:
                flow.review_reasons = list(flow.review_reasons or [])
                if "保有数量不足" not in flow.review_reasons:
                    flow.review_reasons.append("保有数量不足")
            if cost_basis is not None:
                position.quantity -= flow.dispose_quantity
                position.cost_basis_total_jpy -= cost_basis
                if position.quantity <= ZERO:
                    position.quantity = ZERO
                    position.cost_basis_total_jpy = ZERO
                    position.avg_cost_per_unit_jpy = ZERO
                else:
                    position.avg_cost_per_unit_jpy = position.cost_basis_total_jpy / position.quantity
            position.last_updated_at = timestamp

            if year_matches:
                fee_jpy = flow.fee_jpy or ZERO
                pnl = (
                    None
                    if flow.proceeds_jpy is None or cost_basis is None
                    else flow.proceeds_jpy - cost_basis - fee_jpy
                )
                realized_records.append(
                    RealizedPnlRecord(
                        timestamp=timestamp,
                        asset=flow.dispose_asset,
                        quantity_disposed=flow.dispose_quantity,
                        proceeds_jpy=flow.proceeds_jpy,
                        cost_basis_jpy=cost_basis,
                        fee_jpy=flow.fee_jpy,
                        realized_pnl_jpy=pnl,
                        calculation_method=CalculationMethod.MOVING_AVERAGE,
                        source_tx_ids=[tx.id],
                        review_flag=flow.review_flag,
                        review_reason="; ".join(flow.review_reasons or []) or None,
                    )
                )
                asset_stats[flow.dispose_asset]["disposed_quantity"] += flow.dispose_quantity
                asset_stats[flow.dispose_asset]["proceeds_jpy"] += flow.proceeds_jpy or ZERO
                asset_stats[flow.dispose_asset]["fee_jpy"] += fee_jpy
                asset_stats[flow.dispose_asset]["realized_pnl_jpy"] += pnl or ZERO

            inventory_timeline.append(
                {
                    "timestamp": timestamp,
                    "asset": flow.dispose_asset,
                    "event": "dispose",
                    "quantity": flow.dispose_quantity,
                    "avg_cost_per_unit_jpy": position.avg_cost_per_unit_jpy,
                    "cost_basis_total_jpy": position.cost_basis_total_jpy,
                    "tx_id": tx.id,
                }
            )

        if flow.review_flag and year_matches:
            review_required.append(tx)

        if year_matches:
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

    position_rows = sorted(positions.values(), key=lambda row: row.asset)
    asset_summaries = []
    for asset, stats in sorted(asset_stats.items()):
        position = positions.get(asset)
        asset_summaries.append(
            {
                "asset": asset,
                "acquired_quantity": stats["acquired_quantity"],
                "disposed_quantity": stats["disposed_quantity"],
                "ending_quantity": position.quantity if position else ZERO,
                "realized_pnl_jpy": stats["realized_pnl_jpy"],
                "average_cost_per_unit_jpy": position.avg_cost_per_unit_jpy if position else ZERO,
                "acquired_cost_jpy": stats["acquired_cost_jpy"],
                "proceeds_jpy": stats["proceeds_jpy"],
                "fee_jpy": stats["fee_jpy"],
                "reward_income_jpy": stats["reward_income_jpy"],
            }
        )

    realized_pnl_total = sum((row.realized_pnl_jpy or ZERO) for row in realized_records)
    reward_income_total = sum(stats["reward_income_jpy"] for stats in asset_stats.values())
    yearly_summary = {
        "year": year,
        "method": CalculationMethod.MOVING_AVERAGE.value,
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
        run_id=f"calc_{CalculationMethod.MOVING_AVERAGE.value}_{year}_{uuid4().hex[:10]}",
        year=year,
        method=CalculationMethod.MOVING_AVERAGE,
        realized_records=realized_records,
        positions=position_rows,
        review_required_transactions=review_required,
        yearly_summary=yearly_summary,
        asset_summaries=asset_summaries,
        audit_rows=audit_rows,
        inventory_timeline=inventory_timeline,
        source_transaction_count=len(ordered),
    )
