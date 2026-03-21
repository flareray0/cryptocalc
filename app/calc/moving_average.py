from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from app.calc.inventory_engine import resolve_transaction_flow
from app.calc.normalizer import sort_transactions
from app.domain.enums import CalculationMethod, TransactionType
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
            "transfer_in_quantity": ZERO,
            "transfer_out_quantity": ZERO,
        }
    )

    def _recompute_position(position: RunningPosition) -> None:
        known_quantity = position.quantity - position.unknown_cost_quantity
        if position.quantity <= ZERO:
            position.quantity = ZERO
            position.cost_basis_total_jpy = ZERO
            position.avg_cost_per_unit_jpy = ZERO
            position.unknown_cost_quantity = ZERO
            return
        if position.unknown_cost_quantity < ZERO:
            position.unknown_cost_quantity = ZERO
        if known_quantity <= ZERO or position.cost_basis_total_jpy <= ZERO:
            position.cost_basis_total_jpy = ZERO
            position.avg_cost_per_unit_jpy = ZERO
            return
        position.avg_cost_per_unit_jpy = position.cost_basis_total_jpy / known_quantity

    for tx in ordered:
        flow = resolve_transaction_flow(tx, rate_lookup)
        timestamp = tx.timestamp_jst
        year_matches = timestamp is not None and timestamp.year == year

        if flow.acquire_asset and flow.acquire_quantity:
            position = positions.setdefault(
                flow.acquire_asset,
                RunningPosition(
                    asset=flow.acquire_asset,
                    quantity=ZERO,
                    cost_basis_total_jpy=ZERO,
                    avg_cost_per_unit_jpy=ZERO,
                    method=CalculationMethod.MOVING_AVERAGE,
                    last_updated_at=None,
                    unknown_cost_quantity=ZERO,
                ),
            )
            position.quantity += flow.acquire_quantity
            if flow.acquire_value_jpy is not None:
                position.cost_basis_total_jpy += flow.acquire_value_jpy
            else:
                position.unknown_cost_quantity += flow.acquire_quantity
            _recompute_position(position)
            position.last_updated_at = timestamp
            inventory_timeline.append(
                {
                    "timestamp": timestamp,
                    "asset": flow.acquire_asset,
                    "event": "acquire",
                    "quantity": flow.acquire_quantity,
                    "avg_cost_per_unit_jpy": position.avg_cost_per_unit_jpy,
                    "cost_basis_total_jpy": position.cost_basis_total_jpy,
                    "unknown_cost_quantity": position.unknown_cost_quantity,
                    "tx_id": tx.id,
                }
            )
            if year_matches:
                if tx.tx_type is TransactionType.TRANSFER_IN:
                    asset_stats[flow.acquire_asset]["transfer_in_quantity"] += flow.acquire_quantity
                else:
                    asset_stats[flow.acquire_asset]["acquired_quantity"] += flow.acquire_quantity
                    asset_stats[flow.acquire_asset]["acquired_cost_jpy"] += flow.acquire_value_jpy or ZERO
                if flow.income_jpy:
                    asset_stats[flow.acquire_asset]["reward_income_jpy"] += flow.income_jpy

        if tx.tx_type is TransactionType.TRANSFER_OUT and tx.base_asset and tx.quantity:
            position = positions.setdefault(
                tx.base_asset,
                RunningPosition(
                    asset=tx.base_asset,
                    quantity=ZERO,
                    cost_basis_total_jpy=ZERO,
                    avg_cost_per_unit_jpy=ZERO,
                    method=CalculationMethod.MOVING_AVERAGE,
                    last_updated_at=None,
                    unknown_cost_quantity=ZERO,
                ),
            )
            transfer_quantity = tx.quantity
            if tx.fee_asset and tx.fee_asset == tx.base_asset and tx.fee_amount:
                transfer_quantity += tx.fee_amount

            if position.quantity < transfer_quantity:
                flow.review_reasons = list(flow.review_reasons or [])
                if "保有数量不足" not in flow.review_reasons:
                    flow.review_reasons.append("保有数量不足")

            unknown_consumed = min(position.unknown_cost_quantity, transfer_quantity)
            position.quantity -= unknown_consumed
            position.unknown_cost_quantity -= unknown_consumed
            remaining = transfer_quantity - unknown_consumed
            known_quantity = position.quantity - position.unknown_cost_quantity
            if remaining > ZERO and known_quantity > ZERO:
                known_consumed = min(known_quantity, remaining)
                position.quantity -= known_consumed
                position.cost_basis_total_jpy -= position.avg_cost_per_unit_jpy * known_consumed
            _recompute_position(position)
            position.last_updated_at = timestamp
            inventory_timeline.append(
                {
                    "timestamp": timestamp,
                    "asset": tx.base_asset,
                    "event": "transfer_out",
                    "quantity": transfer_quantity,
                    "avg_cost_per_unit_jpy": position.avg_cost_per_unit_jpy,
                    "cost_basis_total_jpy": position.cost_basis_total_jpy,
                    "unknown_cost_quantity": position.unknown_cost_quantity,
                    "tx_id": tx.id,
                }
            )
            if year_matches:
                asset_stats[tx.base_asset]["transfer_out_quantity"] += transfer_quantity

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
                    unknown_cost_quantity=ZERO,
                ),
            )
            avg_cost = position.avg_cost_per_unit_jpy
            cost_basis = None
            if position.quantity < flow.dispose_quantity:
                flow.review_reasons = list(flow.review_reasons or [])
                if "保有数量不足" not in flow.review_reasons:
                    flow.review_reasons.append("保有数量不足")
            if flow.proceeds_jpy is not None:
                known_quantity = position.quantity - position.unknown_cost_quantity
                if known_quantity >= flow.dispose_quantity:
                    cost_basis = avg_cost * flow.dispose_quantity
                    position.quantity -= flow.dispose_quantity
                    position.cost_basis_total_jpy -= cost_basis
                else:
                    known_consumed = min(known_quantity, flow.dispose_quantity)
                    unknown_consumed = min(
                        position.unknown_cost_quantity,
                        flow.dispose_quantity - known_consumed,
                    )
                    if unknown_consumed > ZERO:
                        flow.review_reasons = list(flow.review_reasons or [])
                        if "入庫原価未確定" not in flow.review_reasons:
                            flow.review_reasons.append("入庫原価未確定")
                    if known_consumed > ZERO:
                        position.cost_basis_total_jpy -= avg_cost * known_consumed
                    position.quantity -= known_consumed + unknown_consumed
                    position.unknown_cost_quantity -= unknown_consumed
                _recompute_position(position)
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
                    "unknown_cost_quantity": position.unknown_cost_quantity,
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
        position_row = positions.get(asset)
        asset_summaries.append(
            {
                "asset": asset,
                "acquired_quantity": stats["acquired_quantity"],
                "disposed_quantity": stats["disposed_quantity"],
                "ending_quantity": position_row.quantity if position_row else ZERO,
                "realized_pnl_jpy": stats["realized_pnl_jpy"],
                "average_cost_per_unit_jpy": position_row.avg_cost_per_unit_jpy if position_row else ZERO,
                "acquired_cost_jpy": stats["acquired_cost_jpy"],
                "proceeds_jpy": stats["proceeds_jpy"],
                "fee_jpy": stats["fee_jpy"],
                "reward_income_jpy": stats["reward_income_jpy"],
                "transfer_in_quantity": stats["transfer_in_quantity"],
                "transfer_out_quantity": stats["transfer_out_quantity"],
                "unknown_cost_quantity": position_row.unknown_cost_quantity if position_row else ZERO,
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
