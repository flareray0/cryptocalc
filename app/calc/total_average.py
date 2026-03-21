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


def calculate_total_average(transactions: list, year: int, rate_lookup) -> CalculationRunResult:
    ordered = sort_transactions(transactions)
    asset_years: dict[str, dict[int, dict[str, Decimal]]] = defaultdict(
        lambda: defaultdict(
            lambda: {
                "opening_qty": ZERO,
                "opening_cost": ZERO,
                "opening_unknown_qty": ZERO,
                "acquired_qty": ZERO,
                "acquired_cost": ZERO,
                "acquired_unknown_qty": ZERO,
                "transfer_in_qty": ZERO,
                "transfer_in_known_qty": ZERO,
                "transfer_in_cost": ZERO,
                "transfer_in_unknown_qty": ZERO,
                "disposed_qty": ZERO,
                "transfer_out_qty": ZERO,
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

        if flow.acquire_asset and flow.acquire_quantity:
            asset_state = asset_years[flow.acquire_asset][timestamp.year]
            if tx.tx_type is TransactionType.TRANSFER_IN:
                asset_state["transfer_in_qty"] += flow.acquire_quantity
                if flow.acquire_value_jpy is not None:
                    asset_state["transfer_in_known_qty"] += flow.acquire_quantity
                    asset_state["transfer_in_cost"] += flow.acquire_value_jpy
                else:
                    asset_state["transfer_in_unknown_qty"] += flow.acquire_quantity
            else:
                if flow.acquire_value_jpy is not None:
                    asset_state["acquired_qty"] += flow.acquire_quantity
                    asset_state["acquired_cost"] += flow.acquire_value_jpy
                else:
                    asset_state["acquired_unknown_qty"] += flow.acquire_quantity
            if flow.income_jpy:
                asset_state["reward_income"] += flow.income_jpy

        if flow.dispose_asset and flow.dispose_quantity:
            asset_state = asset_years[flow.dispose_asset][timestamp.year]
            asset_state["disposed_qty"] += flow.dispose_quantity
            asset_state["proceeds"] += flow.proceeds_jpy or ZERO
            asset_state["fee"] += flow.fee_jpy or ZERO

        if tx.tx_type is TransactionType.TRANSFER_OUT and tx.base_asset and tx.quantity:
            transfer_quantity = tx.quantity
            if tx.fee_asset and tx.fee_asset == tx.base_asset and tx.fee_amount:
                transfer_quantity += tx.fee_amount
            asset_state = asset_years[tx.base_asset][timestamp.year]
            asset_state["transfer_out_qty"] += transfer_quantity

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
        opening_known_qty = ZERO
        opening_cost = ZERO
        opening_unknown_qty = ZERO
        for calc_year in sorted(per_year.keys()):
            state = per_year[calc_year]
            state["opening_qty"] = opening_known_qty + opening_unknown_qty
            state["opening_cost"] = opening_cost
            state["opening_unknown_qty"] = opening_unknown_qty
            known_total_qty = opening_known_qty + state["acquired_qty"] + state["transfer_in_known_qty"]
            known_total_cost = opening_cost + state["acquired_cost"] + state["transfer_in_cost"]
            unknown_total_qty = opening_unknown_qty + state["acquired_unknown_qty"] + state["transfer_in_unknown_qty"]
            avg_cost = ZERO if known_total_qty == ZERO else known_total_cost / known_total_qty

            cost_basis = None
            realized_pnl = None
            known_after_tax = known_total_qty
            unknown_after_tax = unknown_total_qty
            ending_cost = known_total_cost
            if state["disposed_qty"] > ZERO or state["proceeds"] > ZERO:
                if known_total_qty >= state["disposed_qty"]:
                    cost_basis = avg_cost * state["disposed_qty"]
                    known_after_tax = known_total_qty - state["disposed_qty"]
                    ending_cost = known_total_cost - cost_basis
                    realized_pnl = state["proceeds"] - cost_basis - state["fee"]
                else:
                    known_consumed = min(known_total_qty, state["disposed_qty"])
                    unknown_consumed = min(
                        unknown_total_qty,
                        state["disposed_qty"] - known_consumed,
                    )
                    if known_consumed > ZERO:
                        ending_cost = known_total_cost - (avg_cost * known_consumed)
                    else:
                        ending_cost = known_total_cost
                    known_after_tax = known_total_qty - known_consumed
                    unknown_after_tax = unknown_total_qty - unknown_consumed

            if state["transfer_out_qty"] > ZERO:
                unknown_removed = min(unknown_after_tax, state["transfer_out_qty"])
                unknown_after_tax -= unknown_removed
                remaining_transfer = state["transfer_out_qty"] - unknown_removed
                if remaining_transfer > ZERO and known_after_tax > ZERO:
                    known_removed = min(known_after_tax, remaining_transfer)
                    known_after_tax -= known_removed
                    ending_cost -= avg_cost * known_removed

            ending_qty = known_after_tax + unknown_after_tax
            if known_after_tax <= ZERO or ending_cost <= ZERO:
                ending_cost = ZERO

            inventory_timeline.append(
                {
                    "timestamp": f"{calc_year}-12-31T23:59:59+09:00",
                    "asset": asset,
                    "event": "year_close",
                    "avg_cost_per_unit_jpy": avg_cost,
                    "cost_basis_total_jpy": ending_cost,
                    "unknown_cost_quantity": unknown_after_tax,
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
                            realized_pnl_jpy=realized_pnl,
                            calculation_method=CalculationMethod.TOTAL_AVERAGE,
                            source_tx_ids=[],
                            review_flag=cost_basis is None,
                            review_reason="入庫原価未確定" if cost_basis is None else None,
                        )
                    )
                asset_summaries.append(
                    {
                        "asset": asset,
                        "acquired_quantity": state["acquired_qty"],
                        "disposed_quantity": state["disposed_qty"],
                        "ending_quantity": ending_qty,
                        "realized_pnl_jpy": realized_pnl,
                        "average_cost_per_unit_jpy": avg_cost,
                        "acquired_cost_jpy": state["acquired_cost"],
                        "proceeds_jpy": state["proceeds"],
                        "fee_jpy": state["fee"],
                        "reward_income_jpy": state["reward_income"],
                        "opening_quantity": state["opening_qty"],
                        "opening_cost_jpy": state["opening_cost"],
                        "opening_unknown_cost_quantity": state["opening_unknown_qty"],
                        "transfer_in_quantity": state["transfer_in_qty"],
                        "transfer_out_quantity": state["transfer_out_qty"],
                        "unknown_cost_quantity": unknown_after_tax,
                    }
                )
                running_positions[asset] = RunningPosition(
                    asset=asset,
                    quantity=ending_qty,
                    cost_basis_total_jpy=ending_cost,
                    avg_cost_per_unit_jpy=(ZERO if known_after_tax == ZERO else avg_cost),
                    method=CalculationMethod.TOTAL_AVERAGE,
                    last_updated_at=None,
                    unknown_cost_quantity=unknown_after_tax,
                )

            opening_known_qty = known_after_tax
            opening_cost = ending_cost
            opening_unknown_qty = unknown_after_tax

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
