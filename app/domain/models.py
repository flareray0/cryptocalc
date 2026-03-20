from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from .enums import (
    CalculationMethod,
    ClassificationStatus,
    ImportSourceKind,
    LedgerEventKind,
    Side,
    TransactionType,
)


@dataclass(slots=True)
class NormalizedTransaction:
    id: str
    source_exchange: str
    source_file: str
    raw_row_number: int
    timestamp_jst: datetime | None
    timestamp_utc: datetime | None
    tx_type: TransactionType
    base_asset: str | None
    quote_asset: str | None
    quantity: Decimal | None
    quote_quantity: Decimal | None
    unit_price_quote: Decimal | None
    price_per_unit_jpy: Decimal | None
    gross_amount_jpy: Decimal | None
    fee_asset: str | None
    fee_amount: Decimal | None
    fee_jpy: Decimal | None
    side: Side
    note: str
    raw_payload: dict[str, Any]
    classification_status: ClassificationStatus
    review_flag: bool
    review_reasons: list[str] = field(default_factory=list)
    source_kind: ImportSourceKind = ImportSourceKind.CSV
    jpy_rate_source: str | None = None
    duplicate_group_key: str | None = None


@dataclass(slots=True)
class LedgerEvent:
    id: str
    tx_id: str
    timestamp_jst: datetime | None
    asset: str
    kind: LedgerEventKind
    quantity: Decimal
    gross_value_jpy: Decimal | None
    fee_jpy: Decimal | None
    review_flag: bool
    review_reason: str | None = None
    source_tx_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RunningPosition:
    asset: str
    quantity: Decimal
    cost_basis_total_jpy: Decimal
    avg_cost_per_unit_jpy: Decimal
    method: CalculationMethod
    last_updated_at: datetime | None


@dataclass(slots=True)
class InventoryLot:
    asset: str
    quantity: Decimal
    cost_basis_total_jpy: Decimal
    avg_cost_per_unit_jpy: Decimal
    method: CalculationMethod
    last_updated_at: datetime | None


@dataclass(slots=True)
class RealizedPnlRecord:
    timestamp: datetime | None
    asset: str
    quantity_disposed: Decimal
    proceeds_jpy: Decimal | None
    cost_basis_jpy: Decimal | None
    fee_jpy: Decimal | None
    realized_pnl_jpy: Decimal | None
    calculation_method: CalculationMethod
    source_tx_ids: list[str]
    review_flag: bool = False
    review_reason: str | None = None


@dataclass(slots=True)
class ImportBatchResult:
    batch_id: str
    source_file: str
    source_kind: ImportSourceKind
    transaction_count: int
    review_required_count: int
    duplicate_count: int
    unknown_column_names: list[str]
    unknown_tx_types: list[str]
    transactions: list[NormalizedTransaction]
    audit_log_path: str | None = None


@dataclass(slots=True)
class CalculationRunResult:
    run_id: str
    year: int
    method: CalculationMethod
    realized_records: list[RealizedPnlRecord]
    positions: list[RunningPosition]
    review_required_transactions: list[NormalizedTransaction]
    yearly_summary: dict[str, Any]
    asset_summaries: list[dict[str, Any]]
    audit_rows: list[dict[str, Any]]
    inventory_timeline: list[dict[str, Any]] = field(default_factory=list)
    source_transaction_count: int = 0


@dataclass(slots=True)
class PortfolioSnapshot:
    timestamp: datetime | None
    total_equity_jpy: Decimal | None
    total_equity_usd: Decimal | None
    cash_jpy: Decimal | None
    cash_usd: Decimal | None
    realized_pnl_jpy: Decimal | None
    realized_pnl_usd: Decimal | None
    unrealized_pnl_jpy: Decimal | None
    unrealized_pnl_usd: Decimal | None
    inventory_revaluation_jpy: Decimal | None
    inventory_revaluation_usd: Decimal | None
    fees_jpy: Decimal | None
    fees_usd: Decimal | None
    spread_cost_jpy: Decimal | None
    spread_cost_usd: Decimal | None
    slippage_jpy: Decimal | None
    slippage_usd: Decimal | None
    funding_jpy: Decimal | None
    funding_usd: Decimal | None
    benchmark_total_equity_jpy: Decimal | None
    benchmark_total_equity_usd: Decimal | None
    edge_vs_benchmark_jpy: Decimal | None
    edge_vs_benchmark_usd: Decimal | None
    trading_edge_jpy: Decimal | None
    trading_edge_usd: Decimal | None
    reward_income_jpy: Decimal | None = None
    reward_income_usd: Decimal | None = None


@dataclass(slots=True)
class AssetQuantitySnapshot:
    timestamp: datetime | None
    symbol: str
    quantity: Decimal
    market_price_jpy: Decimal | None = None
    market_price_usd: Decimal | None = None
    market_value_jpy: Decimal | None = None
    market_value_usd: Decimal | None = None


@dataclass(slots=True)
class BenchmarkSnapshot:
    timestamp: datetime | None
    total_equity_jpy: Decimal | None
    total_equity_usd: Decimal | None
    edge_vs_benchmark_jpy: Decimal | None
    edge_vs_benchmark_usd: Decimal | None


@dataclass(slots=True)
class PnlAttributionSnapshot:
    period_label: str
    period_start: datetime | None
    period_end: datetime | None
    delta_v_jpy: Decimal | None
    delta_v_usd: Decimal | None
    realized_pnl_jpy: Decimal | None
    realized_pnl_usd: Decimal | None
    unrealized_pnl_jpy: Decimal | None
    unrealized_pnl_usd: Decimal | None
    inventory_revaluation_jpy: Decimal | None
    inventory_revaluation_usd: Decimal | None
    fees_jpy: Decimal | None
    fees_usd: Decimal | None
    slippage_jpy: Decimal | None
    slippage_usd: Decimal | None
    funding_jpy: Decimal | None
    funding_usd: Decimal | None
    edge_vs_benchmark_jpy: Decimal | None
    edge_vs_benchmark_usd: Decimal | None


@dataclass(slots=True)
class AnalysisRunResult:
    run_id: str
    year: int
    method_reference: CalculationMethod
    portfolio_snapshots: list[PortfolioSnapshot]
    asset_quantity_history: list[AssetQuantitySnapshot]
    benchmark_snapshots: list[BenchmarkSnapshot]
    pnl_attribution_snapshots: list[PnlAttributionSnapshot]
    asset_summary_table: list[dict[str, Any]]
    edge_report: dict[str, Any]
    review_notes: list[str] = field(default_factory=list)
