from __future__ import annotations

from dataclasses import asdict
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

from app.calc.inventory_engine import resolve_transaction_flow
from app.calc.normalizer import sort_transactions
from app.domain.enums import CalculationMethod, Side, TransactionType
from app.domain.models import (
    AnalysisRunResult,
    AssetQuantitySnapshot,
    BenchmarkSnapshot,
    PnlAttributionSnapshot,
    PortfolioSnapshot,
)
from app.domain.validators import JST, quantize_jpy, serialize_payload


ZERO = Decimal("0")
FIAT_ASSETS = {"JPY", "USD"}
USD_LIKE_ASSETS = ("USD", "USDT", "USDC", "FDUSD", "BUSD")


class PriceHistory:
    def __init__(self) -> None:
        self._points: dict[str, list[tuple[datetime | None, Decimal, str]]] = defaultdict(list)

    def add(self, asset: str | None, timestamp: datetime | None, price_jpy: Decimal | None, source: str) -> None:
        if not asset or price_jpy is None or price_jpy <= ZERO:
            return
        self._points[asset.upper()].append((timestamp, price_jpy, source))

    def finalize(self) -> None:
        for asset in self._points:
            self._points[asset].sort(
                key=lambda row: (
                    row[0] or datetime.min.replace(tzinfo=JST),
                    self._source_rank(row[2]),
                )
            )

    @staticmethod
    def _source_rank(source: str) -> int:
        if source.startswith("manual:"):
            return 3
        if source.startswith("tx:quote_jpy") or source.startswith("file:"):
            return 2
        return 1

    def lookup_jpy(self, asset: str | None, timestamp: datetime | None) -> tuple[Decimal | None, str | None]:
        if not asset:
            return None, None
        code = asset.upper()
        if code == "JPY":
            return Decimal("1"), "fiat:jpy"

        if code in USD_LIKE_ASSETS and code != "USD":
            points = self._points.get(code, [])
            value = self._lookup_points(points, timestamp)
            if value[0] is not None:
                return value
            usd_jpy, source = self._lookup_points(self._points.get("USD", []), timestamp)
            if usd_jpy is not None:
                return usd_jpy, f"{source}:stable_proxy"

        points = self._points.get(code, [])
        return self._lookup_points(points, timestamp)

    def _lookup_points(
        self, points: list[tuple[datetime | None, Decimal, str]], timestamp: datetime | None
    ) -> tuple[Decimal | None, str | None]:
        if not points:
            return None, None
        if timestamp is None:
            _, value, source = points[-1]
            return value, source

        latest_prior = None
        earliest_future = None
        for point_ts, value, source in points:
            if point_ts is None:
                continue
            if point_ts <= timestamp:
                latest_prior = (value, source)
            elif earliest_future is None:
                earliest_future = (value, f"{source}:future_fallback")
                break
        if latest_prior:
            return latest_prior
        if earliest_future:
            return earliest_future
        _, value, source = points[-1]
        return value, f"{source}:latest_fallback"

    def lookup_usd_jpy(self, timestamp: datetime | None) -> tuple[Decimal | None, str | None]:
        for code in USD_LIKE_ASSETS:
            value, source = self._lookup_points(self._points.get(code, []), timestamp)
            if value is not None:
                return value, source
        return None, None

    def lookup(self, asset: str | None, timestamp: datetime | None) -> tuple[Decimal | None, str | None]:
        return self.lookup_jpy(asset, timestamp)


def _build_price_history(transactions: list, manual_rate_table) -> PriceHistory:
    history = PriceHistory()
    for row in getattr(manual_rate_table, "rows", []):
        history.add(row.asset, row.timestamp, row.jpy_rate, f"manual:{row.source}")

    ordered = sort_transactions(transactions)
    for tx in ordered:
        timestamp = tx.timestamp_jst
        if tx.price_per_unit_jpy is not None and tx.base_asset:
            history.add(tx.base_asset, timestamp, tx.price_per_unit_jpy, "tx:quote_jpy")

    history.finalize()
    for tx in ordered:
        flow = resolve_transaction_flow(tx, history)
        timestamp = tx.timestamp_jst
        if flow.acquire_asset and flow.acquire_quantity and flow.acquire_value_jpy:
            history.add(
                flow.acquire_asset,
                timestamp,
                flow.acquire_value_jpy / flow.acquire_quantity,
                "tx:derived_acquire",
            )
        if flow.dispose_asset and flow.dispose_quantity and flow.proceeds_jpy:
            history.add(
                flow.dispose_asset,
                timestamp,
                flow.proceeds_jpy / flow.dispose_quantity,
                "tx:derived_dispose",
            )
        history.finalize()
    return history


def _balance_add(balances: dict[str, Decimal], asset: str | None, delta: Decimal | None) -> None:
    if not asset or delta is None or delta == ZERO:
        return
    balances[asset.upper()] += delta


def _ensure_cost_position(cost_positions: dict[str, dict[str, Decimal]], asset: str | None) -> None:
    if not asset or asset.upper() in FIAT_ASSETS:
        return
    cost_positions.setdefault(asset.upper(), {"quantity": ZERO, "cost_total_jpy": ZERO})


def _acquire_cost(
    cost_positions: dict[str, dict[str, Decimal]],
    asset: str | None,
    quantity: Decimal | None,
    cost_jpy: Decimal | None,
) -> None:
    if not asset or asset.upper() in FIAT_ASSETS or quantity is None or cost_jpy is None:
        return
    _ensure_cost_position(cost_positions, asset)
    position = cost_positions[asset.upper()]
    position["quantity"] += quantity
    position["cost_total_jpy"] += cost_jpy


def _consume_cost(
    cost_positions: dict[str, dict[str, Decimal]],
    asset: str | None,
    quantity: Decimal | None,
) -> Decimal | None:
    if not asset or asset.upper() in FIAT_ASSETS or quantity is None or quantity == ZERO:
        return None
    _ensure_cost_position(cost_positions, asset)
    position = cost_positions[asset.upper()]
    if position["quantity"] <= ZERO:
        return ZERO
    avg_cost = position["cost_total_jpy"] / position["quantity"]
    consumed = avg_cost * quantity
    position["quantity"] -= quantity
    position["cost_total_jpy"] -= consumed
    if position["quantity"] <= ZERO:
        position["quantity"] = ZERO
        position["cost_total_jpy"] = ZERO
    return consumed


def _apply_crypto_fee_inventory(
    cost_positions: dict[str, dict[str, Decimal]],
    fee_asset: str | None,
    fee_amount: Decimal | None,
) -> Decimal | None:
    if not fee_asset or fee_amount is None or fee_amount == ZERO:
        return None
    if fee_asset.upper() in FIAT_ASSETS:
        return None
    return _consume_cost(cost_positions, fee_asset, fee_amount)


def _market_price_jpy(
    history: PriceHistory,
    asset: str | None,
    timestamp: datetime | None,
) -> tuple[Decimal | None, str | None]:
    if not asset:
        return None, None
    code = asset.upper()
    if code == "JPY":
        return Decimal("1"), "fiat:jpy"
    if code == "USD":
        return history.lookup_usd_jpy(timestamp)
    value, source = history.lookup_jpy(code, timestamp)
    if value is not None:
        return value, source
    if code in USD_LIKE_ASSETS:
        usd_jpy, usd_source = history.lookup_usd_jpy(timestamp)
        if usd_jpy is not None:
            return usd_jpy, f"{usd_source}:usd_like"
    return None, None


class AnalysisState:
    def __init__(self) -> None:
        self.balances: dict[str, Decimal] = defaultdict(lambda: ZERO)
        self.cost_positions: dict[str, dict[str, Decimal]] = {}
        self.realized_pnl_jpy = ZERO
        self.fees_jpy = ZERO
        self.spread_cost_jpy = ZERO
        self.slippage_jpy = ZERO
        self.funding_jpy = ZERO
        self.reward_income_jpy = ZERO
        self.realized_by_asset: dict[str, Decimal] = defaultdict(lambda: ZERO)
        self.fees_by_asset: dict[str, Decimal] = defaultdict(lambda: ZERO)


def _is_unpriced_review_swap(tx) -> bool:
    if tx.tx_type is not TransactionType.CRYPTO_SWAP or not tx.review_flag:
        return False
    reasons = " ".join(tx.review_reasons or [])
    return "JPY換算" in reasons or "未確定" in reasons


def _analysis_fee_jpy(tx, flow) -> Decimal:
    if _is_unpriced_review_swap(tx):
        return tx.fee_jpy or ZERO
    return flow.fee_jpy or ZERO


def _trade_value_jpy(tx, history: PriceHistory) -> Decimal | None:
    timestamp = tx.timestamp_jst
    if tx.tx_type is TransactionType.CRYPTO_SWAP:
        if tx.side is Side.BUY:
            if tx.quote_asset and tx.quote_quantity:
                price, _ = _market_price_jpy(history, tx.quote_asset, timestamp)
                if price is not None:
                    return price * tx.quote_quantity
            if tx.base_asset and tx.quantity:
                price, _ = _market_price_jpy(history, tx.base_asset, timestamp)
                if price is not None:
                    return price * tx.quantity
        if tx.side is Side.SELL:
            if tx.base_asset and tx.quantity:
                price, _ = _market_price_jpy(history, tx.base_asset, timestamp)
                if price is not None:
                    return price * tx.quantity
            if tx.quote_asset and tx.quote_quantity:
                price, _ = _market_price_jpy(history, tx.quote_asset, timestamp)
                if price is not None:
                    return price * tx.quote_quantity
    return tx.gross_amount_jpy


def _apply_transaction(state: AnalysisState, tx, history: PriceHistory) -> None:
    flow = resolve_transaction_flow(tx, history)
    qty = tx.quantity or ZERO
    fee_amount = tx.fee_amount or ZERO
    fee_asset = (tx.fee_asset or "").upper() or None
    fee_jpy = _analysis_fee_jpy(tx, flow)
    base_asset = (tx.base_asset or "").upper() or None

    if tx.tx_type is TransactionType.BUY:
        acquired_qty = qty - fee_amount if fee_asset == base_asset else qty
        _balance_add(state.balances, base_asset, acquired_qty)
        _balance_add(state.balances, "JPY", -(tx.gross_amount_jpy or ZERO))
        if fee_asset == "JPY":
            _balance_add(state.balances, "JPY", -fee_amount)
        elif fee_asset and fee_asset != base_asset:
            _balance_add(state.balances, fee_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, fee_asset, fee_amount)
        _acquire_cost(state.cost_positions, base_asset, acquired_qty, tx.gross_amount_jpy or ZERO)
        state.fees_jpy += fee_jpy
        if base_asset:
            state.fees_by_asset[base_asset] += fee_jpy
        return

    if tx.tx_type is TransactionType.SELL:
        _balance_add(state.balances, base_asset, -qty)
        _balance_add(state.balances, "JPY", tx.gross_amount_jpy or ZERO)
        if fee_asset == "JPY":
            _balance_add(state.balances, "JPY", -fee_amount)
        elif fee_asset == base_asset:
            _balance_add(state.balances, base_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, base_asset, fee_amount)
        elif fee_asset:
            _balance_add(state.balances, fee_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, fee_asset, fee_amount)
        cost_basis = _consume_cost(state.cost_positions, base_asset, qty) or ZERO
        realized = (tx.gross_amount_jpy or ZERO) - cost_basis
        state.realized_pnl_jpy += realized
        if base_asset:
            state.realized_by_asset[base_asset] += realized
            state.fees_by_asset[base_asset] += fee_jpy
        state.fees_jpy += fee_jpy
        return

    if tx.tx_type is TransactionType.CRYPTO_SWAP:
        trade_value_jpy = _trade_value_jpy(tx, history) or ZERO
        acquired_asset = (flow.acquire_asset or "").upper() or None
        acquired_qty = flow.acquire_quantity or ZERO
        dispose_asset = (flow.dispose_asset or "").upper() or None
        dispose_qty = tx.quote_quantity if tx.side is Side.BUY else qty
        dispose_qty = dispose_qty or ZERO
        carry_forward_cost_only = _is_unpriced_review_swap(tx)

        _balance_add(state.balances, dispose_asset, -dispose_qty)
        _balance_add(state.balances, acquired_asset, acquired_qty)

        if fee_asset == acquired_asset:
            _balance_add(state.balances, acquired_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, acquired_asset, fee_amount)
        elif fee_asset == dispose_asset:
            _balance_add(state.balances, dispose_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, dispose_asset, fee_amount)
        elif fee_asset == "JPY":
            _balance_add(state.balances, "JPY", -fee_amount)
        elif fee_asset:
            _balance_add(state.balances, fee_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, fee_asset, fee_amount)

        cost_basis = _consume_cost(state.cost_positions, dispose_asset, dispose_qty) or ZERO
        if carry_forward_cost_only:
            _acquire_cost(state.cost_positions, acquired_asset, acquired_qty, cost_basis)
        else:
            realized = trade_value_jpy - cost_basis
            state.realized_pnl_jpy += realized
            if dispose_asset:
                state.realized_by_asset[dispose_asset] += realized
            _acquire_cost(state.cost_positions, acquired_asset, acquired_qty, trade_value_jpy)
        if acquired_asset:
            state.fees_by_asset[acquired_asset] += fee_jpy
        state.fees_jpy += fee_jpy
        return

    if tx.tx_type is TransactionType.REWARD:
        acquired_qty = flow.acquire_quantity or qty
        _balance_add(state.balances, base_asset, acquired_qty)
        reward_value = flow.income_jpy or _trade_value_jpy(tx, history) or ZERO
        _acquire_cost(state.cost_positions, base_asset, acquired_qty, reward_value)
        state.reward_income_jpy += reward_value
        if fee_asset == base_asset and fee_amount:
            _balance_add(state.balances, base_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, base_asset, fee_amount)
        elif fee_asset == "JPY":
            _balance_add(state.balances, "JPY", -fee_amount)
        elif fee_asset:
            _balance_add(state.balances, fee_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, fee_asset, fee_amount)
        state.fees_jpy += fee_jpy
        if base_asset:
            state.fees_by_asset[base_asset] += fee_jpy
        return

    if tx.tx_type is TransactionType.TRANSFER_IN:
        _balance_add(state.balances, base_asset, qty)
        return

    if tx.tx_type is TransactionType.OPENING_BALANCE:
        _balance_add(state.balances, base_asset, qty)
        _acquire_cost(state.cost_positions, base_asset, qty, flow.acquire_value_jpy or ZERO)
        return

    if tx.tx_type is TransactionType.TRANSFER_OUT:
        _balance_add(state.balances, base_asset, -qty)
        if base_asset and base_asset not in FIAT_ASSETS:
            _consume_cost(state.cost_positions, base_asset, qty)
        if fee_asset == base_asset and fee_amount:
            _balance_add(state.balances, base_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, base_asset, fee_amount)
        elif fee_asset == "JPY":
            _balance_add(state.balances, "JPY", -fee_amount)
        elif fee_asset:
            _balance_add(state.balances, fee_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, fee_asset, fee_amount)
        state.fees_jpy += fee_jpy
        return

    if tx.tx_type is TransactionType.ADJUSTMENT:
        if tx.side is Side.BUY:
            _balance_add(state.balances, base_asset, qty)
            _acquire_cost(state.cost_positions, base_asset, qty, tx.gross_amount_jpy or ZERO)
        elif tx.side is Side.SELL:
            _balance_add(state.balances, base_asset, -qty)
            cost_basis = _consume_cost(state.cost_positions, base_asset, qty) or ZERO
            proceeds = tx.gross_amount_jpy or ZERO
            if base_asset:
                state.realized_by_asset[base_asset] += proceeds - cost_basis
            state.realized_pnl_jpy += proceeds - cost_basis
        return


def _apply_benchmark_flow(state: AnalysisState, tx, history: PriceHistory) -> None:
    qty = tx.quantity or ZERO
    fee_amount = tx.fee_amount or ZERO
    base_asset = (tx.base_asset or "").upper() or None
    fee_asset = (tx.fee_asset or "").upper() or None

    if tx.tx_type in {TransactionType.OPENING_BALANCE, TransactionType.TRANSFER_IN}:
        _balance_add(state.balances, base_asset, qty)
        if tx.tx_type is TransactionType.OPENING_BALANCE:
            _acquire_cost(state.cost_positions, base_asset, qty, tx.gross_amount_jpy or ZERO)
        return

    if tx.tx_type is TransactionType.TRANSFER_OUT:
        _balance_add(state.balances, base_asset, -qty)
        if base_asset and base_asset not in FIAT_ASSETS:
            _consume_cost(state.cost_positions, base_asset, qty)
        if fee_asset == "JPY":
            _balance_add(state.balances, "JPY", -fee_amount)
        elif fee_asset:
            _balance_add(state.balances, fee_asset, -fee_amount)
            _apply_crypto_fee_inventory(state.cost_positions, fee_asset, fee_amount)
        return

    if tx.tx_type is TransactionType.ADJUSTMENT:
        _apply_transaction(state, tx, history)


def _total_value_in_jpy(
    balances: dict[str, Decimal],
    history: PriceHistory,
    timestamp: datetime | None,
    review_notes: set[str],
) -> tuple[Decimal, Decimal]:
    total = ZERO
    cash = ZERO
    for asset, quantity in balances.items():
        if quantity == ZERO:
            continue
        price_jpy, source = _market_price_jpy(history, asset, timestamp)
        if price_jpy is None:
            review_notes.add(f"{asset} のJPYレートが見つからないため時価評価に反映できません")
            continue
        value = quantity * price_jpy
        total += value
        if asset in FIAT_ASSETS:
            cash += value
        elif asset in USD_LIKE_ASSETS and source and "stable_proxy" in source:
            cash += value
    return total, cash


def _has_meaningful_starting_portfolio(state: AnalysisState) -> bool:
    return any(quantity != ZERO for quantity in state.balances.values())


def _has_explicit_opening_balance(
    transactions: list,
    period_start: datetime,
    period_end: datetime,
) -> bool:
    for tx in transactions:
        timestamp = tx.timestamp_jst or tx.timestamp_utc
        if timestamp is None:
            continue
        if period_start <= timestamp <= period_end and tx.tx_type is TransactionType.OPENING_BALANCE:
            return True
    return False


def _unrealized_pnl_jpy(
    balances: dict[str, Decimal],
    cost_positions: dict[str, dict[str, Decimal]],
    history: PriceHistory,
    timestamp: datetime | None,
    review_notes: set[str],
) -> Decimal:
    total = ZERO
    for asset, position in cost_positions.items():
        quantity = balances.get(asset, ZERO)
        if quantity <= ZERO:
            continue
        market_price_jpy, _ = _market_price_jpy(history, asset, timestamp)
        if market_price_jpy is None:
            review_notes.add(f"{asset} の時価が不明のため含み損益を確定できません")
            continue
        market_value = quantity * market_price_jpy
        total += market_value - position.get("cost_total_jpy", ZERO)
    return total


def _to_usd(value_jpy: Decimal | None, history: PriceHistory, timestamp: datetime | None, review_notes: set[str]):
    if value_jpy is None:
        return None
    usd_jpy, _ = history.lookup_usd_jpy(timestamp)
    if usd_jpy in (None, ZERO):
        review_notes.add("USD換算レートが無いためUSD建て分析は一部未評価です")
        return None
    return value_jpy / usd_jpy


def _make_snapshot(
    *,
    timestamp: datetime | None,
    actual_state: AnalysisState,
    benchmark_state: AnalysisState,
    benchmark_enabled: bool,
    history: PriceHistory,
    review_notes: set[str],
) -> PortfolioSnapshot:
    total_equity_jpy, cash_jpy = _total_value_in_jpy(actual_state.balances, history, timestamp, review_notes)
    benchmark_equity_jpy = None
    if benchmark_enabled:
        benchmark_equity_jpy, _ = _total_value_in_jpy(benchmark_state.balances, history, timestamp, review_notes)
    unrealized_jpy = _unrealized_pnl_jpy(
        actual_state.balances,
        actual_state.cost_positions,
        history,
        timestamp,
        review_notes,
    )
    total_equity_usd = _to_usd(total_equity_jpy, history, timestamp, review_notes)
    cash_usd = _to_usd(cash_jpy, history, timestamp, review_notes)
    realized_usd = _to_usd(actual_state.realized_pnl_jpy, history, timestamp, review_notes)
    unrealized_usd = _to_usd(unrealized_jpy, history, timestamp, review_notes)
    fees_usd = _to_usd(actual_state.fees_jpy, history, timestamp, review_notes)
    spread_usd = _to_usd(actual_state.spread_cost_jpy, history, timestamp, review_notes)
    slippage_usd = _to_usd(actual_state.slippage_jpy, history, timestamp, review_notes)
    funding_usd = _to_usd(actual_state.funding_jpy, history, timestamp, review_notes)
    benchmark_equity_usd = _to_usd(benchmark_equity_jpy, history, timestamp, review_notes)
    edge_jpy = None if benchmark_equity_jpy is None else total_equity_jpy - benchmark_equity_jpy
    edge_usd = None
    if total_equity_usd is not None and benchmark_equity_usd is not None:
        edge_usd = total_equity_usd - benchmark_equity_usd
    reward_income_usd = _to_usd(actual_state.reward_income_jpy, history, timestamp, review_notes)
    trading_edge_jpy = None if edge_jpy is None else edge_jpy - actual_state.reward_income_jpy
    trading_edge_usd = None
    if edge_usd is not None and reward_income_usd is not None:
        trading_edge_usd = edge_usd - reward_income_usd
    return PortfolioSnapshot(
        timestamp=timestamp,
        total_equity_jpy=quantize_jpy(total_equity_jpy),
        total_equity_usd=quantize_jpy(total_equity_usd),
        cash_jpy=quantize_jpy(cash_jpy),
        cash_usd=quantize_jpy(cash_usd),
        realized_pnl_jpy=quantize_jpy(actual_state.realized_pnl_jpy),
        realized_pnl_usd=quantize_jpy(realized_usd),
        unrealized_pnl_jpy=quantize_jpy(unrealized_jpy),
        unrealized_pnl_usd=quantize_jpy(unrealized_usd),
        inventory_revaluation_jpy=quantize_jpy(unrealized_jpy),
        inventory_revaluation_usd=quantize_jpy(unrealized_usd),
        fees_jpy=quantize_jpy(actual_state.fees_jpy),
        fees_usd=quantize_jpy(fees_usd),
        spread_cost_jpy=quantize_jpy(actual_state.spread_cost_jpy),
        spread_cost_usd=quantize_jpy(spread_usd),
        slippage_jpy=quantize_jpy(actual_state.slippage_jpy),
        slippage_usd=quantize_jpy(slippage_usd),
        funding_jpy=quantize_jpy(actual_state.funding_jpy),
        funding_usd=quantize_jpy(funding_usd),
        benchmark_total_equity_jpy=quantize_jpy(benchmark_equity_jpy),
        benchmark_total_equity_usd=quantize_jpy(benchmark_equity_usd),
        edge_vs_benchmark_jpy=quantize_jpy(edge_jpy),
        edge_vs_benchmark_usd=quantize_jpy(edge_usd),
        trading_edge_jpy=quantize_jpy(trading_edge_jpy),
        trading_edge_usd=quantize_jpy(trading_edge_usd),
        reward_income_jpy=quantize_jpy(actual_state.reward_income_jpy),
        reward_income_usd=quantize_jpy(reward_income_usd),
    )


def _make_asset_quantity_snapshots(
    timestamp: datetime | None,
    balances: dict[str, Decimal],
    history: PriceHistory,
    review_notes: set[str],
) -> list[AssetQuantitySnapshot]:
    rows: list[AssetQuantitySnapshot] = []
    for asset in sorted(balances):
        quantity = balances[asset]
        if quantity == ZERO or asset in FIAT_ASSETS:
            continue
        price_jpy, _ = _market_price_jpy(history, asset, timestamp)
        value_jpy = quantity * price_jpy if price_jpy is not None else None
        value_usd = _to_usd(value_jpy, history, timestamp, review_notes) if value_jpy is not None else None
        price_usd = None
        if value_usd is not None and quantity != ZERO:
            price_usd = value_usd / quantity
        rows.append(
            AssetQuantitySnapshot(
                timestamp=timestamp,
                symbol=asset,
                quantity=quantity,
                market_price_jpy=quantize_jpy(price_jpy),
                market_price_usd=quantize_jpy(price_usd),
                market_value_jpy=quantize_jpy(value_jpy),
                market_value_usd=quantize_jpy(value_usd),
            )
        )
    return rows


def _build_asset_summary_table(
    asset_quantity_history: list[AssetQuantitySnapshot],
    actual_state: AnalysisState,
    history: PriceHistory,
    timestamp: datetime | None,
    review_notes: set[str],
) -> list[dict]:
    first_qty: dict[str, Decimal] = {}
    first_timestamp = min((row.timestamp for row in asset_quantity_history if row.timestamp is not None), default=None)
    for row in asset_quantity_history:
        if first_timestamp is not None and row.timestamp == first_timestamp:
            first_qty[row.symbol] = row.quantity

    table: list[dict] = []
    assets = {
        asset
        for asset, quantity in actual_state.balances.items()
        if asset not in FIAT_ASSETS and quantity != ZERO
    }
    assets.update(asset for asset, value in actual_state.realized_by_asset.items() if value != ZERO)
    assets.update(asset for asset, value in actual_state.fees_by_asset.items() if value != ZERO)

    for asset in sorted(assets):
        current_quantity = actual_state.balances.get(asset, ZERO)
        price_jpy, _ = _market_price_jpy(history, asset, timestamp)
        market_value_jpy = current_quantity * price_jpy if price_jpy is not None else None
        market_value_usd = _to_usd(market_value_jpy, history, timestamp, review_notes) if market_value_jpy is not None else None
        mark_price_usd = None
        if market_value_usd is not None and current_quantity != ZERO:
            mark_price_usd = market_value_usd / current_quantity
        position = actual_state.cost_positions.get(asset, {"quantity": ZERO, "cost_total_jpy": ZERO})
        avg_cost = None
        if position["quantity"] > ZERO:
            avg_cost = position["cost_total_jpy"] / position["quantity"]
        unrealized = ZERO
        if market_value_jpy is not None:
            unrealized = market_value_jpy - position["cost_total_jpy"]
        table.append(
            {
                "symbol": asset,
                "current_quantity": current_quantity,
                "period_change_quantity": current_quantity - first_qty.get(asset, ZERO),
                "avg_cost_jpy": quantize_jpy(avg_cost),
                "mark_price_jpy": quantize_jpy(price_jpy),
                "mark_price_usd": quantize_jpy(mark_price_usd),
                "market_value_jpy": quantize_jpy(market_value_jpy),
                "market_value_usd": quantize_jpy(market_value_usd),
                "realized_pnl_jpy": quantize_jpy(actual_state.realized_by_asset.get(asset, ZERO)),
                "realized_pnl_usd": quantize_jpy(
                    _to_usd(actual_state.realized_by_asset.get(asset, ZERO), history, timestamp, review_notes)
                ),
                "unrealized_pnl_jpy": quantize_jpy(unrealized),
                "unrealized_pnl_usd": quantize_jpy(_to_usd(unrealized, history, timestamp, review_notes)),
                "fees_allocated_jpy": quantize_jpy(actual_state.fees_by_asset.get(asset, ZERO)),
                "fees_allocated_usd": quantize_jpy(
                    _to_usd(actual_state.fees_by_asset.get(asset, ZERO), history, timestamp, review_notes)
                ),
            }
        )
    return serialize_payload(table)


def _build_attribution_rows(
    portfolio_snapshots: list[PortfolioSnapshot],
    history: PriceHistory,
    review_notes: set[str],
) -> list[PnlAttributionSnapshot]:
    if len(portfolio_snapshots) < 2:
        return []

    monthly_last: dict[str, PortfolioSnapshot] = {}
    for snapshot in portfolio_snapshots:
        if snapshot.timestamp is None:
            continue
        monthly_last[snapshot.timestamp.strftime("%Y-%m")] = snapshot

    ordered = [monthly_last[key] for key in sorted(monthly_last)]
    rows: list[PnlAttributionSnapshot] = []
    previous = portfolio_snapshots[0]
    for current in ordered:
        delta_v_jpy = (current.total_equity_jpy or ZERO) - (previous.total_equity_jpy or ZERO)
        realized_delta_jpy = (current.realized_pnl_jpy or ZERO) - (previous.realized_pnl_jpy or ZERO)
        fees_delta_jpy = (current.fees_jpy or ZERO) - (previous.fees_jpy or ZERO)
        funding_delta_jpy = (current.funding_jpy or ZERO) - (previous.funding_jpy or ZERO)
        slippage_delta_jpy = (current.slippage_jpy or ZERO) - (previous.slippage_jpy or ZERO)
        inventory_delta_jpy = (current.inventory_revaluation_jpy or ZERO) - (
            previous.inventory_revaluation_jpy or ZERO
        )
        edge_delta_jpy = None
        if current.edge_vs_benchmark_jpy is not None and previous.edge_vs_benchmark_jpy is not None:
            edge_delta_jpy = (current.edge_vs_benchmark_jpy or ZERO) - (previous.edge_vs_benchmark_jpy or ZERO)
        rows.append(
            PnlAttributionSnapshot(
                period_label=current.timestamp.strftime("%Y-%m"),
                period_start=previous.timestamp,
                period_end=current.timestamp,
                delta_v_jpy=quantize_jpy(delta_v_jpy),
                delta_v_usd=quantize_jpy(_to_usd(delta_v_jpy, history, current.timestamp, review_notes)),
                realized_pnl_jpy=quantize_jpy(realized_delta_jpy),
                realized_pnl_usd=quantize_jpy(_to_usd(realized_delta_jpy, history, current.timestamp, review_notes)),
                unrealized_pnl_jpy=current.unrealized_pnl_jpy,
                unrealized_pnl_usd=current.unrealized_pnl_usd,
                inventory_revaluation_jpy=quantize_jpy(inventory_delta_jpy),
                inventory_revaluation_usd=quantize_jpy(
                    _to_usd(inventory_delta_jpy, history, current.timestamp, review_notes)
                ),
                fees_jpy=quantize_jpy(fees_delta_jpy),
                fees_usd=quantize_jpy(_to_usd(fees_delta_jpy, history, current.timestamp, review_notes)),
                slippage_jpy=quantize_jpy(slippage_delta_jpy),
                slippage_usd=quantize_jpy(_to_usd(slippage_delta_jpy, history, current.timestamp, review_notes)),
                funding_jpy=quantize_jpy(funding_delta_jpy),
                funding_usd=quantize_jpy(_to_usd(funding_delta_jpy, history, current.timestamp, review_notes)),
                edge_vs_benchmark_jpy=quantize_jpy(edge_delta_jpy),
                edge_vs_benchmark_usd=quantize_jpy(
                    _to_usd(edge_delta_jpy, history, current.timestamp, review_notes)
                ),
            )
        )
        previous = current
    return rows


def _execute_portfolio_analysis(
    *,
    transactions: list,
    period_start: datetime,
    period_end: datetime,
    method_reference: CalculationMethod,
    manual_rate_table,
):
    history = _build_price_history(transactions, manual_rate_table)
    ordered = sort_transactions(transactions)
    review_notes: set[str] = set()
    actual_state = AnalysisState()

    for tx in ordered:
        if tx.timestamp_jst and tx.timestamp_jst >= period_start:
            break
        _apply_transaction(actual_state, tx, history)

    benchmark_state = AnalysisState()
    benchmark_state.balances = defaultdict(lambda: ZERO, deepcopy(actual_state.balances))
    benchmark_state.cost_positions = deepcopy(actual_state.cost_positions)
    benchmark_enabled = _has_meaningful_starting_portfolio(actual_state) or _has_explicit_opening_balance(
        ordered,
        period_start,
        period_end,
    )
    if not benchmark_enabled:
        review_notes.add(
            "期首の持ち越し資産・現金が確認できないため、benchmark 比較と trading edge は未評価です"
        )

    portfolio_snapshots: list[PortfolioSnapshot] = [
        _make_snapshot(
            timestamp=period_start,
            actual_state=actual_state,
            benchmark_state=benchmark_state,
            benchmark_enabled=benchmark_enabled,
            history=history,
            review_notes=review_notes,
        )
    ]
    asset_quantity_history: list[AssetQuantitySnapshot] = _make_asset_quantity_snapshots(
        period_start,
        actual_state.balances,
        history,
        review_notes,
    )

    for tx in ordered:
        if tx.timestamp_jst is None or tx.timestamp_jst < period_start or tx.timestamp_jst > period_end:
            continue
        _apply_transaction(actual_state, tx, history)
        if tx.tx_type in {
            TransactionType.OPENING_BALANCE,
            TransactionType.TRANSFER_IN,
            TransactionType.TRANSFER_OUT,
            TransactionType.ADJUSTMENT,
        }:
            _apply_benchmark_flow(benchmark_state, tx, history)
        portfolio_snapshots.append(
            _make_snapshot(
                timestamp=tx.timestamp_jst,
                actual_state=actual_state,
                benchmark_state=benchmark_state,
                benchmark_enabled=benchmark_enabled,
                history=history,
                review_notes=review_notes,
            )
        )
        asset_quantity_history.extend(
            _make_asset_quantity_snapshots(tx.timestamp_jst, actual_state.balances, history, review_notes)
        )

    if portfolio_snapshots[-1].timestamp != period_end:
        portfolio_snapshots.append(
            _make_snapshot(
                timestamp=period_end,
                actual_state=actual_state,
                benchmark_state=benchmark_state,
                benchmark_enabled=benchmark_enabled,
                history=history,
                review_notes=review_notes,
            )
        )
        asset_quantity_history.extend(
            _make_asset_quantity_snapshots(period_end, actual_state.balances, history, review_notes)
        )

    benchmark_snapshots = [
        BenchmarkSnapshot(
            timestamp=row.timestamp,
            total_equity_jpy=row.benchmark_total_equity_jpy,
            total_equity_usd=row.benchmark_total_equity_usd,
            edge_vs_benchmark_jpy=row.edge_vs_benchmark_jpy,
            edge_vs_benchmark_usd=row.edge_vs_benchmark_usd,
        )
        for row in portfolio_snapshots
    ]
    pnl_attribution_snapshots = _build_attribution_rows(portfolio_snapshots, history, review_notes)
    final_timestamp = portfolio_snapshots[-1].timestamp if portfolio_snapshots else period_end
    asset_summary_table = _build_asset_summary_table(
        asset_quantity_history,
        actual_state,
        history,
        final_timestamp,
        review_notes,
    )
    final_snapshot = portfolio_snapshots[-1]
    edge_report = serialize_payload(
        {
            "final_total_equity_jpy": final_snapshot.total_equity_jpy,
            "final_total_equity_usd": final_snapshot.total_equity_usd,
            "final_benchmark_total_equity_jpy": final_snapshot.benchmark_total_equity_jpy,
            "final_benchmark_total_equity_usd": final_snapshot.benchmark_total_equity_usd,
            "final_edge_vs_benchmark_jpy": final_snapshot.edge_vs_benchmark_jpy,
            "final_edge_vs_benchmark_usd": final_snapshot.edge_vs_benchmark_usd,
            "final_trading_edge_jpy": final_snapshot.trading_edge_jpy,
            "final_trading_edge_usd": final_snapshot.trading_edge_usd,
            "fees_jpy": final_snapshot.fees_jpy,
            "fees_usd": final_snapshot.fees_usd,
            "reward_income_jpy": final_snapshot.reward_income_jpy,
            "reward_income_usd": final_snapshot.reward_income_usd,
            "method_reference": method_reference.value,
        }
    )

    return {
        "portfolio_snapshots": portfolio_snapshots,
        "asset_quantity_history": asset_quantity_history,
        "benchmark_snapshots": benchmark_snapshots,
        "pnl_attribution_snapshots": pnl_attribution_snapshots,
        "asset_summary_table": asset_summary_table,
        "edge_report": edge_report,
        "review_notes": sorted(review_notes),
    }


def run_portfolio_analysis(
    transactions: list,
    year: int,
    method_reference: CalculationMethod,
    manual_rate_table,
) -> AnalysisRunResult:
    year_start = datetime(year, 1, 1, tzinfo=JST)
    year_end = datetime(year, 12, 31, 23, 59, 59, tzinfo=JST)
    core = _execute_portfolio_analysis(
        transactions=transactions,
        period_start=year_start,
        period_end=year_end,
        method_reference=method_reference,
        manual_rate_table=manual_rate_table,
    )

    return AnalysisRunResult(
        run_id=f"analysis_{uuid4().hex[:12]}",
        year=year,
        method_reference=method_reference,
        portfolio_snapshots=core["portfolio_snapshots"],
        asset_quantity_history=core["asset_quantity_history"],
        benchmark_snapshots=core["benchmark_snapshots"],
        pnl_attribution_snapshots=core["pnl_attribution_snapshots"],
        asset_summary_table=core["asset_summary_table"],
        edge_report=core["edge_report"],
        review_notes=core["review_notes"],
    )


def run_portfolio_analysis_window(
    transactions: list,
    start_year: int,
    end_year: int,
    method_reference: CalculationMethod,
    manual_rate_table,
) -> dict:
    period_start = datetime(start_year, 1, 1, tzinfo=JST)
    period_end = datetime(end_year, 12, 31, 23, 59, 59, tzinfo=JST)
    core = _execute_portfolio_analysis(
        transactions=transactions,
        period_start=period_start,
        period_end=period_end,
        method_reference=method_reference,
        manual_rate_table=manual_rate_table,
    )
    latest_snapshot = core["portfolio_snapshots"][-1] if core["portfolio_snapshots"] else None
    return {
        "run_id": f"analysis_window_{uuid4().hex[:12]}",
        "scope": "range",
        "start_year": start_year,
        "end_year": end_year,
        "method_reference": method_reference.value,
        "generated_at": latest_snapshot.timestamp.isoformat() if latest_snapshot and latest_snapshot.timestamp else None,
        "latest_snapshot": serialize_payload(asdict(latest_snapshot)) if latest_snapshot else None,
        **core,
    }
