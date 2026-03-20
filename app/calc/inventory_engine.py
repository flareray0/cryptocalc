from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.domain.enums import LedgerEventKind, Side, TransactionType
from app.domain.models import LedgerEvent, NormalizedTransaction


ZERO = Decimal("0")


@dataclass(slots=True)
class ResolvedFlow:
    tx: NormalizedTransaction
    acquire_asset: str | None = None
    acquire_quantity: Decimal | None = None
    acquire_value_jpy: Decimal | None = None
    dispose_asset: str | None = None
    dispose_quantity: Decimal | None = None
    proceeds_jpy: Decimal | None = None
    fee_jpy: Decimal | None = None
    fee_asset: str | None = None
    fee_quantity: Decimal | None = None
    review_reasons: list[str] | None = None
    rate_source: str | None = None
    income_jpy: Decimal | None = None

    @property
    def review_flag(self) -> bool:
        return bool(self.review_reasons)


def _net_acquire_quantity(tx: NormalizedTransaction, raw_quantity: Decimal | None) -> Decimal | None:
    if raw_quantity is None:
        return None
    if tx.fee_asset and tx.base_asset and tx.fee_asset == tx.base_asset and tx.fee_amount:
        return raw_quantity - tx.fee_amount
    return raw_quantity


def _dispose_quantity_with_fee(
    asset: str | None, quantity: Decimal | None, fee_asset: str | None, fee_amount: Decimal | None
) -> Decimal | None:
    if quantity is None:
        return None
    if asset and fee_asset and asset == fee_asset and fee_amount:
        return quantity + fee_amount
    return quantity


def _resolve_fee_jpy(
    tx: NormalizedTransaction,
    rate_lookup,
    timestamp,
) -> tuple[Decimal | None, str | None]:
    if tx.fee_jpy is not None:
        return tx.fee_jpy, tx.jpy_rate_source or "file:fee_jpy"
    if not tx.fee_asset or not tx.fee_amount:
        return None, None
    rate, source = rate_lookup.lookup(tx.fee_asset, timestamp)
    if rate is None:
        return None, None
    return tx.fee_amount * rate, source


def _resolve_value_from_rates(
    *,
    tx: NormalizedTransaction,
    preferred_asset: str | None,
    preferred_quantity: Decimal | None,
    fallback_asset: str | None,
    fallback_quantity: Decimal | None,
    rate_lookup,
) -> tuple[Decimal | None, str | None]:
    timestamp = tx.timestamp_utc or tx.timestamp_jst
    if preferred_asset and preferred_quantity:
        rate, source = rate_lookup.lookup(preferred_asset, timestamp)
        if rate is not None:
            return preferred_quantity * rate, source
    if fallback_asset and fallback_quantity:
        rate, source = rate_lookup.lookup(fallback_asset, timestamp)
        if rate is not None:
            return fallback_quantity * rate, source
    return None, None


def resolve_transaction_flow(tx: NormalizedTransaction, rate_lookup) -> ResolvedFlow:
    review_reasons = list(tx.review_reasons)
    fee_jpy, fee_source = _resolve_fee_jpy(tx, rate_lookup, tx.timestamp_utc or tx.timestamp_jst)
    rate_source = tx.jpy_rate_source or fee_source

    if tx.tx_type is TransactionType.BUY:
        acquire_qty = _net_acquire_quantity(tx, tx.quantity)
        acquire_value = tx.gross_amount_jpy
        if acquire_value is None:
            review_reasons.append("JPY約定金額が不足")
        else:
            acquire_value += fee_jpy or ZERO
        return ResolvedFlow(
            tx=tx,
            acquire_asset=tx.base_asset,
            acquire_quantity=acquire_qty,
            acquire_value_jpy=acquire_value,
            fee_jpy=fee_jpy,
            fee_asset=tx.fee_asset,
            fee_quantity=tx.fee_amount,
            review_reasons=review_reasons,
            rate_source=rate_source,
        )

    if tx.tx_type is TransactionType.SELL:
        dispose_qty = _dispose_quantity_with_fee(
            tx.base_asset, tx.quantity, tx.fee_asset, tx.fee_amount
        )
        if tx.gross_amount_jpy is None:
            review_reasons.append("JPY売却金額が不足")
        return ResolvedFlow(
            tx=tx,
            dispose_asset=tx.base_asset,
            dispose_quantity=dispose_qty,
            proceeds_jpy=tx.gross_amount_jpy,
            fee_jpy=fee_jpy,
            fee_asset=tx.fee_asset,
            fee_quantity=tx.fee_amount,
            review_reasons=review_reasons,
            rate_source=rate_source,
        )

    if tx.tx_type is TransactionType.CRYPTO_SWAP:
        if tx.side is Side.BUY:
            acquire_asset = tx.base_asset
            acquire_qty = _net_acquire_quantity(tx, tx.quantity)
            dispose_asset = tx.quote_asset
            dispose_qty = _dispose_quantity_with_fee(
                tx.quote_asset, tx.quote_quantity, tx.fee_asset, tx.fee_amount
            )
            value_jpy, source = _resolve_value_from_rates(
                tx=tx,
                preferred_asset=dispose_asset,
                preferred_quantity=dispose_qty,
                fallback_asset=acquire_asset,
                fallback_quantity=acquire_qty,
                rate_lookup=rate_lookup,
            )
        else:
            acquire_asset = tx.quote_asset
            acquire_qty = _net_acquire_quantity(tx, tx.quote_quantity)
            dispose_asset = tx.base_asset
            dispose_qty = _dispose_quantity_with_fee(
                tx.base_asset, tx.quantity, tx.fee_asset, tx.fee_amount
            )
            value_jpy, source = _resolve_value_from_rates(
                tx=tx,
                preferred_asset=dispose_asset,
                preferred_quantity=dispose_qty,
                fallback_asset=acquire_asset,
                fallback_quantity=acquire_qty,
                rate_lookup=rate_lookup,
            )
        if value_jpy is None:
            review_reasons.append("暗号資産交換のJPY評価が未確定")
        return ResolvedFlow(
            tx=tx,
            acquire_asset=acquire_asset,
            acquire_quantity=acquire_qty,
            acquire_value_jpy=value_jpy,
            dispose_asset=dispose_asset,
            dispose_quantity=dispose_qty,
            proceeds_jpy=value_jpy,
            fee_jpy=fee_jpy,
            fee_asset=tx.fee_asset,
            fee_quantity=tx.fee_amount,
            review_reasons=review_reasons,
            rate_source=tx.jpy_rate_source or source or fee_source,
        )

    if tx.tx_type is TransactionType.REWARD:
        value_jpy = tx.gross_amount_jpy
        source = tx.jpy_rate_source
        if value_jpy is None:
            value_jpy, source = _resolve_value_from_rates(
                tx=tx,
                preferred_asset=tx.base_asset,
                preferred_quantity=tx.quantity,
                fallback_asset=None,
                fallback_quantity=None,
                rate_lookup=rate_lookup,
            )
        if value_jpy is None:
            review_reasons.append("報酬取引のJPY評価が未確定")
        return ResolvedFlow(
            tx=tx,
            acquire_asset=tx.base_asset,
            acquire_quantity=_net_acquire_quantity(tx, tx.quantity),
            acquire_value_jpy=value_jpy,
            income_jpy=value_jpy,
            fee_jpy=fee_jpy,
            fee_asset=tx.fee_asset,
            fee_quantity=tx.fee_amount,
            review_reasons=review_reasons,
            rate_source=source or fee_source,
        )

    if tx.tx_type is TransactionType.OPENING_BALANCE:
        gross = tx.gross_amount_jpy
        if gross is None and tx.price_per_unit_jpy is not None and tx.quantity is not None:
            gross = tx.price_per_unit_jpy * tx.quantity
        if gross is None:
            review_reasons.append("期首残高の取得原価が未確定")
        return ResolvedFlow(
            tx=tx,
            acquire_asset=tx.base_asset,
            acquire_quantity=tx.quantity,
            acquire_value_jpy=gross,
            review_reasons=review_reasons,
            rate_source=tx.jpy_rate_source,
        )

    if tx.tx_type is TransactionType.ADJUSTMENT:
        if tx.side is Side.BUY:
            return ResolvedFlow(
                tx=tx,
                acquire_asset=tx.base_asset,
                acquire_quantity=tx.quantity,
                acquire_value_jpy=tx.gross_amount_jpy,
                fee_jpy=fee_jpy,
                review_reasons=review_reasons,
                rate_source=rate_source,
            )
        if tx.side is Side.SELL:
            return ResolvedFlow(
                tx=tx,
                dispose_asset=tx.base_asset,
                dispose_quantity=tx.quantity,
                proceeds_jpy=tx.gross_amount_jpy,
                fee_jpy=fee_jpy,
                review_reasons=review_reasons,
                rate_source=rate_source,
            )
        review_reasons.append("調整仕訳のside指定が不足")
        return ResolvedFlow(tx=tx, review_reasons=review_reasons)

    if tx.tx_type in {TransactionType.TRANSFER_IN, TransactionType.TRANSFER_OUT}:
        return ResolvedFlow(
            tx=tx,
            review_reasons=review_reasons,
            rate_source=rate_source,
        )

    review_reasons.append("未対応の取引種別")
    return ResolvedFlow(tx=tx, review_reasons=review_reasons)


def flow_to_ledger_events(flow: ResolvedFlow) -> list[LedgerEvent]:
    events: list[LedgerEvent] = []
    timestamp = flow.tx.timestamp_jst
    if flow.acquire_asset and flow.acquire_quantity:
        kind = (
            LedgerEventKind.REWARD
            if flow.tx.tx_type is TransactionType.REWARD
            else LedgerEventKind.ACQUIRE
        )
        events.append(
            LedgerEvent(
                id=f"ev_{flow.tx.id}_acq",
                tx_id=flow.tx.id,
                timestamp_jst=timestamp,
                asset=flow.acquire_asset,
                kind=kind,
                quantity=flow.acquire_quantity,
                gross_value_jpy=flow.acquire_value_jpy,
                fee_jpy=flow.fee_jpy,
                review_flag=flow.review_flag,
                review_reason="; ".join(flow.review_reasons or []) or None,
                source_tx_ids=[flow.tx.id],
                metadata={"rate_source": flow.rate_source},
            )
        )
    if flow.dispose_asset and flow.dispose_quantity:
        events.append(
            LedgerEvent(
                id=f"ev_{flow.tx.id}_disp",
                tx_id=flow.tx.id,
                timestamp_jst=timestamp,
                asset=flow.dispose_asset,
                kind=LedgerEventKind.DISPOSE,
                quantity=flow.dispose_quantity,
                gross_value_jpy=flow.proceeds_jpy,
                fee_jpy=flow.fee_jpy,
                review_flag=flow.review_flag,
                review_reason="; ".join(flow.review_reasons or []) or None,
                source_tx_ids=[flow.tx.id],
                metadata={"rate_source": flow.rate_source},
            )
        )
    return events
