from __future__ import annotations

from enum import Enum


class TransactionType(str, Enum):
    BUY = "buy"
    SELL = "sell"
    CRYPTO_SWAP = "crypto_swap"
    TRANSFER_IN = "transfer_in"
    TRANSFER_OUT = "transfer_out"
    REWARD = "reward"
    FEE = "fee"
    ADJUSTMENT = "adjustment"
    OPENING_BALANCE = "opening_balance"
    UNKNOWN = "unknown"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"
    NONE = "none"


class ClassificationStatus(str, Enum):
    CLASSIFIED = "classified"
    REVIEW_REQUIRED = "review_required"
    UNKNOWN = "unknown"


class CalculationMethod(str, Enum):
    TOTAL_AVERAGE = "total_average"
    MOVING_AVERAGE = "moving_average"


class ImportSourceKind(str, Enum):
    CSV = "csv"
    XLSX = "xlsx"
    API = "api"
    MANUAL = "manual"


class LedgerEventKind(str, Enum):
    ACQUIRE = "acquire"
    DISPOSE = "dispose"
    TRANSFER = "transfer"
    FEE = "fee"
    REWARD = "reward"
    UNKNOWN = "unknown"

