from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from app.domain.validators import parse_utc_timestamp, to_decimal, utc_to_jst


@dataclass(slots=True)
class RateRow:
    asset: str
    timestamp: datetime | None
    date_key: str | None
    jpy_rate: Decimal
    source: str


class RateInputAdapter:
    def __init__(self) -> None:
        self._rows: list[RateRow] = []

    def load_csv(self, path: Path) -> None:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            rows: list[RateRow] = []
            for row in reader:
                asset = (row.get("asset") or row.get("symbol") or "").strip().upper()
                if not asset:
                    continue
                rate = to_decimal(row.get("jpy_rate") or row.get("rate"))
                if rate is None:
                    continue
                ts = parse_utc_timestamp(row.get("timestamp_utc") or row.get("timestamp"))
                if ts is None and row.get("date"):
                    try:
                        date_key = datetime.strptime(row["date"], "%Y-%m-%d").date().isoformat()
                    except ValueError:
                        date_key = None
                else:
                    date_key = utc_to_jst(ts).date().isoformat() if ts else None
                rows.append(
                    RateRow(
                        asset=asset,
                        timestamp=ts,
                        date_key=date_key,
                        jpy_rate=rate,
                        source=row.get("source", path.name),
                    )
                )
        self._rows = rows

    def lookup(self, asset: str, timestamp: datetime | None) -> tuple[Decimal | None, str | None]:
        asset = asset.upper()
        exact = None
        if timestamp is not None:
            for row in self._rows:
                if row.asset == asset and row.timestamp == timestamp:
                    exact = row
                    break
            if exact:
                return exact.jpy_rate, exact.source
            date_key = utc_to_jst(timestamp).date().isoformat()
            for row in self._rows:
                if row.asset == asset and row.date_key == date_key:
                    return row.jpy_rate, f"{row.source}:date"
        return None, None


@dataclass(slots=True)
class ManualRateTable:
    rows: list[RateRow]

    @classmethod
    def empty(cls) -> "ManualRateTable":
        return cls(rows=[])

    @classmethod
    def from_csv(cls, path: Path) -> "ManualRateTable":
        adapter = RateInputAdapter()
        adapter.load_csv(path)
        return cls(rows=adapter._rows)

    def lookup(self, asset: str, timestamp: datetime | None) -> tuple[Decimal | None, str | None]:
        adapter = RateInputAdapter()
        adapter._rows = self.rows
        return adapter.lookup(asset, timestamp)
