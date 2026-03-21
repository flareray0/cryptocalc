from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    JST: tzinfo = ZoneInfo("Asia/Tokyo")
except ZoneInfoNotFoundError:
    # Windows + tzdata 未導入でも JST は固定オフセットで安全に扱える
    JST = timezone(timedelta(hours=9), name="JST")
DECIMAL_ZERO = Decimal("0")


def to_decimal(value: Any, *, default: Decimal | None = None) -> Decimal | None:
    if value is None or value == "":
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).replace(",", "").strip())
    except (InvalidOperation, AttributeError):
        return default


def quantize_jpy(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def utc_to_jst(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.astimezone(JST)


def safe_slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)


def serialize_payload(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return serialize_payload(asdict(cast(Any, value)))
    if isinstance(value, dict):
        return {k: serialize_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize_payload(v) for v in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value
