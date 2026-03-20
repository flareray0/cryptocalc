from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from app.domain.validators import serialize_payload
from app.storage.settings import get_paths


class AuditService:
    def __init__(self) -> None:
        self.paths = get_paths()

    def write_jsonl(self, prefix: str, rows: list[dict[str, Any]]) -> Path:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        path = self.paths.logs / f"{prefix}_{stamp}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(serialize_payload(row), ensure_ascii=False) + "\n")
        return path

    def write_event(self, prefix: str, payload: dict[str, Any]) -> Path:
        return self.write_jsonl(prefix, [payload])
