from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from app.domain.models import ImportBatchResult


class BaseParser(ABC):
    exchange_name = "unknown"

    @abstractmethod
    def can_parse(self, path: Path) -> bool:
        raise NotImplementedError

    @abstractmethod
    def parse(self, path: Path) -> ImportBatchResult:
        raise NotImplementedError

