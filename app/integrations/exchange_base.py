from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ExchangeClientBase(ABC):
    @abstractmethod
    def test_connection(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def sync_transactions(self, **kwargs: Any) -> list[Any]:
        raise NotImplementedError
