from __future__ import annotations

from abc import ABC, abstractmethod


class ExchangeClientBase(ABC):
    @abstractmethod
    def test_connection(self) -> dict:
        raise NotImplementedError

    @abstractmethod
    def sync_transactions(self, **kwargs) -> list:
        raise NotImplementedError
