from __future__ import annotations

from pydantic import BaseModel, Field


class CalcRunRequest(BaseModel):
    year: int = Field(..., ge=2000, le=2100)
    method: str


class AnalysisRunRequest(BaseModel):
    year: int = Field(..., ge=2000, le=2100)
    method_reference: str


class AnalysisWindowRunRequest(BaseModel):
    start_year: int | None = Field(default=None, ge=2000, le=2100)
    end_year: int | None = Field(default=None, ge=2000, le=2100)
    method_reference: str


class BalanceReconciliationRefreshRequest(BaseModel):
    start_year: int | None = Field(default=None, ge=2000, le=2100)
    end_year: int | None = Field(default=None, ge=2000, le=2100)
    method_reference: str | None = None


class BinanceConnectRequest(BaseModel):
    api_key: str | None = None
    api_secret: str | None = None
    base_url: str | None = None


class CalcWindowRunRequest(BaseModel):
    start_year: int | None = Field(default=None, ge=2000, le=2100)
    end_year: int | None = Field(default=None, ge=2000, le=2100)
    method: str


class BinanceSyncRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    start_time_ms: int | None = None
    end_time_ms: int | None = None


class SettingsUpdateRequest(BaseModel):
    default_year: int | None = Field(default=None, ge=2000, le=2100)
    default_method: str
    disclaimer_acknowledged: bool = False
    manual_rate_file: str | None = None
    host: str | None = None
    port: int | None = Field(default=None, ge=1, le=65535)
