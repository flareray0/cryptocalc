from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.domain.enums import CalculationMethod


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


@dataclass(slots=True)
class AppPaths:
    root: Path
    app_data: Path
    imports: Path
    calc_runs: Path
    logs: Path
    exports: Path
    samples: Path
    secrets: Path


DEFAULT_SETTINGS: dict[str, Any] = {
    "default_year": None,
    "default_method": CalculationMethod.TOTAL_AVERAGE.value,
    "host": "127.0.0.1",
    "port": 8017,
    "allowed_origins": [
        "http://127.0.0.1:8017",
        "http://localhost:8017",
    ],
    "binance_japan_api": {
        "enabled": False,
        "base_url": "https://api.binance.com",
        "read_only_recommended": True,
        "default_symbols": "",
        "default_start_time_ms": None,
        "default_end_time_ms": None,
    },
    "manual_rate_file": None,
    "disclaimer_acknowledged": False,
    "calc_window": {
        "default_start_year": None,
        "default_end_year": None,
    },
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def get_paths() -> AppPaths:
    root = project_root()
    storage_root = root / "app" / "storage"
    app_data = storage_root / "app_data"
    imports = app_data / "imports"
    calc_runs = app_data / "calc_runs"
    logs = root / "logs"
    exports = root / "exports"
    samples = root / "samples"
    secrets = storage_root / "secrets"
    for path in (app_data, imports, calc_runs, logs, exports, samples, secrets):
        path.mkdir(parents=True, exist_ok=True)
    return AppPaths(
        root=root,
        app_data=app_data,
        imports=imports,
        calc_runs=calc_runs,
        logs=logs,
        exports=exports,
        samples=samples,
        secrets=secrets,
    )


def settings_path() -> Path:
    return get_paths().app_data / "settings.json"


def load_settings() -> dict[str, Any]:
    path = settings_path()
    if not path.exists():
        save_settings(DEFAULT_SETTINGS)
        return deepcopy(DEFAULT_SETTINGS)
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return _deep_merge(DEFAULT_SETTINGS, data)


def save_settings(settings: dict[str, Any]) -> None:
    path = settings_path()
    merged = _deep_merge(DEFAULT_SETTINGS, settings)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(merged, fh, ensure_ascii=False, indent=2)
