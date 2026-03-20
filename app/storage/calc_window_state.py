from __future__ import annotations

from pathlib import Path
from typing import Any

from app.domain.enums import CalculationMethod
from app.storage.json_store import dump_json, load_json
from app.storage.settings import get_paths


def _root() -> Path:
    root = get_paths().app_data / "calc_window_runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _index_path() -> Path:
    return _root() / "index.json"


def _run_path(run_id: str) -> Path:
    return _root() / f"{run_id}.json"


def save_calc_window_run(payload: dict[str, Any]) -> None:
    dump_json(_run_path(payload["run_id"]), payload)
    index = load_json(_index_path(), [])
    index = [row for row in index if row.get("run_id") != payload["run_id"]]
    index.append(
        {
            "run_id": payload["run_id"],
            "method": payload["method"],
            "start_year": payload["start_year"],
            "end_year": payload["end_year"],
            "created_at": payload.get("generated_at"),
        }
    )
    dump_json(_index_path(), index)


def load_calc_window_run(run_id: str) -> dict[str, Any] | None:
    path = _run_path(run_id)
    if not path.exists():
        return None
    return load_json(path, None)


def load_latest_calc_window_run(
    method: CalculationMethod | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict[str, Any] | None:
    rows = load_json(_index_path(), [])
    rows = sorted(rows, key=lambda row: row.get("created_at") or "", reverse=True)
    for row in rows:
        if method and row.get("method") != method.value:
            continue
        if start_year and row.get("start_year") != start_year:
            continue
        if end_year and row.get("end_year") != end_year:
            continue
        return load_calc_window_run(row["run_id"])
    return None
