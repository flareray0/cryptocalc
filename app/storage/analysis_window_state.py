from __future__ import annotations

from typing import Any

from app.domain.enums import CalculationMethod
from app.storage.json_store import dump_json, load_json
from app.storage.settings import get_paths
from app.domain.validators import serialize_payload


def _analysis_window_root():
    root = get_paths().app_data / "analysis_window_runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _run_path(run_id: str):
    return _analysis_window_root() / f"{run_id}.json"


def _index_path():
    return _analysis_window_root() / "index.json"


def save_analysis_window_run(payload: dict[str, Any]) -> None:
    serialized = serialize_payload(payload)
    dump_json(_run_path(payload["run_id"]), serialized)
    index = load_json(_index_path(), [])
    index = [row for row in index if row.get("run_id") != payload["run_id"]]
    index.append(
        {
            "run_id": payload["run_id"],
            "start_year": payload["start_year"],
            "end_year": payload["end_year"],
            "method_reference": payload["method_reference"],
            "created_at": serialized.get("generated_at")
            or serialized.get("portfolio_snapshots", [{}])[-1].get("timestamp"),
        }
    )
    dump_json(_index_path(), index)


def load_analysis_window_run(run_id: str) -> dict[str, Any] | None:
    path = _run_path(run_id)
    if not path.exists():
        return None
    return load_json(path, None)


def load_latest_analysis_window_run(
    *,
    method_reference: CalculationMethod | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict[str, Any] | None:
    index = load_json(_index_path(), [])
    rows = sorted(index, key=lambda row: row.get("created_at") or "", reverse=True)
    for row in rows:
        if method_reference and row.get("method_reference") != method_reference.value:
            continue
        if start_year is not None and row.get("start_year") != start_year:
            continue
        if end_year is not None and row.get("end_year") != end_year:
            continue
        return load_analysis_window_run(row["run_id"])
    return None
