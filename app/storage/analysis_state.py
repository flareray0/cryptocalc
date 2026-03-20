from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.domain.enums import CalculationMethod
from app.domain.models import AnalysisRunResult
from app.storage.json_store import dump_json, load_json
from app.storage.settings import get_paths
from app.domain.validators import serialize_payload


def _analysis_root():
    root = get_paths().app_data / "analysis_runs"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _run_path(run_id: str):
    return _analysis_root() / f"{run_id}.json"


def _index_path():
    return _analysis_root() / "index.json"


def save_analysis_run(result: AnalysisRunResult) -> None:
    payload = serialize_payload(asdict(result))
    payload["method_reference"] = result.method_reference.value
    dump_json(_run_path(result.run_id), payload)

    index = load_json(_index_path(), [])
    index = [row for row in index if row.get("run_id") != result.run_id]
    index.append(
        {
            "run_id": result.run_id,
            "year": result.year,
            "method_reference": result.method_reference.value,
            "created_at": payload.get("portfolio_snapshots", [{}])[-1].get("timestamp"),
        }
    )
    dump_json(_index_path(), index)


def load_analysis_run(run_id: str) -> dict[str, Any] | None:
    path = _run_path(run_id)
    if not path.exists():
        return None
    return load_json(path, None)


def load_latest_analysis_run(
    method_reference: CalculationMethod | None = None,
    year: int | None = None,
) -> dict[str, Any] | None:
    index = load_json(_index_path(), [])
    rows = sorted(index, key=lambda row: row.get("created_at") or "", reverse=True)
    for row in rows:
        if method_reference and row.get("method_reference") != method_reference.value:
            continue
        if year and row.get("year") != year:
            continue
        return load_analysis_run(row["run_id"])
    return None
