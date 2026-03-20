from __future__ import annotations


def build_yearly_report(run_data: dict) -> dict:
    return run_data.get("yearly_summary", {})
