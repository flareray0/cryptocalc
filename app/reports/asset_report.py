from __future__ import annotations


def build_asset_report(run_data: dict) -> list[dict]:
    return list(run_data.get("asset_summaries", []))
