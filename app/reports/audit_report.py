from __future__ import annotations


def build_audit_report(run_data: dict) -> list[dict]:
    return list(run_data.get("audit_rows", []))
