from __future__ import annotations

import csv

from app.storage.settings import get_paths


def write_nta_exports(run_data: dict) -> dict[str, str]:
    paths = get_paths()
    year = run_data["year"]
    method = run_data["method"]
    csv_path = paths.exports / f"nta_export_{year}_{method}.csv"
    xlsx_path = paths.exports / f"nta_export_{year}_{method}.xlsx"

    headers = [
        "asset",
        "opening_quantity",
        "acquired_quantity",
        "disposed_quantity",
        "ending_quantity",
        "average_cost_per_unit_jpy",
        "proceeds_jpy",
        "realized_pnl_jpy",
        "review_note",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in run_data.get("asset_summaries", []):
            writer.writerow(
                {
                    "asset": row.get("asset"),
                    "opening_quantity": row.get("opening_quantity", ""),
                    "acquired_quantity": row.get("acquired_quantity"),
                    "disposed_quantity": row.get("disposed_quantity"),
                    "ending_quantity": row.get("ending_quantity"),
                    "average_cost_per_unit_jpy": row.get("average_cost_per_unit_jpy"),
                    "proceeds_jpy": row.get("proceeds_jpy"),
                    "realized_pnl_jpy": row.get("realized_pnl_jpy"),
                    "review_note": "要確認取引は別レポート参照",
                }
            )

    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "NTA Export"
    ws.append(headers)
    for row in run_data.get("asset_summaries", []):
        ws.append(
            [
                row.get("asset"),
                row.get("opening_quantity", ""),
                row.get("acquired_quantity"),
                row.get("disposed_quantity"),
                row.get("ending_quantity"),
                row.get("average_cost_per_unit_jpy"),
                row.get("proceeds_jpy"),
                row.get("realized_pnl_jpy"),
                "要確認取引は別レポート参照",
            ]
        )
    wb.save(xlsx_path)

    return {"csv": str(csv_path), "xlsx": str(xlsx_path)}
