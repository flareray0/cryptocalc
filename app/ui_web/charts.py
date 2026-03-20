from __future__ import annotations

from html import escape


def build_line_chart(
    series_list: list[dict],
    *,
    width: int = 760,
    height: int = 220,
    title: str = "",
) -> str:
    flattened: list[float] = []
    for series in series_list:
        flattened.extend(float(value) for value in series.get("values", []) if value is not None)
    if not flattened:
        return "<div class='muted'>表示できるデータがまだないよ。</div>"

    min_value = min(flattened)
    max_value = max(flattened)
    if max_value == min_value:
        max_value += 1.0

    def point_string(values: list[float | None]) -> str:
        usable = [value for value in values if value is not None]
        if not usable:
            return ""
        step_x = width / max(len(values) - 1, 1)
        points: list[str] = []
        for index, value in enumerate(values):
            if value is None:
                continue
            x = index * step_x
            ratio = (float(value) - min_value) / (max_value - min_value)
            y = height - (ratio * (height - 24)) - 12
            points.append(f"{x:.2f},{y:.2f}")
        return " ".join(points)

    polylines = []
    legend = []
    for series in series_list:
        points = point_string(series.get("values", []))
        if not points:
            continue
        color = series.get("color", "#0f766e")
        label = escape(str(series.get("label", "series")))
        polylines.append(
            f"<polyline fill='none' stroke='{color}' stroke-width='3' stroke-linecap='round' points='{points}' />"
        )
        legend.append(f"<span><i style='background:{color}'></i>{label}</span>")

    return (
        "<div class='chart-shell'>"
        f"<div class='chart-head'><strong>{escape(title)}</strong></div>"
        f"<svg viewBox='0 0 {width} {height}' class='chart-svg'>"
        f"<line x1='0' y1='{height-12}' x2='{width}' y2='{height-12}' stroke='#d7d1c4' />"
        f"{''.join(polylines)}"
        "</svg>"
        f"<div class='chart-legend'>{''.join(legend)}</div>"
        "</div>"
    )
