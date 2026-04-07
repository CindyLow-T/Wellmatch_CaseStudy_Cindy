from __future__ import annotations

from collections import defaultdict
from statistics import mean, median
from typing import Any


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_observations(raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []

    for row in raw_records:
        normalized.append(
            {
                "indicator": row.get("INDICATOR"),
                "ref_area": row.get("REF_AREA"),
                "time_period": _to_int(row.get("TIME_PERIOD")),
                "freq": row.get("FREQ"),
                "obs_value": _to_float(row.get("OBS_VALUE")),
                "unit_measure": row.get("UNIT_MEASURE"),
                "unit_mult": _to_int(row.get("UNIT_MULT")),
                "data_source": row.get("DATA_SOURCE"),
                "latest_data": bool(row.get("LATEST_DATA")),
            }
        )

    return normalized


def filter_observations(
    normalized_records: list[dict[str, Any]],
    min_year: int | None = None,
    max_year: int | None = None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []

    for row in normalized_records:
        year = row.get("time_period")
        value = row.get("obs_value")
        ref_area = row.get("ref_area")

        if row.get("freq") != "A":
            continue
        if year is None or value is None:
            continue
        if min_year is not None and year < min_year:
            continue
        if max_year is not None and year > max_year:
            continue
        if not isinstance(ref_area, str) or len(ref_area) != 3:
            continue

        filtered.append(row)

    return filtered


def compute_yearly_indicator_summary(
    filtered_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[float]] = defaultdict(list)

    for row in filtered_records:
        indicator = row.get("indicator")
        year = row.get("time_period")
        value = row.get("obs_value")
        if isinstance(indicator, str) and isinstance(year, int) and isinstance(value, float):
            grouped[(indicator, year)].append(value)

    summary: list[dict[str, Any]] = []
    for (indicator, year), values in grouped.items():
        summary.append(
            {
                "indicator": indicator,
                "year": year,
                "observation_count": len(values),
                "mean_value": round(mean(values), 4),
                "median_value": round(median(values), 4),
                "min_value": round(min(values), 4),
                "max_value": round(max(values), 4),
            }
        )

    return sorted(summary, key=lambda x: (x["indicator"], x["year"]))


def compute_country_growth(filtered_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_series: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in filtered_records:
        indicator = row.get("indicator")
        ref_area = row.get("ref_area")
        if isinstance(indicator, str) and isinstance(ref_area, str):
            by_series[(indicator, ref_area)].append(row)

    growth_rows: list[dict[str, Any]] = []
    for (indicator, ref_area), rows in by_series.items():
        ordered = sorted(rows, key=lambda x: x["time_period"])
        for prev_row, curr_row in zip(ordered, ordered[1:]):
            prev_value = prev_row["obs_value"]
            curr_value = curr_row["obs_value"]
            prev_year = prev_row["time_period"]
            curr_year = curr_row["time_period"]

            abs_change = curr_value - prev_value
            pct_change = None if prev_value == 0 else (abs_change / prev_value) * 100

            growth_rows.append(
                {
                    "indicator": indicator,
                    "ref_area": ref_area,
                    "from_year": prev_year,
                    "to_year": curr_year,
                    "year_gap": curr_year - prev_year,
                    "previous_value": round(prev_value, 4),
                    "current_value": round(curr_value, 4),
                    "absolute_change": round(abs_change, 4),
                    "percent_change": None if pct_change is None else round(pct_change, 4),
                }
            )

    return sorted(
        growth_rows,
        key=lambda x: (x["indicator"], x["ref_area"], x["from_year"], x["to_year"]),
    )


def compute_latest_country_rankings(
    filtered_records: list[dict[str, Any]],
    top_n: int = 10,
) -> list[dict[str, Any]]:
    by_indicator: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in filtered_records:
        indicator = row.get("indicator")
        if isinstance(indicator, str):
            by_indicator[indicator].append(row)

    rankings: list[dict[str, Any]] = []
    for indicator, rows in by_indicator.items():
        latest_year = max(row["time_period"] for row in rows)
        latest_rows = [row for row in rows if row["time_period"] == latest_year]
        latest_rows = sorted(latest_rows, key=lambda x: x["obs_value"], reverse=True)

        top_rows = latest_rows[:top_n]
        bottom_rows = list(reversed(latest_rows[-top_n:]))

        rankings.append(
            {
                "indicator": indicator,
                "latest_year": latest_year,
                "top_countries": [
                    {"ref_area": row["ref_area"], "value": round(row["obs_value"], 4)}
                    for row in top_rows
                ],
                "bottom_countries": [
                    {"ref_area": row["ref_area"], "value": round(row["obs_value"], 4)}
                    for row in bottom_rows
                ],
            }
        )

    return sorted(rankings, key=lambda x: x["indicator"])

