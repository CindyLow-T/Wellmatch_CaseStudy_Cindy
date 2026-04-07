from __future__ import annotations

from typing import Any

from .transformations import filter_observations, normalize_observations


class WellbeingProcessor:
    """Processing layer that normalizes and filters raw observations."""

    def process(
        self,
        raw_records: list[dict[str, Any]],
        country_profiles: dict[str, dict[str, str]],
        min_year: int,
        max_year: int,
    ) -> dict[str, list[dict[str, Any]]]:
        normalized = normalize_observations(raw_records)
        for idx, row in enumerate(normalized):
            row["config_key"] = raw_records[idx].get("CONFIG_KEY")

        filtered = filter_observations(normalized, min_year=min_year, max_year=max_year)
        filtered = [row for row in filtered if row["ref_area"] in country_profiles]

        for row in filtered:
            profile = country_profiles[row["ref_area"]]
            row["country_name"] = profile["name"]
            row["country_role"] = profile["role"]

        return {
            "normalized": normalized,
            "filtered": filtered,
        }

