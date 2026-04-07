from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from .transformations import (
    compute_country_growth,
    compute_latest_country_rankings,
    compute_yearly_indicator_summary,
)


class WellbeingAnalyzer:
    """Analysis layer for aggregated outputs and missingness checks."""

    def analyze(
        self,
        processed_records: list[dict[str, Any]],
        indicator_map: dict[str, str],
        country_profiles: dict[str, dict[str, str]],
        min_year: int,
        max_year: int,
        top_n: int = 5,
    ) -> dict[str, Any]:
        indicator_details = self._build_indicator_details(indicator_map)

        yearly_summary = self._decorate_indicator_rows(
            compute_yearly_indicator_summary(processed_records),
            indicator_map=indicator_map,
        )
        country_growth = self._decorate_indicator_rows(
            compute_country_growth(processed_records),
            indicator_map=indicator_map,
        )
        rankings = self._decorate_indicator_rows(
            compute_latest_country_rankings(processed_records, top_n=top_n),
            indicator_map=indicator_map,
        )

        missingness = self._build_missingness_report(
            records=processed_records,
            indicator_map=indicator_map,
            country_profiles=country_profiles,
            min_year=min_year,
            max_year=max_year,
        )

        return {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "time_from": min_year,
                "time_to": max_year,
                "indicators": indicator_details,
                "countries": country_profiles,
            },
            "row_counts": {
                "processed_rows": len(processed_records),
            },
            "analysis": {
                "yearly_indicator_summary": yearly_summary,
                "country_growth": country_growth,
                "latest_country_rankings": rankings,
            },
            "missingness_report": missingness,
            "notes": self._build_notes(missingness, max_year=max_year),
        }

    def _build_missingness_report(
        self,
        records: list[dict[str, Any]],
        indicator_map: dict[str, str],
        country_profiles: dict[str, dict[str, str]],
        min_year: int,
        max_year: int,
    ) -> list[dict[str, Any]]:
        expected_years = set(range(min_year, max_year + 1))
        observed_by_pair: dict[tuple[str, str], set[int]] = defaultdict(set)

        code_to_key = {code: key for key, code in indicator_map.items()}
        for row in records:
            indicator_code = row.get("indicator")
            ref_area = row.get("ref_area")
            year = row.get("time_period")
            if (
                isinstance(indicator_code, str)
                and isinstance(ref_area, str)
                and isinstance(year, int)
                and indicator_code in code_to_key
                and ref_area in country_profiles
            ):
                observed_by_pair[(indicator_code, ref_area)].add(year)

        report: list[dict[str, Any]] = []
        for config_key, indicator_code in indicator_map.items():
            for country_code, profile in country_profiles.items():
                available_years = sorted(observed_by_pair.get((indicator_code, country_code), set()))
                missing_years = sorted(expected_years - set(available_years))
                report.append(
                    {
                        "config_key": config_key,
                        "indicator_code": indicator_code,
                        "indicator_name": self._indicator_display_name(config_key),
                        "country_code": country_code,
                        "country_name": profile["name"],
                        "country_role": profile["role"],
                        "available_years": available_years,
                        "missing_years": missing_years,
                        "latest_available_year": max(available_years) if available_years else None,
                        "complete_for_requested_window": len(missing_years) == 0,
                    }
                )

        return sorted(report, key=lambda x: (x["config_key"], x["country_code"]))

    def _decorate_indicator_rows(
        self,
        rows: list[dict[str, Any]],
        indicator_map: dict[str, str],
    ) -> list[dict[str, Any]]:
        code_to_key = {code: key for key, code in indicator_map.items()}
        decorated: list[dict[str, Any]] = []

        for row in rows:
            indicator_code = row.get("indicator")
            config_key = code_to_key.get(indicator_code) if isinstance(indicator_code, str) else None
            decorated_row = dict(row)
            decorated_row.pop("indicator", None)
            decorated_row["indicator_code"] = indicator_code
            decorated_row["config_key"] = config_key
            decorated_row["indicator_name"] = (
                self._indicator_display_name(config_key) if config_key else None
            )
            decorated.append(decorated_row)

        return decorated

    def _build_indicator_details(self, indicator_map: dict[str, str]) -> dict[str, dict[str, str]]:
        details: dict[str, dict[str, str]] = {}
        for config_key, code in indicator_map.items():
            details[config_key] = {
                "code": code,
                "name": self._indicator_display_name(config_key),
            }
        return details

    @staticmethod
    def _indicator_display_name(config_key: str) -> str:
        words = config_key.split("_")
        normalized: list[str] = []
        for word in words:
            if word.lower() == "gdp":
                normalized.append("GDP")
            else:
                normalized.append(word.capitalize())
        return " ".join(normalized)

    @staticmethod
    def _build_notes(missingness: list[dict[str, Any]], max_year: int) -> list[str]:
        latest_year_gaps = [row for row in missingness if max_year in row["missing_years"]]
        if not latest_year_gaps:
            return [f"All indicator-country pairs include {max_year} data."]
        return [
            f"{len(latest_year_gaps)} indicator-country pairs are missing {max_year}.",
            f"Treat {max_year} as latest available where present and use balanced-year comparisons where needed.",
        ]
