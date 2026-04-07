from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


class Exporter:
    """Export layer for JSON and CSV outputs."""

    def export_json(self, payload: dict[str, Any], output_path: str, pretty: bool = True) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            if pretty:
                json.dump(payload, handle, indent=2, sort_keys=True)
            else:
                json.dump(payload, handle, separators=(",", ":"))
        return path

    def export_csv_bundle(self, payload: dict[str, Any], output_dir: str) -> list[Path]:
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        paths: list[Path] = []
        analysis = payload.get("analysis", {})

        paths.append(
            self._write_csv(
                out_dir / "yearly_indicator_summary.csv",
                analysis.get("yearly_indicator_summary", []),
            )
        )
        paths.append(
            self._write_csv(out_dir / "country_growth.csv", analysis.get("country_growth", []))
        )
        paths.append(
            self._write_csv(out_dir / "missingness_report.csv", payload.get("missingness_report", []))
        )
        paths.append(
            self._write_rankings_csv(
                out_dir / "latest_country_rankings.csv",
                analysis.get("latest_country_rankings", []),
            )
        )

        return paths

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
        if not rows:
            with path.open("w", encoding="utf-8", newline="") as handle:
                handle.write("")
            return path

        fieldnames: list[str] = sorted({key for row in rows for key in row.keys()})
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return path

    @staticmethod
    def _write_rankings_csv(path: Path, ranking_rows: list[dict[str, Any]]) -> Path:
        fieldnames = [
            "config_key",
            "indicator_code",
            "indicator_name",
            "latest_year",
            "rank_group",
            "rank",
            "country_code",
            "value",
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()

            for row in ranking_rows:
                config_key = row.get("config_key")
                indicator_code = row.get("indicator_code")
                indicator_name = row.get("indicator_name")
                latest_year = row.get("latest_year")
                for group in ("top_countries", "bottom_countries"):
                    for index, item in enumerate(row.get(group, []), start=1):
                        writer.writerow(
                            {
                                "config_key": config_key,
                                "indicator_code": indicator_code,
                                "indicator_name": indicator_name,
                                "latest_year": latest_year,
                                "rank_group": "top" if group == "top_countries" else "bottom",
                                "rank": index,
                                "country_code": item.get("ref_area"),
                                "value": item.get("value"),
                            }
                        )
        return path
