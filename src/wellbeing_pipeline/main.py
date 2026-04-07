from __future__ import annotations

import argparse
from typing import Any

from .exporter import Exporter
from .settings import (
    COUNTRIES,
    COUNTRY_ROLES,
    DATABASE_ID,
    DEFAULT_CSV_DIR,
    DEFAULT_JSON_OUTPUT,
    INDICATORS,
    TIME_PERIOD_FROM,
    TIME_PERIOD_TO,
    TOP_N,
    load_settings,
)
from .wellbeing_analyzer import WellbeingAnalyzer
from .wellbeing_processor import WellbeingProcessor
from .worldbank_client import Data360ApiError, WorldBankClient


def _parse_indicator_overrides(items: list[str] | None) -> dict[str, str]:
    if not items:
        return dict(INDICATORS)

    parsed: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid --indicator-map value '{item}'. Use key=indicator_code.")
        key, code = item.split("=", 1)
        key = key.strip()
        code = code.strip()
        if not key or not code:
            raise ValueError(f"Invalid --indicator-map value '{item}'. Use key=indicator_code.")
        parsed[key] = code
    return parsed


def _build_country_profiles(country_csv: str) -> dict[str, dict[str, str]]:
    selected = [code.strip().upper() for code in country_csv.split(",") if code.strip()]
    profiles: dict[str, dict[str, str]] = {}
    for code in selected:
        country_name = COUNTRIES.get(code, code)
        country_role = COUNTRY_ROLES.get(code, "Comparator")
        profiles[code] = {"name": country_name, "role": country_role}
    return profiles


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wellbeing-pipeline",
        description="World Bank API -> ingestion -> processing -> analysis -> CSV/JSON export.",
    )
    parser.add_argument("--database-id", help="Data360 database ID (e.g., WB_WDI).")
    parser.add_argument("--time-from", default=TIME_PERIOD_FROM, help="Inclusive start year.")
    parser.add_argument("--time-to", default=TIME_PERIOD_TO, help="Inclusive end year.")
    parser.add_argument(
        "--countries",
        default=",".join(COUNTRIES.keys()),
        help="Comma-separated country codes.",
    )
    parser.add_argument(
        "--indicator-map",
        action="append",
        help="Override indicators with key=indicator_code. Repeat for multiple entries.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=TOP_N,
        help="Number of countries to show in top/bottom ranking outputs.",
    )
    parser.add_argument(
        "--json-output",
        default=DEFAULT_JSON_OUTPUT,
        help="Output JSON path.",
    )
    parser.add_argument(
        "--csv-dir",
        default=DEFAULT_CSV_DIR,
        help="Output directory for CSV files.",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    settings = load_settings()
    worldbank_client = WorldBankClient(settings)
    processor = WellbeingProcessor()
    analyzer = WellbeingAnalyzer()

    database_id = args.database_id or DATABASE_ID
    time_from = args.time_from
    time_to = args.time_to

    indicator_map = _parse_indicator_overrides(args.indicator_map)
    country_profiles = _build_country_profiles(args.countries)
    raw_rows = worldbank_client.fetch_case_study_data(
        database_id=database_id,
        indicator_map=indicator_map,
        country_codes=list(country_profiles.keys()),
        time_from=time_from,
        time_to=time_to,
    )

    processed = processor.process(
        raw_records=raw_rows,
        country_profiles=country_profiles,
        min_year=int(time_from),
        max_year=int(time_to),
    )

    return analyzer.analyze(
        processed_records=processed["filtered"],
        indicator_map=indicator_map,
        country_profiles=country_profiles,
        min_year=int(time_from),
        max_year=int(time_to),
        top_n=args.top_n,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        output = run(args)
    except Data360ApiError as exc:
        raise SystemExit(f"API error: {exc}") from exc

    exporter = Exporter()
    json_path = exporter.export_json(output, output_path=args.json_output, pretty=args.pretty)
    csv_paths = exporter.export_csv_bundle(output, output_dir=args.csv_dir)

    print(f"Wrote JSON output to {json_path.resolve()}")
    print(f"Wrote {len(csv_paths)} CSV files to {args.csv_dir}")
    print(f"Processed rows: {output['row_counts']['processed_rows']}")


if __name__ == "__main__":
    main()
