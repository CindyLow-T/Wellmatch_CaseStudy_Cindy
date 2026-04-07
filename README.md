# Wellmatch Data Engineering Work Assessment

This repository implements a small Python data service that:
1. Ingests data from the World Bank Data360 API.
2. Applies meaningful transformations/aggregations.
3. Exposes results through a CLI that writes structured JSON and CSV outputs.

## Project structure

```text
.
├── src/wellbeing_pipeline/
│   ├── main.py                 # clear entry point (CLI)
│   ├── settings.py             # scope + runtime configuration
│   ├── worldbank_client.py     # Ingestion stage
│   ├── wellbeing_processor.py  # Processing stage
│   ├── wellbeing_analyzer.py   # Analysis stage
│   ├── exporter.py             # Export stage (JSON/CSV)
│   └── transformations.py      # shared transformation utilities
├── tests/
│   ├── test_client.py
│   ├── test_transformations.py
│   ├── test_wellbeing_analyzer.py
│   └── test_wellbeing_processor.py
├── .env.example
├── requirements.txt
└── README.md
```

## Setup

1. Create and activate a virtual environment.
2. Install dependencies.

```bash
cd "/Users/cindylow/Documents/Wellmatch_CaseStudy_Cindy"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest
cp .env.example .env
```

## Run

```bash
PYTHONPATH=src python3 -m wellbeing_pipeline.main --pretty
```

Example with explicit options:

```bash
PYTHONPATH=src python3 -m wellbeing_pipeline.main \
  --database-id WB_WDI \
  --indicator-map life_expectancy=WB_WDI_SP_DYN_LE00_IN \
  --indicator-map gdp_per_capita=WB_WDI_NY_GDP_PCAP_CD \
  --indicator-map health_expenditure_per_capita=WB_WDI_SH_XPD_CHEX_PC_CD \
  --countries GBR,USA,DEU,SWE,CHN \
  --time-from 2018 \
  --time-to 2024 \
  --top-n 5 \
  --json-output output/wellbeing_analysis.json \
  --csv-dir output/csv \
  --pretty
```

The command writes:
- JSON output (default: `output/wellbeing_analysis.json`)
- CSV bundle (default folder: `output/csv`)
- Each output includes both `indicator_code` (machine-readable) and `indicator_name` (human-readable).

## Implemented transformations

1. Data normalization:
Converts `OBS_VALUE` and `TIME_PERIOD` into typed numeric fields, and maps API fields into a consistent internal schema.

2. Time-based grouped aggregation:
`yearly_indicator_summary` computes count, mean, median, min, and max for each `(indicator, year)` group.

3. Derived metrics:
`country_growth` computes year-over-year absolute and percentage changes for each `(indicator, ref_area)`.

4. Filtering and ranking:
Filters to annual valid observations and computes `latest_country_rankings` top/bottom countries for the most recent year.

5. Missingness analysis:
Builds a `missingness_report` that explicitly lists missing years by country and indicator.

## Testing

```bash
pytest -q
```

Tests focus on critical logic:
- `test_client.py`: API pagination, retry behavior, and invalid payload handling in `worldbank_client.py`.
- `test_transformations.py`: normalization/filtering, grouped metrics, growth calculations, ranking logic.
- `test_wellbeing_processor.py`: cleaning logic, type conversion, and missing data handling.
- `test_wellbeing_analyzer.py`: grouped summary correctness, ranking, missingness/imputation policy, and output structure.
- `conftest.py`: pytest helper for test import path setup (support file, not a test case).

## Assumptions

- `DATABASE_ID` is required for `/data360/data`.
- The API returns a maximum of 1000 rows per request and supports `skip` for pagination.
- `FREQ=A` is used by default for annual comparability.
- `REF_AREA` values are treated as 3-character location codes.
- For the case study window (2018-2024), 2024 is treated as "latest available where present", and missing values are explicitly reported.

## Trade-offs and design decisions

- Chose a lightweight standard-library-first approach (no pandas) to keep dependencies small and logic explicit.
- Implemented retries for transient failures (timeouts, connection issues, 429/5xx).
- Kept output as JSON + CSV via CLI for easy downstream consumption and reproducibility.
- Prioritized readability and modular boundaries (ingestion vs business logic vs interface).
