from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Data source selection
DATABASE_ID = "WB_WDI"

# Core case-study indicators
INDICATORS = {
    "life_expectancy": "WB_WDI_SP_DYN_LE00_IN",
    "gdp_per_capita": "WB_WDI_NY_GDP_PCAP_CD",
    "health_expenditure_per_capita": "WB_WDI_SH_XPD_CHEX_PC_CD",
}

# Selected comparison countries
COUNTRIES = {
    "GBR": "United Kingdom",
    "USA": "United States",
    "DEU": "Germany",
    "SWE": "Sweden",
    "CHN": "China",
}

# Optional labels used in output metadata
COUNTRY_ROLES = {
    "GBR": "Primary focus",
    "USA": "High-income benchmark",
    "DEU": "European comparator",
    "SWE": "Welfare model comparator",
    "CHN": "Rapidly developing comparator",
}

# Time window
TIME_PERIOD_FROM = "2018"
TIME_PERIOD_TO = "2024"

# Output defaults
TOP_N = 5
DEFAULT_JSON_OUTPUT = "output/json/wellbeing_analysis.json"
DEFAULT_CSV_DIR = "output/csv"


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    base_url: str
    request_timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    default_database_id: str
    default_indicator: str
    default_time_from: str | None
    default_time_to: str | None


def load_settings(env_file: str | Path | None = ".env") -> Settings:
    if env_file is not None:
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path, override=False)

    return Settings(
        base_url=os.getenv("DATA360_BASE_URL", "https://data360api.worldbank.org"),
        request_timeout_seconds=_env_int("DATA360_REQUEST_TIMEOUT_SECONDS", 30),
        max_retries=_env_int("DATA360_MAX_RETRIES", 3),
        retry_backoff_seconds=_env_float("DATA360_RETRY_BACKOFF_SECONDS", 1.0),
        default_database_id=os.getenv("DATA360_DATABASE_ID", DATABASE_ID),
        default_indicator=os.getenv(
            "DATA360_DEFAULT_INDICATOR",
            INDICATORS["life_expectancy"],
        ),
        default_time_from=os.getenv("DATA360_DEFAULT_TIME_FROM") or TIME_PERIOD_FROM,
        default_time_to=os.getenv("DATA360_DEFAULT_TIME_TO") or TIME_PERIOD_TO,
    )
