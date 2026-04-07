from __future__ import annotations

import time
from typing import Any

import requests

from .settings import Settings


class Data360ApiError(RuntimeError):
    """Raised when the Data360 API call fails or returns invalid payloads."""


class Data360Client:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self._settings = settings
        self._session = session or requests.Session()

    def _request_json(self, path: str, params: dict[str, Any]) -> dict[str, Any] | list[Any]:
        url = f"{self._settings.base_url.rstrip('/')}{path}"
        last_error: Exception | None = None

        for attempt in range(self._settings.max_retries + 1):
            retryable = False
            try:
                response = self._session.get(
                    url,
                    params=params,
                    timeout=self._settings.request_timeout_seconds,
                )
                if response.status_code >= 500:
                    raise requests.HTTPError(
                        f"Server error status={response.status_code}",
                        response=response,
                    )
                response.raise_for_status()
                return response.json()
            except (requests.Timeout, requests.ConnectionError) as exc:
                retryable = True
                last_error = exc
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                retryable = status == 429 or (status is not None and status >= 500)
                if not retryable:
                    raise Data360ApiError(
                        f"Non-retryable API error (status={status}) for {url} with params={params}"
                    ) from exc
                last_error = exc
            except ValueError as exc:
                raise Data360ApiError(
                    f"Received invalid JSON from {url} with params={params}"
                ) from exc

            if not retryable or attempt == self._settings.max_retries:
                break
            time.sleep(self._settings.retry_backoff_seconds * (2**attempt))

        raise Data360ApiError(
            f"API request failed after retries for {url} with params={params}"
        ) from last_error

    def fetch_data(
        self,
        database_id: str,
        indicator: str | None = None,
        ref_area: str | None = None,
        time_period_from: str | None = None,
        time_period_to: str | None = None,
        frequency: str = "A",
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "DATABASE_ID": database_id,
            "skip": 0,
        }
        if indicator:
            params["INDICATOR"] = indicator
        if ref_area:
            params["REF_AREA"] = ref_area
        if time_period_from:
            params["timePeriodFrom"] = time_period_from
        if time_period_to:
            params["timePeriodTo"] = time_period_to
        if frequency:
            params["FREQ"] = frequency

        records: list[dict[str, Any]] = []
        expected_count: int | None = None

        while True:
            payload = self._request_json("/data360/data", params=params)
            if not isinstance(payload, dict):
                raise Data360ApiError("Expected object response for /data360/data request")

            count = payload.get("count")
            value = payload.get("value")

            if not isinstance(count, int) or not isinstance(value, list):
                raise Data360ApiError(
                    "Invalid /data360/data payload. Expected keys: count<int>, value<list>"
                )

            if expected_count is None:
                expected_count = count

            records.extend([row for row in value if isinstance(row, dict)])

            if not value or len(records) >= expected_count:
                break

            params["skip"] = int(params["skip"]) + len(value)

        return records


class WorldBankClient:
    """Ingestion layer for fetching World Bank Data360 records."""

    def __init__(self, settings: Settings) -> None:
        self._client = Data360Client(settings)

    def fetch_case_study_data(
        self,
        database_id: str,
        indicator_map: dict[str, str],
        country_codes: list[str],
        time_from: str,
        time_to: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for config_key, indicator_code in indicator_map.items():
            for country_code in country_codes:
                records = self._client.fetch_data(
                    database_id=database_id,
                    indicator=indicator_code,
                    ref_area=country_code,
                    time_period_from=time_from,
                    time_period_to=time_to,
                    frequency="A",
                )
                for record in records:
                    enriched = dict(record)
                    enriched["CONFIG_KEY"] = config_key
                    rows.append(enriched)

        return rows
