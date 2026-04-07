import requests

from wellbeing_pipeline.settings import Settings
from wellbeing_pipeline.worldbank_client import Data360ApiError, Data360Client


class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}", response=self)

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _settings(max_retries: int = 1) -> Settings:
    return Settings(
        base_url="https://example.org",
        request_timeout_seconds=10,
        max_retries=max_retries,
        retry_backoff_seconds=0,
        default_database_id="WB_WDI",
        default_indicator="WB_WDI_SP_POP_TOTL",
        default_time_from=None,
        default_time_to=None,
    )


def test_fetch_data_paginates_until_expected_count() -> None:
    session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "count": 3,
                    "value": [
                        {"INDICATOR": "I1", "REF_AREA": "USA", "TIME_PERIOD": "2020", "FREQ": "A", "OBS_VALUE": "1"},
                        {"INDICATOR": "I1", "REF_AREA": "GBR", "TIME_PERIOD": "2020", "FREQ": "A", "OBS_VALUE": "2"},
                    ],
                },
            ),
            FakeResponse(
                200,
                {
                    "count": 3,
                    "value": [
                        {"INDICATOR": "I1", "REF_AREA": "FRA", "TIME_PERIOD": "2020", "FREQ": "A", "OBS_VALUE": "3"},
                    ],
                },
            ),
        ]
    )
    client = Data360Client(settings=_settings(), session=session)
    rows = client.fetch_data(database_id="WB_WDI", indicator="I1")

    assert len(rows) == 3
    assert session.calls[0]["params"]["skip"] == 0
    assert session.calls[1]["params"]["skip"] == 2


def test_fetch_data_retries_on_connection_error() -> None:
    session = FakeSession(
        [
            requests.ConnectionError("temporary network issue"),
            FakeResponse(200, {"count": 0, "value": []}),
        ]
    )
    client = Data360Client(settings=_settings(max_retries=1), session=session)
    rows = client.fetch_data(database_id="WB_WDI", indicator="I1")

    assert rows == []
    assert len(session.calls) == 2


def test_fetch_data_raises_on_invalid_payload() -> None:
    session = FakeSession([FakeResponse(200, {"value": []})])
    client = Data360Client(settings=_settings(), session=session)

    try:
        client.fetch_data(database_id="WB_WDI", indicator="I1")
        assert False, "Expected Data360ApiError"
    except Data360ApiError:
        assert True
