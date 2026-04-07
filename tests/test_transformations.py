from wellbeing_pipeline.transformations import (
    compute_country_growth,
    compute_latest_country_rankings,
    compute_yearly_indicator_summary,
    filter_observations,
    normalize_observations,
)


def test_transformations_pipeline_outputs_expected_metrics() -> None:
    raw = [
        {"INDICATOR": "I1", "REF_AREA": "USA", "TIME_PERIOD": "2020", "FREQ": "A", "OBS_VALUE": "100"},
        {"INDICATOR": "I1", "REF_AREA": "USA", "TIME_PERIOD": "2021", "FREQ": "A", "OBS_VALUE": "110"},
        {"INDICATOR": "I1", "REF_AREA": "GBR", "TIME_PERIOD": "2020", "FREQ": "A", "OBS_VALUE": "90"},
        {"INDICATOR": "I1", "REF_AREA": "GBR", "TIME_PERIOD": "2021", "FREQ": "A", "OBS_VALUE": "95"},
        {"INDICATOR": "I1", "REF_AREA": "WLD", "TIME_PERIOD": "2021", "FREQ": "Q", "OBS_VALUE": "999"},
        {"INDICATOR": "I1", "REF_AREA": "USA", "TIME_PERIOD": "2022", "FREQ": "A", "OBS_VALUE": "invalid"},
    ]

    normalized = normalize_observations(raw)
    filtered = filter_observations(normalized)
    yearly = compute_yearly_indicator_summary(filtered)
    growth = compute_country_growth(filtered)
    rankings = compute_latest_country_rankings(filtered, top_n=1)

    assert len(normalized) == 6
    assert len(filtered) == 4

    yearly_2020 = [row for row in yearly if row["year"] == 2020][0]
    assert yearly_2020["mean_value"] == 95.0
    assert yearly_2020["observation_count"] == 2

    assert len(growth) == 2
    usa_growth = [row for row in growth if row["ref_area"] == "USA"][0]
    assert usa_growth["absolute_change"] == 10.0
    assert usa_growth["percent_change"] == 10.0

    assert len(rankings) == 1
    assert rankings[0]["latest_year"] == 2021
    assert rankings[0]["top_countries"][0]["ref_area"] == "USA"
    assert rankings[0]["bottom_countries"][0]["ref_area"] == "GBR"


def test_filter_observations_applies_year_window() -> None:
    raw = [
        {"INDICATOR": "I1", "REF_AREA": "USA", "TIME_PERIOD": "2019", "FREQ": "A", "OBS_VALUE": "10"},
        {"INDICATOR": "I1", "REF_AREA": "USA", "TIME_PERIOD": "2020", "FREQ": "A", "OBS_VALUE": "11"},
        {"INDICATOR": "I1", "REF_AREA": "USA", "TIME_PERIOD": "2021", "FREQ": "A", "OBS_VALUE": "12"},
    ]
    filtered = filter_observations(normalize_observations(raw), min_year=2020, max_year=2020)
    assert len(filtered) == 1
    assert filtered[0]["time_period"] == 2020
