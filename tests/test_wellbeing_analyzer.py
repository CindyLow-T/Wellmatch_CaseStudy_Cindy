from wellbeing_pipeline.wellbeing_analyzer import WellbeingAnalyzer


def test_wellbeing_analyzer_summary_structure_and_pivot_correctness() -> None:
    analyzer = WellbeingAnalyzer()

    records = [
        {
            "indicator": "WB_WDI_SP_DYN_LE00_IN",
            "ref_area": "USA",
            "time_period": 2023,
            "obs_value": 77.0,
            "freq": "A",
        },
        {
            "indicator": "WB_WDI_SP_DYN_LE00_IN",
            "ref_area": "GBR",
            "time_period": 2023,
            "obs_value": 81.5,
            "freq": "A",
        },
        {
            "indicator": "WB_WDI_NY_GDP_PCAP_CD",
            "ref_area": "USA",
            "time_period": 2023,
            "obs_value": 76000.0,
            "freq": "A",
        },
        {
            "indicator": "WB_WDI_NY_GDP_PCAP_CD",
            "ref_area": "GBR",
            "time_period": 2023,
            "obs_value": 49000.0,
            "freq": "A",
        },
        {
            "indicator": "WB_WDI_NY_GDP_PCAP_CD",
            "ref_area": "USA",
            "time_period": 2024,
            "obs_value": 78000.0,
            "freq": "A",
        },
        {
            "indicator": "WB_WDI_NY_GDP_PCAP_CD",
            "ref_area": "GBR",
            "time_period": 2024,
            "obs_value": 50000.0,
            "freq": "A",
        },
    ]

    output = analyzer.analyze(
        processed_records=records,
        indicator_map={
            "life_expectancy": "WB_WDI_SP_DYN_LE00_IN",
            "gdp_per_capita": "WB_WDI_NY_GDP_PCAP_CD",
        },
        country_profiles={
            "USA": {"name": "United States", "role": "Benchmark"},
            "GBR": {"name": "United Kingdom", "role": "Primary"},
        },
        min_year=2023,
        max_year=2024,
        top_n=1,
    )

    assert set(output.keys()) == {
        "generated_at_utc",
        "scope",
        "row_counts",
        "analysis",
        "missingness_report",
        "notes",
    }
    assert set(output["analysis"].keys()) == {
        "yearly_indicator_summary",
        "country_growth",
        "latest_country_rankings",
    }
    assert output["scope"]["indicators"]["gdp_per_capita"]["code"] == "WB_WDI_NY_GDP_PCAP_CD"
    assert output["scope"]["indicators"]["gdp_per_capita"]["name"] == "GDP Per Capita"

    # "Pivot correctness" for grouped year summary:
    # GDP 2023 mean should be average(76000, 49000) = 62500.
    yearly = output["analysis"]["yearly_indicator_summary"]
    gdp_2023 = [
        row
        for row in yearly
        if row["indicator_code"] == "WB_WDI_NY_GDP_PCAP_CD" and row["year"] == 2023
    ][0]
    assert gdp_2023["observation_count"] == 2
    assert gdp_2023["mean_value"] == 62500.0
    assert gdp_2023["indicator_code"] == "WB_WDI_NY_GDP_PCAP_CD"
    assert gdp_2023["indicator_name"] == "GDP Per Capita"
    assert "indicator" not in gdp_2023

    # Ranking correctness for latest year (2024).
    rankings = output["analysis"]["latest_country_rankings"]
    gdp_ranking = [row for row in rankings if row["indicator_code"] == "WB_WDI_NY_GDP_PCAP_CD"][0]
    assert gdp_ranking["latest_year"] == 2024
    assert gdp_ranking["top_countries"][0]["ref_area"] == "USA"
    assert gdp_ranking["bottom_countries"][0]["ref_area"] == "GBR"
    assert gdp_ranking["indicator_code"] == "WB_WDI_NY_GDP_PCAP_CD"
    assert gdp_ranking["indicator_name"] == "GDP Per Capita"
    assert "indicator" not in gdp_ranking


def test_wellbeing_analyzer_missingness_imputation_policy() -> None:
    analyzer = WellbeingAnalyzer()

    records = [
        {
            "indicator": "WB_WDI_SP_DYN_LE00_IN",
            "ref_area": "USA",
            "time_period": 2023,
            "obs_value": 77.0,
            "freq": "A",
        },
        {
            "indicator": "WB_WDI_SP_DYN_LE00_IN",
            "ref_area": "GBR",
            "time_period": 2024,
            "obs_value": 82.0,
            "freq": "A",
        },
    ]

    output = analyzer.analyze(
        processed_records=records,
        indicator_map={"life_expectancy": "WB_WDI_SP_DYN_LE00_IN"},
        country_profiles={
            "USA": {"name": "United States", "role": "Benchmark"},
            "GBR": {"name": "United Kingdom", "role": "Primary"},
        },
        min_year=2023,
        max_year=2024,
        top_n=2,
    )

    usa_row = [row for row in output["missingness_report"] if row["country_code"] == "USA"][0]
    assert usa_row["missing_years"] == [2024]
    assert usa_row["latest_available_year"] == 2023
    assert usa_row["complete_for_requested_window"] is False
    assert usa_row["indicator_name"] == "Life Expectancy"
    # No imputation is performed; row count stays equal to observed records.
    assert output["row_counts"]["processed_rows"] == 2
    assert any("Treat 2024 as latest available where present" in note for note in output["notes"])
