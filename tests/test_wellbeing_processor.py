from wellbeing_pipeline.wellbeing_processor import WellbeingProcessor


def test_wellbeing_processor_cleans_types_and_enriches_records() -> None:
    processor = WellbeingProcessor()
    raw_records = [
        {
            "INDICATOR": "WB_WDI_NY_GDP_PCAP_CD",
            "REF_AREA": "GBR",
            "TIME_PERIOD": "2023",
            "FREQ": "A",
            "OBS_VALUE": "48866.31",
            "CONFIG_KEY": "gdp_per_capita",
        }
    ]

    output = processor.process(
        raw_records=raw_records,
        country_profiles={"GBR": {"name": "United Kingdom", "role": "Primary focus"}},
        min_year=2018,
        max_year=2024,
    )

    assert len(output["normalized"]) == 1
    assert len(output["filtered"]) == 1

    row = output["filtered"][0]
    assert isinstance(row["time_period"], int)
    assert isinstance(row["obs_value"], float)
    assert row["config_key"] == "gdp_per_capita"
    assert row["country_name"] == "United Kingdom"
    assert row["country_role"] == "Primary focus"


def test_wellbeing_processor_handles_missing_or_invalid_records() -> None:
    processor = WellbeingProcessor()
    raw_records = [
        {
            "INDICATOR": "WB_WDI_SP_DYN_LE00_IN",
            "REF_AREA": "GBR",
            "TIME_PERIOD": "2023",
            "FREQ": "A",
            "OBS_VALUE": "81.9",
            "CONFIG_KEY": "life_expectancy",
        },
        {
            "INDICATOR": "WB_WDI_SP_DYN_LE00_IN",
            "REF_AREA": "GBR",
            "TIME_PERIOD": "2024",
            "FREQ": "A",
            "OBS_VALUE": None,
            "CONFIG_KEY": "life_expectancy",
        },
        {
            "INDICATOR": "WB_WDI_SP_DYN_LE00_IN",
            "REF_AREA": "GBR",
            "TIME_PERIOD": "bad-year",
            "FREQ": "A",
            "OBS_VALUE": "82.0",
            "CONFIG_KEY": "life_expectancy",
        },
        {
            "INDICATOR": "WB_WDI_SP_DYN_LE00_IN",
            "REF_AREA": "GBR",
            "TIME_PERIOD": "2023",
            "FREQ": "Q",
            "OBS_VALUE": "81.8",
            "CONFIG_KEY": "life_expectancy",
        },
        {
            "INDICATOR": "WB_WDI_SP_DYN_LE00_IN",
            "REF_AREA": "FRA",
            "TIME_PERIOD": "2023",
            "FREQ": "A",
            "OBS_VALUE": "82.5",
            "CONFIG_KEY": "life_expectancy",
        },
    ]

    output = processor.process(
        raw_records=raw_records,
        country_profiles={"GBR": {"name": "United Kingdom", "role": "Primary focus"}},
        min_year=2018,
        max_year=2024,
    )

    assert len(output["normalized"]) == 5
    assert len(output["filtered"]) == 1
    assert output["filtered"][0]["ref_area"] == "GBR"
    assert output["filtered"][0]["time_period"] == 2023
