-- ============================================================
-- Purpose : Create fact table for COVID-19 vaccination (BigQuery)
-- ============================================================

CREATE TABLE IF NOT EXISTS `fact.fact_vaccination` (
    vaccination_id STRING DEFAULT GENERATE_UUID(),

    -- Foreign keys (documentary only)
    date_id     STRING NOT NULL,
    location_id STRING NOT NULL,

    -- Measures
    total_vaccinations          INT64,
    people_vaccinated           INT64,
    people_fully_vaccinated     INT64,
    total_boosters              INT64,
    new_vaccinations            INT64,
    new_vaccinations_smoothed   FLOAT64,
    
    total_vaccinations_per_hundred   FLOAT64,
    people_vaccinated_per_hundred    FLOAT64,
    people_fully_vaccinated_per_hundred FLOAT64,
    total_boosters_per_hundred       FLOAT64
);