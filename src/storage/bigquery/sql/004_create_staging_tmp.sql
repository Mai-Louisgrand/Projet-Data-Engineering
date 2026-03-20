-- ============================================================
-- Purpose: Temporary staging table for GCS → BigQuery load (pre-merge)
-- ============================================================

-- NOTE: This table is fully refreshed at each run
CREATE TABLE IF NOT EXISTS `staging.stg_owid_covid_tmp` (
    iso_code STRING,
    continent STRING,
    location STRING,
    date DATE,

    -- Measures
    total_vaccinations BIGINT,
    people_vaccinated BIGINT,
    people_fully_vaccinated BIGINT,
    total_boosters BIGINT,
    new_vaccinations BIGINT,
    new_vaccinations_smoothed FLOAT64,
    population BIGINT,
    total_vaccinations_per_hundred FLOAT64,
    people_vaccinated_per_hundred FLOAT64,
    people_fully_vaccinated_per_hundred FLOAT64,
    total_boosters_per_hundred FLOAT64,

    -- Metadata
    load_date DATE
);