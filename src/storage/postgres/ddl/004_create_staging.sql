-- ============================================================
-- Purpose: Create staging table for OWID COVID-19 data
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.stg_owid_covid (
    iso_code TEXT,
    continent TEXT,
    location TEXT,
    date DATE,

    -- Measures
    total_vaccinations BIGINT,
    people_vaccinated BIGINT,
    people_fully_vaccinated BIGINT,
    total_boosters BIGINT,
    new_vaccinations BIGINT,
    new_vaccinations_smoothed DOUBLE PRECISION,
    population BIGINT,
    total_vaccinations_per_hundred DOUBLE PRECISION,
    people_vaccinated_per_hundred DOUBLE PRECISION,
    people_fully_vaccinated_per_hundred DOUBLE PRECISION,
    total_boosters_per_hundred DOUBLE PRECISION,

    -- Metadata
    load_date DATE,
    
    CONSTRAINT uq_owid_iso_date UNIQUE (iso_code, date)
);
