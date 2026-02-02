-- ============================================================
-- Purpose: Populate the vaccination fact table
-- Grain: 1 country / 1 date
-- ============================================================

INSERT INTO fact.fact_vaccination (
    location_id,
    date_id,
    total_vaccinations,
    people_vaccinated,
    people_fully_vaccinated,
    total_boosters,
    new_vaccinations,
    new_vaccinations_smoothed,
    total_vaccinations_per_hundred,
    people_vaccinated_per_hundred,
    people_fully_vaccinated_per_hundred,
    total_boosters_per_hundred
)
SELECT
    loc.location_id,
    dt.date_id,

    s.total_vaccinations,
    s.people_vaccinated,
    s.people_fully_vaccinated,
    s.total_boosters,
    s.new_vaccinations,
    s.new_vaccinations_smoothed,
    s.total_vaccinations_per_hundred,
    s.people_vaccinated_per_hundred,
    s.people_fully_vaccinated_per_hundred,
    s.total_boosters_per_hundred

FROM staging.stg_owid_covid s

JOIN dim.dim_location loc
    ON s.iso_code = loc.iso_code

JOIN dim.dim_date dt
    ON s.date = dt.date_value

WHERE s.iso_code IS NOT NULL
  AND s.date IS NOT NULL;
