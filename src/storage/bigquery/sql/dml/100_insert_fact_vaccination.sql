-- ============================================================
-- Purpose: Populate the vaccination fact table from the staging table in BigQuery
-- Grain: 1 country / 1 date
-- ============================================================

MERGE `owid-covid-19-data-engineering.fact.fact_vaccination` T
USING (
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
    FROM `owid-covid-19-data-engineering.staging.stg_owid_covid` s
    JOIN `owid-covid-19-data-engineering.dim.dim_location` loc
        ON s.iso_code = loc.iso_code
    JOIN `owid-covid-19-data-engineering.dim.dim_date` dt
        ON s.date = dt.date_value
    WHERE s.iso_code IS NOT NULL
      AND s.date IS NOT NULL
) S
ON T.location_id = S.location_id AND T.date_id = S.date_id

WHEN MATCHED THEN
  UPDATE SET
    total_vaccinations = S.total_vaccinations,
    people_vaccinated = S.people_vaccinated,
    people_fully_vaccinated = S.people_fully_vaccinated,
    total_boosters = S.total_boosters,
    new_vaccinations = S.new_vaccinations,
    new_vaccinations_smoothed = S.new_vaccinations_smoothed,
    total_vaccinations_per_hundred = S.total_vaccinations_per_hundred,
    people_vaccinated_per_hundred = S.people_vaccinated_per_hundred,
    people_fully_vaccinated_per_hundred = S.people_fully_vaccinated_per_hundred,
    total_boosters_per_hundred = S.total_boosters_per_hundred
    
WHEN NOT MATCHED THEN
  INSERT (
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
  VALUES (
      S.location_id,
      S.date_id,
      S.total_vaccinations,
      S.people_vaccinated,
      S.people_fully_vaccinated,
      S.total_boosters,
      S.new_vaccinations,
      S.new_vaccinations_smoothed,
      S.total_vaccinations_per_hundred,
      S.people_vaccinated_per_hundred,
      S.people_fully_vaccinated_per_hundred,
      S.total_boosters_per_hundred
  )