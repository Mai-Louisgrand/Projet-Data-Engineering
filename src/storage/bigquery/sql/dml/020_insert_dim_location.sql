-- ============================================================
-- Purpose: Populate the location dimension from staging data in BigQuery
-- ============================================================

MERGE `owid-covid-19-data-engineering.dim.dim_location` T
USING (
    SELECT DISTINCT
        iso_code,
        continent,
        location
    FROM `owid-covid-19-data-engineering.staging.stg_owid_covid`
    WHERE iso_code IS NOT NULL
      AND location IS NOT NULL
      AND iso_code NOT LIKE 'OWID_%'  -- exclude aggregate rows
) S
ON T.iso_code = S.iso_code

WHEN NOT MATCHED THEN
  INSERT (iso_code, continent, location)
  VALUES (S.iso_code, S.continent, S.location)