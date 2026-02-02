-- ============================================================
-- Purpose: Populate the location dimension from staging data
-- ============================================================

INSERT INTO dim.dim_location (
    iso_code,
    continent,
    location
)
SELECT DISTINCT
    iso_code,
    continent,
    location
FROM staging.stg_owid_covid
WHERE iso_code IS NOT NULL
  AND location IS NOT NULL
  AND iso_code NOT LIKE 'OWID_%' -- exclude aggregate rows

-- If iso_code already exists, ignore without raising an error
ON CONFLICT (iso_code) DO NOTHING;
