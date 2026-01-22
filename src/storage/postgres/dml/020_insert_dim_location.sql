-- ============================================================
-- Remplissage de la dimension location
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
  AND iso_code NOT LIKE 'OWID_%'

-- si iso_code existe déjà, ignore et passe à la suite (sans ERREUR)
ON CONFLICT (iso_code) DO NOTHING;
