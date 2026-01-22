-- ============================================================
-- Remplissage de la dimension date (génération indépendante des données métiers)
-- Plage : 2019-01-01 -> 2030-12-31
-- ============================================================

INSERT INTO dim.dim_date (
    date_id, 
    date_value,
    year,
    quarter,
    month,
    month_name,
    week,
    day,
    day_of_week,
    day_name,
    is_weekend
)
SELECT
    -- Clé de substitution 
    TO_CHAR(d, 'YYYYMMDD')::INT            AS date_id,
    
    -- Valeur date réelle
    d                                      AS date_value,
    
    -- Composants temporels pour analyses
    EXTRACT(YEAR FROM d)::INT              AS year,
    EXTRACT(QUARTER FROM d)::INT           AS quarter,
    EXTRACT(MONTH FROM d)::INT             AS month,
    TO_CHAR(d, 'Month')                    AS month_name,
    EXTRACT(WEEK FROM d)::INT              AS week,
    EXTRACT(DAY FROM d)::INT               AS day,
    EXTRACT(ISODOW FROM d)::INT            AS day_of_week,
    TO_CHAR(d, 'Day')                      AS day_name,
    -- Indicateur business utile (week-end)
    CASE
        WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN TRUE
        ELSE FALSE
    END                                   AS is_weekend

FROM generate_series(       -- fonction PostgreSQL générant une suite de dates
    DATE '2019-01-01',      -- date de début
    DATE '2030-12-31',      -- date de fin
    INTERVAL '1 day'        -- incrément quotidien
) AS d

-- si date_id existe déjà, ignore et passe à la suite (sans ERREUR)
ON CONFLICT (date_id) DO NOTHING;
