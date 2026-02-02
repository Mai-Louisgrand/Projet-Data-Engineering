-- ============================================================
-- Purpose: Populate the date dimension independently of business data
-- Date range: 2019-01-01 -> 2030-12-31
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
    -- Surrogate key (YYYYMMDD)
    TO_CHAR(d, 'YYYYMMDD')::INT            AS date_id,
    
    -- Actual date
    d                                      AS date_value,
    
    -- Time components for analysis
    EXTRACT(YEAR FROM d)::INT              AS year,
    EXTRACT(QUARTER FROM d)::INT           AS quarter,
    EXTRACT(MONTH FROM d)::INT             AS month,
    TO_CHAR(d, 'Month')                    AS month_name,
    EXTRACT(WEEK FROM d)::INT              AS week,
    EXTRACT(DAY FROM d)::INT               AS day,
    EXTRACT(ISODOW FROM d)::INT            AS day_of_week,
    TO_CHAR(d, 'Day')                      AS day_name,
    
    -- Business indicator: weekend flag
    CASE
        WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN TRUE
        ELSE FALSE
    END                                   AS is_weekend

FROM generate_series(       -- PostgreSQL function generating a date sequence
    DATE '2019-01-01',
    DATE '2030-12-31',
    INTERVAL '1 day'
) AS d

-- If date_id already exists, ignore without raising an error
ON CONFLICT (date_id) DO NOTHING;
