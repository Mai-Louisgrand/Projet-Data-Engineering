-- ============================================================
-- Purpose: Populate the dim_date table independently of business data
-- ============================================================

INSERT INTO `dim.dim_date` (
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
    FORMAT_DATE('%Y%m%d', d) AS date_id,
    d AS date_value,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(MONTH FROM d) AS month,
    FORMAT_DATE('%B', d) AS month_name,
    EXTRACT(WEEK FROM d) AS week,
    EXTRACT(DAY FROM d) AS day,
    EXTRACT(DAYOFWEEK FROM d) AS day_of_week,  -- 1=Sunday .. 7=Saturday
    FORMAT_DATE('%A', d) AS day_name,
    CASE WHEN EXTRACT(DAYOFWEEK FROM d) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2019-01-01', DATE '2030-12-31')) AS d
WHERE NOT EXISTS (
    SELECT 1 FROM `dim.dim_date` dd
    WHERE dd.date_id = FORMAT_DATE('%Y%m%d', d)
)