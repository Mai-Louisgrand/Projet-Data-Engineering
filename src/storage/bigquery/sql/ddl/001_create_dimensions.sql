-- ============================================================
-- Purpose : Create dimension tables for Date and Location (BigQuery)
-- ============================================================

-- ===========================
-- Dimension: Date
-- ===========================
CREATE TABLE IF NOT EXISTS `dim.dim_date` (
    date_id        STRING,          -- surrogate key as YYYYMMDD string
    date_value     DATE,            -- actual date
    year           INT64,
    quarter        INT64,
    month          INT64,
    month_name     STRING,
    week           INT64,
    day            INT64,
    day_of_week    INT64,           -- 1 (Monday) - 7 (Sunday)
    day_name       STRING,
    is_weekend     BOOL
    -- Note: BigQuery does not enforce PRIMARY KEY / UNIQUE constraints
);

-- ===========================
-- Dimension: Location
-- ===========================
CREATE TABLE IF NOT EXISTS `dim.dim_location` (
    location_id    STRING DEFAULT GENERATE_UUID(),
    iso_code       STRING,
    continent      STRING,
    location       STRING
    -- Note: BigQuery does not enforce PRIMARY KEY / UNIQUE constraints
);