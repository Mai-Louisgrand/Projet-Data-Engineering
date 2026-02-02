-- ============================================================
-- Purpose : Create dimension tables for Date and Location
-- ============================================================

-- ===========================
-- Dimension: Date
-- ===========================
-- Stores calendar information for analytical purposes: each row represents a single date
CREATE TABLE IF NOT EXISTS dim.dim_date (
    date_id        INT PRIMARY KEY,          -- YYYYMMDD
    date_value     DATE NOT NULL UNIQUE,     -- Actual date
    year           INT NOT NULL,
    quarter        INT NOT NULL,
    month          INT NOT NULL,
    month_name     VARCHAR(20) NOT NULL,
    week           INT NOT NULL,
    day            INT NOT NULL,
    day_of_week    INT NOT NULL,              -- 1 (Monday) - 7 (Sunday)
    day_name       VARCHAR(20) NOT NULL,
    is_weekend     BOOLEAN NOT NULL
);


-- ===========================
-- Dimension: Location
-- ===========================
-- Stores country-level location information: used for linking fact tables to geographies
CREATE TABLE IF NOT EXISTS dim.dim_location (
    location_id SERIAL PRIMARY KEY,
    iso_code    VARCHAR(10) UNIQUE,
    continent   VARCHAR(50),
    location    VARCHAR(100) NOT NULL       -- Full country/region name
);