-- ============================================================
-- Objectif : créer tables de dimensions Date et Location
-- ============================================================

-- Dimension Date
CREATE TABLE IF NOT EXISTS dim.dim_date (
    date_id        INT PRIMARY KEY,          -- YYYYMMDD
    date_value     DATE NOT NULL UNIQUE,
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


-- Dimension Location
CREATE TABLE IF NOT EXISTS dim.dim_location (
    location_id SERIAL PRIMARY KEY,
    iso_code    VARCHAR(10) UNIQUE,
    continent   VARCHAR(50),
    location    VARCHAR(100) NOT NULL
);