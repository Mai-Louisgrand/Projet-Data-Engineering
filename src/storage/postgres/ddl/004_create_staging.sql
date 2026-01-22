-- ============================================================
-- Création de la table de staging pour les données OWID COVID
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.stg_owid_covid (
    iso_code TEXT,
    continent TEXT,
    location TEXT,
    date DATE,
    total_vaccinations BIGINT,
    people_vaccinated BIGINT,
    people_fully_vaccinated BIGINT,
    total_boosters BIGINT,
    new_vaccinations BIGINT,
    new_vaccinations_smoothed DOUBLE PRECISION,
    population BIGINT,
    total_vaccinations_per_hundred DOUBLE PRECISION,
    people_vaccinated_per_hundred DOUBLE PRECISION,
    people_fully_vaccinated_per_hundred DOUBLE PRECISION,
    total_boosters_per_hundred DOUBLE PRECISION,
    load_date DATE
);
