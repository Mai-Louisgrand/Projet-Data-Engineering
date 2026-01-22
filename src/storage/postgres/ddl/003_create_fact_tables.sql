-- ============================================================
-- Objectif : créer table de faits pour la vaccination
-- ============================================================

CREATE TABLE IF NOT EXISTS fact.fact_vaccination (
    vaccination_id SERIAL PRIMARY KEY,

    -- Foreign Keys
    date_id     INT NOT NULL,
    location_id INT NOT NULL,

    -- Measures
    total_vaccinations          BIGINT,
    people_vaccinated           BIGINT,
    people_fully_vaccinated     BIGINT,
    total_boosters              BIGINT,
    new_vaccinations            BIGINT,
    new_vaccinations_smoothed DOUBLE PRECISION,
    
    total_vaccinations_per_hundred DOUBLE PRECISION,
    people_vaccinated_per_hundred DOUBLE PRECISION,
    people_fully_vaccinated_per_hundred DOUBLE PRECISION,
    total_boosters_per_hundred DOUBLE PRECISION,

    -- Constraints
    CONSTRAINT fk_date
        FOREIGN KEY (date_id)
        REFERENCES dim.dim_date(date_id), -- assurer que les valeurs date_id de cette table existe dans dim_date

    CONSTRAINT fk_location
        FOREIGN KEY (location_id)
        REFERENCES dim.dim_location(location_id)
);
