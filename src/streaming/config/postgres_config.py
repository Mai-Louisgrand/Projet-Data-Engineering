# Configuration PostgreSQL pour le pipeline streaming OWID

# Paramètres de connection
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "user": "data",
    "password": "data",
    "database": "covid_dw",
}

# Table staging pour écriture des données de streaming
STAGING_TABLE = "staging.stg_owid_covid"
