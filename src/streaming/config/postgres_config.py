'''
PostgreSQL configuration for the OWID streaming pipeline
'''

# Connection parameters for the PostgreSQL database
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "user": "data",
    "password": "data",
    "database": "covid_dw",
}

# Target staging table for writing streaming data
STAGING_TABLE = "staging.stg_owid_covid"
