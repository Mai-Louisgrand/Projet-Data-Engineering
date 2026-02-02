'''
OWID COVID-19 Vaccination Data Loader – Parquet to PostgreSQL Staging

This script performs the following tasks:
- Reads transformed and partitioned Parquet files by country (iso_code)
- Enriches data with technical metadata (load date)
- Loads the batch dataset into a PostgreSQL staging table via JDBC
'''

import logging
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# ============================
# Logging setup
# ============================
logging.basicConfig(
    level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
        )
logger = logging.getLogger(__name__)

# ============================
# Configuration
# ============================
# PostgreSQL parameters
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "covid_dw"
PG_USER = "data"
PG_PASSWORD = "data"
PG_TABLE = "staging.stg_owid_covid"

# Parquet folder
PARQUET_PATH = "data/processed/owid_covid"

# Spark session
spark = (
    SparkSession.builder
    .appName("Load OWID COVID Parquet to PostgreSQL Staging")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
    )

logger.info("Session Spark initialisée")

# ============================
# Load Parquet data and write to PostgreSQL staging
# ============================
try:
    logger.info("Lecture des fichiers Parquet de data/processed")
    df = spark.read.parquet(PARQUET_PATH) # Read all Parquet files under PARQUET_PATH, including partitions
    logger.info(f"Nombre de lignes lues : {df.count()}")

    df = df.withColumn("load_date", lit(date.today()))  # Add load metadata column
    df = df.repartition(4) # Optimize JDBC write by repartitioning: set number of partitions to avoid overloading PostgreSQL

    # Write to PostgreSQL staging table
    logger.info("Chargement des données dans la table de staging PostgreSQL")
    (
    df.write
        .format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", PG_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite") # Drop and recreate table for insert
        .save()
    )

    logger.info("Chargement des données terminé avec succès")

except Exception as e:
    logger.error("Erreur lors du chargement des données dans PostgreSQL", exc_info=True)
    raise

finally:
    spark.stop()
    logger.info("Session Spark arrêtée")


