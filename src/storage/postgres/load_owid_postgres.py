'''
OWID COVID-19 Vaccination Data Loader – Parquet to PostgreSQL Staging

This script performs the following tasks:
- Reads transformed and partitioned Parquet files by country (iso_code)
- Enriches data with technical metadata (load date)
- Loads the batch dataset into a PostgreSQL staging table via JDBC
'''

import logging
import psycopg2
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
# Main function
# ============================
def run_load(
    parquet_path: str = "data/processed/owid_covid",
    pg_host: str = "localhost",
    pg_port: str = "5432",
    pg_db: str = "covid_dw",
    pg_user: str = "data",
    pg_password: str = "data",
    pg_table: str = "staging.stg_owid_covid"
):
    '''
    Load transformed OWID COVID-19 vaccination data into PostgreSQL staging table.

    :param parquet_path: Path to transformed Parquet files
    :param pg_host: PostgreSQL host
    :param pg_port: PostgreSQL port
    :param pg_db: PostgreSQL database name
    :param pg_user: PostgreSQL username
    :param pg_password: PostgreSQL password
    :param pg_table: Target staging table in PostgreSQL
    '''

    # Initialize Spark session with PostgreSQL JDBC driver
    spark = (
        SparkSession.builder
        .appName("Load OWID COVID Parquet to PostgreSQL Staging")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
        )

    logger.info("Session Spark initialisée")

    try:
        # ============================
        # Load Parquet files
        # ============================
        logger.info("Lecture des fichiers Parquet de data/processed")
        df = spark.read.parquet(parquet_path) #lit tous les fichiers Parquet sous PARQUET_PATH, en prenant en compte les partitions

        logger.info(f"Nombre de lignes lues : {df.count()}")

        KEY_COLUMNS = ["iso_code", "date"]
        df = df.dropDuplicates(KEY_COLUMNS) # Deduplicate data based on unique keys
        df = df.withColumn("load_date", lit(date.today())) # Add load date metadata for auditing
        df = df.repartition(4) # Repartition to optimize JDBC write performance

        # ============================
        # Truncate staging table before loading
        # ============================
        logger.info(f"Vider la table {pg_table} avant insertion")
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_password
        )
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {pg_table};")
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Table {pg_table} vidée avec succès")

        # ============================
        # Write to PostgreSQL staging table
        # ============================
        logger.info("Chargement des données dans la table de staging PostgreSQL")
        (
        df.write
            .format("jdbc")
            .option("url", f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}")
            .option("dbtable", pg_table)
            .option("user", pg_user)
            .option("password", pg_password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
                )

        logger.info("Chargement des données terminé avec succès")

    except Exception as e:
        logger.error("Erreur lors du chargement des données dans PostgreSQL", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("Session Spark arrêtée")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    run_load()