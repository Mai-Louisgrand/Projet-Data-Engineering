'''
OWID COVID-19 Data Transformation – Vaccination

This script performs the following tasks:
- Cleans and structures country-level vaccination data.
- Recalculates normalized indicators per 100 inhabitants.
- Writes processed data as Parquet files per country in GCS, ready for downstream analytical processing or ingestion.
'''

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from src.config.settings import RAW_PREFIX, INGESTION_DATE, GCS_BUCKET_NAME, PROCESSED_PATH
from src.utils.spark import get_spark
from src.utils.logging import setup_logging

# Logging configuration
logger = setup_logging()

# ============================
# Helper functions
# ============================
def per_hundred(metric_col: str):
    '''
    Compute normalized indicator per 100 inhabitants.
    Handles division by zero and preserves null values.

    :param metric_col: Column name containing the metric to normalize
    :return: Column expression with normalized values
    '''
    return (
        F.when(
            (F.col(metric_col).isNotNull()) &
            (F.col("population").isNotNull()) &
            (F.col("population") > 0),
            (F.col(metric_col) / F.col("population")) * 100
        )
        .otherwise(None)
        .cast(DoubleType())
    )

# ============================
# Main function
# ============================
def run_transformation():
    '''
    OWID COVID-19 Vaccination Data Transformation

    This function:
    - Loads raw CSV data from GCS bucket,
    - Cleans and structures the dataset,
    - Computes normalized indicators per 100 inhabitants,
    - Writes processed data as partitioned Parquet files by country.

    :param raw_path: Path to the raw CSV files
    '''
    logger.info("Démarrage de la transformation OWID")

    output_path = PROCESSED_PATH

    spark = get_spark("OWID_COVID_Transformation")

    # ============================
    # Read RAW data from GCS
    # ============================
    gcs_path = (
        f"gs://{GCS_BUCKET_NAME}/{RAW_PREFIX}/"
        f"ingestion_date={INGESTION_DATE}/owid_covid_data.csv"
    )
    logger.info(f"Lecture des données depuis GCS : {gcs_path}")

    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(gcs_path)
        )
    except Exception as e:
        logger.exception(f"Erreur lecture GCS : {gcs_path}")
        raise

    # ============================
    # Transformations
    # ============================
    # Columns required for record identification
    CRITICAL_COLUMNS = ["iso_code", "continent", "location", "date"]
    # Business-related columns
    BUSINESS_COLUMNS = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters", "new_vaccinations", "new_vaccinations_smoothed", "population"]
    # Numeric columns requiring consistency checks
    NUMERIC_COLUMNS = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters", "new_vaccinations", "new_vaccinations_smoothed", "population"]

    logger.info("Début des transformations")
    # 1. Filter critical rows
    df = (
        df 
        .filter(F.col("iso_code").isNotNull())
        .filter(F.col("location").isNotNull())
        .filter(F.col("date").isNotNull())
    )
    logger.info("Filtrage des lignes critiques effectué")

    # 2. Exclude OWID aggregate codes
    df = df.filter(~F.col("iso_code").startswith("OWID_"))
    logger.info("Filtrage des agrégats OWID effectué")

    # 3. Reduce schema to relevant columns
    df = df.select(*(CRITICAL_COLUMNS + BUSINESS_COLUMNS))
    logger.info("Sélection des colonnes métiers")

    # 4. Clean numeric columns
    # Assumptions:
    # - Vaccination metrics cannot be negative
    # - Null values are preserved
    for col_name in NUMERIC_COLUMNS:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) < 0, None).otherwise(F.col(col_name))
        )
    logger.info("Nettoyage des colonnes numériques (valeurs négatives)")

    # 5. Compute normalized indicators per 100 inhabitants
    df = (
        df
        .withColumn(
            "total_vaccinations_per_hundred",
            per_hundred("total_vaccinations")
        )
        .withColumn(
            "people_vaccinated_per_hundred",
            per_hundred("people_vaccinated")
        )
        .withColumn(
            "people_fully_vaccinated_per_hundred",
            per_hundred("people_fully_vaccinated")
        )
        .withColumn(
            "total_boosters_per_hundred",
            per_hundred("total_boosters")
        )
    )
    logger.info("Calcul des indicateurs normalisés")

    # ============================
    # Write processed data
    # ============================
    logger.info(f"Écriture des données transformées : {output_path}")

    try:
        (
            df
            .repartition(4) # Repartition for Spark optimization
            .write
            .mode("overwrite")
            .parquet(output_path)
        )
    except Exception as e:
        logger.exception(f"Erreur écriture données : {output_path}")
        raise

    logger.info(f"Écriture des données transformées : {output_path}")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    run_transformation()