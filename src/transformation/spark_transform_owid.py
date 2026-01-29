'''
OWID COVID-19 Data Transformation – Vaccination

This script performs the following tasks:
- Cleans and structures country-level vaccination data.
- Recalculates normalized indicators per 100 inhabitants.
- Writes processed data as partitioned Parquet files per country, ready for downstream analytical processing or ingestion.
'''

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ============================
# Helper functions
# ============================
def create_processed_dir() -> Path:
    '''
    Create the 'processed' directory to store transformed data if it does not exist.
    
    :return: Path to the processed data directory
    '''
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    processed_dir = PROJECT_ROOT / "data" / "processed" / "owid_covid"
    processed_dir.mkdir(parents=True, exist_ok=True)
    return processed_dir

def per_hundred(metric_col: str):
    '''
    Compute normalized indicator per 100 inhabitants.
    Handles division by zero and preserves null values.

    :param metric_col: Column name containing the metric to normalize
    :return: Column expression with normalized values
    '''
    # 
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
def run_transformation(raw_path: str = "data/raw/owid_covid/"):
    """
    OWID COVID-19 Vaccination Data Transformation

    This function:
    - Loads raw CSV data,
    - Cleans and structures the dataset,
    - Computes normalized indicators per 100 inhabitants,
    - Writes processed data as partitioned Parquet files by country.

    :param raw_path: Path to the raw CSV files
    """
    processed_dir = create_processed_dir()
    output_path = str(processed_dir)

    spark = (
        SparkSession.builder
        .appName("OWID_COVID_Transformation")
        .getOrCreate()
    )

    # ============================
    # Read RAW data
    # ============================
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )

    # ============================
    # Transformations
    # ============================
    # Columns required for record identification
    CRITICAL_COLUMNS = ["iso_code", "continent", "location", "date"]
    # Business-related columns
    BUSINESS_COLUMNS = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters", "new_vaccinations", "new_vaccinations_smoothed", "population"]
    # Numeric columns requiring consistency checks
    NUMERIC_COLUMNS = ["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "total_boosters", "new_vaccinations", "new_vaccinations_smoothed", "population"]


    # 1. Filter critical rows
    df = (
        df 
        .filter(F.col("iso_code").isNotNull())
        .filter(F.col("location").isNotNull())
        .filter(F.col("date").isNotNull())
    )

    # 2. Exclude OWID aggregate codes
    df = df.filter(~F.col("iso_code").startswith("OWID_"))

    # 3. Reduce schema to relevant columns
    df = df.select(*(CRITICAL_COLUMNS + BUSINESS_COLUMNS))

    # 4. Clean numeric columns
    # Assumptions:
    # - Vaccination metrics cannot be negative
    # - Null values are preserved
    for col_name in NUMERIC_COLUMNS:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) < 0, None).otherwise(F.col(col_name))
        )

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

    # ============================
    # Write processed data
    # ============================
    (
        df
        .repartition("iso_code") # Repartition by country code for Spark optimization
        .write
        .mode("overwrite")
        .partitionBy("iso_code") # One folder per country
        .parquet(output_path)
    )

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    run_transformation()