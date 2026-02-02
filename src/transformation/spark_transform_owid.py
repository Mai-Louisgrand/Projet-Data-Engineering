'''
OWID COVID-19 Data Transformation – Vaccination

This script performs the following tasks:
- Cleans and structures country-level vaccination data.
- Recalculates normalized indicators per 100 inhabitants.
- Writes processed data as partitioned Parquet files per country, ready for downstream analytical processing or ingestion.
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ============================
# Configuration
# ============================
RAW_PATH = "data/raw/owid_covid/"
OUTPUT_PATH = "data/processed/owid_covid/"

# Initialize Spark session
spark = (
    SparkSession.builder
    .appName("OWID_COVID_Transformation")
    .getOrCreate()
)

# ============================
# Column definitions
# ============================

# Columns required for record identification
CRITICAL_COLUMNS = [
    "iso_code",
    "continent",
    "location",
    "date"
]

# Business-related columns
BUSINESS_COLUMNS = [
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "total_boosters",
    "new_vaccinations",
    "new_vaccinations_smoothed",
    "population"
]

# Numeric columns requiring consistency checks
NUMERIC_COLUMNS = [
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "total_boosters",
    "new_vaccinations",
    "new_vaccinations_smoothed",
    "population"
]

# ============================
# Read raw data
# ============================
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RAW_PATH)
)

# ============================
# Transformations
# ============================

# 1. Filter critical rows (records with essential identifiers)
df = (
    df
    .filter(F.col("iso_code").isNotNull())
    .filter(F.col("location").isNotNull())
    .filter(F.col("date").isNotNull())
)

# 2. Exclude OWID aggregates
df = df.filter(~F.col("iso_code").startswith("OWID_"))

# 3. Reduce schema to relevant columns
df = df.select(*(CRITICAL_COLUMNS + BUSINESS_COLUMNS))

# 4. Preventive cleaning of numeric values
# Assumptions:
# - Vaccination metrics cannot be negative
# - Null values are preserved
for col_name in NUMERIC_COLUMNS:
    df = df.withColumn(
        col_name,
        F.when(F.col(col_name) < 0, None).otherwise(F.col(col_name))
    )

# 5. Recalculate normalized indicators per 100 inhabitants
def per_hundred(metric_col: str):
    '''
    Compute normalized indicator per 100 inhabitants.
    
    :param metric_col: Column name containing metric to normalize
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
    # Repartition by country code for Spark optimization
    .repartition("iso_code")  

     # Write Parquet files, one folder per country
    .write
    .mode("overwrite")
    .partitionBy("iso_code")
    .parquet(OUTPUT_PATH)
)
