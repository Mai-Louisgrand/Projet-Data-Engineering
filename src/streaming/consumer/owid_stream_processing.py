'''
Spark Structured Streaming consumer (Kafka -> BigQuery staging_tmp)

Responsibilities:
 - Consume vaccination events from Kafka
 - Deserialize JSON messages using an explicit schema
 - Transform streaming data to match the shared batch/streaming staging model
 - Deduplicate records at micro-batch level
 - Idempotent writes to BigQuery staging_tmp
'''

import logging
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql import DataFrame
from google.cloud import bigquery

from src.utils.spark import get_spark
from src.streaming.config.kafka_config import OWID_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from src.config.settings import GCP_PROJECT, BQ_DATASET_STAGING, GCS_BUCKET_NAME, LOG_FORMAT

# ============================
# Logging Configuration
# ============================
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# ============================
# BigQuery staging table
# ============================
STAGING_TABLE_BQ = f"{GCP_PROJECT}.{BQ_DATASET_STAGING}.stg_owid_covid_tmp"

# ============================
# Explicit schema for Kafka events
# ============================
event_schema = (
    StructType()
    .add("event_type", StringType())
    .add("event_time", StringType())
    .add("country_code", StringType())
    .add("country", StringType())
    .add("people_vaccinated", DoubleType())
    .add("people_fully_vaccinated", DoubleType())
    .add("total_vaccinations", DoubleType())
)

# ============================
# Helper functions
# ============================
def transform_batch(batch_df: DataFrame) -> DataFrame:
    '''
    Transform a Spark micro-batch to match BigQuery staging_tmp schema

    This transformation:
    - Renames and casts streaming fields to staging-compatible columns
    - Completes missing fields with NULL values
    - Adds load metadata
    - Deduplicates records at micro-batch level based on (date, iso_code)

    :param batch_df: Input Spark DataFrame representing a streaming micro-batch
    :return: Transformed DataFrame ready for PostgreSQL ingestion 
    '''
    return (
        batch_df
        .select(
            F.col("country_code").cast("string").alias("iso_code"),
            F.lit(None).cast("string").alias("continent"),
            F.col("country").cast("string").alias("location"),
            F.to_date("event_time", "yyyy-MM-dd").alias("date"),
            F.when(~F.isnan(F.col("total_vaccinations")), F.col("total_vaccinations").cast("long")).otherwise(None).alias("total_vaccinations"),
            F.when(~F.isnan(F.col("people_vaccinated")), F.col("people_vaccinated").cast("long")).otherwise(None).alias("people_vaccinated"),
            F.when(~F.isnan(F.col("people_fully_vaccinated")), F.col("people_fully_vaccinated").cast("long")).otherwise(None).alias("people_fully_vaccinated"),

            # Fields not provided by streaming are explicitly set to NULL
            F.lit(None).cast("long").alias("total_boosters"),
            F.lit(None).cast("long").alias("new_vaccinations"),
            F.lit(None).cast("double").alias("new_vaccinations_smoothed"),
            F.lit(None).cast("long").alias("population"),
            F.lit(None).cast("double").alias("total_vaccinations_per_hundred"),
            F.lit(None).cast("double").alias("people_vaccinated_per_hundred"),
            F.lit(None).cast("double").alias("people_fully_vaccinated_per_hundred"),
            F.lit(None).cast("double").alias("total_boosters_per_hundred"),
            F.current_date().alias("load_date")
        )
        # Micro-batch deduplication based on (date, iso_code)
        .dropDuplicates(["date", "iso_code"])
    )

def truncate_staging_table():
    '''
    Truncate temporary staging table before streaming load.
    '''
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        table_id = f"{GCP_PROJECT}.{BQ_DATASET_STAGING}.stg_owid_covid_tmp"
        
        query = f"TRUNCATE TABLE `{table_id}`"
        
        logger.info(f"Nettoyage de la table {table_id}...")
        client.query(query).result()
        logger.info("Table de staging vidée avec succès.")
        
    except Exception as e:
        logger.error(f"Erreur lors du truncate de la table : {e}")
        raise

def write_batch_to_bq(batch_df: DataFrame, epoch_id: int) -> None:
    '''
    Write a Spark micro-batch into BigQuery staging_tmp

    :param batch_df: Spark DataFrame for the current micro-batch
    :param epoch_id: Structured Streaming epoch identifier
    '''
    # Skip empty micro-batches
    if batch_df.count() == 0:
        logger.info("Epoch %s : batch vide, aucun enregistrement à écrire", epoch_id)
        return

    df_to_write = transform_batch(batch_df)

    df_to_write.write \
        .format("bigquery") \
        .option("table", STAGING_TABLE_BQ) \
        .option("temporaryGcsBucket", GCS_BUCKET_NAME) \
        .option("parentProject", GCP_PROJECT) \
        .mode("append") \
        .save()

# ============================
# Consumer
# ============================
def run_consumer(checkpoint_dir: Path = Path("/opt/airflow/data/checkpoints/owid_stream")):
    '''
    Spark Structured Streaming consumer : Kafka → BigQuery staging_tmp

    :param checkpoint_dir: Directory used by Spark to persist streaming checkpoints
    '''
    # Clean temporary staging table before loading new data
    truncate_staging_table()

    # Initialize Spark session    
    spark = get_spark("OWID_COVID_Streaming")
    spark.sparkContext.setLogLevel("WARN")

    # Read events from Kafka
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", OWID_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    # Deserialize JSON messages into structured columns
    df_parsed = (
        df_raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", event_schema).alias("data"))
        .select("data.*")
    )
    # Add event-time column and watermark for late data handling
    df_with_event_time = (
        df_parsed
        .withColumn("event_timestamp", F.to_timestamp("event_time", "yyyy-MM-dd"))
        .withWatermark("event_timestamp", "7 days")
    )

    # ============================
    # Streaming execution
    # ============================
    query = (
        df_with_event_time.writeStream
        .foreachBatch(write_batch_to_bq)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_dir.resolve()))
        .start()
    )

    logger.info("Job Spark Structured Streaming OWID démarré")
    
    # Process all currently available micro-batches and stop
    query.processAllAvailable()
    logger.info("Micro-batchs traités, consumer stop")
    query.stop()

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    run_consumer()