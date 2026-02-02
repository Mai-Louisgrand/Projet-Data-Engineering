'''
Spark Structured Streaming job for OWID COVID-19 vaccination events

Responsibilities:
 - Consume vaccination events from Kafka
 - Deserialize JSON messages using an explicit schema
 - Transform streaming data to match the shared batch/streaming staging model
 - Deduplicate records at micro-batch level
 - Idempotent writes into PostgreSQL staging via UPSERT logic
'''

import logging
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql import DataFrame

from src.streaming.config.kafka_config import OWID_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from src.streaming.config.spark_config import get_spark_session
from src.streaming.config.postgres_config import STAGING_TABLE, POSTGRES_CONFIG
from src.streaming.postgres_writer import get_postgres_connection, write_dataframe_to_postgres, upsert_from_temp_table

# ============================
# Configuration
# ============================
# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# PostgreSQL JDBC configuration
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
POSTGRES_JDBC_PROPERTIES = {
    "user": POSTGRES_CONFIG["user"],
    "password": POSTGRES_CONFIG["password"],
    "driver": "org.postgresql.Driver"
}

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
    Transform a Spark micro-batch to match the shared PostgreSQL staging table
    used by both batch and streaming pipelines.

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

def write_batch_to_postgres(batch_df: DataFrame, epoch_id: int) -> None:
    '''
    Write a Spark Structured Streaming micro-batch into the PostgreSQL staging table.

    Processing steps:
    - Skip empty micro-batches
    - Transform data to match PostgreSQL staging schema
    - Write data into a temporary table via Spark JDBC
    - UPSERT records into the shared batch/streaming staging table
    - Ensure idempotent writes using conflict resolution

    :param batch_df: Spark DataFrame for the current micro-batch
    :param epoch_id: Structured Streaming epoch identifier
    '''
    # Skip empty micro-batches
    if batch_df.count() == 0:
        logger.info("Epoch %s : batch vide, aucun enregistrement à écrire", epoch_id)
        return

    df_to_write = transform_batch(batch_df) # Transform data to match PostgreSQL schema
    
    temp_table = "staging.owid_covid_tmp" # Temporary table used to enable UPSERT logic

    # Drop and recreate temporary table to ensure a clean micro-batch load
    pg_conn_setup = get_postgres_connection(
        POSTGRES_CONFIG['host'],
        POSTGRES_CONFIG['port'],
        POSTGRES_CONFIG['database'],
        POSTGRES_CONFIG['user'],
        POSTGRES_CONFIG['password']
    )
    try:
        with pg_conn_setup.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
            cur.execute(f"""
                CREATE TABLE {temp_table} (
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
            """)
        pg_conn_setup.commit()
        logger.info("Temp table %s recréée pour epoch %s", temp_table, epoch_id)
    finally:
        pg_conn_setup.close()
    
    # Write transformed micro-batch into temporary table via Spark JDBC
    write_dataframe_to_postgres(df_to_write, temp_table, POSTGRES_JDBC_URL, POSTGRES_JDBC_PROPERTIES, mode="append")
    
    pg_conn = None
    try:
        pg_conn = get_postgres_connection(POSTGRES_CONFIG['host'],POSTGRES_CONFIG['port'],POSTGRES_CONFIG['database'],POSTGRES_CONFIG['user'],POSTGRES_CONFIG['password'])

        # UPSERT micro-batch data into the shared batch/streaming staging table
        logger.info("Epoch %s : tentative d'écriture du batch...", epoch_id)
        logger.info(f"Écriture dans {STAGING_TABLE}")
        upsert_from_temp_table(
            temp_table=temp_table,
            target_table=STAGING_TABLE,
            conflict_cols=["date", "iso_code"],
            update_cols=["total_vaccinations", "people_vaccinated", "people_fully_vaccinated", "load_date"],
            pg_conn=pg_conn
        )
        logger.info("Epoch %s : batch écrit avec succès", epoch_id)

    except Exception as e:
        logger.exception("Epoch %s : erreur lors de l'écriture du batch", epoch_id)
        raise e

    finally:
        if pg_conn:
            pg_conn.close()


# ============================
# Consumer
# ============================
def run_consumer(checkpoint_dir: Path = Path("/opt/airflow/data/checkpoints/owid_stream")):
    '''
    Run the Spark Structured Streaming consumer for OWID COVID-19 vaccination data.

    This function:
    - Initializes a Spark Structured Streaming session
    - Reads vaccination events from Kafka
    - Deserializes JSON messages using an explicit schema
    - Applies event-time processing with watermarks
    - Writes streaming micro-batches into PostgreSQL staging

    :param checkpoint_dir: Directory used by Spark to persist streaming checkpoints
    '''
    # Initialize Spark session
    spark = get_spark_session("OWID_COVID_Streaming")
    spark.sparkContext.setLogLevel("WARN")

    # ============================
    # Read events and transformation
    # ============================
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
        .foreachBatch(write_batch_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_dir.resolve()))
        .start()
    )

    logger.info("Job Spark Structured Streaming OWID démarré")
    
    # Process all currently available micro-batches and stop
    query.processAllAvailable()
    logger.info("Micro-batchs traités, consumer stop")
    query.stop()

    # For long-running :
    # query.awaitTermination()

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    run_consumer()