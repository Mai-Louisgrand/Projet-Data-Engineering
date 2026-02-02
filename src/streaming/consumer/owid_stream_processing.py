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

# Spark session initialization
spark = get_spark_session("OWID_COVID_Streaming")
spark.sparkContext.setLogLevel("WARN") # Reduce Spark internal logging noise

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
# Kafka stream ingestion
# ============================
# Read events from Kafka topic
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", OWID_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Deserialize JSON payload into structured columns
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(F.from_json("json_str", event_schema).alias("data"))
    .select("data.*")
)

df_with_event_time = (
    df_parsed
    .withColumn("event_timestamp", F.to_timestamp("event_time", "yyyy-MM-dd"))
    .withWatermark("event_timestamp", "7 days")
)

# ============================
# Micro-batch transformation
# ============================
def transform_batch(batch_df: DataFrame) -> DataFrame:
    '''
    Transform a Spark micro-batch to match the PostgreSQL staging table schema.

    This function:
    - Renames and casts streaming fields to staging-compatible columns
    - Fills missing batch-only fields with NULL values
    - Adds load metadata
    - Deduplicates records based on (date, iso_code)

    :param batch_df: Input micro-batch DataFrame from Spark Structured Streaming
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
        # Micro-batch deduplication on (date, iso_code)
        .dropDuplicates(["date", "iso_code"])
    )

# ============================
# PostgreSQL micro-batch sink
# ============================
def write_batch_to_postgres(batch_df: DataFrame, epoch_id: int) -> None:
    '''
    Write a Spark Structured Streaming micro-batch to PostgreSQL staging.

    Processing steps:
    - Skip empty micro-batches
    - Transform data to staging schema
    - Write data into a temporary table via JDBC
    - UPSERT records into the shared batch/streaming staging table
    - Ensure idempotency using conflict resolution

    :param batch_df: Micro-batch DataFrame
    :param epoch_id: Spark Structured Streaming epoch identifier
    '''
    if batch_df.isEmpty(): # Skip empty micro-batches
        logger.info("Epoch %s : batch vide, aucun enregistrement à écrire", epoch_id)
        return

    df_to_write = transform_batch(batch_df) # Transform data to PostgreSQL-compatible schema
    logger.info("Epoch %s : écriture de %s enregistrements", epoch_id, df_to_write.count())
    

    temp_table = "staging.owid_covid_tmp" # Temporary table used to enable UPSERT logic

    # Drop and recreate temporary table for a clean micro-batch load
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
    
    # Write micro-batch to temporary table using Spark JDBC
    write_dataframe_to_postgres(df_to_write, temp_table, POSTGRES_JDBC_URL, POSTGRES_JDBC_PROPERTIES, mode="append")
    
    pg_conn = None
    try:
        pg_conn = get_postgres_connection(POSTGRES_CONFIG['host'],POSTGRES_CONFIG['port'],POSTGRES_CONFIG['database'],POSTGRES_CONFIG['user'],POSTGRES_CONFIG['password'])

        # UPSERT micro-batch data into shared batch/streaming staging table
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
# Streaming query execution
# ============================
query = (
    df_with_event_time.writeStream
    .foreachBatch(write_batch_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "data/checkpoints/owid_stream")
    .start()
)

logger.info("Job Spark Structured Streaming OWID démarré")
query.awaitTermination()
