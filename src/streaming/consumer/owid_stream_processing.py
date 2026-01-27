# Job Spark Structured Streaming pour les événements de vaccination OWID

# - Lecture des événements depuis Kafka
# - Désérialisation des messages JSON avec un schéma explicite
# - Gestion des données en retard
# - Écriture des données de streaming dans la table de staging PostgreSQL

import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from pyspark.sql import DataFrame

from src.streaming.config.kafka_config import OWID_TOPIC, KAFKA_BOOTSTRAP_SERVERS
from src.streaming.config.spark_config import get_spark_session
from src.streaming.config.postgres_config import STAGING_TABLE, POSTGRES_CONFIG
from src.streaming.postgres_writer import write_to_postgres, get_postgres_connection

# ============================
# Configuration
# ============================
# Setup du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Session Spark
spark = get_spark_session("OWID_COVID_Streaming")

# Paramètres PostgreSQL
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
POSTGRES_JDBC_PROPERTIES = {
    "user": POSTGRES_CONFIG["user"],
    "password": POSTGRES_CONFIG["password"],
    "driver": "org.postgresql.Driver"
}

# ============================
# Schéma explicite des événements
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

def transform_batch(batch_df: DataFrame) -> DataFrame:
    # Transformation micro-batch pour matcher le schema PostgreSQL
    return batch_df.select(
        F.col("event_type").cast("string"),
        F.to_date(F.col("event_time"), "yyyy-MM-dd").alias("event_date"),
        F.col("country_code").cast("string"),
        F.col("country").cast("string"),
        F.when(~F.isnan(F.col("people_vaccinated")), F.col("people_vaccinated").cast("long")).otherwise(None).alias("people_vaccinated"),
        F.when(~F.isnan(F.col("people_fully_vaccinated")), F.col("people_fully_vaccinated").cast("long")).otherwise(None).alias("people_fully_vaccinated"),
        F.when(~F.isnan(F.col("total_vaccinations")), F.col("total_vaccinations").cast("long")).otherwise(None).alias("total_vaccinations")
        # ingestion_ts n'est pas présent → PostgreSQL le génère automatiquement
    )

# ============================
# Lecture du flux Kafka
# ============================
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", OWID_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# ============================
# Désérialisation des messages JSON
# ============================
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(F.from_json("json_str", event_schema).alias("data"))
    .select("data.*")
)

# ============================
# Gestion des données en retard
# ============================
df_with_event_time = (
    df_parsed
    .withColumn("event_timestamp", F.to_timestamp("event_time", "yyyy-MM-dd"))
    .withWatermark("event_timestamp", "7 days")
)

# ============================
# Fonction d'écriture dans PostgreSQL
# ============================
def write_batch_to_postgres(batch_df: DataFrame, epoch_id: int) -> None:
    # Écriture d'un micro-batch de données dans la table de staging PostgreSQL

    if batch_df.isEmpty():
        logger.info("Epoch %s : batch vide, aucun enregistrement à écrire", epoch_id)
        return

    # Transformation pour matcher le schema PostgreSQL
    df_to_write = transform_batch(batch_df)

    logger.info("Epoch %s : écriture de %s enregistrements", epoch_id, batch_df.count())

    pg_conn = None
    try:
        pg_conn = get_postgres_connection(POSTGRES_CONFIG['host'],POSTGRES_CONFIG['port'],POSTGRES_CONFIG['database'],POSTGRES_CONFIG['user'],POSTGRES_CONFIG['password'])

        logger.info("Epoch %s : tentative d'écriture du batch...", epoch_id)

        write_to_postgres(
            df=df_to_write,
            table=STAGING_TABLE,
            jdbc_url=POSTGRES_JDBC_URL,
            jdbc_properties=POSTGRES_JDBC_PROPERTIES,
            pg_conn=pg_conn,
        )

        logger.info("Epoch %s : batch écrit avec succès", epoch_id)

    except Exception as e:
        logger.exception("Epoch %s : erreur lors de l'écriture du batch", epoch_id)
        raise e

    finally:
        if pg_conn:
            pg_conn.close()

# ============================
# Lancement du streaming Spark
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
