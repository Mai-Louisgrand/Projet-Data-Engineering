'''
Kafka Producer simulating a daily stream of OWID COVID vaccination updates

Responsibilities:
 - Read processed OWID Parquet files from GCS via Spark
 - Filter records for a given date
 - Transform rows into business events
 - Publish events to Kafka with a simulated real-time delay
'''

import json
import time
import argparse

from pyspark.sql.functions import col, lit
from confluent_kafka import Producer

from src.config.kafka_settings import PRODUCER_CONFIG, OWID_TOPIC
from src.config.settings import PROCESSED_PREFIX, GCS_BUCKET_NAME
from src.utils.spark import get_spark
from src.utils.logging import setup_logging

# Logging configuration
logger = setup_logging()

# ============================
# Business transformations
# ============================
def build_event(row) -> dict:
    '''
    Transform a single OWID vaccination record into a Kafka event.

    :param row: pandas Series representing one OWID record
    :return: dictionary representing the event payload
    '''
    return {
        "event_type": "vaccination_daily_update",
        "event_time": str(row.date),
        "country_code": row.iso_code,
        "country": row.location,
        "people_vaccinated": row.people_vaccinated,
        "people_fully_vaccinated": row.people_fully_vaccinated,
        "total_vaccinations": row.total_vaccinations,
    }

# ============================
# Kafka callbaks
# ============================
def delivery_report(err, msg):
    '''
    Kafka delivery callback.

    Called once the message has been delivered or failed.
    '''
    if err is not None:
        logger.error(f"Echec de l'envoi : {err}")
    else:
        logger.info(
            "Evènement envoyé | topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )

# ============================
# Kafka producer
# ============================
def run_producer(target_date: str):
    '''
    Publish OWID vaccination events for a specific date to Kafka.

    :param bucket_name: name of the bucket containing processed Parquet files
    :param target_date: date to simulate (YYYY-MM-DD)
    '''
    producer = Producer(PRODUCER_CONFIG)
    logger.info("Démarrage du producer OWID")

    spark = get_spark("OWID_COVID_Producer")

    gcs_path = (
        f"gs://{GCS_BUCKET_NAME}/{PROCESSED_PREFIX}"
    )

    df = spark.read.parquet(gcs_path)
    # Filtrer par date
    df_day = df.filter(col("date") == lit(target_date))

    count = df_day.count()
    if count == 0:
        logger.info(f"Aucune donnée pour {target_date}")
        return

    logger.info(f"Enregistrements trouvés pour {target_date}")

    # Publish events row by row to Kafka
    for row in df_day.toLocalIterator():
        event = build_event(row)       
        producer.produce(
            topic=OWID_TOPIC,
            key=row.iso_code.encode('utf-8'),
            value=json.dumps(event).encode("utf-8"), # serialize the event to JSON and encode it as UTF-8 bytes before sending
            callback=delivery_report,
        )

        producer.poll(0) # trigger delivery callbacks
        time.sleep(0.1)  # simulate real-time streaming delay

    producer.flush() # ensure all messages are delivered
    logger.info("Tous les évènements ont été publiés avec succès")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OWID Kafka Producer Spark + GCS")
    parser.add_argument("--target_date", type=str, required=True, help="Date to simulate YYYY-MM-DD")
    args = parser.parse_args()

    TARGET_DATE = args.target_date

    run_producer(TARGET_DATE)

