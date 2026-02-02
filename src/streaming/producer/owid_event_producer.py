'''
Kafka Producer simulating a daily stream of OWID COVID vaccination updates

Responsibilities:
 - Read processed OWID Parquet files
 - Filter records for a given date
 - Transform rows into business events
 - Publish events to Kafka with a simulated real-time delay
'''

import json
import time
from pathlib import Path
import logging

import pandas as pd
from confluent_kafka import Producer

from src.streaming.config.kafka_config import PRODUCER_CONFIG, OWID_TOPIC

# ============================
# Logging setup
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# ============================
# Business transformations
# ============================
def build_event(row: pd.Series) -> dict:
    '''
    Transform a single OWID vaccination record into a Kafka event.

    :param row: pandas Series representing one OWID record
    :return: dictionary representing the event payload
    '''
    return {
        "event_type": "vaccination_daily_update",
        "event_time": row["date"],
        "country_code": row["iso_code"],
        "country": row["location"],
        "people_vaccinated": row.get("people_vaccinated"),
        "people_fully_vaccinated": row.get("people_fully_vaccinated"),
        "total_vaccinations": row.get("total_vaccinations"),
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
def run_producer(input_path: Path,target_date: str):
    '''
    Publish OWID vaccination events for a specific date to Kafka.

    :param input_path: root directory containing processed Parquet files
    :param target_date: date to simulate (YYYY-MM-DD)
    '''
    
    producer = Producer(PRODUCER_CONFIG)
    logger.info("Démarrage du producer OWID")

    for parquet_file in input_path.rglob("*.parquet"):
        df = pd.read_parquet(parquet_file) # Read Parquet file

        iso_code = parquet_file.parent.name.split('=')[1] # Extract iso_code from partitioned folder name
        df['iso_code'] = iso_code
        
        df["date"] = df["date"].astype(str)
        df_day = df[df["date"] == target_date] # Filter records for the target date

        if df_day.empty:
            continue

        # Publish events row by row to Kafka
        for _, row in df_day.iterrows():
            event = build_event(row)

            producer.produce(
                topic=OWID_TOPIC,
                key=row["iso_code"],
                value=json.dumps(event).encode("utf-8"), # serialize the event to JSON and encode it as UTF-8 bytes before sending
                callback=delivery_report,
            )

            producer.poll(0) # trigger delivery callbacks
            time.sleep(0.1) # simulate real-time streaming delay

    producer.flush() # ensure all messages are delivered
    logger.info("Tous les évènements ont été publiés avec succès")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    INPUT_PATH = Path("data/processed/owid_covid")
    TARGET_DATE = "2021-06-01"

    run_producer(INPUT_PATH, TARGET_DATE)

