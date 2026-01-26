# Producer Kafka simulant un flux de mises à jour quotidiennes des données de vaccination COVID (OWID)

# - lecture des fichiers Parquet de données OWID déjà traitées (processed)
# - sélection des lignes à la date donnée
# - transformation des lignes en événements
# - publication des événements dans Kafka avec simulation flux temps réel (délai entre envoi)

import json
import time
from pathlib import Path
import logging

import pandas as pd
from confluent_kafka import Producer

from src.streaming.config.kafka_config import PRODUCER_CONFIG, OWID_TOPIC

# ============================
# Setup du logging
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# ============================
# Transformations métier
# ============================
def build_event(row: pd.Series) -> dict:
    # Transformation ligne OWID en événement métier
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
    # Callback Kafka -> confirmation de l'envoi 
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
    # Publication des évènements à une date données dans Kafka
    
    producer = Producer(PRODUCER_CONFIG)
    logger.info("Démarrage du producer OWID")

    for parquet_file in input_path.rglob("*.parquet"):
        # lecture fichiers Parquet
        df = pd.read_parquet(parquet_file)
        # récupération iso_code depuis le nom du dossier parent
        iso_code = parquet_file.parent.name.split('=')[1]
        df['iso_code'] = iso_code
        
        # récupération des lignes à la date spécifiée
        df["date"] = df["date"].astype(str)
        df_day = df[df["date"] == target_date]
        
        # logger.info(f"{parquet_file}: {df_day.shape[0]} lignes pour la date {target_date}")

        if df_day.empty:
            continue

        # publication ligne par ligne dans Kafka
        for _, row in df_day.iterrows():
            event = build_event(row)

            producer.produce(
                topic=OWID_TOPIC,
                key=row["iso_code"],
                value=json.dumps(event).encode("utf-8"), # envoi une chaîne de caractère JSON encodée en bytes
                callback=delivery_report,
            )

            producer.poll(0) # déclenchement de l'envoi et exécution des callbacks
            time.sleep(0.1) # délai pour simulation flux temps réel

    producer.flush() # attend que tout soit envoyé avant de quitter le script
    logger.info("Tous les évènements ont été publiés avec succès")

# ============================
# Point d'entrée du script
# ============================
# Lancement simulation d'un flux Kafka pour une date donnée
if __name__ == "__main__":
    INPUT_PATH = Path("data/processed/owid_covid")
    TARGET_DATE = "2021-06-01"

    run_producer(INPUT_PATH, TARGET_DATE)

