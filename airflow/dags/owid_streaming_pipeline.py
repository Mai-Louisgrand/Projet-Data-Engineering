# DAG Airflow : owid_streaming_pipeline
# Orchestration du pipeline de streaming simulé pour OWID COVID-19

from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka.admin import KafkaAdminClient, NewTopic

from src.streaming.producer.owid_event_producer import run_producer
from src.streaming.consumer.owid_stream_processing import run_consumer

import logging
import os

# ============================
# Configuration
# ============================
# Paramètres
INPUT_PATH = Path("/opt/airflow/data/processed/owid_covid")

# Topic
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_NAME = "owid_vaccination_events"

# Setup du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================
# Operator pour créer le topic si nécessaire
# ============================
def ensure_topic_exists(topic_name: str, partitions: int = 1, replication_factor: int = 1):
    '''
    Vérifie si le topic existe, le crée sinon
    '''
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor)
        admin_client.create_topics([topic])
        logger.info(f"Topic Kafka créé : {topic_name}")
    else:
        logger.info(f"Topic Kafka déjà existant : {topic_name}")

# ============================
# Arguments par défaut pour les tâches du DAG
# ============================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# ============================
# DAG Airflow
# ============================
with DAG(
    "owid_streaming_pipeline",
    description="Pipeline streaming simulé OWID via Kafka + consumer Spark",
    default_args=default_args,
    start_date=datetime(2026, 1, 30),
    schedule_interval=None,  # event-driven/manuel
    catchup=False,
    params={"target_date": "{{ ds }}"},  # paramètre pour choisir date de simulation de streaming
    doc_md="""
    ## Streaming DAG OWID COVID-19
    - Producer Kafka : envoie micro-batchs simulés
    - Consumer Spark : Spark Structured Streaming vers table de staging
    """
) as dag:

    # Tâche de démarrage
    start_task = PythonOperator(
        task_id="start",
        python_callable=lambda: logger.info("DAG streaming démarré"),
        doc_md="Tâche de démarrage du DAG"
    )

    # ============================
    # Création/validation du topic Kafka
    # ============================
    ensure_topic_task = PythonOperator(
        task_id="ensure_kafka_topic",
        python_callable=ensure_topic_exists,
        op_kwargs={"topic_name": TOPIC_NAME},
        doc_md="Tâche de création/validation du topic Kafka"
    )

    # ============================
    # Producer
    # ============================
    # Tâche de démarrage du producer
    start_producer_task = PythonOperator(
        task_id="start_kafka_producer",
        python_callable=run_producer,
        op_kwargs={"input_path": INPUT_PATH, "target_date": "{{ params.target_date }}"},
        execution_timeout=timedelta(minutes=6),
        doc_md="Tâche de démarrage du producer"
    )

    # ============================
    # Consumer
    # ============================
    # Tâche de démarrage du consumer
    run_consumer_task = PythonOperator(
        task_id="run_streaming_consumer",
        python_callable=run_consumer,
        execution_timeout=timedelta(minutes=10),
        doc_md="Tâche de démarrage du consumer"
    )

    # ============================
    # Fin
    # ============================
    # Tâche de fin
    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: logger.info("DAG streaming terminé")
    )

    # Définition des dépendances (ordre d’exécution)
    start_task >> ensure_topic_task >> start_producer_task >> run_consumer_task >> end_task
