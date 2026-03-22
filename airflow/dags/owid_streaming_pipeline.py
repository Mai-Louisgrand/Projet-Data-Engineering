'''
Airflow DAG: owid_streaming_pipeline

Orchestrates a simulated OWID COVID-19 streaming pipeline using Kafka (producer) and Spark Structured Streaming (consumer)
'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from kafka.admin import KafkaAdminClient, NewTopic

from src.streaming.consumer.owid_stream_processing import run_consumer
from src.storage.bigquery.deduplicate_staging_bq import run_deduplication
from src.storage.bigquery.merge_staging_bq import run_merge
from src.storage.bigquery.load_date_staging_bq import populate_load_date
from src.storage.bigquery.load_dim_fact_bq import run_dml

import logging
import os

# ============================
# Configuration
# ============================
# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_NAME = "owid_vaccination_events"

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================
# Utility: ensure Kafka topic exists
# ============================
def ensure_topic_exists(topic_name: str, partitions: int = 1, replication_factor: int = 1):
    '''
    Ensures that the Kafka topic exists.
    If the topic does not exist, it is created.
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
# Default DAG arguments
# ============================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# ============================
# DAG definition
# ============================
with DAG(
    "owid_streaming_pipeline",
    description="Pipeline streaming simulé OWID via Kafka + consumer Spark",
    default_args=default_args,
    start_date=datetime(2022, 1, 30),
    schedule_interval=None,  # Event-driven / manual execution
    catchup=False,
    params={"target_date": "{{ ds }}"},  # Date used to simulate the streaming events
    doc_md="""
    ## Streaming DAG OWID COVID-19
    - Producer Kafka : envoie micro-batchs simulés
    - Consumer Spark : Spark Structured Streaming vers table de staging
    """
) as dag:

    # -------- Start --------
    start_task = PythonOperator(
        task_id="start",
        python_callable=lambda: logger.info("DAG streaming démarré"),
        doc_md="Tâche de démarrage du DAG"
    )

    # -------- Kafka Topic management --------
    ensure_topic_task = PythonOperator(
        task_id="ensure_kafka_topic",
        python_callable=ensure_topic_exists,
        op_kwargs={"topic_name": TOPIC_NAME},
        doc_md="Tâche de création/validation du topic Kafka"
    )

    # -------- Producer --------
    start_producer_task = BashOperator(
    task_id="start_kafka_producer",
    bash_command="""
    docker exec spark-master bash -c '
        PYTHONPATH=/opt/app /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf "spark.driver.extraJavaOptions=-Duser.home=/tmp" \
            --conf "spark.executor.extraJavaOptions=-Duser.home=/tmp" \
            /opt/app/src/streaming/producer/owid_event_producer.py \
            --target_date 2022-05-01
    '
    """,
    execution_timeout=timedelta(minutes=6),
    doc_md="Tâche de démarrage du producer"
)

    # -------- Consumer --------
    run_consumer_task = BashOperator(
        task_id="run_streaming_consumer",
        bash_command="""
        docker exec spark-master bash -c '
            PYTHONPATH=/opt/app /opt/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
                --conf "spark.driver.extraJavaOptions=-Duser.home=/tmp" \
                --conf "spark.executor.extraJavaOptions=-Duser.home=/tmp" \
                /opt/app/src/streaming/consumer/owid_stream_processing.py
        '
        """,
        execution_timeout=timedelta(minutes=10),
        doc_md="Tâche de démarrage du consumer"
    )

    # -------- Storage --------
    load_date_task = PythonOperator(
        task_id='load_date_staging_tmp',
        python_callable=populate_load_date,
        execution_timeout=timedelta(minutes=6),
        doc_md="Ajout de la date du jour en load date sur les données de staging_tmp sur Bigquery"
    )

    deduplicate_staging_tmp_task = PythonOperator(
        task_id="deduplicate_staging_tmp",
        python_callable=run_deduplication,
        execution_timeout=timedelta(minutes=6),
        doc_md="Déduplications des données de staging_tmp sur BigQuery"
    )

    merge_staging_task = PythonOperator(
        task_id="merge_staging",
        python_callable=run_merge,
        execution_timeout=timedelta(minutes=10),
        doc_md="Upsert des données de staging_tmp vers staging sur BigQuery"
    )

    populate_task = PythonOperator(
        task_id="populate_dim_fact",
        python_callable=run_dml,
        execution_timeout=timedelta(minutes=10),
        doc_md="Population des tables dim et fact à partir de la table de staging sur BigQuery"
    )

    # -------- End --------
    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: logger.info("DAG streaming terminé")
    )

    # Task dependencies
    start_task >> ensure_topic_task >> start_producer_task >> run_consumer_task >> load_date_task >> deduplicate_staging_tmp_task >> merge_staging_task >> populate_task >> end_task
