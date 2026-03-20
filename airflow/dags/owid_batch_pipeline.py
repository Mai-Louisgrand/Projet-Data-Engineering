'''
Airflow DAG: owid_batch_pipeline

 - Definition of the batch pipeline for the OWID COVID-19 dataset
 - Orchestration of ingestion, transformation, and loading into PostgreSQL with monitoring handled by Airflow
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

import subprocess
import os
import psycopg2
import logging

# Import existing business logic functions
from src.ingestion.ingestion_owid_batch import upload_to_gcs

# =============================
# Configuration
# =============================
# PostgreSQL connection parameters
DB_PARAMS = {
    "PGHOST": "postgres",        # PostgreSQL container hostname from Airflow
    "PGUSER": "data",
    "PGPASSWORD": "data",
    "PGDATABASE": "covid_dw",
    "PGPORT": "5432",
}

# =============================
# Airflow task wrapper functions
# =============================
# -------- Storage --------
def task_run_init_storage():
    '''
    Initialize the PostgreSQL database (schemas, tables, constraints).
    '''
    logger = logging.getLogger("airflow.task")

    # Prepare environment variables for the shell script
    env = os.environ.copy()
    env.update(DB_PARAMS)

    subprocess.run(["/opt/airflow/scripts/bash/init_storage.sh"], env=env, check=True)
    
    logger.info("Initialisation PostgreSQL terminée")

def check_staging_data(**kwargs):
    '''
    Verify that the PostgreSQL staging table exists and contains data.
    '''
    logger = logging.getLogger("airflow.task")

    try:
        conn = psycopg2.connect(
            host=DB_PARAMS["PGHOST"],
            database=DB_PARAMS["PGDATABASE"],
            user=DB_PARAMS["PGUSER"],
            password=DB_PARAMS["PGPASSWORD"],
            port=DB_PARAMS["PGPORT"]
        )

        cur = conn.cursor()

        # Check table existence
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema='staging'
              AND table_name='stg_owid_covid';
        """)
        row = cur.fetchone()
        table_count = row[0] if row else 0
        if table_count == 0:
            logger.error("Table staging.stg_owid_covid n'existe pas")
            raise ValueError("La table de staging n'existe pas")

        # Check row count
        cur.execute("SELECT COUNT(*) FROM staging.stg_owid_covid;")
        row = cur.fetchone()
        count = row[0] if row else 0

        if count == 0:
            logger.error("Table de staging vide")
            raise ValueError("La table de staging est vide")

        logger.info(f"Vérification staging OK : {count} lignes présentes")

    finally:
        cur.close()
        conn.close()

def task_run_populate():
    '''
    Populate dimension and fact tables from the staging table.
    '''
    logger = logging.getLogger("airflow.task")

    env = os.environ.copy()
    env.update(DB_PARAMS)

    subprocess.run("/opt/airflow/scripts/bash/run_dml.sh", env=env, check=True)

    logger.info("Population tables dim/fact terminée")

# ============================
# Default DAG arguments
# ============================
default_args = {
    'owner': 'airflow',                   # DAG owner
    'depends_on_past': False,             # Runs do not depend on previous executions
    'email_on_failure': True,
    'retries': 1,                         # Number of retries on failure
    'retry_delay': timedelta(minutes=3),  # Delay between retries
}

# ============================
# DAG definition
# ============================
with DAG(
    'owid_batch_pipeline',
    description="Pipeline batch OWID : ingestion, transformation et stockage PostgreSQL",
    default_args=default_args,
    start_date=datetime(2026, 1, 16),
    schedule_interval='@daily',       # daily execution
    catchup=False,                    # Disable backfilling of past DAG runs
    doc_md="""
    ## Pipeline batch OWID COVID-19
    - Pipeline : start -> ingestion -> transformation -> quality check -> staging -> populate -> end
    """
) as dag:

    start_task = PythonOperator(
        task_id='start', # Nom de la tâche dans Airflow
        python_callable=lambda: print("DAG démarré"),
        doc_md="Tâche de démarrage du DAG"
    )

    # -------- Ingestion --------
    ingestion_task = PythonOperator(
        task_id='stream_to_gcs',
        python_callable=upload_to_gcs,
        execution_timeout=timedelta(minutes=10),
        retries=2,
        retry_delay=timedelta(minutes=3),
        doc_md="Tâche d'upload du dataset OWID COVID-19 sur Google Cloud Storage"
    )

    # -------- Transformation --------
    transform_task = BashOperator(
        task_id="transformation_owid",
        bash_command="""
        docker exec spark-master bash -c '
        PYTHONPATH=/opt/app /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf "spark.driver.extraJavaOptions=-Duser.home=/tmp" \
            --conf "spark.executor.extraJavaOptions=-Duser.home=/tmp" \
            --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2 \
            /opt/app/src/transformation/spark_transform_owid.py
        '
        """,
        execution_timeout=timedelta(minutes=10),
        retries=2,
        retry_delay=timedelta(minutes=3),
        doc_md="Transformation PySpark des fichiers RAW vers Parquet"
    )

    # -------- Storage --------
    init_storage_task = PythonOperator(
        task_id="init_storage",
        python_callable=task_run_init_storage,
        execution_timeout=timedelta(minutes=6),
        doc_md="Initialisation de la BDD PostgreSQL"
    )

    load_task = BashOperator(
        task_id="load_staging",
        bash_command="""
        docker exec spark-master bash -c '
        PYTHONPATH=/opt/app /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf "spark.driver.extraJavaOptions=-Duser.home=/tmp" \
            --conf "spark.executor.extraJavaOptions=-Duser.home=/tmp" \
            --packages com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2,org.postgresql:postgresql:42.6.0 \
            /opt/app/src/storage/postgres/load_owid_postgres.py \
            --pg_host postgres
        '
        """,
        execution_timeout=timedelta(minutes=6),
        doc_md="Chargement des données transformées dans la table de staging"
    )

    check_staging_task = PythonOperator(
        task_id='check_staging_data',
        python_callable=check_staging_data,
        execution_timeout=timedelta(minutes=3),
        doc_md="Vérifie que la table de staging PostgreSQL contient bien des données avant population"
    )

    populate_task = PythonOperator(
        task_id="populate_dim_fact",
        python_callable=task_run_populate,
        execution_timeout=timedelta(minutes=10),
        doc_md="Population des tables dim et fact à partir de la table de staging"
    )

    # -------- End --------
    end_task = PythonOperator(
        task_id='end',
        python_callable=lambda: print("DAG terminé"),
        doc_md="Tâche de fin du DAG"
    )


    # Task dependencies
    start_task >> ingestion_task >> transform_task >> init_storage_task >> load_task >> check_staging_task >> populate_task >> end_task