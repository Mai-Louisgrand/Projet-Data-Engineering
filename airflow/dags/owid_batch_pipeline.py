'''
Airflow DAG: owid_batch_pipeline

 - Definition of the batch pipeline for the OWID COVID-19 dataset
 - Orchestration of ingestion, transformation, and loading into PostgreSQL with monitoring handled by Airflow
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Import existing business logic functions
from src.ingestion.ingestion_owid_batch import upload_to_gcs
from src.storage.bigquery.init_storage_bq import init_bigquery
from src.storage.bigquery.load_gcs_to_bq_staging import run_load
from src.storage.bigquery.deduplicate_staging_bq import run_deduplication
from src.storage.bigquery.merge_staging_bq import run_merge
from src.storage.bigquery.load_date_staging_bq import populate_load_date
from src.storage.bigquery.load_dim_fact_bq import run_dml

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
        task_id='init_storage',
        python_callable=init_bigquery,
        execution_timeout=timedelta(minutes=6),
        doc_md="Initialisation de la DWH BigQuery"
    )

    load_staging_tmp_task = PythonOperator(
        task_id='load_gcs_to_bq_staging_tmp',
        python_callable=run_load,
        execution_timeout=timedelta(minutes=6),
        doc_md="Chargement des données processed depuis GCS vers BigQuery staging_tmp"
    )

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
        task_id='end',
        python_callable=lambda: print("DAG terminé"),
        doc_md="Tâche de fin du DAG"
    )


    # Task dependencies
    start_task >> ingestion_task >> transform_task >> init_storage_task >> load_staging_tmp_task >> load_date_task >> deduplicate_staging_tmp_task >> merge_staging_task >> populate_task >> end_task