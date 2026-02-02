'''
Airflow DAG: owid_batch_pipeline

 - Definition of the batch pipeline for the OWID COVID-19 dataset
 - Orchestration of ingestion, transformation, and loading into PostgreSQL with monitoring handled by Airflow
'''

from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import subprocess
import os
import psycopg2
import logging

# Import existing business logic functions
from src.ingestion.ingestion_owid_batch import create_output_dir, download_csv
from src.transformation.spark_transform_owid import run_transformation
from src.storage.postgres.load_owid_postgres import run_load

# =============================
# Configuration
# =============================
# Default paths
DEFAULT_PROCESSED_DIR = Path("data/processed/owid_covid")

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
# -------- Ingestion --------
def task_create_raw_dir(ti, **kwargs):
    '''
    Create the RAW directory for the current ingestion run and store its path in XCom for downstream tasks.
    '''
    logger = logging.getLogger("airflow.task")
    output_dir = create_output_dir()
    ti.xcom_push(key='raw_dir', value=str(output_dir)) # store Path as string in XCom for compatibility
    logger.info(f"Dossier RAW créé : {output_dir}")

def task_download_csv(ti, **kwargs):
    '''
    Download CSV file into the RAW directory retrieved from XCom.
    '''
    logger = logging.getLogger("airflow.task")
    output_dir_str = ti.xcom_pull(key='raw_dir')
    output_dir = Path(output_dir_str)
    download_csv(output_dir)
    logger.info(f"CSV téléchargé dans : {output_dir}")

def check_raw_data(ti, **kwargs):
    '''
    Verify that the CSV file has been successfully downloaded and is not empty.
    '''
    logger = logging.getLogger("airflow.task")
    raw_dir = Path(ti.xcom_pull(key='raw_dir'))
    csv_file = raw_dir / "owid_covid_data.csv"

    if not csv_file.exists() or csv_file.stat().st_size == 0:
        logger.error(f"CSV manquant ou vide dans {raw_dir}")
        raise ValueError(f"CSV manquant ou vide dans {raw_dir}")
    
    logger.info(f"CSV présent et non vide : {csv_file}")

# -------- Transformation --------
def task_run_transformation(ti, **kwargs):
    '''
    Execute the OWID COVID transformation using PySpark.
    '''
    logger = logging.getLogger("airflow.task")
    raw_dir_str = ti.xcom_pull(key='raw_dir')
    raw_dir = Path(raw_dir_str)
    run_transformation(raw_path=str(raw_dir))
    logger.info(f"Transformation terminée pour RAW : {raw_dir}")

def check_transformed_data(**kwargs):
    '''
    Verify that transformed Parquet files exist before loading them into storage.
    '''
    logger = logging.getLogger("airflow.task")
    processed_dir = kwargs["params"]["processed_dir"]

    parquet_files = list(Path(processed_dir).glob("**/*.parquet"))
    if not parquet_files:
        logger.error(f"Pas de fichiers transformés dans {processed_dir}")
        raise ValueError(f"Pas de fichiers transformés dans {processed_dir} à charger")
    
    logger.info(f"{len(parquet_files)} fichiers transformés prêts à être chargés")

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

def task_load_staging(ti, **kwargs):
    '''
    Load transformed Parquet files into the PostgreSQL staging table.
    '''
    logger = logging.getLogger("airflow.task")

    processed_dir = kwargs["params"]["processed_dir"]
    run_load(parquet_path=processed_dir, pg_host=DB_PARAMS["PGHOST"])
    logger.info(f"Chargement Parquet -> staging terminé : {processed_dir}")

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
    params={
        "processed_dir": str(DEFAULT_PROCESSED_DIR),
    },
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
    create_dir_task = PythonOperator(
        task_id='create_raw_dir',
        python_callable=task_create_raw_dir,
        execution_timeout=timedelta(minutes=3),
        doc_md="Création du dossier de données RAW"
    )

    download_csv_task = PythonOperator(
        task_id='download_csv',
        python_callable=task_download_csv,
        execution_timeout=timedelta(minutes=6),
        doc_md="Téléchargement des données CSV"
    )

    check_csv_task = PythonOperator(
        task_id='check_raw_data',
        python_callable=check_raw_data,
        execution_timeout=timedelta(minutes=3)
    )

    # -------- Transformation --------
    transform_task = PythonOperator(
        task_id='transformation_owid',
        python_callable=task_run_transformation,
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
        doc_md="Transformation PySpark des fichiers RAW vers Parquet"
    )

    quality_check_task = PythonOperator(
        task_id='check_transformed_data',
        python_callable=check_transformed_data,
        params={"processed_dir": str(DEFAULT_PROCESSED_DIR)},
        execution_timeout=timedelta(minutes=3),
        doc_md="Vérification de l'existence des fichiers transformés avant stockage"
    )

    # -------- Storage --------
    init_storage_task = PythonOperator(
        task_id="init_storage",
        python_callable=task_run_init_storage,
        execution_timeout=timedelta(minutes=6),
        doc_md="Initialisation de la BDD PostgreSQL"
    )
    
    load_task = PythonOperator(
        task_id='load_staging',
        python_callable=task_load_staging,
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
    start_task >> create_dir_task >> download_csv_task >> check_csv_task  >> transform_task >> quality_check_task >> init_storage_task >> load_task >> check_staging_task >> populate_task >> end_task