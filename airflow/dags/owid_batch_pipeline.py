# DAG Airflow : owid_batch_pipeline

# - définition du pipeline batch pour le dataset OWID COVID-19 
# - orchestration de : ingestion, transformation et chargement des données dans PostgreSQL, avec suivi via Airflow

from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import subprocess
import os

# Import des fonctions métier existantes
from src.ingestion.ingestion_owid_batch import create_output_dir, download_csv
from src.transformation.spark_transform_owid import run_transformation
from src.storage.postgres.load_owid_postgres import run_load

# =============================
# Paramètres
# =============================
DEFAULT_PROCESSED_DIR = Path("data/processed/owid_covid")

DB_PARAMS = {
    "PGHOST": "postgres",       # host du container PostgreSQL pour Airflow
    "PGUSER": "data",
    "PGPASSWORD": "data",
    "PGDATABASE": "covid_dw",
    "PGPORT": "5432",
}

# =============================
# Fonctions wrappers pour Airflow
# =============================
# Pour l'ingestion
def task_create_raw_dir(ti, **kwargs):
    '''
    Crée le dossier RAW pour l'ingestion du jour et le stocke dans XCom pour la tâche suivante
    '''
    output_dir = create_output_dir()
    ti.xcom_push(key='raw_dir', value=str(output_dir)) # stocke Path via un string dans XCom pour les tâches suivantes

def task_download_csv(ti, **kwargs):
    '''
    Télécharge le CSV OWID dans le dossier RAW récupéré depuis XCom
    '''
    output_dir_str = ti.xcom_pull(key='raw_dir')
    output_dir = Path(output_dir_str)
    download_csv(output_dir)

# Pour la transformation
def task_run_transformation(ti, **kwargs):
    '''
    Exécute la transformation OWID COVID
    '''
    raw_dir_str = ti.xcom_pull(key='raw_dir')
    raw_dir = Path(raw_dir_str)

    run_transformation(raw_path=str(raw_dir))

# Pour le stockage
def task_run_init_storage():
    # Variables d'environnement pour le script
    env = os.environ.copy()
    env.update(DB_PARAMS)

    subprocess.run(["/opt/airflow/scripts/bash/init_storage.sh"], env=env, check=True)

def task_load_staging(ti, **kwargs):
    """
    Charge les fichiers Parquet transformés vers table de staging dans PostgreSQL
    """
    processed_dir = kwargs["params"]["processed_dir"]
    run_load(parquet_path=processed_dir, pg_host=DB_PARAMS["PGHOST"])

def task_run_populate():
    env = os.environ.copy()
    env.update(DB_PARAMS)

    subprocess.run("/opt/airflow/scripts/bash/run_dml.sh", env=env, check=True)

# ============================
# Arguments par défaut pour les tâches du DAG
# ============================
default_args = {
    'owner': 'airflow',                   # Propriétaire du DAG
    'depends_on_past': False,             # Les exécutions ne dépendent pas des runs précédents
    'retries': 1,                         # Nombre de retry en cas d’échec
    'retry_delay': timedelta(minutes=5),  # Délai avant de retenter
}

# ============================
# DAG Airflow
# ============================
with DAG(
    'owid_batch_pipeline',            # Nom unique du DAG
    description="Pipeline batch OWID : ingestion, transformation et stockage PostgreSQL",
    default_args=default_args,        # Arguments par défaut
    start_date=datetime(2026, 1, 16), # Date de début des exécutions
    schedule_interval='@daily',       # Exécution quotidienne
    catchup=False,                    # Ne pas exécuter les DAGs passés non lancés
    params={
        "processed_dir": str(DEFAULT_PROCESSED_DIR),
    },
) as dag:

    # Tâche de démarrage
    start_task = PythonOperator(
        task_id='start', # Nom de la tâche dans Airflow
        python_callable=lambda: print("DAG démarré")
    )

    # ============================
    # Ingestion
    # ============================
    # Tâche 1 : création du dossier RAW
    create_dir_task = PythonOperator(
        task_id='create_raw_dir',
        python_callable=task_create_raw_dir
    )

    # Tâche 2 : téléchargement du CSV
    download_csv_task = PythonOperator(
        task_id='download_csv',
        python_callable=task_download_csv
    )

    # ============================
    # Transformation
    # ============================
    # Tâche de transformation des données
    transform_task = PythonOperator(
        task_id='transformation_owid',
        python_callable=task_run_transformation
    )

    # ============================
    # Stockage
    # ============================
    # Tâche d'initialisation du stockage sur PostgreSQL
    init_storage_task = PythonOperator(
        task_id="init_storage",
        python_callable=task_run_init_storage
    )
    
    # Tâche de chargement dans PostgreSQL
    load_task = PythonOperator(
        task_id='load_staging',
        python_callable=task_load_staging
    )

    # Tâche de population des tables via table de staging dans PostgreSQL
    populate_task = PythonOperator(
        task_id="populate_dim_fact",
        python_callable=task_run_populate
    )


    # ============================
    # Fin
    # ============================
    # Tâche de fin
    end_task = PythonOperator(
        task_id='end',
        python_callable=lambda: print("DAG terminé")  # Simple print pour signaler la fin
    )


    # Définition des dépendances (ordre d’exécution)
    start_task >> create_dir_task >> download_csv_task >> transform_task >> init_storage_task >> load_task >> populate_task >> end_task
