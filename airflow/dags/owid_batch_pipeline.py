# DAG Airflow : owid_batch_pipeline

# - définition du pipeline batch pour le dataset OWID COVID-19 
# - orchestration de : ingestion, transformation et chargement des données dans PostgreSQL, avec suivi via Airflow

from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import subprocess
import os
import psycopg2
import logging

# Import des fonctions métier existantes
from src.ingestion.ingestion_owid_batch import create_output_dir, download_csv
from src.transformation.spark_transform_owid import run_transformation
from src.storage.postgres.load_owid_postgres import run_load

# =============================
# Configuration
# =============================
# Paramètres
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
    logger = logging.getLogger("airflow.task")
    output_dir = create_output_dir()
    ti.xcom_push(key='raw_dir', value=str(output_dir)) # stocke Path via un string dans XCom pour les tâches suivantes
    logger.info(f"Dossier RAW créé : {output_dir}")

def task_download_csv(ti, **kwargs):
    '''
    Télécharge le CSV OWID dans le dossier RAW récupéré depuis XCom
    '''
    logger = logging.getLogger("airflow.task")
    output_dir_str = ti.xcom_pull(key='raw_dir')
    output_dir = Path(output_dir_str)
    download_csv(output_dir)
    logger.info(f"CSV téléchargé dans : {output_dir}")

def check_raw_data(ti, **kwargs):
    '''
    Vérifie que le CSV est bien téléchargé après download
    '''
    logger = logging.getLogger("airflow.task")
    raw_dir = Path(ti.xcom_pull(key='raw_dir'))
    csv_file = raw_dir / "owid_covid_data.csv"
    if not csv_file.exists() or csv_file.stat().st_size == 0:
        logger.error(f"CSV manquant ou vide dans {raw_dir}")
        raise ValueError(f"CSV manquant ou vide dans {raw_dir}")
    logger.info(f"CSV présent et non vide : {csv_file}")

# Pour la transformation
def task_run_transformation(ti, **kwargs):
    '''
    Exécute la transformation OWID COVID
    '''
    logger = logging.getLogger("airflow.task")
    raw_dir_str = ti.xcom_pull(key='raw_dir')
    raw_dir = Path(raw_dir_str)
    run_transformation(raw_path=str(raw_dir))
    logger.info(f"Transformation terminée pour RAW : {raw_dir}")

def check_transformed_data(**kwargs):
    '''
    Vérifie existence des fichiers transformés avant chargement vers stockage
    '''
    logger = logging.getLogger("airflow.task")
    processed_dir = kwargs["params"]["processed_dir"]
    parquet_files = list(Path(processed_dir).glob("**/*.parquet"))
    if not parquet_files:
        logger.error(f"Pas de fichiers transformés dans {processed_dir}")
        raise ValueError(f"Pas de fichiers transformés dans {processed_dir} à charger")
    logger.info(f"{len(parquet_files)} fichiers transformés prêts à être chargés")

# Pour le stockage
def task_run_init_storage():
    """
    Initialise la base de données PostgreSQL
    """
    logger = logging.getLogger("airflow.task")

    # Variables d'environnement pour le script
    env = os.environ.copy()
    env.update(DB_PARAMS)
    subprocess.run(["/opt/airflow/scripts/bash/init_storage.sh"], env=env, check=True)
    logger.info("Initialisation PostgreSQL terminée")

def task_load_staging(ti, **kwargs):
    """
    Charge les fichiers Parquet transformés vers table de staging dans PostgreSQL
    """
    logger = logging.getLogger("airflow.task")

    processed_dir = kwargs["params"]["processed_dir"]
    run_load(parquet_path=processed_dir, pg_host=DB_PARAMS["PGHOST"])
    logger.info(f"Chargement Parquet -> staging terminé : {processed_dir}")

def check_staging_data(**kwargs):
    '''
    Vérifie que la table de staging PostgreSQL contient des données
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

        # Vérification existence table
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

        # Vérification nombre de lignes
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
    logger = logging.getLogger("airflow.task")

    env = os.environ.copy()
    env.update(DB_PARAMS)
    subprocess.run("/opt/airflow/scripts/bash/run_dml.sh", env=env, check=True)
    logger.info("Population tables dim/fact terminée")

# ============================
# Arguments par défaut pour les tâches du DAG
# ============================
default_args = {
    'owner': 'airflow',                   # Propriétaire du DAG
    'depends_on_past': False,             # Les exécutions ne dépendent pas des runs précédents
    'email_on_failure': True,
    'retries': 1,                         # Nombre de retry en cas d’échec
    'retry_delay': timedelta(minutes=3),  # Délai avant de retenter
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
    doc_md="""
    ## Pipeline batch OWID COVID-19
    - Pipeline : start -> ingestion -> transformation -> quality check -> staging -> populate -> end
    """
) as dag:

    # Tâche de démarrage
    start_task = PythonOperator(
        task_id='start', # Nom de la tâche dans Airflow
        python_callable=lambda: print("DAG démarré"),
        doc_md="Tâche de démarrage du DAG"
    )

    # ============================
    # Ingestion
    # ============================
    # Tâche de création du dossier RAW
    create_dir_task = PythonOperator(
        task_id='create_raw_dir',
        python_callable=task_create_raw_dir,
        execution_timeout=timedelta(minutes=3),
        doc_md="Création du dossier de données RAW"
    )

    # Tâche de téléchargement du CSV
    download_csv_task = PythonOperator(
        task_id='download_csv',
        python_callable=task_download_csv,
        execution_timeout=timedelta(minutes=6),
        doc_md="Téléchargement des données CSV"
    )

    # Tâche de vérification du téléchargement
    check_csv_task = PythonOperator(
        task_id='check_raw_data',
        python_callable=check_raw_data,
        execution_timeout=timedelta(minutes=3)
    )

    # ============================
    # Transformation
    # ============================
    # Tâche de transformation des données
    transform_task = PythonOperator(
        task_id='transformation_owid',
        python_callable=task_run_transformation,
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
        doc_md="Transformation PySpark des fichiers RAW vers Parquet"
    )

    # Tâche de vérification de la transformation
    quality_check_task = PythonOperator(
        task_id='check_transformed_data',
        python_callable=check_transformed_data,
        params={"processed_dir": str(DEFAULT_PROCESSED_DIR)},
        execution_timeout=timedelta(minutes=3),
        doc_md="Vérification de l'existence des fichiers transformés avant stockage"
    )

    # ============================
    # Stockage
    # ============================
    # Tâche d'initialisation du stockage sur PostgreSQL
    init_storage_task = PythonOperator(
        task_id="init_storage",
        python_callable=task_run_init_storage,
        execution_timeout=timedelta(minutes=6),
        doc_md="Initialisation de la BDD PostgreSQL"
    )
    
    # Tâche de chargement dans PostgreSQL
    load_task = PythonOperator(
        task_id='load_staging',
        python_callable=task_load_staging,
        execution_timeout=timedelta(minutes=6),
        doc_md="Chargement des données transformées dans la table de staging"
    )

    # Tâche de vérification du chargement
    check_staging_task = PythonOperator(
        task_id='check_staging_data',
        python_callable=check_staging_data,
        execution_timeout=timedelta(minutes=3),
        doc_md="Vérifie que la table de staging PostgreSQL contient bien des données avant population"
    )

    # Tâche de population des tables via table de staging dans PostgreSQL
    populate_task = PythonOperator(
        task_id="populate_dim_fact",
        python_callable=task_run_populate,
        execution_timeout=timedelta(minutes=10),
        doc_md="Population des tables dim et fact à partir de la table de staging"
    )

    # ============================
    # Fin
    # ============================
    # Tâche de fin
    end_task = PythonOperator(
        task_id='end',
        python_callable=lambda: print("DAG terminé"),
        doc_md="Tâche de fin du DAG"
    )


    # Définition des dépendances (ordre d’exécution)
    start_task >> create_dir_task >> download_csv_task >> check_csv_task  >> transform_task >> quality_check_task >> init_storage_task >> load_task >> check_staging_task >> populate_task >> end_task