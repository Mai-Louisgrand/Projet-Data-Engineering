'''
Initialize BigQuery datasets and tables for the OWID COVID-19 pipeline.

- Creates datasets if they do not exist (staging, dim, fact)
- Creates tables (via DDL SQL files)
- Populate static dimensions (dim_date)
'''
import logging
from pathlib import Path
from google.cloud import bigquery
from src.config.settings import GCP_PROJECT, BQ_REGION, BQ_DATASET_STAGING, BQ_DATASET_DIM, BQ_DATASET_FACT, LOG_FORMAT

# ============================
# Logging configuration
# ============================
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT
)
logger = logging.getLogger(__name__)

# ============================
# Main function
# ============================
def init_bigquery():
    '''
    Initialize BigQuery: datasets, tables, static dimensions.
    Can be called standalone or from Airflow PythonOperator.
    '''
    # ============================
    # Ensure BigQuery connectivity
    # ============================
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        client.list_datasets()
        logger.info(f"Connexion BigQuery établie")
    except Exception as e:
        logger.error(f"Impossible de se connecter à BigQuery: {e}")
        raise

    # ============================
    # Datasets creation
    # ============================
    for dataset_name in [BQ_DATASET_STAGING, BQ_DATASET_DIM, BQ_DATASET_FACT]:
        dataset_id = f"{GCP_PROJECT}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = BQ_REGION
        try:
            client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset créé / existant : {dataset_id}")
        except Exception as e:
            logger.error(f"Erreur création dataset {dataset_id}: {e}")

    # ============================
    # Tables creation
    # ============================
    # Execute DDL SQL files
    DDL_DIR = Path(__file__).parent / "sql" / "ddl"
    ddl_files = sorted(DDL_DIR.glob("*.sql"))

    if not ddl_files:
        logger.warning(f"Aucun fichier SQL trouvé dans {DDL_DIR}")
    else:
        for ddl_file in ddl_files:
            logger.info(f"Exécution de {ddl_file.name} ...")
            sql = ddl_file.read_text()
            try:
                client.query(sql).result()
                logger.info(f"{ddl_file.name} exécuté avec succès.")
            except Exception as e:
                logger.error(f"Erreur lors de l'exécution de {ddl_file.name}: {e}")

    # Execute DML SQL files for dim date
    DIM_DATE_SQL = DDL_DIR.parent / "dml" / "010_insert_dim_date.sql"
    if DIM_DATE_SQL.is_file():
        logger.info(f"Peuplement de dim.date via {DIM_DATE_SQL.name} ...")
        sql = DIM_DATE_SQL.read_text()
        try:
            client.query(sql).result()
            logger.info("dim.date peuplée avec succès.")
        except Exception as e:
            logger.error(f"Erreur lors du peuplement de dim.date: {e}")
    else:
        logger.warning(f"Fichier introuvable : {DIM_DATE_SQL}")

    logger.info("Initialisation des tables BigQuery terminée")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    try:
        init_bigquery()
    except Exception as e:
        logger.exception("Initialisation BigQuery échouée")
        exit(1)