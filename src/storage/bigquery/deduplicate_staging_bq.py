'''
Deduplicate staging_tmp table in BigQuery.

- Ensures one row per (iso_code, date)
- Keeps the most recent record based on load_date
'''

from google.cloud import bigquery
from src.config.settings import GCP_PROJECT, BQ_DATASET_STAGING
from src.utils.logging import setup_logging

# Logging configuration
logger = setup_logging()

# ============================
# Main function
# ============================
def run_deduplication():
    '''
    Deduplicate staging_tmp table to ensure uniqueness on (iso_code, date).
    '''
    # BigQuery Connection
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        logger.info("Connexion BigQuery établie")
    except Exception as e:
        logger.error(f"Erreur connexion BigQuery: {e}")
        raise

    # SQL query
    table_tmp = f"{GCP_PROJECT}.{BQ_DATASET_STAGING}.stg_owid_covid_tmp"

    query = f"""
    CREATE OR REPLACE TABLE `{table_tmp}` AS
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY iso_code, date
                   ORDER BY load_date DESC
               ) AS row_num
        FROM `{table_tmp}`
    )
    WHERE row_num = 1
    """

    try:
        logger.info("Déduplication de la table staging_tmp...")
        client.query(query).result()
        logger.info("Déduplication terminée avec succès")

    except Exception as e:
        logger.error("Erreur lors de la déduplication", exc_info=True)
        raise


# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    try:
        run_deduplication()
    except Exception:
        logger.exception("Echec de la déduplication")
        exit(1)