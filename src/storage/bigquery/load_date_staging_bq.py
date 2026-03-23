'''
Populate the load_date column in BigQuery temporary staging table

- Sets load_date = CURRENT_DATE() for all rows
'''
from google.cloud import bigquery
from src.config.settings import GCP_PROJECT, BQ_DATASET_STAGING
from src.utils.logging import setup_logging

# Logging configuration
logger = setup_logging()

# ============================
# Main function
# ============================
def populate_load_date():
    '''
    Update the staging table to set load_date for all rows.
    '''
    # BigQuery Connection
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        logger.info("Connexion BigQuery établie")
    except Exception as e:
        logger.error(f"Erreur connexion BigQuery: {e}")
        raise

    # SQL query
    query = f"""
    UPDATE `{GCP_PROJECT}.{BQ_DATASET_STAGING}.stg_owid_covid_tmp`
    SET load_date = CURRENT_DATE()
    WHERE load_date IS NULL
    """

    try:
        logger.info("Peuplement de la colonne load_date en cours...")
        client.query(query).result()  # Waits for the update to finish
        logger.info("Colonne load_date mise à jour avec succès")

    except Exception as e:
        logger.error("Erreur lors de la mise à jour de load_date", exc_info=True)
        raise

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    try:
        populate_load_date()
    except Exception as e:
        logger.exception("Peuplement load_date échoué")
        exit(1)