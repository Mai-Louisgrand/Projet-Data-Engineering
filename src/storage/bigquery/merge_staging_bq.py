'''
Merge staging_tmp into final staging table in BigQuery

- Performs upsert based on unique keys (iso_code, date)
- Ensures final staging table is consistent and ready for downstream
'''
from google.cloud import bigquery
from src.config.settings import GCP_PROJECT, BQ_DATASET_STAGING
from src.utils.logging import setup_logging

# Logging configuration
logger = setup_logging()

# ============================
# Main function
# ============================
def run_merge():
    '''
    Merge staging_tmp into staging table with an upsert logic.
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
    MERGE `{GCP_PROJECT}.{BQ_DATASET_STAGING}.stg_owid_covid` T
    USING `{GCP_PROJECT}.{BQ_DATASET_STAGING}.stg_owid_covid_tmp` S
    ON T.iso_code = S.iso_code
    AND T.date = S.date

    WHEN MATCHED THEN
      UPDATE SET
        continent = S.continent,
        location = S.location,
        total_vaccinations = S.total_vaccinations,
        people_vaccinated = S.people_vaccinated,
        people_fully_vaccinated = S.people_fully_vaccinated,
        total_boosters = S.total_boosters,
        new_vaccinations = S.new_vaccinations,
        new_vaccinations_smoothed = S.new_vaccinations_smoothed,
        population = S.population,
        total_vaccinations_per_hundred = S.total_vaccinations_per_hundred,
        people_vaccinated_per_hundred = S.people_vaccinated_per_hundred,
        people_fully_vaccinated_per_hundred = S.people_fully_vaccinated_per_hundred,
        total_boosters_per_hundred = S.total_boosters_per_hundred,
        load_date = S.load_date

    WHEN NOT MATCHED THEN
      INSERT ROW
    """

    try:
        logger.info("Lancement du MERGE staging_tmp → staging")
        client.query(query).result()
        logger.info("MERGE terminé avec succès")

    except Exception as e:
        logger.error("Erreur lors du MERGE", exc_info=True)
        raise