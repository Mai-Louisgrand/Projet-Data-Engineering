'''
Load OWID processed data from GCS to BigQuery temporary staging table.

- Source: GCS (processed parquet)
- Target: BigQuery temporary staging table
- Mode: Full refresh (WRITE_TRUNCATE)
'''
from google.cloud import bigquery
from src.config.settings import GCP_PROJECT, GCS_BUCKET_NAME, PROCESSED_PREFIX, BQ_DATASET_STAGING
from src.utils.logging import setup_logging

# Logging configuration
logger = setup_logging()

# ============================
# Main function
# ============================
def run_load():
    '''
    Load processed OWID data from GCS into BigQuery temporary staging table.
    '''
    # BigQuery Connection
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        logger.info("Connexion BigQuery établie")
    except Exception as e:
        logger.error(f"Erreur connexion BigQuery: {e}")
        raise

    # Source & Target
    uri = f"gs://{GCS_BUCKET_NAME}/{PROCESSED_PREFIX}/*.parquet"
    table_id = f"{GCP_PROJECT}.{BQ_DATASET_STAGING}.stg_owid_covid_tmp"

    logger.info(f"Source GCS : {uri}")
    logger.info(f"Table cible : {table_id}")

    # Load configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
        bigquery.SchemaField("iso_code", "STRING"),
        bigquery.SchemaField("continent", "STRING"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("total_vaccinations", "INT64"),
        bigquery.SchemaField("people_vaccinated", "INT64"),
        bigquery.SchemaField("people_fully_vaccinated", "INT64"),
        bigquery.SchemaField("total_boosters", "INT64"),
        bigquery.SchemaField("new_vaccinations", "INT64"),
        bigquery.SchemaField("new_vaccinations_smoothed", "FLOAT64"),
        bigquery.SchemaField("population", "INT64"),
        bigquery.SchemaField("total_vaccinations_per_hundred", "FLOAT64"),
        bigquery.SchemaField("people_vaccinated_per_hundred", "FLOAT64"),
        bigquery.SchemaField("people_fully_vaccinated_per_hundred", "FLOAT64"),
        bigquery.SchemaField("total_boosters_per_hundred", "FLOAT64"),
        bigquery.SchemaField("load_date", "DATE")
    ]
    )

    # Execute load job
    try:
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )

        logger.info("Lancement du job de chargement...")
        load_job.result()  # wait for completion

        logger.info("Chargement BigQuery terminé avec succès")

    except Exception as e:
        logger.error("Erreur lors du chargement GCS → BigQuery", exc_info=True)
        raise