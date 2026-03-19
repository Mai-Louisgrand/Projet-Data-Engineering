'''
Batch ingestion script for OWID COVID-19 data

This module is responsible for:
- downloading the OWID COVID-19 CSV dataset from the official OWID GitHub repository
- streaming the dataset directly to a Google Cloud Storage (GCS) bucket
- organizing raw data in a date-partitioned structure inside the data lake
- logging execution details for traceability and monitoring
'''

import requests # to download remote files
from datetime import datetime, timezone
import logging
from google.cloud.exceptions import GoogleCloudError

from src.storage.gcs.client import GCSClient
from src.config.settings import OWID_COVID_CSV_URL, INGESTION_DATE, GCS_BUCKET_NAME, RAW_PREFIX, LOG_FORMAT, LOG_PATH

# ============================
# Logging configuration
# ============================
LOG_PATH.mkdir(parents=True, exist_ok=True)
log_file = LOG_PATH / f"ingestion_{INGESTION_DATE}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format=LOG_FORMAT
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter(LOG_FORMAT)
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# ============================
# Ingestion helper functions
# ============================
def upload_to_gcs():
    '''
    Downloads the OWID dataset and uploads it directly to a GCS bucket.
    This approach avoids any intermediate local storage.
    '''
    try:
        logging.info(f"Téléchargement depuis {OWID_COVID_CSV_URL}")

        response = requests.get(OWID_COVID_CSV_URL, stream=True, timeout=60)
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur lors du téléchargement : {e}")
        raise

    try:
        # Initialize the GCS client
        gcs = GCSClient(GCS_BUCKET_NAME)

        destination_path = (
            f"{RAW_PREFIX}/ingestion_date={INGESTION_DATE}/"
            f"owid_covid_data.csv"
        )

        # Upload the stream directly to GCS
        gcs.upload_file(response.raw, destination_path)

        logging.info("Upload vers GCS réussi")

    except GoogleCloudError as e:
        logging.error(f"Erreur GCS : {e}")
        raise

# ============================
# Main
# ============================
def main():
    '''
    Execute the OWID COVID-19 batch ingestion workflow.
    '''
    start_time = datetime.now(timezone.utc)
    logging.info(f"Début de l'ingestion OWID COVID : {start_time.isoformat()}")

    upload_to_gcs()

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).seconds
    logging.info(f"Fin de l'ingestion. Durée : {duration} secondes")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    main()
