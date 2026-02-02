'''
Batch ingestion script for OWID COVID-19 data.

This module is responsible for:
- downloading the OWID COVID-19 CSV dataset from the official OWID GitHub repository
- creating a date-partitioned RAW directory for data historization
- logging ingestion execution details for traceability and monitoring
'''

import requests # to download remote files
from pathlib import Path
from datetime import datetime, timezone
import logging
from config import RAW_DATA_PATH, LOG_PATH, OWID_COVID_CSV_URL, INGESTION_DATE, HTTP_CHUNK_SIZE, LOG_FORMAT

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
def create_output_dir() -> Path:
    '''
    Create the RAW output directory for the current ingestion date.

    :return: Path to the date-partitioned RAW ingestion directory
    '''
    output_dir = RAW_DATA_PATH / f"ingestion_date={INGESTION_DATE}"
    output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Dossier RAW créé : {output_dir}")
    return output_dir

def download_csv(output_dir: Path) -> Path:
    '''
    Download the OWID COVID-19 CSV dataset and store it in the RAW directory.

    :param output_dir: Target directory where the CSV file will be stored
    :return: Path to the downloaded CSV file
    '''
    try:
        logging.info(f"Téléchargement du CSV depuis {OWID_COVID_CSV_URL}...")
        response = requests.get(OWID_COVID_CSV_URL, stream=True)
        response.raise_for_status()

        output_file = output_dir / "owid_covid_data.csv"
        with open(output_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=HTTP_CHUNK_SIZE):
                f.write(chunk)

        logging.info(f"Fichier téléchargé et sauvegardé : {output_file}")
        return output_file
    except Exception as e:
        logging.error(f"Erreur lors du téléchargement : {e}")
        raise

# ============================
# Main
# ============================
def main():
    '''
    Execute the OWID COVID-19 batch ingestion workflow.

    This function orchestrates directory creation, data download,
    and execution logging.
    '''
    start_time = datetime.now(timezone.utc)
    logging.info(f"Début de l'ingestion OWID COVID : {start_time.isoformat()}")

    output_dir = create_output_dir()
    download_csv(output_dir)

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).seconds
    logging.info(f"Fin de l'ingestion. Durée : {duration} secondes")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    main()
