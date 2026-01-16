# Script d'ingestion batch des données OWID COVID-19.

# Fonctionnalités :
# - Téléchargement du CSV officiel OWID
# - Création dossier RAW daté pour historisation
# - Écriture de logs pour suivi des exécutions

import requests # pour télécharger fichier depuis internet
from pathlib import Path
from datetime import datetime, timezone
import logging
from config import RAW_DATA_PATH, LOG_PATH, OWID_COVID_CSV_URL, INGESTION_DATE, HTTP_CHUNK_SIZE, LOG_FORMAT

# ============================
# Setup du logging
# ============================
# création fichier logs
LOG_PATH.mkdir(parents=True, exist_ok=True)
log_file = LOG_PATH / f"ingestion_{INGESTION_DATE}.log"

# logs écrits dans fichier .log
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format=LOG_FORMAT
)

# logs affichés dans console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter(LOG_FORMAT)
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# ============================
# Fonctions d'ingestion
# ============================
def create_output_dir() -> Path:
    # Crée le dossier RAW pour la date d'ingestion
    output_dir = RAW_DATA_PATH / f"ingestion_date={INGESTION_DATE}"
    output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Dossier RAW créé : {output_dir}")
    return output_dir

def download_csv(output_dir: Path) -> Path:
    # Télécharge le CSV OWID COVID et l'enregistre dans le dossier RAW
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
    start_time = datetime.now(timezone.utc)
    logging.info(f"Début de l'ingestion OWID COVID : {start_time.isoformat()}")

    output_dir = create_output_dir()
    download_csv(output_dir)

    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).seconds
    logging.info(f"Fin de l'ingestion. Durée : {duration} secondes")

if __name__ == "__main__":
    main()
