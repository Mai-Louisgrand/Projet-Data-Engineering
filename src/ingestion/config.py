# Configuration globale pour l'ingestion des données OWID COVID-19.
# - chemins vers dossiers RAW et logs
# - URL officielle des données
# - date d'exécution
# - paramètres généraux pour l'ingestion

from pathlib import Path
from datetime import date

# Dossier racine du projet
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Dossiers pour stockage et logs
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "owid_covid"
LOG_PATH = PROJECT_ROOT / "logs"

# Dataset OWID COVID-19
OWID_COVID_CSV_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

# Date du jour pour historisation des données RAW
INGESTION_DATE = date.today().isoformat()

# Fonctions et paramètres optionnels
HTTP_CHUNK_SIZE = 1024 * 1024  # 1Mo

# Logging format
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
