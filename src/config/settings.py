'''
Global configuration for OWID COVID-19 data pipeline

Centralizes:
- dataset source
- GCS storage paths (raw, processed)
- execution parameters
'''
from datetime import date
from pathlib import Path

# Dataset
OWID_COVID_CSV_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# GCS configuration
GCS_BUCKET_NAME = "owid-datalake-dev-eu-2026"

RAW_PREFIX = "raw/owid_covid"
RAW_PATH = f"gs://{GCS_BUCKET_NAME}/raw/owid_covid"
PROCESSED_PREFIX = "processed/owid_covid"
PROCESSED_PATH = f"gs://{GCS_BUCKET_NAME}/processed/owid_covid"

# Logging configuration
LOG_PATH = PROJECT_ROOT / "logs"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# Execution
INGESTION_DATE = date.today().isoformat()
