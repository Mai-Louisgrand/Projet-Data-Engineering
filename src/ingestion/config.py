'''
Global configuration for OWID COVID-19 data ingestion.

This module centralizes all configuration parameters required for the batch ingestion process, including:
- project root resolution
- logging configuration (paths, format)
- official OWID COVID-19 dataset URL
- ingestion execution date for data historization
- Google Cloud Storage (GCS) bucket configuration
'''

from pathlib import Path
from datetime import date

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Logging configuration
LOG_PATH = PROJECT_ROOT / "logs"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

# OWID COVID-19 dataset
OWID_COVID_CSV_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

# Execution date used for raw data historization
INGESTION_DATE = date.today().isoformat()

# GCS configuration
GCS_BUCKET_NAME = "owid-datalake-dev-eu-2026"


