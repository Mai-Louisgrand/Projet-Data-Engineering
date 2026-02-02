'''
Global configuration for OWID COVID-19 data ingestion.

This module centralizes all configuration parameters required for the batch ingestion process, including:
- project root resolution
- raw data and logging paths
- official OWID COVID-19 dataset URL
- ingestion execution date for data historization
- generic ingestion parameters (e.g. HTTP chunk size, logging format)

The configuration is designed to support reproducible and traceable data ingestion runs in a production-like environment.
'''

from pathlib import Path
from datetime import date

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Raw data storage and logs directories
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "owid_covid"
LOG_PATH = PROJECT_ROOT / "logs"

# OWID COVID-19 dataset
OWID_COVID_CSV_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

# Execution date used for raw data historization
INGESTION_DATE = date.today().isoformat()

# Optional ingestion parameters
HTTP_CHUNK_SIZE = 1024 * 1024  # 1Mo

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
