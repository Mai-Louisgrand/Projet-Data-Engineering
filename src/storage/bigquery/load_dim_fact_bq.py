'''
Populate dimension and fact tables in BigQuery from the staging table

Features:
 - Verify BigQuery connectivity
 - Execute all DML SQL files (excluding dim_date)
'''
from pathlib import Path
from google.cloud import bigquery
from src.config.settings import GCP_PROJECT
from src.utils.logging import setup_logging

# Logging configuration
logger = setup_logging()

# ============================
# Main function
# ============================
def run_dml():
    '''
    Execute all DML scripts to populate dimension and fact tables in BigQuery.
    Ignores 010_insert_dim_date.sql since dim_date is already populated.
    '''
    
    # Ensure BigQuery connectivity
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        client.list_datasets() 
        logger.info("Connexion BigQuery établie")
    except Exception as e:
        logger.error(f"Impossible de se connecter à BigQuery: {e}")
        raise

    # Locate DML files
    DML_DIR = Path(__file__).parent / "sql" / "dml"
    if not DML_DIR.exists():
        logger.error(f"Dossier DML introuvable : {DML_DIR}")
        return

    dml_files = sorted(DML_DIR.glob("[0-9]*_*.sql"))
    if not dml_files:
        logger.warning(f"Aucun fichier DML trouvé dans {DML_DIR}")
        return

    # Execute DML scripts
    for dml_file in dml_files:
        if dml_file.name == "010_insert_dim_date.sql":
            logger.info(f"Ignoré (dim_date déjà exécutée) : {dml_file.name}")
            continue

        logger.info(f"Exécution de {dml_file.name} ...")
        sql = dml_file.read_text()
        try:
            client.query(sql).result()
            logger.info(f"{dml_file.name} exécuté avec succès")
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution de {dml_file.name}: {e}", exc_info=True)
            raise

    logger.info("Peuplement des tables dim/fact terminé avec succès")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    try:
        run_dml()
    except Exception as e:
        logger.exception("Peuplement des tables BigQuery échoué")
        exit(1)