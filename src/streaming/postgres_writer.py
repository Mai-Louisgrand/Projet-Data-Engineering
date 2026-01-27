# Module PostgreSQL Sink

# - Gérer l'écriture des données Spark Structured Streaming vers PostgreSQL
# - Vérifier l'existence des tables cibles avant écriture

import logging
from typing import Dict
from pyspark.sql import DataFrame
import psycopg2
from psycopg2.extensions import connection as PGConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection à PostgreSQL : crée et retourne une connexion à PostgreSQL
def get_postgres_connection(host: str, port: int, database: str, user: str, password: str) -> PGConnection:
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password
        )
        conn.autocommit = False
        return conn

    except Exception as e:
        logger.error("Impossible de se connecter à PostgreSQL", exc_info=True)
        raise
    
# Vérification de l'existence d'une table PostgreSQL, retourne True si la table existe, False sinon
def table_exists(conn: PGConnection, table: str) -> bool:
    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        )
    """

    try:
        schema, table_name = table.split(".")

        with conn.cursor() as cur:
            cur.execute(sql, (schema, table_name))
            result = cur.fetchone()

            if result is None:
                raise RuntimeError("Résultat SQL vide")

            return result[0]

    except Exception:
        logger.error("Erreur lors de la vérification de la table %s", table, exc_info=True)
        raise

# Écriture d'un DataFrame Spark dans PostgreSQL après vérification de la table
def write_to_postgres(df: DataFrame, table: str, jdbc_url: str, jdbc_properties: Dict[str, str], pg_conn: PGConnection) -> None:
    try:
        if not table_exists(pg_conn, table):
            raise RuntimeError(f"Table PostgreSQL inexistante : {table}")

        (
            df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table)
            .options(**jdbc_properties)
            .mode("append")
            .save()
        )

        pg_conn.commit()
        logger.info("Batch écrit avec succès dans %s", table)

    except Exception:
        pg_conn.rollback()
        logger.error("Erreur lors de l'écriture du batch", exc_info=True)
        raise