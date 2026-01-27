# Module PostgreSQL Sink

# - Gérer l'écriture des données Spark Structured Streaming vers PostgreSQL
# - Vérifier l'existence des tables cibles avant écriture

import logging
from typing import Dict, List
from pyspark.sql import DataFrame
import psycopg2
from psycopg2.extensions import connection as PGConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# Connexion PostgreSQL
# =========================
def get_postgres_connection(host: str, port: int, database: str, user: str, password: str) -> PGConnection:
    # Crée et retourne une connexion à PostgreSQL
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
    
# =========================
# Vérification existence table
# =========================
def table_exists(conn: PGConnection, table: str) -> bool:
    # Vérification de l'existence d'une table PostgreSQL, retourne True si la table existe, False sinon
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

# =========================
# Écriture DataFrame Spark → PostgreSQL (via JDBC)
# =========================
def write_dataframe_to_postgres(df: DataFrame, table: str, jdbc_url: str, jdbc_properties: Dict[str, str], mode: str = "overwrite") -> None:
    # Écriture d'un DataFrame Spark dans PostgreSQL après vérification de la table
    try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .options(**jdbc_properties) \
            .mode(mode) \
            .save()

        logger.info("DataFrame Spark écrit avec succès dans %s (mode=%s)", table, mode)

    except Exception:
        logger.exception("Erreur lors de l'écriture Spark JDBC dans %s", table)
        raise

# =========================
# UPSERT depuis table temporaire
# =========================
def upsert_from_temp_table(temp_table: str, target_table: str, conflict_cols: List[str], update_cols: List[str], pg_conn: PGConnection):
    # UPSERT les données d'une table temporaire vers la table finale
    # conflict_cols : colonnes clés uniques
    # update_cols : colonnes à mettre à jour si conflit
    
    conflict_cols_str = ", ".join(conflict_cols)
    set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
    sql = f"""
        INSERT INTO {target_table} (
            iso_code, continent, location, date, total_vaccinations, people_vaccinated, people_fully_vaccinated,
            total_boosters, new_vaccinations, new_vaccinations_smoothed,
            population, total_vaccinations_per_hundred,
            people_vaccinated_per_hundred, people_fully_vaccinated_per_hundred,
            total_boosters_per_hundred, load_date
        )
        SELECT
            iso_code, continent, location, date, total_vaccinations, people_vaccinated, people_fully_vaccinated,
            total_boosters, new_vaccinations, new_vaccinations_smoothed,
            population, total_vaccinations_per_hundred,
            people_vaccinated_per_hundred, people_fully_vaccinated_per_hundred,
            total_boosters_per_hundred, load_date
        FROM {temp_table}
        ON CONFLICT ({conflict_cols_str}) DO UPDATE
        SET {set_clause};
    """
    try:
        with pg_conn.cursor() as cur:
            cur.execute(sql)
        pg_conn.commit()
        logger.info("UPSERT effectué de %s → %s", temp_table, target_table)
    
    except Exception:
        pg_conn.rollback()
        logger.exception("Erreur UPSERT table %s", target_table)
        raise