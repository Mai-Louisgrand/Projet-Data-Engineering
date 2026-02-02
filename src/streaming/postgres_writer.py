'''
PostgreSQL Sink Module for OWID Streaming Pipeline

This module provides utilities to:
 - Establish and manage PostgreSQL connections
 - Verify table existence
 - Write Spark DataFrames to PostgreSQL via JDBC
 - Perform UPSERT operations from a temporary table
'''

import logging
from typing import Dict, List
from pyspark.sql import DataFrame
import psycopg2
from psycopg2.extensions import connection as PGConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# PostgreSQL Connection
# =========================
def get_postgres_connection(host: str, port: int, database: str, user: str, password: str) -> PGConnection:
    '''
    Create and return a PostgreSQL connection.

    :param host: PostgreSQL host
    :param port: PostgreSQL port
    :param database: database name
    :param user: username
    :param password: password
    :return: psycopg2 connection object
    '''
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
# Table existence check
# =========================
def table_exists(conn: PGConnection, table: str) -> bool:
    '''
    Check if a PostgreSQL table exists.

    :param conn: active PostgreSQL connection
    :param table: full table name in the format 'schema.table'
    :return: True if table exists, False otherwise
    '''
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
# Spark DataFrame → PostgreSQL
# =========================
def write_dataframe_to_postgres(df: DataFrame, table: str, jdbc_url: str, jdbc_properties: Dict[str, str], mode: str = "overwrite") -> None:
    '''
    Write a Spark DataFrame to a PostgreSQL table via JDBC.

    :param df: Spark DataFrame to write
    :param table: target PostgreSQL table (schema.table)
    :param jdbc_url: JDBC URL to PostgreSQL
    :param jdbc_properties: dictionary with JDBC properties (user, password, driver, etc.)
    :param mode: write mode ("overwrite", "append", etc.)
    '''
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
# UPSERT from temporary table
# =========================
def upsert_from_temp_table(temp_table: str, target_table: str, conflict_cols: List[str], update_cols: List[str], pg_conn: PGConnection):
    '''
    Perform an UPSERT from a temporary table to the target PostgreSQL table.

    :param temp_table: name of the temporary table
    :param target_table: name of the target table
    :param conflict_cols: list of columns to detect conflicts (unique keys)
    :param update_cols: list of columns to update on conflict
    :param pg_conn: active PostgreSQL connection
    '''
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