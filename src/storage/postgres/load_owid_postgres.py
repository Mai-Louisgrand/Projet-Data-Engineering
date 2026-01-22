# Chargement des données OWID COVID-19 – Vaccination

# Fonctionnalités :
# - Lecture des données Parquet transformées et partitionnées par pays (iso_code)
# - Enrichissement avec métadonnées techniques (date de chargement)
# - Chargement batch dans PostgreSQL via JDBC pour alimenter la table de staging

import logging
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# ============================
# Setup du logging
# ============================
logging.basicConfig(
    level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
        )
logger = logging.getLogger(__name__)

# ============================
# Configuration
# ============================
# Paramètres PostgreSQL
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "covid_dw"
PG_USER = "data"
PG_PASSWORD = "data"
PG_TABLE = "staging.stg_owid_covid"

# Dossier Parquet
PARQUET_PATH = "data/processed/owid_covid"

# Spark session
spark = (
    SparkSession.builder
    .appName("Load OWID COVID Parquet to PostgreSQL Staging")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
    )

logger.info("Session Spark initialisée")

# ============================
# Chargement des données vers la table de staging
# ============================
try:
    # Lecture des fichiers Parquet
    logger.info("Lecture des fichiers Parquet de data/processed")
    df = spark.read.parquet(PARQUET_PATH) #lit tous les fichiers Parquet sous PARQUET_PATH, en prenant en compte les partitions

    logger.info(f"Nombre de lignes lues : {df.count()}")

    # Ajout des métadonnées de chargement dans une nouvelle colonne
    df = df.withColumn("load_date", lit(date.today()))

    # Optimisation écriture JDBC
    df = df.repartition(4) # définit nb partitions à 4 pour éviter de surcharger PostgreSQL

    # Écriture dans PostgreSQL (table de staging)
    logger.info("Chargement des données dans la table de staging PostgreSQL")

    (
    df.write
        .format("jdbc") # vers base relationnelle

        # connection à la BDD
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", PG_TABLE)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")

        # supprime et recrée la table pour insérer les données
        .mode("overwrite")

        # lance l'écriture
        .save()
            )

    logger.info("Chargement des données terminé avec succès")

except Exception as e:
    logger.error("Erreur lors du chargement des données dans PostgreSQL", exc_info=True)
    raise

finally:
    spark.stop()
    logger.info("Session Spark arrêtée")


