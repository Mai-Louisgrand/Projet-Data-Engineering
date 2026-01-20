# Script de profiling PySpark du dataset OWID COVID.

import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, round as spark_round

# ============================
# Configuration
# ============================
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_PATH = PROJECT_ROOT / "data/raw/owid_covid"
PROFILING_OUTPUT_PATH = PROJECT_ROOT / "data/profiling/owid_covid"

# ============================
# Setup du Logging
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# ============================
# Fonctions de profiling
# ============================
def create_spark_session() -> SparkSession:
    # Initialiser SparkSession locale pour le profiling
    return (
        SparkSession.builder
        .appName("OWID_COVID_Profiling")
        .master("local[*]")
        .getOrCreate()
    )

def load_raw_data(spark: SparkSession):
    # Charger le dernier CSV OWID disponible depuis le dossier raw
    latest_ingestion = max(RAW_DATA_PATH.glob("ingestion_date=*"))
    csv_path = latest_ingestion / "owid_covid_data.csv"

    logger.info(f"Chargement des données depuis {csv_path}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(csv_path))
    )
    return df

def show_basic_info(df):
    # Afficher infos de base du dataset
    logger.info("Affichage du schéma du DataFrame")
    df.printSchema()

    row_count = df.count()
    col_count = len(df.columns)

    logger.info(f"Nombre de lignes : {row_count}")
    logger.info(f"Nombre de colonnes : {col_count}")

def sample_data(df, n=20):
    # Afficher un échantillon du dataset
    logger.info(f"Affichage d'un échantillon de {n} lignes")
    df.show(n, truncate=False)

def compute_null_statistics(df):
    # Calcul statistiques nulls pour chaque colonne
    total_rows = df.count()

    # Créer un DataFrame avec null_count par colonne
    null_counts_exprs = [
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ]

    null_counts_df = df.select(null_counts_exprs)

    # Transposer le DataFrame Spark
    stack_expr = ", ".join([f"'{c}', {c}" for c in df.columns])
    null_stats_spark = null_counts_df.selectExpr(f"stack({len(df.columns)}, {stack_expr}) as (column, null_count)")

    # Ajouter le pourcentage
    null_stats_spark = null_stats_spark.withColumn("null_percentage", spark_round(col("null_count") / lit(total_rows) * 100, 2))

    return null_stats_spark

def compute_numeric_statistics(df):
    # Calcul statistiques descriptives pour les colonnes numériques
    logger.info("Calcul des statistiques descriptives numériques")
    return df.describe()

# ============================
# Résultats
# ============================
def save_schema(df):
    # Sauvegarder schéma du DataFrame Spark dans un fichier texte pour consultation hors Spark
    schema_path = PROFILING_OUTPUT_PATH / "schema.txt"

    logger.info(f"Sauvegarde du schéma dans {schema_path}")

    with open(schema_path, "w") as f:
        f.write(df._jdf.schema().treeString())

def save_sample_data(df, n=100):
    # Sauvegarder un échantillon du dataset pour vérification manuelle
    sample_path = PROFILING_OUTPUT_PATH / "sample_data"

    logger.info(f"Sauvegarde d'un échantillon de {n} lignes dans {sample_path}")

    (
        df.limit(n)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(sample_path))
    )

def save_profiling_results(null_stats, numeric_stats):
    # Sauvegarder les résultats du profiling
    PROFILING_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    null_stats_path = PROFILING_OUTPUT_PATH / "null_statistics"
    numeric_stats_path = PROFILING_OUTPUT_PATH / "numeric_statistics"

    logger.info(f"Sauvegarde des statistiques de nulls dans {null_stats_path}")
    (
        null_stats
        .coalesce(1)  # pour créer un seul fichier CSV
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(null_stats_path))
    )

    logger.info(f"Sauvegarde des statistiques numériques : {numeric_stats_path}")
    (
        numeric_stats
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(numeric_stats_path))
    )

# ============================
# Main
# ============================
def main():
    logger.info("Démarrage du profiling OWID COVID")

    spark = create_spark_session()
    df = load_raw_data(spark)

    show_basic_info(df)
    sample_data(df)

    save_schema(df)
    save_sample_data(df)

    null_stats = compute_null_statistics(df)
    numeric_stats = compute_numeric_statistics(df)

    save_profiling_results(null_stats, numeric_stats)

    spark.stop()
    logger.info("Profiling terminé avec succès")

if __name__ == "__main__":
    main()
