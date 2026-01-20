# Transformation des données OWID COVID-19 – Vaccination

# Fonctionnalités :
# - Nettoyage et structuration des données de vaccination par pays
# - Recalcul des indicateurs normalisés par populatio
# - Préparation table analytique fiable pour downstream

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ============================
# Configuration
# ============================

# TODO (Airflow):
# - Paramétrer dynamiquement la date d’ingestion
# - Passer RAW_PATH via le DAG
RAW_PATH = "data/raw/owid_covid/"
OUTPUT_PATH = "data/processed/owid_covid/"

# Spark Session
spark = (
    SparkSession.builder
    .appName("OWID_COVID_Transformation")
    .getOrCreate()
)

# ============================
# Colonnes utiles
# ============================

# colonnes nécessaires à l'identification des enregistrements
CRITICAL_COLUMNS = [
    "iso_code",
    "continent",
    "location",
    "date"
]

# colonnes liées au périmètre métier
BUSINESS_COLUMNS = [
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "total_boosters",
    "new_vaccinations",
    "new_vaccinations_smoothed",
    "population"
]

# colonnes numériques nécessitant des contrôles de cohérence
NUMERIC_COLUMNS = [
    "total_vaccinations",
    "people_vaccinated",
    "people_fully_vaccinated",
    "total_boosters",
    "new_vaccinations",
    "new_vaccinations_smoothed",
    "population"
]

# ============================
# Lecture données RAW
# ============================
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(RAW_PATH)
)

# ============================
# Transformations
# ============================

# 1. Filtrage lignes critiques
df = (
    df
    .filter(F.col("iso_code").isNotNull())
    .filter(F.col("location").isNotNull())
    .filter(F.col("date").isNotNull())
)

# 2. Exclusion des agrégats OWID
df = df.filter(~F.col("iso_code").startswith("OWID_"))

# 3. Réduction du schéma
df = df.select(*(CRITICAL_COLUMNS + BUSINESS_COLUMNS))

# 4. Nettoyage préventif des valeurs numériques
# Hypothèses :
# - Les métriques vaccination ne peuvent pas être négatives
# - Les valeurs nulles sont conservées
for col_name in NUMERIC_COLUMNS:
    df = df.withColumn(
        col_name,
        F.when(F.col(col_name) < 0, None).otherwise(F.col(col_name))
    )

# 5. Recalcul indicateurs normalisés
def per_hundred(metric_col: str):
    # Calcul d'un indicateur pour 100 habitants (protection division par zéro et conservation des nulls)
    return (
        F.when(
            (F.col(metric_col).isNotNull()) &
            (F.col("population").isNotNull()) &
            (F.col("population") > 0),
            (F.col(metric_col) / F.col("population")) * 100
        )
        .otherwise(None)
        .cast(DoubleType())
    )


df = (
    df
    .withColumn(
        "total_vaccinations_per_hundred",
        per_hundred("total_vaccinations")
    )
    .withColumn(
        "people_vaccinated_per_hundred",
        per_hundred("people_vaccinated")
    )
    .withColumn(
        "people_fully_vaccinated_per_hundred",
        per_hundred("people_fully_vaccinated")
    )
    .withColumn(
        "total_boosters_per_hundred",
        per_hundred("total_boosters")
    )
)

# ============================
# Ecriture données Processed
# ============================
(
    df
    # Redistribuer lignes par iso_code pour optimiser les opérations Spark
    .repartition("iso_code")  

    # Créer un dossier par pays avec fichiers Parquets
    .write
    .mode("overwrite")
    .partitionBy("iso_code")
    .parquet(OUTPUT_PATH)
)
