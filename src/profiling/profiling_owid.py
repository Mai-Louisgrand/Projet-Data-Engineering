'''
PySpark profiling script for the OWID COVID-19 dataset.

Performs basic profiling of the raw dataset:
- Shows schema and row/column counts
- Samples data
- Computes null statistics per column
- Computes descriptive statistics for numeric columns
- Saves schema, sample, and profiling results to .txt and CSV files
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, round as spark_round
from src.config.settings import PROFILING_OUTPUT_PATH, INGESTION_DATE, GCS_BUCKET_NAME, RAW_PREFIX
from src.utils.spark import get_spark
from src.utils.logging import setup_logging

# Logging configuration
logger = setup_logging()

# ============================
# Profiling helper functions
# ============================
def load_raw_data(spark: SparkSession):
    '''
    Load the latest raw OWID CSV from the ingestion folder.

    :param spark: SparkSession object
    :return: DataFrame containing the raw dataset
    '''
    gcs_path = (
        f"gs://{GCS_BUCKET_NAME}/{RAW_PREFIX}/"
        f"ingestion_date={INGESTION_DATE}/owid_covid_data.csv"
    )

    try:
        logger.info(f"Chargement des données depuis {gcs_path}")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(gcs_path)
        )
        return df
    except Exception as e:
        logger.error(f"Erreur lors de la lecture des données depuis GCS : {e}")
        raise

# ============================
# Profiling functions
# ============================
def show_basic_info(df):
    '''
    Display basic information about the dataset: schema, number of rows and columns.

    :param df: Spark DataFrame
    '''
    logger.info("Affichage du schéma du DataFrame")
    df.printSchema()

    row_count = df.count()
    col_count = len(df.columns)

    logger.info(f"Nombre de lignes : {row_count}")
    logger.info(f"Nombre de colonnes : {col_count}")

def sample_data(df, n=20):
    '''
    Show a sample of the dataset.

    :param df: Spark DataFrame
    :param n: Number of rows to display
    '''
    logger.info(f"Affichage d'un échantillon de {n} lignes")
    df.show(n, truncate=False)

def compute_null_statistics(df):
    '''
    Compute the number and percentage of nulls per column.

    :param df: Spark DataFrame
    :return: DataFrame with columns 'column', 'null_count', 'null_percentage'
    '''
    total_rows = df.count()
    null_counts_exprs = [
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ]
    null_counts_df = df.select(null_counts_exprs)

    stack_expr = ", ".join([f"'{c}', {c}" for c in df.columns]) # Transpose the Spark DataFrame
    null_stats_spark = null_counts_df.selectExpr(f"stack({len(df.columns)}, {stack_expr}) as (column, null_count)")
    null_stats_spark = null_stats_spark.withColumn("null_percentage", spark_round(col("null_count") / lit(total_rows) * 100, 2))

    return null_stats_spark

def compute_numeric_statistics(df):
    '''
    Compute descriptive statistics for numeric columns.

    :param df: Spark DataFrame
    :return: DataFrame with descriptive statistics
    '''
    logger.info("Calcul des statistiques descriptives numériques")
    return df.describe()

# ============================
# Results saving functions
# ============================
def save_schema(df):
    '''
    Save the Spark DataFrame schema to a text file.

    :param df: Spark DataFrame
    '''
    schema_path = PROFILING_OUTPUT_PATH / "schema.txt"

    logger.info(f"Sauvegarde du schéma dans {schema_path}")

    with open(schema_path, "w") as f:
        f.write(df._jdf.schema().treeString())

def save_sample_data(df, n=100):
    '''
    Save a sample of the dataset to CSV for manual inspection.

    :param df: Spark DataFrame
    :param n: Number of rows to save
    '''
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
    '''
    Save null and numeric statistics to CSV files.

    :param null_stats: DataFrame of null statistics
    :param numeric_stats: DataFrame of numeric descriptive statistics
    '''
    PROFILING_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    null_stats_path = PROFILING_OUTPUT_PATH / "null_statistics"
    numeric_stats_path = PROFILING_OUTPUT_PATH / "numeric_statistics"

    logger.info(f"Sauvegarde des statistiques de nulls dans {null_stats_path}")
    (
        null_stats
        .coalesce(1)  # create only one CSV
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
    '''
    Execute the OWID COVID-19 profiling workflow.
    '''
    logger.info("Démarrage du profiling OWID COVID")

    spark = get_spark("OWID_COVID_Profiling")
    df = load_raw_data(spark)
    
    PROFILING_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    show_basic_info(df)
    sample_data(df)

    save_schema(df)
    save_sample_data(df)

    null_stats = compute_null_statistics(df)
    numeric_stats = compute_numeric_statistics(df)

    save_profiling_results(null_stats, numeric_stats)

    spark.stop()
    logger.info("Profiling terminé avec succès")

# ============================
# Standalone execution
# ============================
if __name__ == "__main__":
    main()
