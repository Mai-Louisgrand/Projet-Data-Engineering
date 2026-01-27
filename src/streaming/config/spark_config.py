# Configuration Spark pour le pipeline streaming OWID

from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "OWID_COVID_Streaming"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")  # réduire partitions pour dev local
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints")  # checkpoint global par défaut
        .getOrCreate()
    )
    return spark
