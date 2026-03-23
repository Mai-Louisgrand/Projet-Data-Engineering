import os
from pathlib import Path
from pyspark.sql import SparkSession

GCP_CREDENTIALS_JSON = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        str(Path.home() / ".config/gcloud/application_default_credentials.json")
    )


def get_spark(app_name: str = "OWID_PIPELINE") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # CORE CONFIG
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", "/opt/airflow/data/checkpoints")

        # GCS AUTH
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_CREDENTIALS_JSON)

        # Optim GCS
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.gs",
                "org.apache.hadoop.fs.gs.GCSOutputCommitterFactory")

        .getOrCreate()
    )