'''
Spark configuration for the OWID streaming pipeline
'''

from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "OWID_COVID_Streaming"):
    '''
    Create and return a SparkSession configured for the OWID streaming pipeline.

    :param app_name: name of the Spark application
    :return: SparkSession instance
    '''
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")  # Reduce number of partitions for local development
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints") # Default checkpoint location
        .getOrCreate()
    )
    return spark
