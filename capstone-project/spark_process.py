import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

BUCKET = os.getenv("BUCKET_NAME", "dsr-udacity-capstone")
STORM_DATA_PATH = os.getenv("STORM_DATA_PATH", "storm-data")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "processed-data")
TEMP_DATA_FILE = os.getenv("TEMP_DATA_FILE")


if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName("StormApp")\
        .getOrCreate()

    df_storms_raw = spark\
        .read\
        .option("header", "true")\
        .csv(f"s3://{BUCKET}/{STORM_DATA_PATH}/*.csv.gz")

    df_temp_raw = spark \
        .read \
        .option("header", "true") \
        .csv(f"s3://{BUCKET}/{TEMP_DATA_FILE}")

    spark.stop()
