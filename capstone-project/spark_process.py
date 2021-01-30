import os
from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession, DataFrame

BUCKET = os.getenv("BUCKET_NAME", "dsr-udacity-capstone")
STORM_DATA_PATH = os.getenv("STORM_DATA_PATH", "storm-data")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "processed-data")
TEMP_DATA_FILE = os.getenv("TEMP_DATA_FILE")

pad_day = F.udf(lambda d: f"{int(d):02d}", StringType())


def make_date_table(
        df_storms: DataFrame,
        df_temps: Optional[DataFrame] = None) -> DataFrame:
    """
    Makes the dates dimension table from the storms dataframe.

    :param df_storms: Dataframe read from storm data csvs.
    :param df_temps Dataframe read from the temperature dataset.
    :return: The Dates dataframe.
    """
    df_dates = df_storms \
        .withColumn(
            "date_str",
            F.concat(
                F.col("BEGIN_YEARMONTH"),
                pad_day(F.col("BEGIN_DAY")))
        ) \
        .withColumn("date", F.to_date(F.col("date_str"), "yyyyMMdd")) \
        .select("date")

    if df_temps is not None:
        # Add dates from the other dataset just in case some months
        # are missing
        temp_dates = df_temps \
            .select("dt") \
            .withColumn("date", F.to_date(F.col("dt"), "yyyy-MM-dd"))\
            .select("date")
        df_dates = df_dates.union(temp_dates)

    df_dates = df_dates\
        .distinct() \
        .withColumn("year", F.year(F.col("date"))) \
        .withColumn("month", F.month(F.col("date"))) \
        .withColumn("day", F.dayofmonth(F.col("date"))) \
        .withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))

    return df_dates


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

    dates_tab = make_date_table(df_storms_raw, df_temp_raw)

    spark.stop()
