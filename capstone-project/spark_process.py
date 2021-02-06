import os
from typing import Optional
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, FloatType

BUCKET = os.getenv("BUCKET_NAME", "dsr-udacity-capstone")
STORM_DATA_PATH = os.getenv("STORM_DATA_PATH", "storm-data")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "processed-data")
TEMP_DATA_FILE = os.getenv("TEMP_DATA_FILE")

pad_day = F.udf(lambda d: f"{int(d):02d}", StringType())


@F.udf(returnType=FloatType())
def parse_damage_figure(fig_str: str) -> Optional[float]:
    """
    UDF to parse the damage figures in the storm data. These are given in the
    format 67K or 3.4M.
    :param fig_str: Damage figure string.
    :return:
    """
    if fig_str is None:
        return None

    mult_dict = {
        "K": 1000.,
        "M": 1000000.
    }
    if fig_str[-1].isalpha():
        out = float(fig_str[:-1]) * mult_dict.get(fig_str[-1], 1.)
    else:
        out = float(fig_str)
    return out


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


def make_location_table(temp_raw: DataFrame) -> DataFrame:
    """
    Makes the locations table from the temperature dataframe.
    :param temp_raw: Temperature dataframe from csv.
    :return:
    """
    df_locations = temp_raw \
        .selectExpr(
            "trim(upper(State)) as state",
            "trim(upper(Country)) as country"
        ) \
        .dropDuplicates() \
        .withColumn("location_id", F.md5(F.concat(
            F.col("country"),
            F.col("state")
        )))

    return df_locations


def make_storms_table(storms_raw: DataFrame) -> DataFrame:
    """
    Makes the storms table from the raw storms data read from the csvs.
    :param storms_raw: Raw storm data from csvs.
    :return: The prepared DataFrame.
    """
    df_storms_processed = storms_raw\
        .withColumn(
            "date_str",
            F.concat(
                F.col("BEGIN_YEARMONTH"),
                pad_day(F.col("BEGIN_DAY")))
        )\
        .withColumn("start_date", F.to_date(F.col("date_str"), "yyyyMMdd"))\
        .withColumn("country", F.lit("UNITED STATES")) \
        .withColumn("location_id", F.md5(F.concat(
            F.col("country"),
            F.trim(F.col("STATE"))
        )))\
        .selectExpr(
            "start_date",
            "EVENT_ID as event_id",
            "EPISODE_ID as episode_id",
            "location_id",
            "EVENT_TYPE as event_type",
            "MAGNITUDE_TYPE as magnitude_type",
            "MAGNITUDE as magnitude",
            "DAMAGE_PROPERTY as damage_property",
            "DAMAGE_CROPS as damage_crops",
            "DEATHS_DIRECT as deaths_direct",
            "INJURIES_DIRECT as injuries_direct"
        ) \
        .withColumn(
            "damage_property", parse_damage_figure(F.col("damage_property"))
        ) \
        .withColumn(
            "damage_crops", parse_damage_figure(F.col("damage_crops"))
        )
    return df_storms_processed


if __name__ == '__main__':
    spark = SparkSession.builder\
        .appName("StormApp")\
        .getOrCreate()

    # Read the NOAA storms data
    df_storms_raw = spark\
        .read\
        .option("header", "true")\
        .csv(f"s3://{BUCKET}/{STORM_DATA_PATH}/*.csv.gz")

    # Read the temperatures data
    df_temp_raw = spark \
        .read \
        .option("header", "true") \
        .csv(f"s3://{BUCKET}/{TEMP_DATA_FILE}")

    # Dates dimension
    dates_tab = make_date_table(df_storms_raw, df_temp_raw)

    # Locations Dimension
    locations_tab = make_location_table(df_temp_raw)

    # Storms Fact
    storms_tab = make_storms_table(df_storms_raw)

    spark.stop()
