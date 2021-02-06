import os
from typing import Optional, Tuple
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
        "M": 1000000.,
        "B": 1000000000.
    }
    if fig_str[-1].isalpha():
        base_num = float(fig_str[:-1]) if len(fig_str) > 1 else 1.
        out = base_num * mult_dict.get(fig_str[-1], 1.)
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


def process_temp_data(temp_raw: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Makes the locations and temperatures tables from the temperature
    raw dataframe.
    :param temp_raw: Temperature dataframe from csv.
    :return: The locations and temperatures dataframes.
    """
    df_with_locid = temp_raw \
        .withColumn("state", F.trim(F.upper(F.col("State"))))\
        .withColumn("country", F.trim(F.upper(F.col("Country")))) \
        .withColumn("location_id", F.md5(F.concat(
            F.col("country"),
            F.col("state")
        ))) \
        .cache()

    df_locations = df_with_locid\
        .select("location_id", "state", "country")\
        .dropDuplicates()

    df_temperatures = df_with_locid \
        .withColumn("date", F.to_date(F.col("dt"), "yyyy-MM-dd")) \
        .withColumn("date_year_month", F.date_format(F.col("date"), "yyyy-MM"))\
        .selectExpr(
            "date_year_month",
            "location_id",
            "AverageTemperature as average_temperature",
            "AverageTemperatureUncertainty as average_temperature_uncertainty"
        )

    return df_locations, df_temperatures


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


def save_tables(
        storms: DataFrame,
        temperatures: DataFrame,
        locations: DataFrame,
        dates: DataFrame,
        w_mode: str = "overwrite"):
    """
    Saves the dataframes to the S3 bucket in Parquet files.
    :param storms: Storms df.
    :param temperatures: Temperatures df.
    :param locations: Locations df.
    :param dates: Dates df.
    :param w_mode: Writing mode for saving.
    :return:
    """
    storms.write\
        .mode(w_mode)\
        .parquet(f"s3://{BUCKET}/{OUTPUT_PATH}/storms")

    temperatures.write\
        .mode(w_mode)\
        .parquet(f"s3://{BUCKET}/{OUTPUT_PATH}/temperatures")

    locations.write\
        .mode(w_mode)\
        .parquet(f"s3://{BUCKET}/{OUTPUT_PATH}/locations")

    dates.write\
        .mode(w_mode)\
        .parquet(f"s3://{BUCKET}/{OUTPUT_PATH}/dates")


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
    locations_tab, temperature_tab = process_temp_data(df_temp_raw)

    # Storms Fact
    storms_tab = make_storms_table(df_storms_raw)

    # Quality check: assert location_id match for storms df
    num_storm_recs = storms_tab.count()
    joined_num = storms_tab\
        .select("location_id")\
        .join(locations_tab, "location_id", "inner")\
        .count()

    if joined_num == 0:
        # Only assert the number is positive since some locations in the
        # storms dataset will find no matches since they are not states
        # proper but could be sea regions or territories.
        print("ERROR: Storm data location ids are incorrect!")
        raise ValueError("Storm locations incorrect!")

    # Quality check: assert all dates are present
    joined_num_dates = storms_tab\
        .selectExpr("start_date as date")\
        .join(dates_tab, "date", "inner")\
        .count()

    if joined_num_dates != num_storm_recs:
        print("ERROR: Storm data dates do not match!")
        raise ValueError("Storm dates do not match!")

    # Save Files
    save_tables(storms_tab, temperature_tab, locations_tab, dates_tab)

    spark.stop()
