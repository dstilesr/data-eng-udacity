import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month
from pyspark.sql.functions import dayofmonth, hour, weekofyear, date_format


def create_spark_session():
    """
    Creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extracts the data from the song jsons, processes it and saves the
    resulting `songs` and `artists` tables as parquet files.
    :param spark: Spark session.
    :param input_data: Location of the input JSON dataset (S3 or local
        filesystem address).
    :param output_data: Location where output files will be stored (S3 or
        local filesystem address).
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark \
        .read \
        .json(song_data) \
        .dropna(subset=["song_id"]) \
        .dropDuplicates(["song_id"]) \
        .cache()  # Cache this df since we'll use it twice!

    # extract columns to create songs table
    songs_table = df.selectExpr(
        "song_id",
        "artist_id",
        "title",
        "case when year > 0 then year else null end as year",
        "duration"
    ) # Year 0 is missing data!

    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .partitionBy("year", "artist_id") \
        .parquet(output_data + "/songs")

    # extract columns to create artists table
    artists_table = df.selectExpr(
            "artist_id",
            "artist_name as name",
            "artist_location as location",
            "artist_latitude as latitude",
            "artist_longitude as longitude"
        ) \
        .dropna(subset=["artist_id"]) \
        .dropDuplicates(subset=["artist_id"])  # One record per artist!

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "/artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df =

    # filter by actions for song plays
    df =

    # extract columns for users table
    artists_table =

    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df =

    # create datetime column from original timestamp column
    get_datetime = udf()
    df =

    # extract columns to create time table
    time_table =

    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df =

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    if "AWS" in config:
        os.environ['AWS_ACCESS_KEY_ID'] = config["AWS"]['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config["AWS"]['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config["OUT"]["OUTPUT_lOC"]

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
