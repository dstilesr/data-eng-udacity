import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import year, month
from pyspark.sql.functions import monotonically_increasing_id


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
    )  # Year 0 is missing data!

    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .mode("overwrite") \
        .partitionBy("year", "artist_id") \
        .parquet(output_data + "songs")

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
    artists_table.write \
        .mode("overwrite") \
        .parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Processes the logs json files and stores the `time`, `users`, and `songplays`
    tables as parquet files.
    :param spark: Spark session.
    :param input_data: Location of the input JSON dataset (S3 or local
        filesystem address).
    :param output_data: Location where output files will be stored (S3 or
        local filesystem address).
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read\
        .json(log_data)\
        .where("page = 'NextSong'")\
        .cache()

    # extract columns for users table
    users_table = df \
        .selectExpr(
            "userId as user_id",
            "firstName as first_name",
            "lastName as last_name",
            "gender",
            "level"
        ) \
        .dropDuplicates(subset=["user_id"])  # Ensure one record per user

    # write users table to parquet files
    users_table.write\
        .mode("overwrite")\
        .parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x // 1000),
        TimestampType()
    )
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.selectExpr(
            "start_time",
            "hour(start_time) as hour",
            "dayofmonth(start_time) as day",
            "weekofyear(start_time) as week",
            "month(start_time) as month",
            "year(start_time) as year",
            "dayofweek(start_time) as weekday"
        )\
        .dropna(subset=["start_time"])\
        .dropDuplicates(subset=["start_time"])

    # write time table to parquet files partitioned by year and month
    time_table.write\
        .mode("overwrite")\
        .partitionBy("year", "month")\
        .parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs") \
        .select("title", "song_id", "artist_id") \
        .join(
            spark.read
                .parquet(output_data + "artists")
                .select("artist_id", "name"),
            "artist_id",
            "inner"
        )\
        .selectExpr("title as song", "name as artist", "song_id", "artist_id")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, ["song", "artist"], "left")\
        .withColumn("songplay_id", monotonically_increasing_id())\
        .selectExpr(
            "songplay_id",
            "start_time",
            "userId as user_id",
            "level",
            "song_id",
            "artist_id",
            "sessionId as session_id",
            "location",
            "userAgent as user_agent"
        )

    # write songplays table to parquet files partitioned by year and month
    songplays_table\
        .withColumn("year", year(songplays_table["start_time"]))\
        .withColumn("month", month(songplays_table["start_time"]))\
        .write\
        .partitionBy("year", "month")\
        .parquet(output_data + "songplays")


def main():
    """
    Runs the ETL process.
    :return:
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    if "AWS" in config:
        # Credentials are not necessary on a cluster (managed by IAM role)
        os.environ['AWS_ACCESS_KEY_ID'] = config["AWS"]['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config["AWS"]['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config["OUT"]["OUTPUT_lOC"]

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
