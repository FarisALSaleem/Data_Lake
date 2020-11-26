import configparser
import os
from pyspark.sql import SparkSession


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session with the appropriate modules to
    integrate spark with aws
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads song data from the input_data bucket, create a set of
    dimensional tables and save them to the output_data bucket
    """
    song_data = "song_data/*/*/*/*.json"

    df = spark.read.json(input_data + song_data)

    df.createOrReplaceTempView("song_data")

    songs_table = spark.sql("""
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM song_data;
    """)

    songs_table.write.partitionBy('year', 'artist_id').parquet(
        output_data + "song_table.parquet", 'overwrite')

    artists_table = spark.sql("""
        SELECT DISTINCT
            artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
        FROM song_data;
    """)

    artists_table.write.parquet(output_data + "artists_table.parquet",
                                'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Loads log data from the input_data bucket, create a set of
    dimensional tables and save them to the output_data bucket
    """
    log_data = "log_data/*/*/*.json"

    df = spark.read.json(input_data + log_data)

    df.createOrReplaceTempView("log_data")

    time_table = spark.sql("""
        SELECT DISTINCT
            cast(ts as TIMESTAMP) as start_time,
            hour(cast(ts as TIMESTAMP)) as hour,
            day(cast(ts as TIMESTAMP)) as day,
            weekofyear(cast(ts as TIMESTAMP)) as week,
            month(cast(ts as TIMESTAMP))as month,
            year(cast(ts as TIMESTAMP))as year,
            dayofweek(cast(ts as TIMESTAMP)) as weekday
        FROM log_data;
    """)

    time_table.write.partitionBy('year', 'month').parquet(
        output_data + "time_table.parquet", 'overwrite')

    users_table = spark.sql("""
        SELECT DISTINCT
            userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
        FROM log_data
        WHERE userId is NOT null;
    """)

    users_table.write.parquet(output_data + "users_table.parquet", 'overwrite')

    songplays_table = spark.sql("""
        SELECT
            cast(ts as TIMESTAMP) as start_time,
            log_data.userId as user_id,
            log_data.level,
            song_data.song_id,
            song_data.artist_id,
            log_data.sessionId as session_id,
            log_data.location,
            log_data.userAgent as user_agent
        FROM log_data, song_data
        WHERE log_data.page = 'NextSong'
        AND log_data.song = song_data.title
        AND log_data.artist = song_data.artist_name
        AND log_data.length = song_data.duration;
    """)

    songplays_table.write.parquet(output_data + "songplays_table.parquet",
                                  'overwrite')


def main():
    """Connects to sparkify s3 bucket, extracts sparkify data from it,
    loads it in to spark, create a set of dimensional tables and
    saves them to another s3 bucket.

    - Creates a Spark session.
    - Extracts user, song data from sparkify S3 bucket
    - Transforms the staged data into a set of dimensional tables
    - Loads the set of dimensional tables to another S3 bucket.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakes3bucket/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
