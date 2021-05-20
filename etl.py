import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
        Reads song_data .json files from S3, processes them, extracts artists and songs tables and writes them back to S3 as parquet files.
       
        Parameters:
            spark       : Spark Session
            input_data  : S3 bucket path for input files
            output_data : S3 bucket path for output files
    """
    # takes input filepath
    song_data = input_data + 'song_data/*/*/*/*.json'

    # reads song data files
    df = spark.read.json(song_data)

    # extracts columns for songs while droping duplicates
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()

    # overwrites parquet files in partitioned file format
    songs_table.write.partitionBy("year", "artist_id").parquet("{}songs/songs_table.parquet".format(output_data),mode="overwrite")

    # extracts columns for artists while droping duplicates
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    
    # overwrites parquet files in partitioned file format
    artists_table.write.parquet("{}artists/artists_table.parquet".format(output_data),mode="overwrite")



def process_log_data(spark, input_data, output_data):
    
    """
        Reads log_data .json files from S3, processes them, extracts users, timestamp and songplays tables and writes them back to S3 as parquet files.
        
        Parameters:
            spark       : Spark Session
            input_data  : S3 bucket path for input files
            output_data : S3 bucket path for output files
    """
    # takes input filepath
    log_data = input_data + 'log_data/*/*/*.json'
    
    # reads log data files
    log_df = spark.read.json(log_data)

    # applies filter by choosing only NextSong values for page field
    log_df = log_df.filter(log_df.page == "NextSong").cache()

    # extracts columns for users while droping duplicates
    users_table = log_df.select("userId","firstName","lastName","gender","level").drop_duplicates()

    # overwrites parquet files in partitioned file format
    users_table.write.parquet("{}users/users_table.parquet".format(output_data),mode="overwrite")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn("start_time", get_timestamp(col("ts")))

    # extracts columns for users while droping duplicates
    time_table = log_df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    

    # overwrites parquet files in partitioned file format
    time_table.write.partitionBy("year", "month").parquet("{}time/time_table.parquet".format(output_data),mode="overwrite")

     # reads songs files to create songs_df in order to join with log data to extract songplays
    songs_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/*/*/"))
    
    
    # extracts columns for songplays by joining songs_df and log_df
    songplays_table = log_df.join(songs_df, log_df.song == songs_df.title, how='inner').\
    select(monotonically_increasing_id().alias("songplay_id"), \
           col("start_time"),col("userId").alias("user_id"),"level","song_id","artist_id", \
           col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"))

    # extracts columns for songplays by joining songplays_table and time_table   
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, \
                      how="inner").select("songplay_id", songplays_table.start_time, \
                      "user_id", "level", "song_id", "artist_id", "session_id",\
                                          "location", "user_agent", "year", "month")
    
    # overwrites parquet files in partitioned file format
    songplays_table.write.partitionBy("year", "month").parquet("{}songplays/songplays_table.parquet".\
                                                               format(output_data),mode="overwrite")


def main():
    spark = create_spark_session()
    # input and output paths
    input_data = "s3a://udacity-dend/"
    output_data = "s3://celebis/sparkify-output/"

    # processing song data
    #process_song_data(spark, input_data, output_data)   
    # processing log data
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
