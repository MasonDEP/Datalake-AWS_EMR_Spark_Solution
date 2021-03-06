  
import configparser
from datetime import datetime,date
import os
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


def create_spark_session():
    """
        This function creates a spark instance with the right config 
        setting ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        This function creates read in raw json files from s3 source 
        bucket, namely 's3a://udacity-dend/song_data'. It first reads
        in the json file with inferred schema then populates the songs_table 
        and artists_table dimentional tables with extracted columns from the read dataframe.
        At last, all curated tables saved as parquet files into a s3 bucket.
    """
    
    # get filepath to song data file
    song_data = 's3a://udacity-dend/song_data/*/*/*/*.json'

    # read song data file
    df_song_load = spark.read\
                        .format("json")\
                        .option("inferSchema","true")\
                    .load(song_data)

    # extract columns to create songs table
    songs_table = df_song_load.select("song_id","title","artist_id","year","duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").format("parquet").save("{}songs.parquet".format(output_data))

    # extract columns to create artists table
    artists_table = df_song_load.selectExpr("artist_id","artist_name as name","artist_location as location","artist_latitude as lattitude","artist_longitude as longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.partitionBy("name").format("parquet").save("{}artists.parquet".format(output_data))


def process_log_data(spark, input_data, output_data):
    
     """
        This function creates read in raw json files from s3 source 
        bucket, namely 's3a://udacity-dend/log_data'. It first reads
        in the json file with inferred schema then populates the users_table 
        and time_table dimentional tables with extracted columns from the read dataframe.
        As the last step, the function join these tables to produce songplays_table. 
        At last, all curated tables saved as parquet files into a s3 bucket.
    """

    # get filepath to log data file
    log_data  = 's3a://udacity-dend/log_data/*/*/*.json'

    # read log data file
    df_log_load =  spark.read\
                    .format("json")\
                    .option("inferSchema","true")\
                    .load(log_data) 
    
    # filter by actions for song plays
    df_filtered = df_log_load.where(col('page') == 'NextSong')

    # extract columns for users table    
    users_table = df_filtered.selectExpr("userId as user_id",
                                         "firstName as first_name",
                                         "lastName as last_name",
                                         "gender",
                                         "level").distinct()
    
    # write users table to parquet files
    users_table.write.partitionBy("user_id").format("parquet").save("{}users.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000.0), TimestampType())
    df_filtered = df_filtered.withColumn('start_time',get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: date.fromtimestamp(ts / 1000.0), DateType())
    df_filtered = df_filtered.withColumn('start_date',get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df_filtered.select("start_time",
                                    hour("start_time").alias('hour'),
                                    dayofmonth("start_time").alias('day'),
                                    weekofyear("start_time").alias('week'),
                                    month("start_time").alias('month'),
                                    year("start_time").alias('year'),
                                    dayofweek("start_time").alias('dayofweek')
                                   ).distinct()
    time_table.write.partitionBy("year","month").format("parquet").save("{}time.parquet".format(output_data))

    # read in song data to use for songplays table
    song_df = df_filtered.join(songs_table,(df_filtered.song == songs_table.title),'leftouter')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df_join_songid.selectExpr(
                               'monotonically_increasing_id() as songplay_id',
                               'start_time',
                               'userId as user_id',
                               'level','song_id',
                               'artist_id',
                               'sessionId as session_id',
                               'location',
                               'userAgent as user_agent')
    
    #add month year for partition
    time_table_lookup = time_table.select('start_time','year','month')
    songplays_table_partition_key = songplays_table.join(time_table_lookup,['start_time'],'leftouter')

    # write songplays table to parquet files partitioned by year and month
    songplays_table_partition_key.write.partitionBy("year","month").format("parquet").save("{}songplays.parquet".format(output_data))


def main():
    
    '''
        The main() function runs the above function in order.
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dwh-dev-007/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
