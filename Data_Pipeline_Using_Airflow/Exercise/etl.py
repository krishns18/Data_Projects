import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: 
        This function loads song_data from S3 and process it by extracting the songs and
        artist tables and then again load back to S3
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """
    # get filepath to song data file
    #song_data = input_data + 'data/song_data/*/*/*/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json'
    #print(song_data)
    
    # read song data file
    df = spark.read.json(song_data)
    
    # created song view to write SQL Queries
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT sdtn.song_id, 
                            sdtn.title,
                            sdtn.artist_id,
                            sdtn.year,
                            sdtn.duration
                            FROM song_data_table sdtn
                            WHERE song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT arti.artist_id, 
                                arti.artist_name,
                                arti.artist_location,
                                arti.artist_latitude,
                                arti.artist_longitude
                                FROM song_data_table arti
                                WHERE arti.artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and process it by extracting the
        songs and artist tables and then again loaded back to S3. Also output from previous
        function is used in by spark.read.json command
        
        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data
            output_data : S3 bucket were dimensional tables in parquet format will be stored
            
    """
    # get filepath to log data file
    #log_data = input_data + 'data/log_data/*.json'
    log_data = input_data + 'log_data/*.json'
    print(log_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # created log view to write SQL Queries
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT userT.userId as user_id, 
                            userT.firstName as first_name,
                            userT.lastName as last_name,
                            userT.gender as gender,
                            userT.level as level
                            FROM log_data_table userT
                            WHERE userT.userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                            A.start_time_sub as start_time,
                            hour(A.start_time_sub) as hour,
                            dayofmonth(A.start_time_sub) as day,
                            weekofyear(A.start_time_sub) as week,
                            month(A.start_time_sub) as month,
                            year(A.start_time_sub) as year,
                            dayofweek(A.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(timeSt.ts/1000) as start_time_sub
                            FROM log_data_table timeSt
                            WHERE timeSt.ts IS NOT NULL
                            ) A
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(logT.ts/1000) as start_time,
                                month(to_timestamp(logT.ts/1000)) as month,
                                year(to_timestamp(logT.ts/1000)) as year,
                                logT.userId as user_id,
                                logT.level as level,
                                songT.song_id as song_id,
                                songT.artist_id as artist_id,
                                logT.sessionId as session_id,
                                logT.location as location,
                                logT.userAgent as user_agent

                                FROM log_data_table logT
                                JOIN song_data_table songT on logT.artist = songT.artist_name and logT.song = songT.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    """
    Extract songs and events data from S3, Transform it into dimensional tables format, and
    Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-shailesh/dloutput/"
    
    #input_data = "./"
    #output_data = "./dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
