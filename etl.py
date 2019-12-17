import configparser #comment out when the code execute on master node
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld,\
DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType


config = configparser.ConfigParser()#comment out when the code execute on master node
config.read('dl.cfg')#comment out when the code execute on master node

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#comment out when the code execute on master node
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
#comment out when the code execute on master node


def create_spark_session():
    """
    The function create spark session.

    Args:
    None
    
    Returns:
    spark : instance of spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    The function create a etl which extract data from song data and create tables \
    which are songs_table and artist_table.

    Args:
        spark : instance of spark
        input_data : filepath of the input data
        output_data : filepath of the output data
    
    Returns:
        None
    """

    # get filepath to song data file
    #song_data = "song_data/A/B/C" # for check
    song_data = "song_data/*/*/*/.json"
    
    # read song data file
    song_schema = R([
        Fld("artist_id",Str(),True),
        Fld("artist_latitude",Dbl(),True),
        Fld("artist_location",Str(),True),
        Fld("artist_longitude",Dbl(),True),
        Fld("artist_name",Str(),True),
        Fld("duration",Dbl(),True),
        Fld("num_songs",Int(),True),
        Fld("song_id",Str(),True),
        Fld("title",Str(),True),
        Fld("year",Int(),True)
    ])
    df=spark.read.json(os.path.join(input_data,song_data),schema = song_schema)
    df.persist()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id")\
    .parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude",\
                              "artist_longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    The function create a etl which extract data from log data and song data and create tables \
    which are songs_table and artist_table.

    Args:
        spark: instance of spark
        input_data: filepath of the input data
        output_data: filepath of the output data
        
    Returns:
        None

    """
    # get filepath to log data file
    log_data = "log_data/2018/11/2018-11-13-events.json" #for check
    #log_data = "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(os.path.join(input_data, log_data))
    df.persist()
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong').withColumn("userId", df.userId.cast(Int()))\
                        .withColumnRenamed("userId", "user_id")\
                        .withColumnRenamed("firstName", "first_name")\
                        .withColumnRenamed("lastName", "last_name")\
                        .withColumnRenamed("userAgent", "user_agent")\
                        .withColumnRenamed("sessionId", "session_id")\

    # extract columns for users table    
    users_table = df.withColumn("user_id", df.user_id.cast(Int()))\
                    .select("user_id", "first_name", "last_name", "gender", "level").distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data,"users"))
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : x / 1000.0 ,Dbl())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.ts))\
        .withColumn("hour",date_format(col("start_time"),"HH"))\
        .withColumn("day",date_format(col("start_time"),"dd"))\
        .withColumn("week",weekofyear(col("start_time")))\
        .withColumn("month",date_format(col("start_time"),"MM"))\
        .withColumn("year", date_format(col("start_time"),"yyyy"))\
        .withColumn("weekday",date_format(col("start_time"),"u"))
    
    # extract columns to create time table
    time_table = df.select("start_time","hour", "day", "week", "month", "year",\
                           "weekday").distinct()
                           
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month")\
    .parquet(os.path.join(output_data,"time"))
    
    # read in song data to use for songplays table
    st=spark.read.parquet(output_data+"songs")
    at=spark.read.parquet(output_data+"artists")
    song_df = st.join(at, st.artist_id == at.artist_id, "left").drop(at["artist_id"])\
                .select("song_id","artist_id" ,"title","duration","artist_name")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) &\
                                (df.length == song_df.duration)&\
                                (df.artist == song_df.artist_name), "left")\
                                .filter(col('page') == 'NextSong')\
                                .select("start_time", "user_id", "level" ,"song_id",\
                                        "artist_id", "session_id",\
                                        "location", "user_agent","year","month").distinct()

    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month")\
    .parquet(os.path.join(output_data, "songplays"))



def main():
    """
    The function execute several tasks which are creating spark session,\
    Setls for song and log data.
    
    Argues:
        None
    
    Return:
        None

    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #output_data = "output/" #for check
    output_data = "s3a://project-udacity-datalake/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    spark.stop()


if __name__ == "__main__":
    main()
