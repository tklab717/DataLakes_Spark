Purpose of this database for Sparkify and their analytical goals
=================================================================
Sparkify has grown their user base and gotten a lot data.
Therefore, they wnat to move their dataware house to the data lake which have scalability of data and flexibility of analysis.
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
I will create the etl pipeline that extracts data from S3 and load it to tables in S3.

Their analytical goals are to get insights in what songs their users are listening to.
Therefore I create the data lake which offers analytical tables to ralize it.


Database schema design 
========================
This data lake in S3 is organized by some tables. Fact table is songplays. Dimention tables are users, songs, artists, time.
The ETL pipeline makes some tables in this data lake by using songs_data and logs_data in the S3. Their data are extracted from S3 and written to parquet files in a separate analytics directory on S3.

- Fact Table
 1.songplays  
     songplay_id IntegerType,  
     start_time TIMESTAMP,  
     user_id IntegerType,  
     level StringType,  
     song_id StringType,  
     artist_id StringType,  
     session_id IntegerType,  
     location StringType,  
     user_agent StringType  
     
- Demension Table
 2.users  
     user_id integer,  
     first_name varchar,  
     last_name varchar,  
     gender varchar,  
     level varchar  
 3.songs  
     song_id StingType,  
     title StringType,  
     artist_id StringType,  
     year IntegerType,  
     duration DoubleType
 4.artists  
     artist_id StringType,  
     artist_name StringType,  
     artist_location StringType,  
     artist_lattitude DoubleType,  
     artist_longitude DoubleType  
 5.time  
     start_time TimestampType,  
     hour IntegerType,  
     day IntegerType,  
     week IntegerType,  
     month IntegerType,  
     year IntegerType  

How to use this programs
========================
1.Create S3 bucket that is used to create data lake
2.Write "output_data = s3a://bucket_name/output/" in etl.py ※bucket_name is the name that you created.
3.Create EMR cluster on AWS and get key pair. You need to select region and subnet that offer notebook environment.
4.You need to set my IP about ssh in the Security group of EMR cluster.
5.Connect to Master Node from a BASH shell. 
  Command: ssh -i xxx.pem hadoop@ec2-xx-xxx-xx-xxx.us-west-2.compute.amazonaws.com
  You can confirm this command on the page of EMR cluster.
6.Create etl.py on the Master Node. I created the file by using nano. When the file created, I deleted the module of configparser and related functions that is not nessesary.
4.Execute etl.py
5.Check S3 bucket

About files in repositry
=========================
1.data
 The data in the folder is same data used to create data lake in S3.

2.output
 There are some data that is the result that was created when etl.py executed in the local environment.

3.create_tables.py
 Information for using AWS

4.etl.py
 Code for extracting data from S3 and creating parquet files in S3.

5.Diagram
 This folder include the figure of the structure of this etl.