### Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

#### Goal
The purpose of the project is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

#### Purpose
The purpose of the building the data lake is to find insights about what song their users are listening to using different tables in data lake.

#### Design
The database schema design used for this project is using Facts and Dimensions Tables with below details:

##### Fact table
songplays_table - records in the log data associated with song plays i.e. records with page NextSong

##### Dimension table
artists_table - artists in music database
songs_table - songs in music database
time_table - timestamps of records in songplays broken down into specific units
users_table - users in the application

#### ETL pipeline process
1. Load credentials
2. Read data from s3
    a. Song Data
    b. Log Data
3. Process Data using Spark
4. Load data back to s3 into parquet files

