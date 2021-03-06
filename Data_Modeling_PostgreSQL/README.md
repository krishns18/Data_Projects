#### Introduction
A startup called **Sparkify** wants to analyze the data they have been collecting on songs and user activity on their new music streaming app.  
The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.    

#### Description
The task is create a Postgres database with tables designed to optimize queries on song play analysis. Need to create a database schema and ETL pipeline for this analysis. The project involves defining fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.    

#### Datasets
**Song Dataset:** This dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/) . Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.    

**Log Dataset:** This dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.    

#### Schema Details
**Fact Table**
1. songplays: records in log data associated with song plays.
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimensions Table**    

2. users: users in the app.  
- user_id, first_name, last_name, gender, level

3. songs: songs in the music database.  
- song_id, title, artist_id, year, duration

4. artists: artists in music database.  
- artist_id, name, location, latitude, longitude

5. time: timestamp of records in **songplays** broken down into specific units.  
- start_time, hour, day, week, month, year, weekday

#### Additional Details

1. test.ipynb: displays the first few rows of each table to let you check your database.  

2. create_tables.py: drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.

3. etl.ipynb: reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.

4. etl.py: reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.

5. sql_queries.py: contains all your sql queries, and is imported into the last three files above.





