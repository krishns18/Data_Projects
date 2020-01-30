#### Introduction
A startup called **Sparkify** wants to analyze the data they have been collecting on songs and user activity on their new music streaming app.  
The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.    

#### Description
The task is create a Postgres database with tables designed to optimize queries on song play analysis. Need to create a database schema and ETL pipeline for this analysis. The project involves defining fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.    

#### Datasets
**Song Dataset:** This dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/) . Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.    

**Log Dataset:** This dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.    


