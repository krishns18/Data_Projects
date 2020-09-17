# Data Engineering Capstone Project
## Project Summary
The purpose of this project is to understand the immigration trends using a data pipeline created using Spark and Parquet file format
The project follows the follow steps:

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Project Scope and Data Gathering
* I have used the following 3 data sources:
    * I94 Immigration
    * U.S. City Demographic
    * Airport Code Table
    
* I used the above data sources to create a data model used to understand immigration patterns in the US.
* Tool/Framework used was Spark in parquet file format to extract, transform and build data model used to understand 
immigration patterns. 
* Spark is used because of the ability to process massive datasets and parquet file format for efficient querying due to
 columnar storage format.
 
## Conceptual Data Model
* Used the following star schema
* Fact Table
    * immi_fact (i94addr,i94port,visa_code,visa_mode, cicid, i94cit, i94res, arrdate, depdate, i94bir, gender, visatype, 
    dtadfile, arrival_yr, arrival_mon)
* Dimension Tables
    * demog_dim (state_code,race,male_pop,female_pop,total_pop,foreign_born_pop,veteran_pop)
    * airport_dim (local_code,ident,type,name,elevation_ft,continent,iso_country,iso_region,coordinates)
    * visa_code - mapped i94visa column into corresponding values using I94_SAS_Labels_Descriptions file
    * visa_mode - mapped i94mode into corresponding values using I94_SAS_Labels_Descriptions file
* Reason for the model choice:
    * The star schema separates business process data into facts, and measurable descriptive attributes related to the 
    fact table into dimension tables
    * I wanted to analyze the following trends using the above data model:
        * Determine male vs female count arriving at different airport types in US per month? (Use immi_fact and airport_dim)
        * Determine which airport ports have the maximum number of immigrants arrival per month? (Use immi_fact and airport_dim)
        * Determine trends between foreign_born population and the number of immigrants arrival per US state? (Use immi_fact and demog_dim)
        * Determine trends between male_population and number of men arrived in US per state? (Use immi_fact and demog_dim)
        * Determine trends between female_population and number of females arrived in US per state? (Use immi_fact and demog_dim)
    * Thus by using the above star schema model, based on the above analysis we can determine key trends and immigration 
    activity in US which can guide key business decisions
    
## Data dictionary
  * Please refer to the Data_Dictionary.md file

## Project Write-up
   * I used Apache Spark for ETL process because we can load the Petabytes of data and can process it without any hassle 
   by setting up multi-node clusters.
   * Data needs to be updated every month since immigration fact table is partitioned by month. However if required we 
   can also have weekly granularity, we can partition it weekly and hence update it respectively.
   * I would use the following approch under the following scenarios:
   * The data was increased by 100x - I would use AWS + Spark to store input sources and output sources in s3 buckets 
   along with appropriately partitoned columns
   * The data populates a dashboard that must be updated on a daily basis by 7am every day - I would use AWS + Spark +
   Airflow to run the ETL job by 7 am everyday to visualize ETL workflows and debug when necessary.
   * The database needed to be accessed by 100+ people - I would use AWS + Spark + Airflow and store output tables in 
   Redshift so that 100+ people can run/access Analytical queries