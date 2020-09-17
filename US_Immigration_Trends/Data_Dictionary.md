# Data Dictionary of Data Engineering Capstone Project

 1. Immigration Fact Table
    * cicid: double (nullable = true) - CIC id
    * i94cit: double (nullable = true) - 3-digit country code
    * i94res: double (nullable = true) - 3-character state/country code
    * i94port: string (nullable = true) - 3-character port code
    * arrdate: double (nullable = true) - is the Arrival Date in the USA
    * i94addr: string (nullable = true) - valid state codes
    * depdate: double (nullable = true) - is the Departure Date from the USA (SAS date numeric field)
    * i94bir: double (nullable = true) - 4 digit year of birth 
    * gender: string (nullable = true) - Gender
    * visatype: string (nullable = true) - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.
    * dtadfile: string (nullable = true) - Date to which admitted to U.S. (allowed to stay until)
    * visa_code: string (nullable = true) - visa code
    * visa_mode: string (nullable = true) - visa mode
    * arrival_yr: integer (nullable = true) - arrival year 
    * arrival_mon: integer (nullable = true) - arrival month
   
 2. US Demographic Dimension Table
    * state_code: string (nullable = true) - state code
    * race: string (nullable = true) - race
    * male_pop: long (nullable = true) - total sum of male population
    * female_pop: long (nullable = true) - total sum of female population
    * total_pop: long (nullable = true) - total sum of population
    * foreign_born_pop: long (nullable = true) - total sum of foreign born population
    * veteran_pop: long (nullable = true) - total sum of veteran born population
      
 3. Airport Code Dimension Table
    * ident: string (nullable = true) - airport id
    * type: string (nullable = true) - size of airport
    * name: string (nullable = true) - airport name
    * elevation_ft: integer (nullable = true) - elevation in feet
    * continent: string (nullable = true) - continent of airport
    * iso_country: string (nullable = true) - iso country code
    * iso_region: string (nullable = true) - iso region code
    * local_code: string (nullable = true) - local port code
    * coordinates: string (nullable = true) - airport coordinated