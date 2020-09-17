from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import col, isnan, when, trim
from pyspark.sql.functions import *

class CleanSource:
    def __init__(self,airport_df,immi_df,demog_df):
        self.airport_df = airport_df
        self.immi_df = immi_df
        self.demog_df = demog_df
        
    def get_null(self,c):
        # Function to get null rows
        return when(~(col(c).isNull() | isnan(col(c)) | (trim(col(c)) == "")), col(c))
    
    def get_clean(self,df,table_name):
        # Function to clean NAN's, missing values and return distinct records
        print("Total records in {} table before cleaning is {}".format(table_name,df.count()))
        df_clean = df.select([self.get_null(c).alias(c) for c in df.columns]).na.drop()
        df_distinct = df_clean.distinct()
        print("Total records in {} table after cleaning is {}".format(table_name,df_distinct.count()))
        return df_distinct
    
    def clean_airport(self):
        airport_sel = self.airport_df.select("ident",
                             "type",
                             "name",
                             "elevation_ft",
                             "continent",
                             "iso_country",
                             "iso_region",
                             "local_code",
                             "coordinates")
        airport_clean = self.get_clean(airport_sel,"Airport-Codes")
        return airport_clean
    
    def clean_immigration(self):
        immi_sel = self.immi_df.select("cicid",
                                       "i94yr",
                                       "i94mon",
                                       "i94cit",
                                       "i94res",
                                       "i94port",
                                       "arrdate",
                                       "i94mode",
                                       "i94addr",
                                       "depdate",
                                       "i94bir",
                                       "i94visa",
                                       "gender",
                                       "visatype",
                                       "dtadfile")
        immi_clean = self.get_clean(immi_sel,"SAS-immigration")
        return immi_clean
    
    def clean_demographics(self):
        # Dropped Average household size
        demog_sel = self.demog_df.select("City",
                                               "State",
                                               "Median Age",
                                               "Male Population",
                                               "Female Population",
                                               "Total Population",
                                               "Number of Veterans",
                                               "Foreign-born",
                                               "State Code",
                                               "Race",
                                               "Count")
        demog_sel_rename = demog_sel.withColumnRenamed("State Code","state_code")
        demog_clean = self.get_clean(demog_sel_rename,"US-demographics")
        return demog_clean