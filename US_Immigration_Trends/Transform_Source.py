from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import isnan, when, count, col
import pyspark.sql.types as T


class TransformSource():
    def __init__(self,immi_clean,demog_clean,airport_clean):
        self.immi_clean = immi_clean
        self.demog_clean = demog_clean
        self.airport_clean = airport_clean
        self.get_visa_codes_udf = F.udf(TransformSource.get_visa_codes,T.StringType())
        self.get_visa_modes_udf = F.udf(TransformSource.get_visa_modes,T.StringType())
        
    def transform_demog(self):
        # Function to transform demographics data
        demog_grp = self.demog_clean.groupBy("state_code","Race").agg(F.sum("Male Population").alias("male_pop"),
                                                          F.sum("Female Population").alias("female_pop"),
                                                          F.sum("Total Population").alias("total_pop"),
                                                          F.sum("Foreign-born").alias("foreign_born_pop"),
                                                          F.sum("Number of Veterans").alias("veteran_pop"))
        demog_grp_renamed = demog_grp.withColumnRenamed("Race","race")
        return demog_grp_renamed
    
    @staticmethod
    def get_visa_codes(val):
        # UDF function for visa code mapping 
        code = ""
        if val == 1.0:
            code = "Business"
        elif val == 2.0:
            code = "Pleasure"
        elif val == 3.0:
            code = "Student"
        return code
    
    @staticmethod
    def get_visa_modes(val):
        # UDF function for visa mode mapping
        mode = ""
        if val == 1.0:
            mode = "Air"
        elif val == 2.0:
            mode = "Sea"
        elif val == 3.0:
            mode = "Land"
        elif val == 9.0:
            mode = "Not reported"
        return mode
    
    def transform_immi(self):
        # Function to transform immigration data
        self.immi_clean = self.immi_clean.withColumn("visa_code", self.get_visa_codes_udf(self.immi_clean.i94visa))
        self.immi_clean = self.immi_clean.withColumn("visa_mode", self.get_visa_modes_udf(self.immi_clean.i94mode))
        self.immi_clean = self.immi_clean.withColumn("arrival_yr", self.immi_clean.i94yr.cast("int")).drop("i94yr")
        self.immi_clean = self.immi_clean.withColumn("arrival_mon", self.immi_clean.i94mon.cast("int")).drop("i94mon")
        self.immi_clean = self.immi_clean.drop("i94visa")
        self.immi_clean = self.immi_clean.drop("i94mode")
        return self.immi_clean