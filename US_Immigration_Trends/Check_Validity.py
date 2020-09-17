from pyspark.sql.functions import *
from pyspark.sql.functions import isnan, when, count, col

class CheckValidity:
    def __init__(self,spark,fact_path,demog_dim_path,airport_dim_path):
        self.fact_path = fact_path
        self.demog_dim_path = demog_dim_path
        self.airport_dim_path = airport_dim_path
        self.spark = spark
        
    def table_check(self,path,table,column):
        # Function to check table for distinct records and PK is not null
        df = self.spark.read.parquet(path)
        print("Distinct records in {} is {}".format(table,df.distinct().count()))
        print("Distinct {} values in {} Table is {}".format(column,table,df.select(column).distinct().count()))
        expected = df.distinct().count()
        actual = df.count()
        print("Checking for unique records")
        assert expected == actual, "Table has duplicate records"
        expected = df.where(col(column).isNull()).count()
        actual = 0
        print("Checking if primary key is not null")
        assert expected == actual,"Primary key can not be null"
        print("Table check complete")
        return True
        
    def check_fact_table(self):
        path = self.fact_path
        column = "i94addr"
        table = "immi_fact"
        if self.table_check(path,table,column):
            return True
        return False
    
    def check_demog_dim(self):
        path = self.demog_dim_path
        column = "state_code"
        table = "demog_dim"
        if self.table_check(path,table,column):
            return True
        return False
    
    def check_airport_dim(self):
        path = self.airport_dim_path
        column = "local_code"
        table = "airport_dim"
        if self.table_check(path,table,column):
            return True
        return False
    
    def check_integrity(self):
        # Function to check integrity constrainst between fact dimension tables
        immi_path = self.fact_path
        immi_df = self.spark.read.parquet(immi_path)
        demog_path = self.demog_dim_path
        demog_df = self.spark.read.parquet(demog_path)
        airport_path = self.airport_dim_path
        airport_df = self.spark.read.parquet(airport_path)
        
        print("Checking integrity constraints")
        expected = immi_df.select("i94addr").distinct().join(demog_df, immi_df.i94addr == demog_df.state_code,\
                                                             how = "left_anti").count()
        actual = 0
        assert expected == actual, "Pk-FK between fact-dim not satisfied"
        expected = immi_df.select("i94port").distinct().join(airport_df, immi_df.i94port == airport_df.local_code,\
                                                            how = "left_anti").count()
        actual = 0
        assert expected == actual, "Pk-FK between fact-dim not satisfied"
        print("Tests passed")
        return True