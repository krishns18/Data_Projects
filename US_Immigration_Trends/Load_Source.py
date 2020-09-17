class LoadSource:
    def __init__(self,path,spark):
        self.path = path
        self.spark = spark
    
    def load_sas_data(self):
        # Function to read immigration sas data
        df = self.spark.read.option("delimiter",",").parquet(self.path)
        return df
    
    def load_csv_data(self,delim=";"):
        # Function to read csv data for demographics, temperatures and airport code
        df = self.spark.read.format('csv').options(delimiter = delim,header='true', inferSchema='true').load(self.path)
        return df