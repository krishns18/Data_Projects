class BuildPipeline:
    def __init__(self,immi_fact,demog_dim,airport_dim,output_path):
        self.immi_fact = immi_fact
        self.demog_dim = demog_dim
        self.airport_dim = airport_dim
        self.output_path = output_path
    
    def gen_immi_fact(self):
        try:
            print("Begin:writing Immigration Fact table to location {}".format(self.output_path))
            self.immi_fact.write.mode("overwrite").partitionBy("i94addr","arrival_yr","arrival_mon").\
            parquet(self.output_path +'/immi_fact.parquet')
        except:
            print("File not written")
        else:
            print("End:File written to {}".format(self.output_path))
        return
        
        
    def gen_demog_dim(self):
        try:
            print("Begin:writing Demog Dimension table to location {}".format(self.output_path))
            self.demog_dim.write.mode("overwrite").partitionBy("state_code","race").\
            parquet(self.output_path +'/demog_dim.parquet')
        except:
            print("File not written")
        else:
            print("End:File written to {}".format(self.output_path))
        return
        
    def gen_airport_dim(self):
        try:
            print("Begin:writing Airport Dimension table to location {}".format(self.output_path))
            self.airport_dim.write.mode("overwrite").partitionBy("local_code","iso_region").\
            parquet(self.output_path +'/airport_dim.parquet')
        except:
            print("File not written")
        else:
            print("End:File written to {}".format(self.output_path))
        return