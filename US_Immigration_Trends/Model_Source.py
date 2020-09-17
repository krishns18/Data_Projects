class ModelSource:
    def __init__(self,immi_trans,demog_trans,airport_clean):
        self.immi_trans = immi_trans
        self.demog_trans = demog_trans
        self.airport_clean = airport_clean
        
    def model_fact(self):
        # Function to model fact table
        immi_fact = self.immi_trans.join(self.demog_trans,self.immi_trans.i94addr == self.demog_trans.state_code, 
                                         how = 'left_semi')
        immi_fact = immi_fact.join(self.airport_clean,immi_fact.i94port == self.airport_clean.local_code, 
                                   how = 'left_semi')
        return immi_fact
    
    def star_model(self):
        # Function to model and generate fact and dimension tables
        immi_fact = self.model_fact()
        dim_demog = self.demog_trans
        dmi_airport = self.airport_clean
        return immi_fact,self.demog_trans,self.airport_clean
        
    