import pandas as pd
import os
from app_logging.logger import logger



class data_transform:

    def __init__(self):
        self.dir='prediction_raw_files_validated/good_raw/'
        self.logger_object=logger()


    def addquotestocolumn(self):
        self.file_object = open("prediction_logs/pred_data_transformation_log.txt", "a+")
        onlyfiles=[files for files in os.listdir(self.dir)]
        try:
            self.logger_object.log(self.file_object,"adding quotes to column date in csv file")
            for file in onlyfiles:
                df=pd.read_csv(self.dir+file)
                df['DATE']=df['DATE'].apply(lambda x : "'"+str(x)+"'")
                df.to_csv(self.dir+file,header=True,index=None)

        except Exception as e:
            self.logger_object.log(self.file_object,"error in adding quotes to date column : %s"%e)
            self.file_object.close()
            raise e
