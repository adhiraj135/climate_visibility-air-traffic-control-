import pandas as pd


class data_getter:
    def __init__(self,file_object,logger_object):
        self.file_path = 'trainingfilefromDB/input_file.csv'
        self.file_object=file_object
        self.logger_object=logger_object

    def get_data(self):
        self.logger_object.log(self.file_object,"data loading started.entered the get data method of data_getter class")
        try:
            data = pd.read_csv(self.file_path)
            file = open("training_logs/dataloading.txt", "a+")
            self.logger_object.log(self.file_object,"data loaded successfully.exited the get data method of data_getter class")
            return data

        except Exception as e:
            self.logger_object.log(self.file_object,"error in data loading : %s" %e)
            self.logger_object.log(self.file_object,"data loading unsuccessful.exited the get data method of data_getter class")
            raise e


