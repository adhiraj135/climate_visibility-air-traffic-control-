from prediction_data_ingestion.loader import data_getter
from data_preprocessing.preprocessing import preprocessing
from file_operation.file_method import fileoperation
from app_logging.logger import logger
from Prediction_files_validation.raw_pred_validation import raw_pred_data_validation
import pandas as pd



class prediction_model:

    def __init__(self,path):
        self.validate=raw_pred_data_validation(path)
        self.logger_object=logger()
        self.file_object=open("prediction_logs/model_predcition_log.txt","a+")

    def predict(self):
        self.logger_object.log(self.file_object,"predict from model started!!")
        try:
            self.validate.deletePredictionFile()
            self.data_load=data_getter(self.file_object,self.logger_object)
            data=self.data_load.get_data()

            self.preprocessor=preprocessing(self.file_object,self.logger_object)
            columns=['DATE','WETBULBTEMPF','DewPointTempF','StationPressure','Precip']

            data=self.preprocessor.removeunnecessarycolumn(data=data,columnlist=columns)
            is_null,null_columns=self.preprocessor.is_null_present(data=data)

            if is_null:
                data=self.preprocessor.impute_missing_value(data=data)
            self.file=fileoperation(self.file_object,self.logger_object)
            kmeans=self.file.model_loading(filename='KMeans')
            data['clusters']=kmeans.fit_predict(data)
            self.logger_object.log(self.file_object,"data is %s"%data)
            self.logger_object.log(self.file_object, "clusters are %s" %data['clusters'].unique())
            list_of_cluster=data['clusters'].unique()
            result= []
            for i in list_of_cluster:
                cluster_data = data[data['clusters'] == i]
                cluster_data = cluster_data.drop(columns=['clusters'], axis=1)
                model_name = self.file.model_selector(cluster_number=i)
                model = self.file.model_loading(filename=model_name)
                for val in (model.predict(cluster_data.values)):
                    result.append(val)

            result=pd.DataFrame(result,columns=['prediction'])
            path="prediction_output_file/prediction.csv"
            result.to_csv("prediction_output_file/prediction.csv",header=True)
            self.logger_object.log(self.file_object, "predict from model ended!!,file created at %s"%path)
            self.file_object.close()

        except Exception as e:
            self.logger_object.log(self.file_object, "error in predicting from data: %s"%e)
            self.file_object.close()
            raise e
        return path












