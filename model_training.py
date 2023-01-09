
from training_data_ingestion.loader import data_getter
from data_preprocessing.preprocessing import preprocessing
from data_preprocessing.clustering import Kmeanclustering
from file_operation.file_method import fileoperation
from app_logging.logger import logger
from data_modelling.model_selection import model_finder
from sklearn.model_selection import train_test_split



class training_model:

    def __init__(self):
        self.logger_object=logger()
        self.file_object=open("training_logs/modeltraininglogs.txt","a+")

    def training_model(self):
        self.logger_object.log(self.file_object,"training the model started!")
        try:
           self.data_load=data_getter(self.file_object,self.logger_object)
           data=self.data_load.get_data()
           self.logger_object.log(self.file_object, "data loaded successfully!")

           self.preprocessor=preprocessing(self.file_object,self.logger_object)
           columns=['DATE','WETBULBTEMPF','DewPointTempF','StationPressure','Precip']

           data=self.preprocessor.removeunnecessarycolumn(data=data,columnlist=columns)
           self.logger_object.log(self.file_object, "unnecessary columns removed successfully!")
           is_null,null_columns=self.preprocessor.is_null_present(data=data)

           if is_null:
              data=self.preprocessor.impute_missing_value(data=data)
              self.logger_object.log(self.file_object, "as null values are present : %s null values imputed successfully!"%is_null)

           x,y=self.preprocessor.separatelabelcolumn(data=data,labelcolumnname='VISIBILITY')
           self.logger_object.log(self.file_object, "label column separated successfully!")

           self.cluster=Kmeanclustering(self.file_object,self.logger_object)
           number_of_clusters=self.cluster.elbow_plot(data=x)
           data=self.cluster.cluster_creation(data=x,number_of_clusters=number_of_clusters)
           self.logger_object.log(self.file_object, "clustering done successfully! and data is %s"%data)

           data['label']=y
           self.logger_object.log(self.file_object, "clustering done successfully! and data is %s"%data)
           for i in data['clusters'].unique():
               cluster_data=data[data['clusters']==i]
               cluster_features=cluster_data.drop(columns=['clusters','label'],axis=1)
               cluster_label=cluster_data['label']
               self.logger_object.log(self.file_object, "clustering done successfully! and data is %s" %cluster_features)
               self.logger_object.log(self.file_object,"clustering done successfully! and data is %s" %cluster_label)
               x_train,x_test,y_train,y_test=train_test_split(cluster_features,cluster_label,test_size=1/3,random_state=36)
               self.model=model_finder(self.file_object,self.logger_object)
               model_name,model=self.model.best_model(x_train=x_train,x_test=x_test,y_train=y_train,y_test=y_test)
               self.logger_object.log(self.file_object, "best model derived successfully!")

               self.file=fileoperation(self.file_object,self.logger_object)
               self.file.model_saving(model=model,filename=model_name+str(i))
               self.logger_object.log(self.file_object, "model saved successfully!")

           self.logger_object.log(self.file_object, "model training successfully!")
           self.file_object.close()

        except Exception as e:
            self.logger_object.log(self.file_object,"model training unsuccessful")
            self.file_object.close()
            raise e









