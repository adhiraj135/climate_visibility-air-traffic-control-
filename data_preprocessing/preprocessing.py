import pandas as pd
from sklearn.preprocessing import StandardScaler
import numpy as np
from sklearn.impute import KNNImputer


class preprocessing:

    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger_object=logger_object


    def separatelabelcolumn(self, data, labelcolumnname):
        self.logger_object.log(self.file_object,"entered the separate label column method of preprocessing class")
        try:
            x= data.drop(columns=labelcolumnname, axis=1)
            y= data[labelcolumnname]
            self.logger_object.log(self.file_object, "separating label column successful.exited the separate label column method of preprocessing class")
            return x,y
        except Exception as e:
            self.logger_object.log(self.file_object,"error in label separation: %s"%e)
            self.logger_object.log(self.file_object,"separating label column unsuccessful.exited the separate label column method of preprocessing class")
            raise e

    def removeunnecessarycolumn(self, data, columnlist):
        self.logger_object.log(self.file_object, "entered the remove unnecessary column method of preprocessing class")
        try:
           data = data.drop(columns=columnlist, axis=1)
           self.logger_object.log(self.file_object,"removing unnecessary column successful.exited the separate label column method of preprocessing class")
           return data
        except Exception as e:
            self.logger_object.log(self.file_object,"error in removal column : %s"%e)
            self.logger_object.log(self.file_object,"removing unnecessary column unsuccessful.exited the separate label column method of preprocessing class")
            raise e

    def replaceinvalidwithnull(self, data):
        for column in data.columns:
            count = data[column][data[column] == "?"].count()
            if count != 0:
                data[column] = data[column].replace("?", np.nan)
        return data

    def is_null_present(self,data):
        self.logger_object.log(self.file_object,"entered the is null present method of preprocessing class")
        try:
           data_null = pd.DataFrame(data.isnull().sum() == 0)
           null_columns = list(data_null[data_null[0] == False].index)
           self.logger_object.log(self.file_object,"null columns present : %s.exited the is null present method of preprocessing class" % null_columns)
           if data.isnull().sum().sum()== 0:
               return False, null_columns

           else:
               return True, null_columns
        except Exception as e:
            self.logger_object.log(self.file_object,"error occurred in is null method : %s"%e)
            self.logger_object.log(self.file_object,"null columns retrieval unsuccessful.exited the is null present method of preprocessing class" % null_columns)
            raise e



    def scalingdata(self, x):
        scaler = StandardScaler()
        x_scaled = scaler.fit_transform(x)
        return pd.DataFrame(x_scaled, columns=[x.columns])

    def impute_missing_value(self,data):
        self.logger_object.log(self.file_object,"entered the impute missing value method of preprocessing class")
        try:
           imputer = KNNImputer(n_neighbors=3, missing_values=np.nan)
           array = imputer.fit_transform(data)
           data = pd.DataFrame(array)
           self.logger_object.log(self.file_object,"missing values imputation successful.exited the is missing value method of preprocessing class")
           return data

        except Exception as e:
            self.logger_object.log(self.file_object,"error occurred in missing values imputation : %s"%e)
            self.logger_object.log(self.file_object,"missing values imputation unsuccessful.exited the is missing value method of preprocessing class")
            raise e


    def col_with_zero_std(self, data):
        col_to_drop = []
        self.logger_object.log(self.file_object,"entered the column with 0 std method of preprocessing")
        try:
           for col in data.describe().columns:
               if data.describe()[col]['std'] == 0:
                    col_to_drop.append(col)
           self.logger_object.log(self.file_object,"column with 0 std retrieval successful.exited the is col with 0 std of preprocessing")
           return col_to_drop
        except Exception as e:
            self.logger_object.log(self.file_object,"error in column with 0 std retrieval: %s"%e)
            self.logger_object.log(self.file_object,"column with 0 std unsuccessful.exited the column with 0 std method of preprocessing")
            raise e

