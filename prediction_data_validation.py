from Prediction_files_validation.raw_pred_validation import raw_pred_data_validation
from Prediction_database_connection.prediciton_database_insertion import dboperation
from prediction_data_transformation.pred_transform import data_transform
from app_logging.logger import logger


class data_validatation_and_transformation:

    def __init__(self,path):
        self.path='Prediction_Batch_files/'
        self.db=dboperation()
        self.raw_data=raw_pred_data_validation(self.path)
        self.tranform=data_transform()
        self.logger_object=logger()
        self.file_object=open("prediction_logs/prediction_main_logs.txt","a+")

    def validate_and_transform(self):
        self.logger_object.log(self.file_object,"prediction validation  and transformation started!!")
        try:

            self.logger_object.log(self.file_object, "file name validation started!!")
            sample,datestamp,timestamp,noofcolumns,column_names=self.raw_data.valuefromschema()
            regex=self.raw_data.manualregexcreation()

            self.raw_data.validatefilename(pattern=regex,lengthofdatestampinfile=datestamp,lengthoftimestampinfile=timestamp)
            self.logger_object.log(self.file_object, "filename validation ended!!")
            self.logger_object.log(self.file_object, "column number validation started!!")
            self.raw_data.validatecolumnlength(numberofcolumns=noofcolumns)
            self.logger_object.log(self.file_object, "column number validation ended!!")
            self.logger_object.log(self.file_object, "empty columns validation started!!")
            self.raw_data.validateemptycolumns()
            self.logger_object.log(self.file_object, "empty columns validation ended!!")
            self.logger_object.log(self.file_object, "data transformation validation started!!")
            self.tranform.addquotestocolumn()
            self.logger_object.log(self.file_object, "data transformation validation ended!!")

            self.logger_object.log(self.file_object, "database operation started!!")

            self.logger_object.log(self.file_object, "table creation started!!")
            self.db.table_creation('prediction',column_names=column_names)
            self.logger_object.log(self.file_object, "table successfully created!!")

            self.logger_object.log(self.file_object, "insertion of data into table started!")
            self.db.insert_into_table('prediction')
            self.logger_object.log(self.file_object, "insertion of data into table successful!")

            self.logger_object.log(self.file_object, "insert table data into csv file started!")
            self.db.insert_table_into_csv('prediction')
            self.logger_object.log(self.file_object, "insert table data into csv file successful!")
            self.logger_object.log(self.file_object, "prediction validation  and transformation successfully ended!")
            self.file_object.close()

        except Exception as e:
            self.logger_object.log(self.file_object,"error in validation and transformation : %s"%e)
            self.file_object.close()
            raise e
