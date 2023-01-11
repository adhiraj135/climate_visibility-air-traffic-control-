from Training_files_validation.raw_validation import raw_data_validation
from Training_database_connection.training_database_insertion import dboperation

from app_logging.logger import logger


class training_data_validatation_and_transformation:

    def __init__(self,path):
        self.raw_data=raw_data_validation(path)
        self.logger_object=logger()
        self.db=dboperation()
        self.file_object = open("training_logs/training_main_log.txt", "a+")


    def training_validate_and_transform(self):


        try:
            self.logger_object.log(self.file_object,"validation started!")
            sample,datestamp,timestamp,noofcolumns,column_names=self.raw_data.valuefromschema()
            regex=self.raw_data.manualregexcreation()

            self.raw_data.validatefilename(pattern=regex,lengthofdatestampinfile=datestamp,lengthoftimestampinfile=timestamp)
            self.logger_object.log(self.file_object, "filename validated successfully!")
            self.raw_data.validatecolumnlength(numberofcolumns=noofcolumns)
            self.logger_object.log(self.file_object, "column len validated successfully!")
            self.raw_data.validateemptycolumns()
            self.logger_object.log(self.file_object, "empty columns validated successfully!")

            self.logger_object.log(self.file_object, "validation completed!")

            self.logger_object.log(self.file_object, "db operation started!")
            self.logger_object.log(self.file_object, "table creation started!")
            self.db.table_creation('training',column_names=column_names)
            self.logger_object.log(self.file_object, "table created successfully!")
            self.logger_object.log(self.file_object, "data insertion into table started!")
            self.db.insert_into_table('training')
            self.logger_object.log(self.file_object, "data inserted inside table successfully!")
            self.logger_object.log(self.file_object, "data insertion in a input csv file started!")
            self.db.insert_table_into_csv('training')
            self.logger_object.log(self.file_object, "data inserted in a input csv file successfully!")
            self.logger_object.log(self.file_object,"validation ended")
            self.file_object.close()

        except Exception as e:
            self.logger_object.log(self.file_object,"error in validating and transformation : %s"%e)
            self.file_object.close()
            raise e