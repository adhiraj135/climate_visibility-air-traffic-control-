import os
import pandas as pd
import json
import shutil
from datetime import datetime
import re
from app_logging.logger import logger


class raw_pred_data_validation:

    def __init__(self, path):
        self.directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger_object=logger()

    def valuefromschema(self):
        try:
            with open(self.schema_path) as f:
                  dic = json.load(f)
                  f.close()
            sample = dic['SampleFileName']
            lengthofdatestampinfile = dic['LengthOfDateStampInFile']
            lengthoftimestampinfile = dic['LengthOfTimeStampInFile']
            numberofcolumns = dic['NumberofColumns']
            colname = dic['ColName']

            self.file_object=open("prediction_logs/valuefromschemavalidationlog.txt","a+")
            message="lengthofdatestampinfile : %s"%lengthofdatestampinfile + "\t" + "lengthoftimestampinfile : %s" %lengthoftimestampinfile + "\t" + "numberofcolumns %s" %numberofcolumns + "\n"
            self.logger_object.log(self.file_object,message)
            self.file_object.close()

        except Exception as e:
            self.file_object=open("prediction_logs/valuefromschemavalidationlog.txt","a+")
            self.logger_object.log(self.file_object,"error occurred in defining values from schema file : %s" %e)
            self.file_object.close()
            raise e


        return sample, lengthofdatestampinfile, lengthoftimestampinfile, numberofcolumns, colname

    def manualregexcreation(self):
        pattern = "['visibility']+['\_']+[\d_]+[\d]+\.csv"
        return pattern

    def crerationofgoodandbadrawdatadir(self):
        try:
            path = os.path.join('prediction_raw_files_validated', 'good_raw')
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join('prediction_raw_files_validated', 'bad_raw')
            if not os.path.isdir(path):
                os.makedirs(path)
            file = open("prediction_logs/generallog.txt", "a+")
            self.logger_object.log(file, "existing good and bad prediction dir created")
            file.close()
        except Exception as e:
            file = open("prediction_logs/generallog.txt", "a+")
            self.logger_object.log(file, "error occurred in creation of good and bad dir : %s" %e)
            file.close()
            raise e


    def existingoodpredictionfolder(self):
        try:
            path = 'prediction_raw_files_validated/good_raw/'
            if os.path.isdir(path):
                 shutil.rmtree(path)
                 file = open("prediction_logs/generallog.txt", "a+")
                 self.logger_object.log(file, "existing good prediction dir removed")
                 file.close()
        except Exception as e:
            file = open("prediction_logs/generallog.txt", "a+")
            self.logger_object.log(file, "error occurred in removing existing good prediction dir: %s" %e)
            file.close()
            raise e


    def existinbadpredictionfolder(self):
        try:
            path = 'prediction_raw_files_validated/bad_raw/'
            if os.path.isdir(path):
                  shutil.rmtree(path)
                  file = open("prediction_logs/generallog.txt", "a+")
                  self.logger_object.log(file, "existing bad prediction dir removed")
                  file.close()

        except Exception as e:
            file = open("prediction_logs/generallog.txt", "a+")
            self.logger_object.log(file, "error occurred in removing existing bad prediction dir : %s" %e)
            file.close()
            raise e


    def movebadfiletoarchive(self):
        try:
            now = datetime.now()
            date = now.date()
            time = datetime.now().strftime('%H%M%S')
            source = 'prediction_raw_files_validated/bad_raw'
            if os.path.isdir(source):
                path = 'prediction_archive_bad_data'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'prediction_archive_bad_data/bad_raw_' + str(date) + '_' + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                for file in os.listdir(source):
                    if file not in os.listdir(dest):
                        shutil.move(source + file, dest)
                if os.path.isdir(path + 'bad_raw/'):
                   shutil.rmtree(path + 'bad_raw/')
            file = open("prediction_logs/generallog.txt", "a+")
            self.logger_object.log(file, "bad prediction data moved to archive folder")
            file.close()

        except Exception as e:
            file = open("prediction_logs/generallog.txt", "a+")
            self.logger_object.log(file, "error in moving bad prediction data to archive folder : %s" %e)
            file.close()
            raise e


    def validatefilename(self, pattern, lengthofdatestampinfile, lengthoftimestampinfile):
        try:
            self.existinbadpredictionfolder()
            self.existingoodpredictionfolder()

            onlyfiles = [files for files in os.listdir(self.directory)]
            self.crerationofgoodandbadrawdatadir()
            self.file_object = open("prediction_logs/filenamevalidation.txt", "a+")
            self.logger_object.log(self.file_object, "file name validation started")
            for file in onlyfiles:
                if re.match(pattern, file):
                    splitatdot = re.split('.csv', file)
                    splitatdot = re.split('_', splitatdot[0])
                    if len(splitatdot[1]) == lengthofdatestampinfile:
                         if len(splitatdot[2]) == lengthoftimestampinfile:
                              shutil.copy('Prediction_Batch_Files/' + file, 'prediction_raw_files_validated/good_raw')
                              self.logger_object.log(self.file_object,"valid file name !! batch file transferred to good dir :: %s" %file)

                         else:
                             shutil.copy('Prediction_Batch_Files/' + file, 'prediction_raw_files_validated/bad_raw')
                             self.logger_object.log(self.file_object, "Invalid file name !! batch file transferred to bad dir :: %s" % file)
                    else:
                        shutil.copy('Prediction_Batch_Files/' + file, 'prediction_raw_files_validated/bad_raw')
                        self.logger_object.log(self.file_object,"Invalid file name !! batch file transferred to bad dir :: %s" % file)
                else:
                    shutil.copy('Prediction_Batch_Files/' + file, 'prediction_raw_files_validated/bad_raw')
                    self.logger_object.log(self.file_object, "Invalid file name !! batch file transferred to bad dir :: %s" % file)
            self.logger_object.log(self.file_object, "file name succesfully validated and batch data transferred to good or bad dir respectively")
            self.file_object.close()
        except Exception as e:
            self.file_object = open("prediction_logs/filenamevalidation.txt", "a+")
            self.logger_object.log(self.file_object, "error in validating filename : %s" %e)
            self.file_object.close()
            raise e
    def validatecolumnlength(self, numberofcolumns):
        try:
            path = 'prediction_raw_files_validated/good_raw/'
            onlyfiles = [files for files in os.listdir(path)]
            self.file_object = open("prediction_logs/columnlengthvalidation.txt", "a+")
            self.logger_object.log(self.file_object, "column len validation started")

            for file in onlyfiles:
                csv = pd.read_csv(path + file)
                if csv.shape[1] == numberofcolumns:
                    pass
                else:
                    shutil.move(path + file, 'prediction_raw_files_validated/bad_raw')
                    self.logger_object.log(self.file_object, "Invalid column length in csv file !! batch file transferred to bad dir :: %s" % file)
            self.logger_object.log(self.file_object, "column len succesfully validated and default files moved to bad directory")
            self.file_object.close()
        except Exception as e:
            self.file_object = open("prediction_logs/columnlengthvalidation.txt", "a+")
            self.logger_object.log(self.file_object, "error in column len validation : %s" %e)
            self.file_object.close()
            raise e



    def validateemptycolumns(self):
        try:
            path = 'prediction_raw_files_validated/good_raw/'
            onlyfiles = [files for files in os.listdir(path)]
            file = open("prediction_logs/emptycolumnvalidation.txt", "a+")
            self.logger_object.log(file, "empty columns validation started")
            for file in onlyfiles:
               csv = pd.read_csv(path + file)
               for col in csv.columns:
                   if len(csv[col]) - csv[col].count() == 0:
                         pass
                   else:
                        shutil.move(path + file, 'prediction_raw_files_validated/bad_raw')
                        self.logger_object.log(file, "invalid file file moved to bad dir :: %s" % file)
                        break

            self.logger_object.log(file, "empty columns successfully validated and default file moved to bad dir")
            file.close()
        except Exception as e:
            file = open("prediction_logs/emptycolumnvalidation.txt", "a+")
            self.logger_object.log(file, "error in emptycolumnvalidation : %s"%e)
            file.close()

    def deletePredictionFile(self):

        if os.path.exists('prediction_output_file/prediction.csv'):
            os.remove('prediction_output_file/prediction.csv')

