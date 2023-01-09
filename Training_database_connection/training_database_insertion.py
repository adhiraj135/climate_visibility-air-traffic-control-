import os
import csv
import sqlite3
import shutil
from app_logging.logger import logger
import  mysql.connector as connection
import pandas as pd


class dboperation:
    def __init__(self):
        self.path = "training_database/"
        self.json = 'schema_training.json'
        self.good_data = "training_raw_files_validated/good_raw/"
        self.bad_data = "training_raw_files_validated/bad_raw/"
        self.logger_object=logger()







    def databasecreation(self, database_name):
        try:
            conn= connection.connect(host="localhost", user="root", passwd="mysql@123", use_pure=True)
            cursor=conn.cursor()
            cursor.execute("create database if not exists {name}".format(name=database_name))
            file = open("training_logs/databasecreation.txt", "a+")
            self.logger_object.log(file, "database %s succesfully created" %database_name)
            file.close()
        except Exception as e:
            file = open("training_logs/databasecreation.txt", "a+")
            self.logger_object.log(file, "error in database connection : %s" % e)
            file.close()
            raise e
        conn.close()

    def connection(self,database_name):
        try:
            self.databasecreation(database_name)
            conn=connection.connect(host="localhost",database=database_name,user="root", passwd="mysql@123", use_pure=True)
            file = open("training_logs/databaseconnection.txt", "a+")
            self.logger_object.log(file, "database %s succesfully created" % database_name)
            file.close()
        except Exception as e:
            file = open("training_logs/databaseconnection.txt", "a+")
            self.logger_object.log(file, "error in database connection : %s" % e)
            file.close()
            raise e
        return conn


    def table_creation(self, database_name, column_names):
        try:
            conn=self.connection(database_name)
            c = conn.cursor()
            c.execute("SELECT count(*) from INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'AND TABLE_NAME = 'good_raw_data_final'")
            if c.fetchone()[0]==1:
                conn.close()
                file = open("training_logs/tablecreation.txt", "a+")
                self.logger_object.log(file, "table already exists now data from table will be transfered to csv file")
                file.close()
            else:
                for column in column_names:
                   data_type = column_names[column]
                   try:
                       c.execute('ALTER TABLE good_raw_data_final ADD COLUMN {column_names} {type}'.format(column_names=column,
                                                                                            type=data_type))
                   except:
                       c.execute('CREATE TABLE good_raw_data_final ({column_names} {type}({number}))'.format(column_names=column,
                                                                                      type=data_type,number=20))
                conn.close()
                file = open("training_logs/tablecreation.txt", "a+")
                self.logger_object.log(file, "table successfully created")
                file.close()

        except Exception as e:

            file = open("training_logs/tablecreation.txt", "a+")
            self.logger_object.log(file, "error in table creation : %s" % e)
            file.close()

            raise e

    def insert_into_table(self, database_name):

        conn=self.connection(database_name)
        c = conn.cursor()
        onlyfiles = [files for files in os.listdir(self.good_data)]

        try:
            rows=[]
            c.execute("select count(*) from good_raw_data_final")
            for file in onlyfiles:
                df=pd.read_csv(self.good_data+file)
                rows.append(df.shape[0])
            if sum(rows)==c.fetchone()[0]:
                conn.close()
                self.file_object = open("training_logs/insertionintotable.txt", "a+")
                self.logger_object.log(self.file_object, "data already present in table,no need to insert")
                self.file_object.close()

            else:
                for file in onlyfiles:
                    with open(self.good_data + file, "r") as f:
                        next(f)
                        reader = csv.reader(f, delimiter="\n")
                        for i in enumerate(reader):
                            for data in i[1]:
                               try:
                                   c.execute("insert into good_raw_data_final values({values})".format(values=data))
                                   conn.commit()
                               except Exception as e:
                                    raise e
                conn.close()
                self.file_object = open("training_logs/insertionintotable.txt", "a+")
                self.logger_object.log(self.file_object, "data sucessfully inserted in table")
                self.file_object.close()

        except Exception as e:
            conn.rollback()
            self.file_object = open("training_logs/insertionintotable.txt", "a+")
            self.logger_object.log(self.file_object, "error in table insertion : %s" % e)
            self.file_object.close()
            conn.close()
            raise e



    def insert_table_into_csv(self, database_name):

        path = 'trainingfilefromDB/'
        file = 'input_file.csv'
        try:
            conn=self.connection(database_name)
            c = conn.cursor()
            query="select * from good_raw_data_final;"

            if not os.path.isdir(path):
                os.makedirs(path)

            df=pd.read_sql_query(query,conn)
            df.to_csv(path+file,index=False)



            self.file_object = open("training_logs/insertintocsv.txt", "a+")
            self.logger_object.log(self.file_object,"data of table sucessfully inserted in csv file %s" %file)
            self.file_object.close()
        except Exception as e:
            self.file_object = open("training_logs/insertintocsv.txt", "a+")
            self.logger_object.log(self.file_object, "error in insertion in csv : %s" % e)
            self.file_object.close()
            raise e
