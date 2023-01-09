import pickle
import os
import shutil

class fileoperation:

    def __init__(self,file_object,logger_object):
        self.dir='models/'
        self.file_object=file_object
        self.logger_object=logger_object

    def model_saving(self,model,filename):
        self.logger_object.log(self.file_object,"model saving started.entered the model saving method of fileoperation class")
        try:
            path=os.path.join(self.dir,filename)
            if not os.path.isdir(path):
                   os.makedirs(path,exist_ok=True)

            with open(path+"/"+filename+".sav",'wb') as f:
                 pickle.dump(model,f)
            self.logger_object.log(self.file_object,"model successfully saved.exited the model saving method of fileoperation class")
        except Exception as e:
            self.logger_object.log(self.file_object,"error ocurred in model saving : %s" %e)
            self.logger_object.log(self.file_object,"model saving unsuccessful.exited the model saving method of fileoperation class")
            raise e


    def model_loading(self,filename):
        self.logger_object.log(self.file_object,"model loading started.entered the model loading method of fileoperation class")
        try:
            with open(self.dir+filename+'/'+filename+'.sav',"rb") as f:
                self.logger_object.log(self.file_object,"model loading successfull.exited the model loading method of fileoperation class")
                return pickle.load(f)

        except Exception as e:
            self.logger_object.log(self.file_object,"error in model loading : %s" %e)
            self.logger_object.log(self.file_object,"model loading unsuccessful.exited the model loading method of fileoperation class")
            raise e


    def model_selector(self,cluster_number):

        self.logger_object.log(self.file_object,"model selection started.entered the model selection method of fileoperation class")
        try:
           onlyfile = [files for files in os.listdir(self.dir)]
           for file in onlyfile:
               if str(cluster_number) in file:
                   return file
           self.logger_object.log(self.file_object,"model %s successfully selected.exited the model loading method of fileoperation class" %file)


        except Exception as e:
            self.logger_object.log(self.file_object,"error occured in model selection : %s" %e)
            self.logger_object.log(self.file_object,"model selection unsuccessfull.exited the model loading method of fileoperation class")
            raise e





