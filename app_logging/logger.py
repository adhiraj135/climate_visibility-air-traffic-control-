from datetime import datetime

class logger:
    def __init__(self):
        pass

    def log(self,file_object,log_message):
        self.now=datetime.now()
        self.date=self.now.date()
        self.time=self.now.strftime("%H:%M:%S")

        file_object.write(str(self.date)+"/"+str(self.time) + "\t\t" + log_message + "\n")



