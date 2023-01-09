import os.path

import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operation.file_method import fileoperation
from app_logging.logger import logger

class Kmeanclustering:
    def __init__(self,file_object,logger_object):
        self.logger_object=logger_object
        self.file_object=file_object

    def elbow_plot(self, data):
        self.logger_object.log(self.file_object, "entered the elbow_plot method of Kmeanclustering class")
        try:
            wcss = []
            for i in range(2, 11):
                kmeans = KMeans(n_clusters=i, random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)

            if not os.path.isdir('figures/'):
                os.makedirs('figures/',exist_ok=True)
            plt.plot(range(2, 11), wcss)
            plt.title('Elbow Plot')
            plt.xlabel('k_value')
            plt.ylabel('wcss')
            plt.savefig('figures/elbow_plot.PNG')
            knee = KneeLocator(range(2, 11), wcss, direction='decreasing', curve='convex')
            number_of_clusters = knee.knee
            self.logger_object.log(self.file_object,'no. of clusters are : %s.Exited the elbow_plot method of Kmeanclustering class'%number_of_clusters)
            return number_of_clusters
        except Exception as e:
            self.logger_object.log(self.file_object,'exception occurred in detemining number of clusters: %s'%e)
            self.logger_object.log(self.file_object,'number of clusters not derived.Exited the elbow_plot method of Kmeanclustering class')
            raise e
    def cluster_creation(self, data, number_of_clusters):
        self.logger_object.log(self.file_object,'Entered the cluster creation method of Kmeanclustering class')
        try:
            kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++',n_init='auto', random_state=42)
            data['clusters'] = kmeans.fit_predict(data)
            self.logger_object.log(self.file_object, 'clustering successful,the cluster creation method of Kmeanclustering class')
            file=fileoperation(file_object=open("training_logs/modeltraininglogs.txt","a+"),logger_object=logger())
            file.model_saving(kmeans,'KMeans')
            self.logger_object.log(self.file_object, 'kmean model saved successfully,Exited the cluster creation method of Kmeanclustering class')

            return data
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in cluster creation : %s'%e)
            self.logger_object.log(self.file_object, 'cluster_creation failed,Exited the cluster creation method of Kmeanclustering class')
            raise e