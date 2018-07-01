from numpy import array
from math import sqrt
import sys
import os
import numpy as np

sys.path.append('/usr/lib/spark/python');
sys.path.append('/usr/lib/spark/python/lib/py4j-0.9-src.zip')
os.environ["SPARK_HOME"]='/usr/lib/spark'

from pyspark import SparkContext
from pyspark.mllib.clustering import KMeans

def parseVector(line):
	n_line = np.array([float(x) for x in line.split(',')])
	return n_line[1:]


if __name__ == "__main__":
	sc = SparkContext(appName="KMeansPts")
	data_file = sc.textFile(sys.argv[1])
	header = data_file.first()
	data = data_file.filter(lambda x:x != header)
	parsedData = data.map(parseVector).cache()
        
	clusters = KMeans.train(parsedData, 15, maxIterations=100, initializationMode="random")
	print("Final centers: " + str(clusters.clusterCenters))

	def error(point):
		center = clusters.centers[clusters.predict(point)]
		return sqrt(sum([x**2 for x in (point - center)]))

	WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
	print("Within Set Sum of Squared Error = " + str(WSSSE))
	
	predict_list = data.collect();
	for each in predict_list:
		n_line = np.array([float(x) for x in each.split(',')])
	 	cluster_ind = clusters.predict(n_line[1:])
		print(str(int(n_line)) + " patient is in cluster " + str(cluster_ind))
		

