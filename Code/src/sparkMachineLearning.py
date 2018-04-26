# Copyright 2018 Stephen Blystone, Taniya Riar, Juhi Bhandari, & Ishwank Singh

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

    # http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

# https://spark.apache.org/docs/2.3.0/api/python/pyspark.html
from pyspark import SparkConf, SparkContext
# https://spark.apache.org/docs/2.3.0/api/python/pyspark.streaming.html
from pyspark.streaming import StreamingContext
# https://spark.apache.org/docs/2.3.0/api/python/pyspark.streaming.html#module-pyspark.streaming.kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.stat import Statistics
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import stddev, stddev_pop, max, mean
import numpy as np

from elasticsearch import Elasticsearch
import elasticsearch.helpers

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from math import sqrt, ceil

import json
import sys
import requests

DEBUGMODE = False
SCALING_FLAG = True
NUM_STDDEV_ABOVE = 2

# Initialize global variables.
kmeansModel = None
clusterCenters = None
sumOfBytes_stats = None
uniqDstIPs_stats = None
uniqDstPorts_stats = None
thresholds = None
FEATURE_COLS = ['sumOfBytes', 'uniqDstIPs', 'uniqDstPorts']

# https://rsandstroem.github.io/sparkkmeans.html
# https://medium.com/tensorist/using-k-means-to-analyse-hacking-attacks-81957c492c93
# https://stackoverflow.com/questions/43406887/spark-dataframe-how-to-add-a-index-column



def printDebugMsg(msg):
	if DEBUGMODE:
		print(msg)

def calcDistance(point, clusterCenter, clusterCenters):
	cluster_x, cluster_y, cluster_z = clusterCenter
	point_x, point_y, point_z = point
	
	x_safe = False
	y_safe = False
	z_safe = False
	
	
	for c in clusterCenters:
		# If point is less than cluster, then allow it.  we can send less traffic and that is okay.
		# More is where anomalies occur.
		# Since everything is scaled to be centered at 0, then some points are above and others below 0.
		# As long as c[x] > point, then it is good.
		if (c[0] > point_x):
			x_safe = True
		if (c[1] > point_y):
			y_safe = True
		if (c[2] > point_z):
			z_safe = True
		
		# Safe in all dimensions, set each point = cluster point.  This will set dist = 0.
		if x_safe and y_safe and z_safe:
			point_x = cluster_x
			point_y = cluster_y
			point_z = cluster_z
			# If safe by one cluster, then don't need to keep checking.
			break
	
	dist = sqrt((cluster_x - point_x)**2 + (cluster_y - point_y)**2 + (cluster_z - point_z)**2)
	
	return dist

def checkAnomaly(data):
	# # # For each cluster:
	# # # 1) scale data.
	# # # 2) combine data into tuple.
	# # # 3) send to function to calculate distance from center.
	# # # 4) filter where count further than threshold for cluster.
	
	for row in data:
		ANOMALY_FLAG = False
		
		print("row = ", row)
		
		ip = row['srcAddr']
		sumOfBytes = row['sumOfBytes']
		uniqDstIPs = row['uniqDstIPs']
		uniqDstPorts = row['uniqDstPorts']
		
		distanceFromCenters = [None, None, None]
		minDistance = None
		minDistanceCluster = None
		
		
		# For each cluster.
		for i in range(0, 3):
			# Get values for this cluster.
			_, sumOfBytes_mean, sumOfBytes_stddev = sumOfBytes_stats[i]
			_, uniqDstIPs_mean, uniqDstIPs_stddev = uniqDstIPs_stats[i]
			_, uniqDstPorts_mean, uniqDstPorts_stddev = uniqDstPorts_stats[i]
			threshold = thresholds[i]
			center = clusterCenters[i]
			
			
			
			if SCALING_FLAG:
				# If stddev is 0, then set to 1 so don't divide by zero.
				if sumOfBytes_stddev == 0:
					sumOfBytes_stddev = 1
				if uniqDstIPs_stddev == 0:
					uniqDstIPs_stddev = 1
				if uniqDstPorts_stddev == 0:
					uniqDstPorts_stddev = 1
				
				# scale the data.
				sumOfBytes_scaled = (sumOfBytes - sumOfBytes_mean)/(sumOfBytes_stddev)
				uniqDstIPs_scaled = (uniqDstIPs - uniqDstIPs_mean)/(uniqDstIPs_stddev)
				uniqDstPorts_scaled = (uniqDstPorts - uniqDstPorts_mean)/(uniqDstPorts_stddev)
			else:
				# Don't scale.
				sumOfBytes_scaled = sumOfBytes
				uniqDstIPs_scaled = uniqDstIPs
				uniqDstPorts_scaled = uniqDstPorts
			
			
			# Create point.
			point = (sumOfBytes_scaled, uniqDstIPs_scaled, uniqDstPorts_scaled)
			
			# Get distance from center.
			distFromCenter = calcDistance(point, center, clusterCenters)
			
			# Add to list.
			distanceFromCenters[i] = distFromCenter
			
			# Get minDistance and cluster.
			if (minDistance is None) or (minDistance > distFromCenter):
				minDistance = distFromCenter
				minDistanceCluster = i
			
		# # # print("distanceFromCenters =", distanceFromCenters)
		print("distance from closest cluster is", minDistance)
		print("Assigned to cluster", minDistanceCluster)
		
		# Point would be assigned to the closest cluster center.
		# If distance to that cluster center is greater than threshold, then anomaly.
		if minDistance > thresholds[minDistanceCluster]:
			print("threshold distance is", thresholds[minDistanceCluster])
			sendToONOS(ip)
		
		# Print a newline character.
		print("")
		
		
	
	

def extract(row):
	# From https://stackoverflow.com/questions/38384347/how-to-split-vector-into-columns-using-pyspark
	return (row["id"], row["prediction"], row["scaledFeatures"]) + tuple(row['scaledFeatures'].toArray().tolist())
	

def readDataFromES():
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
	results_gen = elasticsearch.helpers.scan(es, index='netflowrepo', doc_type='entry', query={"query": {"match_all": {}}})
	
	results = list(results_gen)
	
	id_list = []
	sumOfBytes_list = []
	uniqDstIPs_list = []
	uniqDstPorts_list = []
	

	for row in results:
		id_list.append(row['_id'])
		sumOfBytes_list.append(row['_source']['sumOfBytes'])
		uniqDstIPs_list.append(row['_source']['uniqDstIPs'])
		uniqDstPorts_list.append(row['_source']['uniqDstPorts'])
	
	
	# Convert data to numpy arrays.
	np_ID = np.array(id_list)
	np_Bytes = np.array(sumOfBytes_list)
	np_DstIPs = np.array(uniqDstIPs_list)
	np_DstPorts = np.array(uniqDstPorts_list)

	# Convert data into Matrix. Each feature is in a column.
	tmp1 = np.concatenate((np_Bytes.reshape((-1,1)), np_DstIPs.reshape((-1,1))), axis=1)
	tmp2 = np.concatenate((tmp1, np_DstPorts.reshape((-1,1))), axis=1)
	mat = sc.parallelize(tmp2.tolist())
	
	# Convert to Data Frame.
	df = spark.createDataFrame(mat)
	df = df.toDF('sumOfBytes', 'uniqDstIPs', 'uniqDstPorts')  # Add headers.
	if DEBUGMODE: df.show()
	
	# Add unique numeric ID, and place in first column.
	df = df.withColumn("id", monotonically_increasing_id())
	df = df.select("id", FEATURE_COLS[0], FEATURE_COLS[1], FEATURE_COLS[2])
	if DEBUGMODE: df.show()
	
	# Convert all data columns to float.
	for col in df.columns:
		if col in FEATURE_COLS:
			df = df.withColumn(col,df[col].cast('float'))
	if DEBUGMODE: df.show()
	
	# Need to convert this to a vector for Spark's implementation of KMeans.
	vecAssembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
	df_kmeans = vecAssembler.transform(df).select('id', 'features')  # Drop other columns.
	if DEBUGMODE: df_kmeans.show()
	
	
	
	
	if SCALING_FLAG:
		# Scale the data.
		scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
		scaler_model = scaler.fit(df_kmeans)
		df_scaled = scaler_model.transform(df_kmeans)
		if DEBUGMODE: df_scaled.show()
		
		# Train the Machine Learning Model.
		k = 3  # silhouette score of 0.799529809602
		kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("scaledFeatures")
		model = kmeans.fit(df_scaled)
		
		centers = model.clusterCenters()
		print("Cluster Centers: ")
		for center in centers:
			print(center)
		
		# Assign events to clusters.
		predictions = model.transform(df_scaled).select('id', 'scaledFeatures', 'prediction')
		
		if DEBUGMODE: predictions.show()
		
		# Extract scaledFeatures column back to FEATURE_COLS
		predictions = predictions.rdd.map(extract).toDF(["id", "prediction", "scaledFeatures", "sumOfBytes", "uniqDstIPs", "uniqDstPorts"])
		
		# Rename scaledFeatures to features.
		predictions = predictions.withColumnRenamed("scaledFeatures", "features")
		
		df_pred = predictions
		
		# # # # Find optimal choice for k.
		# # # cost = np.zeros(20)
		# # # for k in range(2,20):
			# # # kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("scaledFeatures")
			# # # model = kmeans.fit(df_scaled.sample(False,0.1, seed=42))
			# # # cost[k] = model.computeCost(df_scaled)
		# # # printDebugMsg("Cost =")
		# # # for k in range(2, 20):
			# # # printDebugMsg("{0}: {1}".format(k, cost[k]))
		# # # sys.exit(1)
	
	else:
		# Train the Machine Learning Model.
		k = 3  # silhouette score of 0.997791174741 with no scaling.
		# Using "features" has a higher silhouette score of 0.997791174741
		kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
		model = kmeans.fit(df_kmeans)

		centers = model.clusterCenters()
		printDebugMsg("Cluster Centers: ")
		for center in centers:
			printDebugMsg(center)
		
		# Assign events to clusters.
		predictions = model.transform(df_kmeans).select('id', 'features', 'prediction')
		
		# # # # Find optimal choice for k.
		# # # cost = np.zeros(20)
		# # # for k in range(2,20):
			# # # kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
			# # # model = kmeans.fit(df_kmeans.sample(False,0.1, seed=42))
			# # # cost[k] = model.computeCost(df_kmeans)
		# # # printDebugMsg("Cost =")
		# # # for k in range(2, 20):
			# # # printDebugMsg("{0}: {1}".format(k, cost[k]))
		# # # sys.exit(1)
		
		rows = predictions.collect()
		# Create prediction dataframe.
		df_pred = spark.createDataFrame(rows)
		
		# Join prediction with original data.
		df_pred = df_pred.join(df, 'id')
		if DEBUGMODE: df_pred.show()
	
	
	if DEBUGMODE: predictions.show()
	printDebugMsg("Prediction counts for each cluster:")
	if DEBUGMODE: predictions.groupBy('prediction').count().show()
	
	# Evaluate clustering by computing Silhouette score
	evaluator = ClusteringEvaluator()
	silhouette = evaluator.evaluate(predictions)
	printDebugMsg("Silhouette with squared euclidean distance = {0}".format(silhouette))
	
	# Get max, stddev, and mean by cluster.
	row_0 = df_pred.filter(df_pred['prediction'] == 0).groupBy().max('sumOfBytes', 'uniqDstIPs', 'uniqDstPorts').collect()[0]
	row_1 = df_pred.filter(df_pred['prediction'] == 1).groupBy().max('sumOfBytes', 'uniqDstIPs', 'uniqDstPorts').collect()[0]
	row_2 = df_pred.filter(df_pred['prediction'] == 2).groupBy().max('sumOfBytes', 'uniqDstIPs', 'uniqDstPorts').collect()[0]
	sumOfBytes_0_max = row_0[0]
	uniqDstIPs_0_max = row_0[1]
	uniqDstPorts_0_max = row_0[2]
	sumOfBytes_1_max = row_1[0]
	uniqDstIPs_1_max = row_1[1]
	uniqDstPorts_1_max = row_1[2]
	sumOfBytes_2_max = row_2[0]
	uniqDstIPs_2_max = row_2[1]
	uniqDstPorts_2_max = row_2[2]
	
	printDebugMsg("sumOfBytes_0_max = {0}".format(sumOfBytes_0_max))
	printDebugMsg("uniqDstIPs_0_max = {0}".format(uniqDstIPs_0_max))
	printDebugMsg("uniqDstPorts_0_max = {0}".format(uniqDstPorts_0_max))
	printDebugMsg("sumOfBytes_1_max = {0}".format(sumOfBytes_1_max))
	printDebugMsg("uniqDstIPs_1_max = {0}".format(uniqDstIPs_1_max))
	printDebugMsg("uniqDstPorts_1_max = {0}".format(uniqDstPorts_1_max))
	printDebugMsg("sumOfBytes_2_max = {0}".format(sumOfBytes_2_max))
	printDebugMsg("uniqDstIPs_2_max = {0}".format(uniqDstIPs_2_max))
	printDebugMsg("uniqDstPorts_2_max = {0}".format(uniqDstPorts_2_max))
	
	# Get original data stddev.  This is for scaling the new input.
	sumOfBytes_Orig_stddev = df.select(stddev('sumOfBytes')).collect()[0][0]
	uniqDstIPs_Orig_stddev = df.select(stddev('uniqDstIPs')).collect()[0][0]
	uniqDstPorts_Orig_stddev = df.select(stddev('uniqDstPorts')).collect()[0][0]
	printDebugMsg("sumOfBytes_Orig_stddev = {0}".format(sumOfBytes_Orig_stddev))
	printDebugMsg("uniqDstIPs_Orig_stddev = {0}".format(uniqDstIPs_Orig_stddev))
	printDebugMsg("uniqDstPorts_Orig_stddev = {0}".format(uniqDstPorts_Orig_stddev))
	
	# Get scaled data stddev for All clusters.  This is for determining the threshold.
	sumOfBytes_All_stddev = df_pred.select(stddev('sumOfBytes')).collect()[0][0]
	uniqDstIPs_All_stddev = df_pred.select(stddev('uniqDstIPs')).collect()[0][0]
	uniqDstPorts_All_stddev = df_pred.select(stddev('uniqDstPorts')).collect()[0][0]
	printDebugMsg("sumOfBytes_All_stddev = {0}".format(sumOfBytes_All_stddev))
	printDebugMsg("uniqDstIPs_All_stddev = {0}".format(uniqDstIPs_All_stddev))
	printDebugMsg("uniqDstPorts_All_stddev = {0}".format(uniqDstPorts_All_stddev))
	
	# Set values to scaled data for each cluster for determining threshold.
	sumOfBytes_0_stddev = sumOfBytes_1_stddev = sumOfBytes_2_stddev = sumOfBytes_All_stddev
	uniqDstIPs_0_stddev = uniqDstIPs_1_stddev = uniqDstIPs_2_stddev = uniqDstIPs_All_stddev
	uniqDstPorts_0_stddev = uniqDstPorts_1_stddev = uniqDstPorts_2_stddev = uniqDstPorts_All_stddev
	
	
	# Get original data mean.  This is for scaling the new input.
	sumOfBytes_Orig_mean = df.select(mean('sumOfBytes')).collect()[0][0]
	uniqDstIPs_Orig_mean = df.select(mean('uniqDstIPs')).collect()[0][0]
	uniqDstPorts_Orig_mean = df.select(mean('uniqDstPorts')).collect()[0][0]
	printDebugMsg("sumOfBytes_Orig_mean = {0}".format(sumOfBytes_Orig_mean))
	printDebugMsg("uniqDstIPs_Orig_mean = {0}".format(uniqDstIPs_Orig_mean))
	printDebugMsg("uniqDstPorts_Orig_mean = {0}".format(uniqDstPorts_Orig_mean))
	
	# Get scaled data mean for All clusters.  This is for determining the threshold.
	sumOfBytes_All_mean = df_pred.select(mean('sumOfBytes')).collect()[0][0]
	uniqDstIPs_All_mean = df_pred.select(mean('uniqDstIPs')).collect()[0][0]
	uniqDstPorts_All_mean = df_pred.select(mean('uniqDstPorts')).collect()[0][0]
	printDebugMsg("sumOfBytes_All_mean = {0}".format(sumOfBytes_All_mean))
	printDebugMsg("uniqDstIPs_All_mean = {0}".format(uniqDstIPs_All_mean))
	printDebugMsg("uniqDstPorts_All_mean = {0}".format(uniqDstPorts_All_mean))
	
	# Set values to scaled data for each cluster for determining threshold.
	sumOfBytes_0_mean = sumOfBytes_1_mean = sumOfBytes_2_mean = sumOfBytes_All_mean
	uniqDstIPs_0_mean = uniqDstIPs_1_mean = uniqDstIPs_2_mean = uniqDstIPs_All_mean
	uniqDstPorts_0_mean = uniqDstPorts_1_mean = uniqDstPorts_2_mean = uniqDstPorts_All_mean
	
	upperThreshold_0_Bytes = sumOfBytes_0_max + NUM_STDDEV_ABOVE * sumOfBytes_0_stddev
	printDebugMsg("upperThreshold_0_Bytes = {0}".format(upperThreshold_0_Bytes))
	
	upperThreshold_1_Bytes = sumOfBytes_1_max + NUM_STDDEV_ABOVE * sumOfBytes_1_stddev
	printDebugMsg("upperThreshold_1_Bytes = {0}".format(upperThreshold_1_Bytes))
	
	upperThreshold_2_Bytes = sumOfBytes_2_max + NUM_STDDEV_ABOVE * sumOfBytes_2_stddev
	printDebugMsg("upperThreshold_2_Bytes = {0}".format(upperThreshold_2_Bytes))
	
	upperThreshold_0_DstIPs = uniqDstIPs_0_max + NUM_STDDEV_ABOVE * uniqDstIPs_0_stddev
	printDebugMsg("upperThreshold_0_DstIPs = {0}".format(upperThreshold_0_DstIPs))
	
	upperThreshold_1_DstIPs = uniqDstIPs_1_max + NUM_STDDEV_ABOVE * uniqDstIPs_1_stddev
	printDebugMsg("upperThreshold_1_DstIPs = {0}".format(upperThreshold_1_DstIPs))
	
	upperThreshold_2_DstIPs = uniqDstIPs_2_max + NUM_STDDEV_ABOVE * uniqDstIPs_2_stddev
	printDebugMsg("upperThreshold_2_DstIPs = {0}".format(upperThreshold_2_DstIPs))
	
	upperThreshold_0_DstPorts = uniqDstPorts_0_max + NUM_STDDEV_ABOVE * uniqDstPorts_0_stddev
	printDebugMsg("upperThreshold_0_DstPorts = {0}".format(upperThreshold_0_DstPorts))
	
	upperThreshold_1_DstPorts = uniqDstPorts_1_max + NUM_STDDEV_ABOVE * uniqDstPorts_1_stddev
	printDebugMsg("upperThreshold_1_DstPorts = {0}".format(upperThreshold_1_DstPorts))
	
	upperThreshold_2_DstPorts = uniqDstPorts_2_max + NUM_STDDEV_ABOVE * uniqDstPorts_2_stddev
	printDebugMsg("upperThreshold_2_DstPorts = {0}".format(upperThreshold_2_DstPorts))
	
	# Combined upper threshold is pythagorean in 3 dimensions.
	# s^2 = x^2 + y^2 + z^2
	threshold_0 = sqrt(upperThreshold_0_Bytes**2 + upperThreshold_0_DstIPs**2 + upperThreshold_0_DstPorts**2)
	threshold_1 = sqrt(upperThreshold_1_Bytes**2 + upperThreshold_1_DstIPs**2 + upperThreshold_1_DstPorts**2)
	threshold_2 = sqrt(upperThreshold_2_Bytes**2 + upperThreshold_2_DstIPs**2 + upperThreshold_2_DstPorts**2)
	
	
	printDebugMsg("threshold_0 = {0}".format(threshold_0))
	printDebugMsg("threshold_1 = {0}".format(threshold_1))
	printDebugMsg("threshold_2 = {0}".format(threshold_2))
	
	
	# Combine everything needed to return values.
	sumOfBytes_0 = (sumOfBytes_0_max, sumOfBytes_Orig_mean, sumOfBytes_Orig_stddev)
	sumOfBytes_1 = (sumOfBytes_1_max, sumOfBytes_Orig_mean, sumOfBytes_Orig_stddev)
	sumOfBytes_2 = (sumOfBytes_2_max, sumOfBytes_Orig_mean, sumOfBytes_Orig_stddev)
	uniqDstIPs_0 = (uniqDstIPs_0_max, uniqDstIPs_Orig_mean, uniqDstIPs_Orig_stddev)
	uniqDstIPs_1 = (uniqDstIPs_1_max, uniqDstIPs_Orig_mean, uniqDstIPs_Orig_stddev)
	uniqDstIPs_2 = (uniqDstIPs_2_max, uniqDstIPs_Orig_mean, uniqDstIPs_Orig_stddev)
	uniqDstPorts_0 = (uniqDstPorts_0_max, uniqDstPorts_Orig_mean, uniqDstPorts_Orig_stddev)
	uniqDstPorts_1 = (uniqDstPorts_1_max, uniqDstPorts_Orig_mean, uniqDstPorts_Orig_stddev)
	uniqDstPorts_2 = (uniqDstPorts_2_max, uniqDstPorts_Orig_mean, uniqDstPorts_Orig_stddev)
	
	# Update global variables.
	global kmeansModel
	global clusterCenters
	global sumOfBytes_stats
	global uniqDstIPs_stats
	global uniqDstPorts_stats
	global thresholds
	kmeansModel = model
	clusterCenters = centers
	sumOfBytes_stats = [sumOfBytes_0, sumOfBytes_1, sumOfBytes_2]
	uniqDstIPs_stats = [uniqDstIPs_0, uniqDstIPs_1, uniqDstIPs_2]
	uniqDstPorts_stats = [uniqDstPorts_0, uniqDstPorts_1, uniqDstPorts_2]
	thresholds = (threshold_0, threshold_1, threshold_2)
	
	
	
def sendToONOS(anomalyIP):
	# First get a list of all hosts.
	# Create dictionary mapping from IP to host switch.
	ipToSwitchMap = {}
	ipToSwitchPortMap = {}
	
	hosts = requests.get('http://10.28.34.39:8181/onos/v1/hosts', auth=('onos', 'rocks'))
	
	host_json = hosts.json()
	for host in host_json['hosts']:
		IP = host['ipAddresses'][0]
		switch = host['locations'][0]['elementId']
		
		ipToSwitchMap[IP] = switch
	
	# Send a request to ONOS to drop traffic for that anomalyIP from the
	# switch the bad device is connected on.
	print("Send to ONOS: Need to block {0}".format(anomalyIP))
	# Configure parameters needed for POST request
	blockData = {
		"priority": 40000,
		"timeout": 0,
		"isPermanent": "true",
		"deviceId": ipToSwitchMap[anomalyIP],
		"treatment": {},  # blank treatment means drop traffic.
		"selector": {
			"criteria": [
				{
					"type": "ETH_TYPE",
					"ethType": "0x0800"  # IPv4 Traffic.
				},
				{
					"type": "IPV4_SRC",
					"ip": "{0}/32".format(anomalyIP)  # Must include subnet mask.
				}
			]
		}
	}
	
	urlToPost = "http://10.28.34.39:8181/onos/v1/flows/{0}?appId=org.onosproject.fwd".format(ipToSwitchMap[anomalyIP])
	print("urlToPost = {0}".format(urlToPost))
	resp = requests.post(urlToPost, data=json.dumps(blockData), auth=('onos', 'rocks'))
	print("response is {0}".format(resp))
		
	
if __name__ == "__main__":
	# SparkContext represents connection to a Spark cluster.
	conf = SparkConf()
	conf.setAppName("Spark Machine Learning App")
	conf.setMaster('local[2]')
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	
	spark = SparkSession \
	.builder \
	.config(conf=conf) \
	.getOrCreate()
	
	readDataFromES()
	
	# StreamingContext represents connection to a Spark cluster from existing SparkContext.
	ssc = StreamingContext(sc, 60)  # the number indicates how many seconds each batch lasts.
	
	# Creates an input stream that pulls events from Kafka.
	kvs = KafkaUtils.createStream(ssc, "streamsetApp:2181", "spark-streaming-consumer", {"NETFLOW":1})
	parsed = kvs.map(lambda x: json.loads(x[1]))
	
	# Get only elements that are needed and rename to make it clear.
	netflow_dict = parsed.map(lambda x: ({'srcAddr': x['srcaddr_s'], 'srcPort': x['srcport'], 'dstAddr': x['dstaddr_s'], 'dstPort': x['dstport'], 'tcpFlags': x['tcp_flags'], 'protocol': x['proto'], 'timestampStart': x['first'], 'timestampEnd': x['last'], 'numBytes': x['dOctets'], 'numFlows': x['count']}))
	# # # netflow_dict.pprint()
	
	# Get Sum of Flows sent from Source IP in window.
	sumOfFlows = netflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["numFlows"])).reduceByKey(lambda x, y: x + y)
	
	# Get sum of Bytes sent from Source IP in window.
	sumOfBytes = netflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["numBytes"])).reduceByKey(lambda x, y: x + y)
	
	# Count of unique dest IP that source IP talked to in window.
	# First map gets unique src/dst pairs.  Second map reduces just to srcAddr and counts number of uniq dstAddr.
	uniqDstIPs = netflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["dstAddr"])).countByValue().map(lambda e: e[0][0]).countByValue()
	
	# Count of unique destination ports that source IP talked to in window.
	# First map gets unique src/dst pairs.  Second map reduces just to srcAddr and counts number of uniq dstPort.
	uniqDstPorts = netflow_dict.filter(lambda e: "srcAddr" in e).map(lambda e: (e["srcAddr"], e["dstPort"])).countByValue().map(lambda e: e[0][0]).countByValue()
	
	
	
	# Join data together, joining by srcAddr.  Need to do fullOuterJoin since some srcAddr may not be in some RDDs.
	# Unlikely that a srcAddr exceeded threshold in all of the features.
	join1 = sumOfBytes.fullOuterJoin(uniqDstIPs)
	join2 = join1.fullOuterJoin(uniqDstPorts)
	
	# Map into format: (SrcAddr, sumOfBytes, uniqDstIPs, uniqDstPorts).
	joined = join2.map(lambda e: ({"srcAddr": e[0], "sumOfBytes": e[1][0][0], "uniqDstIPs": e[1][0][1], "uniqDstPorts": e[1][1]}))
	joined.pprint(12)  # Show for all 12 IPs.
	
	# Check for anomaly.
	joined.foreachRDD(lambda rdd: rdd.foreachPartition(checkAnomaly))
	
	
	# Start the execution of streams.
	ssc.start()
	
	# Wait for execution to stop.
	ssc.awaitTermination()
