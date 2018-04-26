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
import numpy as np

from elasticsearch import Elasticsearch
import elasticsearch.helpers

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from math import sqrt

from itertools import combinations
from sklearn.preprocessing import PolynomialFeatures
from sklearn.cross_validation import train_test_split
import sklearn.feature_selection

import json
import sys
import requests

import pandas as pd

def _map_to_pandas(rdds):
	""" Needs to be here due to pickling issues """
	# From https://gist.github.com/joshlk/871d58e01417478176e7
	return [pd.DataFrame(list(rdds))]

def toPandas(df, n_partitions=None):
	"""
	Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
	repartitioned if `n_partitions` is passed.
	:param df:			 	pyspark.sql.DataFrame
	:param n_partitions:	int or None
	:return:				pandas.DataFrame
	"""
	# From https://gist.github.com/joshlk/871d58e01417478176e7
	if n_partitions is not None: df = df.repartition(n_partitions)
	df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
	df_pand = pd.concat(df_pand)
	df_pand.columns = df.columns
	return df_pand


# https://rsandstroem.github.io/sparkkmeans.html
# https://medium.com/tensorist/using-k-means-to-analyse-hacking-attacks-81957c492c93
# https://stackoverflow.com/questions/43406887/spark-dataframe-how-to-add-a-index-column

# # # # Evaluate clustering by computing Within Set Sum of Squared Errors
# # # def error(clusters, point):
	# # # center = clusters.centers[clusters.predict(point)]
	# # # return sqrt(sum([x**2 for x in (point - center)]))
	
	
FEATURE_COLS = ['sumOfBytes', 'uniqDstIPs', 'uniqDstPorts']

def add_interactions(df):
	combos = list(combinations(list(df.columns), 2))
	colnames = list(df.columns) + ['_'.join(x) for x in combos]
	
	# Have to convert to Pandas Dataframe.
	pandas_df = toPandas(df)

	#Finding interactions
	poly = PolynomialFeatures(interaction_only=True, include_bias=False)
	df = poly.fit_transform(pandas_df)  
	df = pd.DataFrame(df)  # Pandas Dataframe.
	df.columns = colnames
	
	noint_indices = [i for i, x in enumerate(list((df==0).all())) if x]
	df = df.drop(df.columns[noint_indices], axis=1)
	
	# remove columns beginning with "id_"
	no_id_indices = [i for i, x in enumerate(df.columns.values) if x.startswith("id_")]
	df = df.drop(df.columns[no_id_indices], axis=1)
	
	# Convert back to Spark Dataframe.
	df = spark.createDataFrame(df)
	return df


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

	# # # # Convert data into Matrix. Each feature is in a column.
	# # # tmp1 = np.concatenate((np_ID.reshape((-1,1)), np_Bytes.reshape((-1,1))), axis=1)
	# # # tmp2 = np.concatenate((tmp1, np_DstIPs.reshape((-1,1))), axis=1)
	# # # tmp3 = np.concatenate((tmp2, np_DstPorts.reshape((-1,1))), axis=1)
	# # # mat = sc.parallelize(tmp3.tolist())
	# Convert data into Matrix. Each feature is in a column.
	tmp1 = np.concatenate((np_Bytes.reshape((-1,1)), np_DstIPs.reshape((-1,1))), axis=1)
	tmp2 = np.concatenate((tmp1, np_DstPorts.reshape((-1,1))), axis=1)
	mat = sc.parallelize(tmp2.tolist())
	
	# Convert to Data Frame.
	df = spark.createDataFrame(mat)
	df = df.toDF('sumOfBytes', 'uniqDstIPs', 'uniqDstPorts')  # Add headers.
	df.show()
	
	# Add unique numeric ID, and place in first column.
	df = df.withColumn("id", monotonically_increasing_id())
	df = df.select("id", FEATURE_COLS[0], FEATURE_COLS[1], FEATURE_COLS[2])
	df.show()
	
	# Convert all data columns to float.
	for col in df.columns:
		if col in FEATURE_COLS:
			df = df.withColumn(col,df[col].cast('float'))
	df.show()
	
	# Add Interactions.
	df = add_interactions(df)
	df.show()
	
	# Create labels for training showing 
	
	X_train, X_test, y_train, y_test = train_test_split(df, y, train_size=0.80, random_state=42)
	

	select = sklearn.feature_selection.SelectKBest(k=60)
	selected_features = select.fit(X_train, y_train)
	indices_selected = selected_features.get_support(indices=True)
	colnames_selected = [X.columns[i] for i in indices_selected]
	
	# Need to convert this to a vector for Spark's implementation of KMeans.
	vecAssembler = VectorAssembler(inputCols=df.columns, outputCol="features")
	df_kmeans = vecAssembler.transform(df).select('id', 'features')  # Drop other columns.
	df_kmeans.show()

	# Scale the data.
	scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
	scaler_model = scaler.fit(df_kmeans)
	df_scaled = scaler_model.transform(df_kmeans)
	df_scaled.show()
	
	
	# Find optimal choice for k.
	cost = np.zeros(20)
	for k in range(2,20):
		kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("scaledFeatures")
		model = kmeans.fit(df_scaled.sample(False,0.1, seed=42))
		cost[k] = model.computeCost(df_scaled)
	print("Cost =")
	for k in range(2, 20):
		print("{0}: {1}".format(k, cost[k]))
	sys.exit(1)
		
	# Train the Machine Learning Model.
	k = 3  # silhouette score of 0.997791174741 with no scaling.
	# TODO: Using "features" has a higher silhouette score of 0.997791174741 vs 0.242504059888
	kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("scaledFeatures")
	model = kmeans.fit(df_scaled)

	centers = model.clusterCenters()
	print("Cluster Centers: ")
	for center in centers:
		print(center)
	
	
	# Assign events to clusters.
	predictions = model.transform(df_scaled).select('id', 'features', 'prediction')
	
	print("Prediction counts for each cluster:")
	predictions.groupBy('prediction').count().show()
	
	rows = predictions.collect()
	print(rows[:3])
	
	
	# Evaluate clustering by computing Silhouette score
	evaluator = ClusteringEvaluator()
	silhouette = evaluator.evaluate(predictions)
	print("Silhouette with squared euclidean distance = " + str(silhouette))
	
	# Create prediction dataframe.
	df_pred = spark.createDataFrame(rows)
	df_pred.show()
	
	# Join prediction with original data.
	df_pred = df_pred.join(df, 'id')
	df_pred.show()
	
	sys.exit(1)
	
	
	
	# # # for maxIter in range(300, 1000, 100):
		# # # for x in range(1, 50):
			# # # # Build the model (cluster the data)
			# # # clusters = KMeans.train(mat, x, maxIterations=maxIter, initializationMode="random")

			# # # WSSSE = mat.map(lambda point: error(clusters, point)).reduce(lambda x, y: x + y)
			# # # print("cluster {0}: maxIter {1}: Within Set Sum of Squared Error = {2}".format(x, maxIter, WSSSE))
	
	
	
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