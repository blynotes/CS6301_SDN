from __future__ import print_function
from elasticsearch import Elasticsearch
import elasticsearch.helpers

from scipy import stats
import numpy as np
from pyspark.mllib.stat import Statistics
from pyspark import SparkConf, SparkContext

# https://stackoverflow.com/questions/41961245/how-to-retrieve-1m-documents-with-elasticsearch-in-python

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
results_gen = elasticsearch.helpers.scan(es, index='netflowrepo', doc_type='entry', query={"query": {"match_all": {}}})

results = list(results_gen)

print("Got {0} hits:".format(len(results)))

sumOfFlows_list = []
sumOfBytes_list = []
uniqDstIPs_list = []
uniqDstPorts_list = []

for row in results:
	sumOfFlows_list.append(row['_source']['sumOfFlows'])
	sumOfBytes_list.append(row['_source']['sumOfBytes'])
	uniqDstIPs_list.append(row['_source']['uniqDstIPs'])
	uniqDstPorts_list.append(row['_source']['uniqDstPorts'])


conf = SparkConf()
conf.setAppName("Test Read ES")
conf.setMaster('local[2]')
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


# Convert data to numpy arrays.
np_Flows = np.array(sumOfFlows_list)
np_Bytes = np.array(sumOfBytes_list)
np_DstIPs = np.array(uniqDstIPs_list)
np_DstPorts = np.array(uniqDstPorts_list)

# Convert data into Matrix. Each feature is in a column.
tmp1 = np.concatenate((np_Flows.reshape((-1,1)), np_Bytes.reshape((-1,1))), axis=1)
tmp2 = np.concatenate((tmp1, np_DstIPs.reshape((-1,1))), axis=1)
tmp3 = np.concatenate((tmp2, np_DstPorts.reshape((-1,1))), axis=1)

print("Matrix shape =", tmp3.shape)

mat = sc.parallelize(tmp3.tolist())

summary = Statistics.colStats(mat)

mean = summary.mean()
max = summary.max()
stddev = np.sqrt(summary.variance())
print("count =", summary.count())
print("mean =", summary.mean())
print("min =", summary.min())
print("max =", summary.max())
print("variance =", summary.variance())
# # # print("Pearson Correlation = ", Statistics.corr(mat, method="pearson"))



#######################################################
print("NORMALIZED DATA:")

scaled_Flows = (np_Flows - mean[0])/(stddev[0])
scaled_Bytes = (np_Bytes - mean[1])/(stddev[1])
scaled_DstIPs = (np_DstIPs - mean[2])/(stddev[2])
scaled_DstPorts = (np_DstPorts - mean[3])/(stddev[3])


tmp1 = np.concatenate((scaled_Flows.reshape((-1,1)), scaled_Bytes.reshape((-1,1))), axis=1)
tmp2 = np.concatenate((tmp1, scaled_DstIPs.reshape((-1,1))), axis=1)
tmp3 = np.concatenate((tmp2, scaled_DstPorts.reshape((-1,1))), axis=1)

mat = sc.parallelize(tmp3.tolist())

summary = Statistics.colStats(mat)

print("count =", summary.count())
print("mean =", summary.mean())
print("min =", summary.min())
print("max =", summary.max())
print("variance =", summary.variance())
# # # print("Pearson Correlation = ", Statistics.corr(mat, method="pearson"))

#######################################################
# Checking the Normalized Data
print("Z-SCORE:")
zscore_Flows = stats.zscore(np_Flows)
zscore_Bytes = stats.zscore(np_Bytes)
zscore_DstIPs = stats.zscore(np_DstIPs)
zscore_DstPorts = stats.zscore(np_DstPorts)

tmp1 = np.concatenate((zscore_Flows.reshape((-1,1)), zscore_Bytes.reshape((-1,1))), axis=1)
tmp2 = np.concatenate((tmp1, zscore_DstIPs.reshape((-1,1))), axis=1)
tmp3 = np.concatenate((tmp2, zscore_DstPorts.reshape((-1,1))), axis=1)

mat = sc.parallelize(tmp3.tolist())

summary = Statistics.colStats(mat)

print("count =", summary.count())
print("mean =", summary.mean())
print("min =", summary.min())
print("max =", summary.max())
print("variance =", summary.variance())
# # # print("Pearson Correlation = ", Statistics.corr(mat, method="pearson"))
