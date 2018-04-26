from __future__ import print_function

# https://spark.apache.org/docs/2.3.0/api/python/pyspark.html
from pyspark import SparkConf, SparkContext
# https://spark.apache.org/docs/2.3.0/api/python/pyspark.streaming.html
from pyspark.streaming import StreamingContext
# https://spark.apache.org/docs/2.3.0/api/python/pyspark.streaming.html#module-pyspark.streaming.kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.stat import Statistics
import numpy as np

from elasticsearch import Elasticsearch
import elasticsearch.helpers

import json
import sys
import requests

def readDataFromES():
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
	results_gen = elasticsearch.helpers.scan(es, index='netflowrepo', doc_type='entry', query={"query": {"match_all": {}}})
	
	results = list(results_gen)
	
	sumOfFlows_list = []
	sumOfBytes_list = []
	uniqDstIPs_list = []
	uniqDstPorts_list = []

	for row in results:
		sumOfFlows_list.append(row['_source']['sumOfFlows'])
		sumOfBytes_list.append(row['_source']['sumOfBytes'])
		uniqDstIPs_list.append(row['_source']['uniqDstIPs'])
		uniqDstPorts_list.append(row['_source']['uniqDstPorts'])

	
	# Convert data to numpy arrays.
	np_Flows = np.array(sumOfFlows_list)
	np_Bytes = np.array(sumOfBytes_list)
	np_DstIPs = np.array(uniqDstIPs_list)
	np_DstPorts = np.array(uniqDstPorts_list)

	# Convert data into Matrix. Each feature is in a column.
	tmp1 = np.concatenate((np_Flows.reshape((-1,1)), np_Bytes.reshape((-1,1))), axis=1)
	tmp2 = np.concatenate((tmp1, np_DstIPs.reshape((-1,1))), axis=1)
	tmp3 = np.concatenate((tmp2, np_DstPorts.reshape((-1,1))), axis=1)
	mat = sc.parallelize(tmp3.tolist())
	
	summary = Statistics.colStats(mat)
	
	print("count =", summary.count())
	print("mean =", summary.mean())
	print("min =", summary.min())
	print("max =", summary.max())
	print("variance =", summary.variance())
	
	mean = summary.mean()
	max = summary.max()
	stddev = np.sqrt(summary.variance())
	
	return (mean, max, stddev)
	
	
def sendToONOS(anomalies):
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
	
	# For each anomaly IP, send a request to ONOS to drop traffic for that srcAddr from the
	# switch the bad device is connected on.
	for entry in anomalies:
		print("Send to ONOS: Need to block {0}".format(entry["srcAddr"]))
		# Configure parameters needed for POST request
		blockData = {
			"priority": 40000,
			"timeout": 0,
			"isPermanent": "true",
			"deviceId": ipToSwitchMap[entry["srcAddr"]],
			"treatment": {},  # blank treatment means drop traffic.
			"selector": {
				"criteria": [
					{
						"type": "ETH_TYPE",
						"ethType": "0x0800"  # IPv4 Traffic.
					},
					{
						"type": "IPV4_SRC",
						"ip": "{0}/32".format(entry["srcAddr"])  # Must include subnet mask.
					}
				]
			}
		}
		
		urlToPost = "http://10.28.34.39:8181/onos/v1/flows/{0}?appId=org.onosproject.fwd".format(ipToSwitchMap[entry["srcAddr"]])
		print("urlToPost = {0}".format(urlToPost))
		resp = requests.post(urlToPost, data=json.dumps(blockData), auth=('onos', 'rocks'))
		print("response is {0}".format(resp))
		


if __name__ == "__main__":
	# SparkContext represents connection to a Spark cluster.
	conf = SparkConf()
	conf.setAppName("Spark Statistical Methods App")
	conf.setMaster('local[2]')
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	
	# Read data from elasticsearch and return mean, max, and stddev.
	mean, max, stddev = readDataFromES()
	
	# Determine the numStdDevAboveMean for each feature.
	# We want to increase it until the max value is within that range.
	numStdDevAboveMean_Flows = 2
	while (max[0] > mean[0] + (numStdDevAboveMean_Flows * stddev[0])):
		# # # print("Increasing numStdDevAboveMean_Flows")
		numStdDevAboveMean_Flows += 1
	upperThreshold_Flows = mean[0] + numStdDevAboveMean_Flows * stddev[0]
	print("max[0] = {0}".format(max[0]))
	print("mean[0] = {0}".format(mean[0]))
	print("stddev[0] = {0}".format(stddev[0]))
	print("numStdDevAboveMean_Flows = {0}".format(numStdDevAboveMean_Flows))
	print("upperThreshold_Flows = {0}".format(upperThreshold_Flows))
	
	numStdDevAboveMean_Bytes = 2
	while (max[1] > mean[1] + (numStdDevAboveMean_Bytes * stddev[1])):
		# # # print("Increasing numStdDevAboveMean_Bytes")
		numStdDevAboveMean_Bytes += 1
	upperThreshold_Bytes = mean[1] + numStdDevAboveMean_Bytes * stddev[1]
	print("max[1] = {0}".format(max[1]))
	print("mean[1] = {0}".format(mean[1]))
	print("stddev[1] = {0}".format(stddev[1]))
	print("numStdDevAboveMean_Bytes = {0}".format(numStdDevAboveMean_Bytes))
	print("upperThreshold_Bytes = {0}".format(upperThreshold_Bytes))
		
	numStdDevAboveMean_DstIPs = 2
	while (max[2] > mean[2] + (numStdDevAboveMean_DstIPs * stddev[2])):
		# # # print("Increasing numStdDevAboveMean_DstIPs")
		numStdDevAboveMean_DstIPs += 1
	upperThreshold_DstIPs = mean[2] + numStdDevAboveMean_DstIPs * stddev[2]
	print("max[2] = {0}".format(max[2]))
	print("mean[2] = {0}".format(mean[2]))
	print("stddev[2] = {0}".format(stddev[2]))
	print("numStdDevAboveMean_DstIPs = {0}".format(numStdDevAboveMean_DstIPs))
	print("upperThreshold_DstIPs = {0}".format(upperThreshold_DstIPs))
		
	numStdDevAboveMean_DstPorts = 2
	while (max[3] > mean[3] + (numStdDevAboveMean_DstPorts * stddev[3])):
		# # # print("Increasing numStdDevAboveMean_DstPorts")
		numStdDevAboveMean_DstPorts += 1
	upperThreshold_DstPorts = mean[3] + numStdDevAboveMean_DstPorts * stddev[3]
	print("max[3] = {0}".format(max[3]))
	print("mean[3] = {0}".format(mean[3]))
	print("stddev[3] = {0}".format(stddev[3]))
	print("numStdDevAboveMean_DstPorts = {0}".format(numStdDevAboveMean_DstPorts))
	print("upperThreshold_DstPorts = {0}".format(upperThreshold_DstPorts))
	
	
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
	
	
	# Filter and get all inputs that exceed their respective thresholds.
	# # # flowsAboveThreshold = sumOfFlows.filter(lambda e: e[1] > upperThreshold_Flows)  # Remove since giving false positives.
	bytesAboveThreshold = sumOfBytes.filter(lambda e: e[1] > upperThreshold_Bytes)
	dstIPsAboveThreshold = uniqDstIPs.filter(lambda e: e[1] > upperThreshold_DstIPs)
	dstPortsAboveThreshold = uniqDstPorts.filter(lambda e: e[1] > upperThreshold_DstPorts)
	
	# Join data together, joining by srcAddr.  Need to do fullOuterJoin since some srcAddr may not be in some RDDs.
	# Unlikely that a srcAddr exceeded threshold in all of the features.
	join1 = bytesAboveThreshold.fullOuterJoin(dstIPsAboveThreshold)
	join2 = join1.fullOuterJoin(dstPortsAboveThreshold)
	
	# Map into format: (SrcAddr, sumOfBytes, uniqDstIPs, uniqDstPorts).
	joined = join2.map(lambda e: ({"srcAddr": e[0], "sumOfBytes": e[1][0][0], "uniqDstIPs": e[1][0][1], "uniqDstPorts": e[1][1]}))
	joined.pprint(12)  # Show for all 12 IPs.
	
	
	# Send srcAddr to ONOS to block.
	joined.foreachRDD(lambda rdd: rdd.foreachPartition(sendToONOS))
	
		
	
	# Start the execution of streams.
	ssc.start()
	
	# Wait for execution to stop.
	ssc.awaitTermination()
