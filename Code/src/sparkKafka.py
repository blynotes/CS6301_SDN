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
import sys

# Sample code at https://www.rittmanmead.com/blog/2017/01/getting-started-with-spark-streaming-with-python-and-kafka/
# https://cambridgespark.com/content/tutorials/interactively-analyse-100GB-of-JSON-data-with-Spark/index.html
# http://nbviewer.jupyter.org/github/jkthompson/pyspark-pictures/blob/master/pyspark-pictures.ipynb
# http://nbviewer.jupyter.org/github/jkthompson/pyspark-pictures/blob/master/pyspark-pictures-dataframes.ipynb


# https://spark.apache.org/docs/2.3.0/api/python/pyspark.html
from pyspark import SparkConf, SparkContext
# https://spark.apache.org/docs/2.3.0/api/python/pyspark.streaming.html
from pyspark.streaming import StreamingContext
# https://spark.apache.org/docs/2.3.0/api/python/pyspark.streaming.html#module-pyspark.streaming.kafka
from pyspark.streaming.kafka import KafkaUtils
import json
from elasticsearch import Elasticsearch
import datetime

def send_ES(item):
	es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
	for part in item:
		try:
			es.index(index='netflowrepo', doc_type='entry', body={
				# 'srcAddr': part["srcAddr"],  # Skip this when inputting data.
				'sumOfFlows': part["sumOfFlows"],
				'sumOfBytes': part["sumOfBytes"],
				'uniqDstIPs': part["uniqDstIPs"],
				'uniqDstPorts': part["uniqDstPorts"]
				# # # "timestamp" : datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			})
		except:
			print ("ERROR: " , sys.exc_info()[0])


if __name__ == "__main__":
	# SparkContext represents connection to a Spark cluster.
	conf = SparkConf()
	conf.setAppName("Kafka Spark App")
	conf.setMaster('local[2]')
	sc = SparkContext(conf=conf)
	sc.setLogLevel("WARN")
	
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
	
	# Join data together.
	join1 = sumOfFlows.join(sumOfBytes)
	join2 = join1.join(uniqDstIPs)
	join3 = join2.join(uniqDstPorts)
	
	# Map into format: (SrcAddr, sumOfFlows, sumOfBytes, uniqDstIPs, uniqDstPorts).
	joined = join3.map(lambda e: ({"srcAddr": e[0], "sumOfFlows": e[1][0][0][0], "sumOfBytes": e[1][0][0][1], "uniqDstIPs": e[1][0][1], "uniqDstPorts": e[1][1]}))
	joined.pprint(12)  # Show for all 12 IPs.
	
	# Send to ElasticSearch.
	joined.foreachRDD(lambda rdd: rdd.foreachPartition(send_ES))
	
	# Start the execution of streams.
	ssc.start()
	
	# Wait for execution to stop.
	ssc.awaitTermination()
