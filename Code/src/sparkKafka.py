from __future__ import print_function
import sys

# Sample code at https://www.rittmanmead.com/blog/2017/01/getting-started-with-spark-streaming-with-python-and-kafka/

# https://spark.apache.org/docs/2.3.0/api/python/pyspark.html
from pyspark import SparkContext
# https://spark.apache.org/docs/2.3.0/api/python/pyspark.streaming.html
from pyspark.streaming import StreamingContext
# https://spark.apache.org/docs/2.3.0/api/python/pyspark.streaming.html#module-pyspark.streaming.kafka
from pyspark.streaming.kafka import KafkaUtils
import json


if __name__ == "__main__":
	# SparkContext represents connection to a Spark cluster.
	sc = SparkContext(appName="Kafka Spark App")
	sc.setLogLevel("WARN")
	
	# StreamingContext represents connection to a Spark cluster from existing SparkContext.
	ssc = StreamingContext(sc, 1)  # 1 indicates how many seconds each batch lasts.
	
	# Creates an input stream that pulls events from Flume.
	kvs = KafkaUtils.createStream(ssc, "streamsetApp:2181", "spark-streaming-consumer", {"NETFLOW":1})
	parsed = kvs.map(lambda x: json.loads(x[1]))
	parsed.pprint()
	
	# Start the execution of streams.
	ssc.start()
	
	# Wait for execution to stop.
	ssc.awaitTermination()
