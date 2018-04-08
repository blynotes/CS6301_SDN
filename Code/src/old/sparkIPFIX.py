# https://spark.apache.org/docs/2.2.0/api/python/pyspark.html
from pyspark import SparkContext
# https://spark.apache.org/docs/2.2.0/api/python/pyspark.streaming.html
from pyspark.streaming import StreamingContext
# https://spark.apache.org/docs/2.2.0/api/python/pyspark.streaming.html#module-pyspark.streaming.flume
from pyspark.streaming.flume import FlumeUtils


if __name__ == "__main__":
	# SparkContext represents connection to a Spark cluster.
	sc = SparkContext(appName="IPFIX Spark App")
	
	# StreamingContext represents connection to a Spark cluster from existing SparkContext.
	ssc = StreamingContext(sc, 1)
	
	# Creates an input stream that pulls events from Flume.
	flumeStream = FlumeUtils.createStream(ssc, "localhost", 9876)
	lines = flumeStream.map(lambda x: x[1])
	
	print(flumeStream)
	print(lines)
	lines.pprint()
	
	
	# Start the execution of streams.
	ssc.start()
	
	# Wait for execution to stop.
	ssc.awaitTermination()
