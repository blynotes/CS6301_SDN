from __future__ import print_function

from elasticsearch import Elasticsearch
import elasticsearch.helpers

import csv

def saveDataFromES():
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
	
	with open("ESdata.csv", 'wb') as f:
		writer = csv.writer(f, lineterminator="\n")
		for index, sumOfBytes in enumerate(sumOfBytes_list):
			writer.writerow([sumOfBytes, uniqDstIPs_list[index], uniqDstPorts_list[index]])
		
	

if __name__ == "__main__":
	saveDataFromES()
	