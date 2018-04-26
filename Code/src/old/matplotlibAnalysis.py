import csv
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np
import os

def analyzeData():
	sumOfBytes_list = []
	uniqDstIPs_list = []
	uniqDstPorts_list = []
	
	with open("C:\\Users\\Stephen\\OneDrive\\School\\CS6301-SDN\\Project\\ESdata.csv") as f:
		reader = csv.reader(f)
		for row in reader:
			bytes, ips, ports = row
			
			sumOfBytes_list.append(bytes)
			uniqDstIPs_list.append(ips)
			uniqDstPorts_list.append(ports)
		
	
	sumOfBytes_list = list(map(float, sumOfBytes_list))
	uniqDstIPs_list = list(map(float, uniqDstIPs_list))
	uniqDstPorts_list = list(map(float, uniqDstPorts_list))
	
	
	fig = plt.figure()
	ax = fig.add_subplot(111, projection='3d')
	ax.scatter(sumOfBytes_list, uniqDstIPs_list, uniqDstPorts_list, c='r', marker='o')
	
	ax.set_xlabel('Sum Of Bytes')
	ax.set_ylabel('Uniq Dst IPs')
	ax.set_zlabel('Uniq Dst Ports')
	
	plt.show()

if __name__ == "__main__":
	analyzeData()
	