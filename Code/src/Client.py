#!/usr/bin/python3
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



# Based on sample in Python documentation at:
# https://docs.python.org/3/library/socketserver.html#socketserver.TCPServer
import socket
import argparse
import random
import requests
import string
import time
import subprocess
import os

# Set with command-line arguments.
DEBUGMODE = False

def printDebugMsg(msg):
	if DEBUGMODE:
		print(msg)
	
def generate_String(length):
	"""Generate random string of length."""
	return ''.join([random.choice(string.ascii_letters + string.digits) for n in range(length)])


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Client Application')
	parser.add_argument('--ip', required=True, help="Mininet host IP (it's own IP)")
	parser.add_argument('--debug', action='store_true')
	
	args = parser.parse_args()
	
	if args.debug:
		DEBUGMODE = True
	
	myIP = args.ip
	
	HOST_IPs = ['10.0.0.1', '10.0.0.2', '10.0.0.3',
				'10.0.0.4', '10.0.0.5', '10.0.0.6',
				'10.0.0.7', '10.0.0.8', '10.0.0.9']
	BASE_HOST_PORT = 5000
	
	WEB_SERVER_IP = '10.0.0.10'
	WEB_PORT = 8000
	
	DB_SERVER_IP = '10.0.0.11'
	DB_PORT = 2000
	
	APP_SERVER_IP = '10.0.0.12'
	APP_PORT = 3000
	
	
	
	# Parameters:
	INITIAL_SLEEP_INTERVAL = 10
	MAX_SLEEP_INTERVAL = 10
	
	
	# Wait INITIAL_SLEEP_INTERVAL seconds to let all servers have a chance to start.
	time.sleep(INITIAL_SLEEP_INTERVAL)
	
	
	if myIP in HOST_IPs:
		# Hosts talk to:
		# 	- Other hosts on HOST_PORT
		#	- Web Server on WEB_PORT
		#	- App Server on APP_PORT
		# Hosts do NOT talk to DB Server.
		printDebugMsg("{0} is a Host.".format(myIP))
		
		# Start with same list as HOST_IPs and remove myIP
		ALL_IPs = ['10.0.0.1', '10.0.0.2', '10.0.0.3',
						'10.0.0.4', '10.0.0.5', '10.0.0.6',
						'10.0.0.7', '10.0.0.8', '10.0.0.9',
						'10.0.0.10', '10.0.0.11', '10.0.0.12']
						
		# Remove myIP from list to make it easier below.
		ALL_IPs.remove(myIP)
		
		
		# Start with same list as HOST_IPs and remove myIP
		OTHER_HOST_IPs = ['10.0.0.1', '10.0.0.2', '10.0.0.3',
						'10.0.0.4', '10.0.0.5', '10.0.0.6',
						'10.0.0.7', '10.0.0.8', '10.0.0.9']
		
		# Remove myIP from list to make it easier below.
		OTHER_HOST_IPs.remove(myIP)
		
		printDebugMsg("OTHER_HOST_IPs with {0} removed is {1}".format(myIP, OTHER_HOST_IPs))
		
		while True:
			# At random time interval (between 1 and MAX_SLEEP_INTERVAL seconds), 
			# choose Host Task at random:
			# 	1) Send data (between 1 and 1024 bytes) to other host.
			#		- Choose host to send to at random (other than self).
			#	2) Ping another IP.
			#		- Choose host to send to at random (other than self).
			#	3) Send HTTP request to Web Server.
			#	4) Send data (between 1 and 1024 bytes) to App Server.
			
			# Sleep for random interval.
			time.sleep(random.randint(1, MAX_SLEEP_INTERVAL))
			printDebugMsg("Awake and time to select a task.")
			
			# Choose a task.
			taskChoice = random.randint(1, 4)
			
			if taskChoice == 1:
				# Send to other Host.
				destIP = OTHER_HOST_IPs[random.randint(0, len(OTHER_HOST_IPs) - 1)]
				
				# Each device has it's own unique server port.
				SERVER_PORT = BASE_HOST_PORT + int(destIP[-1:])
				
				printDebugMsg("Task selected: Send to Other Host {0} on port {1}".format(destIP, SERVER_PORT))
				
				try:
					s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					s.connect((destIP, SERVER_PORT))
					
					printDebugMsg("Connected to Other Host {0}".format(destIP))
					
					# Generate random string and send to destination.
					textToSend = generate_String(random.randint(1, 1024))
					s.sendall(bytes(textToSend + "\n", 'ascii'))
					
					printDebugMsg("Data Sent to Other Host {0}".format(destIP))
					
					# Receive response back.
					dataRcvd = s.recv(2048)
					
					printDebugMsg("Data Received from Other Host {0}".format(destIP))
					
					# Close connection.
					s.close()
				except Exception as e:
					printDebugMsg("ERROR: Failed connection to {0} with error {1}".format(destIP, e))
			
			elif taskChoice == 2:
				# Ping another IP.
				
				# Send to other Host.
				destIP = ALL_IPs[random.randint(0, len(ALL_IPs) - 1)]
				
				printDebugMsg("Task selected: Ping {0}".format(destIP))
				
				try:
					# Ping for 3 counts, and hide output.
					FNULL = open(os.devnull, 'w')
					retcode = subprocess.call(['ping', '-c', '3', destIP], stdout=FNULL, stderr=FNULL)
				except Exception as e:
					printDebugMsg("ERROR: Failed ping to {0} with error {1}".format(destIP, e))
				
				printDebugMsg("Ping complete with return code {0}".format(retcode))
				
			elif taskChoice == 3:
				# Send to web server.
				printDebugMsg("Task selected: Send to Web Server")
				
				try:
					r = requests.get("http://{0}:{1}/index.html".format(WEB_SERVER_IP, WEB_PORT))
					printDebugMsg("Status Code from web server is {0}".format(r.status_code))
				except Exception as e:
					printDebugMsg("Unable to connect to web server.  Error is {0}.".format(e))
					
				
			elif taskChoice == 4:
				# Send to App Server.
				printDebugMsg("Task selected: Send to App Server")
				
				try:
					s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					s.connect((APP_SERVER_IP, APP_PORT))
					
					printDebugMsg("Connected to App Server")
					
					# Generate random string and send to destination.
					textToSend = generate_String(random.randint(1, 1024))
					s.sendall(bytes(textToSend + "\n", 'ascii'))
					
					printDebugMsg("Data Sent to App Server")
					
					# Receive response back.
					dataRcvd = s.recv(2048)
					
					printDebugMsg("Data Received from App Server")
					
					# Close connection.
					s.close()
				except Exception as e:
					printDebugMsg("Unable to connect to app server.  Error is {0}.".format(e))
					
			
	elif myIP == WEB_SERVER_IP:
		# Web Server is client to DB Server.
		printDebugMsg("{0} is Web Server.".format(myIP))
		
		while True:
			# At random interval, send data (between 1 and 1024 bytes) to DB Server.
			
			# Sleep for random interval.
			time.sleep(random.randint(1, MAX_SLEEP_INTERVAL))
			printDebugMsg("Awake and time to send to DB Server")
			
			try:
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect((DB_SERVER_IP, DB_PORT))
					
				printDebugMsg("Connected to DB Server")
				
				# Generate random string and send to destination.
				textToSend = generate_String(random.randint(1, 1024))
				s.sendall(bytes(textToSend + "\n", 'ascii'))
					
				printDebugMsg("Data Sent to DB Server")
				
				# Receive response back.
				dataRcvd = s.recv(2048)
					
				printDebugMsg("Data Received from DB Server")
				
				# Close connection.
				s.close()
			except Exception as e:
				printDebugMsg("Unable to connect to app server.  Error is {0}.".format(e))
		
	elif myIP == DB_SERVER_IP:
		# DB Server is client to no one.  Ignore.
		printDebugMsg("{0} is DB Server.".format(myIP))
		
		# Do nothing.
		pass
		
	elif myIP == APP_SERVER_IP:
		# App Server is client to no one.  Ignore.
		printDebugMsg("{0} is App Server.".format(myIP))
		
		# Do nothing.
		pass
		
	else:
		# Unknown IP address.  Ignore.
		printDebugMsg("Unknown IP address: {0}".format(myIP))
		
		# Ignore.
		pass
