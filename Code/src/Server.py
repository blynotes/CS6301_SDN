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
# https://docs.python.org/3/library/socketserver.html#asynchronous-mixins
import socket
import threading
import socketserver
import argparse
import sys
import http.server
import os

# Set with command-line arguments.
DEBUGMODE = False

def printDebugMsg(msg):
	if DEBUGMODE:
		print(msg)
		
def generate_FakeText():
	"""Generate Lorem Ipsum fake text."""
	return "<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis sagittis imperdiet finibus. Nunc odio risus, condimentum venenatis elit sed, dictum accumsan leo. Pellentesque ut semper dolor. Morbi laoreet bibendum congue. Suspendisse sit amet consectetur dolor. Aenean laoreet tempor tristique. Nam in odio magna. Proin facilisis commodo magna, nec luctus erat.</p><p>Mauris tincidunt consectetur arcu sed ornare. Curabitur congue, ante eget aliquam semper, sapien lorem interdum metus, id lobortis nisi ante eu velit. Suspendisse id risus lectus. Sed vulputate congue sapien, at malesuada mi fringilla id. Donec sit amet tortor consequat, maximus ex vel, volutpat lectus. Donec sit amet lectus efficitur, luctus turpis ac, pellentesque mi. Donec tincidunt mi ultrices, sodales eros et, pharetra orci. Vivamus pellentesque mattis orci eu vulputate. In hac habitasse platea dictumst. Quisque cursus lacus sit amet dolor egestas, sed gravida augue porta. Praesent nec tristique purus. Suspendisse a finibus justo, eu hendrerit lectus. Sed vulputate diam vel purus feugiat, a finibus massa consequat. Nullam condimentum tempor ex, a faucibus risus scelerisque a. Phasellus viverra finibus lectus id lacinia. Donec sed elit a libero iaculis tristique.</p><p>Suspendisse aliquam nisl quis fermentum suscipit. Pellentesque eleifend diam nec risus condimentum, placerat dapibus arcu finibus. Aliquam ante erat, consequat eget augue id, accumsan eleifend orci. Pellentesque et neque et justo pretium suscipit. Nam id venenatis eros, vitae rutrum eros. Nam laoreet, orci feugiat faucibus porttitor, eros orci dapibus massa, eget molestie lectus ante sed mi. Vestibulum imperdiet accumsan hendrerit.</p><p>Integer scelerisque ex nec sapien pharetra sodales. Sed posuere condimentum sagittis. Sed tristique aliquet arcu, non tempor nisl hendrerit non. Duis gravida ipsum et lacus pellentesque tincidunt. Nam ac dui non turpis rhoncus lobortis id eget mi. Duis interdum quis nunc in vulputate. Nullam iaculis tortor dolor, eget sollicitudin erat fermentum vel. Sed pulvinar, nunc eget porttitor molestie, massa tortor porta tellus, eu venenatis ante felis non arcu. Pellentesque lobortis justo ut quam malesuada ultrices. Maecenas pulvinar dictum ipsum sit amet congue. Cras porta velit vel mi pharetra congue. Phasellus sed gravida urna. Pellentesque vitae tellus vitae felis faucibus posuere.</p><p>Nunc facilisis pharetra tortor vel vestibulum. Proin vel arcu at felis volutpat consectetur laoreet ut quam. Fusce ac malesuada ligula. Phasellus iaculis dolor eget tortor aliquam pretium. Duis feugiat mattis gravida. Maecenas lectus neque, sodales hendrerit ipsum at, porttitor bibendum ligula. Sed in turpis faucibus libero efficitur maximus. Donec a mauris feugiat quam feugiat varius quis et tortor. Aliquam erat volutpat. Cras quis velit ex. Pellentesque vehicula justo lacus, vel consequat magna egestas ut. Donec tincidunt dolor sapien, nec mollis felis sagittis eu. Duis facilisis ut sapien quis tristique. Curabitur nec augue a libero gravida rutrum in in ante. Nunc at maximus diam, ac consequat metus. Sed fringilla hendrerit eros nec luctus.</p>"


# ThreadingMixIn supports asynchronous behavior
class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
	pass

# Create the class for handling each client request
class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
	def handle(self):
		data = str(self.request.recv(2048), 'ascii').rstrip("\r\n")
		cur_thread = threading.current_thread()
		printDebugMsg("{}: Data is {}".format(cur_thread, data))
		
		self.request.send(bytes("RECEIVED: {0}".format(data), 'ascii'))
	


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Server Application')
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
	
	# Set server IP to listen on any IP.
	SERVER_IP = "0.0.0.0"
	
	if myIP in HOST_IPs:
		SERVER_PORT = BASE_HOST_PORT + int(myIP[-1:])
	elif myIP == WEB_SERVER_IP:
		SERVER_PORT = WEB_PORT
	elif myIP == DB_SERVER_IP:
		SERVER_PORT = DB_PORT
	elif myIP == APP_SERVER_IP:
		SERVER_PORT = APP_PORT
	else:
		# Unknown IP address.  Ignore.
		printDebugMsg("Unknown IP address: {0}".format(myIP))
		
		# Ignore.
		sys.exit(1)
	
	printDebugMsg("SERVER_PORT = {0}".format(SERVER_PORT))
	
	if myIP == WEB_SERVER_IP:
		# Web Server.
		
		htmlPage = "<html><head>Hello World</head><body>{0}</body></html>\n".format(generate_FakeText())
		
		# Create index.html if not already existing.
		if not os.path.exists('index.html'):
			printDebugMsg("index.html does not exist... creating.")
			with open('index.html', 'w') as f:
				f.write(htmlPage)
		
		# Start HTTP Server
		Handler = http.server.SimpleHTTPRequestHandler
		
		httpd = socketserver.TCPServer(("", SERVER_PORT), Handler)
		printDebugMsg("HTTP Server running on Port {0}".format(SERVER_PORT))
		httpd.serve_forever()
	else:
		# Non-Web Server.
		
		### Set-up Server.
		server = Server((SERVER_IP, SERVER_PORT), ThreadedTCPRequestHandler)
		
		# Start a thread for the server. When client connects, a new thread will open for each client.
		server_thread = threading.Thread(target=server.serve_forever)
		# Exit the server thread when the main thread terminates
		server_thread.daemon = True
		server_thread.start()
		printDebugMsg("Server running in thread: {}".format(server_thread.name))
		
		while True:
			pass
			
		# Shut down server when finished.  Never actually reaches here since infinite loop above.
		server.shutdown()
		printDebugMsg("Shutting down server.")
