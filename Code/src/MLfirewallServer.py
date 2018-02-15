# Based on sample in Python documentation at:
# https://docs.python.org/3/library/socketserver.html#asynchronous-mixins
import socket
import threading
import socketserver


DEBUGMODE = True

# SERVER PARAMETERS
SERVER_IP = "0.0.0.0"
SERVER_PORT = 5678
NUM_CONNECTIONS = 10

def printDebugMsg(msg):
	if DEBUGMODE:
		print(msg)

# ThreadingMixIn supports asynchronous behavior
class MLfirewallServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
	pass

# Create the class for handling each client request
class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
	def handle(self):
		data = str(self.request.recv(2048), 'ascii').rstrip("\r\n")
		cur_thread = threading.current_thread()
		printDebugMsg("{}: Data is {}".format(cur_thread, data))
		
		# Process data and make decision
		import random
		if random.randint(0, 1):
			printDebugMsg("{}: ALLOW packets".format(cur_thread))
			self.request.send(bytes("ALLOW", 'ascii'))
		else:
			printDebugMsg("{}: BLOCK packets".format(cur_thread))
			self.request.send(bytes("BLOCK", 'ascii'))
	


if __name__ == "__main__":
	server = MLfirewallServer((SERVER_IP, SERVER_PORT), ThreadedTCPRequestHandler)
	
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
