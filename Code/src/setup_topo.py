#!/usr/bin/python

"""
Inspiration from following sources:
	https://github.com/mininet/mininet/blob/master/mininet/link.py
	https://github.com/mininet/mininet/blob/master/examples/hwintf.py
	https://github.com/mininet/mininet/blob/master/examples/bind.py
	https://inside-openflow.com/2016/06/29/custom-mininet-topologies-and-introducing-atom/
"""
from __future__ import print_function
from mininet.net import Mininet
from mininet.node import RemoteController, Host
from mininet.log import setLogLevel, info, error
from mininet.cli import CLI
from mininet.link import Intf
from mininet.topo import Topo
from mininet.util import quietRun

from functools import partial

import os
import re
import sys
import subprocess


DEBUGMODE = True

def printDebugMsg(msg):
	if DEBUGMODE:
		print(msg)


CONTROLLER_IP = "10.28.34.39"
CONTROLLER_PORT = 6633

NETFLOW_TARGET = "10.28.34.17:9995"


class MyTopo(Topo):
	"""My Custom Topo."""
	
	def build(self):
		# Add Hosts.
		h11 = self.addHost('h11')
		h12 = self.addHost('h12')
		h13 = self.addHost('h13')
		
		h21 = self.addHost('h21')
		h22 = self.addHost('h22')
		h23 = self.addHost('h23')
		
		h31 = self.addHost('h31')
		h32 = self.addHost('h32')
		h33 = self.addHost('h33')
		
		hWeb = self.addHost('h41')  # Web/FTP Server that hosts talk to.
		hDB = self.addHost('h42')  # DB Server - only talked to by hWeb.
		hApp = self.addHost('h51')  # App Server that hosts talk to.
		
		# Add Switches
		s1 = self.addSwitch('s1')
		s2 = self.addSwitch('s2')
		s3 = self.addSwitch('s3')
		
		sWebDB = self.addSwitch('s4')
		sApp = self.addSwitch('s5')
		
		sAgg1 = self.addSwitch('s61')
		sAgg2 = self.addSwitch('s62')
		
		sCore1 = self.addSwitch('s71')
		sCore2 = self.addSwitch('s72')
		sCore3 = self.addSwitch('s73')
		
		# Add Links
		self.addLink(s1, h11)
		self.addLink(s1, h12)
		self.addLink(s1, h13)
		
		self.addLink(s2, h21)
		self.addLink(s2, h22)
		self.addLink(s2, h23)
		
		self.addLink(s3, h31)
		self.addLink(s3, h32)
		self.addLink(s3, h33)
		
		self.addLink(s1, sAgg1)
		self.addLink(s1, sAgg2)
		self.addLink(s2, sAgg1)
		self.addLink(s2, sAgg2)
		self.addLink(s3, sAgg1)
		self.addLink(s3, sAgg2)
		
		self.addLink(sAgg1, sAgg2)
		
		self.addLink(sAgg1, sCore1)
		self.addLink(sAgg1, sCore2)
		self.addLink(sAgg2, sCore1)
		self.addLink(sAgg2, sCore3)
		
		self.addLink(sCore1, sCore2)
		self.addLink(sCore1, sCore3)
		self.addLink(sCore2, sCore3)
		
		self.addLink(sCore2, sWebDB)
		self.addLink(sCore2, sApp)
		self.addLink(sCore3, sWebDB)
		self.addLink(sCore3, sApp)
		
		self.addLink(sWebDB, hWeb)
		self.addLink(sWebDB, hDB)
		self.addLink(sApp, hApp)
		

def runMyTopo():
	"""Create a Mininet network using MyTopo."""
	info( '*** Creating network\n' )
	# Create network using myTopo, hostWithDir, and remote controller.
	net = Mininet(
		topo=MyTopo(),
		controller=lambda name: RemoteController(name, ip=CONTROLLER_IP, port=CONTROLLER_PORT)
	)
	
	# Start the network.
	net.start()
	
	# Setup Netflow on the switches.
	for s in net.switches:
		# For simplicity only configure netflow on access switches (directly connected to host or server).
		if str(s) not in ['s1', 's2', 's3', 's4', 's5']:
			# skip if not one of the above switches.
			printDebugMsg("switch {0} is being skipped".format(s))
			continue
		
		netflowCommand = "ovs-vsctl -- set bridge {0} netflow=@nf -- --id=@nf create Netflow targets=\"{1}\" active-timeout=60".format(s, NETFLOW_TARGET)
		
		printDebugMsg("NETFLOW command is: {0}".format(netflowCommand))
		printDebugMsg("netflowCommand.split() command is: {0}".format(netflowCommand.split()))
		
		# ovs-vsctl returns 0 for success. Non-zero means an error occurred.
		FNULL = open(os.devnull, 'w')
		if subprocess.call(netflowCommand.split(), stdout=FNULL, stderr=subprocess.STDOUT):
			error( "*** Error configuring netflow on switch {0}".format(s))
			error( "*** Exiting...")
			net.stop()
			sys.exit()
	
	
	# Run the programs that need run on various hosts.
	for h in net.hosts:
		printDebugMsg("Starting Client and Server on {0}. IP is {1}".format(h, h.IP()))
		h.cmd("python3 Client.py --ip={0} &".format(h.IP()))
		h.cmd("python3 Server.py --ip={0} &".format(h.IP()))
	
	# Let user enter CLI to run commands.
	CLI(net)
	
	# After user exits CLI, shutdown network.
	net.stop()


if __name__ == '__main__':
	setLogLevel('info')
	runMyTopo()

topos = {
	'mytopo': MyTopo
}
