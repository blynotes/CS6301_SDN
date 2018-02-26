#!/usr/bin/python

"""
Based on Mininet examples/hwintf.py
"""

import re
import sys

from mininet.cli import CLI
from mininet.log import setLogLevel, info, error
from mininet.net import Mininet
from mininet.link import Intf
from mininet.topo import Topo
from mininet.util import quietRun

from mininet.node import RemoteController, OVSSwitch


CONTROLLER_IP = "10.28.34.39"
CONTROLLER_PORT = 6633

INTERFACE_FOR_VM1_PFSENSE = 'enp0s8'
INTERFACE_FOR_VM2_LINUX = 'enp0s9'


class TriangleTopo( Topo ):
	"Switches in triangle configuration with 3 hosts off each"
	
	def build(self):
		"Method to override in custom topology class."
		
		# Add switches
		s1 = self.addSwitch( 's1' )
		s2 = self.addSwitch( 's2' )
		s3 = self.addSwitch( 's3' )
		
		# Add hosts
		h11 = self.addHost('h11')
		h12 = self.addHost('h12')
		h13 = self.addHost('h13')
		
		h21 = self.addHost('h21')
		h22 = self.addHost('h22')
		h23 = self.addHost('h23')
		
		h31 = self.addHost('h31')
		h32 = self.addHost('h32')
		h33 = self.addHost('h33')
		
		# Add Links
		self.addLink(s1, s2)
		self.addLink(s1, s3)
		self.addLink(s2, s3)
		
		self.addLink(s1, h11)
		self.addLink(s1, h12)
		self.addLink(s1, h13)
		
		self.addLink(s2, h21)
		self.addLink(s2, h22)
		self.addLink(s2, h23)
		
		self.addLink(s3, h31)
		self.addLink(s3, h32)
		self.addLink(s3, h33)


def checkIntf( intf ):
	"Make sure intf exists and is not configured."
	config = quietRun( 'ifconfig %s 2>/dev/null' % intf, shell=True )
	if not config:
		error( 'Error:', intf, 'does not exist!\n' )
		exit( 1 )
	ips = re.findall( r'\d+\.\d+\.\d+\.\d+', config )
	if ips:
		error( 'Error:', intf, 'has an IP address,'
			   'and is probably in use!\n' )
		exit( 1 )


def runNetworkTopology():
	"Create a Mininet network using the TriangleTopo"
	
	# Confirm interfaces are good
	info( '*** Checking', INTERFACE_FOR_VM1_PFSENSE, '\n' )
	checkIntf( INTERFACE_FOR_VM1_PFSENSE )
	info( '*** Checking', INTERFACE_FOR_VM2_LINUX, '\n' )
	checkIntf( INTERFACE_FOR_VM2_LINUX )
	
	info( '*** Creating network\n' )
	# Create network using remote controller
	net = Mininet(
		topo=TriangleTopo(),
		controller=lambda name: RemoteController(name, ip=CONTROLLER_IP, port=CONTROLLER_PORT),
		switch=OVSSwitch,
		autoSetMacs = True)
	
	# Connect VM1 to switch1
	switch1 = net.switches[ 1 ]  # switch 1
	info( '*** Adding hardware interface', INTERFACE_FOR_VM1_PFSENSE, 'to switch',
		  switch1.name, '\n' )
	_intf1 = Intf( INTERFACE_FOR_VM1_PFSENSE, node=switch1 )
	
	# Connect VM2 to switch2
	switch2 = net.switches[ 2 ]  # switch 2
	info( '*** Adding hardware interface', INTERFACE_FOR_VM2_LINUX, 'to switch',
		  switch2.name, '\n' )
	_intf2 = Intf( INTERFACE_FOR_VM2_LINUX, node=switch2 )
	
	# This only needs done if you select IP addresses for VM1 and VM2 that
	# are the same IP as a node in your Mininet network.
	info( '*** Note: you may need to reconfigure the interfaces for '
		  'the Mininet hosts:\n', net.hosts, '\n' )
	
	# Start the network
	net.start()
	
	# Let user enter CLI to run commands
	CLI(net)
	
	# After user exits CLI, shutdown network.
	net.stop()
	
	
	
if __name__ == '__main__':
	setLogLevel( 'info' )
	runNetworkTopology()

# Allows the file to be imported using 'mn --custom <filename> --topo triangle'
topos = {
	'triangle' : TriangleTopo
}