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

from mininet.node import RemoteController


CONTROLLER_IP = "10.28.34.39"
CONTROLLER_PORT = 6633

INTERFACE_FOR_PARROT_OS = 'enp0s8'
INTERFACE_FOR_SNORT_IDS = 'enp0s9'


class TriangleTopo( Topo ):
	"Switches in triangle configuration with 3 hosts off each"
	
	def __init__(self):
		# Initialize topology
		Topo.__init__(self)
		
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

if __name__ == '__main__':
    setLogLevel( 'info' )

    # try to get hw intf from the command line; by default, use eth1
    intfName = sys.argv[ 1 ] if len( sys.argv ) > 1 else 'enp0s8'
    info( '*** Connecting to hw intf: %s' % intfName )

    info( '*** Checking', intfName, '\n' )
    checkIntf( intfName )

    info( '*** Creating network\n' )
    net = Mininet( controller=None, topo=TriangleTopo() )
	net.addController("c0", controller=RemoteController, ip=CONTROLLER_IP, port=CONTROLLER_PORT)

    switch = net.switches[ 0 ]
    info( '*** Adding hardware interface', intfName, 'to switch',
          switch.name, '\n' )
    _intf = Intf( intfName, node=switch )

    info( '*** Note: you may need to reconfigure the interfaces for '
          'the Mininet hosts:\n', net.hosts, '\n' )

    net.start()
    CLI( net )
    net.stop(