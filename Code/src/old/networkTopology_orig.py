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
from mininet.topolib import TreeTopo
from mininet.util import quietRun

from mininet.node import RemoteController

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
	net = Mininet( controller=None, topo=TreeTopo( depth=2, fanout=2 ) )
	net.addController("c0", controller=RemoteController, ip="10.28.34.39", port=6633)

	switch = net.switches[ 0 ]
	info( '*** Adding hardware interface', intfName, 'to switch',
		  switch.name, '\n' )
	_intf = Intf( intfName, node=switch )

	info( '*** Note: you may need to reconfigure the interfaces for '
		  'the Mininet hosts:\n', net.hosts, '\n' )

	net.start()
	CLI( net )
	net.stop(