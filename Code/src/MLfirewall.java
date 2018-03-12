/*
 * Copyright 2018 Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.MLfirewall.app;

import com.google.common.collect.HashMultimap;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onlab.packet.Ethernet;
import org.onlab.packet.IPv4;
import org.onlab.packet.IpAddress;
import org.onlab.packet.TCP;
import org.onlab.packet.UDP;
import org.onlab.packet.MacAddress;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleEvent;
import org.onosproject.net.flow.FlowRuleListener;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.EthCriterion;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import java.io.*;
import java.net.*;

import static org.onosproject.net.flow.FlowRuleEvent.Type.RULE_REMOVED;
import static org.onosproject.net.flow.criteria.Criterion.Type.ETH_SRC;

/**
 * Application that calls a Machine Learning server to
 * determine if traffic should be ALLOWed or BLOCKed.
 */
@Component(immediate = true)
public class MLfirewall {
	private static String SERVER_IP = "localhost";
	private static int SERVER_PORT = 5678;

    private static Logger log = LoggerFactory.getLogger(MLfirewall.class);

    private static final String MSG_BLOCKING =
		    "Blocking Traffic";
    private static final String MSG_ALLOWING =
		    "Allowing Traffic";
    private static final String MSG_FLOW_REMOVED =
		    "Flow Removed";

    private static final int PRIORITY = 128;
    private static final int DROPALLOW_PRIORITY = 129;
    // // // private static final int TIMEOUT_SEC = 60; // seconds

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowObjectiveService flowObjectiveService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PacketService packetService;

    private ApplicationId appId;
    private final PacketProcessor packetProcessor = new MyPacketProcessor();
    private final FlowRuleListener flowListener = new InternalFlowListener();

    // Selector for traffic that is to be intercepted
    private final TrafficSelector intercept = DefaultTrafficSelector.builder()
		    .matchEthType(Ethernet.TYPE_IPV4).build();

    // Means to track detected pings from each device on a temporary basis
    // // // private final HashMultimap<DeviceId, PingRecord> pings = HashMultimap.create();

    @Activate
    public void activate() {
		appId = coreService.registerApplication("org.MLfirewall.app",
												() -> log.info("MLfirewall registered"));
		packetService.addProcessor(packetProcessor, PRIORITY);
		flowRuleService.addListener(flowListener);
		packetService.requestPackets(intercept, PacketPriority.CONTROL, appId,
								     Optional.empty());
		log.info("Started");
    }

    @Deactivate
    public void deactivate() {
		packetService.removeProcessor(packetProcessor);
		flowRuleService.removeFlowRulesById(appId);
		flowRuleService.removeListener(flowListener);
		log.info("Stopped");
    }

    // Processes the specified packet.
    private void processPacket(PacketContext context) {
		InboundPacket pkt = context.inPacket();
		
		// Get where the packet was received from.
		ConnectPoint connectPoint = pkt.receivedFrom();
		DeviceId deviceId = connectPoint.deviceId();
		
		// Get L2 Info.
		Ethernet eth = pkt.parsed();
		MacAddress srcMAC = eth.getSourceMAC();
		MacAddress dstMAC = eth.getDestinationMAC();
		
		
		// Get L3 Info.
		// initialize L3 variables.
		IPv4 ipv4;
		IpAddress srcIPv4;
		IpAddress dstIPv4;
		if (eth.getEtherType() == Ethernet.TYPE_IPV4) {
			//https://github.com/opennetworkinglab/onos/blob/master/apps/bgprouter/src/main/java/org/onosproject/bgprouter/IcmpHandler.java
			ipv4 = (IPv4) eth.getPayload();
			srcIPv4 = IpAddress.valueOf(ipv4.getSourceAddress());
			dstIPv4 = IpAddress.valueOf(ipv4.getDestinationAddress());
		} else {
			// only handle IPv4 packets. Let all other message types pass.
			log.warn(MSG_ALLOWING, srcMAC, dstMAC, deviceId);
			allowPackets(deviceId, srcMAC, dstMAC);
			return;
		}
		
		
		
		// Get L4 Info.
		int srcPort;
		int dstPort;
		
		if (ipv4.getProtocol() == IPv4.PROTOCOL_TCP) {
			// https://github.com/opennetworkinglab/onos/blob/master/apps/bgprouter/src/main/java/org/onosproject/bgprouter/TunnellingConnectivityManager.java
			TCP L4pkt = (TCP) ipv4.getPayload();
			srcPort = L4pkt.getSourcePort();
			dstPort = L4pkt.getDestinationPort();
		} else if (ipv4.getProtocol() == IPv4.PROTOCOL_UDP) {
			UDP L4pkt = (UDP) ipv4.getPayload();
			srcPort = L4pkt.getSourcePort();
			dstPort = L4pkt.getDestinationPort();
		} else {
			// Not TCP or UDP.
			// Let it use default port value of 0.
			srcPort = 0;
			dstPort = 0;
		}
		
		String sendToServer = "srcMAC=" + srcMAC + ";dstMAC=" + dstMAC + ";srcIPv4=" + srcIPv4 + ";dstIPv4=" + dstIPv4 + ";srcPort=" + srcPort + ";dstPort=" + dstPort;
		
		// If cannot reach the server, then allow the packets.
		boolean SERVER_DECISION_ALLOW = true;
		try {
			Socket socket = new Socket(SERVER_IP, SERVER_PORT);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			
			out.println(sendToServer);
			String responseBack = in.readLine();
			
			if(responseBack.equals("ALLOW")) {
				SERVER_DECISION_ALLOW = true;
			} else {
				SERVER_DECISION_ALLOW = false;
			}
			
			log.warn("Server decided:", responseBack);
			
		} catch(Exception e) {
			log.warn("Server Connection Error:", e);
		}
		
		
		//TODO: use something other than MAC for this.
		if(SERVER_DECISION_ALLOW) {
			log.warn(MSG_ALLOWING, srcMAC, dstMAC, deviceId);
			allowPackets(deviceId, srcMAC, dstMAC);
//			context.send();
		} else {
			log.warn(MSG_BLOCKING, srcMAC, dstMAC, deviceId);
			banPackets(deviceId, srcMAC, dstMAC);
			context.block();
		}
		
    }

    // Installs a drop rule for the packets between given src/dst.
    private void banPackets(DeviceId deviceId, MacAddress src, MacAddress dst) {
		TrafficSelector selector = DefaultTrafficSelector.builder()
				.matchEthSrc(src).matchEthDst(dst).build();
		TrafficTreatment drop = DefaultTrafficTreatment.builder()
				.drop().build();

		flowObjectiveService.forward(deviceId, DefaultForwardingObjective.builder()
				.fromApp(appId)
				.withSelector(selector)
				.withTreatment(drop)
				.withFlag(ForwardingObjective.Flag.VERSATILE)
				.withPriority(DROPALLOW_PRIORITY)
				.add());
    }

    // Installs an allow rule for the packets between given src/dst.
    private void allowPackets(DeviceId deviceId, MacAddress src, MacAddress dst) {
		TrafficSelector selector = DefaultTrafficSelector.builder()
				.matchEthSrc(src).matchEthDst(dst).build();
		TrafficTreatment allow = DefaultTrafficTreatment.builder()
				.build();

		flowObjectiveService.forward(deviceId, DefaultForwardingObjective.builder()
				.fromApp(appId)
				.withSelector(selector)
				.withTreatment(allow)
				.withFlag(ForwardingObjective.Flag.VERSATILE)
				.withPriority(DROPALLOW_PRIORITY)
				.add());
    }

    // Intercepts packets
    private class MyPacketProcessor implements PacketProcessor {
		@Override
		public void process(PacketContext context) {
			processPacket(context);
		}
    }

    // // // // Record of a ping between two end-station MAC addresses
    // // // private class PingRecord {
		// // // private final MacAddress src;
		// // // private final MacAddress dst;

		// // // PingRecord(MacAddress src, MacAddress dst) {
		    // // // this.src = src;
		    // // // this.dst = dst;
		// // // }

		// // // @Override
		// // // public int hashCode() {
		    // // // return Objects.hash(src, dst);
		// // // }

		// // // @Override
		// // // public boolean equals(Object obj) {
		    // // // if (this == obj) {
				// // // return true;
		    // // // }
		    // // // if (obj == null || getClass() != obj.getClass()) {
				// // // return false;
		    // // // }
		    // // // final PingRecord other = (PingRecord) obj;
		    // // // return Objects.equals(this.src, other.src) && Objects.equals(this.dst, other.dst);
		// // // }
    // // // }

    // // // // Prunes the given ping record from the specified device.
    // // // private class PingPruner extends TimerTask {
		// // // private final DeviceId deviceId;
		// // // private final PingRecord ping;

		// // // public PingPruner(DeviceId deviceId, PingRecord ping) {
		    // // // this.deviceId = deviceId;
		    // // // this.ping = ping;
		// // // }

		// // // @Override
		// // // public void run() {
		    // // // pings.remove(deviceId, ping);
		// // // }
    // // // }

    // Listens for our removed flows.
    private class InternalFlowListener implements FlowRuleListener {
		@Override
		public void event(FlowRuleEvent event) {
		    FlowRule flowRule = event.subject();
		    if (event.type() == RULE_REMOVED && flowRule.appId() == appId.id()) {
				Criterion criterion = flowRule.selector().getCriterion(ETH_SRC);
				MacAddress src = ((EthCriterion) criterion).mac();
				MacAddress dst = ((EthCriterion) criterion).mac();
				log.warn(MSG_FLOW_REMOVED, src, dst, flowRule.deviceId());
		    }
		}
    }
}