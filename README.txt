CS 6301-503 Software Defined Networking
Professor:
	Timothy Culver

Team Members:
	Stephen Blystone
	Taniya Riar
	Juhi Bhandari
	Ishwank Singh

Project Name:
	Machine Learning Approach for an Anomaly Intrusion Detection System using ONOS

Project Report:
	Machine Learning Approach for an Anomaly IDS using ONOS.docx

Project Presentation:
	Project Presentation.pptx

======================================================================
SETTING UP THE PROJECT

For the ONOS VM:
	Follow the instructions in the "ONOS 1.12 installation Guide.docx" Guide.

For the Mininet VM:
	Follow the instructions in the "Mininet VM Guide.docx" Guide.

For the Application VM:
	Follow the instructions in the "App Installation Guide.docx" Guide.

======================================================================
RUNNING THE PROJECT

Follow the instructions to run the project and to stop running the project in the "Run Applications Guide.docx" Guide.

======================================================================
FILES IN PROJECT DIRECTORY

•	Elasticsearchdata_csv.ipynb
		iPython Jupyter Notebook used to visualize the ElasticsearchNormaldata.csv and use elbow graph to determine K value.

•	ElasticsearchNormaldata.csv
		"Normal" traffic data stored in Elasticsearch.

•	Flow Diagram.pptx
		Diagrams used in presentations.

•	Important Netflow Fields.txt
		Sample NetFlow data captured with only important fields remaining.

•	Machine Learning Approach for an Anomaly IDS using ONOS.docx
		Project Report

•	Netflow Field Explanations.txt
		Explanation of NetFlow fields.

•	Project Presentation.pptx
		Project Presentation.

•	README.txt
		This README file.

•	sampleNetflowData.txt
		Sample raw NetFlow data.

•	SDN Project Proposal.docx
		Our Project Proposal.

•	SDNProjectDemoFinal.mp4
		Demo Video showing how to launch each application, starting the "Normal" traffic, viewing visualizations in Kibana, triggering "anomalies", and how to view flow rules in the ONOS web gui.

======================================================================
PROJECT GUIDES (located in the "Guides" directory)

•	App Installation Guide.docx
		How to install and configure everything in the Application VM.

•	Mininet VM Guide.docx
		Install required packages into the VM to run Mininet and trigger the anomalies.

•	NetFlow Guide.docx
		Guide for how to configure NetFlow on Open vSwitch.

•	ONOS 1.12 installation Guide.docx
		3 methods of installing and configuring ONOS:
		Option 1 installs an OVA file and provides a link to a Distributed ONOS tutorial.
		Option 2 installs ONOS as a service (I did not get this to work).
		Option 3 is the recommended option. There is also information for configuring IntelliJ if building an Internal ONOS application.

•	ONOS Rest API Guide.docx
		Contains information on how to view a nice webpage on localhost (after launching ONOS) to query the ONOS REST API.

•	Run Applications Guide.docx
		How to start and stop all applications in the Big Data pipeline and run the demo.

======================================================================
CODE DESCRIPTIONS (located in the "Code/src" directory)

NOTE: All code files use UNIX EOL characters (line-feed "\n"). Opening these files in most Windows programs will not maintain the formatting, since Windows expects carriage-return line-feed "\r\n". If you use Windows, opening the files using Notepad++ will maintain the correct formatting.

•	Client.py
		Generates random “Normal” traffic.

•	index_ES.txt
		Information placed into Kibana Dev Tool to create our Elasticsearch index.

•	scapyPortScan.py
		Use Python library Scapy to perform a UDP port scan from port 1 to port 65535 on the target device.

•	Server.py
		Receives messages from Client.py from other hosts and responds.

•	setup_topo.py
		Setup Mininet topology, configure Open vSwitches with NetFlow, call Client.py and Server.py for each Mininet host.

•	sparkKafka.py
		Perform feature engineering to get our features and send to Elasticsearch.

•	sparkMachineLearning.py
		Train K-Means algorithm on data in Elasticsearch, perform feature engineering on new data, standardize new data and check if anomaly. If anomaly detected, send REST API call to ONOS.
