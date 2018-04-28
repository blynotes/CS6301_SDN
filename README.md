# Machine Learning Approach for an Anomaly Intrusion Detection System using ONOS

University of Texas at Dallas<br />
CS 6301-503 Software Defined Networking

<u>Professor:</u><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Timothy Culver

<u>Team Members:</u><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Stephen Blystone<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Taniya Riar<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Juhi Bhandari<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Ishwank Singh<br />

<u>Project Name:</u><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Machine Learning Approach for an Anomaly Intrusion Detection System using ONOS

<u>Project Report:</u><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Machine Learning Approach for an Anomaly IDS using ONOS.docx

<u>Project Presentation:</u><br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Project Presentation.pptx

======================================================================

### SETTING UP THE PROJECT

For the ONOS VM:<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Follow the instructions in the "ONOS 1.12 installation Guide.docx" Guide.

For the Mininet VM:<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Follow the instructions in the "Mininet VM Guide.docx" Guide.

For the Application VM:<br />
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Follow the instructions in the "App Installation Guide.docx" Guide.

======================================================================

### RUNNING THE PROJECT

Follow the instructions to run the project and to stop running the project in the "Run Applications Guide.docx" Guide.

======================================================================
### FILES IN PROJECT DIRECTORY

<ul>
<li>Elasticsearchdata_csv.ipynb
	<ul><li>iPython Jupyter Notebook used to visualize the ElasticsearchNormaldata.csv and use elbow graph to determine K value.</li></ul></li>
<br />
<li>ElasticsearchNormaldata.csv
	<ul><li>"Normal" traffic data stored in Elasticsearch.</li></ul></li>
<br />
<li>Flow Diagram.pptx
	<ul><li>Diagrams used in presentations.</li></ul></li>
<br />
<li>Important Netflow Fields.txt
	<ul><li>Sample NetFlow data captured with only important fields remaining.</li></ul></li>
<br />
<li>Machine Learning Approach for an Anomaly IDS using ONOS.docx
	<ul><li>Project Report</li></ul></li>
<br />
<li>Netflow Field Explanations.txt
	<ul><li>Explanation of NetFlow fields.</li></ul></li>
<br />
<li>Project Presentation.pptx
	<ul><li>Project Presentation.</li></ul></li>
<br />
<li>README.txt
	<ul><li>This README file.</li></ul></li>
<br />
<li>sampleNetflowData.txt
	<ul><li>Sample raw NetFlow data.</li></ul></li>
<br />
<li>SDN Project Proposal.docx
	<ul><li>Our Project Proposal.</li></ul></li>
</ul>

======================================================================

### PROJECT GUIDES (located in the "Guides" directory)

<ul>
<li>App Installation Guide.docx
	<ul><li>How to install and configure everything in the Application VM.</li></ul></li>
<br />
<li>Mininet VM Guide.docx
	<ul><li>Install required packages into the VM to run Mininet and trigger the anomalies.</li></ul></li>
<br />
<li>NetFlow Guide.docx
	<ul><li>Guide for how to configure NetFlow on Open vSwitch.</li></ul></li>
<br />
<li>ONOS 1.12 installation Guide.docx
	<ul><li>3 methods of installing and configuring ONOS:
	<ol><li>Option 1 installs an OVA file and provides a link to a Distributed ONOS tutorial.</li>
	<li>Option 2 installs ONOS as a service (I did not get this to work).</li>
	<li>Option 3 is the recommended option. There is also information for configuring IntelliJ if building an Internal ONOS application.</li></ol></li></ul></li>
<br />
<li>ONOS Rest API Guide.docx
	<ul><li>Contains information on how to view a nice webpage on localhost (after launching ONOS) to query the ONOS REST API.</li></ul></li>
<br />
<li>Run Applications Guide.docx
	<ul><li>How to start and stop all applications in the Big Data pipeline and run the demo.</li></ul></li>
</ul>

======================================================================

### CODE DESCRIPTIONS (located in the "Code/src" directory)

<b>NOTE: All code files use UNIX EOL characters (line-feed "\n"). Opening these files in most Windows programs will not maintain the formatting, since Windows expects carriage-return line-feed "\r\n". If you use Windows, opening the files using Notepad++ will maintain the correct formatting.</b>

<ul>
<li>Client.py
	<ul><li>Generates random “Normal” traffic.</li></ul></li>
<br />
<li>index_ES.txt
	<ul><li>Information placed into Kibana Dev Tool to create our Elasticsearch index.</li></ul></li>
<br />
<li>scapyPortScan.py
	<ul><li>Use Python library Scapy to perform a UDP port scan from port 1 to port 65535 on the target device.</li></ul></li>
<br />
<li>Server.py
	<ul><li>Receives messages from Client.py from other hosts and responds.</li></ul></li>
<br />
<li>setup_topo.py
	<ul><li>Setup Mininet topology, configure Open vSwitches with NetFlow, call Client.py and Server.py for each Mininet host.</li></ul></li>
<br />
<li>sparkKafka.py
	<ul><li>Perform feature engineering to get our features and send to Elasticsearch.</li></ul></li>
<br />
<li>sparkMachineLearning.py
	<ul><li>Train K-Means algorithm on data in Elasticsearch, perform feature engineering on new data, standardize new data and check if anomaly. If anomaly detected, send REST API call to ONOS.</li></ul></li>
</ul>
