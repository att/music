..
  This licence applies to all files in this repository unless otherwise specifically
  stated inside of the file.

  ---------------------------------------------------------------------------  
   Copyright (c) 2016 AT&T Intellectual Property

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at:

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
  ---------------------------------------------------------------------------  

==================================
Multi-site Installation Guide for Music
==================================

Instructions
============

General stuff and Java:
======================

Open /etc/hosts as sudo and enter the name of the vm alongside localhost in the line for 127.0.0.1.
E.g. 127.0.0.1 localhost music-1

Install Java using the instructions here. I followed these commands:
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
sudo apt-get install oracle-java8-set-default
Run java -version to make sure the right version of java is running (1.8.something)


Cassandra:
==========

Download and decompress cassandra using the following command:
curl -L http://archive.apache.org/dist/cassandra/3.2/apache-cassandra-3.2-bin.tar.gz | tar xz


In the cassandra.yaml file which is present in the cassa_install/conf directory, set the following
parameters:
	cluster_name: ‘name of cluster’
	num_tokens: 256
	seed_provider:
  	- class_name: org.apache.cassandra.locator.SimpleSeedProvider
    	parameters:
         - seeds:  "<public ip of first seed>, <public ip of second seed>, etc"
	listen_address: private ip of VM 
	broadcast_address: public ip of VM
	endpoint_snitch: GossipingPropertyFileSnitch
	rpc_address: <private ip> //note..using cqlsh u need to say ./cqlsh <private ip> cos of this
	phi_convict_threshold: 12

The last one was because of an error I was facing and its corresponding resolution as described
here. Not very common. 

In the cassandra-rackdc.properties file, assign some data center and rack names. For example (YOU
CAN IGNORE THIS step is all nodes within DC):
Nodes 0 and 1
# indicate the rack and dc for this node
	dc=BigSite
	rack=RAC1:
Node 2
# indicate the rack and dc for this node
	dc=Agave
	rack=RAC1

Go to the install directory/bin and start cassandra: sudo ./cassandra. Wait till you get “No gossip
backlog; proceeding”. To confirm that is running also do “ps auwx | grep cassandra” and make sure a
process is running. 
In the same bin folder, if you can run ./cqlsh  <private ip> and see the command prompt “cqlsh>”
then the node has cassandra running. 
To stop cassandra “ps auwx | grep cassandra”, find the process id and “sudo kill pid”
To clean all the data in it: sudo rm -rf /var/lib/cassandra/data/system/*


Zookeeper 
=========

For Zk installation, use a subset of nodes on which cassandra was set up. Need an odd number. I
choose 2 in one and 1 in the other data center for starters. So in this particular example all cassa
nodes are also running zk. 

Download using: curl -L http://apache.arvixe.com/zookeeper/stable/zookeeper-3.4.8.tar.gz | tar xz
curl -L http://apache.claz.org/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz | tar xz

Create a file in all three nodes under zk_install_location/conf/zoo.cfg with the following lines:
tickTime=2000
dataDir=/var/zookeeper
clientPort=2181
initLimit=5
syncLimit=2
quorumListenOnAllIPs=true (//comment: this is key and not there in general instructions)
server.1=zoo1:2888:3888
server.2=zoo2:2888:3888
server.3=zoo3:2888:3888

Modify the zoo1, zoo2 and zoo3 to the IP addresses of the machines in the system. 
Create the directory /var/zookeeper in all the machines and within that create a file called myid
that contains the id of the machine. The machine running server.i will contain just the number i in
the file myid. So for example, if zoo1 = 135.207.223.43, then the machine with that IP will have 1
in its myid file. 

Start each of the servers one by one from the zk_install_location/bin folder using the command “sudo
./zkServer.sh” 

On each server check the file zookeeper.out in the  zk_install_location/bin to make sure all the
machines are talking to each other and there are no errors. Note that while the machines are yet to
come up there maybe error messages saying that connection has not yet been established. Clearly this
is ok.

If there are no errors, then from zk_install_location/bin  simply run ./zkCli.sh to get command line
access to zk. 

Run these commands on different machines to make sure the zk nodes are syncing. i

[zkshell] ls /
	[zookeeper]
       
Next, create a new znode by running create /zk_test my_data. This creates a new znode and associates
the string "my_data" with the node. You should see:
[zkshell] create /zk_test my_data
	Created /zk_test
     
Issue another ls / command to see what the directory looks like:
[zkshell] ls /
	[zookeeper, zk_test]


Webserver 
=========
Install tomcat and deploy MUSIC on all machines using these instructions:

sudo apt-get update
sudo apt-get install tomcat8.0.26
In /etc/default/tomcat7 change java_home to point to correct location
Copy MUSIC.war into var/lib/tomcat7/webapps

Testing the client app
======================

Download the client app for Music from `here
<https://github.com/att/music/tree/master/tests/musicTest.jar>`__, then use a java editor to
import the maven project, VoteAppForMUSIC. In the file VotingApp.java, change the musicIps to point
to your music ips and run the file.  The expected output
should be pretty easy to understand just by looking at the file VotingApp.java.




	
	


