
# Multi-site Coordination for Replicated Services

The complexity of replicated, multi-site
distributed applications brings forth the need for rich distribution coordination patterns to manage
these applications. We contend that, to build such patterns, it is necessary to tightly integrate
coordination primitives such as mutual exclusion and barriers with state-management in these
replicated systems. This is easier said than done, since coordination primitives typically need
strong consistency that may render them unavailable during partitions. On the other hand, the
relative ubiquity of network partitions and large WAN latencies in a multi-site setting dictate that
replicated state is usually maintained in an eventually consistent store. We address this conflict
by presenting a MUlti-SIte Coordination service (MUSIC), that combines a strongly consistent locking
service with an eventually consistent state store to provide abstractions that enable rich
distributed coordination on shared state, as and when required.

[Local Installation](#local-install)

[Multi-site Installation](#ms-install)

[Logging](#ms-logging)




<a name="local-install"> 

## Local Installation 

</a>

### Prerequisites

If you are using a VM make sure it has at least 8 GB of RAM (It may work with 4 GB, but with 2 GB it
does give issues).

### Instructions

- Open /etc/hosts as sudo and enter the name of the vm alongside localhost in the line for
  127.0.0.1.
E.g. 127.0.0.1 localhost music-1. Some of the apt-get installation seem to require this. 
- Ensure you have java jdk 8 or above working on your machine.
- Download apache Apache Cassandra 3.2 and follow these instructions
  <http://cassandra.apache.org/doc/latest/getting_started/installing.html> till and including Step
4. By the end of this you should have Cassandra working.
- Download Apache Zookeeper 3.4.6 from and follow these instructions
  <https://zookeeper.apache.org/doc/trunk/zookeeperStarted.html> pertaining to the standalone
operation. By the end of this you should have Zookeeper working.
- Download the latest Apache Tomcat and follow these instructions
  <http://tecadmin.net/install-tomcat-9-on-ubuntu/> (this is for version 9).  
- Create a music.properties file and place it /etc/music/music.properties. Here is a simple of the file: 

			myId=0
			all.ids=0
			my.public.ip=localhost
			all.public.ips=localhost
	
- Build the MUSIC war file and place within the webapps folder of the tomcat installation.
- Download the client app for MUSIC from
  <https://github.com/att/music/tree/master/tests/musicTest.jar>, and run the jar file
musicTest.jar with localhost as parameter. If there
are no errors and all the tests pass, then you have MUSIC working. 

<a name ="ms-install">

## Multi-site Installation 

</a>

- Follow the instructions for local MUSIC installation on all the machines/VMs/hosts (referred to as
  a node) on which you
want MUSIC installed. However, Cassandra and Zookeeper needs to be configured to run as multi-node
installations (instructions below) before running them. 

- Cassandra: 

	- In the cassandra.yaml file which is present in the cassa_install/conf 	directory in each node,
	  set the following parameters:


			cluster_name: ‘name of cluster’
			num_tokens: 256
			seed_provider:
 			class_name: org.apache.cassandra.locator.SimpleSeedProvider
  			parameters:
   			seeds:  "<public ip of first seed>, <public ip of second seed>, etc"
			listen_address: private ip of VM 
			broadcast_address: public ip of VM
			endpoint_snitch: GossipingPropertyFileSnitch
			rpc_address: <private ip> 
			phi_convict_threshold: 12
	

		The last one was because of an error I was facing and its corresponding 		resolution as
described here. Not very common. 

	- In the cassandra-rackdc.properties file, assign data center and rack names 	as if required. 
	- Once this is done on all three nodes, you can run cassandra on each of the nodes through the
	  cassandra bin folder with this command:
	
			./cassandra
	- In the cassandra bin folder, if you run 
	
			./nodetool status
	 it will tell you the state of the cluster. 
	- To access cassandra, one any of the nodes you can run

			./cqlsh <private ip>
		and then perform CQL queries. 
		

- Zookeeper: 
	
	- Once zookeeper has been installed on all the nodes, modify the  zk_install_location/conf/zoo.cfg
	  on all the nodes with the following lines:
		
			tickTime=2000
			dataDir=/var/zookeeper
			clientPort=2181
			initLimit=5
			syncLimit=2
			quorumListenOnAllIPs=true 
			node.1=public IP of node 1:2888:3888
			node.2=public IP of node 2:2888:3888
			node.3=public IP of node 3:2888:3888


	- Create the directory /var/zookeeper in all the machines and within that 	  create a file called
	  myid that contains the id of the machine. The machine 	  running node.i will contain just the
number i in
	  the file myid. 
	  
	- Start each of the nodes one by one from the zk_install_location/bin 	  folder using the
	  command:
	
			sudo ./zkServer.sh start

	- On each node check the file zookeeper.out in the  zk_install_location/	  bin to make sure all
	  the machines are talking to each other and there are 	  no errors. Note that while the machines
are yet to come up there maybe 	  error messages saying that connection has not yet been
established. 	  Clearly, this is ok.

	- If there are no errors, then from zk_install_location/bin simply run 
	
				./zkCli.sh 
				
	  to get command line access to zookeeper. 

	- Run these commands on different machines to make sure the zk nodes are 		syncing. 

			[zkshell] ls /
			[zookeeper]
       
		Next, create a new znode by running 
		
			create /zk_test my_data. 
		This creates a new znode and associates the string "my_data" with the 		node. You should see:
		
			[zkshell] create /zk_test my_data
			Created /zk_test
     
		Issue another ls / command to see what the directory looks like:
		
			[zkshell] ls /
			[zookeeper, zk_test]
			
- Download the latest Apache Tomcat and follow these instructions
  <http://tecadmin.net/install-tomcat-9-on-ubuntu/> (this is for version 9). 
- Create a music.properties file and place it in /etc/music/music.properties at each node. Here is a simple of the file: 

			myId=0
			all.ids=0:1:2
			my.public.ip=public IP of node 0
			all.public.ips=public IP of node 0:public IP of node 1:public IP of node 2

- Build the MUSIC war file and place within the webapps folder of the tomcat installation.
- Download the client app for MUSIC from
  <https://github.com/att/music/tree/master/tests/musicTest.jar>, and run the jar file
musicTest.jar with any of the node public IPs as parameter. If there
are no errors and all the tests pass, then you have MUSIC working. 

<a name="ms-logging">

## Logging

</a>
### log4j

This section explains how MUSIC log4j properties can be used and modified to control logging. 

Once MUSIC.war is installed, tomcat7 will unpack it into /var/lib/tomcat7/webapps/MUSIC (this is the
standard Ubuntu installation, the location may differ for self installs).

Look at /var/lib/tomcat7/webapps/MUSIC/WEB-INF/log4j.properties:

```properties
   # Root logger option
   log4j.rootLogger=INFO, file

   # Direct log messages to a log file
   log4j.appender.file=org.apache.log4j.RollingFileAppender
   log4j.appender.file.File=/var/log/music/music.log
   log4j.appender.file.MaxFileSize=10MB
   log4j.appender.file.MaxBackupIndex=10
   log4j.appender.file.layout=org.apache.log4j.PatternLayout
   log4j.appender.file.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n

   # Direct log messages to stdout to use this option, add stdout to rootLogger options
   log4j.appender.stdout=org.apache.log4j.ConsoleAppender
   log4j.appender.stdout.Target=System.out
   log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
   log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n
```

Notice there are two log4j.appender sections. The first one directs log lines to a file. The second
one directs log lines to stdout (which winds up in catalina.out). 

The music.log is placed /var/log/music/music.log. To change this location modify this line in the web.xml of MUSIC within the WEB-INF folder:

		<init-param>
			<param-name>log4j-properties-location</param-name>
			<param-value>/etc/music/log4j.properties</param-value>
		</init-param>


To redirect MUSIC's log info to a log file, with more control over rotation rules:

5. Adjust "MaxFileSize" to the largest size desired for each log file prior to rotation.
6. Adjust "MaxBackupIndex" to the max number of desired rotated logs.
7. Remove any unwanted files from /var/log/tomcat7.
8. Restart tomcat7 with "service tomcat7 restart".

Note that the logrotate.d settings for tomcat7 may stay in place (for catalina.out). In the case of
MUSIC, logrotate.d may not run often enough for the file to be
rotated before running out of disk space. It's expected that using log4j's rotation in conjunction
with a separate log file will help alleviate filesystem pressure.

More info about log4j.properties:

https://docs.oracle.com/cd/E29578_01/webhelp/cas_webcrawler/src/cwcg_config_log4j_file.html

MUSIC uses log4j 1.2.17 which is EOL. MUSIC will be changing to 2.x, at which point this
file's syntax will change significantly (new info will be sent at that time).

### Muting MUSIC jersey output

The jersey package that MUSIC uses to parse REST calls prints out the entire header and json body by
default. To mute it (if it exists), remove the following lines from the web.xml in the WEB_INF foler:

```xml
<init-param>
  <param-name>com.sun.jersey.spi.container.ContainerResponseFilters</param-name>
  <param-value>com.sun.jersey.api.container.filter.LoggingFilter</param-value>
</init-param>
```
