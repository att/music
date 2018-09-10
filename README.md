xxxtest## MUSIC - Multi-site State Coordination Service
123
To achieve 5 9s of availability on 3 9s or lower software and infrastructure in a cost-effective manner, ONAP components need to work in a reliable, active-active manner across multiple sites (platform-maturity resiliency level 3). A fundamental aspect of this is  state management across geo-distributed sites in a reliable, scalable, highly available and efficient manner. This is an important and challenging problem because of three fundamental reasons:

* Current solutions for state-management of  ONAP components like MariaDB clustering, that work very effectively within a site, may not scale across geo-distributed sites (e.g., Beijing, Amsterdam and Irvine) or allow partitioned operation (thereby compromising availability). This is mainly because WAN latencies are much higher across sites and frequent network partitions can occur.

* ONAP components often have a diverse range of requirements in terms of state replication. While some components need to synchronously manage state across replicas, others may tolerate asynchronous replication. This diversity needs to be leveraged to provide better performance and higher availability across sites.

* ONAP components often need to partition state across different replicas, perform consistent operations on them and ensure that on failover, the new owner has access to the latest state. The distributed protocols to achieve such consistent ownership is complex and replete with corners cases, especially in the face of network partitions. Currently, each component is building its own handcrafted solution which is  wasteful and worse, can be erroneous.

In this project, we identify common state management concerns across ONAP components and provide a multi-site state coordination/management service (MUSIC) with a rich suite of recipes that each ONAP component can simply configure and use for their state-management needs.

---

[Local Installation](#local-install)

[Multi-site Installation](#ms-install)

[Muting MUSIC jersey output](#jersey-mute)

[Authentications/AAF Setup](#auth-setup)


Prerequisites

MUSIC is to be installed in a single Dir. 

```bash
#The main MUSIC dir should be
/opt/app/music
# These also need to be set up
/opt/app/music/etc
/opt/app/music/logs
```
When installing Tomcat, Cassandra and Zookeeper they should also be installed here. 
```bash
/opt/app/music/apache-cassandra-n.n.n
/opt/app/music/zookeeper-n.n.n
/opt/app/music/apache-tomcat-n.n.n
```
Its suggested you create links from install dirs to a common name ie:
```bash 
ln -s /opt/app/music/apache-cassandra-n.n.n cassandra
ln -s /opt/app/music/zookeeper-n.n.n zookeeper
ln -s /opt/app/music/apache-tomcat-n.n.n tomcat
```
 
Cassandra and Zookeeper have data dirs. 
```bash
# For cassandra it should be (This is the default) 
/opt/app/music/cassandra/data
# For Zookeeper it should be 
/opt/app/music/var/zookeeper/
```
 
If you are using a VM make sure it has at least 8 GB of RAM (It may work with 4 GB, but with 2 GB it
does give issues).

<a name="local-install"> 

## Local Installation 

</a>

### Instructions

- Create MUSIC Install dir /opt/app/music
- Open /etc/hosts as sudo and enter the name of the vm alongside localhost in the line for 127.0.0.1. E.g. 127.0.0.1 localhost music-1. Some of the apt-get installation seem to require this.
- Ensure you have java jdk 8 or above working on your machine.
- Download apache Apache Cassandra 3.2, install into /opt/app/music and follow these instructions http://cassandra.apache.org/doc/latest/getting_started/installing.html till and including Step
- By the end of this you should have Cassandra working.
- Download Apache Zookeeper 3.4.6, install into /opt/app/music and follow these instructions https://zookeeper.apache.org/doc/trunk/zookeeperStarted.html pertaining to the standalone operation. By the end of this you should have Zookeeper working.
- Create a music.properties file and place it in /opt/app/music/etc/. Here is a sample of the file:

```properties 
my.id=0
all.ids=0
my.public.ip=localhost
all.public.ips=localhost
#######################################
# Optional current values are defaults
#######################################
#zookeeper.host=localhost
#cassandra.host=localhost
#music.ip=localhost
#debug=true
#music.rest.ip=localhost
#lock.lease.period=6000
cassandra.user=cassandra
cassandra.password=cassandra
aaf.endpoint.url=http://aafendpoint/proxy/authz/nss/
```

- Make a dir /opt/app/music/logs MUSIC dir with MUSIC logs will be created in this dir after MUSIC starts.
- Download the latest Apache Tomcat and install it using these instructions http://tecadmin.net/install-tomcat-9-on-ubuntu/ (this is for version 9).
- Build the MUSIC.war (or download it from https://github.com/att/music/blob/master/MUSIC.war) and place it within the webapps folder of the tomcat installation.
- Authentications/AAF Setup For Authentication setup.
- Start tomcat and you should now have MUSIC running.

 
Extra Cassandra information for Authentication:

To create first user in Cassandra

Edit conf/Cassandra.yaml file

```yaml 
authenticator: PasswordAuthenticator
authorizer: CassandraAuthorizer
```
Restart Cassandra
Login to cqlsh with default credentials

```bash
cqlsh -u cassandra -p cassandra
```

To change default user create new user with the following command.

```sql
CREATE USER new_user WITH PASSWORD ‘new_password’ SUPERUSER;
```
Change password for default user ‘Cassandra’ so that no one will be able to login

```sql
ALTER USER cassandra WITH PASSWORD ‘SomeLongRandomStringNoOneWillThinkOf’;
```
Provide the new user credentials to Music. Update music.properties file and uncomment or add the following:

```properties
cassandra.user=<new_user>
cassandra.password=<new_password>
```

To access keyspace through cqlsh, login with credentials that are passed to MUSIC while creating the keyspace.



<a name ="ms-install">

## Multi-site Installation 

</a>

Follow the instructions for local MUSIC installation on all the machines/VMs/hosts (referred to as a node) on which you want MUSIC installed. However, Cassandra and Zookeeper needs to be configured to run as multi-node installations (instructions below) before running them.

#### Cassandra:
In the cassandra.yaml file which is present in the cassa_install/conf directory in each node, set the following parameters:
cassandra.yaml
```yaml
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
authenticator: PasswordAuthenticator
authorizer: CassandraAuthorizer
```
 

In the cassandra-rackdc.properties file, assign data center and rack names as if required.

Once this is done on all three nodes, you can run cassandra on each of the nodes through the cassandra bin folder with this command     
```bash
./cassandra
```
    In the cassandra bin folder, if you run the following it will tell you the state of the cluster.       
```bash
./nodetool status
```
To access cassandra, one any of the nodes you can run the following and then perform CQL queries.
```bash
./cqlsh <private ip>
```

To create first user in Cassandra

    Restart Cassandra
    Login to cqlsh with default credentials

```bash
cqlsh -u cassandra -p cassandra
```

To change default user create new user with the following command.

```sql
CREATE USER new_user WITH PASSWORD ‘new_password’ SUPERUSER;
```
Change password for default user ‘Cassandra’ so that no one will be able to login

```sql
ALTER USER cassandra WITH PASSWORD ‘SomeLongRandomStringNoOneWillThinkOf’;
```
Provide the new user credentials to Music. Update music.properties file and uncomment or add the following:

```properties
cassandra.user=<new_user>
cassandra.password=<new_password>
```

To access keyspace through cqlsh, login with credentials that are passed to MUSIC while creating the keyspace.

#### Zookeeper:

Once zookeeper has been installed on all the nodes, modify the  zk_install_location/conf/zoo.cfg on all the nodes with the following lines:
```properties
tickTime=2000
dataDir=/opt/app/music/var/zookeeper
clientPort=2181
initLimit=5
syncLimit=2
quorumListenOnAllIPs=true
server.1=public IP of node 1:2888:3888
server.2=public IP of node 2:2888:3888
server.3=public IP of node 3:2888:3888
```
In /opt/app/music/var/zookeeper in all the machines, create a file called myid that contains the id of the machine. The machine running node.i will contain just the number i in the file myid.

Start each of the nodes one by one from the zk_install_location/bin folder using the command:
```bash
./zkServer.sh start
```
On each node check the file zookeeper.out in the  zk_install_location/ bin to make sure all the machines are talking to each other and there are no errors. Note that while the machines are yet to come up there maybe error messages saying that connection has not yet been established. Clearly, this is ok.

If there are no errors, then from zk_install_location/bin simply run the following to get command line access to zookeeper.   ./zkCli.sh

Run these commands on different machines to make sure the zk nodes are syncing.
```bash
[zkshell] ls /
[zookeeper]
```
Next, create a new znode by running
```bash
create /zk_test my_data.
```
This creates a new znode and associates the string "my_data" with the node. You should see:
```bash
[zkshell] create /zk_test my_data
Created /zk_test
```
Issue another ls / command to see what the directory looks like:
```bash
[zkshell] ls /
[zookeeper, zk_test]
```

#### MUSIC

Create a music.properties file and place it in /opt/app/music/etc at each node. Here is a sample of the file: If this location is to be changed please update the file project.properties in the src/main/resources directory before compiling MUSIC to a war.
```properties
my.id=0
all.ids=0:1:2
my.public.ip=public IP of node 0
#For each node, a separate file needs to be created with its own id (between 0 and the number of nodes) and with information about its own public ip.
all.public.ips=public IP of node 0:public IP of node 1:public IP of node 2
#######################################
# Optional current values are defaults
#######################################
#zookeeper.host=localhost
#cassandra.host=localhost
#music.ip=localhost
#debug=true
#music.rest.ip=localhost
#lock.lease.period=6000
cassandra.user=cassandra
cassandra.password=cassandra
aaf.endpoint.url=http://aafendpoint/proxy/authz/nss/
```
 
- Download the latest Apache Tomcat and install it using these instructions http://tecadmin.net/install-tomcat-9-on-ubuntu/ (this is for version 9).
- Build the MUSIC.war and place it within the webapps folder of the tomcat installation.
- Start tomcat and you should now have MUSIC running.

- For Logging create a dir /opt/app/music/logs. When MUSIC/Tomcat starts a dir opt/app/music/logsMUSIC with various logs will be created.
 The application log is music.log and the others are debug.log and error.log.

<a name="jersey-mute">

### Muting MUSIC jersey output

The jersey package that MUSIC uses to parse REST calls prints out the entire header and json body by
default. To mute it (if it exists), remove the following lines from the web.xml in the WEB_INF foler:

```xml
<init-param>
  <param-name>com.sun.jersey.spi.container.ContainerResponseFilters</param-name>
  <param-value>com.sun.jersey.api.container.filter.LoggingFilter</param-value>
</init-param>
```

<a name ="auth-setup">

### Authentication Setup

MUSIC has been enhanced to support applications which are already authenticated using AAF and applications which are not authenticated using AAF. 
If an application has already been using AAF, it should have required namespace, mechId and password. Non AAF applications will be provided with a random unique id. MUSIC will tag the keyspace with the UID and store internally and any calls to use/modify the keyspace will require the UID. 
All the required params should be sent as headers.

- MUSIC has been enhanced to support applications which are already authenticated using AAF and applications which are not authenticated using AAF.
- If an application has already been using AAF, it should have required namespace, mechId and password.
- Non AAF applications (AID) will be provided with a random unique id. MUSIC will tag the keyspace with the UID and store internally and any calls to use/modify the keyspace will require the UID.

All the required params should be sent as headers.

Admin needs to create the following keyspace and table via cslsh.

In the cassandra bin dir run ./cqlsh -u <user> -p <password> and log in to db then:

If you want to save the following in a file you can then run ./cqlsh -f <file.cql>
For Single install:
```sql
//Create Admin Keyspace
 
CREATE KEYSPACE admin
  WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor': 1
  }
  AND DURABLE_WRITES = true;
 
CREATE TABLE admin.keyspace_master (
  uuid uuid,
  keyspace_name text,
  application_name text,
  is_api boolean,
  password text,
  username text,
  is_aaf boolean,
  PRIMARY KEY (uuid)
);
```
Multi-Site Install:
```sql
//Create Admin Keyspace
 
CREATE KEYSPACE admin
  WITH REPLICATION = {
    'class' : 'NetworkTopologyStrategy',
    'DC1':2,
    'DC2':2,
    'DC3':2
  }
  AND DURABLE_WRITES = true;
 
CREATE TABLE admin.keyspace_master (
  uuid uuid,
  keyspace_name text,
  application_name text,
  is_api boolean,
  password text,
  username text,
  is_aaf boolean,
  PRIMARY KEY (uuid)
);
```

Headers:
- For AAF applications all the 3 of the following headers are mandatory. 
  - <b>ns</b>
  - <b>mechId</b>
  - <b>password</b>
- For Non AAF applications if the header <b>aid</b> is not provided MUSIC creates new random unique UUID and returns to caller. 
- Caller application then need to save the UUID and need to pass the UUID to further modify/access the keyspace.
- Make sure steps to set up Cassandra for authentication above in the cassandra.yaml were done.


### On Boarding app
The following API is used to onboard the applications.
/MUSIC/rest/v2/admin/onboardAppWithMusic 
This is a POST call and requires the following JSON:
```json
{
"appname": "<the Namespace for aaf or the Identifier for the specific app using AID access>",
"userId" : "<userid>",
"isAAF"  : true|false
}
```

