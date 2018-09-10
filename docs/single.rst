======================
Single VM/Site install
======================
Local Installation
------------------
Prerequisites

If you are using a VM make sure it has at least 8 GB of RAM (It may work with 4 GB, but with 2 GB it
does give issues).

Instructions

- Create MUSIC Install dir /opt/app/music
- Open /etc/hosts as sudo and enter the name of the vm alongside localhost in the line for 127.0.0.1. E.g. 127.0.0.1 localhost music-1. Some of the apt-get installation seem to require this.
- Ensure you have OpenJDK 8 on your machine.
- Download Apache Cassandra 3.0, install into /opt/app/music and follow these instructions http://cassandra.apache.org/doc/latest/getting_started/installing.html till and including Step
- By the end of this you should have Cassandra working.
- Download Apache Zookeeper 3.4.6, install into /opt/app/music and follow these instructions https://zookeeper.apache.org/doc/trunk/zookeeperStarted.html pertaining to the standalone operation. By the end of this you should have Zookeeper working.
- Download the Version 8.5 Apache Tomcat and install it using these instructions https://tomcat.apache.org/download-80.cgi (this is for version 8.5).
- Create a music.properties file and place it in /opt/app/music/etc/. Here is a sample of the file:

music.properties::

    music.properties
    my.id=0
    all.ids=0
    my.public.ip=localhost
    all.public.ips=localhost
    ########################
    # Optional current values are defaults
    ######################################
    # If using docker this would point to the specific docker name.
    #zookeeper.host=localhost
    #cassandra.host=localhost
    #music.ip=localhost
 
    #debug=true
    #music.rest.ip=localhost
    #lock.lease.period=6000
    # Cassandra Login - Do not user cassandra/cassandra
    cassandra.user=cassandra1
    cassandra.password=cassandra1
    # AAF Endpoint
    #aaf.endpoint.url=<aaf url>

- Make a dir /opt/app/music/logs MUSIC dir with MUSIC logs will be created in this dir after MUSIC starts.
- Build the MUSIC.war and place in tomcat webapps dir. 
- Authentications/AAF Setup For Authentication setup.
- Start tomcat and you should now have MUSIC running.

Extra Cassandra information for Authentication:

To create first user in Cassandra

1. Edit conf/Cassandra.yaml file::

    authenticator: PasswordAuthenticator
    authorizer: CassandraAuthorizer


2. Restart Cassandra
3. Login to cqlsh with default credentials::

    cqlsh -u cassandra -p cassandra

4. To change default user create new user with the following command.::

    CREATE USER new_user WITH PASSWORD ‘new_password’ SUPERUSER;

5. Change password for default user ‘Cassandra’ so that no one will be able to login::
   
    ALTER USER cassandra WITH PASSWORD ‘SomeLongRandomStringNoonewillthinkof’;

6. Provide the new user credentials to Music. Update music.properties file and uncomment or add the following::

    cassandra.user=<new_user>
    cassandra.password=<new_password>

To access keyspace through cqlsh, login with credentials that are passed to MUSIC while creating the keyspace.

Continue with `Authentication <./automation.rst>`_



   