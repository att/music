Setup for Developing MUSIC
==========================

.. toctree::
   :maxdepth: 1

   Single-Site Install <single>
   Muili-Site Install <multi>
   Authentication

MUSIC is to be installed in a single Dir on a vm. 


The main MUSIC dir should be::

    /opt/app/music
    # These also need to be set up
    /opt/app/music/etc
    /opt/app/music/logs
    /opt/app/music/lib/zookeeper

When installing Tomcat, Cassandra and Zookeeper they should also be installed here.::

    /opt/app/music/apache-cassandra-n.n.n
    /opt/app/music/zookeeper-n.n.n
    /opt/app/music/apache-tomcat-n.n.n


You could also create links from install dirs to a common name ie\:::

    ln -s /opt/app/music/apache-cassandra-n.n.n cassandra
    ln -s /opt/app/music/zookeeper-n.n.n zookeeper
    ln -s /opt/app/music/apache-tomcat-n.n.n tomcat

Cassandra and Zookeeper have data dirs.::
    
    # For cassandra it should be (This is the default) 
    /opt/app/music/cassandra/data    
    # For Zookeeper it should be 
    /opt/app/music/zookeeper/


Continue by selecting the link to the setup you are doing.

.. toctree::
   :maxdepth: 1

   Single-Site Install <single>
   Muili-Site Install <multi>
   Authentication
