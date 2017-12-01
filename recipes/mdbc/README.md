MDBC
====

This is MDBC, the Music DataBase Connector.  It allows an application to use normal SQL
syntax and semantics, while simultaneously copying all changed rows to/from MUSIC.
It is implemented as a Java JDBC driver.

## Building MDBC

MDBC is built with Maven.  This directory contains two pom.xml files.
The first (*pom.xml*) will build a jar file to be used by applications wishing to use the
MDBC JDBC driver.
The second (*pom-h2server.xml*) is used to built the special code that needs to be loaded
into an H2 server, when running MDBC against a copy of H2 running as a server.

### Building the JBoss MDBC Module

There is a shell script (located in `src/main/shell/mk_jboss_module`) which, when run in
the mdbc source directory, will create a tar file `target/mdbc-jboss-module.tar` which can
be used as a JBoss module.  This tar file should be installed by un-taring it in the
$JBOSS_DIR/modules directory on the JBoss server.

## Using MDBC

This package provides a JDBC driver that can be used to mirror the contents of a database
to and from Cassandra. The mirroring occurs as a side effect of execute() statements against
a JDBC connection, and triggers placed in the database to catch database modifications.
The initial implementation is written to support H2, MySQL, and MariaDB databases.

This JDBC driver will intercept all table creations, SELECTs, INSERTs, DELETEs, and UPDATEs
made to the underlying database, and make sure they are copied to Cassandra.
In addition, for every table XX that is created, another table DIRTY\_XX will be created to
communicate the existence of dirty rows to other Cassandra replicas (with the Cassandra2
Mixin, the table is called DIRTY\_\_\_\_ and there is only one table).
Dirty rows will be copied, as needed back into the database from Cassandra before any SELECT.

### To use directly with JDBC

1. Add this jar, and all dependent jars to your CLASSPATH.
2. Rewrite your JDBC URLs from jdbc:_yourdb_:... to jdbc:mdbc:....
3. If you supply properties to the DriverManager.getConnection(String, Properties) call,
 use the properties defined below to control behavior of the proxy.
4. Load the driver using the following call:
        Class.forName("com.att.research.mdbc.ProxyDriver");

The following properties can be passed to the JDBC DriverManager.getConnection(String, Properties)
call to influence how MDBC works.

| Property Name	     | Property Value	                                                              | Default Value |
|--------------------|--------------------------------------------------------------------------------|---------------|
| MDBC\_DB\_MIXIN	 | The mixin name to use to select the database mixin to use for this connection. | h2            |
| MDBC\_MUSIC\_MIXIN | The mixin name to use to select the MUSIC mixin to use for this connection.    | cassandra2    |
| myid	             | The ID of this replica in the collection of replicas sharing the same tables.  | 0             |
| replicas           | A comma-separated list of replica names for the collection of replicas sharing the same tables. | the value of myid |
| music\_keyspace    | The keyspace name to use in Cassandra for all tables created by this instance of MDBC. | mdbc  |
| music\_address     | The IP address to use to connect to Cassandra.	                              | localhost     |
| music\_rfactor     | The replication factor to use for the new keyspace that is created.	          | 2            	 |
| disabled	         | If set to true the mirroring is completely disabled; this is the equivalent of using the database driver directly. | false |

The values of the mixin properties may be:

| Property Name	     | Property Value | Purpose |
|--------------------|----------------|---------------|
| MDBC\_DB\_MIXIN	 | h2             | This mixin provides access to either an in-memory, or a local (file-based) version of the H2 database. |
| MDBC\_DB\_MIXIN	 | h2server       | This mixin provides access to a copy of the H2 database running as a server. Because the server needs special Java classes in order to handle certain TRIGGER actions, the server must be et up in a special way (see below). |
| MDBC\_DB\_MIXIN	 | mysql          | This mixin provides access to MySQL or MariaDB running on a remote server. |
| MDBC\_MUSIC\_MIXIN | cassandra      | A Cassandra based persistence layer (without any of the table locking that MUSIC normally provides). |
| MDBC\_MUSIC\_MIXIN | cassandra2     | Similar to the _cassandra_ mixin, but stores all dirty row information in one table, rather than one table per real table. |

### To Define a JBoss DataSource

The following code snippet can be used as a guide when setting up a JBoss DataSource.
This snippet goes in the JBoss *service.xml* file. The connection-property tags may
need to be added/modified for your purposes.  See the table above for names and values for
these tags.

```
<datasources>
  <datasource jta="true" jndi-name="java:jboss/datasources/ProcessEngine" pool-name="ProcessEngine" enabled="true" use-java-context="true" use-ccm="true">
    <connection-url>jdbc:mdbc:/opt/jboss-eap-6.2.4/standalone/camunda-h2-dbs/process-engine;DB_CLOSE_DELAY=-1;MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE</connection-url>
    <connection-property name="music_keyspace">
      camunda
    </connection-property>
    <driver>mdbc</driver>
    <security>
      <user-name>sa</user-name>
      <password>sa</password>
    </security>
  </datasource>
  <drivers>
    <driver name="mdbc" module="com.att.research.mdbc">
      <driver-class>com.att.research.mdbc.ProxyDriver</driver-class>
    </driver>
  </drivers>
</datasources>
```

Note: This assumes that you have built and installed the com.att.research.mdbc module within JBoss.

### To Define a Tomcat DataSource Resource

The following code snippet can be used as a guide when setting up a Tomcat DataSource resource.
This snippet goes in the Tomcat *server.xml* file.  As with the JBoss DataSource, you will
probably need to make changes to the _connectionProperties_ attribute.

```
<Resource name="jdbc/ProcessEngine"
    auth="Container"
    type="javax.sql.DataSource"
    factory="org.apache.tomcat.jdbc.pool.DataSourceFactory"
    uniqueResourceName="process-engine"
    driverClassName="com.att.research.mdbc.ProxyDriver"
    url="jdbc:mdbc:./camunda-h2-dbs/process-engine;MVCC=TRUE;TRACE_LEVEL_FILE=0;DB_CLOSE_ON_EXIT=FALSE"
    connectionProperties="myid=0;replicas=0,1,2;music_keyspace=camunda;music_address=localhost"
    username="sa"
    password="sa"
    maxActive="20"
    minIdle="5" />
```

## Databases Supported

Currently, the following databases are supported with MDBC:

* H2: The `H2Mixin` mixin is used when H2 is used with an in-memory (`jdbc:h2:mem:...`)
or local file based (`jdbc:h2:path_to_file`) database.

* H2 (Server): The `H2ServerMixin` mixin is used when H2 is used with an H2 server (`jdbc:h2:tcp:...`).

* MySQL: The `MySQLMixin` mixin is used for MySQL.

* MariaDB: The `MySQLMixin` mixin is used for MariaDB, which is functionally identical to MySQL.

## Testing Mixin Combinations

The files under `src/main/java/com/att/research/mdbc/tests` can be used to test various MDBC
operations with various combinations of Mixins.  The tests are controlled via the file
`src/main/resources/tests.json`.  More details are available in the javadoc for this package.

## Limitations of MDBC

* The `java.sql.Statement.executeBatch()` method is not supported by MDBC.
It is not prohibited either; if you use this, your results will be unpredictable (and probably wrong).

* When used with a DB server, there is some delay as dirty row information is copied
from a table in the database, to the dirty table in Cassandra.  This opens a window
during which all sorts of mischief may occur.

* MDBC *only* copies the results of SELECTs, INSERTs, DELETEs, and UPDATEs.  Other database
operations must be performed outside of the purview of MDBC.  In particular, CREATE-ing or
DROP-ing tables or databases must be done individually on each database instance.

* Some of the table definitions may need adjusting depending upon the variables of your use
of MDBC.  For example, the MySQL mixin assumes (in its definition of the MDBC_TRANSLOG table)
that all table names will be no more than 255 bytes, and that tables rows (expressed in JSON)
will be no longer than 512 bytes. If this is not true, you should adjust, edit, and recompile.

* MDBC is limited to only data types that can be easily translated to a Cassandra equivalent;
e.g. BIGINT, BOOLEAN, BLOB, DOUBLE, INT, TIMESTAMP, VARCHAR
